/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.lakestream.impl;

import io.lakestream.api.LogId;
import io.lakestream.api.StreamLayout;
import io.lakestream.api.StreamReader;
import io.lakestream.ursa.storage.OwnedResultFutures;
import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;
import java.util.function.Supplier;
import lombok.extern.slf4j.Slf4j;

/**
 * A reader handle whose lifetime is visible to its owning catalog.
 *
 * <p>Closing rejects new operations, drains operations already handed to the delegate, closes the
 * delegate, and only then releases the catalog handle. Cancellation of a caller-facing future
 * never shortens that lifetime; the delegate operation is allowed to finish and any undeliverable
 * entries are released.
 */
@Slf4j
final class CatalogOwnedStreamReader implements StreamReader {

    private static final long CLOSE_TIMEOUT_MILLIS = TimeUnit.SECONDS.toMillis(10);
    private static final long EVENTUAL_CLOSE_INITIAL_RETRY_MILLIS = 100L;
    private static final long EVENTUAL_CLOSE_MAX_RETRY_MILLIS = TimeUnit.SECONDS.toMillis(10);

    private final StreamReader delegate;
    private final Executor delegateCloseExecutor;
    private final Runnable onFullyClosed;
    private final long closeTimeoutMillis;
    private final Object lifecycleMutex = new Object();

    private boolean closing;
    private int activeOperations;
    private CompletableFuture<Void> operationsDrained;
    private CompletableFuture<Void> delegateCloseFuture;
    private boolean delegateClosed;
    private boolean fullyClosedNotified;
    private CompletableFuture<Void> eventualCloseFuture;

    private final class TrackedOperationFuture<T> extends CompletableFuture<T> {

        @Override
        public boolean cancel(boolean mayInterruptIfRunning) {
            synchronized (this) {
                return super.cancel(mayInterruptIfRunning);
            }
        }

        @Override
        public boolean complete(T value) {
            synchronized (this) {
                return super.complete(value);
            }
        }

        @Override
        public boolean completeExceptionally(Throwable failure) {
            synchronized (this) {
                return super.completeExceptionally(failure);
            }
        }

        private boolean deliver(T value) {
            synchronized (this) {
                if (isDone()) {
                    return false;
                }
                operationFinished();
                return super.complete(value);
            }
        }

        private void deliverFailure(Throwable failure, boolean canceledAtSource) {
            synchronized (this) {
                operationFinished();
                if (canceledAtSource) {
                    super.cancel(false);
                } else {
                    super.completeExceptionally(failure);
                }
            }
        }
    }

    CatalogOwnedStreamReader(
            StreamReader delegate, Executor delegateCloseExecutor, Runnable onFullyClosed) {
        this(delegate, delegateCloseExecutor, onFullyClosed, CLOSE_TIMEOUT_MILLIS);
    }

    CatalogOwnedStreamReader(
            StreamReader delegate,
            Executor delegateCloseExecutor,
            Runnable onFullyClosed,
            long closeTimeoutMillis) {
        this.delegate = Objects.requireNonNull(delegate, "delegate");
        this.delegateCloseExecutor = Objects.requireNonNull(
            delegateCloseExecutor, "delegateCloseExecutor");
        this.onFullyClosed = Objects.requireNonNull(onFullyClosed, "onFullyClosed");
        if (closeTimeoutMillis <= 0) {
            throw new IllegalArgumentException("closeTimeoutMillis must be positive");
        }
        this.closeTimeoutMillis = closeTimeoutMillis;
    }

    @Override
    public CompletableFuture<ReadResult> read(
            LogId logId, long startOffset, int maxMessageCount, long maxSizeBytes) {
        return trackOperation(
            () -> delegate.read(logId, startOffset, maxMessageCount, maxSizeBytes), result -> {
            if (result != null) {
                OwnedResultFutures.closeLogEntries(result.entries());
            }
        });
    }

    @Override
    public CompletableFuture<List<LogId>> logIds() {
        return trackOperation(delegate::logIds, ignored -> { });
    }

    @Override
    public StreamLayout layout() {
        return delegate.layout();
    }

    private <T> CompletableFuture<T> trackOperation(
            Supplier<CompletableFuture<T>> operation,
            Consumer<? super T> abandonedResultCleanup) {
        synchronized (lifecycleMutex) {
            if (closing) {
                return CompletableFuture.failedFuture(
                    new IllegalStateException("Stream reader is closing or closed"));
            }
            activeOperations++;
        }

        final CompletableFuture<T> source;
        try {
            source = Objects.requireNonNull(operation.get(), "delegate operation future");
        } catch (RuntimeException | Error failure) {
            operationFinished();
            return CompletableFuture.failedFuture(failure);
        }
        TrackedOperationFuture<T> exposed = new TrackedOperationFuture<>();
        source.whenComplete((result, failure) -> {
            if (failure != null) {
                exposed.deliverFailure(failure, source.isCancelled());
            } else if (!exposed.deliver(result)) {
                try {
                    abandonedResultCleanup.accept(result);
                } finally {
                    operationFinished();
                }
            }
        });
        return exposed;
    }

    private void operationFinished() {
        CompletableFuture<Void> drained = null;
        synchronized (lifecycleMutex) {
            activeOperations--;
            if (activeOperations == 0 && operationsDrained != null) {
                drained = operationsDrained;
                operationsDrained = null;
            }
        }
        if (drained != null) {
            drained.complete(null);
        }
    }

    @Override
    public synchronized void close() throws Exception {
        long deadlineNanos = System.nanoTime()
            + TimeUnit.MILLISECONDS.toNanos(closeTimeoutMillis);
        CompletableFuture<Void> drain;
        synchronized (lifecycleMutex) {
            closing = true;
            if (activeOperations == 0) {
                drain = CompletableFuture.completedFuture(null);
            } else {
                if (operationsDrained == null) {
                    operationsDrained = new CompletableFuture<>();
                }
                drain = operationsDrained;
            }
        }
        awaitCompletion(drain, deadlineNanos,
            "accepted stream-reader operations to drain");
        if (!delegateClosed) {
            CompletableFuture<Void> closeAttempt = startDelegateClose();
            try {
                awaitCompletion(closeAttempt, deadlineNanos, "delegate stream reader to close");
            } catch (ExecutionException failure) {
                synchronized (lifecycleMutex) {
                    if (delegateCloseFuture == closeAttempt) {
                        delegateCloseFuture = null;
                    }
                }
                throwFailure(unwrap(failure));
            }
            delegateClosed = true;
        }
        if (!fullyClosedNotified) {
            onFullyClosed.run();
            fullyClosedNotified = true;
        }
    }

    private CompletableFuture<Void> startDelegateClose() {
        synchronized (lifecycleMutex) {
            if (delegateCloseFuture != null) {
                return delegateCloseFuture;
            }
            CompletableFuture<Void> result = new CompletableFuture<>();
            delegateCloseFuture = result;
            try {
                delegateCloseExecutor.execute(() -> {
                    try {
                        delegate.close();
                        result.complete(null);
                    } catch (Throwable failure) {
                        result.completeExceptionally(failure);
                    }
                });
            } catch (Throwable failure) {
                result.completeExceptionally(failure);
            }
            return result;
        }
    }

    /** Starts a supervised close that retries until the delegate and catalog handle are released. */
    CompletableFuture<Void> closeEventually(Executor executor) {
        Objects.requireNonNull(executor, "executor");
        final CompletableFuture<Void> result;
        synchronized (lifecycleMutex) {
            if (eventualCloseFuture != null) {
                return eventualCloseFuture;
            }
            result = new CompletableFuture<>();
            eventualCloseFuture = OwnedResultFutures.nonCancellableCompletion(result);
        }
        attemptEventualClose(executor, result, 0);
        return eventualCloseFuture;
    }

    private void attemptEventualClose(
            Executor executor, CompletableFuture<Void> result, int retryAttempt) {
        try {
            executor.execute(() -> {
                try {
                    close();
                    result.complete(null);
                } catch (Throwable failure) {
                    scheduleEventualCloseRetry(executor, result, retryAttempt, failure);
                }
            });
        } catch (Throwable failure) {
            scheduleEventualCloseRetry(executor, result, retryAttempt, failure);
        }
    }

    private void scheduleEventualCloseRetry(
            Executor executor,
            CompletableFuture<Void> result,
            int retryAttempt,
            Throwable failure) {
        int nextAttempt = retryAttempt == Integer.MAX_VALUE
            ? Integer.MAX_VALUE : retryAttempt + 1;
        long delayMillis = eventualCloseRetryDelayMillis(retryAttempt);
        log.warn("Failed to close abandoned stream reader; retrying in {} ms (attempt {})",
            delayMillis, nextAttempt, failure);
        try {
            CompletableFuture.delayedExecutor(delayMillis, TimeUnit.MILLISECONDS)
                .execute(() -> attemptEventualClose(executor, result, nextAttempt));
        } catch (Throwable schedulingFailure) {
            failure.addSuppressed(schedulingFailure);
            result.completeExceptionally(failure);
        }
    }

    private static long eventualCloseRetryDelayMillis(int retryAttempt) {
        int shift = Math.min(retryAttempt, 6);
        return Math.min(
            EVENTUAL_CLOSE_INITIAL_RETRY_MILLIS << shift,
            EVENTUAL_CLOSE_MAX_RETRY_MILLIS);
    }

    private void awaitCompletion(
            CompletableFuture<Void> completion,
            long deadlineNanos,
            String description) throws Exception {
        long remainingNanos = deadlineNanos - System.nanoTime();
        if (remainingNanos <= 0) {
            throw new IOException("Timed out after " + closeTimeoutMillis
                + " ms waiting for " + description);
        }
        try {
            completion.get(remainingNanos, TimeUnit.NANOSECONDS);
        } catch (TimeoutException failure) {
            throw new IOException("Timed out after " + closeTimeoutMillis
                + " ms waiting for " + description, failure);
        } catch (InterruptedException failure) {
            Thread.currentThread().interrupt();
            throw new IOException(
                "Interrupted while waiting for " + description,
                failure);
        }
    }

    private static void throwFailure(Throwable cause) throws Exception {
        if (cause instanceof Error error) {
            throw error;
        }
        if (cause instanceof Exception exception) {
            throw exception;
        }
        throw new RuntimeException(cause);
    }

    private static Throwable unwrap(Throwable failure) {
        Throwable current = failure;
        while ((current instanceof CompletionException || current instanceof ExecutionException)
                && current.getCause() != null) {
            current = current.getCause();
        }
        return current;
    }
}
