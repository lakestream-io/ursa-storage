/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.lakestream.impl;

import io.lakestream.api.EntryIndex;
import io.lakestream.api.Log;
import io.lakestream.api.LogCursor;
import io.lakestream.api.LogEntry;
import io.lakestream.api.LogEntryHeader;
import io.lakestream.api.LogId;
import io.lakestream.api.LogOffset;
import io.lakestream.api.LogStorage;
import io.lakestream.api.Position;
import io.lakestream.ursa.storage.OwnedResultFutures;
import io.lakestream.ursa.storage.StorageApi.StreamWriteLease;
import io.netty.buffer.ByteBuf;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.function.Supplier;
import lombok.extern.slf4j.Slf4j;

/**
 * A catalog-opened log whose lifetime owns a durable-storage write lease.
 *
 * <p>Closing first rejects new operations, waits for operations already handed to the delegate and
 * child cursors to finish, closes the delegate, and only then releases the durable lease. This
 * ordering prevents a controller-side purge from racing work accepted by an already-open handle.
 */
@Slf4j
final class LeasedLog implements Log {

    private static final long DEFAULT_CLOSE_TIMEOUT_MILLIS = TimeUnit.SECONDS.toMillis(10);
    private static final long EVENTUAL_CLOSE_INITIAL_RETRY_MILLIS = 100L;
    private static final long EVENTUAL_CLOSE_MAX_RETRY_MILLIS = TimeUnit.SECONDS.toMillis(10);

    private final Log delegate;
    private final LogId logId;
    private final StreamWriteLease lease;
    private final LogStorage scopedLogStorage = new ScopedLogStorage();
    private final Object lifecycleMutex = new Object();
    private final long closeTimeoutMillis;
    private final ClassLoader contextClassLoader;
    private final Executor delegateCloseExecutor;
    private final Runnable onFullyClosed;

    private boolean closing;
    private int activeOperations;
    private int activeCursors;
    private CompletableFuture<Void> operationsDrained;
    private CompletableFuture<Void> delegateCloseFuture;
    private CompletableFuture<Void> leaseCloseFuture;
    private boolean delegateClosed;
    private boolean leaseReleased;
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

    LeasedLog(
            Log delegate,
            StreamWriteLease lease,
            Executor delegateCloseExecutor,
            Runnable onFullyClosed) {
        this(delegate, lease, DEFAULT_CLOSE_TIMEOUT_MILLIS, true,
            delegateCloseExecutor, onFullyClosed);
    }

    LeasedLog(
            Log delegate,
            StreamWriteLease lease,
            long closeTimeoutMillis,
            Executor delegateCloseExecutor) {
        this(delegate, lease, closeTimeoutMillis, true, delegateCloseExecutor, () -> { });
    }

    private LeasedLog(
            Log delegate,
            StreamWriteLease lease,
            long closeTimeoutMillis,
            boolean validateLeaseOwner,
            Executor delegateCloseExecutor,
            Runnable onFullyClosed) {
        this(delegate, Objects.requireNonNull(delegate, "delegate").id(), lease,
            closeTimeoutMillis, validateLeaseOwner, delegateCloseExecutor, onFullyClosed);
    }

    private LeasedLog(
            Log delegate,
            LogId logId,
            StreamWriteLease lease,
            long closeTimeoutMillis,
            boolean validateLeaseOwner,
            Executor delegateCloseExecutor,
            Runnable onFullyClosed) {
        this.delegate = Objects.requireNonNull(delegate, "delegate");
        this.logId = Objects.requireNonNull(logId, "logId");
        this.lease = Objects.requireNonNull(lease, "lease");
        if (closeTimeoutMillis <= 0) {
            throw new IllegalArgumentException("closeTimeoutMillis must be positive");
        }
        this.closeTimeoutMillis = closeTimeoutMillis;
        this.contextClassLoader = Thread.currentThread().getContextClassLoader();
        this.delegateCloseExecutor = Objects.requireNonNull(
            delegateCloseExecutor, "delegateCloseExecutor");
        this.onFullyClosed = Objects.requireNonNull(onFullyClosed, "onFullyClosed");
        if (validateLeaseOwner && logId.id() != lease.streamId()) {
            throw new IllegalArgumentException(
                "Lease for stream " + lease.streamId() + " cannot protect log " + logId);
        }
    }

    /**
     * Creates a guard used only to clean up a failed open whose delegate could not be validated.
     * The guard is never exposed to callers, so it may safely close the raw delegate before
     * releasing the acquired lease even when the delegate reports an unexpected log ID.
     */
    static LeasedLog forFailedOpen(
            Log delegate, StreamWriteLease lease, Executor delegateCloseExecutor) {
        Objects.requireNonNull(lease, "lease");
        return new LeasedLog(
            delegate, LogId.of(lease.streamId()), lease,
            DEFAULT_CLOSE_TIMEOUT_MILLIS, false,
            delegateCloseExecutor, () -> { });
    }

    /** Starts a supervised release for a lease acquired before a raw log could be created. */
    static CompletableFuture<Void> releaseLeaseEventually(
            StreamWriteLease lease, Executor executor) {
        Objects.requireNonNull(lease, "lease");
        Objects.requireNonNull(executor, "executor");
        CompletableFuture<Void> result = new CompletableFuture<>();
        ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
        attemptEventualLeaseRelease(lease, contextClassLoader, executor, result, 0);
        return OwnedResultFutures.nonCancellableCompletion(result);
    }

    private static void attemptEventualLeaseRelease(
            StreamWriteLease lease,
            ClassLoader contextClassLoader,
            Executor executor,
            CompletableFuture<Void> result,
            int retryAttempt) {
        try {
            executor.execute(() -> {
            ClassLoader previousClassLoader = Thread.currentThread().getContextClassLoader();
            Thread.currentThread().setContextClassLoader(contextClassLoader);
            final CompletableFuture<Void> release;
            try {
                release = Objects.requireNonNull(lease.closeAsync(), "lease close future");
            } catch (Throwable failure) {
                scheduleEventualLeaseReleaseRetry(
                    lease, contextClassLoader, executor, result, retryAttempt, failure);
                Thread.currentThread().setContextClassLoader(previousClassLoader);
                return;
            }
            Thread.currentThread().setContextClassLoader(previousClassLoader);
            release.whenComplete((ignored, failure) -> {
                if (failure == null) {
                    result.complete(null);
                } else {
                    scheduleEventualLeaseReleaseRetry(
                        lease, contextClassLoader, executor, result, retryAttempt,
                        unwrapCompletionFailure(failure));
                }
            });
            });
        } catch (Throwable failure) {
            scheduleEventualLeaseReleaseRetry(
                lease, contextClassLoader, executor, result, retryAttempt, failure);
        }
    }

    private static void scheduleEventualLeaseReleaseRetry(
            StreamWriteLease lease,
            ClassLoader contextClassLoader,
            Executor executor,
            CompletableFuture<Void> result,
            int retryAttempt,
            Throwable failure) {
        int nextAttempt = retryAttempt == Integer.MAX_VALUE
            ? Integer.MAX_VALUE : retryAttempt + 1;
        long retryDelayMillis = eventualCloseRetryDelayMillis(retryAttempt);
        log.warn("Failed to release write lease for log {}; retrying in {} ms (attempt {})",
            lease.streamId(), retryDelayMillis, nextAttempt, failure);
        try {
            CompletableFuture.delayedExecutor(retryDelayMillis, TimeUnit.MILLISECONDS)
                .execute(() -> attemptEventualLeaseRelease(
                    lease, contextClassLoader, executor, result, nextAttempt));
        } catch (Throwable schedulingFailure) {
            failure.addSuppressed(schedulingFailure);
            result.completeExceptionally(failure);
        }
    }

    @Override
    public LogId id() {
        return logId;
    }

    @Override
    public CompletableFuture<LogEntryHeader> append(int numberOfRecords, ByteBuf data) {
        return trackNonCancellableOperation(
            () -> delegate.append(numberOfRecords, data));
    }

    private <T> CompletableFuture<T> trackNonCancellableOperation(
            Supplier<CompletableFuture<T>> operation) {
        return OwnedResultFutures.nonCancellableCompletion(
            trackOperation(operation, ignored -> { }));
    }

    private CompletableFuture<List<LogEntry>> trackLogEntriesOperation(
            Supplier<CompletableFuture<List<LogEntry>>> operation) {
        return trackOperation(operation, OwnedResultFutures::closeLogEntries);
    }

    private CompletableFuture<LogEntry> trackLogEntryOperation(
            Supplier<CompletableFuture<LogEntry>> operation) {
        return trackOperation(operation, entry -> {
            if (entry != null) {
                entry.close();
            }
        });
    }

    private <T> CompletableFuture<T> trackOperation(
            Supplier<CompletableFuture<T>> operation,
            Consumer<? super T> abandonedResultCleanup) {
        synchronized (lifecycleMutex) {
            if (closing) {
                return CompletableFuture.failedFuture(
                    new IllegalStateException("Log " + id() + " is closing or closed"));
            }
            activeOperations++;
        }

        final CompletableFuture<T> source;
        try {
            source = Objects.requireNonNull(operation.get(), "delegate operation future");
        } catch (RuntimeException | Error failure) {
            operationFinished();
            throw failure;
        }
        TrackedOperationFuture<T> exposed = new TrackedOperationFuture<>();
        source.whenComplete((value, failure) -> {
            if (failure != null) {
                exposed.deliverFailure(failure, source.isCancelled());
            } else if (!exposed.deliver(value)) {
                try {
                    abandonedResultCleanup.accept(value);
                } finally {
                    operationFinished();
                }
            }
        });
        return exposed;
    }

    private <T> T trackSynchronousOperation(Supplier<T> operation) {
        synchronized (lifecycleMutex) {
            if (closing) {
                throw new IllegalStateException("Log " + id() + " is closing or closed");
            }
            activeOperations++;
        }
        try {
            return operation.get();
        } finally {
            operationFinished();
        }
    }

    private void runSynchronousOperation(Runnable operation) {
        trackSynchronousOperation(() -> {
            operation.run();
            return null;
        });
    }

    private void operationFinished() {
        CompletableFuture<Void> drained = null;
        synchronized (lifecycleMutex) {
            activeOperations--;
            if (lifecycleDrained() && operationsDrained != null) {
                drained = operationsDrained;
                operationsDrained = null;
            }
        }
        if (drained != null) {
            drained.complete(null);
        }
    }

    private void cursorOpened() {
        synchronized (lifecycleMutex) {
            activeCursors++;
        }
    }

    private void cursorClosed() {
        CompletableFuture<Void> drained = null;
        synchronized (lifecycleMutex) {
            activeCursors--;
            if (lifecycleDrained() && operationsDrained != null) {
                drained = operationsDrained;
                operationsDrained = null;
            }
        }
        if (drained != null) {
            drained.complete(null);
        }
    }

    private boolean lifecycleDrained() {
        return activeOperations == 0 && activeCursors == 0;
    }

    @Override
    public CompletableFuture<List<LogEntry>> readEntries(
            long startOffset, int maxMessageCount, long maxSizeBytes) {
        return trackLogEntriesOperation(
            () -> delegate.readEntries(startOffset, maxMessageCount, maxSizeBytes));
    }

    @Override
    public CompletableFuture<List<LogEntry>> readEntries(
            long startOffset, int maxMessageCount, long maxSizeBytes, boolean includeTrimmed) {
        return trackLogEntriesOperation(() -> delegate.readEntries(
            startOffset, maxMessageCount, maxSizeBytes, includeTrimmed));
    }

    @Override
    public CompletableFuture<LogEntry> readEntry(long offset) {
        return trackLogEntryOperation(() -> delegate.readEntry(offset));
    }

    @Override
    public CompletableFuture<LogEntryHeader> getEntryMetadata(long offset) {
        return trackOperation(() -> delegate.getEntryMetadata(offset), ignored -> { });
    }

    @Override
    public CompletableFuture<EntryIndex> getEntryIndex(long offset) {
        return trackOperation(() -> delegate.getEntryIndex(offset), ignored -> { });
    }

    @Override
    public CompletableFuture<List<EntryIndex>> readIndexRange(long startOffset, long endOffset) {
        return trackOperation(
            () -> delegate.readIndexRange(startOffset, endOffset), ignored -> { });
    }

    @Override
    public CompletableFuture<List<LogEntryHeader>> getEntryMetadataRange(
            long startOffset, long endOffset) {
        return trackOperation(
            () -> delegate.getEntryMetadataRange(startOffset, endOffset), ignored -> { });
    }

    @Override
    public CompletableFuture<LogOffset> getFirstOffset() {
        return trackOperation(delegate::getFirstOffset, ignored -> { });
    }

    @Override
    public CompletableFuture<LogOffset> getFirstOffset(boolean includeTrimmed) {
        return trackOperation(
            () -> delegate.getFirstOffset(includeTrimmed), ignored -> { });
    }

    @Override
    public CompletableFuture<LogOffset> getLastOffset() {
        return trackOperation(delegate::getLastOffset, ignored -> { });
    }

    @Override
    public CompletableFuture<Long> softTrim(long offsetIncluded) {
        return trackOperation(() -> delegate.softTrim(offsetIncluded), ignored -> { });
    }

    @Override
    public LogStorage logStorage() {
        return scopedLogStorage;
    }

    @Override
    public void cacheIndex(EntryIndex index) {
        runSynchronousOperation(() -> delegate.cacheIndex(index));
    }

    @Override
    public void invalidateCache() {
        runSynchronousOperation(delegate::invalidateCache);
    }

    @Override
    public void invalidateCache(long offset) {
        runSynchronousOperation(() -> delegate.invalidateCache(offset));
    }

    @Override
    public long getMessageCount(long startOffset, long endOffset) {
        return trackSynchronousOperation(
            () -> delegate.getMessageCount(startOffset, endOffset));
    }

    @Override
    public void fence() {
        runSynchronousOperation(delegate::fence);
    }

    @Override
    public CompletableFuture<LogCursor> openCursor(String name, long initialOffset) {
        return trackNonCancellableOperation(() -> delegate.openCursor(name, initialOffset)
            .thenApply(this::wrapCursor));
    }

    @Override
    public CompletableFuture<LogCursor> openEphemeralCursor(String name, long initialOffset) {
        return trackNonCancellableOperation(() -> delegate.openEphemeralCursor(name, initialOffset)
            .thenApply(this::wrapCursor));
    }

    @Override
    public CompletableFuture<LogCursor> loadCursor(String name) {
        return trackNonCancellableOperation(() -> delegate.loadCursor(name)
            .thenApply(this::wrapCursor));
    }

    @Override
    public CompletableFuture<List<LogCursor>> loadAllCursors() {
        return trackNonCancellableOperation(() -> delegate.loadAllCursors()
            .thenApply(cursors -> {
                List<LogCursor> delegateCursors = Objects.requireNonNull(
                    cursors, "delegate cursors");
                delegateCursors.forEach(cursor ->
                    Objects.requireNonNull(cursor, "delegate cursor"));
                return delegateCursors.stream()
                    .map(this::wrapCursor)
                    .map(LogCursor.class::cast)
                    .toList();
            }));
    }

    @Override
    public CompletableFuture<Void> deleteCursor(String name) {
        return trackOperation(() -> delegate.deleteCursor(name), ignored -> { });
    }

    @Override
    public CompletableFuture<Long> computeRetentionTrimOffset(
            long maxOffset, long retentionMillis, long retentionSizeBytes) {
        return trackOperation(() -> delegate.computeRetentionTrimOffset(
            maxOffset, retentionMillis, retentionSizeBytes), ignored -> { });
    }

    @Override
    public CompletableFuture<Long> binarySearchOffset(
            long min, long max, Predicate<LogEntryHeader> predicate) {
        return trackOperation(
            () -> delegate.binarySearchOffset(min, max, predicate), ignored -> { });
    }

    @Override
    public synchronized void close() throws Exception {
        long deadlineNanos = System.nanoTime()
            + TimeUnit.MILLISECONDS.toNanos(closeTimeoutMillis);
        CompletableFuture<Void> drain;
        synchronized (lifecycleMutex) {
            if (!closing) {
                closing = true;
                if (lifecycleDrained()) {
                    drain = CompletableFuture.completedFuture(null);
                } else {
                    operationsDrained = new CompletableFuture<>();
                    drain = operationsDrained;
                }
            } else {
                drain = operationsDrained == null
                    ? CompletableFuture.completedFuture(null) : operationsDrained;
            }
        }

        awaitCloseStage(
            drain, deadlineNanos, "accepted log operations and cursors to drain");
        if (!delegateClosed) {
            if (delegateCloseFuture == null) {
                delegateCloseFuture = startDelegateClose();
            }
            try {
                awaitCloseStage(delegateCloseFuture, deadlineNanos, "delegate log to close");
                delegateClosed = true;
            } catch (Exception | Error failure) {
                if (delegateCloseFuture.isDone()) {
                    delegateCloseFuture = null;
                }
                throw failure;
            }
        }

        if (!leaseReleased) {
            if (leaseCloseFuture == null) {
                try {
                    leaseCloseFuture = Objects.requireNonNull(
                        lease.closeAsync(), "lease close future");
                } catch (RuntimeException | Error failure) {
                    throwCloseFailure(failure);
                }
            }
            try {
                awaitCloseStage(leaseCloseFuture, deadlineNanos, "write lease to release");
                leaseReleased = true;
            } catch (Exception | Error failure) {
                if (leaseCloseFuture.isDone()) {
                    leaseCloseFuture = null;
                }
                throw failure;
            }
        }
        if (!fullyClosedNotified) {
            onFullyClosed.run();
            fullyClosedNotified = true;
        }
    }

    @Override
    public CompletableFuture<Void> closeAsync() {
        return closeEventually(delegateCloseExecutor);
    }

    /**
     * Starts a supervised, non-cancellable close that retries until both the delegate and lease are
     * closed in the required order. The retry chain retains this handle even if its caller drops the
     * returned future.
     */
    synchronized CompletableFuture<Void> closeEventually(Executor executor) {
        Objects.requireNonNull(executor, "executor");
        if (eventualCloseFuture != null) {
            return eventualCloseFuture;
        }
        eventualCloseFuture = new CompletableFuture<>();
        attemptEventualClose(executor, eventualCloseFuture, 0);
        return eventualCloseFuture;
    }

    private void attemptEventualClose(
            Executor executor, CompletableFuture<Void> result, int retryAttempt) {
        try {
            executor.execute(() -> {
            ClassLoader previousClassLoader = Thread.currentThread().getContextClassLoader();
            Thread.currentThread().setContextClassLoader(contextClassLoader);
            try {
                close();
                result.complete(null);
            } catch (Throwable failure) {
                scheduleEventualCloseRetry(executor, result, retryAttempt, failure);
            } finally {
                Thread.currentThread().setContextClassLoader(previousClassLoader);
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
        long retryDelayMillis = eventualCloseRetryDelayMillis(retryAttempt);
        log.warn("Failed to close log {} and release its write lease; retrying in {} ms "
                + "(attempt {})", lease.streamId(), retryDelayMillis, nextAttempt, failure);
        try {
            CompletableFuture.delayedExecutor(retryDelayMillis, TimeUnit.MILLISECONDS)
                .execute(() -> attemptEventualClose(executor, result, nextAttempt));
        } catch (Throwable schedulingFailure) {
            failure.addSuppressed(schedulingFailure);
            result.completeExceptionally(failure);
        }
    }

    private static long eventualCloseRetryDelayMillis(int retryAttempt) {
        int shift = Math.min(Math.max(retryAttempt, 0), 30);
        long exponentialDelay = EVENTUAL_CLOSE_INITIAL_RETRY_MILLIS << shift;
        return Math.min(exponentialDelay, EVENTUAL_CLOSE_MAX_RETRY_MILLIS);
    }

    private CompletableFuture<Void> startDelegateClose() {
        CompletableFuture<Void> result = new CompletableFuture<>();
        try {
            delegateCloseExecutor.execute(() -> {
                ClassLoader previousClassLoader = Thread.currentThread().getContextClassLoader();
                Thread.currentThread().setContextClassLoader(contextClassLoader);
                try {
                    delegate.close();
                    result.complete(null);
                } catch (Throwable failure) {
                    result.completeExceptionally(failure);
                } finally {
                    Thread.currentThread().setContextClassLoader(previousClassLoader);
                }
            });
        } catch (RuntimeException | Error failure) {
            result.completeExceptionally(failure);
        }
        return result;
    }

    private void awaitCloseStage(
            CompletableFuture<Void> stage,
            long deadlineNanos,
            String description) throws Exception {
        long remainingNanos = deadlineNanos - System.nanoTime();
        if (remainingNanos <= 0) {
            throw closeTimeout(description, null);
        }
        try {
            stage.get(remainingNanos, TimeUnit.NANOSECONDS);
        } catch (TimeoutException failure) {
            throw closeTimeout(description, failure);
        } catch (InterruptedException failure) {
            Thread.currentThread().interrupt();
            throw new IOException("Interrupted while waiting for " + description, failure);
        } catch (ExecutionException failure) {
            throwCloseFailure(unwrapCompletionFailure(failure));
        }
    }

    private IOException closeTimeout(String description, Throwable cause) {
        String message = "Timed out after " + closeTimeoutMillis + " ms waiting for " + description;
        return cause == null ? new IOException(message) : new IOException(message, cause);
    }

    private static Throwable unwrapCompletionFailure(Throwable failure) {
        Throwable current = failure;
        while ((current instanceof CompletionException || current instanceof ExecutionException)
                && current.getCause() != null) {
            current = current.getCause();
        }
        return current;
    }

    private ScopedLogCursor wrapCursor(LogCursor cursor) {
        return new ScopedLogCursor(Objects.requireNonNull(cursor, "delegate cursor"));
    }

    private static void throwCloseFailure(Throwable failure) throws Exception {
        if (failure instanceof Error error) {
            throw error;
        }
        if (failure instanceof Exception exception) {
            throw exception;
        }
        throw new RuntimeException(failure);
    }

    /** Prevents a cursor from exposing the unfenced delegate through {@link LogCursor#log()}. */
    private final class ScopedLogCursor implements LogCursor {
        private final LogCursor cursor;
        private boolean cursorClosed;

        private ScopedLogCursor(LogCursor cursor) {
            this.cursor = cursor;
            cursorOpened();
        }

        @Override
        public String name() {
            return trackSynchronousOperation(cursor::name);
        }

        @Override
        public Log log() {
            return LeasedLog.this;
        }

        @Override
        public long readOffset() {
            return trackSynchronousOperation(cursor::readOffset);
        }

        @Override
        public long markDeleteOffset() {
            return trackSynchronousOperation(cursor::markDeleteOffset);
        }

        @Override
        public CompletableFuture<List<LogEntry>> readEntries(
                int maxEntries, long maxSizeBytes) {
            return trackLogEntriesOperation(
                () -> cursor.readEntries(maxEntries, maxSizeBytes));
        }

        @Override
        public CompletableFuture<List<LogEntry>> readEntries(
                int maxEntries, long maxSizeBytes,
                Predicate<Long> skipCondition, long maxOffset) {
            return trackLogEntriesOperation(() -> cursor.readEntries(
                maxEntries, maxSizeBytes, skipCondition, maxOffset));
        }

        @Override
        public CompletableFuture<LogEntry> readEntry(long offset) {
            return trackLogEntryOperation(() -> cursor.readEntry(offset));
        }

        @Override
        public CompletableFuture<Void> markDelete(
                long offset, Map<String, Long> properties) {
            return trackOperation(
                () -> cursor.markDelete(offset, properties), ignored -> { });
        }

        @Override
        public CompletableFuture<Void> seek(long offset) {
            return trackOperation(() -> cursor.seek(offset), ignored -> { });
        }

        @Override
        public CompletableFuture<LogEntryHeader> getEntryMetadata(long offset) {
            return trackOperation(
                () -> cursor.getEntryMetadata(offset), ignored -> { });
        }

        @Override
        public CompletableFuture<Void> individualDelete(long offset, int numberOfRecords) {
            return trackOperation(
                () -> cursor.individualDelete(offset, numberOfRecords), ignored -> { });
        }

        @Override
        public boolean isOffsetIndividuallyDeleted(long offset) {
            return trackSynchronousOperation(
                () -> cursor.isOffsetIndividuallyDeleted(offset));
        }

        @Override
        public long individualDeleteCount() {
            return trackSynchronousOperation(cursor::individualDeleteCount);
        }

        @Override
        public long firstNonDeletedOffset() {
            return trackSynchronousOperation(cursor::firstNonDeletedOffset);
        }

        @Override
        public CompletableFuture<Void> persistState() {
            return trackOperation(cursor::persistState, ignored -> { });
        }

        @Override
        public long persistedMarkDeleteOffset() {
            return trackSynchronousOperation(cursor::persistedMarkDeleteOffset);
        }

        @Override
        public Map<String, Long> properties() {
            return trackSynchronousOperation(cursor::properties);
        }

        @Override
        public boolean hasMoreEntries() {
            return trackSynchronousOperation(cursor::hasMoreEntries);
        }

        @Override
        public long getNumberOfEntriesInBacklog() {
            return trackSynchronousOperation(cursor::getNumberOfEntriesInBacklog);
        }

        @Override
        public CompletableFuture<Void> deleteCursor() {
            return trackOperation(cursor::deleteCursor, ignored -> { });
        }

        @Override
        public synchronized void close() throws Exception {
            if (cursorClosed) {
                return;
            }
            cursor.close();
            cursorClosed = true;
            LeasedLog.this.cursorClosed();
        }
    }

    /** Prevents callers of the advanced storage view from bypassing this handle's lease. */
    private final class ScopedLogStorage implements LogStorage {

        @Override
        public CompletableFuture<LogEntryHeader> append(
                LogId logId, int numberOfRecords, ByteBuf data) {
            requireOwnLog(logId);
            return LeasedLog.this.append(numberOfRecords, data);
        }

        @Override
        public CompletableFuture<List<LogEntry>> readEntries(
                LogId logId, long startOffset, int maxMessageCount, long maxSizeBytes) {
            requireOwnLog(logId);
            return trackLogEntriesOperation(() -> delegate.logStorage().readEntries(
                logId, startOffset, maxMessageCount, maxSizeBytes));
        }

        @Override
        public CompletableFuture<List<LogEntry>> readEntriesByIndex(
                LogId logId, List<EntryIndex> indices, long startOffset, long maxOffset,
                int maxMessageCount, long maxSizeBytes, Predicate<Long> offsetDeleted,
                Predicate<Long> skipCondition) {
            requireOwnLog(logId);
            return trackLogEntriesOperation(() -> delegate.logStorage().readEntriesByIndex(
                logId, indices, startOffset, maxOffset, maxMessageCount, maxSizeBytes,
                offsetDeleted, skipCondition));
        }

        @Override
        public CompletableFuture<LogOffset> getFirstOffset(LogId logId) {
            requireOwnLog(logId);
            return trackOperation(
                () -> delegate.logStorage().getFirstOffset(logId), ignored -> { });
        }

        @Override
        public CompletableFuture<LogOffset> getFirstOffset(
                LogId logId, boolean includeTrimmed) {
            requireOwnLog(logId);
            return trackOperation(() -> delegate.logStorage().getFirstOffset(
                logId, includeTrimmed), ignored -> { });
        }

        @Override
        public CompletableFuture<LogOffset> getLastOffset(LogId logId) {
            requireOwnLog(logId);
            return trackOperation(
                () -> delegate.logStorage().getLastOffset(logId), ignored -> { });
        }

        @Override
        public CompletableFuture<Long> softTrim(LogId logId, long offsetIncluded) {
            requireOwnLog(logId);
            return LeasedLog.this.softTrim(offsetIncluded);
        }

        @Override
        public CompletableFuture<List<EntryIndex>> readIndexRange(
                LogId logId, long startOffset, long endOffset) {
            requireOwnLog(logId);
            return trackOperation(() -> delegate.logStorage().readIndexRange(
                logId, startOffset, endOffset), ignored -> { });
        }

        @Override
        public CompletableFuture<Void> hardTrim(LogId logId, long offsetExcluded) {
            requireOwnLog(logId);
            return trackOperation(
                () -> delegate.logStorage().hardTrim(logId, offsetExcluded),
                ignored -> { });
        }

        @Override
        public CompletableFuture<Void> deleteLog(LogId logId) {
            requireOwnLog(logId);
            return CompletableFuture.failedFuture(new UnsupportedOperationException(
                "Log deletion is owned by StreamCatalog.dropStream"));
        }

        @Override
        public void preFetchEntries(LogId logId, List<Position> positions) {
            requireOwnLog(logId);
            runSynchronousOperation(
                () -> delegate.logStorage().preFetchEntries(logId, positions));
        }

        @Override
        public void close() {
            // The delegate and its shared LogStorage have separate lifetimes.
        }

        private void requireOwnLog(LogId logId) {
            if (!id().equals(logId)) {
                throw new IllegalArgumentException(
                    "Log handle " + id() + " cannot access " + logId);
            }
        }
    }
}
