/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.storage;

import io.lakestream.api.LogEntry;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.function.Consumer;
import java.util.function.Supplier;

/**
 * Bridges asynchronous results whose ownership transfers to a successful caller.
 *
 * <p>Canceling the exposed future does not cancel the source operation. The source is allowed to
 * finish so an otherwise abandoned owned result can be released deterministically.
 */
public final class OwnedResultFutures {

    private static final class SourceCompletionFuture<T> extends CompletableFuture<T> {

        @Override
        public boolean complete(T value) {
            return false;
        }

        @Override
        public boolean completeExceptionally(Throwable failure) {
            return false;
        }

        @Override
        public boolean cancel(boolean mayInterruptIfRunning) {
            return false;
        }

        @Override
        public void obtrudeValue(T value) {
            throw new UnsupportedOperationException(
                "Completion is owned by the source operation");
        }

        @Override
        public void obtrudeException(Throwable failure) {
            throw new UnsupportedOperationException(
                "Completion is owned by the source operation");
        }

        @Override
        public CompletableFuture<T> completeAsync(Supplier<? extends T> supplier) {
            throw new UnsupportedOperationException(
                "Completion is owned by the source operation");
        }

        @Override
        public CompletableFuture<T> completeAsync(
                Supplier<? extends T> supplier, Executor executor) {
            throw new UnsupportedOperationException(
                "Completion is owned by the source operation");
        }

        private boolean completeFromSource(T value) {
            return super.complete(value);
        }

        private boolean completeExceptionallyFromSource(Throwable failure) {
            return super.completeExceptionally(failure);
        }

        private boolean cancelFromSource() {
            return super.cancel(false);
        }
    }

    private OwnedResultFutures() {
    }

    /**
     * Exposes an owned source result and releases it if cancellation wins the completion race.
     *
     * @param source internal future that must remain uncanceled until it can produce or fail
     * @param release cleanup for a result that cannot be delivered to the caller
     * @return a caller-facing future
     */
    public static <T> CompletableFuture<T> transfer(
            CompletableFuture<T> source, Consumer<? super T> release) {
        CompletableFuture<T> exposed = new CompletableFuture<>();
        source.whenComplete((result, error) -> {
            if (error != null) {
                if (source.isCancelled()) {
                    exposed.cancel(false);
                } else {
                    exposed.completeExceptionally(error);
                }
            } else if (!exposed.complete(result)) {
                release.accept(result);
            }
        });
        return exposed;
    }

    /**
     * Mirrors a source operation without allowing its observer to force completion early.
     *
     * <p>This is used when the source may still consume caller-owned input after it starts. The
     * returned future therefore rejects cancellation, explicit completion, obtrusion, and timeout
     * helpers that try to complete the same future before the source has settled. A cancellation
     * originating in the source is still propagated.
     *
     * @param source the operation whose actual completion defines the ownership boundary
     * @return a caller-facing future completed only by the source operation
     */
    public static <T> CompletableFuture<T> nonCancellableCompletion(
            CompletableFuture<T> source) {
        Objects.requireNonNull(source, "source");
        SourceCompletionFuture<T> exposed = new SourceCompletionFuture<>();
        source.whenComplete((result, error) -> {
            if (error == null) {
                exposed.completeFromSource(result);
            } else if (source.isCancelled()) {
                exposed.cancelFromSource();
            } else {
                exposed.completeExceptionallyFromSource(error);
            }
        });
        return exposed;
    }

    /**
     * Exposes a caller-owned log-entry batch and closes the batch if it cannot be delivered.
     */
    public static CompletableFuture<List<LogEntry>> transferLogEntries(
            CompletableFuture<List<LogEntry>> source) {
        return transfer(source, OwnedResultFutures::closeLogEntries);
    }

    /**
     * Closes every entry in a batch, continuing cleanup if an individual close fails.
     */
    public static void closeLogEntries(List<? extends LogEntry> entries) {
        if (entries == null) {
            return;
        }
        Throwable firstFailure = null;
        for (LogEntry entry : entries) {
            if (entry == null) {
                continue;
            }
            try {
                entry.close();
            } catch (RuntimeException | Error cleanupFailure) {
                if (firstFailure == null) {
                    firstFailure = cleanupFailure;
                } else {
                    firstFailure.addSuppressed(cleanupFailure);
                }
            }
        }
        if (firstFailure instanceof RuntimeException runtimeFailure) {
            throw runtimeFailure;
        }
        if (firstFailure instanceof Error errorFailure) {
            throw errorFailure;
        }
    }
}
