/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.storage;

import io.lakestream.api.LogEntry;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

/**
 * Bridges asynchronous results whose ownership transfers to a successful caller.
 *
 * <p>Canceling the exposed future does not cancel the source operation. The source is allowed to
 * finish so an otherwise abandoned owned result can be released deterministically.
 */
public final class OwnedResultFutures {

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
