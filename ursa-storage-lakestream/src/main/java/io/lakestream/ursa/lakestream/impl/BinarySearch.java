/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.lakestream.impl;

import io.lakestream.api.EntryHeader;
import io.lakestream.api.EntryIndex;
import io.lakestream.api.LogEntry;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Predicate;

/**
 * Binary search utilities over log entries and entry headers.
 *
 * <p>Uses async chaining via {@code thenComposeAsync} to avoid gRPC event
 * loop deadlocks when Oxia-backed index/read calls are nested.
 */
public class BinarySearch {

    /**
     * Binary search for the newest offset where the header predicate holds true.
     */
    public static CompletableFuture<Long> binarySearch(
            BiFunction<Long, Long, CompletableFuture<EntryIndex>> indexSupplier,
            long streamId, long min, long max,
            Predicate<EntryHeader> predicate) {
        if (min >= max) {
            return indexSupplier.apply(streamId, min)
                    .thenApply(index -> predicate.test(index.header()) ? min : -1L);
        }
        long mid = min + (max - min + 1) / 2;
        return indexSupplier.apply(streamId, mid)
                .thenComposeAsync(index -> {
                    if (predicate.test(index.header())) {
                        return binarySearch(indexSupplier, streamId, mid, max, predicate);
                    } else {
                        return binarySearch(indexSupplier, streamId, min, mid - 1, predicate);
                    }
                });
    }

    /**
     * Binary search for the newest offset where the payload predicate holds true.
     *
     * <p>Returns the {@link EntryHeader} of the found offset. For batched entries
     * (where a single WAL entry contains multiple messages), the returned header's
     * offset reflects the searched offset, not the batch base offset.
     */
    public static CompletableFuture<EntryHeader> binarySearchWithPayload(
            Function<Long, CompletableFuture<LogEntry>> readEntry,
            BiFunction<Long, Long, CompletableFuture<EntryIndex>> indexSupplier,
            long streamId,
            long min, long max,
            Predicate<LogEntry> predicate) {
        if (min >= max) {
            return readEntry.apply(min)
                    .thenApplyAsync(e -> {
                        try (e) {
                            if (predicate.test(e)) {
                                return new EntryHeader(e.offset(), e.numberOfRecords(),
                                        e.timestamp(), e.size(), 0);
                            }
                            return null;
                        }
                    });
        }

        long mid = min + (max - min + 1) / 2;
        return indexSupplier.apply(streamId, mid)
                .thenComposeAsync(index -> readEntry.apply(mid)
                        .thenComposeAsync(midEntry -> {
                            boolean matches;
                            try (midEntry) {
                                matches = predicate.test(midEntry);
                            }
                            if (matches) {
                                return binarySearchWithPayload(readEntry, indexSupplier, streamId,
                                        mid, max, predicate);
                            } else {
                                return binarySearchWithPayload(readEntry, indexSupplier, streamId,
                                        min, mid - 1, predicate);
                            }
                        }));
    }
}
