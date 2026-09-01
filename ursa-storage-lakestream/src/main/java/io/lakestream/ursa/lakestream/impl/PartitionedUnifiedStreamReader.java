/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.lakestream.impl;

import io.lakestream.api.Log;
import io.lakestream.api.LogEntry;
import io.lakestream.api.LogId;
import io.lakestream.ursa.storage.OwnedResultFutures;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

/**
 * Routes reads to lazily created, independently owned logs for each stream partition.
 *
 * <p>Each child log owns its compacted-object reader. The router itself owns the child logs and
 * closes every opened child exactly once.
 */
final class PartitionedUnifiedStreamReader implements UnifiedStreamReader {

    private final Function<LogId, Log> logFactory;
    private final Map<LogId, Log> logs = new HashMap<>();
    private boolean closed;

    PartitionedUnifiedStreamReader(Function<LogId, Log> logFactory) {
        this.logFactory = logFactory;
    }

    @Override
    public CompletableFuture<ReadResult> readEntries(
            LogId logId, long startOffset, int maxMessageCount, long maxSizeBytes) {
        final CompletableFuture<List<LogEntry>> childRead;
        try {
            childRead = getOrCreateLog(logId)
                .readEntries(startOffset, maxMessageCount, maxSizeBytes);
        } catch (RuntimeException | Error openFailure) {
            return CompletableFuture.failedFuture(openFailure);
        }
        CompletableFuture<ReadResult> read = childRead.thenApply(entries -> {
            try {
                long nextOffset = startOffset;
                for (LogEntry entry : entries) {
                    nextOffset = entry.offset() + entry.numberOfRecords();
                }
                return new ReadResult(entries, nextOffset);
            } catch (RuntimeException | Error metadataFailure) {
                closeEntriesAfterFailure(entries, metadataFailure);
                throw metadataFailure;
            }
        });
        return OwnedResultFutures.transfer(read, result -> {
            if (result != null) {
                OwnedResultFutures.closeLogEntries(result.entries());
            }
        });
    }

    private synchronized Log getOrCreateLog(LogId logId) {
        if (closed) {
            throw new IllegalStateException("Stream reader is closed");
        }
        Log existing = logs.get(logId);
        if (existing != null) {
            return existing;
        }
        Log created = logFactory.apply(logId);
        if (created == null) {
            throw new IllegalStateException("Log factory returned null for " + logId);
        }
        logs.put(logId, created);
        return created;
    }

    @Override
    public synchronized void close() throws Exception {
        closed = true;
        List<Map.Entry<LogId, Log>> openedLogs = new ArrayList<>(logs.entrySet());
        Throwable firstFailure = null;
        for (Map.Entry<LogId, Log> opened : openedLogs) {
            try {
                opened.getValue().close();
                logs.remove(opened.getKey(), opened.getValue());
            } catch (Exception | Error closeFailure) {
                if (firstFailure == null) {
                    firstFailure = closeFailure;
                } else {
                    firstFailure.addSuppressed(closeFailure);
                }
            }
        }
        if (firstFailure instanceof Exception exception) {
            throw exception;
        }
        if (firstFailure instanceof Error error) {
            throw error;
        }
    }

    private static void closeEntriesAfterFailure(List<LogEntry> entries, Throwable failure) {
        try {
            OwnedResultFutures.closeLogEntries(entries);
        } catch (RuntimeException | Error cleanupFailure) {
            failure.addSuppressed(cleanupFailure);
        }
    }
}
