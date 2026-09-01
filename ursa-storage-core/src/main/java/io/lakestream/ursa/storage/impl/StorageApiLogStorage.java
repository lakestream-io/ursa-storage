/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.storage.impl;

import io.lakestream.api.EntryHeader;
import io.lakestream.api.EntryIndex;
import io.lakestream.api.LogEntry;
import io.lakestream.api.LogEntryHeader;
import io.lakestream.api.LogId;
import io.lakestream.api.LogOffset;
import io.lakestream.api.LogStorage;
import io.lakestream.api.Position;
import io.lakestream.ursa.storage.Entry;
import io.lakestream.ursa.storage.EntryList;
import io.lakestream.ursa.storage.OwnedResultFutures;
import io.lakestream.ursa.storage.StorageApi;
import io.netty.buffer.ByteBuf;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * Adapter that wraps {@link StorageApi} to implement the clean {@link LogStorage} interface.
 *
 * <p>Bridges between the new Lakestream API types and the existing StorageApi:
 * <ul>
 *   <li>{@link LogId#id()} → {@code long streamId}</li>
 *   <li>{@link LogEntry} ← {@link Entry} (ownership-transferring adapter)</li>
 *   <li>{@link LogOffset} ← extracted from {@link EntryIndex}/{@link EntryHeader}</li>
 *   <li>{@code numberOfRecords} → {@code numberOfMessages} (terminology bridge)</li>
 * </ul>
 *
 * <p>Thread safety: this adapter is as thread-safe as the underlying {@link StorageApi}.
 */
public class StorageApiLogStorage implements LogStorage {

    private final StorageApi storageApi;

    public StorageApiLogStorage(StorageApi storageApi) {
        this.storageApi = storageApi;
    }

    public StorageApi getStorageApi() {
        return storageApi;
    }

    @Override
    public CompletableFuture<LogEntryHeader> append(LogId logId, int numberOfRecords, ByteBuf data) {
        CompletableFuture<LogEntryHeader> append =
            storageApi.append(logId.id(), numberOfRecords, data)
                .thenApply(result -> result.header());
        return OwnedResultFutures.nonCancellableCompletion(append);
    }

    @Override
    public CompletableFuture<List<LogEntry>> readEntries(LogId logId, long startOffset,
                                                          int maxMessageCount, long maxSizeBytes) {
        // StorageApi.readEntries uses int maxSize; clamp to Integer.MAX_VALUE for safety
        int maxSize = (int) Math.min(maxSizeBytes, Integer.MAX_VALUE);
        CompletableFuture<List<LogEntry>> converted =
            storageApi.readEntries(logId.id(), startOffset, maxMessageCount, maxSize)
                .thenApply(Entry::toLogEntries);
        return OwnedResultFutures.transferLogEntries(converted);
    }

    @Override
    public CompletableFuture<List<LogEntry>> readEntriesByIndex(LogId logId, List<EntryIndex> indices,
            long startOffset, long maxOffset, int maxMessageCount, long maxSizeBytes,
            java.util.function.Predicate<Long> offsetDeleted,
            java.util.function.Predicate<Long> skipCondition) {
        if (indices.isEmpty()) {
            return CompletableFuture.completedFuture(List.of());
        }
        EntryList entryList = new EntryList(logId.id(), startOffset, maxOffset,
            maxMessageCount, maxSizeBytes, offsetDeleted, skipCondition);
        CompletableFuture<List<LogEntry>> converted = storageApi.readEntries(indices, entryList)
            .whenComplete((__, e) -> {
                if (e != null) {
                    entryList.clear();
                }
            })
            .thenApply(__ -> Entry.toLogEntries(entryList.getEntries()));
        return OwnedResultFutures.transferLogEntries(converted);
    }

    @Override
    public CompletableFuture<LogOffset> getFirstOffset(LogId logId) {
        return storageApi.getFirstEntry(logId.id())
            .thenApply(StorageApiLogStorage::entryIndexToLogOffset);
    }

    @Override
    public CompletableFuture<LogOffset> getFirstOffset(LogId logId, boolean includeTrimmed) {
        return storageApi.getFirstEntry(logId.id(), includeTrimmed)
            .thenApply(StorageApiLogStorage::entryIndexToLogOffset);
    }

    @Override
    public CompletableFuture<LogOffset> getLastOffset(LogId logId) {
        return storageApi.getLastEntry(logId.id())
            .thenApply(StorageApiLogStorage::entryIndexToLogOffset);
    }

    @Override
    public CompletableFuture<Long> softTrim(LogId logId, long offsetIncluded) {
        return storageApi.softTrimStream(logId.id(), offsetIncluded);
    }

    @Override
    public CompletableFuture<List<EntryIndex>> readIndexRange(LogId logId, long startOffset, long endOffset) {
        return storageApi.readIndexes(logId.id(), startOffset, endOffset);
    }

    @Override
    public CompletableFuture<Void> hardTrim(LogId logId, long offsetExcluded) {
        return storageApi.hardTrimStream(logId.id(), offsetExcluded);
    }

    @Override
    public CompletableFuture<Void> deleteLog(LogId logId) {
        return storageApi.deleteStream(logId.id());
    }

    @Override
    public void preFetchEntries(LogId logId, List<Position> positions) {
        if (!positions.isEmpty()) {
            storageApi.preFetchEntries(logId.id(), positions);
        }
    }

    @Override
    public void close() throws IOException {
        storageApi.close();
    }

    private static LogOffset entryIndexToLogOffset(EntryIndex entryIndex) {
        if (entryIndex == EntryIndex.NOT_FOUND) {
            return LogOffset.NOT_FOUND;
        }
        EntryHeader header = entryIndex.header();
        return new LogOffset(
            header.offset(),
            header.numberOfMessages(),
            header.writtenTimestamp(),
            header.entrySize(),
            header.cumulativeSize()
        );
    }
}
