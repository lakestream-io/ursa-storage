/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.api;

import io.netty.buffer.ByteBuf;
import java.io.Closeable;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * Level 1 interface: single-log operations.
 *
 * <p>Provides append, read, offset query, trim, and delete operations on individual logs
 * identified by {@link LogId}. Uses clean API types only ({@link LogId}, {@link LogEntry},
 * {@link LogOffset}) — no internal types like {@code EntryIndex} or {@code Position}.
 *
 * <p>The primary implementation is {@code StorageApiLogStorage} in {@code ursa-storage-core},
 * which adapts the existing {@code StorageApi} to this interface.
 *
 * <p>Thread safety: implementations must be safe for concurrent use from multiple threads.
 *
 * <p>Note: {@code createLog()} and {@code getLogIdByKey()} are internal to
 * {@link StreamCatalog} implementations. Users get {@link LogId} from
 * {@link StreamLayout#logIds()} or {@link StreamLayout#resolveForWrite(RoutingKey)}.
 */
public interface LogStorage extends Closeable {

    /**
     * Appends one entry containing the given number of records to the log.
     *
     * @param logId the target log
     * @param numberOfRecords the number of records in the entry being written (≥ 1)
     * @param data the entry payload
     * @return a future resolving to the header of the written entry
     */
    CompletableFuture<LogEntryHeader> append(LogId logId, int numberOfRecords, ByteBuf data);

    /**
     * Reads entries from the log starting at the given offset.
     *
     * <p>Reads up to {@code maxMessageCount} messages/records, with total payload size
     * not exceeding {@code maxSizeBytes}.
     *
     * @param logId the log to read from
     * @param startOffset the record offset to begin reading from (inclusive)
     * @param maxMessageCount maximum number of messages/records to read
     * @param maxSizeBytes maximum total payload size in bytes across all returned entries
     * @return a future resolving to the list of entries read; the caller must close every returned entry
     */
    CompletableFuture<List<LogEntry>> readEntries(LogId logId, long startOffset,
                                                   int maxMessageCount, long maxSizeBytes);

    /**
     * Reads entries using pre-fetched entry indexes, skipping the index lookup step.
     * This is an optimization for cursors that have pre-loaded entry indexes.
     *
     * <p>Only RAW entries should be passed; PARQUET entries require routing
     * through the unified reader.
     *
     * @param logId the log to read from
     * @param indices pre-fetched entry indexes with position information
     * @param startOffset the offset to start reading from (for mid-index resume)
     * @param maxOffset the maximum offset to read up to (exclusive)
     * @param maxMessageCount maximum number of messages/records to read
     * @param maxSizeBytes maximum total payload size
     * @param offsetDeleted predicate to skip deleted offsets during read
     * @param skipCondition additional predicate to skip entries
     * @return a future resolving to the entries read; the caller must close every returned entry
     */
    default CompletableFuture<List<LogEntry>> readEntriesByIndex(LogId logId, List<EntryIndex> indices,
            long startOffset, long maxOffset, int maxMessageCount, long maxSizeBytes,
            java.util.function.Predicate<Long> offsetDeleted,
            java.util.function.Predicate<Long> skipCondition) {
        if (indices.isEmpty()) {
            return CompletableFuture.completedFuture(List.of());
        }
        return readEntries(logId, startOffset, maxMessageCount, maxSizeBytes);
    }

    /**
     * Returns the offset information of the first entry in the log.
     *
     * @param logId the log to query
     * @return a future resolving to the first offset, or an exceptional future if the log is empty
     */
    CompletableFuture<LogOffset> getFirstOffset(LogId logId);

    /**
     * Returns the offset information of the first entry in the log,
     * optionally including soft-trimmed entries.
     *
     * @param logId the log to query
     * @param includeTrimmed if true, include soft-trimmed entries
     * @return a future resolving to the first offset, or an exceptional future if the log is empty
     */
    default CompletableFuture<LogOffset> getFirstOffset(LogId logId, boolean includeTrimmed) {
        return getFirstOffset(logId);
    }

    /**
     * Returns the offset information of the last entry in the log.
     *
     * @param logId the log to query
     * @return a future resolving to the last offset, or an exceptional future if the log is empty
     */
    CompletableFuture<LogOffset> getLastOffset(LogId logId);

    /**
     * Marks entries up to and including the given offset as deleted (soft trim).
     *
     * <p>Soft-trimmed entries are logically removed but may still exist on storage.
     *
     * @param logId the log to trim
     * @param offsetIncluded the offset up to which (inclusive) entries are marked deleted
     * @return a future resolving to the first entry's offset after trimming
     */
    CompletableFuture<Long> softTrim(LogId logId, long offsetIncluded);

    /**
     * Reads entry indexes within a given offset range.
     *
     * @param logId the log to read from
     * @param startOffset the start offset (inclusive)
     * @param endOffset the end offset (exclusive)
     * @return a future resolving to the list of entry indexes in the range
     */
    CompletableFuture<List<EntryIndex>> readIndexRange(LogId logId, long startOffset, long endOffset);

    /**
     * Physically deletes entries up to (exclusive) the given offset (hard trim).
     *
     * @param logId the log to trim
     * @param offsetExcluded the offset up to which (exclusive) entries are physically removed
     * @return a future that completes when the deletion is done
     */
    CompletableFuture<Void> hardTrim(LogId logId, long offsetExcluded);

    /**
     * Deletes the log entirely.
     *
     * @param logId the log to delete
     * @return a future that completes when the log is deleted
     */
    CompletableFuture<Void> deleteLog(LogId logId);

    /**
     * Hints to the storage layer that the given positions will be read soon.
     * This pre-warms the read cache (e.g., loading WAL files from S3) asynchronously.
     *
     * @param logId the log to prefetch for
     * @param positions the file positions to prefetch
     */
    default void preFetchEntries(LogId logId, List<Position> positions) {
        // No-op by default — implementations may optimize
    }
}
