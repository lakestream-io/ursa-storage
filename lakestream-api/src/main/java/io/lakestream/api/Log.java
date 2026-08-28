/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.api;

import io.netty.buffer.ByteBuf;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Predicate;

/**
 * Per-log managed operations.
 *
 * <p>Provides append, read, metadata query, and fencing operations on a single log.
 * Internally uses {@link LogStorage} for data operations, a shared entry index cache
 * for metadata queries, and {@code UnifiedStreamReader} for transparent RAW/PARQUET reads.
 *
 * <p>Obtained via {@link Stream#getLog(LogId)}.
 *
 * <p>Thread safety: implementations must be safe for concurrent use.
 */
public interface Log extends AutoCloseable {

    /**
     * Returns the identifier of this log.
     *
     * @return the log ID
     */
    LogId id();

    /**
     * Appends one entry containing the given number of records to the log.
     *
     * @param numberOfRecords the number of records in the entry being written (&ge; 1)
     * @param data the entry payload
     * @return a future resolving to the header of the written entry
     */
    CompletableFuture<LogEntryHeader> append(int numberOfRecords, ByteBuf data);

    /**
     * Reads entries from the log starting at the given offset.
     *
     * <p>Reads up to {@code maxMessageCount} messages/records, with total payload size
     * not exceeding {@code maxSizeBytes}. Handles both RAW and PARQUET data transparently.
     *
     * @param startOffset the record offset to begin reading from (inclusive)
     * @param maxMessageCount maximum number of messages/records to read
     * @param maxSizeBytes maximum total payload size in bytes across all returned entries
     * @return a future resolving to the list of entries read; the caller must close every returned entry
     */
    CompletableFuture<List<LogEntry>> readEntries(long startOffset, int maxMessageCount, long maxSizeBytes);

    /**
     * Reads entries from the log starting at the given offset, optionally including trimmed entries.
     *
     * @param startOffset the record offset to begin reading from (inclusive)
     * @param maxMessageCount maximum number of messages/records to read
     * @param maxSizeBytes maximum total payload size in bytes across all returned entries
     * @param includeTrimmed if true, include entries that have been soft-trimmed
     * @return a future resolving to the list of entries read; the caller must close every returned entry
     */
    CompletableFuture<List<LogEntry>> readEntries(long startOffset, int maxMessageCount,
                                                   long maxSizeBytes, boolean includeTrimmed);

    /**
     * Reads a single entry at the given offset, resolving the index internally.
     *
     * @param offset the offset to read
     * @return a future resolving to the entry at that offset; the caller must close the returned entry
     */
    CompletableFuture<LogEntry> readEntry(long offset);

    /**
     * Returns the entry metadata (header) at the given offset, using the shared cache.
     *
     * @param offset the offset to query
     * @return a future resolving to the entry header at that offset
     */
    CompletableFuture<LogEntryHeader> getEntryMetadata(long offset);

    /**
     * Returns the full entry index at the given offset, using the shared cache.
     *
     * @param offset the offset to query
     * @return a future resolving to the entry index at that offset
     */
    CompletableFuture<EntryIndex> getEntryIndex(long offset);

    /**
     * Returns entry indexes for a range of offsets.
     *
     * @param startOffset the start offset (inclusive)
     * @param endOffset the end offset (exclusive)
     * @return a future resolving to the list of entry indexes in the range
     */
    CompletableFuture<List<EntryIndex>> readIndexRange(long startOffset, long endOffset);

    /**
     * Returns entry metadata for a range of offsets.
     *
     * @param startOffset the start offset (inclusive)
     * @param endOffset the end offset (exclusive)
     * @return a future resolving to the list of entry headers in the range
     */
    CompletableFuture<List<LogEntryHeader>> getEntryMetadataRange(long startOffset, long endOffset);

    /**
     * Returns the offset information of the first entry in the log.
     *
     * @return a future resolving to the first offset
     */
    CompletableFuture<LogOffset> getFirstOffset();

    /**
     * Returns the offset information of the first entry in the log,
     * optionally including soft-trimmed entries.
     *
     * @param includeTrimmed if true, include soft-trimmed entries
     * @return a future resolving to the first offset
     */
    CompletableFuture<LogOffset> getFirstOffset(boolean includeTrimmed);

    /**
     * Returns the offset information of the last entry in the log.
     *
     * @return a future resolving to the last offset
     */
    CompletableFuture<LogOffset> getLastOffset();

    /**
     * Marks entries up to and including the given offset as deleted (soft trim).
     *
     * @param offsetIncluded the offset up to which (inclusive) entries are marked deleted
     * @return a future resolving to the first entry's offset after trimming
     */
    CompletableFuture<Long> softTrim(long offsetIncluded);

    /**
     * Deletes the entire log.
     *
     * @return a future that completes when the log is deleted
     */
    CompletableFuture<Void> delete();

    /**
     * Returns the underlying {@link LogStorage} for advanced operations.
     * Callers should prefer the higher-level methods on {@code Log} when possible.
     *
     * @return the log storage
     */
    LogStorage logStorage();

    /**
     * Caches an entry index for future lookups.
     *
     * @param index the entry index to cache
     */
    void cacheIndex(EntryIndex index);

    /**
     * Invalidates all cached entry metadata for this log.
     */
    void invalidateCache();

    /**
     * Invalidates the cached entry metadata at the given offset.
     *
     * @param offset the offset whose cached metadata to invalidate
     */
    void invalidateCache(long offset);

    /**
     * Returns the number of messages between two offsets, using cached index data.
     *
     * @param startOffset the start offset (inclusive)
     * @param endOffset the end offset (exclusive)
     * @return the message count, or -1 if not available
     */
    long getMessageCount(long startOffset, long endOffset);

    /**
     * Sets this log's stream state to NORMAL (complement of {@link #fence()}).
     */
    void activate();

    /**
     * Fences this log — subsequent append operations will fail.
     */
    void fence();

    // --- Cursor management ---

    /**
     * Opens or creates a cursor on this log.
     *
     * @param name the cursor name
     * @param initialOffset the initial mark-delete offset (-1 for none)
     * @return a future resolving to the opened cursor
     */
    default CompletableFuture<LogCursor> openCursor(String name, long initialOffset) {
        return CompletableFuture.failedFuture(new UnsupportedOperationException("Cursor management not supported"));
    }

    /**
     * Opens an ephemeral (non-persistent) cursor for temporary reads.
     * The cursor tracks read position in memory only — no Oxia persistence.
     * Suitable for fetch reads, timestamp scanning, and replay operations.
     *
     * @param name the cursor name
     * @param initialOffset the initial read offset
     * @return a future resolving to the opened ephemeral cursor
     */
    default CompletableFuture<LogCursor> openEphemeralCursor(String name, long initialOffset) {
        return CompletableFuture.failedFuture(
            new UnsupportedOperationException("Ephemeral cursors not supported"));
    }

    /**
     * Loads an existing cursor by name.
     *
     * @param name the cursor name
     * @return a future resolving to the loaded cursor
     */
    default CompletableFuture<LogCursor> loadCursor(String name) {
        return CompletableFuture.failedFuture(new UnsupportedOperationException("Cursor management not supported"));
    }

    /**
     * Loads all cursors associated with this log.
     *
     * @return a future resolving to the list of loaded cursors
     */
    default CompletableFuture<List<LogCursor>> loadAllCursors() {
        return CompletableFuture.completedFuture(List.of());
    }

    /**
     * Deletes a cursor by name.
     *
     * @param name the cursor name to delete
     * @return a future that completes when the cursor is deleted
     */
    default CompletableFuture<Void> deleteCursor(String name) {
        return CompletableFuture.completedFuture(null);
    }

    // --- Retention ---

    /**
     * Computes the trim offset based on retention policies.
     *
     * @param maxOffset the cursor-based maximum trim offset
     * @param retentionMillis time-based retention in milliseconds (0 = none, negative = infinite)
     * @param retentionSizeBytes size-based retention in bytes (0 = none, negative = infinite)
     * @return a future resolving to the computed trim offset
     */
    default CompletableFuture<Long> computeRetentionTrimOffset(long maxOffset, long retentionMillis,
            long retentionSizeBytes) {
        return CompletableFuture.completedFuture(maxOffset);
    }

    // --- Search ---

    /**
     * Binary search over entry headers in the given offset range.
     *
     * @param min minimum offset (inclusive)
     * @param max maximum offset (inclusive)
     * @param predicate returns true for entries that should be on the "left" side of the search
     * @return a future resolving to the found offset
     */
    default CompletableFuture<Long> binarySearchOffset(long min, long max, Predicate<LogEntryHeader> predicate) {
        return CompletableFuture.completedFuture(min);
    }
}
