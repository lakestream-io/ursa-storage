/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.api;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.Predicate;

/**
 * Per-log cursor — tracks read position and acknowledgment for a single {@link Log}.
 *
 * <p>Provides sequential reads (advancing an internal read offset), single-entry
 * random reads, mark-delete acknowledgment, seek, and entry metadata lookup.
 *
 * <p>Thread safety: implementations must be safe for concurrent use.
 */
public interface LogCursor extends AutoCloseable {

    /**
     * Returns the name of this cursor.
     */
    String name();

    /**
     * Returns the underlying log this cursor reads from.
     */
    Log log();

    /**
     * Returns the current read offset — the next offset that {@link #readEntries} will start from.
     */
    long readOffset();

    /**
     * Returns the mark-delete offset — all entries up to and including this offset are acknowledged.
     */
    long markDeleteOffset();

    /**
     * Reads entries starting at the current {@link #readOffset()}, advancing the read position.
     *
     * @param maxEntries maximum number of entries to return
     * @param maxSizeBytes maximum total payload size in bytes
     * @return a future resolving to the list of entries read; the caller must close every returned entry
     */
    CompletableFuture<List<LogEntry>> readEntries(int maxEntries, long maxSizeBytes);

    /**
     * Reads a single entry at the specified offset without advancing the read position.
     *
     * @param offset the offset to read
     * @return a future resolving to the entry at that offset; the caller must close the returned entry
     */
    CompletableFuture<LogEntry> readEntry(long offset);

    /**
     * Acknowledges all entries up to and including the given offset.
     *
     * @param offset the offset to mark as deleted (inclusive)
     * @param properties cursor properties to persist alongside the position
     * @return a future that completes when the mark-delete is persisted
     */
    CompletableFuture<Void> markDelete(long offset, Map<String, Long> properties);

    /**
     * Moves the read position to the given offset.
     *
     * <p>For ephemeral cursors this only updates the in-memory read position.
     *
     * @param offset the offset to seek to
     * @return a future that completes when the seek is done
     */
    CompletableFuture<Void> seek(long offset);

    /**
     * Returns the entry metadata (header) at the given offset, using the log's cache.
     *
     * @param offset the offset to query
     * @return a future resolving to the entry header at that offset
     */
    CompletableFuture<LogEntryHeader> getEntryMetadata(long offset);

    // --- Individual acknowledgment ---

    /**
     * Individually acknowledges entries starting at the given offset.
     *
     * @param offset the first offset to acknowledge
     * @param numberOfRecords number of consecutive records to acknowledge (0 means single offset)
     * @return a future that completes when the ack is recorded (not necessarily persisted)
     */
    default CompletableFuture<Void> individualDelete(long offset, int numberOfRecords) {
        return CompletableFuture.completedFuture(null);
    }

    /**
     * Returns whether the given offset has been individually deleted.
     */
    default boolean isOffsetIndividuallyDeleted(long offset) {
        return false;
    }

    /**
     * Returns the number of individually deleted entries tracked by this cursor.
     */
    default long individualDeleteCount() {
        return 0;
    }

    /**
     * Returns the first non-deleted offset after the mark-delete position.
     */
    default long firstNonDeletedOffset() {
        return markDeleteOffset() + 1;
    }

    // --- Prefetch-aware batch read with skip conditions ---

    /**
     * Reads entries starting at the current read offset with skip conditions.
     *
     * @param maxEntries maximum number of entries to return
     * @param maxSizeBytes maximum total payload size in bytes
     * @param skipCondition predicate returning true for offsets to skip (may be null)
     * @param maxOffset exclusive upper bound on offsets to read
     * @return a future resolving to the list of entries read; the caller must close every returned entry
     */
    default CompletableFuture<List<LogEntry>> readEntries(int maxEntries, long maxSizeBytes,
            Predicate<Long> skipCondition, long maxOffset) {
        return readEntries(maxEntries, maxSizeBytes);
    }

    // --- Persistence ---

    /**
     * Persists the current cursor state (mark-delete offset, individual acks).
     *
     * @return a future that completes when state is persisted
     */
    default CompletableFuture<Void> persistState() {
        return CompletableFuture.completedFuture(null);
    }

    /**
     * Returns the last persisted mark-delete offset.
     */
    default long persistedMarkDeleteOffset() {
        return markDeleteOffset();
    }

    /**
     * Returns the cursor properties persisted alongside the mark-delete position.
     */
    default Map<String, Long> properties() {
        return Map.of();
    }

    // --- Lifecycle and stats ---

    /**
     * Returns true if there are more entries available to read.
     */
    default boolean hasMoreEntries() {
        return false;
    }

    /**
     * Returns the number of entries in the backlog (between mark-delete and last offset).
     */
    default long getNumberOfEntriesInBacklog() {
        return 0;
    }

    /**
     * Deletes this cursor and its persisted state.
     *
     * @return a future that completes when the cursor is deleted
     */
    default CompletableFuture<Void> deleteCursor() {
        return CompletableFuture.completedFuture(null);
    }

    /**
     * Releases this cursor.
     *
     * <p>An ephemeral cursor holds in-memory state only, so closing it releases that state and
     * returns immediately without any catalog or storage round trip. A durable cursor also has to
     * detach from its persisted state, so its close may block for the length of that round trip;
     * closing a durable cursor never deletes its persisted state — see {@link #deleteCursor()}.
     *
     * <p>The declaration keeps the checked {@code Exception} of {@link AutoCloseable#close()} so a
     * durable implementation can report a failed detach; the ephemeral case does not throw.
     *
     * @throws Exception if a durable cursor fails to detach from its persisted state
     */
    @Override
    void close() throws Exception;
}
