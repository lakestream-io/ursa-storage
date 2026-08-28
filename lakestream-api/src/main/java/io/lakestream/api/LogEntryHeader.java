/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.api;

/**
 * Metadata for a single log entry.
 *
 * <p>Provides offset, record count, timestamp, and size information about an entry.
 * The primary implementation is {@code EntryHeader} in {@code ursa-storage-core},
 * which implements this interface with zero-overhead bridge methods.
 *
 * <p>Thread safety: implementations should be immutable and safe for concurrent reads.
 */
public interface LogEntryHeader {

    /**
     * Returns the offset of the first record in this entry.
     *
     * @return the record offset (zero-based, monotonically increasing within a log)
     */
    long offset();

    /**
     * Returns the number of records batched in this entry.
     *
     * @return the count of records, always &ge; 1
     */
    int numberOfRecords();

    /**
     * Returns the timestamp when this entry was written.
     *
     * @return the write timestamp in milliseconds since epoch
     */
    long timestamp();

    /**
     * Returns the size of this entry in bytes.
     *
     * @return the entry size in bytes
     */
    int entrySize();

    /**
     * Returns the cumulative size of all entries in the log up to and including this one.
     *
     * @return the cumulative size in bytes
     */
    long cumulativeSize();

    /**
     * Returns whether this entry is newer than the given entry.
     *
     * <p>An entry is considered newer if the offset of its last record
     * (offset + numberOfRecords) is greater than the other's.
     *
     * @param other the entry to compare against, may be null
     * @return true if this entry is newer, or if other is null
     */
    boolean newerThan(LogEntryHeader other);
}
