/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.api;

/**
 * Offset information for a log position.
 *
 * <p>Used by {@link LogStorage#getFirstOffset(LogId)} and
 * {@link LogStorage#getLastOffset(LogId)} to return position metadata.
 *
 * @param offset the record offset position in the log
 * @param numberOfRecords the number of records at this offset position
 * @param timestamp the timestamp of the entry at this offset
 * @param entrySize the size of the entry at this offset in bytes
 * @param cumulativeSize the cumulative size of all entries up to and including this one
 */
public record LogOffset(long offset, int numberOfRecords, long timestamp,
                        int entrySize, long cumulativeSize) {

    /**
     * Sentinel value representing a non-existent log offset.
     */
    public static final LogOffset NOT_FOUND = new LogOffset(-1, 0, 0, 0, 0);

    /**
     * Constructs a LogOffset without size information (backward-compatible).
     *
     * @param offset the record offset position
     * @param numberOfRecords the number of records at this position
     * @param timestamp the timestamp of the entry
     */
    public LogOffset(long offset, int numberOfRecords, long timestamp) {
        this(offset, numberOfRecords, timestamp, 0, 0);
    }
}
