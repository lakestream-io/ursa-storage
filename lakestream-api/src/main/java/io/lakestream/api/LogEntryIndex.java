/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.api;

/**
 * Entry index information for a log entry.
 *
 * <p>Provides access to the entry header, entry count, and search operations
 * on entries within a compacted index. It is intended for {@link Log} and catalog
 * implementations rather than application-level record processing.
 *
 * <p>The primary implementation is {@code EntryIndex} in {@code ursa-storage-core}.
 *
 * <p>Thread safety: implementations should be safe for concurrent reads.
 */
public interface LogEntryIndex {

    /**
     * Returns the header metadata for this index entry.
     *
     * @return the entry header
     */
    LogEntryHeader header();

    /**
     * Returns the number of entries in this index.
     *
     * @return the entry count
     */
    int entryCount();

    /**
     * Searches for the entry header containing the given target offset.
     *
     * @param targetOffset the offset to search for
     * @return the entry header containing the target offset
     * @throws IllegalStateException if the offset is not within this index's range
     */
    LogEntryHeader searchEntryHeader(long targetOffset);

    /**
     * Returns the header of the first entry in this index.
     *
     * @return the first entry's header
     */
    LogEntryHeader getFirstEntryHeader();

    /**
     * Returns the header of the last entry in this index.
     *
     * @return the last entry's header
     */
    LogEntryHeader getLastEntryHeader();
}
