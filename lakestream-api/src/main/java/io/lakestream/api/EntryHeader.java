/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.api;

import java.util.Objects;

/**
 * Represents the header information for an entry in the storage system.
 * This record encapsulates metadata about a single entry, which typically contains
 * one or more messages.
 *
 * Usage Notes:
 * 1. This class is immutable, ensuring thread-safety when shared across threads.
 * 2. It's used extensively throughout the storage system to track entry information.
 * 3. The `NOT_FOUND` constant should be used to represent a non-existent entry.
 * 4. When working with streams, always ensure the `offset` and `cumulativeSize`
 *    are consistent with the stream's state to maintain data integrity.
 */
public record EntryHeader(
        long offset,         /// Offset in stream assigned to the first message in the entry
        int numberOfMessages, /// Number of messages contained in the single entry batch
        long writtenTimestamp, /// Timestamp of when the entry was written
        int entrySize,        /// The size of this entry in bytes
        long cumulativeSize   /// Cumulative size of all the entries in the stream up to this point
) implements LogEntryHeader {

    /**
     * Represents a non-existent or invalid entry.
     * Use this constant when an entry is not found or to initialize empty states.
     */
    public static final EntryHeader NOT_FOUND = new EntryHeader(-1, 0, 0, 0, 0);

    // --- LogEntryHeader bridge methods ---

    @Override
    public int numberOfRecords() {
        return numberOfMessages;
    }

    @Override
    public long timestamp() {
        return writtenTimestamp;
    }

    @Override
    public boolean newerThan(LogEntryHeader other) {
        if (other == null) {
            return true;
        }
        return (offset + numberOfRecords()) > (other.offset() + other.numberOfRecords());
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        EntryHeader that = (EntryHeader) o;
        return offset == that.offset
               && entrySize == that.entrySize
               && cumulativeSize == that.cumulativeSize
               && numberOfMessages == that.numberOfMessages
               && writtenTimestamp == that.writtenTimestamp;
    }

    @Override
    public int hashCode() {
        return Objects.hash(offset, numberOfMessages, writtenTimestamp, entrySize, cumulativeSize);
    }
}
