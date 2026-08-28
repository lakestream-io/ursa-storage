/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.api;

import io.netty.buffer.ByteBuf;

/**
 * Clean read-only view of a log entry.
 *
 * <p>An entry is a storage-level batch of one or more records. One {@code append()}
 * call creates one entry. The entry's offset equals its first record's offset.
 *
 * <p>Implemented by {@code Entry} in {@code ursa-storage-core}.
 *
 * <p>Thread safety: implementations should be safe for concurrent reads.
 * The payload {@link ByteBuf} should be treated as read-only by consumers.
 */
public interface LogEntry extends AutoCloseable {

    /**
     * Returns the offset of the first record in this entry.
     *
     * @return the record offset (zero-based, monotonically increasing within a log)
     */
    long offset();

    /**
     * Returns the number of records batched in this entry.
     *
     * <p>An entry at offset {@code o} with {@code n} records spans offsets {@code [o, o+n)}.
     *
     * <p>Note: the new API uses "record" as the standard term. The existing
     * {@code StorageApi}/{@code EntryHeader} uses "message" — the adapter bridges the naming.
     *
     * @return the count of records in this entry, always ≥ 1
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
    int size();

    /**
     * Returns the entry payload as a read-only ByteBuf.
     *
     * <p>The returned buffer is a borrowed, read-only view of the reference owned by this entry.
     * Callers must not modify or release the returned buffer directly. To keep or transfer the payload
     * beyond this entry's lifetime, create an independently owned reference with {@code retainedDuplicate()}
     * and release that duplicate when it is no longer needed.
     *
     * @return the payload data
     */
    ByteBuf payload();

    /**
     * Releases the single payload reference owned by this entry.
     *
     * <p>Every entry returned by a read operation must be closed exactly once by its caller. Implementations
     * must make repeated calls safe, so cleanup paths may close an entry idempotently.
     */
    @Override
    void close();
}
