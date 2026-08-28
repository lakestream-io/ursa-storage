/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.lakestream.impl;

import io.lakestream.api.LogEntry;
import io.lakestream.api.LogId;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * Internal interface that routes reads between RAW (WAL) and PARQUET (compacted) storage.
 *
 * <p>Given a log ID and offset range, it determines the storage format through the entry index cache
 * and delegates to the appropriate reader.
 *
 * <p>This interface is used by {@link StreamReaderImpl} and {@link LogCursorImpl}; it is not part of
 * the public Lakestream API.
 */
public interface UnifiedStreamReader extends AutoCloseable {

    /**
     * Read entries from a log, transparently handling RAW and PARQUET data.
     *
     * <p>The implementation resolves the entry index for the given offset,
     * checks the file type, and delegates to either the WAL storage (RAW)
     * or the compacted object reader (PARQUET).
     *
     * @param logId the log to read from
     * @param startOffset the record offset to begin reading from (inclusive)
     * @param maxMessageCount maximum number of messages/records to read
     * @param maxSizeBytes maximum total payload size in bytes
     * @return a future completing with the read result
     */
    CompletableFuture<ReadResult> readEntries(
        LogId logId, long startOffset, int maxMessageCount, long maxSizeBytes);

    /**
     * Result of a unified read operation.
     *
     * @param entries the entries read (may be from RAW or PARQUET storage)
     * @param nextOffset the offset to use for the next read (offset of last entry + its record count)
     */
    record ReadResult(List<LogEntry> entries, long nextOffset) { }
}
