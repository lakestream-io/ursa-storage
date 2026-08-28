/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.api;

import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * Stream-level read interface.
 *
 * <p>Encapsulates layout resolution and unified reading (PARQUET/RAW routing)
 * in a single interface. Handles both RAW and PARQUET data transparently.
 *
 * <p>Thread safety: implementations must be safe for concurrent use.
 */
public interface StreamReader extends AutoCloseable {

    /**
     * Reads entries from a specific log in the stream.
     *
     * <p>Handles both RAW and PARQUET data transparently.
     *
     * @param logId the log to read from
     * @param startOffset the record offset to begin reading from (inclusive)
     * @param maxMessageCount maximum number of messages/records to read
     * @param maxSizeBytes maximum total payload size in bytes
     * @return a future resolving to the read result; the caller must close every entry in the result
     */
    CompletableFuture<ReadResult> read(LogId logId, long startOffset,
                                        int maxMessageCount, long maxSizeBytes);

    /**
     * Returns all log IDs in this stream (delegates to layout).
     *
     * @return a future resolving to the list of log IDs
     */
    CompletableFuture<List<LogId>> logIds();

    /**
     * Returns the stream's layout (for consumers that need routing info).
     *
     * @return the underlying stream layout
     */
    StreamLayout layout();

    /**
     * Result of a read operation.
     *
     * @param entries the entries read; the caller must close every entry
     * @param nextOffset the offset to use for the next read call
     */
    record ReadResult(List<LogEntry> entries, long nextOffset) {
    }
}
