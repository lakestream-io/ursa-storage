/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.api;

import io.netty.buffer.ByteBuf;
import java.util.concurrent.CompletableFuture;

/**
 * Stream-level write interface.
 *
 * <p>Encapsulates routing (via {@link StreamLayout}) and append (via {@link LogStorage})
 * in a single call. The routing key determines which log receives the data.
 *
 * <p>Thread safety: implementations must be safe for concurrent use.
 */
public interface StreamWriter extends AutoCloseable {

    /**
     * Writes data to the stream. The routing key determines which log receives the data.
     *
     * <p>Internally: resolves layout → gets LogId → appends to LogStorage.
     *
     * @param key routing context for selecting the target log
     * @param numberOfRecords the number of records in the entry being written (≥ 1)
     * @param data the entry payload; the caller retains ownership of its reference until the
     *             returned future completes and must then release that reference exactly once
     * @return a future resolving to the write result (target log ID and offset)
     */
    CompletableFuture<WriteResult> write(RoutingKey key, int numberOfRecords, ByteBuf data);

    /**
     * Returns the stream's layout (for consumers that need direct log access).
     *
     * @return the underlying stream layout
     */
    StreamLayout layout();

    /**
     * Result of a write operation.
     *
     * @param logId the log that received the write
     * @param offset the offset assigned to the written entry
     */
    record WriteResult(LogId logId, long offset) {
    }
}
