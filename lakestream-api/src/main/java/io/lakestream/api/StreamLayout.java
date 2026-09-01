/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.api;

import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * Defines how a stream is composed from logs.
 *
 * <p>A stream resolves directly to log IDs — no intermediate Partition type.
 * Different implementations handle different composition strategies:
 * <ul>
 *   <li>{@code IndexedLayout}: fixed number of logs accessed by integer index</li>
 *   <li>{@code RangeLayout} (future): key-range segments with split/merge</li>
 * </ul>
 *
 * <p>Per-log metadata (offsets, size, state) is accessed via {@link LogStorage},
 * not through this interface.
 *
 * <p>A committed layout is exposed by {@link StreamMetadata#layout()} and
 * {@link StreamCatalog#getLayout(StreamIdentifier)}. Data-plane resources using the layout are
 * opened explicitly through {@link StreamCatalog#openLog}, {@link StreamCatalog#openReader}, or
 * {@link StreamCatalog#openWriter}.
 *
 * <p>Thread safety: implementations must be safe for concurrent use.
 */
public interface StreamLayout {

    /**
     * Returns the composition strategy and configuration.
     *
     * @return the partitioning configuration
     */
    Partitioning partitioning();

    /**
     * Returns all log IDs that make up this stream, in order.
     *
     * <p>For {@code INDEXED} strategy, the list order corresponds to partition indices.
     *
     * @return a future resolving to the ordered list of log IDs
     */
    CompletableFuture<List<LogId>> logIds();

    /**
     * Resolves which log should receive a write for the given routing key.
     *
     * <p>Routing depends on implementation:
     * <ul>
     *   <li>{@code indexHint} set → log at that ordinal index</li>
     *   <li>neither → implementation default (round-robin, etc.)</li>
     * </ul>
     *
     * @param key the routing context for the write
     * @return a future resolving to the target log ID
     * @throws IllegalArgumentException if the index hint is out of range
     */
    CompletableFuture<LogId> resolveForWrite(RoutingKey key);

    /**
     * Returns the number of logs in this stream.
     *
     * @return the log count
     */
    int logCount();

    /**
     * Creates a {@link StreamPosition} for the given log and offset.
     *
     * <p>The returned value preserves the layout-specific interpretation of the log and offset for
     * layout-aware callers.
     *
     * @param logId the log within the stream
     * @param offset the offset within the log
     * @return a stream position
     */
    StreamPosition position(LogId logId, long offset);
}
