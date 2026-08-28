/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.lakestream.impl;

import io.lakestream.api.LogId;
import io.lakestream.api.Partitioning;
import io.lakestream.api.PartitioningStrategy;
import io.lakestream.api.RoutingKey;
import io.lakestream.api.StreamLayout;
import io.lakestream.api.StreamPosition;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Layout for streams with a fixed number of logs accessed by integer index.
 *
 * <p>Supports explicit index routing ({@link RoutingKey#ofIndex(int)}) and
 * round-robin routing ({@link RoutingKey#roundRobin()}).
 */
public class IndexedLayout implements StreamLayout {

    private final List<LogId> logIds;
    private final Partitioning partitioning;
    private final AtomicInteger roundRobinCounter = new AtomicInteger(0);

    /**
     * Creates an indexed layout with the given log IDs.
     *
     * @param logIds the ordered list of log IDs (index = partition number)
     */
    public IndexedLayout(List<LogId> logIds) {
        this.logIds = Collections.unmodifiableList(logIds);
        this.partitioning = new Partitioning(
            PartitioningStrategy.INDEXED,
            Map.of("numPartitions", String.valueOf(logIds.size()))
        );
    }

    @Override
    public Partitioning partitioning() {
        return partitioning;
    }

    @Override
    public CompletableFuture<List<LogId>> logIds() {
        return CompletableFuture.completedFuture(logIds);
    }

    @Override
    public CompletableFuture<LogId> resolveForWrite(RoutingKey key) {
        if (key.indexHint().isPresent()) {
            int index = key.indexHint().getAsInt();
            if (index < 0 || index >= logIds.size()) {
                return CompletableFuture.failedFuture(
                    new IllegalArgumentException(
                        "Index " + index + " out of range [0, " + logIds.size() + ")"));
            }
            return CompletableFuture.completedFuture(logIds.get(index));
        }
        // Round-robin
        int index = Math.floorMod(roundRobinCounter.getAndIncrement(), logIds.size());
        return CompletableFuture.completedFuture(logIds.get(index));
    }

    @Override
    public int logCount() {
        return logIds.size();
    }

    @Override
    public StreamPosition position(LogId logId, long offset) {
        return new IndexedStreamPosition(logId, offset);
    }
}
