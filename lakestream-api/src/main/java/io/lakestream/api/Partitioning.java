/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.api;

import java.util.Map;

/**
 * Partitioning configuration for a stream.
 *
 * <p>Describes the strategy and configuration for how a stream is divided into logs.
 *
 * @param strategy the partitioning strategy
 * @param config strategy-specific configuration (e.g., "numPartitions" for INDEXED)
 */
public record Partitioning(PartitioningStrategy strategy, Map<String, String> config) {

    /**
     * Returns the number of partitions (logs) configured for this stream.
     *
     * @return the partition count, defaults to 1 if not configured
     */
    public int numPartitions() {
        return Integer.parseInt(config.getOrDefault("numPartitions", "1"));
    }
}
