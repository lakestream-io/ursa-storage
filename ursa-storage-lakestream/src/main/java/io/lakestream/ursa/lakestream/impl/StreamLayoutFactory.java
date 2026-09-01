/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.lakestream.impl;

import io.lakestream.api.LogId;
import io.lakestream.api.Partitioning;
import io.lakestream.api.PartitioningStrategy;
import io.lakestream.api.StreamLayout;
import java.util.List;

/**
 * Factory for building {@link StreamLayout} instances from partitioning config and log IDs.
 */
public final class StreamLayoutFactory {

    private StreamLayoutFactory() {
    }

    /**
     * Builds a layout from the partitioning configuration and ordered log IDs.
     *
     * @param partitioning the partitioning configuration
     * @param logIds the ordered list of log IDs
     * @return the appropriate StreamLayout implementation
     */
    public static StreamLayout create(Partitioning partitioning, List<LogId> logIds) {
        if (partitioning.strategy() == PartitioningStrategy.INDEXED) {
            return new IndexedLayout(logIds);
        }
        throw new UnsupportedOperationException(
            "Unsupported partitioning strategy: " + partitioning.strategy());
    }
}
