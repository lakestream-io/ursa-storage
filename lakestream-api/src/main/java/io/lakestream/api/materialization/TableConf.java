/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.api.materialization;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * Table-level (engine-specific) configuration for a materialization.
 *
 * <p>List-valued fields are defensively copied via {@link List#copyOf(java.util.Collection)}
 * (after unwrapping the Optional) so callers cannot mutate the record after construction.
 *
 * @param mode                 lifecycle ownership of the table
 * @param partitionBy          partition specification (immutable copy stored)
 * @param sortBy               sort columns (immutable copy stored)
 * @param retention            retention configuration
 * @param targetFileSizeBytes  target file size in bytes for data files
 * @param compression          codec for data files
 */
public record TableConf(
        Optional<TableMode> mode,
        Optional<List<PartitionSpec>> partitionBy,
        Optional<List<SortColumn>> sortBy,
        Optional<RetentionConfig> retention,
        Optional<Long> targetFileSizeBytes,
        Optional<Compression> compression) {

    /**
     * Canonical constructor: validates all Optional fields are non-null and
     * defensively copies any contained lists.
     */
    public TableConf {
        Objects.requireNonNull(mode, "mode cannot be null; use Optional.empty()");
        Objects.requireNonNull(partitionBy, "partitionBy cannot be null; use Optional.empty()");
        Objects.requireNonNull(sortBy, "sortBy cannot be null; use Optional.empty()");
        Objects.requireNonNull(retention, "retention cannot be null; use Optional.empty()");
        Objects.requireNonNull(targetFileSizeBytes,
                "targetFileSizeBytes cannot be null; use Optional.empty()");
        Objects.requireNonNull(compression, "compression cannot be null; use Optional.empty()");
        partitionBy = partitionBy.map(List::copyOf);
        sortBy = sortBy.map(List::copyOf);
    }
}
