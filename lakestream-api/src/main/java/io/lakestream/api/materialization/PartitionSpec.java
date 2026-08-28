/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.api.materialization;

import java.util.Objects;
import java.util.Optional;

/**
 * One entry in a table's partitioning specification.
 *
 * <p>The {@code parameter} field carries transform arguments, for example the
 * bucket count {@code N} for {@link PartitionTransform#BUCKET} or the
 * truncation width {@code W} for {@link PartitionTransform#TRUNCATE}. Whether
 * a parameter is required depends on the transform; this record performs no
 * cross-field validation.
 *
 * @param column     source column name (non-null, non-empty)
 * @param transform  partition transform
 * @param parameter  optional transform argument
 */
public record PartitionSpec(
        String column,
        PartitionTransform transform,
        Optional<String> parameter) {

    /** Magic column name representing the stream's intrinsic partition. */
    public static final String STREAM_PARTITION_COLUMN = "__partition";

    /** Canonical constructor: validates non-null/non-empty fields. */
    public PartitionSpec {
        Objects.requireNonNull(column, "column");
        if (column.isEmpty()) {
            throw new IllegalArgumentException("column must not be empty");
        }
        Objects.requireNonNull(transform, "transform");
        Objects.requireNonNull(parameter, "parameter cannot be null; use Optional.empty()");
    }

    /**
     * Returns the sentinel partition spec that mirrors today's
     * {@code __partition} magic value: column {@code __partition},
     * {@link PartitionTransform#IDENTITY}, no parameter.
     */
    public static PartitionSpec streamPartition() {
        return new PartitionSpec(STREAM_PARTITION_COLUMN, PartitionTransform.IDENTITY, Optional.empty());
    }
}
