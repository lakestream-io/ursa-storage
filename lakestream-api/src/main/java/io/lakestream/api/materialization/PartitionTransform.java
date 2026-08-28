/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.api.materialization;

/**
 * Partition transform applied to a source column to derive a partition value.
 *
 * <p>Mirrors the Iceberg partition transform vocabulary; engines that do not
 * support a given transform should reject the spec at policy resolution time.
 */
public enum PartitionTransform {

    /** Identity: partition by the raw column value. */
    IDENTITY,

    /** Bucket: hash the column into {@code N} buckets (parameter required). */
    BUCKET,

    /** Truncate: truncate the column to width {@code W} (parameter required). */
    TRUNCATE,

    /** Year part of a timestamp/date column. */
    YEAR,

    /** Month part of a timestamp/date column. */
    MONTH,

    /** Day part of a timestamp/date column. */
    DAY,

    /** Hour part of a timestamp column. */
    HOUR,

    /** Custom engine-specific expression (parameter required). */
    EXPRESSION
}
