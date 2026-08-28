/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.api.materialization;

import java.util.Objects;
import java.util.Optional;

/**
 * Retention configuration for a materialized table.
 *
 * @param snapshotRetentionMs how long to keep table snapshots, in milliseconds
 * @param maxSnapshots        maximum number of snapshots to keep
 * @param rowRetentionMs      how long to keep rows, in milliseconds
 */
public record RetentionConfig(
        Optional<Long> snapshotRetentionMs,
        Optional<Integer> maxSnapshots,
        Optional<Long> rowRetentionMs) {

    /** Canonical constructor: validates that no Optional field is {@code null}. */
    public RetentionConfig {
        Objects.requireNonNull(snapshotRetentionMs,
                "snapshotRetentionMs cannot be null; use Optional.empty()");
        Objects.requireNonNull(maxSnapshots, "maxSnapshots cannot be null; use Optional.empty()");
        Objects.requireNonNull(rowRetentionMs,
                "rowRetentionMs cannot be null; use Optional.empty()");
    }
}
