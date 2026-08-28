/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.materialization;

import java.util.Map;
import java.util.Objects;

/**
 * Sink-side outcome of a {@link TableMaterializer#commit()} call.
 *
 * <p>The {@code sinkMetadata} map is defensively copied via
 * {@link Map#copyOf(Map)} so the caller cannot mutate state after the commit.
 *
 * @param recordsCommitted number of records that were committed in this batch
 * @param bytesCommitted   number of bytes that were committed in this batch
 * @param sinkMetadata     opaque sink-specific commit metadata (e.g.,
 *                         {@code {"iceberg.snapshot-id":"…"}}); immutable copy
 */
public record CommitResult(
        long recordsCommitted,
        long bytesCommitted,
        Map<String, String> sinkMetadata) {

    /** Canonical constructor: defensively copies {@code sinkMetadata}. */
    public CommitResult {
        Objects.requireNonNull(sinkMetadata, "sinkMetadata");
        sinkMetadata = Map.copyOf(sinkMetadata);
    }
}
