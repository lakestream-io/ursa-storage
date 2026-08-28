/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.materialization;

import io.lakestream.api.StreamIdentifier;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * Per-record context passed to {@link TableMaterializer#write(Object, MaterializationContext)}.
 *
 * <p>Carries the offset and timestamp of the source record, plus any
 * source-format-specific metadata (for example Kafka headers) needed for the sink-side write.
 *
 * @param stream              the originating stream identifier
 * @param offset              the source-stream offset of this record
 * @param timestamp           the source-stream timestamp (millis) of this record
 * @param sourceSchemaVersion the source schema version, when known
 * @param sourceMetadata      additional source-format metadata; immutable copy
 */
public record MaterializationContext(
        StreamIdentifier stream,
        long offset,
        long timestamp,
        Optional<Long> sourceSchemaVersion,
        Map<String, String> sourceMetadata) {

    /**
     * Canonical constructor: validates non-null fields and defensively copies
     * {@code sourceMetadata}.
     */
    public MaterializationContext {
        Objects.requireNonNull(stream, "stream");
        Objects.requireNonNull(sourceSchemaVersion,
                "sourceSchemaVersion cannot be null; use Optional.empty()");
        Objects.requireNonNull(sourceMetadata, "sourceMetadata");
        sourceMetadata = Map.copyOf(sourceMetadata);
    }
}
