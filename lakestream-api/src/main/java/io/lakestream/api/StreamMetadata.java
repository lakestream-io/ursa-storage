/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.api;

import io.lakestream.api.materialization.TableMaterializationPolicy;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * An immutable snapshot of a stream's catalog metadata.
 *
 * <p>Returned by {@link StreamCatalog#createStream} and {@link StreamCatalog#loadStream}. This
 * value does not own any data-plane resources and does not need to be closed. In particular,
 * loading stream metadata does not open a reader, writer, log, or cache. Data-plane resources are
 * opened explicitly through {@link StreamCatalog#openLog}, {@link StreamCatalog#openReader}, or
 * {@link StreamCatalog#openWriter}.
 *
 * @param identifier the stream identifier
 * @param config the stream configuration
 * @param partitioning the partitioning configuration
 * @param schema the schema configuration
 * @param properties the stream properties
 * @param materialization the stream-level materialization policy override
 * @param state the current lifecycle state
 * @param layout the committed stream layout represented by this snapshot
 * @param metadataVersion the catalog version of the stream metadata
 */
public record StreamMetadata(
        StreamIdentifier identifier,
        StreamConfig config,
        Partitioning partitioning,
        SchemaConfig schema,
        Map<String, String> properties,
        Optional<TableMaterializationPolicy> materialization,
        LifecycleState state,
        StreamLayout layout,
        long metadataVersion) {

    /**
     * Creates a defensively copied stream metadata snapshot.
     */
    public StreamMetadata {
        Objects.requireNonNull(identifier, "identifier");
        Objects.requireNonNull(config, "config");
        Objects.requireNonNull(partitioning, "partitioning");
        Objects.requireNonNull(schema, "schema");
        Objects.requireNonNull(properties, "properties");
        Objects.requireNonNull(materialization, "materialization");
        Objects.requireNonNull(state, "state");
        Objects.requireNonNull(layout, "layout");

        config = new StreamConfig(Map.copyOf(config.properties()));
        partitioning = new Partitioning(
            partitioning.strategy(), Map.copyOf(partitioning.config()));
        schema = new SchemaConfig(schema.schemaType(), Map.copyOf(schema.properties()));
        properties = Map.copyOf(properties);
    }
}
