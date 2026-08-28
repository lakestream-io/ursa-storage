/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.api.materialization;

import java.util.Map;
import java.util.Objects;

/**
 * A registered, named table store the cluster can materialize streams into.
 *
 * <p>A {@code TableCatalog} represents the destination side of the
 * stream-to-table framework: a named handle to a backing system (Iceberg REST,
 * Delta on object storage, ClickHouse, etc.). Streams reference a catalog by
 * its {@link #name()} via {@link TableMaterializationPolicy#catalogRef()}.
 *
 * <p>The split between {@link #connection()} and {@link #properties()} is
 * intentional:
 * <ul>
 *   <li>{@code connection} carries per-catalog connection settings (URI,
 *       warehouse, DSN, auth references, {@code catalog-impl} class for
 *       Iceberg sub-flavours, etc.).</li>
 *   <li>{@code properties} carries catalog-level tuning defaults (e.g.,
 *       {@code target-file-size-bytes}, feature flags).</li>
 * </ul>
 *
 * <p>Both maps are defensively copied via {@link Map#copyOf(Map)} so callers
 * cannot mutate the record's state after construction.
 *
 * @param name        unique catalog name (non-null, non-empty)
 * @param type        backing catalog type
 * @param connection  connection settings; an immutable copy is stored
 * @param properties  catalog-level tuning defaults; an immutable copy is stored
 */
public record TableCatalog(
        String name,
        TableCatalogType type,
        Map<String, String> connection,
        Map<String, String> properties) {

    /**
     * Canonical constructor: validates non-null/non-empty {@code name},
     * non-null {@code type}, and defensively copies both maps.
     */
    public TableCatalog {
        Objects.requireNonNull(name, "name");
        if (name.isEmpty()) {
            throw new IllegalArgumentException("name must not be empty");
        }
        Objects.requireNonNull(type, "type");
        Objects.requireNonNull(connection, "connection");
        Objects.requireNonNull(properties, "properties");
        connection = Map.copyOf(connection);
        properties = Map.copyOf(properties);
    }
}
