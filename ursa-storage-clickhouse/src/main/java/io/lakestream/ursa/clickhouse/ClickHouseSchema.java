/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.clickhouse;

import java.util.List;
import java.util.Objects;

/**
 * Typed description of a ClickHouse destination schema produced by
 * {@link ClickHouseTableSchemaService}.
 *
 * <p>The record bundles three pieces of information used to drive DDL and row
 * decoding:
 * <ul>
 *   <li>{@link #columns()} — ordered list of {@link ClickHouseColumn} entries
 *       carrying the column name, type string and a nullability flag.</li>
 *   <li>{@link #primaryKey()} — ordered list of column names that form the
 *       {@code PRIMARY KEY} / {@code ORDER BY} clause when the table is
 *       created. For upsert engines this drives the
 *       {@code ReplacingMergeTree} dedup key; for append engines it still
 *       contributes to the {@code ORDER BY} for sort-on-merge.</li>
 *   <li>{@link #engine()} — the engine to assume when creating the table.</li>
 * </ul>
 *
 * <p>Both list fields are defensively copied via {@link List#copyOf(java.util.Collection)}
 * so callers cannot mutate the schema after construction.
 */
public record ClickHouseSchema(
        List<ClickHouseColumn> columns,
        List<String> primaryKey,
        ClickHouseTableEngine engine) {

    /** Canonical constructor: defensively copies list fields and rejects nulls. */
    public ClickHouseSchema {
        columns = List.copyOf(Objects.requireNonNull(columns, "columns"));
        primaryKey = List.copyOf(Objects.requireNonNull(primaryKey, "primaryKey"));
        Objects.requireNonNull(engine, "engine");
    }
}
