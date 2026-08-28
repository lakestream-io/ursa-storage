/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.clickhouse;

import java.util.Objects;

/**
 * A single ClickHouse column descriptor used by {@link ClickHouseSchema}.
 *
 * <p>The {@link #type()} string is the verbatim ClickHouse type that lands in
 * the DDL (e.g. {@code Int64}, {@code String}, {@code Nullable(String)},
 * {@code Array(Int32)}, {@code Map(String, Int64)}). The {@link #nullable()}
 * flag duplicates the {@code Nullable(...)} wrapping for fast inspection by
 * the schema service / decoder without re-parsing the type string.
 *
 * @param name     the unquoted ClickHouse column name (must not be null)
 * @param type     the ClickHouse type string (must not be null)
 * @param nullable whether the column is wrapped in {@code Nullable(...)}
 */
public record ClickHouseColumn(String name, String type, boolean nullable) {

    /** Canonical constructor: rejects null name / type. */
    public ClickHouseColumn {
        Objects.requireNonNull(name, "name");
        Objects.requireNonNull(type, "type");
    }
}
