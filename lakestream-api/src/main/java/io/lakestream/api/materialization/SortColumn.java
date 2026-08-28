/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.api.materialization;

import java.util.Objects;

/**
 * A single column within a table's sort order.
 *
 * @param column     source column name (non-null, non-empty)
 * @param direction  ascending or descending
 * @param nullsFirst whether nulls sort first within the direction
 */
public record SortColumn(String column, SortDirection direction, boolean nullsFirst) {

    /** Canonical constructor: validates {@code column} and {@code direction}. */
    public SortColumn {
        Objects.requireNonNull(column, "column");
        if (column.isEmpty()) {
            throw new IllegalArgumentException("column must not be empty");
        }
        Objects.requireNonNull(direction, "direction");
    }
}
