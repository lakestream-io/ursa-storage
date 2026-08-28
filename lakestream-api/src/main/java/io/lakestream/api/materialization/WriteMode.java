/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.api.materialization;

/**
 * Strategy used by the materializer to apply stream records to the destination table.
 */
public enum WriteMode {

    /** Append-only: every record becomes a new row. */
    APPEND,

    /** Upsert by primary key: existing rows are replaced. */
    UPSERT,

    /** Change-data-capture: insert/update/delete propagated from upstream op codes. */
    CDC
}
