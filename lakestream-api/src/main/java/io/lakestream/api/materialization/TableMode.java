/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.api.materialization;

/**
 * Lifecycle ownership of the destination table.
 */
public enum TableMode {

    /** Ursa creates and owns the table (drop on stream delete is allowed). */
    MANAGED,

    /** Ursa writes to a pre-existing table owned by the user; never dropped. */
    EXTERNAL,

    /** User-supplied or plugin-managed table mode. */
    CUSTOM
}
