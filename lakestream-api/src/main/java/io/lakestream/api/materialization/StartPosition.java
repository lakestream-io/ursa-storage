/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.api.materialization;

/**
 * Where a newly-enabled materialization begins reading from the source stream.
 *
 * <p>The concrete offset / timestamp value for {@link #OFFSET} and {@link #TIMESTAMP}
 * is carried separately and is out of scope for this enum.
 */
public enum StartPosition {

    /** Begin from the oldest available position. */
    EARLIEST,

    /** Begin from the newest position (skip historical data). */
    LATEST,

    /** Begin from a specific log offset (value carried separately). */
    OFFSET,

    /** Begin from a specific timestamp (value carried separately). */
    TIMESTAMP
}
