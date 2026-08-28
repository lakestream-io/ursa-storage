/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.api.materialization;

/**
 * Compression codec used when writing data files for a materialized table.
 */
public enum Compression {

    /** Zstandard codec. */
    ZSTD,

    /** Snappy codec. */
    SNAPPY,

    /** gzip codec. */
    GZIP,

    /** LZ4 codec. */
    LZ4,

    /** No compression. */
    UNCOMPRESSED
}
