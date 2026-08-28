/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.api.materialization;

/**
 * Backing implementation type of a registered {@link TableCatalog}.
 */
public enum TableCatalogType {

    /** Apache Iceberg catalogs (REST, Hadoop, Hive, Glue, ...). */
    ICEBERG,

    /** Delta Lake (storage-only) tables. */
    DELTA,

    /** Delta Lake managed by a Unity Catalog. */
    DELTA_UC,

    /** ClickHouse table store. */
    CLICKHOUSE,

    /**
     * No external catalog. Marks a managed-only materialization (SBT / Ursa protocol): the stream is
     * compacted into topic-grouped parquet Compacted Objects by the internal managed writer, with no
     * external table sink. There is no {@code TableMaterializerFactory} for this type — the dispatch
     * path builds only the managed writer.
     */
    NONE
}
