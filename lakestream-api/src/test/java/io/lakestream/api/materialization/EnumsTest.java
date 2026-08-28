/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.api.materialization;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

/**
 * Verifies the value counts of every enum defined by the materialization API
 * to guard against accidental additions/removals.
 */
class EnumsTest {

    @Test
    void testTableCatalogType() {
        assertEquals(5, TableCatalogType.values().length);
        assertEquals(TableCatalogType.ICEBERG, TableCatalogType.valueOf("ICEBERG"));
        assertEquals(TableCatalogType.DELTA, TableCatalogType.valueOf("DELTA"));
        assertEquals(TableCatalogType.DELTA_UC, TableCatalogType.valueOf("DELTA_UC"));
        assertEquals(TableCatalogType.CLICKHOUSE, TableCatalogType.valueOf("CLICKHOUSE"));
    }

    @Test
    void testWriteMode() {
        assertEquals(3, WriteMode.values().length);
        assertEquals(WriteMode.APPEND, WriteMode.valueOf("APPEND"));
        assertEquals(WriteMode.UPSERT, WriteMode.valueOf("UPSERT"));
        assertEquals(WriteMode.CDC, WriteMode.valueOf("CDC"));
    }

    @Test
    void testStartPosition() {
        assertEquals(4, StartPosition.values().length);
        assertEquals(StartPosition.EARLIEST, StartPosition.valueOf("EARLIEST"));
        assertEquals(StartPosition.LATEST, StartPosition.valueOf("LATEST"));
        assertEquals(StartPosition.OFFSET, StartPosition.valueOf("OFFSET"));
        assertEquals(StartPosition.TIMESTAMP, StartPosition.valueOf("TIMESTAMP"));
    }

    @Test
    void testTableMode() {
        assertEquals(3, TableMode.values().length);
        assertEquals(TableMode.MANAGED, TableMode.valueOf("MANAGED"));
        assertEquals(TableMode.EXTERNAL, TableMode.valueOf("EXTERNAL"));
        assertEquals(TableMode.CUSTOM, TableMode.valueOf("CUSTOM"));
    }

    @Test
    void testCompression() {
        assertEquals(5, Compression.values().length);
        assertEquals(Compression.ZSTD, Compression.valueOf("ZSTD"));
        assertEquals(Compression.SNAPPY, Compression.valueOf("SNAPPY"));
        assertEquals(Compression.GZIP, Compression.valueOf("GZIP"));
        assertEquals(Compression.LZ4, Compression.valueOf("LZ4"));
        assertEquals(Compression.UNCOMPRESSED, Compression.valueOf("UNCOMPRESSED"));
    }

    @Test
    void testMaterializationState() {
        assertEquals(5, MaterializationState.values().length);
        assertEquals(MaterializationState.PENDING, MaterializationState.valueOf("PENDING"));
        assertEquals(MaterializationState.RUNNING, MaterializationState.valueOf("RUNNING"));
        assertEquals(MaterializationState.DEGRADED, MaterializationState.valueOf("DEGRADED"));
        assertEquals(MaterializationState.SUSPENDED, MaterializationState.valueOf("SUSPENDED"));
        assertEquals(MaterializationState.PAUSED, MaterializationState.valueOf("PAUSED"));
    }

    @Test
    void testErrorMode() {
        assertEquals(3, ErrorMode.values().length);
        assertEquals(ErrorMode.SUSPEND, ErrorMode.valueOf("SUSPEND"));
        assertEquals(ErrorMode.SKIP, ErrorMode.valueOf("SKIP"));
        assertEquals(ErrorMode.LOG, ErrorMode.valueOf("LOG"));
    }

    @Test
    void testPartitionTransform() {
        assertEquals(8, PartitionTransform.values().length);
        assertEquals(PartitionTransform.IDENTITY, PartitionTransform.valueOf("IDENTITY"));
        assertEquals(PartitionTransform.BUCKET, PartitionTransform.valueOf("BUCKET"));
        assertEquals(PartitionTransform.TRUNCATE, PartitionTransform.valueOf("TRUNCATE"));
        assertEquals(PartitionTransform.YEAR, PartitionTransform.valueOf("YEAR"));
        assertEquals(PartitionTransform.MONTH, PartitionTransform.valueOf("MONTH"));
        assertEquals(PartitionTransform.DAY, PartitionTransform.valueOf("DAY"));
        assertEquals(PartitionTransform.HOUR, PartitionTransform.valueOf("HOUR"));
        assertEquals(PartitionTransform.EXPRESSION, PartitionTransform.valueOf("EXPRESSION"));
    }

    @Test
    void testSortDirection() {
        assertEquals(2, SortDirection.values().length);
        assertEquals(SortDirection.ASC, SortDirection.valueOf("ASC"));
        assertEquals(SortDirection.DESC, SortDirection.valueOf("DESC"));
    }
}
