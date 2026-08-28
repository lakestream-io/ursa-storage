/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.lakehouse.delta;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;

import io.delta.kernel.types.LongType;
import io.delta.kernel.types.StringType;
import io.delta.kernel.types.StructField;
import io.delta.kernel.types.StructType;
import io.lakestream.ursa.lakehouse.utils.LakehouseFieldNames;
import java.util.List;
import org.junit.jupiter.api.Test;

class DeltaTableUtilsNormalizeTest {

    @Test
    void testNormalizeS3Location() {
        assertEquals("s3a://bucket/path", DeltaTableUtils.normalizeStorageLocation("s3://bucket/path"));
    }

    @Test
    void testNormalizeS3aLocationUnchanged() {
        assertEquals("s3a://bucket/path", DeltaTableUtils.normalizeStorageLocation("s3a://bucket/path"));
    }

    @Test
    void testNormalizeAbfssLocationUnchanged() {
        assertEquals("abfss://container@account.dfs.core.windows.net/path",
            DeltaTableUtils.normalizeStorageLocation("abfss://container@account.dfs.core.windows.net/path"));
    }

    @Test
    void testNormalizeNullReturnsNull() {
        assertNull(DeltaTableUtils.normalizeStorageLocation(null));
    }

    @Test
    void testNormalizeEmptyStringUnchanged() {
        assertEquals("", DeltaTableUtils.normalizeStorageLocation(""));
    }

    @Test
    void testRemoveMetaColumnDropsMetaField() {
        StructType schema = new StructType(List.of(
            new StructField("id", LongType.LONG, false),
            new StructField(LakehouseFieldNames.META, StringType.STRING, true),
            new StructField("name", StringType.STRING, true)));

        StructType filteredSchema = DeltaTableUtils.removeMetaColumn(schema);

        assertEquals(List.of("id", "name"), filteredSchema.fieldNames());
    }

    @Test
    void testRemoveMetaColumnKeepsSchemaWhenMetaIsAbsent() {
        StructType schema = new StructType(List.of(
            new StructField("id", LongType.LONG, false),
            new StructField("name", StringType.STRING, true)));

        StructType filteredSchema = DeltaTableUtils.removeMetaColumn(schema);

        assertSame(schema, filteredSchema);
    }
}
