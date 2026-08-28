/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.lakehouse.delta;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.delta.kernel.types.ArrayType;
import io.delta.kernel.types.LongType;
import io.delta.kernel.types.MapType;
import io.delta.kernel.types.StringType;
import io.delta.kernel.types.StructField;
import io.delta.kernel.types.StructType;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag("lakehouse")
class CustomColumnMappingTest {

    private StructType oldSchema() {
        return new StructType()
            .add(new StructField("id", LongType.LONG, false))
            .add(new StructField("details", new StructType()
                .add(new StructField("category", StringType.STRING, true)), true));
    }

    private StructType newSchema() {
        return new StructType()
            .add(new StructField("id", LongType.LONG, false))
            .add(new StructField("details", new StructType()
                .add(new StructField("category", StringType.STRING, true))
                .add(new StructField("processing_notes",
                    new ArrayType(StringType.STRING, false), false)), true))
            .add(new StructField("audit_trail",
                new ArrayType(StringType.STRING, false), false));
    }

    @Test
    void newFieldsForcedNullableWhenFlagOn() {
        StructType result = CustomColumnMapping.assignColumnIdAndPhysicalNameForTableEvolution(
            oldSchema(), newSchema(), new AtomicInteger(2), false, true);

        assertTrue(result.get("audit_trail").isNullable());
        StructType details = (StructType) result.get("details").getDataType();
        assertTrue(details.get("processing_notes").isNullable());
        assertFalse(result.get("id").isNullable());
    }

    @Test
    void newRequiredFieldStaysNonNullableWhenFlagOff() {
        StructType result = CustomColumnMapping.assignColumnIdAndPhysicalNameForTableEvolution(
            oldSchema(), newSchema(), new AtomicInteger(2), false, false);

        assertFalse(result.get("audit_trail").isNullable());
        StructType details = (StructType) result.get("details").getDataType();
        assertFalse(details.get("processing_notes").isNullable());
    }

    @Test
    void softDeletedFieldReappendedAsNullable() {
        StructType oldWithExtra = new StructType()
            .add(new StructField("id", LongType.LONG, false))
            .add(new StructField("legacy", LongType.LONG, false));
        StructType withoutLegacy = new StructType()
            .add(new StructField("id", LongType.LONG, false));

        StructType result = CustomColumnMapping.assignColumnIdAndPhysicalNameForTableEvolution(
            oldWithExtra, withoutLegacy, new AtomicInteger(2), true, true);

        // soft-delete is on: the removed "legacy" field is re-appended, made nullable.
        assertTrue(result.get("legacy").isNullable());
        assertTrue(result.get("id") != null);
    }

    @Test
    void assignsColumnMappingMetadataToNewNestedFieldsInsideExistingArrayAndMap() {
        StructType itemV1 = new StructType()
            .add(new StructField("raised", LongType.LONG, true));
        StructType valueV1 = new StructType()
            .add(new StructField("raised", LongType.LONG, true));
        StructType oldSchemaWithMetadata = CustomColumnMapping.assignColumnIdAndPhysicalNameForCreateTable(
            new StructType()
                .add(new StructField("items", new ArrayType(itemV1, true), true))
                .add(new StructField("entries", new MapType(StringType.STRING, valueV1, true), true)),
            new AtomicInteger(0));

        StructType itemV2 = new StructType()
            .add(new StructField("raised", LongType.LONG, true))
            .add(new StructField("status", StringType.STRING, false));
        StructType valueV2 = new StructType()
            .add(new StructField("raised", LongType.LONG, true))
            .add(new StructField("status", StringType.STRING, false));
        StructType newSchema = new StructType()
            .add(new StructField("items", new ArrayType(itemV2, true), true))
            .add(new StructField("entries", new MapType(StringType.STRING, valueV2, true), true));

        StructType result = CustomColumnMapping.assignColumnIdAndPhysicalNameForTableEvolution(
            oldSchemaWithMetadata, newSchema, new AtomicInteger(4), false, true);

        ArrayType resultItems = (ArrayType) result.get("items").getDataType();
        StructType resultItem = (StructType) resultItems.getElementType();
        StructField itemStatus = resultItem.get("status");
        assertTrue(itemStatus.isNullable());
        assertTrue(CustomColumnMapping.hasColumnId(itemStatus));
        assertTrue(CustomColumnMapping.hasPhysicalName(itemStatus));

        MapType resultEntries = (MapType) result.get("entries").getDataType();
        StructType resultValue = (StructType) resultEntries.getValueType();
        StructField valueStatus = resultValue.get("status");
        assertTrue(valueStatus.isNullable());
        assertTrue(CustomColumnMapping.hasColumnId(valueStatus));
        assertTrue(CustomColumnMapping.hasPhysicalName(valueStatus));
    }
}
