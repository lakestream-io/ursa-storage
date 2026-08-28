/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.lakehouse.iceberg;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.HashMap;
import java.util.Map;
import org.apache.iceberg.Schema;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;

class IcebergTableUtilsTest {

    // =================================================================================
    // Test: isV2TableFormat
    // =================================================================================

    @Test
    void testIsV2TableFormat_V1() {
        Map<String, String> props = new HashMap<>();
        props.put(TableProperties.FORMAT_VERSION, "1");
        assertTrue(IcebergTableUtils.isV2TableFormat(props), "Version 1 should be considered V2-compatible/legacy");
    }

    @Test
    void testIsV2TableFormat_V2() {
        Map<String, String> props = new HashMap<>();
        props.put(TableProperties.FORMAT_VERSION, "2");
        assertTrue(IcebergTableUtils.isV2TableFormat(props), "Version 2 should return true");
    }

    @Test
    void testIsV2TableFormat_V3() {
        Map<String, String> props = new HashMap<>();
        props.put(TableProperties.FORMAT_VERSION, "3");
        assertFalse(IcebergTableUtils.isV2TableFormat(props), "Version 3 should return false");
    }

    @Test
    void testIsV2TableFormat_Missing() {
        Map<String, String> props = new HashMap<>();
        // No FORMAT_VERSION key
        assertTrue(IcebergTableUtils.isV2TableFormat(props), "Missing version should default to true (V2)");
    }

    @Test
    void testIsV2TableFormat_Invalid() {
        Map<String, String> props = new HashMap<>();
        props.put(TableProperties.FORMAT_VERSION, "invalid_text");
        assertTrue(IcebergTableUtils.isV2TableFormat(props), "Invalid version format should fallback to true");
    }

    // =================================================================================
    // Test: containsV3Types (Standard Types)
    // =================================================================================

    @Test
    void testContainsV3Types_StandardTypesOnly() {
        Schema schema = new Schema(
                Types.NestedField.required(1, "id", Types.IntegerType.get()),
                Types.NestedField.optional(2, "data", Types.StringType.get()),
                Types.NestedField.optional(3, "ts", Types.TimestampType.withZone()), // Standard V1/V2 timestamp
                Types.NestedField.optional(4, "is_active", Types.BooleanType.get())
        );

        assertFalse(IcebergTableUtils.containsV3Types(schema), "Schema with standard types should not contain V3 types");
    }

    // =================================================================================
    // Test: containsV3Types (V3 Primitive Types)
    // =================================================================================

    @Test
    void testContainsV3Types_TimestampNano() {
        Schema schema = new Schema(
                Types.NestedField.required(1, "ts_nano", Types.TimestampNanoType.withoutZone())
        );
        assertTrue(IcebergTableUtils.containsV3Types(schema), "Should detect TimestampNanoType");
    }

    // Depending on your Iceberg version, ensure TimestamptzNanoType exists
    // If not, you can comment this specific test out.
    @Test
    void testContainsV3Types_TimestamptzNano() {
        // Note: Check if your iceberg-api has Types.TimestamptzNanoType
        try {
            Schema schema = new Schema(
                    Types.NestedField.required(1, "ts_tz_nano", Types.TimestampNanoType.withZone())
                    // Replace with Types.TimestamptzNanoType.get() if available
            );
            assertTrue(IcebergTableUtils.containsV3Types(schema));
        } catch (NoClassDefFoundError | NoSuchMethodError e) {
            // Ignore if type not present in local test env
        }
    }

    @Test
    void testContainsV3Types_Variant() {
        Schema schema = new Schema(
                Types.NestedField.required(1, "payload", Types.VariantType.get())
        );
        assertTrue(IcebergTableUtils.containsV3Types(schema), "Should detect VariantType");
    }

    @Test
    void testContainsV3Types_Unknown() {
        Schema schema = new Schema(
                Types.NestedField.optional(1, "unknown_col", Types.UnknownType.get())
        );
        assertTrue(IcebergTableUtils.containsV3Types(schema), "Should detect UnknownType");
    }

    // =================================================================================
    // Test: containsV3Types (Nested Structures)
    // =================================================================================

    @Test
    void testContainsV3Types_NestedInStruct() {
        // Struct -> Variant
        Schema schema = new Schema(
                Types.NestedField.required(1, "user", Types.StructType.of(
                        Types.NestedField.required(2, "name", Types.StringType.get()),
                        Types.NestedField.optional(3, "metadata", Types.VariantType.get()) // V3 here
                ))
        );
        assertTrue(IcebergTableUtils.containsV3Types(schema), "Should detect V3 type nested in Struct");
    }

    @Test
    void testContainsV3Types_NestedInList() {
        // List<Variant>
        Schema schema = new Schema(
                Types.NestedField.required(1, "events", Types.ListType.ofRequired(2, Types.VariantType.get()))
        );
        assertTrue(IcebergTableUtils.containsV3Types(schema), "Should detect V3 type nested in List");
    }

    @Test
    void testContainsV3Types_NestedInMapKey() {
        // Map<TimestampNano, String>
        Schema schema = new Schema(
                Types.NestedField.required(1, "time_map", Types.MapType.ofRequired(
                        2, 3,
                        Types.TimestampNanoType.withoutZone(), // Key is V3
                        Types.StringType.get()
                ))
        );
        assertTrue(IcebergTableUtils.containsV3Types(schema), "Should detect V3 type in Map Key");
    }

    @Test
    void testContainsV3Types_NestedInMapValue() {
        // Map<String, Variant>
        Schema schema = new Schema(
                Types.NestedField.required(1, "attributes", Types.MapType.ofRequired(
                        2, 3,
                        Types.StringType.get(),
                        Types.VariantType.get() // Value is V3
                ))
        );
        assertTrue(IcebergTableUtils.containsV3Types(schema), "Should detect V3 type in Map Value");
    }

    @Test
    void testContainsV3Types_DeeplyNested() {
        // Struct -> List -> Struct -> Map -> Variant
        // A complex nested hierarchy to ensure the visitor traverses fully

        Types.MapType mapWithVariant = Types.MapType.ofRequired(10, 11, Types.StringType.get(), Types.VariantType.get());
        Types.StructType innerStruct = Types.StructType.of(Types.NestedField.required(9, "inner_map", mapWithVariant));
        Types.ListType listStruct = Types.ListType.ofRequired(8, innerStruct);

        Schema schema = new Schema(
                Types.NestedField.required(1, "complex_root", Types.StructType.of(
                        Types.NestedField.required(7, "deep_list", listStruct)
                ))
        );

        assertTrue(IcebergTableUtils.containsV3Types(schema), "Should detect deeply nested V3 types");
    }
}