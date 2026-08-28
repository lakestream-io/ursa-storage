/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.lakehouse.iceberg;

import static io.lakestream.ursa.lakehouse.LakehouseConfiguration.FIXED_PARTITION_KEY;
import static io.lakestream.ursa.lakehouse.LakehouseConfiguration.NONE_PARTITION_KEY;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag("lakehouse")
public class TableOptionsTest {

    private Schema createTestSchema() {
        return new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.required(2, "name", Types.StringType.get()),
            Types.NestedField.optional(3, "age", Types.IntegerType.get()),
            Types.NestedField.optional(4, "email", Types.StringType.get()),
            Types.NestedField.required(5, "created_at", Types.TimestampType.withZone())
        );
    }

    @Test
    void testBasicTableOptionsCreation() {
        Schema schema = createTestSchema();

        TableOptions options = TableOptions.builder()
            .schema(schema)
            .location("/tmp/test-table")
            .build();

        assertNotNull(options.getSchema());
        assertEquals("/tmp/test-table", options.getLocation());
        assertEquals("", options.getPartitionKey());
        assertTrue(options.getProperties().isEmpty());
        assertTrue(options.getIdentifierFields().isEmpty());
    }

    @Test
    void testSchemaWithIdentifierFields() {
        Schema schema = createTestSchema();
        Set<String> identifierFields = Set.of("id", "name");

        TableOptions options = TableOptions.builder()
            .schema(schema)
            .identifierFields(identifierFields)
            .build();

        Schema processedSchema = options.getSchema();

        assertTrue(processedSchema.identifierFieldIds().contains(1));
        assertTrue(processedSchema.identifierFieldIds().contains(2));
        assertFalse(processedSchema.identifierFieldIds().contains(3));

        assertEquals(schema.asStruct().fields().size(), processedSchema.asStruct().fields().size());
    }

    @Test
    void testIdentifierFieldsOnlyRequired() {
        Schema schema = createTestSchema();
        Set<String> identifierFields = Set.of("id", "age");

        TableOptions options = TableOptions.builder()
            .schema(schema)
            .identifierFields(identifierFields)
            .build();

        Schema processedSchema = options.getSchema();

        assertTrue(processedSchema.identifierFieldIds().contains(1));
        assertFalse(processedSchema.identifierFieldIds().contains(3));
    }

    @Test
    void testNonExistentIdentifierFields() {
        Schema schema = createTestSchema();
        Set<String> identifierFields = Set.of("id", "non_existent_field");

        TableOptions options = TableOptions.builder()
            .schema(schema)
            .identifierFields(identifierFields)
            .build();

        Schema processedSchema = options.getSchema();

        assertTrue(processedSchema.identifierFieldIds().contains(1));
        assertEquals(1, processedSchema.identifierFieldIds().size());
    }

    @Test
    void testPartitionSpecWithNoneKey() {
        Schema schema = createTestSchema();

        TableOptions options = TableOptions.builder()
            .schema(schema)
            .partitionKey(NONE_PARTITION_KEY)
            .build();

        IcebergPartitionSpec partitionSpec = options.getPartitionSpec();
        assertNotNull(partitionSpec);
    }

    @Test
    void testPartitionSpecWithFixedKey() {
        Schema schema = createTestSchema();

        TableOptions options = TableOptions.builder()
            .schema(schema)
            .partitionKey(FIXED_PARTITION_KEY)
            .build();

        IcebergPartitionSpec partitionSpec = options.getPartitionSpec();
        assertNotNull(partitionSpec);
    }

    @Test
    void testPartitionSpecWithJsonConfig() {
        Schema schema = createTestSchema();
        String jsonPartitionKey = "[{\"field\":\"created_at\",\"transform\":\"day\"}]";

        TableOptions options = TableOptions.builder()
            .schema(schema)
            .partitionKey(jsonPartitionKey)
            .build();

        IcebergPartitionSpec partitionSpec = options.getPartitionSpec();
        assertNotNull(partitionSpec);
    }

    @Test
    void testPartitionSpecWithInvalidJson() {
        Schema schema = createTestSchema();
        String invalidJsonPartitionKey = "invalid-json";

        TableOptions options = TableOptions.builder()
            .schema(schema)
            .partitionKey(invalidJsonPartitionKey)
            .build();

        IcebergPartitionSpec partitionSpec = options.getPartitionSpec();
        assertNotNull(partitionSpec);
    }

    @Test
    void testTableOptionsWithProperties() {
        Schema schema = createTestSchema();
        Map<String, String> properties = new HashMap<>();
        properties.put("write.format.default", "parquet");
        properties.put("write.metadata.compression-codec", "gzip");

        TableOptions options = TableOptions.builder()
            .schema(schema)
            .properties(properties)
            .build();

        assertEquals("parquet", options.getProperties().get("write.format.default"));
        assertEquals("gzip", options.getProperties().get("write.metadata.compression-codec"));
        assertEquals(2, options.getProperties().size());
    }

    @Test
    void testComplexSchema() {
        Schema complexSchema = new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.optional(2, "data", Types.StructType.of(
                Types.NestedField.required(3, "value", Types.StringType.get()),
                Types.NestedField.optional(4, "metadata", Types.MapType.ofRequired(5, 6,
                    Types.StringType.get(), Types.StringType.get()))
            )),
            Types.NestedField.optional(7, "tags", Types.ListType.ofRequired(8,
                Types.StringType.get())),
            Types.NestedField.required(9, "timestamp", Types.TimestampType.withZone())
        );

        TableOptions options = TableOptions.builder()
            .schema(complexSchema)
            .identifierFields(Set.of("id"))
            .partitionKey("[{\"field\":\"timestamp\",\"transform\":\"hour\"}]")
            .build();

        Schema processedSchema = options.getSchema();
        assertNotNull(processedSchema);
        assertTrue(processedSchema.identifierFieldIds().contains(1));

        IcebergPartitionSpec partitionSpec = options.getPartitionSpec();
        assertNotNull(partitionSpec);
    }

    @Test
    void testNullSchemaThrowsException() {
        assertThrows(IllegalArgumentException.class, () -> {
            TableOptions.builder()
                .schema(null)
                .build();
        });
    }

    @Test
    void testDefaultValues() {
        Schema schema = createTestSchema();

        TableOptions options = TableOptions.builder()
            .schema(schema)
            .build();

        assertNotNull(options.getProperties());
        assertTrue(options.getProperties().isEmpty());
        assertNotNull(options.getIdentifierFields());
        assertTrue(options.getIdentifierFields().isEmpty());
        assertEquals("", options.getPartitionKey());
        assertNull(options.getLocation());
    }

    @Test
    void testSchemaFieldAccess() {
        Schema schema = createTestSchema();

        TableOptions options = TableOptions.builder()
            .schema(schema)
            .build();

        Schema retrievedSchema = options.getSchema();

        assertEquals(5, retrievedSchema.asStruct().fields().size());
        assertNotNull(retrievedSchema.findField("id"));
        assertNotNull(retrievedSchema.findField("name"));
        assertNotNull(retrievedSchema.findField("created_at"));
        assertEquals(Types.LongType.get(), retrievedSchema.findType("id"));
        assertEquals(Types.StringType.get(), retrievedSchema.findType("name"));
    }
}
