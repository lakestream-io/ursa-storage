/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.lakehouse.iceberg;

import static io.lakestream.ursa.lakehouse.LakehouseConfiguration.CHECK_NULLABILITY;
import static io.lakestream.ursa.lakehouse.LakehouseConfiguration.CHECK_ORDERING;
import static io.lakestream.ursa.lakehouse.LakehouseConfiguration.ICEBERG_PREFIX;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.lakestream.ursa.lakehouse.LakehouseConfiguration;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.stream.Stream;
import org.apache.iceberg.Schema;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.inmemory.InMemoryCatalog;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Comprehensive tests for incompatible schema evolution scenarios.
 * Tests various combinations of nullability and ordering checks.
 */
public class IcebergTableSchemaIncompatibleTest {

    private Catalog catalog;
    private LakehouseConfiguration configuration;
    private TableIdentifier tableIdentifier;
    private IcebergTable icebergTable;

    @BeforeEach
    public void setUp() {
        catalog = new InMemoryCatalog();
        catalog.initialize("test-catalog", new HashMap<>());

        Namespace namespace = Namespace.of("test_namespace");
        if (catalog instanceof org.apache.iceberg.catalog.SupportsNamespaces) {
            ((org.apache.iceberg.catalog.SupportsNamespaces) catalog).createNamespace(namespace);
        }

        tableIdentifier = TableIdentifier.of(namespace, "test_incompatible_table");
    }

    @AfterEach
    public void tearDown() {
        if (icebergTable != null) {
            icebergTable.close();
        }
    }

    // ========== Incompatible Type Changes ==========

    @Test
    public void testIncompatibleTypeChange_StringToInt() {
        // String to Int is incompatible
        configuration = createConfiguration(true, true);

        Schema initialSchema = new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.optional(2, "value", Types.StringType.get())
        );

        TableOptions tableOptions = createTableOptions(initialSchema);
        icebergTable = new IcebergTable(catalog, tableIdentifier, tableOptions, configuration);
        icebergTable.create(tableOptions);

        Schema incompatibleSchema = new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.optional(2, "value", Types.IntegerType.get())
        );

        // Should throw exception due to incompatible type change
        assertThrows(Exception.class, () ->
            icebergTable.updateTableSchemaIfNeeded(incompatibleSchema)
        );
    }

    @Test
    public void testIncompatibleTypeChange_LongToInt() {
        // Long to Int is incompatible (narrowing)
        configuration = createConfiguration(true, true);

        Schema initialSchema = new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.optional(2, "count", Types.LongType.get())
        );

        TableOptions tableOptions = createTableOptions(initialSchema);
        icebergTable = new IcebergTable(catalog, tableIdentifier, tableOptions, configuration);
        icebergTable.create(tableOptions);

        Schema incompatibleSchema = new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.optional(2, "count", Types.IntegerType.get())
        );

        assertThrows(Exception.class, () ->
            icebergTable.updateTableSchemaIfNeeded(incompatibleSchema)
        );
    }

    @Test
    public void testIncompatibleTypeChange_DoubleToFloat() {
        // Double to Float is incompatible (narrowing)
        configuration = createConfiguration(true, true);

        Schema initialSchema = new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.optional(2, "price", Types.DoubleType.get())
        );

        TableOptions tableOptions = createTableOptions(initialSchema);
        icebergTable = new IcebergTable(catalog, tableIdentifier, tableOptions, configuration);
        icebergTable.create(tableOptions);

        Schema incompatibleSchema = new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.optional(2, "price", Types.FloatType.get())
        );

        assertThrows(Exception.class, () ->
            icebergTable.updateTableSchemaIfNeeded(incompatibleSchema)
        );
    }

    @Test
    public void testIncompatibleDecimalChange_LowerPrecision() {
        // Decreasing precision is incompatible
        configuration = createConfiguration(true, true);

        Schema initialSchema = new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.optional(2, "amount", Types.DecimalType.of(15, 2))
        );

        TableOptions tableOptions = createTableOptions(initialSchema);
        icebergTable = new IcebergTable(catalog, tableIdentifier, tableOptions, configuration);
        icebergTable.create(tableOptions);

        Schema incompatibleSchema = new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.optional(2, "amount", Types.DecimalType.of(10, 2))
        );

        assertThrows(Exception.class, () ->
            icebergTable.updateTableSchemaIfNeeded(incompatibleSchema)
        );
    }

    @Test
    public void testIncompatibleDecimalChange_LowerScale() {
        // Decreasing scale is incompatible
        configuration = createConfiguration(true, true);

        Schema initialSchema = new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.optional(2, "amount", Types.DecimalType.of(10, 4))
        );

        TableOptions tableOptions = createTableOptions(initialSchema);
        icebergTable = new IcebergTable(catalog, tableIdentifier, tableOptions, configuration);
        icebergTable.create(tableOptions);

        Schema incompatibleSchema = new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.optional(2, "amount", Types.DecimalType.of(10, 2))
        );

        assertThrows(Exception.class, () ->
            icebergTable.updateTableSchemaIfNeeded(incompatibleSchema)
        );
    }

    @Test
    public void testIncompatibleTypeChange_StructToString() {
        // Struct to String is incompatible
        configuration = createConfiguration(true, true);

        Schema initialSchema = new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.optional(2, "data", Types.StructType.of(
                Types.NestedField.optional(3, "name", Types.StringType.get())
            ))
        );

        TableOptions tableOptions = createTableOptions(initialSchema);
        icebergTable = new IcebergTable(catalog, tableIdentifier, tableOptions, configuration);
        icebergTable.create(tableOptions);

        Schema incompatibleSchema = new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.optional(2, "data", Types.StringType.get())
        );

        assertThrows(Exception.class, () ->
            icebergTable.updateTableSchemaIfNeeded(incompatibleSchema)
        );
    }

    // ========== Nullability Tests with Different Configurations ==========

    private static Stream<Arguments> nullabilityOrderingCombinations() {
        return Stream.of(
            Arguments.of(true, true),   // Both enabled
            Arguments.of(true, false),  // Only nullability
            Arguments.of(false, true),  // Only ordering
            Arguments.of(false, false)  // Both disabled
        );
    }

    @ParameterizedTest
    @MethodSource("nullabilityOrderingCombinations")
    public void testOptionalToRequired_DifferentConfigs(boolean checkNullability, boolean checkOrdering)
        throws Exception {
        configuration = createConfiguration(checkNullability, checkOrdering);

        Schema initialSchema = new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.optional(2, "name", Types.StringType.get())
        );

        TableOptions tableOptions = createTableOptions(initialSchema);
        icebergTable = new IcebergTable(catalog, tableIdentifier, tableOptions, configuration);
        icebergTable.create(tableOptions);

        Schema newSchema = new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.required(2, "name", Types.StringType.get())
        );

        if (checkNullability) {
            // Should fail when nullability check is enabled
            assertThrows(Exception.class, () ->
                icebergTable.updateTableSchemaIfNeeded(newSchema)
            );
        } else {
            // Should succeed (but Iceberg still won't allow it at runtime)
            // The method will log a warning but not enforce

            icebergTable.updateTableSchemaIfNeeded(newSchema);
            Schema updated = icebergTable.getTable().schema();
            assertNotNull(updated.findField("name"));
            assertTrue(updated.findField("name").isOptional());
        }
    }

    @ParameterizedTest
    @MethodSource("nullabilityOrderingCombinations")
    public void testAddRequiredField_DifferentConfigs(boolean checkNullability, boolean checkOrdering) {
        configuration = createConfiguration(checkNullability, checkOrdering);

        Schema initialSchema = new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.optional(2, "name", Types.StringType.get())
        );

        TableOptions tableOptions = createTableOptions(initialSchema);
        icebergTable = new IcebergTable(catalog, tableIdentifier, tableOptions, configuration);
        icebergTable.create(tableOptions);

        Schema newSchema = new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.optional(2, "name", Types.StringType.get()),
            Types.NestedField.required(3, "email", Types.StringType.get())
        );

        // Should fail - can't add required field to existing table
        assertThrows(Exception.class, () ->
            icebergTable.updateTableSchemaIfNeeded(newSchema)
        );
    }

    @ParameterizedTest
    @MethodSource("nullabilityOrderingCombinations")
    public void testNestedRequiredField_DifferentConfigs(boolean checkNullability, boolean checkOrdering) {
        configuration = createConfiguration(checkNullability, checkOrdering);

        Schema initialSchema = new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.optional(2, "user", Types.StructType.of(
                Types.NestedField.optional(3, "name", Types.StringType.get())
            ))
        );

        TableOptions tableOptions = createTableOptions(initialSchema);
        icebergTable = new IcebergTable(catalog, tableIdentifier, tableOptions, configuration);
        icebergTable.create(tableOptions);

        Schema newSchema = new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.optional(2, "user", Types.StructType.of(
                Types.NestedField.optional(3, "name", Types.StringType.get()),
                Types.NestedField.required(4, "email", Types.StringType.get())
            ))
        );

        assertThrows(Exception.class, () ->
            icebergTable.updateTableSchemaIfNeeded(newSchema)
        );
    }

    // ========== Field Ordering Tests ==========

    @ParameterizedTest
    @MethodSource("nullabilityOrderingCombinations")
    public void testFieldReordering_DifferentConfigs(boolean checkNullability, boolean checkOrdering)
        throws Exception {
        configuration = createConfiguration(checkNullability, checkOrdering);

        Schema initialSchema = new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.optional(2, "name", Types.StringType.get()),
            Types.NestedField.optional(3, "email", Types.StringType.get())
        );

        TableOptions tableOptions = createTableOptions(initialSchema);
        icebergTable = new IcebergTable(catalog, tableIdentifier, tableOptions, configuration);
        icebergTable.create(tableOptions);

        // Reorder fields
        Schema reorderedSchema = new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.optional(3, "email", Types.StringType.get()),
            Types.NestedField.optional(2, "name", Types.StringType.get())
        );

        // Should succeed - ordering not enforced
        icebergTable.updateTableSchemaIfNeeded(reorderedSchema);
        Schema updatedSchema = icebergTable.getTable().schema();
        assertNotNull(updatedSchema.findField("name"));
        assertNotNull(updatedSchema.findField("email"));
    }

    @ParameterizedTest
    @MethodSource("nullabilityOrderingCombinations")
    public void testNestedFieldReordering_DifferentConfigs(boolean checkNullability, boolean checkOrdering)
        throws Exception {
        configuration = createConfiguration(checkNullability, checkOrdering);

        Schema initialSchema = new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.optional(2, "user", Types.StructType.of(
                Types.NestedField.optional(3, "name", Types.StringType.get()),
                Types.NestedField.optional(4, "email", Types.StringType.get()),
                Types.NestedField.optional(5, "phone", Types.StringType.get())
            ))
        );

        TableOptions tableOptions = createTableOptions(initialSchema);
        icebergTable = new IcebergTable(catalog, tableIdentifier, tableOptions, configuration);
        icebergTable.create(tableOptions);

        // Reorder nested fields
        Schema reorderedSchema = new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.optional(2, "user", Types.StructType.of(
                Types.NestedField.optional(5, "phone", Types.StringType.get()),
                Types.NestedField.optional(3, "name", Types.StringType.get()),
                Types.NestedField.optional(4, "email", Types.StringType.get())
            ))
        );

        icebergTable.updateTableSchemaIfNeeded(reorderedSchema);
        Schema updatedSchema = icebergTable.getTable().schema();
        Types.StructType userStruct = updatedSchema.findField("user").type().asStructType();
        assertNotNull(userStruct.field("name"));
        assertNotNull(userStruct.field("email"));
        assertNotNull(userStruct.field("phone"));
    }

    // ========== Complex Incompatible Scenarios ==========

    @Test
    public void testMultipleIncompatibleChanges() {
        configuration = createConfiguration(true, true);

        Schema initialSchema = new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.optional(2, "name", Types.StringType.get()),
            Types.NestedField.optional(3, "count", Types.LongType.get())
        );

        TableOptions tableOptions = createTableOptions(initialSchema);
        icebergTable = new IcebergTable(catalog, tableIdentifier, tableOptions, configuration);
        icebergTable.create(tableOptions);

        // Multiple incompatible changes:
        // - Make optional required (name)
        // - Narrow type (count: long -> int)
        Schema incompatibleSchema = new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.required(2, "name", Types.StringType.get()),
            Types.NestedField.optional(3, "count", Types.IntegerType.get())
        );

        assertThrows(Exception.class, () ->
            icebergTable.updateTableSchemaIfNeeded(incompatibleSchema)
        );
    }

    @Test
    public void testIncompatibleNestedTypeChange() {
        configuration = createConfiguration(true, true);

        Schema initialSchema = new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.optional(2, "user", Types.StructType.of(
                Types.NestedField.optional(3, "age", Types.LongType.get())
            ))
        );

        TableOptions tableOptions = createTableOptions(initialSchema);
        icebergTable = new IcebergTable(catalog, tableIdentifier, tableOptions, configuration);
        icebergTable.create(tableOptions);

        // Incompatible nested type change
        Schema incompatibleSchema = new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.optional(2, "user", Types.StructType.of(
                Types.NestedField.optional(3, "age", Types.IntegerType.get())
            ))
        );

        assertThrows(Exception.class, () ->
            icebergTable.updateTableSchemaIfNeeded(incompatibleSchema)
        );
    }

    @Test
    public void testListElementTypeChange() {
        configuration = createConfiguration(true, true);

        Schema initialSchema = new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.optional(2, "tags", Types.ListType.ofOptional(3, Types.LongType.get()))
        );

        TableOptions tableOptions = createTableOptions(initialSchema);
        icebergTable = new IcebergTable(catalog, tableIdentifier, tableOptions, configuration);
        icebergTable.create(tableOptions);

        // Change list element type (incompatible)
        Schema incompatibleSchema = new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.optional(2, "tags", Types.ListType.ofOptional(3, Types.IntegerType.get()))
        );

        assertThrows(Exception.class, () ->
            icebergTable.updateTableSchemaIfNeeded(incompatibleSchema)
        );
    }

    @Test
    public void testMapValueTypeChange() {
        configuration = createConfiguration(true, true);

        Schema initialSchema = new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.optional(2, "metadata", Types.MapType.ofOptional(
                3, 4, Types.StringType.get(), Types.LongType.get()))
        );

        TableOptions tableOptions = createTableOptions(initialSchema);
        icebergTable = new IcebergTable(catalog, tableIdentifier, tableOptions, configuration);
        icebergTable.create(tableOptions);

        // Change map value type (incompatible)
        Schema incompatibleSchema = new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.optional(2, "metadata", Types.MapType.ofOptional(
                3, 4, Types.StringType.get(), Types.IntegerType.get()))
        );

        assertThrows(Exception.class, () ->
            icebergTable.updateTableSchemaIfNeeded(incompatibleSchema)
        );
    }

    // ========== Edge Cases ==========

    @Test
    public void testDeeplyNestedIncompatibleChange() {
        configuration = createConfiguration(true, true);

        Schema initialSchema = new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.optional(2, "level1", Types.StructType.of(
                Types.NestedField.optional(3, "level2", Types.StructType.of(
                    Types.NestedField.optional(4, "level3", Types.StructType.of(
                        Types.NestedField.optional(5, "value", Types.LongType.get())
                    ))
                ))
            ))
        );

        TableOptions tableOptions = createTableOptions(initialSchema);
        icebergTable = new IcebergTable(catalog, tableIdentifier, tableOptions, configuration);
        icebergTable.create(tableOptions);

        // Incompatible change in deeply nested field
        Schema incompatibleSchema = new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.optional(2, "level1", Types.StructType.of(
                Types.NestedField.optional(3, "level2", Types.StructType.of(
                    Types.NestedField.optional(4, "level3", Types.StructType.of(
                        Types.NestedField.optional(5, "value", Types.IntegerType.get())
                    ))
                ))
            ))
        );

        assertThrows(Exception.class, () ->
            icebergTable.updateTableSchemaIfNeeded(incompatibleSchema)
        );
    }

    @Test
    public void testCompatibleAndIncompatibleChangesMixed() {
        configuration = createConfiguration(true, true);

        Schema initialSchema = new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.optional(2, "count", Types.IntegerType.get()),
            Types.NestedField.optional(3, "price", Types.FloatType.get())
        );

        TableOptions tableOptions = createTableOptions(initialSchema);
        icebergTable = new IcebergTable(catalog, tableIdentifier, tableOptions, configuration);
        icebergTable.create(tableOptions);

        // Mix of compatible (int->long, float->double) and incompatible (add required)
        Schema mixedSchema = new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.optional(2, "count", Types.LongType.get()),
            Types.NestedField.optional(3, "price", Types.DoubleType.get()),
            Types.NestedField.required(4, "name", Types.StringType.get())
        );

        // Should fail due to incompatible change (adding required field)
        assertThrows(Exception.class, () ->
            icebergTable.updateTableSchemaIfNeeded(mixedSchema)
        );
    }

    @ParameterizedTest
    @MethodSource("nullabilityOrderingCombinations")
    public void testEmptySchemaUpdate_DifferentConfigs(boolean checkNullability, boolean checkOrdering)
        throws Exception {
        configuration = createConfiguration(checkNullability, checkOrdering);

        Schema initialSchema = new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get())
        );

        TableOptions tableOptions = createTableOptions(initialSchema);
        icebergTable = new IcebergTable(catalog, tableIdentifier, tableOptions, configuration);
        icebergTable.create(tableOptions);

        // Try to update with empty schema (only ID field)
        Schema emptySchema = new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get())
        );

        // Should succeed - no changes
        icebergTable.updateTableSchemaIfNeeded(emptySchema);
        assertEquals(1, icebergTable.getTable().schema().columns().size());
    }

    // ========== Decimal Edge Cases ==========

    @Test
    public void testDecimalPromotionWithFieldId() throws Exception {
        // Test that decimal promotion using delete+add maintains data integrity
        configuration = createConfiguration(true, true);

        Schema initialSchema = new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.optional(2, "amount", Types.DecimalType.of(10, 2))
        );

        TableOptions tableOptions = createTableOptions(initialSchema);
        icebergTable = new IcebergTable(catalog, tableIdentifier, tableOptions, configuration);
        icebergTable.create(tableOptions);

        int originalFieldId = icebergTable.getTable().schema().findField("amount").fieldId();

        // Promote decimal
        Schema newSchema = new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.optional(2, "amount", Types.DecimalType.of(15, 2))
        );

        icebergTable.updateTableSchemaIfNeeded(newSchema);

        // After delete+add, field ID will be different (this is the bug!)
        int newFieldId = icebergTable.getTable().schema().findField("amount").fieldId();

        assertEquals(originalFieldId, newFieldId);
    }

    @Test
    public void testNestedDecimalPromotion() throws Exception {
        configuration = createConfiguration(true, true);

        Schema initialSchema = new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.optional(2, "payment", Types.StructType.of(
                Types.NestedField.optional(3, "amount", Types.DecimalType.of(10, 2))
            ))
        );

        TableOptions tableOptions = createTableOptions(initialSchema);
        icebergTable = new IcebergTable(catalog, tableIdentifier, tableOptions, configuration);
        icebergTable.create(tableOptions);

        Schema newSchema = new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.optional(2, "payment", Types.StructType.of(
                Types.NestedField.optional(3, "amount", Types.DecimalType.of(15, 2))
            ))
        );

        // Should succeed (but with field ID change issue)
        icebergTable.updateTableSchemaIfNeeded(newSchema);

        Schema updatedSchema = icebergTable.getTable().schema();
        Types.StructType paymentStruct = updatedSchema.findField("payment").type().asStructType();
        Types.DecimalType amountType = (Types.DecimalType) paymentStruct.field("amount").type();
        assertEquals(15, amountType.precision());
    }

    // ========== Helper Methods ==========

    private TableOptions createTableOptions(Schema schema) {
        return TableOptions.builder().schema(schema).build();
    }

    private LakehouseConfiguration createConfiguration(boolean checkNullability, boolean checkOrdering) {
        Properties properties = new Properties();
        properties.setProperty("cluster", "test-cluster");
        properties.setProperty(ICEBERG_PREFIX + CHECK_NULLABILITY, String.valueOf(checkNullability));
        properties.setProperty(ICEBERG_PREFIX + CHECK_ORDERING, String.valueOf(checkOrdering));

        return new LakehouseConfiguration(properties) {
            @Override
            public Properties getProperties() {
                return properties;
            }

            @Override
            public boolean checkIcebergNullability() {
                return checkNullability;
            }

            @Override
            public boolean checkIcebergOrdering() {
                return checkOrdering;
            }

            @Override
            public boolean makeNewFieldsOptionalOnEvolution() {
                // This suite verifies strict-mode (DLT) behavior; the optional-downgrade
                // feature is covered by IcebergTableMakeNewFieldsOptionalTest.
                return false;
            }

            @Override
            public String getIcebergCatalogType(Optional<String> catalogName) {
                return "inmemory";
            }

            @Override
            public IcebergCatalogBackendType getIcebergCatalogBackendType(Optional<String> catalogName) {
                return IcebergCatalogBackendType.TABULAR;
            }

            @Override
            public Map<String, String> getIcebergTableProperties() {
                return new HashMap<>();
            }

            @Override
            public Optional<String> getCatalogName() {
                return Optional.of("test-catalog");
            }

            @Override
            public Duration getCatalogMaxOpenTime() {
                return Duration.ofMinutes(5);
            }

            @Override
            public int getIcebergSnapshotExpirationInterval() {
                return 3600;
            }

            @Override
            public String getBucketPath() {
                return "s3://test-bucket/";
            }
        };
    }
}
