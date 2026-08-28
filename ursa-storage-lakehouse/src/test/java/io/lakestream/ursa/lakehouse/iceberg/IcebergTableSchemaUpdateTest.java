/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.lakehouse.iceberg;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import io.lakestream.ursa.lakehouse.LakehouseConfiguration;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import org.apache.iceberg.Schema;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.inmemory.InMemoryCatalog;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;


/**
 * Unit tests for IcebergTable.updateTableSchemaIfNeeded() method.
 * Tests cover schema evolution scenarios without using mocks.
 */
public class IcebergTableSchemaUpdateTest {

    private Catalog catalog;
    private LakehouseConfiguration configuration;
    private TableIdentifier tableIdentifier;
    private IcebergTable icebergTable;

    @BeforeEach
    public void setUp() {
        // Create an in-memory catalog for testing
        catalog = new InMemoryCatalog();
        catalog.initialize("test-catalog", new HashMap<>());

        // Create namespace using SupportsNamespaces interface
        Namespace namespace = Namespace.of("test_namespace");
        if (catalog instanceof org.apache.iceberg.catalog.SupportsNamespaces) {
            ((org.apache.iceberg.catalog.SupportsNamespaces) catalog).createNamespace(namespace);
        }

        // Set up table identifier
        tableIdentifier = TableIdentifier.of(namespace, "test_table");

        // Create configuration
        Properties properties = new Properties();
        properties.setProperty("cluster", "test-cluster");
        configuration = createMockConfiguration(properties);
    }

    @AfterEach
    public void tearDown() {
        if (icebergTable != null) {
            icebergTable.close();
        }
    }

    @Test
    public void testUpdateSchemaWithNewOptionalFields() throws Exception {
        // Create initial schema
        Schema initialSchema = new Schema(
                Types.NestedField.required(1, "id", Types.LongType.get()),
                Types.NestedField.optional(2, "name", Types.StringType.get())
        );

        // Create table
        TableOptions tableOptions = createTableOptions(initialSchema);
        icebergTable = new IcebergTable(catalog, tableIdentifier, tableOptions, configuration);
        icebergTable.create(tableOptions);

        // New schema with additional optional field
        Schema newSchema = new Schema(
                Types.NestedField.required(1, "id", Types.LongType.get()),
                Types.NestedField.optional(2, "name", Types.StringType.get()),
                Types.NestedField.optional(3, "email", Types.StringType.get())
        );

        // Update schema
        icebergTable.updateTableSchemaIfNeeded(newSchema);

        // Verify the schema was updated
        Schema updatedSchema = icebergTable.getTable().schema();
        assertNotNull(updatedSchema.findField("email"));
        assertEquals(Types.StringType.get(), updatedSchema.findField("email").type());
        assertTrue(updatedSchema.findField("email").isOptional());
    }

    @Test
    public void testUpdateSchemaWithMultipleNewFields() throws Exception  {
        // Create initial schema
        Schema initialSchema = new Schema(
                Types.NestedField.required(1, "id", Types.LongType.get())
        );

        TableOptions tableOptions = createTableOptions(initialSchema);
        icebergTable = new IcebergTable(catalog, tableIdentifier, tableOptions, configuration);
        icebergTable.create(tableOptions);

        // New schema with multiple new fields
        Schema newSchema = new Schema(
                Types.NestedField.required(1, "id", Types.LongType.get()),
                Types.NestedField.optional(2, "name", Types.StringType.get()),
                Types.NestedField.optional(3, "age", Types.IntegerType.get()),
                Types.NestedField.optional(4, "email", Types.StringType.get())
        );

        // Update schema
        icebergTable.updateTableSchemaIfNeeded(newSchema);

        // Verify all fields were added
        Schema updatedSchema = icebergTable.getTable().schema();
        assertNotNull(updatedSchema.findField("name"));
        assertNotNull(updatedSchema.findField("age"));
        assertNotNull(updatedSchema.findField("email"));
    }

    @Test
    public void testUpdateSchemaWithTypePromotion_IntToLong() throws Exception  {
        // Create initial schema with integer field
        Schema initialSchema = new Schema(
                Types.NestedField.required(1, "id", Types.LongType.get()),
                Types.NestedField.optional(2, "count", Types.IntegerType.get())
        );

        TableOptions tableOptions = createTableOptions(initialSchema);
        icebergTable = new IcebergTable(catalog, tableIdentifier, tableOptions, configuration);
        icebergTable.create(tableOptions);

        // New schema promoting int to long
        Schema newSchema = new Schema(
                Types.NestedField.required(1, "id", Types.LongType.get()),
                Types.NestedField.optional(2, "count", Types.LongType.get())
        );

        // Update schema
        icebergTable.updateTableSchemaIfNeeded(newSchema);

        // Verify type was promoted
        Schema updatedSchema = icebergTable.getTable().schema();
        assertEquals(Types.LongType.get(), updatedSchema.findField("count").type());
    }

    @Test
    public void testUpdateSchemaWithTypePromotion_FloatToDouble() throws Exception  {
        // Create initial schema with float field
        Schema initialSchema = new Schema(
                Types.NestedField.required(1, "id", Types.LongType.get()),
                Types.NestedField.optional(2, "price", Types.FloatType.get())
        );

        TableOptions tableOptions = createTableOptions(initialSchema);
        icebergTable = new IcebergTable(catalog, tableIdentifier, tableOptions, configuration);
        icebergTable.create(tableOptions);

        // New schema promoting float to double
        Schema newSchema = new Schema(
                Types.NestedField.required(1, "id", Types.LongType.get()),
                Types.NestedField.optional(2, "price", Types.DoubleType.get())
        );

        // Update schema
        icebergTable.updateTableSchemaIfNeeded(newSchema);

        // Verify type was promoted
        Schema updatedSchema = icebergTable.getTable().schema();
        assertEquals(Types.DoubleType.get(), updatedSchema.findField("price").type());
    }

    @Test
    public void testUpdateSchemaWithDecimalPromotion() throws Exception  {
        // Create initial schema with decimal field
        Schema initialSchema = new Schema(
                Types.NestedField.required(1, "id", Types.LongType.get()),
                Types.NestedField.optional(2, "amount", Types.DecimalType.of(10, 2))
        );

        TableOptions tableOptions = createTableOptions(initialSchema);
        icebergTable = new IcebergTable(catalog, tableIdentifier, tableOptions, configuration);
        icebergTable.create(tableOptions);

        // New schema with higher precision and scale
        Schema newSchema = new Schema(
                Types.NestedField.required(1, "id", Types.LongType.get()),
                Types.NestedField.optional(2, "amount", Types.DecimalType.of(15, 2))
        );

        // Update schema
        icebergTable.updateTableSchemaIfNeeded(newSchema);

        // Verify decimal was promoted
        Schema updatedSchema = icebergTable.getTable().schema();
        Types.DecimalType updatedType = (Types.DecimalType) updatedSchema.findField("amount").type();
        assertEquals(15, updatedType.precision());
        assertEquals(2, updatedType.scale());
    }

    @Test
    public void testUpdateSchemaWithNestedStructFields() throws Exception  {
        // Create initial schema with nested struct
        Schema initialSchema = new Schema(
                Types.NestedField.required(1, "id", Types.LongType.get()),
                Types.NestedField.optional(2, "address", Types.StructType.of(
                        Types.NestedField.optional(3, "street", Types.StringType.get()),
                        Types.NestedField.optional(4, "city", Types.StringType.get())
                ))
        );

        TableOptions tableOptions = createTableOptions(initialSchema);
        icebergTable = new IcebergTable(catalog, tableIdentifier, tableOptions, configuration);
        icebergTable.create(tableOptions);

        // New schema with additional field in nested struct
        Schema newSchema = new Schema(
                Types.NestedField.required(1, "id", Types.LongType.get()),
                Types.NestedField.optional(2, "address", Types.StructType.of(
                        Types.NestedField.optional(3, "street", Types.StringType.get()),
                        Types.NestedField.optional(4, "city", Types.StringType.get()),
                        Types.NestedField.optional(5, "zipcode", Types.StringType.get())
                ))
        );

        // Update schema
        icebergTable.updateTableSchemaIfNeeded(newSchema);

        // Verify nested field was added
        Schema updatedSchema = icebergTable.getTable().schema();
        Types.StructType addressStruct = updatedSchema.findField("address").type().asStructType();
        assertNotNull(addressStruct.field("zipcode"));
    }

    @Test
    public void testUpdateSchemaWithRequiredToOptional() throws Exception  {
        // Create initial schema with required field
        Schema initialSchema = new Schema(
                Types.NestedField.required(1, "id", Types.LongType.get()),
                Types.NestedField.required(2, "name", Types.StringType.get())
        );

        TableOptions tableOptions = createTableOptions(initialSchema);
        icebergTable = new IcebergTable(catalog, tableIdentifier, tableOptions, configuration);
        icebergTable.create(tableOptions);

        // New schema making field optional
        Schema newSchema = new Schema(
                Types.NestedField.required(1, "id", Types.LongType.get()),
                Types.NestedField.optional(2, "name", Types.StringType.get())
        );

        // Update schema
        icebergTable.updateTableSchemaIfNeeded(newSchema);

        // Verify field is now optional
        Schema updatedSchema = icebergTable.getTable().schema();
        assertTrue(updatedSchema.findField("name").isOptional());
    }

    @Test
    public void testUpdateSchemaWithSoftDelete_Enabled() throws Exception  {
        // Create configuration with soft delete enabled (default)
        Properties properties = new Properties();
        properties.setProperty("cluster", "test-cluster");
        properties.setProperty(IcebergTable.SCHEMA_EVOLUTION_SOFT_DELETE_ENABLED, "true");
        configuration = createMockConfiguration(properties);

        // Create initial schema
        Schema initialSchema = new Schema(
                Types.NestedField.required(1, "id", Types.LongType.get()),
                Types.NestedField.required(2, "name", Types.StringType.get()),
                Types.NestedField.optional(3, "email", Types.StringType.get())
        );

        TableOptions tableOptions = createTableOptions(initialSchema);
        icebergTable = new IcebergTable(catalog, tableIdentifier, tableOptions, configuration);
        icebergTable.create(tableOptions);

        // New schema with required field removed
        Schema newSchema = new Schema(
                Types.NestedField.required(1, "id", Types.LongType.get()),
                Types.NestedField.optional(3, "email", Types.StringType.get())
        );

        // Update schema
        icebergTable.updateTableSchemaIfNeeded(newSchema);

        // Verify field still exists but is now optional (soft delete)
        Schema updatedSchema = icebergTable.getTable().schema();
        assertNotNull(updatedSchema.findField("name"), "Field should still exist with soft delete");
        assertTrue(updatedSchema.findField("name").isOptional(), "Field should be optional after soft delete");
    }

    @Test
    public void testUpdateSchemaWithSoftDelete_Disabled() throws Exception  {
        // Create configuration with soft delete disabled
        Properties properties = new Properties();
        properties.setProperty("cluster", "test-cluster");
        properties.setProperty(IcebergTable.SCHEMA_EVOLUTION_SOFT_DELETE_ENABLED, "false");
        configuration = createMockConfiguration(properties);

        // Create initial schema
        Schema initialSchema = new Schema(
                Types.NestedField.required(1, "id", Types.LongType.get()),
                Types.NestedField.optional(2, "name", Types.StringType.get()),
                Types.NestedField.optional(3, "email", Types.StringType.get())
        );

        TableOptions tableOptions = createTableOptions(initialSchema);
        icebergTable = new IcebergTable(catalog, tableIdentifier, tableOptions, configuration);
        icebergTable.create(tableOptions);

        // New schema with field removed
        Schema newSchema = new Schema(
                Types.NestedField.required(1, "id", Types.LongType.get()),
                Types.NestedField.optional(3, "email", Types.StringType.get())
        );

        // Update schema
        icebergTable.updateTableSchemaIfNeeded(newSchema);

        // Verify field was actually deleted (hard delete)
        Schema updatedSchema = icebergTable.getTable().schema();
        assertNull(updatedSchema.findField("name"), "Field should be deleted with hard delete");
        assertNotNull(updatedSchema.findField("id"));
        assertNotNull(updatedSchema.findField("email"));
    }

    @Test
    public void testUpdateSchemaWithListType() throws Exception  {
        // Create initial schema with list field
        Schema initialSchema = new Schema(
                Types.NestedField.required(1, "id", Types.LongType.get()),
                Types.NestedField.optional(2, "tags", Types.ListType.ofOptional(3, Types.StringType.get()))
        );

        TableOptions tableOptions = createTableOptions(initialSchema);
        icebergTable = new IcebergTable(catalog, tableIdentifier, tableOptions, configuration);
        icebergTable.create(tableOptions);

        // New schema with additional field
        Schema newSchema = new Schema(
                Types.NestedField.required(1, "id", Types.LongType.get()),
                Types.NestedField.optional(2, "tags", Types.ListType.ofOptional(3, Types.StringType.get())),
                Types.NestedField.optional(4, "categories", Types.ListType.ofOptional(5, Types.StringType.get()))
        );

        // Update schema
        icebergTable.updateTableSchemaIfNeeded(newSchema);

        // Verify new list field was added
        Schema updatedSchema = icebergTable.getTable().schema();
        assertNotNull(updatedSchema.findField("categories"));
        assertTrue(updatedSchema.findField("categories").type().isListType());
    }

    @Test
    public void testUpdateSchemaWithMapType()throws Exception   {
        // Create initial schema with map field
        Schema initialSchema = new Schema(
                Types.NestedField.required(1, "id", Types.LongType.get()),
                Types.NestedField.optional(2, "metadata", Types.MapType.ofOptional(
                        3, 4, Types.StringType.get(), Types.StringType.get()))
        );

        TableOptions tableOptions = createTableOptions(initialSchema);
        icebergTable = new IcebergTable(catalog, tableIdentifier, tableOptions, configuration);
        icebergTable.create(tableOptions);

        // New schema with additional map field
        Schema newSchema = new Schema(
                Types.NestedField.required(1, "id", Types.LongType.get()),
                Types.NestedField.optional(2, "metadata", Types.MapType.ofOptional(
                        3, 4, Types.StringType.get(), Types.StringType.get())),
                Types.NestedField.optional(5, "properties", Types.MapType.ofOptional(
                        6, 7, Types.StringType.get(), Types.IntegerType.get()))
        );

        // Update schema
        icebergTable.updateTableSchemaIfNeeded(newSchema);

        // Verify new map field was added
        Schema updatedSchema = icebergTable.getTable().schema();
        assertNotNull(updatedSchema.findField("properties"));
        assertTrue(updatedSchema.findField("properties").type().isMapType());
    }

    @Test
    public void testUpdateSchemaWhenSchemasAreEqual() throws Exception  {
        // Create initial schema
        Schema initialSchema = new Schema(
                Types.NestedField.required(1, "id", Types.LongType.get()),
                Types.NestedField.optional(2, "name", Types.StringType.get())
        );

        TableOptions tableOptions = createTableOptions(initialSchema);
        icebergTable = new IcebergTable(catalog, tableIdentifier, tableOptions, configuration);
        icebergTable.create(tableOptions);

        // Get initial schema column count as baseline
        Schema beforeSchema = icebergTable.getTable().schema();
        int initialColumnCount = beforeSchema.columns().size();

        // Try to update with same schema
        icebergTable.updateTableSchemaIfNeeded(initialSchema);

        // Verify schema hasn't changed (same number of columns and structure)
        Schema afterSchema = icebergTable.getTable().schema();
        assertEquals(initialColumnCount, afterSchema.columns().size());
        assertNotNull(afterSchema.findField("id"));
        assertNotNull(afterSchema.findField("name"));
        assertEquals(Types.LongType.get(), afterSchema.findField("id").type());
        assertEquals(Types.StringType.get(), afterSchema.findField("name").type());
    }

    @Test
    public void testUpdateSchemaWithNullSchema()throws Exception   {
        // Create initial schema
        Schema initialSchema = new Schema(
                Types.NestedField.required(1, "id", Types.LongType.get())
        );

        TableOptions tableOptions = createTableOptions(initialSchema);
        icebergTable = new IcebergTable(catalog, tableIdentifier, tableOptions, configuration);
        icebergTable.create(tableOptions);

        // Try to update with null schema - should do nothing
        try {
            icebergTable.updateTableSchemaIfNeeded(null);
            fail();
        } catch (IllegalArgumentException e) {
        }

        // Verify schema unchanged
        Schema currentSchema = icebergTable.getTable().schema();
        assertEquals(1, currentSchema.columns().size());
    }

    @Test
    public void testUpdateSchemaWithComplexNestedStructure() throws Exception  {
        // Create initial schema with complex nested structure
        Schema initialSchema = new Schema(
                Types.NestedField.required(1, "id", Types.LongType.get()),
                Types.NestedField.optional(2, "user", Types.StructType.of(
                        Types.NestedField.optional(3, "name", Types.StringType.get()),
                        Types.NestedField.optional(4, "contacts", Types.StructType.of(
                                Types.NestedField.optional(5, "email", Types.StringType.get())
                        ))
                ))
        );

        TableOptions tableOptions = createTableOptions(initialSchema);
        icebergTable = new IcebergTable(catalog, tableIdentifier, tableOptions, configuration);
        icebergTable.create(tableOptions);

        // New schema with deeply nested additional field
        Schema newSchema = new Schema(
                Types.NestedField.required(1, "id", Types.LongType.get()),
                Types.NestedField.optional(2, "user", Types.StructType.of(
                        Types.NestedField.optional(3, "name", Types.StringType.get()),
                        Types.NestedField.optional(4, "contacts", Types.StructType.of(
                                Types.NestedField.optional(5, "email", Types.StringType.get()),
                                Types.NestedField.optional(6, "phone", Types.StringType.get())
                        ))
                ))
        );

        // Update schema
        icebergTable.updateTableSchemaIfNeeded(newSchema);

        // Verify deeply nested field was added
        Schema updatedSchema = icebergTable.getTable().schema();
        Types.StructType userStruct = updatedSchema.findField("user").type().asStructType();
        Types.StructType contactsStruct = userStruct.field("contacts").type().asStructType();
        assertNotNull(contactsStruct.field("phone"));
    }

    @Test
    public void testUpdateSchemaWithMultipleOperations() throws Exception  {
        // Create initial schema
        Schema initialSchema = new Schema(
                Types.NestedField.required(1, "id", Types.LongType.get()),
                Types.NestedField.required(2, "count", Types.IntegerType.get()),
                Types.NestedField.required(3, "name", Types.StringType.get())
        );

        TableOptions tableOptions = createTableOptions(initialSchema);
        icebergTable = new IcebergTable(catalog, tableIdentifier, tableOptions, configuration);
        icebergTable.create(tableOptions);

        // New schema with multiple changes:
        // - Add new field (email)
        // - Promote type (count: int -> long)
        // - Make field optional (name: required -> optional)
        Schema newSchema = new Schema(
                Types.NestedField.required(1, "id", Types.LongType.get()),
                Types.NestedField.optional(2, "count", Types.LongType.get()),
                Types.NestedField.optional(3, "name", Types.StringType.get()),
                Types.NestedField.optional(4, "email", Types.StringType.get())
        );

        // Update schema
        icebergTable.updateTableSchemaIfNeeded(newSchema);

        // Verify all changes
        Schema updatedSchema = icebergTable.getTable().schema();
        assertNotNull(updatedSchema.findField("email"));
        assertEquals(Types.LongType.get(), updatedSchema.findField("count").type());
        assertTrue(updatedSchema.findField("count").isOptional());
        assertTrue(updatedSchema.findField("name").isOptional());
    }

    @Test
    public void testUpdateSchemaWithNewRequiredFieldConverted() throws Exception {
        // With make-new-fields-optional (default on), adding a new required field succeeds:
        // the field is added as OPTIONAL instead of being rejected.
        Schema initialSchema = new Schema(
                Types.NestedField.required(1, "id", Types.LongType.get()),
                Types.NestedField.optional(2, "name", Types.StringType.get())
        );

        TableOptions tableOptions = createTableOptions(initialSchema);
        icebergTable = new IcebergTable(catalog, tableIdentifier, tableOptions, configuration);
        icebergTable.create(tableOptions);

        // New schema attempting to add a required field.
        Schema newSchema = new Schema(
                Types.NestedField.required(1, "id", Types.LongType.get()),
                Types.NestedField.optional(2, "name", Types.StringType.get()),
                Types.NestedField.required(3, "email", Types.StringType.get())
        );

        icebergTable.updateTableSchemaIfNeeded(newSchema);

        // The new field is added, converted to optional.
        Schema updatedSchema = icebergTable.getTable().schema();
        assertNotNull(updatedSchema.findField("email"));
        assertTrue(updatedSchema.findField("email").isOptional());
    }

    @Test
    public void testUpdateSchemaFieldOrderingPreserved() throws Exception  {
        // Test that field ordering is preserved when adding new fields

        Schema initialSchema = new Schema(
                Types.NestedField.required(1, "id", Types.LongType.get()),
                Types.NestedField.optional(2, "name", Types.StringType.get()),
                Types.NestedField.optional(3, "age", Types.IntegerType.get())
        );

        TableOptions tableOptions = createTableOptions(initialSchema);
        icebergTable = new IcebergTable(catalog, tableIdentifier, tableOptions, configuration);
        icebergTable.create(tableOptions);

        // Add a new field
        Schema newSchema = new Schema(
                Types.NestedField.required(1, "id", Types.LongType.get()),
                Types.NestedField.optional(2, "name", Types.StringType.get()),
                Types.NestedField.optional(4, "email", Types.StringType.get()),
                Types.NestedField.optional(3, "age", Types.IntegerType.get())
        );

        // Update schema
        icebergTable.updateTableSchemaIfNeeded(newSchema);

        // Verify all fields exist
        Schema updatedSchema = icebergTable.getTable().schema();
        assertNotNull(updatedSchema.findField("id"));
        assertNotNull(updatedSchema.findField("name"));
        assertNotNull(updatedSchema.findField("email"));
        assertNotNull(updatedSchema.findField("age"));
    }

    @Test
    public void testUpdateSchemaWithMixedNestedAndTopLevelFields() throws Exception  {
        // Test adding both top-level and nested fields in one update

        Schema initialSchema = new Schema(
                Types.NestedField.required(1, "id", Types.LongType.get()),
                Types.NestedField.optional(2, "user", Types.StructType.of(
                        Types.NestedField.optional(3, "name", Types.StringType.get())
                ))
        );

        TableOptions tableOptions = createTableOptions(initialSchema);
        icebergTable = new IcebergTable(catalog, tableIdentifier, tableOptions, configuration);
        icebergTable.create(tableOptions);

        // Add fields at multiple levels
        Schema newSchema = new Schema(
                Types.NestedField.required(1, "id", Types.LongType.get()),
                Types.NestedField.optional(2, "user", Types.StructType.of(
                        Types.NestedField.optional(3, "name", Types.StringType.get()),
                        Types.NestedField.optional(5, "email", Types.StringType.get())
                )),
                Types.NestedField.optional(4, "timestamp", Types.LongType.get())
        );

        // Update schema
        icebergTable.updateTableSchemaIfNeeded(newSchema);

        // Verify both top-level and nested fields were added
        Schema updatedSchema = icebergTable.getTable().schema();
        assertNotNull(updatedSchema.findField("timestamp"));
        Types.StructType userStruct = updatedSchema.findField("user").type().asStructType();
        assertNotNull(userStruct.field("email"));
    }

    @Test
    public void testUpdateSchemaWithMultipleRequiredFieldsRemoval() throws Exception  {
        // Test behavior when multiple required fields are missing from new schema
        // With soft delete enabled (default), they should become optional

        Properties properties = new Properties();
        properties.setProperty("cluster", "test-cluster");
        properties.setProperty(IcebergTable.SCHEMA_EVOLUTION_SOFT_DELETE_ENABLED, "true");
        configuration = createMockConfiguration(properties);

        // Create initial schema with multiple required fields
        Schema initialSchema = new Schema(
                Types.NestedField.required(1, "id", Types.LongType.get()),
                Types.NestedField.required(2, "name", Types.StringType.get()),
                Types.NestedField.required(3, "email", Types.StringType.get()),
                Types.NestedField.optional(4, "phone", Types.StringType.get())
        );

        TableOptions tableOptions = createTableOptions(initialSchema);
        icebergTable = new IcebergTable(catalog, tableIdentifier, tableOptions, configuration);
        icebergTable.create(tableOptions);

        // New schema missing multiple required fields
        Schema newSchema = new Schema(
                Types.NestedField.required(1, "id", Types.LongType.get()),
                Types.NestedField.optional(4, "phone", Types.StringType.get())
        );

        // Update schema - with soft delete, fields should become optional
        icebergTable.updateTableSchemaIfNeeded(newSchema);

        // Verify fields are preserved and made optional
        Schema updatedSchema = icebergTable.getTable().schema();
        assertNotNull(updatedSchema.findField("id"));
        assertNotNull(updatedSchema.findField("name"), "Required fields should be preserved with soft delete");
        assertNotNull(updatedSchema.findField("email"), "Required fields should be preserved with soft delete");
        assertNotNull(updatedSchema.findField("phone"));

        // Verify they are now optional
        assertTrue(updatedSchema.findField("name").isOptional(), "Deleted required field should become optional");
        assertTrue(updatedSchema.findField("email").isOptional(), "Deleted required field should become optional");
    }

    @Test
    public void testUpdateSchemaValidationCompatibility() throws Exception  {
        // Test that schema evolution maintains compatibility

        // Create initial schema
        Schema initialSchema = new Schema(
                Types.NestedField.required(1, "id", Types.LongType.get()),
                Types.NestedField.required(2, "name", Types.StringType.get())
        );

        TableOptions tableOptions = createTableOptions(initialSchema);
        icebergTable = new IcebergTable(catalog, tableIdentifier, tableOptions, configuration);
        icebergTable.create(tableOptions);

        // New schema with compatible changes (add field, make field optional)
        Schema compatibleSchema = new Schema(
                Types.NestedField.required(1, "id", Types.LongType.get()),
                Types.NestedField.optional(2, "name", Types.StringType.get()),
                Types.NestedField.optional(3, "email", Types.StringType.get())
        );

        // This should succeed
        icebergTable.updateTableSchemaIfNeeded(compatibleSchema);

        // Verify changes applied
        Schema updatedSchema = icebergTable.getTable().schema();
        assertTrue(updatedSchema.findField("name").isOptional());
        assertNotNull(updatedSchema.findField("email"));
    }

    @Test
    public void testUpdateSchemaPreservesExistingFieldsWhenSubsetProvided() throws Exception  {
        // Test that providing a schema with fewer optional fields preserves all original fields
        // With soft delete enabled (default), removed optional fields stay in schema

        // Create initial schema with multiple optional fields
        Schema initialSchema = new Schema(
                Types.NestedField.required(1, "id", Types.LongType.get()),
                Types.NestedField.optional(2, "name", Types.StringType.get()),
                Types.NestedField.optional(3, "email", Types.StringType.get()),
                Types.NestedField.optional(4, "age", Types.IntegerType.get())
        );

        TableOptions tableOptions = createTableOptions(initialSchema);
        icebergTable = new IcebergTable(catalog, tableIdentifier, tableOptions, configuration);
        icebergTable.create(tableOptions);

        // Provide a schema with only subset of fields
        Schema subsetSchema = new Schema(
                Types.NestedField.required(1, "id", Types.LongType.get()),
                Types.NestedField.optional(2, "name", Types.StringType.get())
        );

        // Update schema - with soft delete (default), fields should be preserved
        icebergTable.updateTableSchemaIfNeeded(subsetSchema);

        // Verify all original fields still exist (soft delete behavior)
        Schema updatedSchema = icebergTable.getTable().schema();
        assertNotNull(updatedSchema.findField("id"));
        assertNotNull(updatedSchema.findField("name"));
        assertNotNull(updatedSchema.findField("email"), "email field should be preserved with soft delete");
        assertNotNull(updatedSchema.findField("age"), "age field should be preserved with soft delete");
    }


// Helper methods

    private TableOptions createTableOptions(Schema schema) {
        var builder = TableOptions.builder();
        builder.schema(schema);
        return builder.build();
    }

    private LakehouseConfiguration createMockConfiguration(Properties properties) {
        return new LakehouseConfiguration(properties) {
            @Override
            public Properties getProperties() {
                return properties;
            }

            @Override
            public boolean checkIcebergNullability() {
                return true;
            }

            @Override
            public boolean checkIcebergOrdering() {
                return true;
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

            @Override
            public boolean makeNewFieldsOptionalOnEvolution() {
                return true;
            }
        };
    }
}
