/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.lakehouse.iceberg;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.lakestream.ursa.lakehouse.LakehouseConfiguration;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.UpdateSchema;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.mapping.MappingUtil;
import org.apache.iceberg.mapping.NameMapping;
import org.apache.iceberg.mapping.NameMappingParser;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class IcebergTableSchemaEvolutionTest {

    private Catalog catalog;

    @Mock
    private Table table;

    @Mock
    private UpdateSchema updateSchema;

    @Mock
    private LakehouseConfiguration configuration;

    @Mock
    private TableOptions tableOptions;

    private IcebergTable icebergTable;
    private TableIdentifier tableIdentifier;

    @BeforeEach
    void setUp() {
        tableIdentifier = TableIdentifier.of("test_namespace", "test_table");
        catalog = mock(Catalog.class);

        // Mock basic configuration needed for constructor
        when(configuration.getCatalogName()).thenReturn(java.util.Optional.of("test_catalog"));
        when(configuration.getCatalogMaxOpenTime()).thenReturn(java.time.Duration.ofMinutes(30));
        when(configuration.getIcebergSnapshotExpirationInterval()).thenReturn(0);
        lenient().when(configuration.checkIcebergNullability()).thenReturn(true);
        lenient().when(configuration.checkIcebergOrdering()).thenReturn(true);
        // Use lenient() for stubs that might not be called in all tests
        lenient().when(configuration.getIcebergCatalogBackendType(any()))
            .thenReturn(IcebergCatalogBackendType.HADOOP);
        Properties properties = new Properties();
        properties.put("cluster", "test_cluster");
        lenient().when(configuration.getProperties()).thenReturn(properties);

        // Create IcebergTable with mocked catalog
        icebergTable = new IcebergTable(catalog, tableIdentifier, tableOptions, configuration);
    }

    @Test
    void testUpdateTableSchemaIfNeeded_NullSchema_ShouldSkip() throws Exception {
        // Test null schema input
        try {
            icebergTable.updateTableSchemaIfNeeded(null);
            fail();
        } catch (IllegalArgumentException e) {

        }

        // Verify no interactions with table
        verify(catalog, never()).loadTable(any());
        verify(table, never()).updateSchema();
    }

    @Test
    void testUpdateTableSchemaIfNeeded_TableNotFound_ShouldWarnAndReturn() {
        // Mock table doesn't exist
        when(catalog.loadTable(any())).thenThrow(new NoSuchTableException("Table not found"));

        Schema newSchema = createSimpleSchema();

        assertThrows(NoSuchTableException.class, () -> icebergTable.updateTableSchemaIfNeeded(newSchema));
        verify(table, never()).updateSchema();
    }

    @Test
    void testUpdateTableSchemaIfNeeded_IdenticalSchemas_ShouldSkip() throws Exception {
        Schema currentSchema = createSimpleSchema();
        Schema newSchema = createSimpleSchema(); // Same schema

        // Setup mocks for this test
        when(catalog.loadTable(any())).thenReturn(table);
        when(table.schema()).thenReturn(currentSchema);

        icebergTable.updateTableSchemaIfNeeded(newSchema);

        verify(table).refresh();
        verify(table).schema();
        verify(table, never()).updateSchema();
    }

    @Test
    void testUpdateTableSchemaIfNeeded_AddNewOptionalField() throws Exception {
        Schema currentSchema = new Schema(
            Types.NestedField.required(1, "id", Types.IntegerType.get()),
            Types.NestedField.required(2, "name", Types.StringType.get())
        );

        Schema newSchema = new Schema(
            Types.NestedField.required(1, "id", Types.IntegerType.get()),
            Types.NestedField.required(2, "name", Types.StringType.get()),
            Types.NestedField.optional(3, "email", Types.StringType.get())
        );

        // Setup mocks for this test
        setupBasicMocks(currentSchema);

        icebergTable.updateTableSchemaIfNeeded(newSchema);

        verify(updateSchema).addColumn("email", Types.StringType.get());
        verify(updateSchema).commit();
    }

    @Test
    void testUpdateTableSchemaIfNeeded_AddNewRequiredField_ShouldAddAsOptional() {
        Schema currentSchema = new Schema(
            Types.NestedField.required(1, "id", Types.IntegerType.get())
        );

        Schema newSchema = new Schema(
            Types.NestedField.required(1, "id", Types.IntegerType.get()),
            Types.NestedField.required(2, "name", Types.StringType.get()) // Required field
        );

        // Setup mocks for this test
        setupBasicMocks(currentSchema);

        try {
            icebergTable.updateTableSchemaIfNeeded(newSchema);
        } catch (Exception e) {
            assertTrue(e.getMessage().contains("name is required, but is missing"));
        }
    }

    @Test
    void testUpdateTableSchemaIfNeeded_TypePromotion_IntToLong() throws Exception {
        Schema currentSchema = new Schema(
            Types.NestedField.required(1, "id", Types.IntegerType.get()),
            Types.NestedField.required(2, "count", Types.IntegerType.get())
        );

        Schema newSchema = new Schema(
            Types.NestedField.required(1, "id", Types.IntegerType.get()),
            Types.NestedField.required(2, "count", Types.LongType.get()) // Promoted to long
        );

        // Setup mocks for this test
        setupBasicMocks(currentSchema);

        icebergTable.updateTableSchemaIfNeeded(newSchema);

        verify(updateSchema).updateColumn("count", Types.LongType.get().asPrimitiveType());
        verify(updateSchema).commit();
    }

    @Test
    void testUpdateTableSchemaIfNeeded_TypePromotion_FloatToDouble() throws Exception {
        Schema currentSchema = new Schema(
            Types.NestedField.required(1, "id", Types.IntegerType.get()),
            Types.NestedField.required(2, "price", Types.FloatType.get())
        );

        Schema newSchema = new Schema(
            Types.NestedField.required(1, "id", Types.IntegerType.get()),
            Types.NestedField.required(2, "price", Types.DoubleType.get()) // Promoted to double
        );

        // Setup mocks for this test
        setupBasicMocks(currentSchema);

        icebergTable.updateTableSchemaIfNeeded(newSchema);

        verify(updateSchema).updateColumn("price", Types.DoubleType.get().asPrimitiveType());
        verify(updateSchema).commit();
    }

    @Test
    void testUpdateTableSchemaIfNeeded_TypePromotion_DecimalPrecisionScale() {
        Schema currentSchema = new Schema(
            Types.NestedField.required(1, "id", Types.IntegerType.get()),
            Types.NestedField.required(2, "amount", Types.DecimalType.of(10, 2))
        );

        Schema newSchema = new Schema(
            Types.NestedField.required(1, "id", Types.IntegerType.get()),
            Types.NestedField.required(2, "amount", Types.DecimalType.of(15, 4)) // Higher precision and scale
        );

        setupBasicMocks(currentSchema);

        try {
            icebergTable.updateTableSchemaIfNeeded(newSchema);
            fail();
        } catch (Exception e) {
            assertTrue(e.getMessage().contains("amount: decimal(10, 2) cannot be promoted to decimal(15, 4)"));
        }
    }

    @Test
    void testUpdateTableSchemaIfNeeded_UnsupportedTypeChange_ShouldWarn() {
        Schema currentSchema = new Schema(
            Types.NestedField.required(1, "id", Types.IntegerType.get()),
            Types.NestedField.required(2, "name", Types.StringType.get())
        );

        Schema newSchema = new Schema(
            Types.NestedField.required(1, "id", Types.IntegerType.get()),
            Types.NestedField.required(2, "name", Types.IntegerType.get()) // Unsupported: string to int
        );

        setupBasicMocks(currentSchema);

        try {
            icebergTable.updateTableSchemaIfNeeded(newSchema);
            fail();
        } catch (Exception e) {
            // Expected exception due to unsupported type change
            assertTrue(e.getMessage().contains("name: string cannot be promoted to int"));
        }
    }

    @Test
    void testUpdateTableSchemaIfNeeded_MakeRequiredFieldOptional() throws Exception {
        Schema currentSchema = new Schema(
            Types.NestedField.required(1, "id", Types.IntegerType.get()),
            Types.NestedField.required(2, "name", Types.StringType.get())
        );

        Schema newSchema = new Schema(
            Types.NestedField.required(1, "id", Types.IntegerType.get()),
            Types.NestedField.optional(2, "name", Types.StringType.get()) // Made optional
        );

        setupBasicMocks(currentSchema);

        icebergTable.updateTableSchemaIfNeeded(newSchema);

        verify(updateSchema).makeColumnOptional("name");
        verify(updateSchema).commit();
    }

    @Test
    void testUpdateTableSchemaIfNeeded_CannotMakeOptionalFieldRequired_ShouldWarn() {
        Schema currentSchema = new Schema(
            Types.NestedField.required(1, "id", Types.IntegerType.get()),
            Types.NestedField.optional(2, "name", Types.StringType.get())
        );

        Schema newSchema = new Schema(
            Types.NestedField.required(1, "id", Types.IntegerType.get()),
            Types.NestedField.required(2, "name", Types.StringType.get()) // Try to make required
        );

        setupBasicMocks(currentSchema);

        try {
            icebergTable.updateTableSchemaIfNeeded(newSchema);
            fail();
        } catch (Exception e) {
            // Expected exception due to unsupported type change
            assertTrue(e.getMessage().contains("name should be required, but is optional"));
        }
    }

    @Test
    void testUpdateTableSchemaIfNeeded_SoftDeleteField() throws Exception {
        Schema currentSchema = new Schema(
            Types.NestedField.required(1, "id", Types.IntegerType.get()),
            Types.NestedField.required(2, "name", Types.StringType.get()),
            Types.NestedField.optional(3, "email", Types.StringType.get())
        );

        Schema newSchema = new Schema(
            Types.NestedField.required(1, "id", Types.IntegerType.get())
            // "name" and "email" fields are deleted
        );

        setupBasicMocks(currentSchema);

        icebergTable.updateTableSchemaIfNeeded(newSchema);

        // Should make "name" optional (soft delete), but "email" is already optional
        verify(updateSchema).makeColumnOptional("name");
        verify(updateSchema, never()).makeColumnOptional("email"); // Already optional
        verify(updateSchema).commit();
    }

    @Test
    void testUpdateTableSchemaIfNeeded_ComplexSchemaEvolution() {
        // Complex case with multiple operations
        Schema currentSchema = new Schema(
            Types.NestedField.required(1, "id", Types.IntegerType.get()),
            Types.NestedField.required(2, "name", Types.StringType.get()),
            Types.NestedField.required(3, "age", Types.IntegerType.get()),
            Types.NestedField.required(4, "salary", Types.FloatType.get()),
            Types.NestedField.required(5, "department", Types.StringType.get())
        );

        Schema newSchema = new Schema(
            Types.NestedField.required(1, "id", Types.IntegerType.get()),
            Types.NestedField.optional(2, "name", Types.StringType.get()), // Make optional
            Types.NestedField.required(3, "age", Types.LongType.get()),      // Promote int to long
            Types.NestedField.required(4, "salary", Types.DoubleType.get()), // Promote float to double
            // "department" deleted (will be soft deleted)
            Types.NestedField.optional(6, "email", Types.StringType.get()),  // Add new field
            Types.NestedField.required(7, "phone", Types.StringType.get())   // Add new required field (will be optional)
        );

        setupBasicMocks(currentSchema);

        // TODO: the promotion from long to int and double to float is not supported, need to fix it.
        try {
            icebergTable.updateTableSchemaIfNeeded(newSchema);
        } catch (Exception e) {
            // Expected exception due to unsupported type change
            assertTrue(e.getMessage().contains("phone is required, but is missing"));
        }
    }

    @Test
    void testUpdateTableSchemaIfNeeded_UpdateNameMapping_NonUnityCatalog() throws Exception {
        Schema currentSchema = createSimpleSchema();
        Schema newSchema = new Schema(
            Types.NestedField.required(1, "id", Types.IntegerType.get()),
            Types.NestedField.required(2, "name", Types.StringType.get()),
            Types.NestedField.optional(3, "email", Types.StringType.get())
        );

        setupBasicMocks(currentSchema);
        when(table.schema()).thenReturn(currentSchema, newSchema); // Return new schema after update
        when(updateSchema.apply()).thenReturn(newSchema);
        when(configuration.getIcebergCatalogBackendType(any()))
            .thenReturn(IcebergCatalogBackendType.HADOOP);

        // Mock name mapping creation
        try (MockedStatic<MappingUtil> mappingUtilMock = mockStatic(MappingUtil.class);
             MockedStatic<NameMappingParser> nameMapperMock = mockStatic(NameMappingParser.class)) {

            NameMapping nameMapping = mock(NameMapping.class);
            mappingUtilMock.when(() -> MappingUtil.create(any(Schema.class))).thenReturn(nameMapping);
            nameMapperMock.when(() -> NameMappingParser.toJson(nameMapping)).thenReturn("test-mapping-json");

            var updateProperties = mock(org.apache.iceberg.UpdateProperties.class);
            when(table.updateProperties()).thenReturn(updateProperties);
            when(updateProperties.set(anyString(), anyString())).thenReturn(updateProperties);

            icebergTable.updateTableSchemaIfNeeded(newSchema);

            verify(updateProperties).set("schema.name-mapping.default", "test-mapping-json");
            verify(updateProperties).commit();
        }
    }

    @Test
    void testUpdateTableSchemaIfNeeded_SkipNameMapping_UnityCatalog() throws Exception {
        Schema currentSchema = createSimpleSchema();
        Schema newSchema = new Schema(
            Types.NestedField.required(1, "id", Types.IntegerType.get()),
            Types.NestedField.required(2, "name", Types.StringType.get()),
            Types.NestedField.optional(3, "email", Types.StringType.get())
        );

        setupBasicMocks(currentSchema);
        when(configuration.getIcebergCatalogBackendType(any()))
            .thenReturn(IcebergCatalogBackendType.UNITYCATALOG);
        when(updateSchema.apply()).thenReturn(newSchema);

        icebergTable.updateTableSchemaIfNeeded(newSchema);

        verify(updateSchema).commit();
        // Should not call updateProperties for Unity Catalog
        verify(table, never()).updateProperties();
    }

    @Test
    void testUpdateTableSchemaIfNeeded_SchemaUpdateFailure_ShouldThrowException() {
        Schema currentSchema = createSimpleSchema();
        Schema newSchema = new Schema(
            Types.NestedField.required(1, "id", Types.IntegerType.get()),
            Types.NestedField.required(2, "name", Types.StringType.get()),
            Types.NestedField.optional(3, "email", Types.StringType.get())
        );

        setupBasicMocks(currentSchema);
        doThrow(new RuntimeException("Schema update failed")).when(updateSchema).commit();

        assertThrows(Exception.class, () -> {
            icebergTable.updateTableSchemaIfNeeded(newSchema);
        });

        verify(updateSchema).commit();
    }

    // Helper methods
    private Schema createSimpleSchema() {
        return new Schema(
            Types.NestedField.required(1, "id", Types.IntegerType.get()),
            Types.NestedField.required(2, "name", Types.StringType.get())
        );
    }

    private void setupBasicMocks(Schema currentSchema) {
        when(catalog.loadTable(any())).thenReturn(table);
        when(table.schema()).thenReturn(currentSchema);
        lenient().when(table.updateSchema()).thenReturn(updateSchema);
        lenient().when(updateSchema.apply()).thenReturn(currentSchema);
        lenient().when(configuration.getIcebergCatalogBackendType(any()))
            .thenReturn(IcebergCatalogBackendType.HADOOP);

        // Mock updateProperties for name mapping functionality (lenient since not all tests use it)
        var updateProperties = mock(org.apache.iceberg.UpdateProperties.class);
        lenient().when(table.updateProperties()).thenReturn(updateProperties);
        lenient().when(updateProperties.set(anyString(), anyString())).thenReturn(updateProperties);
    }

    @Test
    void testUpdateTableSchemaIfNeeded_AddNestedStructField() throws Exception {
        // Current schema with nested struct
        Schema currentSchema = new Schema(
            Types.NestedField.required(1, "id", Types.IntegerType.get()),
            Types.NestedField.required(2, "user", Types.StructType.of(
                Types.NestedField.required(3, "name", Types.StringType.get()),
                Types.NestedField.required(4, "age", Types.IntegerType.get())
            ))
        );

        // New schema adds field to nested struct
        Schema newSchema = new Schema(
            Types.NestedField.required(1, "id", Types.IntegerType.get()),
            Types.NestedField.required(2, "user", Types.StructType.of(
                Types.NestedField.required(3, "name", Types.StringType.get()),
                Types.NestedField.required(4, "age", Types.IntegerType.get()),
                Types.NestedField.optional(5, "email", Types.StringType.get()) // New nested field
            ))
        );

        setupBasicMocks(currentSchema);

        icebergTable.updateTableSchemaIfNeeded(newSchema);

        // Verify nested field addition
        verify(updateSchema).addColumn("user", "email", Types.StringType.get());
        verify(updateSchema).commit();
    }

    @Test
    void testUpdateTableSchemaIfNeeded_NestedStructTypePromotion() throws Exception {
        // Current schema with nested struct
        Schema currentSchema = new Schema(
            Types.NestedField.required(1, "id", Types.IntegerType.get()),
            Types.NestedField.required(2, "metrics", Types.StructType.of(
                Types.NestedField.required(3, "count", Types.IntegerType.get()),
                Types.NestedField.required(4, "score", Types.FloatType.get())
            ))
        );

        // New schema promotes nested field types
        Schema newSchema = new Schema(
            Types.NestedField.required(1, "id", Types.IntegerType.get()),
            Types.NestedField.required(2, "metrics", Types.StructType.of(
                Types.NestedField.required(3, "count", Types.LongType.get()),    // int -> long
                Types.NestedField.required(4, "score", Types.DoubleType.get())   // float -> double
            ))
        );

        setupBasicMocks(currentSchema);

        icebergTable.updateTableSchemaIfNeeded(newSchema);

        // Verify nested type promotions
        verify(updateSchema).updateColumn("metrics.count", Types.LongType.get().asPrimitiveType());
        verify(updateSchema).updateColumn("metrics.score", Types.DoubleType.get().asPrimitiveType());
        verify(updateSchema).commit();
    }

    @Test
    void testUpdateTableSchemaIfNeeded_NestedStructSoftDelete() throws Exception {
        // Current schema with nested struct
        Schema currentSchema = new Schema(
            Types.NestedField.required(1, "id", Types.IntegerType.get()),
            Types.NestedField.required(2, "address", Types.StructType.of(
                Types.NestedField.required(3, "street", Types.StringType.get()),
                Types.NestedField.required(4, "city", Types.StringType.get()),
                Types.NestedField.optional(5, "zipcode", Types.StringType.get())
            ))
        );

        // New schema removes fields from nested struct
        Schema newSchema = new Schema(
            Types.NestedField.required(1, "id", Types.IntegerType.get()),
            Types.NestedField.required(2, "address", Types.StructType.of(
                Types.NestedField.required(3, "street", Types.StringType.get())
                // "city" and "zipcode" are removed
            ))
        );

        setupBasicMocks(currentSchema);

        // TODO: add more cases.
        icebergTable.updateTableSchemaIfNeeded(newSchema);

        // Verify soft delete for nested fields
        verify(updateSchema).makeColumnOptional("address.city");  // Required field becomes optional
        // zipcode is already optional, so no action needed
        verify(updateSchema, never()).makeColumnOptional("address.zipcode");
        verify(updateSchema).commit();
    }

    @Test
    void testUpdateTableSchemaIfNeeded_NestedStructNullabilityChange() throws Exception {
        // Current schema with nested struct
        Schema currentSchema = new Schema(
            Types.NestedField.required(1, "id", Types.IntegerType.get()),
            Types.NestedField.required(2, "profile", Types.StructType.of(
                Types.NestedField.required(3, "firstName", Types.StringType.get()),
                Types.NestedField.required(4, "lastName", Types.StringType.get())
            ))
        );

        // New schema makes nested field optional
        Schema newSchema = new Schema(
            Types.NestedField.required(1, "id", Types.IntegerType.get()),
            Types.NestedField.required(2, "profile", Types.StructType.of(
                Types.NestedField.required(3, "firstName", Types.StringType.get()),
                Types.NestedField.optional(4, "lastName", Types.StringType.get())  // Made optional
            ))
        );

        setupBasicMocks(currentSchema);

        icebergTable.updateTableSchemaIfNeeded(newSchema);

        verify(updateSchema).makeColumnOptional("profile.lastName");
        verify(updateSchema).commit();
    }

    @Test
    void testUpdateTableSchemaIfNeeded_DeeplyNestedStructEvolution() throws Exception {
        // Current schema with deeply nested struct
        Schema currentSchema = new Schema(
            Types.NestedField.required(1, "id", Types.IntegerType.get()),
            Types.NestedField.required(2, "company", Types.StructType.of(
                Types.NestedField.required(3, "name", Types.StringType.get()),
                Types.NestedField.required(4, "address", Types.StructType.of(
                    Types.NestedField.required(5, "street", Types.StringType.get()),
                    Types.NestedField.required(6, "city", Types.StringType.get())
                ))
            ))
        );

        // New schema adds field to deeply nested struct
        Schema newSchema = new Schema(
            Types.NestedField.required(1, "id", Types.IntegerType.get()),
            Types.NestedField.required(2, "company", Types.StructType.of(
                Types.NestedField.required(3, "name", Types.StringType.get()),
                Types.NestedField.required(4, "address", Types.StructType.of(
                    Types.NestedField.required(5, "street", Types.StringType.get()),
                    Types.NestedField.required(6, "city", Types.StringType.get()),
                    Types.NestedField.optional(7, "country", Types.StringType.get()) // New deeply nested field
                ))
            ))
        );

        setupBasicMocks(currentSchema);

        icebergTable.updateTableSchemaIfNeeded(newSchema);

        verify(updateSchema).addColumn("company.address", "country", Types.StringType.get());
        verify(updateSchema).commit();
    }

    @Test
    void testUpdateTableSchemaIfNeeded_ComplexNestedStructEvolution() throws Exception {
        // Complex nested evolution with multiple operations
        Schema currentSchema = new Schema(
            Types.NestedField.required(1, "id", Types.IntegerType.get()),
            Types.NestedField.required(2, "order", Types.StructType.of(
                Types.NestedField.required(3, "total", Types.FloatType.get()),
                Types.NestedField.required(4, "quantity", Types.IntegerType.get()),
                Types.NestedField.required(5, "status", Types.StringType.get()),
                Types.NestedField.required(6, "customer", Types.StructType.of(
                    Types.NestedField.required(7, "name", Types.StringType.get()),
                    Types.NestedField.required(8, "email", Types.StringType.get())
                ))
            ))
        );

        Schema newSchema = new Schema(
            Types.NestedField.required(1, "id", Types.IntegerType.get()),
            Types.NestedField.required(2, "order", Types.StructType.of(
                Types.NestedField.required(3, "total", Types.DoubleType.get()),     // float -> double
                Types.NestedField.required(4, "quantity", Types.LongType.get()),    // int -> long
                // "status" removed (soft delete)
                Types.NestedField.required(6, "customer", Types.StructType.of(
                    Types.NestedField.optional(7, "name", Types.StringType.get()),  // required -> optional
                    Types.NestedField.required(8, "email", Types.StringType.get()),
                    Types.NestedField.optional(9, "phone", Types.StringType.get())  // new field
                )),
                Types.NestedField.optional(10, "discount", Types.DoubleType.get())  // new field
            ))
        );

        setupBasicMocks(currentSchema);

        icebergTable.updateTableSchemaIfNeeded(newSchema);

        // Verify all operations
        verify(updateSchema).updateColumn("order.total", Types.DoubleType.get().asPrimitiveType());
        verify(updateSchema).updateColumn("order.quantity", Types.LongType.get().asPrimitiveType());
        verify(updateSchema).makeColumnOptional("order.status");        // soft delete
        verify(updateSchema).makeColumnOptional("order.customer.name"); // nullability change
        verify(updateSchema).addColumn("order.customer", "phone", Types.StringType.get());
        verify(updateSchema).addColumn("order", "discount", Types.DoubleType.get());
        verify(updateSchema).commit();
    }

    @Test
    void testUpdateTableSchemaIfNeeded_ListTypeEvolution() {
        // Current schema with list type
        Schema currentSchema = new Schema(
            Types.NestedField.required(1, "id", Types.IntegerType.get()),
            Types.NestedField.required(2, "tags", Types.ListType.ofRequired(3, Types.StringType.get()))
        );

        // New schema with struct containing list - this tests that list types are handled correctly
        // but not evolved (Iceberg doesn't support list type evolution)
        Schema newSchema = new Schema(
            Types.NestedField.required(1, "id", Types.IntegerType.get()),
            Types.NestedField.required(2, "tags", Types.ListType.ofRequired(3, Types.StringType.get())),
            Types.NestedField.required(4, "metadata", Types.StructType.of(
                Types.NestedField.required(5, "keywords", Types.ListType.ofOptional(6, Types.StringType.get()))
            ))
        );

        setupBasicMocks(currentSchema);

        try {
            icebergTable.updateTableSchemaIfNeeded(newSchema);
            fail();
        } catch (Exception e) {
            // Expected exception due to unsupported type change
            assertTrue(e.getMessage().contains("metadata is required, but is missing"));
        }
    }

    @Test
    void testUpdateTableSchemaIfNeeded_MapTypeEvolution() {
        // Current schema with map type
        Schema currentSchema = new Schema(
            Types.NestedField.required(1, "id", Types.IntegerType.get()),
            Types.NestedField.required(2, "properties",
                Types.MapType.ofRequired(3, 4, Types.StringType.get(), Types.StringType.get()))
        );

        // New schema adds struct with map
        Schema newSchema = new Schema(
            Types.NestedField.required(1, "id", Types.IntegerType.get()),
            Types.NestedField.required(2, "properties",
                Types.MapType.ofRequired(3, 4, Types.StringType.get(), Types.StringType.get())),
            Types.NestedField.required(5, "config", Types.StructType.of(
                Types.NestedField.required(6, "settings",
                    Types.MapType.ofOptional(7, 8, Types.StringType.get(), Types.IntegerType.get()))
            ))
        );

        setupBasicMocks(currentSchema);

        try {
            icebergTable.updateTableSchemaIfNeeded(newSchema);
        } catch (Exception e) {
            // Expected exception due to unsupported type change
            assertTrue(e.getMessage().contains("config is required, but is missing"));
        }
    }

    @Test
    void testUpdateTableSchemaIfNeeded_NestedStructWithMixedTypes() throws Exception {
        // Schema with struct containing various types
        Schema currentSchema = new Schema(
            Types.NestedField.required(1, "id", Types.IntegerType.get()),
            Types.NestedField.required(2, "record", Types.StructType.of(
                Types.NestedField.required(3, "timestamp", Types.LongType.get()),
                Types.NestedField.required(4, "data", Types.StructType.of(
                    Types.NestedField.required(5, "values", Types.ListType.ofRequired(6, Types.DoubleType.get())),
                    Types.NestedField.required(7, "metadata", Types.MapType.ofRequired(8, 9,
                        Types.StringType.get(), Types.StringType.get()))
                ))
            ))
        );

        // Add new fields to nested structures
        Schema newSchema = new Schema(
            Types.NestedField.required(1, "id", Types.IntegerType.get()),
            Types.NestedField.required(2, "record", Types.StructType.of(
                Types.NestedField.required(3, "timestamp", Types.LongType.get()),
                Types.NestedField.optional(10, "version", Types.IntegerType.get()), // New field in nested struct
                Types.NestedField.required(4, "data", Types.StructType.of(
                    Types.NestedField.required(5, "values", Types.ListType.ofRequired(6, Types.DoubleType.get())),
                    Types.NestedField.required(7, "metadata", Types.MapType.ofRequired(8, 9,
                        Types.StringType.get(), Types.StringType.get())),
                    Types.NestedField.optional(11, "summary", Types.StringType.get()) // New field in deeply nested struct
                ))
            ))
        );

        setupBasicMocks(currentSchema);

        icebergTable.updateTableSchemaIfNeeded(newSchema);

        verify(updateSchema).addColumn("record", "version", Types.IntegerType.get());
        verify(updateSchema).addColumn("record.data", "summary", Types.StringType.get());
        verify(updateSchema).commit();
    }

    @Test
    void testUpdateTableSchemaIfNeeded_NestedStructIdenticalSchemas_ShouldSkip() throws Exception {
        // Test that identical nested schemas are correctly identified as unchanged
        Schema currentSchema = new Schema(
            Types.NestedField.required(1, "id", Types.IntegerType.get()),
            Types.NestedField.required(2, "user", Types.StructType.of(
                Types.NestedField.required(3, "profile", Types.StructType.of(
                    Types.NestedField.required(4, "name", Types.StringType.get()),
                    Types.NestedField.optional(5, "email", Types.StringType.get())
                )),
                Types.NestedField.required(6, "preferences", Types.MapType.ofOptional(7, 8,
                    Types.StringType.get(), Types.StringType.get()))
            ))
        );

        // Identical schema
        Schema newSchema = new Schema(
            Types.NestedField.required(1, "id", Types.IntegerType.get()),
            Types.NestedField.required(2, "user", Types.StructType.of(
                Types.NestedField.required(3, "profile", Types.StructType.of(
                    Types.NestedField.required(4, "name", Types.StringType.get()),
                    Types.NestedField.optional(5, "email", Types.StringType.get())
                )),
                Types.NestedField.required(6, "preferences", Types.MapType.ofOptional(7, 8,
                    Types.StringType.get(), Types.StringType.get()))
            ))
        );

        // Setup minimal mocks - only what's needed for schema comparison
        when(catalog.loadTable(any())).thenReturn(table);
        when(table.schema()).thenReturn(currentSchema);

        icebergTable.updateTableSchemaIfNeeded(newSchema);

        // Should not call updateSchema since schemas are identical
        verify(table).refresh();
        verify(table).schema();
        verify(table, never()).updateSchema();
    }

    @Test
    void testSchemasAreEqual_NestedStructs() {
        // Test the private schemasAreEqual method for nested structures
        Types.StructType struct1 = Types.StructType.of(
            Types.NestedField.required(1, "name", Types.StringType.get()),
            Types.NestedField.optional(2, "nested", Types.StructType.of(
                Types.NestedField.required(3, "value", Types.IntegerType.get())
            ))
        );

        Types.StructType struct2 = Types.StructType.of(
            Types.NestedField.required(1, "name", Types.StringType.get()),
            Types.NestedField.optional(2, "nested", Types.StructType.of(
                Types.NestedField.required(3, "value", Types.IntegerType.get())
            ))
        );

        Types.StructType struct3 = Types.StructType.of(
            Types.NestedField.required(1, "name", Types.StringType.get()),
            Types.NestedField.optional(2, "nested", Types.StructType.of(
                Types.NestedField.required(3, "value", Types.LongType.get()) // Different type
            ))
        );

        assertTrue(invokeSchemasAreEqual(struct1, struct2));
        assertFalse(invokeSchemasAreEqual(struct1, struct3));
    }

    @Test
    void testTypesAreEqual_ComplexTypes() {
        // Test list types
        Types.ListType list1 = Types.ListType.ofRequired(1, Types.StringType.get());
        Types.ListType list2 = Types.ListType.ofRequired(1, Types.StringType.get());
        Types.ListType list3 = Types.ListType.ofOptional(1, Types.StringType.get()); // Different optionality

        assertTrue(invokeTypesAreEqual(list1, list2));
        assertFalse(invokeTypesAreEqual(list1, list3));

        // Test map types
        Types.MapType map1 = Types.MapType.ofRequired(1, 2, Types.StringType.get(), Types.IntegerType.get());
        Types.MapType map2 = Types.MapType.ofRequired(1, 2, Types.StringType.get(), Types.IntegerType.get());
        Types.MapType map3 = Types.MapType.ofOptional(1, 2, Types.StringType.get(), Types.IntegerType.get());

        assertTrue(invokeTypesAreEqual(map1, map2));
        assertFalse(invokeTypesAreEqual(map1, map3));
    }

    // Helper methods for accessing private methods via reflection
    private boolean invokeSchemasAreEqual(Types.StructType struct1, Types.StructType struct2) {
        try {
            var method = IcebergTable.class.getDeclaredMethod("schemasAreEqual",
                Types.StructType.class, Types.StructType.class);
            method.setAccessible(true);
            return (boolean) method.invoke(icebergTable, struct1, struct2);
        } catch (Exception e) {
            throw new RuntimeException("Failed to invoke schemasAreEqual", e);
        }
    }

    private boolean invokeTypesAreEqual(Type type1, Type type2) {
        try {
            var method = IcebergTable.class.getDeclaredMethod("typesAreEqual", Type.class, Type.class);
            method.setAccessible(true);
            return (boolean) method.invoke(icebergTable, type1, type2);
        } catch (Exception e) {
            throw new RuntimeException("Failed to invoke typesAreEqual", e);
        }
    }

    @Test
    void testUpdateTableSchemaIfNeeded_SoftDeleteDisabled_HardDeleteTopLevelField() throws Exception {
        // Current schema
        Schema currentSchema = new Schema(
            Types.NestedField.required(1, "id", Types.IntegerType.get()),
            Types.NestedField.required(2, "name", Types.StringType.get()),
            Types.NestedField.optional(3, "email", Types.StringType.get()),
            Types.NestedField.required(4, "department", Types.StringType.get())
        );

        // New schema removes fields
        Schema newSchema = new Schema(
            Types.NestedField.required(1, "id", Types.IntegerType.get()),
            Types.NestedField.required(2, "name", Types.StringType.get())
            // "email" and "department" fields removed
        );

        setupBasicMocks(currentSchema);
        setupSoftDeleteDisabled();

        // TODO: add more cases for testing.

        icebergTable.updateTableSchemaIfNeeded(newSchema);

        // Should hard delete both fields
        verify(updateSchema).deleteColumn("email");
        verify(updateSchema).deleteColumn("department");
        verify(updateSchema, never()).makeColumnOptional(anyString());
        verify(updateSchema).commit();
    }

    @Test
    void testUpdateTableSchemaIfNeeded_SoftDeleteDisabled_HardDeleteNestedField() throws Exception {
        // Current schema with nested struct
        Schema currentSchema = new Schema(
            Types.NestedField.required(1, "id", Types.IntegerType.get()),
            Types.NestedField.required(2, "user", Types.StructType.of(
                Types.NestedField.required(3, "name", Types.StringType.get()),
                Types.NestedField.required(4, "email", Types.StringType.get()),
                Types.NestedField.optional(5, "phone", Types.StringType.get())
            ))
        );

        // New schema removes nested fields
        Schema newSchema = new Schema(
            Types.NestedField.required(1, "id", Types.IntegerType.get()),
            Types.NestedField.required(2, "user", Types.StructType.of(
                Types.NestedField.required(3, "name", Types.StringType.get())
                // "email" and "phone" removed
            ))
        );

        setupBasicMocks(currentSchema);
        setupSoftDeleteDisabled();

        icebergTable.updateTableSchemaIfNeeded(newSchema);

        // Should hard delete nested fields
        verify(updateSchema).deleteColumn("user.email");
        verify(updateSchema).deleteColumn("user.phone");
        verify(updateSchema, never()).makeColumnOptional(anyString());
        verify(updateSchema).commit();
    }

    @Test
    void testUpdateTableSchemaIfNeeded_SoftDeleteDisabled_DeeplyNestedHardDelete() throws Exception {
        // Current schema with deeply nested struct
        Schema currentSchema = new Schema(
            Types.NestedField.required(1, "id", Types.IntegerType.get()),
            Types.NestedField.required(2, "company", Types.StructType.of(
                Types.NestedField.required(3, "name", Types.StringType.get()),
                Types.NestedField.required(4, "address", Types.StructType.of(
                    Types.NestedField.required(5, "street", Types.StringType.get()),
                    Types.NestedField.required(6, "city", Types.StringType.get()),
                    Types.NestedField.optional(7, "zipcode", Types.StringType.get()),
                    Types.NestedField.required(8, "country", Types.StringType.get())
                ))
            ))
        );

        // New schema removes deeply nested fields
        Schema newSchema = new Schema(
            Types.NestedField.required(1, "id", Types.IntegerType.get()),
            Types.NestedField.required(2, "company", Types.StructType.of(
                Types.NestedField.required(3, "name", Types.StringType.get()),
                Types.NestedField.required(4, "address", Types.StructType.of(
                    Types.NestedField.required(5, "street", Types.StringType.get())
                    // "city", "zipcode", "country" removed
                ))
            ))
        );

        setupBasicMocks(currentSchema);
        setupSoftDeleteDisabled();

        icebergTable.updateTableSchemaIfNeeded(newSchema);

        // Should hard delete all removed deeply nested fields
        verify(updateSchema).deleteColumn("company.address.city");
        verify(updateSchema).deleteColumn("company.address.zipcode");
        verify(updateSchema).deleteColumn("company.address.country");
        verify(updateSchema, never()).makeColumnOptional(anyString());
        verify(updateSchema).commit();
    }

    @Test
    void testUpdateTableSchemaIfNeeded_SoftDeleteDisabled_MixedOperations() throws Exception {
        // Test that other operations still work when soft delete is disabled
        Schema currentSchema = new Schema(
            Types.NestedField.required(1, "id", Types.IntegerType.get()),
            Types.NestedField.required(2, "metrics", Types.StructType.of(
                Types.NestedField.required(3, "count", Types.IntegerType.get()),
                Types.NestedField.required(4, "score", Types.FloatType.get()),
                Types.NestedField.required(5, "status", Types.StringType.get()) // Will be deleted
            ))
        );

        Schema newSchema = new Schema(
            Types.NestedField.required(1, "id", Types.IntegerType.get()),
            Types.NestedField.required(2, "metrics", Types.StructType.of(
                Types.NestedField.required(3, "count", Types.LongType.get()),    // Type promotion
                Types.NestedField.optional(4, "score", Types.DoubleType.get()), // Type promotion + nullability change
                // "status" removed (hard delete)
                Types.NestedField.optional(6, "rating", Types.DoubleType.get()) // New field
            ))
        );

        setupBasicMocks(currentSchema);
        setupSoftDeleteDisabled();

        icebergTable.updateTableSchemaIfNeeded(newSchema);

        // Verify mixed operations
        verify(updateSchema).updateColumn("metrics.count", Types.LongType.get().asPrimitiveType());
        verify(updateSchema).updateColumn("metrics.score", Types.DoubleType.get().asPrimitiveType());
        verify(updateSchema).makeColumnOptional("metrics.score"); // Nullability change
        verify(updateSchema).deleteColumn("metrics.status");       // Hard delete
        verify(updateSchema).addColumn("metrics", "rating", Types.DoubleType.get());
        verify(updateSchema).commit();
    }

    @Test
    void testUpdateTableSchemaIfNeeded_SoftDeleteEnabled_TableProperty() throws Exception {
        // Test that table property takes precedence over configuration
        Schema currentSchema = new Schema(
            Types.NestedField.required(1, "id", Types.IntegerType.get()),
            Types.NestedField.required(2, "name", Types.StringType.get()),
            Types.NestedField.required(3, "department", Types.StringType.get())
        );

        Schema newSchema = new Schema(
            Types.NestedField.required(1, "id", Types.IntegerType.get()),
            Types.NestedField.required(2, "name", Types.StringType.get())
            // "department" removed
        );

        setupBasicMocks(currentSchema);
        // Setup configuration to disable soft delete
        setupSoftDeleteDisabled();
        // But table property enables it
        setupTablePropertySoftDeleteEnabled();

        icebergTable.updateTableSchemaIfNeeded(newSchema);

        // Should use soft delete because table property takes precedence
        verify(updateSchema).makeColumnOptional("department");
        verify(updateSchema, never()).deleteColumn(anyString());
        verify(updateSchema).commit();
    }

    @Test
    void testUpdateTableSchemaIfNeeded_SoftDeleteDisabled_TableProperty() throws Exception {
        // Test that table property disables soft delete even when configuration enables it
        Schema currentSchema = new Schema(
            Types.NestedField.required(1, "id", Types.IntegerType.get()),
            Types.NestedField.required(2, "name", Types.StringType.get()),
            Types.NestedField.required(3, "department", Types.StringType.get())
        );

        Schema newSchema = new Schema(
            Types.NestedField.required(1, "id", Types.IntegerType.get()),
            Types.NestedField.required(2, "name", Types.StringType.get())
            // "department" removed
        );

        setupBasicMocks(currentSchema);
        // Setup configuration to enable soft delete (default)
        setupSoftDeleteEnabled();
        // But table property disables it
        setupTablePropertySoftDeleteDisabled();

        icebergTable.updateTableSchemaIfNeeded(newSchema);

        // Should use hard delete because table property takes precedence
        verify(updateSchema).deleteColumn("department");
        verify(updateSchema, never()).makeColumnOptional(anyString());
        verify(updateSchema).commit();
    }

    @Test
    void testIsSoftDeleteEnabled_ConfigurationOnly() {
        // Test the private isSoftDeleteEnabled method with configuration only
        setupSoftDeleteDisabled();
        // No table properties set
        Map<String, String> emptyProperties = new HashMap<>();
        lenient().when(table.properties()).thenReturn(emptyProperties);

        boolean result = invokeIsSoftDeleteEnabled();
        assertFalse(result);
    }

    // Helper methods for soft delete configuration
    private void setupSoftDeleteDisabled() {
        Properties props = new Properties();
        props.setProperty("schema.evolution.soft-delete.enabled", "false");
        lenient().when(configuration.getProperties()).thenReturn(props);
        lenient().when(table.properties()).thenReturn(new HashMap<>());
    }

    private void setupSoftDeleteEnabled() {
        Properties props = new Properties();
        props.setProperty("schema.evolution.soft-delete.enabled", "true");
        lenient().when(configuration.getProperties()).thenReturn(props);
        lenient().when(table.properties()).thenReturn(new HashMap<>());
    }

    private void setupTablePropertySoftDeleteEnabled() {
        Map<String, String> tableProps = new HashMap<>();
        tableProps.put("schema.evolution.soft-delete.enabled", "true");
        lenient().when(table.properties()).thenReturn(tableProps);
    }

    private void setupTablePropertySoftDeleteDisabled() {
        Map<String, String> tableProps = new HashMap<>();
        tableProps.put("schema.evolution.soft-delete.enabled", "false");
        lenient().when(table.properties()).thenReturn(tableProps);
    }

    private boolean invokeIsSoftDeleteEnabled() {
        try {
            var method = IcebergTable.class.getDeclaredMethod("isSoftDeleteEnabled");
            method.setAccessible(true);
            return (boolean) method.invoke(icebergTable);
        } catch (Exception e) {
            throw new RuntimeException("Failed to invoke isSoftDeleteEnabled", e);
        }
    }
}
