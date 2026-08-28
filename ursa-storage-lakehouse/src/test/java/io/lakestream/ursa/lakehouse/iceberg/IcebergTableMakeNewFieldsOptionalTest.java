/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.lakehouse.iceberg;

import static io.lakestream.ursa.lakehouse.LakehouseConfiguration.MAKE_NEW_FIELDS_OPTIONAL;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.lakestream.ursa.lakehouse.LakehouseConfiguration;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import org.apache.iceberg.Schema;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.inmemory.InMemoryCatalog;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag("lakehouse")
public class IcebergTableMakeNewFieldsOptionalTest {

    private Catalog catalog;
    private TableIdentifier tableIdentifier;
    private IcebergTable icebergTable;

    @BeforeEach
    public void setUp() {
        catalog = new InMemoryCatalog();
        catalog.initialize("test-catalog", new HashMap<>());
        Namespace namespace = Namespace.of("test_namespace");
        if (catalog instanceof SupportsNamespaces sn) {
            sn.createNamespace(namespace);
        }
        tableIdentifier = TableIdentifier.of(namespace, "test_optional_table");
    }

    @AfterEach
    public void tearDown() {
        if (icebergTable != null) {
            icebergTable.close();
        }
    }

    private Schema initialSchema() {
        return new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.required(2, "labels", Types.ListType.ofRequired(3, Types.StringType.get())),
            Types.NestedField.optional(4, "details", Types.StructType.of(
                Types.NestedField.optional(5, "category", Types.StringType.get())
            ))
        );
    }

    private Schema evolvedSchema() {
        return new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.required(2, "labels", Types.ListType.ofRequired(3, Types.StringType.get())),
            Types.NestedField.optional(4, "details", Types.StructType.of(
                Types.NestedField.optional(5, "category", Types.StringType.get()),
                Types.NestedField.required(20, "processing_notes",
                    Types.ListType.ofRequired(21, Types.StringType.get())),
                Types.NestedField.optional(22, "source_system", Types.StringType.get())
            )),
            Types.NestedField.required(30, "audit_trail", Types.ListType.ofRequired(31,
                Types.StructType.of(
                    Types.NestedField.required(32, "action", Types.StringType.get()),
                    Types.NestedField.required(33, "timestamp", Types.TimestampType.withZone())
                ))),
            Types.NestedField.optional(40, "version_info", Types.StringType.get())
        );
    }

    @Test
    public void testNewRequiredFieldsAddedAsOptional_flagOn() throws Exception {
        LakehouseConfiguration configuration = createConfiguration(true);
        TableOptions tableOptions = TableOptions.builder().schema(initialSchema()).build();
        icebergTable = new IcebergTable(catalog, tableIdentifier, tableOptions, configuration);
        icebergTable.create(tableOptions);

        icebergTable.updateTableSchemaIfNeeded(evolvedSchema());

        Schema updated = icebergTable.getTable().schema();
        assertNotNull(updated.findField("audit_trail"));
        assertTrue(updated.findField("audit_trail").isOptional());
        assertNotNull(updated.findField("details.processing_notes"));
        assertTrue(updated.findField("details.processing_notes").isOptional());
        assertTrue(updated.findField("version_info").isOptional());
        assertTrue(updated.findField("labels").isRequired());
    }

    @Test
    public void testRequiredFieldNestedInNewOptionalStructMadeOptional_flagOn() throws Exception {
        // Reproduces the production failure: a brand-new optional struct ('retry_info') whose nested
        // field ('attempt_number') is required. A REST catalog rejects the add with
        // "Cannot add non-nullable field attempt_number", so make-new-fields-optional must
        // recursively optionalize fields nested inside newly-added structs.
        LakehouseConfiguration configuration = createConfiguration(true);
        Schema initial = new Schema(Types.NestedField.required(1, "id", Types.LongType.get()));
        TableOptions tableOptions = TableOptions.builder().schema(initial).build();
        icebergTable = new IcebergTable(catalog, tableIdentifier, tableOptions, configuration);
        icebergTable.create(tableOptions);

        Schema evolved = new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.optional(10, "retry_info", Types.StructType.of(
                Types.NestedField.required(11, "attempt_number", Types.IntegerType.get()),
                Types.NestedField.optional(12, "last_error", Types.StringType.get())
            )));

        icebergTable.updateTableSchemaIfNeeded(evolved);

        Schema updated = icebergTable.getTable().schema();
        assertNotNull(updated.findField("retry_info.attempt_number"));
        assertTrue(updated.findField("retry_info").isOptional());
        assertTrue(updated.findField("retry_info.attempt_number").isOptional());
    }

    @Test
    public void testRequiredFieldNestedInNewListOfStructMadeOptional_flagOn() throws Exception {
        // A new field that is a list of structs with required nested fields must also have those
        // nested fields optionalized (e.g. audit_trail.element.action).
        LakehouseConfiguration configuration = createConfiguration(true);
        Schema initial = new Schema(Types.NestedField.required(1, "id", Types.LongType.get()));
        TableOptions tableOptions = TableOptions.builder().schema(initial).build();
        icebergTable = new IcebergTable(catalog, tableIdentifier, tableOptions, configuration);
        icebergTable.create(tableOptions);

        Schema evolved = new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.required(30, "audit_trail", Types.ListType.ofRequired(31,
                Types.StructType.of(
                    Types.NestedField.required(32, "action", Types.StringType.get()),
                    Types.NestedField.required(33, "timestamp", Types.TimestampType.withZone())
                ))));

        icebergTable.updateTableSchemaIfNeeded(evolved);

        Schema updated = icebergTable.getTable().schema();
        assertTrue(updated.findField("audit_trail").isOptional());
        assertNotNull(updated.findField("audit_trail.element.action"));
        assertTrue(updated.findField("audit_trail.element.action").isOptional());
        assertTrue(updated.findField("audit_trail.element.timestamp").isOptional());
    }

    @Test
    public void testNewRequiredFieldRejected_flagOff() {
        LakehouseConfiguration configuration = createConfiguration(false);
        TableOptions tableOptions = TableOptions.builder().schema(initialSchema()).build();
        icebergTable = new IcebergTable(catalog, tableIdentifier, tableOptions, configuration);
        icebergTable.create(tableOptions);

        assertThrows(Exception.class, () -> icebergTable.updateTableSchemaIfNeeded(evolvedSchema()));
    }

    @Test
    public void testIncompatibleTypeChangeStillRejected_flagOn() {
        LakehouseConfiguration configuration = createConfiguration(true);
        Schema initial = new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.optional(2, "value", Types.StringType.get()));
        TableOptions tableOptions = TableOptions.builder().schema(initial).build();
        icebergTable = new IcebergTable(catalog, tableIdentifier, tableOptions, configuration);
        icebergTable.create(tableOptions);

        Schema incompatible = new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.optional(2, "value", Types.IntegerType.get()));
        assertThrows(Exception.class, () -> icebergTable.updateTableSchemaIfNeeded(incompatible));
    }

    @Test
    public void testMakeExistingFieldRequiredStillRejected_flagOn() {
        LakehouseConfiguration configuration = createConfiguration(true);
        Schema initial = new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.optional(2, "name", Types.StringType.get()));
        TableOptions tableOptions = TableOptions.builder().schema(initial).build();
        icebergTable = new IcebergTable(catalog, tableIdentifier, tableOptions, configuration);
        icebergTable.create(tableOptions);

        Schema tightened = new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.required(2, "name", Types.StringType.get()));
        assertThrows(Exception.class, () -> icebergTable.updateTableSchemaIfNeeded(tightened));
    }

    private LakehouseConfiguration createConfiguration(boolean makeNewFieldsOptional) {
        Properties properties = new Properties();
        properties.setProperty("cluster", "test-cluster");
        properties.setProperty(MAKE_NEW_FIELDS_OPTIONAL, String.valueOf(makeNewFieldsOptional));

        return new LakehouseConfiguration(properties) {
            @Override
            public Properties getProperties() {
                return properties;
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
