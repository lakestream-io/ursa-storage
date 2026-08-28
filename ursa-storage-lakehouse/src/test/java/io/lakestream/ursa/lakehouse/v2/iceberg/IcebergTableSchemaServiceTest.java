/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.lakehouse.v2.iceberg;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import io.lakestream.ursa.lakehouse.LakehouseConfiguration;
import io.lakestream.ursa.lakehouse.iceberg.IcebergCatalogBackendType;
import io.lakestream.ursa.lakehouse.iceberg.IcebergTable;
import io.lakestream.ursa.lakehouse.iceberg.TableOptions;
import io.lakestream.ursa.lakehouse.iceberg.exception.SchemaMappingException;
import java.time.Duration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.TreeMap;
import org.apache.iceberg.Schema;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.inmemory.InMemoryCatalog;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag("lakehouse")
public class IcebergTableSchemaServiceTest {

    private Catalog catalog;
    private LakehouseConfiguration configuration;
    private TableIdentifier tableIdentifier;
    private IcebergTable icebergTable;

    @BeforeEach
    public void setUp() {
        catalog = new InMemoryCatalog();
        catalog.initialize("test-catalog", new HashMap<>());

        Namespace namespace = Namespace.of("svc_ns");
        if (catalog instanceof org.apache.iceberg.catalog.SupportsNamespaces) {
            ((org.apache.iceberg.catalog.SupportsNamespaces) catalog).createNamespace(namespace);
        }

        tableIdentifier = TableIdentifier.of(namespace, "svc_table");

        Properties properties = new Properties();
        properties.setProperty("cluster", "test-cluster");
        configuration = createTestConfiguration(properties);

        icebergTable = new IcebergTable(catalog, tableIdentifier, null, configuration);
    }

    @AfterEach
    public void tearDown() {
        if (icebergTable != null) {
            icebergTable.close();
        }
    }

    @Test
    public void testEvolveCreatesTableAndMapsVersions() throws Exception {
        IcebergTableSchemaService service = new IcebergTableSchemaService(icebergTable, configuration);

        Schema v1 = new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.optional(2, "name", Types.StringType.get())
        );
        Schema v2 = new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.optional(2, "name", Types.StringType.get()),
            Types.NestedField.optional(3, "email", Types.StringType.get())
        );

        TreeMap<Long, Schema> schemas = new TreeMap<>();
        schemas.put(2L, v2);
        schemas.put(1L, v1);

        var results = service.evolveTableSchema(schemas);
        assertEquals(new HashSet<>(java.util.List.of(1L, 2L)), results);

        // Verify schema retrievable by version
        Schema s1 = service.getTableSchema(1L);
        Schema s2 = service.getTableSchema(2L);
        assertNotNull(s1);
        assertNotNull(s2);
        assertNotNull(s1.findField("id"));
        assertNull(s1.findField("email"));
        assertNotNull(s2.findField("email"));
    }

    @Test
    public void testEvolveContinuesOnSchemaEvolutionException() throws Exception {
        IcebergTableSchemaService service = new IcebergTableSchemaService(icebergTable, configuration);

        Schema v1 = new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get())
        );
        // v2 tries to add a required field to existing table: should fail during evolution
        Schema v2Incompatible = new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.required(2, "name", Types.StringType.get())
        );
        // v3 adds optional field: should succeed
        Schema v3 = new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.optional(2, "name", Types.StringType.get())
        );

        TreeMap<Long, Schema> schemas = new TreeMap<>();
        schemas.put(1L, v1);
        schemas.put(2L, v2Incompatible);
        schemas.put(3L, v3);

        var results = service.evolveTableSchema(schemas);
        // v1 and v3 should succeed; v2 should fail but not stop others
        assertTrue(results.contains(1L));
        assertTrue(results.contains(3L));
        assertTrue(!results.contains(2L));

        // Check final schema has optional name
        Schema s3 = service.getTableSchema(3L);
        assertNotNull(s3);
        assertTrue(s3.findField("name").isOptional());
    }

    @Test
    public void testEvolveBreaksOnSchemaMappingException() throws Exception {
        IcebergTableSchemaService service = new IcebergTableSchemaService(icebergTable, configuration);

        // First evolve with v1 to create table and a valid mapping
        Schema v1 = new Schema(Types.NestedField.required(1, "id", Types.LongType.get()));
        TreeMap<Long, Schema> first = new TreeMap<>();
        first.put(1L, v1);
        var r1 = service.evolveTableSchema(first);
        assertTrue(r1.contains(1L));

        // Corrupt the schema mapping so that subsequent evolve throws SchemaMappingException
        icebergTable.getTable().updateProperties()
            .set("lakestream.schema.mapping", "{not-json}")
            .commit();

        // Now try to evolve v2 and v3; should break immediately and return empty set
        Schema v2 = new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.optional(2, "name", Types.StringType.get())
        );
        Schema v3 = new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.optional(2, "name", Types.StringType.get()),
            Types.NestedField.optional(3, "email", Types.StringType.get())
        );
        TreeMap<Long, Schema> next = new TreeMap<>();
        next.put(2L, v2);
        next.put(3L, v3);

        try {
            service.evolveTableSchema(next);
            fail();
        } catch (SchemaMappingException e) {
            // expected
        }

        // getTableSchema for unknown version should return null (mapping corrupted -> not found)
        try {
            service.getTableSchema(2L);
            fail();
        } catch (SchemaMappingException e) {
            // expected
        }
    }

    @Test
    public void testGetTableSchemaWhenTableNotExists() throws Exception {
        // New service with a fresh table identifier that does not exist
        Catalog freshCatalog = new InMemoryCatalog();
        freshCatalog.initialize("fresh-catalog", new HashMap<>());
        Namespace ns = Namespace.of("new_ns");
        if (freshCatalog instanceof org.apache.iceberg.catalog.SupportsNamespaces) {
            ((org.apache.iceberg.catalog.SupportsNamespaces) freshCatalog).createNamespace(ns);
        }
        TableIdentifier id = TableIdentifier.of(ns, "new_table");
        IcebergTable freshTable = new IcebergTable(freshCatalog, id, null, configuration);
        IcebergTableSchemaService service = new IcebergTableSchemaService(freshTable, configuration);

        Schema s = service.getTableSchema(1L);
        assertNull(s);
        freshTable.close();
    }

    @Test
    public void testGetTableSchemaWhenSchemaNotExists() throws Exception {
        Schema v1 = new Schema(Types.NestedField.required(1, "id", Types.LongType.get()));
        icebergTable.create(TableOptions.builder().schema(v1).build());
        IcebergTableSchemaService service = new IcebergTableSchemaService(icebergTable, configuration);
        Schema s = service.getTableSchema(1L);
        assertNull(s);
    }

    private LakehouseConfiguration createTestConfiguration(Properties properties) {
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
                // This suite verifies the schema-evolution service orchestration under strict
                // (reject) semantics; the optional-new-fields feature is covered by
                // IcebergTableMakeNewFieldsOptionalTest.
                return false;
            }
        };
    }
}
