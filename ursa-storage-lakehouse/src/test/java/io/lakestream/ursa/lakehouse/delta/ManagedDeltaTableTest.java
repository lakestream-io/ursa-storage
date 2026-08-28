/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.lakehouse.delta;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.delta.kernel.Snapshot;
import io.delta.kernel.data.Row;
import io.delta.kernel.internal.SnapshotImpl;
import io.delta.kernel.types.LongType;
import io.delta.kernel.types.StringType;
import io.delta.kernel.types.StructField;
import io.delta.kernel.types.StructType;
import io.lakestream.ursa.lakehouse.LakehouseConfiguration;
import io.lakestream.ursa.lakehouse.iceberg.exception.SchemaMappingException;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class ManagedDeltaTableTest {

    @TempDir
    Path tempDir;

    private LakehouseConfiguration config;
    private ManagedDeltaTable table;

    @BeforeEach
    void setUp() {
        Properties props = new Properties();
        props.put("storagePath", tempDir.toString());
        props.put("partitionKey", "none");
        config = new LakehouseConfiguration(props);
        table = new ManagedDeltaTable(config, "test-topic");
    }

    @Test
    void testCreateAndCheckTableExists() {
        assertFalse(table.tableExists());

        StructType schema = new StructType()
            .add("id", LongType.LONG)
            .add("name", StringType.STRING);
        table.createDeltaTable(null, schema);

        assertTrue(table.tableExists());
    }

    @Test
    void testCreateTableWithSchemaVersion() {
        StructType schema = new StructType()
            .add("id", LongType.LONG);
        table.createDeltaTable(1L, schema);

        assertTrue(table.tableExists());
        Snapshot snapshot = table.getLatestSnapshot();
        assertNotNull(snapshot);
        Map<String, String> configuration = ((SnapshotImpl) snapshot).getMetadata().getConfiguration();
        assertEquals("[1]", configuration.get(DeltaTable.LAKESTREAM_SCHEMA_MAPPING));
    }

    @Test
    void testGetLatestSnapshot() {
        StructType schema = new StructType()
            .add("id", LongType.LONG);
        table.createDeltaTable(null, schema);

        Snapshot snapshot = table.getLatestSnapshot();
        assertNotNull(snapshot);
        StructType resultSchema = snapshot.getSchema();
        assertEquals(1, resultSchema.length());
    }

    @Test
    void testCommitSnapshotEmptyActions() {
        StructType schema = new StructType()
            .add("id", LongType.LONG);
        table.createDeltaTable(null, schema);

        // Should return without error
        table.commitSnapshot(Collections.emptyList());
    }

    @Test
    void testCommitSnapshotWithAddAction() {
        StructType schema = new StructType()
            .add("id", LongType.LONG);
        table.createDeltaTable(null, schema);

        Row addAction = DeltaTableUtils.buildAddFileAction(
            "test-file.parquet", 1024, System.currentTimeMillis(),
            Collections.emptyMap(), true, null, Collections.emptyMap());

        table.commitSnapshot(List.of(addAction));

        Snapshot snapshot = table.getLatestSnapshot();
        assertNotNull(snapshot);
    }

    @Test
    void testBuildAddFileAction() {
        List<Row> rows = table.buildAddFileAction(Collections.emptyList());
        assertTrue(rows.isEmpty());
    }

    @Test
    void testCreateTableIdempotent() {
        StructType schema = new StructType()
            .add("id", LongType.LONG);
        table.createDeltaTable(null, schema);

        // Second call should not throw
        table.createDeltaTable(null, schema);

        assertTrue(table.tableExists());
    }

    @Test
    void testGetSchemaMapping() throws Exception {
        StructType schema = new StructType()
            .add("id", LongType.LONG);
        table.createDeltaTable(1L, schema);

        Set<Long> mapping = table.getSchemaMapping();
        assertEquals(Set.of(1L), mapping);
    }

    @Test
    void testGetSchemaMappingWhenTableNotCreated() throws SchemaMappingException {
        Set<Long> schemaMapping = table.getSchemaMapping();
        assertTrue(schemaMapping.isEmpty());
    }

    @Test
    void testSchemaEvolution() throws Exception {
        StructType schemaV1 = new StructType()
            .add(new StructField("id", LongType.LONG, false));
        table.createDeltaTable(1L, schemaV1);

        StructType schemaV2 = new StructType()
            .add(new StructField("id", LongType.LONG, false))
            .add(new StructField("name", StringType.STRING, true));
        table.evolveSchemaWithVersion(2L, schemaV2);

        StructType resultSchema = table.getLatestSnapshot().getSchema();
        assertEquals(2, resultSchema.length());

        Set<Long> mapping = table.getSchemaMapping();
        assertEquals(Set.of(1L, 2L), mapping);
    }

    @Test
    void testSchemaEvolutionSkipsDuplicate() throws Exception {
        StructType schemaV1 = new StructType()
            .add(new StructField("id", LongType.LONG, false));
        table.createDeltaTable(1L, schemaV1);

        // Evolving with same version should be skipped
        StructType schemaV1Again = new StructType()
            .add(new StructField("id", LongType.LONG, false))
            .add(new StructField("extra", StringType.STRING, true));
        table.evolveSchemaWithVersion(1L, schemaV1Again);

        // Schema should not change since version 1 was already processed
        StructType resultSchema = table.getLatestSnapshot().getSchema();
        assertEquals(1, resultSchema.length());
    }

    @Test
    void testSchemaEvolutionSoftDeleteEnabled() throws Exception {
        Properties props = new Properties();
        props.put("storagePath", tempDir.toString());
        props.put("partitionKey", "none");
        props.put(DeltaTable.SCHEMA_EVOLUTION_SOFT_DELETE_ENABLED, "true");
        table = new ManagedDeltaTable(new LakehouseConfiguration(props), "test-topic-soft-delete");

        StructType schemaV1 = new StructType()
            .add(new StructField("id", LongType.LONG, false))
            .add(new StructField("name", StringType.STRING, false));
        table.createDeltaTable(1L, schemaV1);

        StructType schemaV2 = new StructType()
            .add(new StructField("id", LongType.LONG, false));
        table.evolveSchemaWithVersion(2L, schemaV2);

        StructType resultSchema = table.getLatestSnapshot().getSchema();
        StructField nameField = resultSchema.get("name");
        assertNotNull(nameField);
        assertTrue(nameField.isNullable());
        assertEquals(Set.of(1L, 2L), table.getSchemaMapping());
    }

    @Test
    void testSchemaEvolutionSoftDeleteDisabled() throws Exception {
        Properties props = new Properties();
        props.put("storagePath", tempDir.toString());
        props.put("partitionKey", "none");
        props.put(DeltaTable.SCHEMA_EVOLUTION_SOFT_DELETE_ENABLED, "false");
        table = new ManagedDeltaTable(new LakehouseConfiguration(props), "test-topic-hard-delete");

        StructType schemaV1 = new StructType()
            .add(new StructField("id", LongType.LONG, false))
            .add(new StructField("name", StringType.STRING, true));
        table.createDeltaTable(1L, schemaV1);

        StructType schemaV2 = new StructType()
            .add(new StructField("id", LongType.LONG, false));
        table.evolveSchemaWithVersion(2L, schemaV2);

        StructType resultSchema = table.getLatestSnapshot().getSchema();
        assertEquals(-1, resultSchema.indexOf("name"));
        assertEquals(Set.of(1L, 2L), table.getSchemaMapping());
    }
}
