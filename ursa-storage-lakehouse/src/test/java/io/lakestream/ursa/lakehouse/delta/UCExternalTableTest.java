/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.lakehouse.delta;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.databricks.sdk.service.catalog.TableInfo;
import io.delta.kernel.Snapshot;
import io.delta.kernel.data.Row;
import io.delta.kernel.types.LongType;
import io.delta.kernel.types.StringType;
import io.delta.kernel.types.StructField;
import io.delta.kernel.types.StructType;
import io.lakestream.ursa.lakehouse.LakehouseConfiguration;
import io.lakestream.ursa.lakehouse.catalog.unity.MockUnityCatalog;
import io.lakestream.ursa.lakehouse.catalog.unity.UnityCatalogApi;
import io.lakestream.ursa.lakehouse.catalog.unity.UnityTableIdentifier;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Tests for UCExternalTable using MockUnityCatalog (local filesystem-backed mock).
 */
class UCExternalTableTest {

    @TempDir
    Path tempDir;

    private LakehouseConfiguration config;

    @AfterEach
    void tearDown() {
        MockUnityCatalog.resetInstance();
    }

    @BeforeEach
    void setUp() {
        Properties props = new Properties();
        props.put("storagePath", tempDir.toString());
        props.put("partitionKey", "none");
        props.put("mockUnityCatalog", "true");
        props.put("unityCatalogUri", "http://localhost:8080");
        props.put("unityCatalogName", "test-catalog");
        props.put("mockedUnityCatalogRootStorage", tempDir.toString());
        props.put("streamTableMode", "EXTERNAL");
        config = new LakehouseConfiguration(props);
    }

    @Test
    void testConstructorWithMockCatalog() {
        String topic = "ns/test-topic";
        UCExternalTable table = new UCExternalTable(config, topic);
        assertNotNull(table);
        assertNotNull(table.getTableLocation());
    }

    @Test
    void testCreateDeltaTableAndVerifyExists() {
        String topic = "ns/create-test";
        UCExternalTable table = new UCExternalTable(config, topic);

        StructType schema = new StructType()
            .add("id", LongType.LONG)
            .add("name", StringType.STRING);

        table.createDeltaTable(null, schema);

        assertTrue(table.tableExists());
    }

    @Test
    void testCreateDeltaTableWithSchemaVersion() {
        String topic = "ns/schema-ver-test";
        UCExternalTable table = new UCExternalTable(config, topic);

        StructType schema = new StructType()
            .add("id", LongType.LONG);

        table.createDeltaTable(1L, schema);

        assertTrue(table.tableExists());
        Snapshot snapshot = table.getLatestSnapshot();
        assertNotNull(snapshot);
    }

    @Test
    void testGetLatestSnapshotBeforeTableCreated() {
        String topic = "ns/no-table-test";
        UCExternalTable table = new UCExternalTable(config, topic);

        // table field is null when using single-arg constructor without TableInfo
        assertNull(table.getLatestSnapshot());
    }

    @Test
    void testCommitSnapshotEmptyActions() {
        String topic = "ns/commit-empty";
        UCExternalTable table = new UCExternalTable(config, topic);

        StructType schema = new StructType()
            .add("id", LongType.LONG);
        table.createDeltaTable(null, schema);

        // Should not throw
        table.commitSnapshot(Collections.emptyList());
    }

    @Test
    void testCommitSnapshotWithActions() {
        String topic = "ns/commit-test";
        UCExternalTable table = new UCExternalTable(config, topic);

        StructType schema = new StructType()
            .add("id", LongType.LONG);
        table.createDeltaTable(null, schema);

        Row addAction = DeltaTableUtils.buildAddFileAction(
            "test-file.parquet", 1024, System.currentTimeMillis(),
            Collections.emptyMap(), true, null, Collections.emptyMap());

        table.commitSnapshot(List.of(addAction));
        assertNotNull(table.getLatestSnapshot());
    }

    @Test
    void testCreateTableIdempotent() {
        String topic = "ns/idempotent-test";
        UCExternalTable table = new UCExternalTable(config, topic);

        StructType schema = new StructType()
            .add("id", LongType.LONG);

        table.createDeltaTable(null, schema);
        // Second call should not throw
        table.createDeltaTable(null, schema);

        assertTrue(table.tableExists());
    }

    @Test
    void testSchemaEvolution() throws Exception {
        String topic = "ns/evolve-test";
        UCExternalTable table = new UCExternalTable(config, topic);

        StructType schemaV1 = new StructType()
            .add(new StructField("id", LongType.LONG, false));
        table.createDeltaTable(1L, schemaV1);

        StructType schemaV2 = new StructType()
            .add(new StructField("id", LongType.LONG, false))
            .add(new StructField("name", StringType.STRING, true));
        table.evolveSchemaWithVersion(2L, schemaV2);

        StructType resultSchema = table.getLatestSnapshot().getSchema();
        assertEquals(2, resultSchema.length());
    }

    @Test
    void testConstructorWithTableInfoSetsUnityTableInParent() {
        // First create a table via mock catalog so we have a TableInfo
        String topic = "ns/parent-field-test";
        UCExternalTable table1 = new UCExternalTable(config, topic);
        StructType schema = new StructType().add("id", LongType.LONG);
        table1.createDeltaTable(null, schema);

        // Now get the TableInfo from the mock catalog
        Optional<TableInfo> tableInfoOpt =
            io.lakestream.ursa.lakehouse.catalog.unity.UnityCatalogApi.getInstance(config)
                .getTable(config.getUnityCatalogName(), UnityTableIdentifier.parse(topic));
        assertTrue(tableInfoOpt.isPresent());

        // Create UCExternalTable using the two-arg constructor with TableInfo
        TableInfo tableInfo = tableInfoOpt.get();
        tableInfo.setTableId("test-id");
        UCExternalTable table2 = new UCExternalTable(config, topic, tableInfo);

        // Verify the table is properly initialized - getLatestSnapshot should work
        // because the parent's unityTable field is set (not shadowed)
        assertNotNull(table2.getLatestSnapshot());
        assertTrue(table2.tableExists());
    }

    @Test
    void testBuildAddFileActionUsesRelativePaths() {
        String topic = "ns/add-file-test";
        UCExternalTable table = new UCExternalTable(config, topic);

        io.lakestream.ursa.lakehouse.writer.ParquetFileStat fileStat =
            io.lakestream.ursa.lakehouse.writer.ParquetFileStat.builder()
                .filePath("part-0.parquet")
                .fileSize(1024L)
                .partitionValues(Collections.emptyMap())
                .stats(null)
                .tags(Collections.emptyMap())
                .deltaFiles(Collections.emptyList())
                .build();

        List<Row> rows = table.buildAddFileAction(List.of(fileStat));
        // UCTable.buildAddFileAction iterates deltaFiles, which is empty here
        assertTrue(rows.isEmpty());
    }

    @Test
    void testDifferentDeltaCatalogsUseDifferentMockCatalogInstances() {
        String topic = "ns/multi-catalog-test";

        Properties alphaProps = new Properties();
        alphaProps.put("storagePath", tempDir.toString());
        alphaProps.put("partitionKey", "none");
        alphaProps.put("mockUnityCatalog", "true");
        alphaProps.put("streamTableMode", "EXTERNAL");
        alphaProps.put("catalog.name", "alpha");
        alphaProps.put("delta.catalog.alpha.unityCatalogName", "uc-alpha");
        alphaProps.put("delta.catalog.alpha.unityCatalogUri", "http://localhost:8080");
        alphaProps.put("delta.catalog.alpha.mockedUnityCatalogRootStorage", tempDir.resolve("alpha").toString());

        Properties betaProps = new Properties();
        betaProps.put("storagePath", tempDir.toString());
        betaProps.put("partitionKey", "none");
        betaProps.put("mockUnityCatalog", "true");
        betaProps.put("streamTableMode", "EXTERNAL");
        betaProps.put("catalog.name", "beta");
        betaProps.put("delta.catalog.beta.unityCatalogName", "uc-beta");
        betaProps.put("delta.catalog.beta.unityCatalogUri", "http://localhost:8080");
        betaProps.put("delta.catalog.beta.mockedUnityCatalogRootStorage", tempDir.resolve("beta").toString());

        UCExternalTable alphaTable = new UCExternalTable(new LakehouseConfiguration(alphaProps), topic);
        alphaTable.createDeltaTable(null, new StructType().add("id", LongType.LONG));

        UCExternalTable betaTable = new UCExternalTable(new LakehouseConfiguration(betaProps), topic);
        UnityCatalogApi alphaApi = UnityCatalogApi.getInstance(new LakehouseConfiguration(alphaProps));
        UnityCatalogApi betaApi = UnityCatalogApi.getInstance(new LakehouseConfiguration(betaProps));

        assertNull(betaTable.getLatestSnapshot());
        assertNotSame(alphaApi, betaApi);
        assertEquals(tempDir.resolve("alpha").toString(),
            alphaApi.getCatalog("ignored").orElseThrow().getStorageRoot());
        assertEquals(tempDir.resolve("beta").toString(),
            betaApi.getCatalog("ignored").orElseThrow().getStorageRoot());
    }

}
