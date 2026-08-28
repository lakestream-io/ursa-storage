/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.lakehouse.delta;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.delta.kernel.Snapshot;
import io.delta.kernel.data.Row;
import io.delta.kernel.types.LongType;
import io.delta.kernel.types.StringType;
import io.delta.kernel.types.StructField;
import io.delta.kernel.types.StructType;
import io.lakestream.ursa.lakehouse.LakehouseConfiguration;
import io.lakestream.ursa.lakehouse.iceberg.exception.SchemaEvolutionException;
import io.lakestream.ursa.lakehouse.writer.ParquetFileStat;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class DirectExternalTableTest {

    @TempDir
    Path tempDir;

    private LakehouseConfiguration config;

    @BeforeEach
    void setUp() {
        Properties props = new Properties();
        props.put("directExternalStoragePath", tempDir.toUri().toString());
        props.put("partitionKey", "none");
        props.put("streamTableMode", "EXTERNAL");
        config = new LakehouseConfiguration(props);
    }

    @Test
    void testCreateDeltaTableAndVerifyExists() {
        String topic = "ns/create-test";
        DirectExternalTable table = new DirectExternalTable(config, topic);

        StructType schema = new StructType()
            .add("id", LongType.LONG)
            .add("name", StringType.STRING);

        table.createDeltaTable(null, schema);

        assertTrue(table.tableExists());
    }

    @Test
    void testCreateDeltaTableWithSchemaVersion() {
        String topic = "ns/schema-ver-test";
        DirectExternalTable table = new DirectExternalTable(config, topic);

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
        DirectExternalTable table = new DirectExternalTable(config, topic);

        assertNull(table.getLatestSnapshot());
    }

    @Test
    void testCommitSnapshotWithActions() {
        String topic = "ns/commit-test";
        DirectExternalTable table = new DirectExternalTable(config, topic);

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
    void testSchemaEvolution() throws Exception {
        String topic = "ns/evolve-test";
        DirectExternalTable table = new DirectExternalTable(config, topic);

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
    void testSchemaEvolutionRejectsIncompatibleTypeChangeByDefault() throws Exception {
        String topic = "ns/evolve-incompatible-type-default-test";
        DirectExternalTable table = new DirectExternalTable(config, topic);

        StructType schemaV1 = new StructType()
            .add(new StructField("raised", LongType.LONG, true));
        table.createDeltaTable(1L, schemaV1);

        StructType schemaV2 = new StructType()
            .add(new StructField("raised", StringType.STRING, true));

        assertThrows(SchemaEvolutionException.class, () -> table.evolveSchemaWithVersion(2L, schemaV2));
    }

    @Test
    void testBuildAddFileActionUsesDeltaFiles() {
        String topic = "ns/add-file-test";
        DirectExternalTable table = new DirectExternalTable(config, topic);

        ParquetFileStat deltaFile = ParquetFileStat.builder()
            .filePath("part-0.parquet")
            .fileSize(1024L)
            .partitionValues(Collections.emptyMap())
            .stats(null)
            .tags(Collections.emptyMap())
            .build();
        ParquetFileStat wrapper = ParquetFileStat.fromDeltaFiles(List.of(deltaFile), Collections.emptyMap());

        List<Row> rows = table.buildAddFileAction(List.of(wrapper));
        assertEquals(1, rows.size());
    }

    @Test
    void testGetTableHadoopConfigurationUsesCustomerProvidedHadoopSettings() {
        Properties props = new Properties();
        props.put("directExternalStoragePath", "s3a://customer-bucket/customer-prefix");
        props.put("partitionKey", "none");
        props.put("streamTableMode", "EXTERNAL");
        props.put("cloudStorageEndpoint", "http://localhost:8080");
        props.put("compactionBucketRegion", "us-west-1");
        props.put("hadoop.fs.s3a.endpoint", "http://minio.internal:9000");
        props.put("hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider");
        props.put("hadoop.fs.s3a.access.key", "customer-access-key");
        props.put("hadoop.fs.s3a.secret.key", "customer-secret-key");

        DirectExternalTable table = new DirectExternalTable(new LakehouseConfiguration(props), "ns/topic");
        Configuration hadoopConf = table.getTableHadoopConfiguration();

        assertEquals("http://minio.internal:9000", hadoopConf.get("fs.s3a.endpoint"));
        assertEquals("org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
            hadoopConf.get("fs.s3a.aws.credentials.provider"));
        assertEquals("customer-access-key", hadoopConf.get("fs.s3a.access.key"));
        assertEquals("customer-secret-key", hadoopConf.get("fs.s3a.secret.key"));
        assertEquals("us-west-1", hadoopConf.get("fs.s3a.endpoint.region"));
        assertNull(hadoopConf.get("fs.s3a.aws.region"));
    }

    @Test
    void testConstructorRequiresDirectExternalStoragePath() {
        Properties props = new Properties();
        props.put("partitionKey", "none");
        props.put("streamTableMode", "EXTERNAL");

        LakehouseConfiguration configuration = new LakehouseConfiguration(props);

        IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
            () -> new DirectExternalTable(configuration, "ns/topic"));
        assertEquals("directExternalStoragePath must be configured for delta direct external table.",
            e.getMessage());
    }

    @Test
    void testDirectExternalStoragePathNormalizesS3SchemeAndTrailingSlash() {
        Properties props = new Properties();
        props.put("directExternalStoragePath", "s3://customer-bucket/customer-prefix/");
        props.put("partitionKey", "none");
        props.put("streamTableMode", "EXTERNAL");

        DirectExternalTable table = new DirectExternalTable(new LakehouseConfiguration(props),
            "ns/topic");

        assertTrue(table.getTableLocation().startsWith("s3a://customer-bucket/customer-prefix"));
    }

    @Test
    void testDirectExternalStoragePathWithoutSchemeUsesBackendType() {
        Properties props = new Properties();
        props.put("directExternalStoragePath", "customer-bucket/customer-prefix");
        props.put("compactionBackendStorageType", "S3");
        props.put("partitionKey", "none");
        props.put("streamTableMode", "EXTERNAL");

        DirectExternalTable table = new DirectExternalTable(new LakehouseConfiguration(props),
            "ns/topic");

        assertTrue(table.getTableLocation().startsWith("s3a://customer-bucket/customer-prefix"));
    }
}
