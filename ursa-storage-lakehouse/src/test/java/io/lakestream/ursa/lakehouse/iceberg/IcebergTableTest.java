/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.lakehouse.iceberg;

import static io.lakestream.ursa.lakehouse.iceberg.IcebergTable.URSA_KEYS_PROPERTY;
import static org.apache.iceberg.TableProperties.METADATA_DELETE_AFTER_COMMIT_ENABLED;
import static org.apache.iceberg.TableProperties.PARQUET_COMPRESSION;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;

import io.lakestream.ursa.lakehouse.LakehouseConfiguration;
import io.lakestream.ursa.lakehouse.exception.LakehouseException;
import io.lakestream.ursa.lakehouse.iceberg.exception.SchemaEvolutionException;
import io.lakestream.ursa.lakehouse.iceberg.exception.SchemaMappingException;
import io.lakestream.ursa.lakehouse.utils.AvroSchemaUtilExtended;
import io.lakestream.ursa.lakehouse.utils.TableNameFormatUtils;
import io.lakestream.ursa.lakehouse.writer.ParquetFileStat;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.PartitionSpecParser;
import org.apache.iceberg.Table;
import org.apache.iceberg.Transaction;
import org.apache.iceberg.UpdateProperties;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.io.DataWriter;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.io.TaskWriter;
import org.apache.iceberg.io.WriteResult;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

@Slf4j
@Tag("lakehouse")
public class IcebergTableTest {
    @TempDir
    private Path baseDir;

    @Test
    public void testS3Identifier() {
        Properties properties = new Properties();
        properties.put("iceberg.catalog-backend", "s3Table");
        var configuration = new LakehouseConfiguration(properties);

        var namespace = Namespace.of("public", "default");
        var identifier = TableIdentifier.of(namespace, "test_table");
        var table = new IcebergTable(configuration, identifier);
        var tableIdentifier = table.getIdentifier();
        assertEquals("ursa_public_default", tableIdentifier.namespace().toString());
    }

    @Test
    public void testMultipleCatalogConfigurations() {
        Properties properties = new Properties();
        properties.put("iceberg.catalog-backend", "s3Table");
        properties.put("catalogOpsRetryMaxAttempts", "0");
        var configuration = new LakehouseConfiguration(properties);

        var namespace = Namespace.of("public", "default");
        var identifier = TableIdentifier.of(namespace, "test_table");
        var table = new IcebergTable(configuration, identifier);
        Catalog catalog = table.getCatalog();
        assertEquals("default", catalog.name());
        assertInstanceOf(HadoopCatalog.class, catalog);

        properties = new Properties();
        // catalog test
        properties.put("iceberg.catalog.test.type", "hadoop");
        properties.put("iceberg.catalog.test.warehouse", baseDir.toAbsolutePath().toString() + "/test");
        properties.put("iceberg.catalog.test.catalog-backend", "hadoop");

        // catalog staging
        properties.put("iceberg.catalog.staging.type", "hadoop");
        properties.put("iceberg.catalog.staging.warehouse", baseDir.toAbsolutePath().toString() + "/staging");
        properties.put("iceberg.catalog.staging.catalog-backend", "hadoop");

        // catalog prod
        properties.put("iceberg.catalog.prod.type", "hadoop");
        properties.put("iceberg.catalog.prod.warehouse", baseDir.toAbsolutePath().toString() + "/prod");
        properties.put("iceberg.catalog.prod.catalog-backend", "hadoop");

        // set default catalog
        properties.put("catalog.default", "test");
        properties.put("catalog.name", "test");
        properties.put("catalogOpsRetryMaxAttempts", "0");

        configuration = new LakehouseConfiguration(properties);
        // init table with test catalog
        table = new IcebergTable(configuration, identifier);
        catalog = table.getCatalog();
        assertEquals("test", catalog.name());
        assertInstanceOf(HadoopCatalog.class, catalog);
        assertTrue(catalog.toString().contains("location=" + baseDir.toAbsolutePath().toString() + "/test"));

        // init table with staging catalog
        properties.put("catalog.name", "staging");

        configuration = new LakehouseConfiguration(properties);
        table = new IcebergTable(configuration, identifier);
        catalog = table.getCatalog();
        assertEquals("staging", catalog.name());
        assertInstanceOf(HadoopCatalog.class, catalog);
        assertTrue(catalog.toString().contains("location=" + baseDir.toAbsolutePath().toString() + "/staging"));

        // init table with prod catalog
        properties.put("catalog.name", "prod");
        configuration = new LakehouseConfiguration(properties);
        table = new IcebergTable(configuration, identifier);
        catalog = table.getCatalog();
        assertEquals("prod", catalog.name());
        assertInstanceOf(HadoopCatalog.class, catalog);
        assertTrue(catalog.toString().contains("location=" + baseDir.toAbsolutePath().toString() + "/prod"));

        // init table with empty catalog name and fallback to default catalog
        properties.put("catalog.name", "");
        configuration = new LakehouseConfiguration(properties);
        table = new IcebergTable(configuration, identifier);
        catalog = table.getCatalog();
        assertEquals("default", catalog.name());
        assertInstanceOf(HadoopCatalog.class, catalog);
        assertTrue(catalog.toString().contains("location=" + baseDir.toAbsolutePath().toString() + "/test"));

        // init table with non-existing catalog name and fallback to default catalog
        properties.put("catalog.name", "non-existing");
        configuration = new LakehouseConfiguration(properties);
        table = new IcebergTable(configuration, identifier);
        catalog = table.getCatalog();
        assertEquals("non-existing", catalog.name());
        assertInstanceOf(HadoopCatalog.class, catalog);
        assertTrue(catalog.toString().contains("location=" + baseDir.toAbsolutePath().toString() + "/test"));
    }

    static final String SCHEMA_STR = """
         {
          "type": "record",
          "name": "IcebergAllTypes",
          "namespace": "com.example",
          "fields": [
            {"name": "boolean_field", "type": "boolean"},
            {"name": "int_field", "type": "int"},
            {"name": "long_field", "type": "long"},
            {"name": "float_field", "type": "float"},
            {"name": "double_field", "type": "double"},
            {
              "name": "decimal_field",
              "type": {
                "type": "bytes",
                "logicalType": "decimal",
                "precision": 10,
                "scale": 2
              }
            },
            {
              "name": "date_field",
              "type": {
                "type": "int",
                "logicalType": "date"
              }
            },
            {
              "name": "time_field",
              "type": {
                "type": "long",
                "logicalType": "time-micros"
              }
            },
            {
              "name": "timestamp_field",
              "type": {
                "type": "long",
                "logicalType": "timestamp-micros"
              }
            },
            {
              "name": "timestamptz_field",
              "type": {
                "type": "long",
                "logicalType": "timestamp-micros"
              },
              "adjustToUTC": true
            },
            {"name": "string_field", "type": "string"},
            {
              "name": "uuid_field",
              "type": {
                "type": "fixed",
                "name": "uuid",
                "size": 16,
                "logicalType": "uuid"
              }
            },
            {"name": "binary_field", "type": "bytes"},
            {
              "name": "fixed_field",
              "type": {
                "type": "fixed",
                "name": "FixedType",
                "size": 4
              }
            },
            {
              "name": "struct_field",
              "type": {
                "type": "record",
                "name": "StructType",
                "fields": [
                  {"name": "nested_int", "type": "int"},
                  {"name": "nested_string", "type": "string"}
                ]
              }
            },
            {
              "name": "list_field",
              "type": {
                "type": "array",
                "items": "string"
              }
            },
            {
              "name": "map_field",
              "type": {
                "type": "map",
                "values": "int"
              }
            }
          ]
        }
        """;

    static final String SCHEMA_STR_V2 = """
         {
          "type": "record",
          "name": "IcebergAllTypes",
          "namespace": "com.example",
          "fields": [
            {"name": "boolean_field", "type": "boolean"},
            {"name": "int_field", "type": "int"},
            {"name": "long_field", "type": "long"},
            {"name": "float_field", "type": "float"},
            {"name": "double_field", "type": "double"},
            {
               "name": "double_field_v2",
               "type": ["null", "double"],
               "default": null
            },
            {
              "name": "decimal_field",
              "type": {
                "type": "bytes",
                "logicalType": "decimal",
                "precision": 10,
                "scale": 2
              }
            },
            {
              "name": "date_field",
              "type": {
                "type": "int",
                "logicalType": "date"
              }
            },
            {
              "name": "time_field",
              "type": {
                "type": "long",
                "logicalType": "time-micros"
              }
            },
            {
              "name": "timestamp_field",
              "type": {
                "type": "long",
                "logicalType": "timestamp-micros"
              }
            },
            {
              "name": "timestamptz_field",
              "type": {
                "type": "long",
                "logicalType": "timestamp-micros"
              },
              "adjustToUTC": true
            },
            {"name": "string_field", "type": "string"},
            {
              "name": "uuid_field",
              "type": {
                "type": "fixed",
                "name": "uuid",
                "size": 16,
                "logicalType": "uuid"
              }
            },
            {"name": "binary_field", "type": "bytes"},
            {
              "name": "fixed_field",
              "type": {
                "type": "fixed",
                "name": "FixedType",
                "size": 4
              }
            },
            {
              "name": "struct_field",
              "type": {
                "type": "record",
                "name": "StructType",
                "fields": [
                  {"name": "nested_int", "type": "int"},
                  {"name": "nested_string", "type": "string"}
                ]
              }
            },
            {
              "name": "list_field",
              "type": {
                "type": "array",
                "items": "string"
              }
            },
            {
              "name": "map_field",
              "type": {
                "type": "map",
                "values": "int"
              }
            }
          ]
        }
        """;

    @Test
    public void testWrite() throws IOException, LakehouseException {
        var avroSchema = new Schema.Parser().parse(SCHEMA_STR);
        var icebergSchema = AvroSchemaUtilExtended.toIceberg(avroSchema);
        var records = prepareAvroRecord(10);
        var icebergRecords = records.stream()
            .map(avroRecord -> AvroToIcebergConverter.convert(avroRecord, icebergSchema)).toList();

        Properties properties = new Properties();
        properties.put("storagePath", baseDir.toAbsolutePath().toString());
        var configuration = new LakehouseConfiguration(properties);
        var tableOptions = TableOptions.builder().schema(icebergSchema).build();
        var tableIdentifier = TableIdentifier.of("test");
        var table = new IcebergTable(configuration, tableOptions, tableIdentifier);
        table.createIfAbsent();
        TaskWriter<Record> taskWriter = Utilities.createTableWriter(table.getTable(), table.getTable().schema(),
                1, new IcebergSinkConfig(configuration.getProperties()));
        try {
            for (Record icebergRecord : icebergRecords) {
                taskWriter.write(icebergRecord);
            }
            WriteResult result = taskWriter.complete();
            ParquetFileStat stat = ParquetFileStat.fromWriteResults(List.of(result), null);
            table.commitExternal(Collections.singletonList(stat));
            var r = new ArrayList<>(icebergRecords);
            for (Record record : IcebergGenerics.read(table.getTable()).build()) {
                r.remove(record);
            }
            Assertions.assertTrue(r.isEmpty());
        } finally {
            table.dropTable();
        }

    }

    @Test
    public void testWriteWithSchemaEvolution() throws IOException, LakehouseException {
        var avroSchema = new Schema.Parser().parse(SCHEMA_STR);
        var icebergSchema = AvroSchemaUtilExtended.toIceberg(avroSchema);
        var records = prepareAvroRecord(10);
//        var icebergRecords = records.stream()
//            .map(avroRecord -> AvroToIcebergConverter.convert(avroRecord, icebergSchema)).toList();

        var avroSchemaV2 = new Schema.Parser().parse(SCHEMA_STR_V2);
        var icebergSchemaV2 = AvroSchemaUtilExtended.toIceberg(avroSchemaV2);
        var recordsV2 = prepareAvroRecordV2(10);
        var icebergRecordsV2 = recordsV2.stream()
            .map(avroRecord -> AvroToIcebergConverter.convert(avroRecord, icebergSchemaV2)).toList();

        var icebergRecords = records.stream()
            .map(avroRecord -> AvroToIcebergConverter.convert(avroRecord, icebergSchemaV2)).toList();

        Properties properties = new Properties();
        properties.put("storagePath", baseDir.toAbsolutePath().toString());
        var configuration = new LakehouseConfiguration(properties);
        var tableOptions = TableOptions.builder().schema(icebergSchemaV2).build();
        var tableIdentifier = TableIdentifier.of("test");
        var table = new IcebergTable(configuration, tableOptions, tableIdentifier);
        table.createIfAbsent();
        TaskWriter<Record> taskWriter = Utilities.createTableWriter(table.getTable(), table.getTable().schema(),
                1, new IcebergSinkConfig(configuration.getProperties()));
        try {


            for (Record icebergRecord : icebergRecordsV2) {
                taskWriter.write(icebergRecord);
            }

            for (Record icebergRecord : icebergRecords) {
                taskWriter.write(icebergRecord);
            }
            WriteResult result = taskWriter.complete();
            ParquetFileStat stat = ParquetFileStat.fromWriteResults(List.of(result), null);
            table.commitExternal(Collections.singletonList(stat));
            var r = new ArrayList<>(icebergRecords);
            r.addAll(icebergRecordsV2);
            for (Record record : IcebergGenerics.read(table.getTable()).build()) {
                r.remove(record);
            }
            Assertions.assertTrue(r.isEmpty());
        } finally {
            table.dropTable();
        }

    }


    // prepare n avro record and return with a list
    List<GenericData.Record> prepareAvroRecord(int n) {
        Schema schema = new Schema.Parser().parse(SCHEMA_STR);
        List<GenericData.Record> records = new ArrayList<>();

        for (int i = 0; i < n; i++) {
            GenericData.Record record = new GenericData.Record(schema);

            // Set primitive fields
            record.put("boolean_field", i % 2 == 0);
            record.put("int_field", i);
            record.put("long_field", (long) i);
            record.put("float_field", (float) i);
            record.put("double_field", (double) i);

            // Set decimal field (as bytes with scale 2)
            ByteBuffer decimalBytes = ByteBuffer.wrap(BigDecimal.valueOf(i, 2).unscaledValue().toByteArray());
            record.put("decimal_field", decimalBytes);

            // Set date and time fields
            record.put("date_field", i);  // Days since epoch
            record.put("time_field", i * 1000000L);  // Microseconds since midnight
            record.put("timestamp_field", System.currentTimeMillis() * 1000 + i);  // Microseconds since epoch
            record.put("timestamptz_field", System.currentTimeMillis() * 1000 + i);  // UTC timestamp in microseconds

            // Set string field
            record.put("string_field", "string" + i);

            // Set UUID field
            byte[] uuidBytes = new byte[16];
            Arrays.fill(uuidBytes, (byte) i);
            record.put("uuid_field", new GenericData.Fixed(schema.getField("uuid_field").schema(), uuidBytes));

            // Set binary field
            record.put("binary_field", ByteBuffer.wrap(("binary" + i).getBytes()));

            // Set fixed field
            byte[] fixedBytes = new byte[4];
            Arrays.fill(fixedBytes, (byte) i);
            record.put("fixed_field", new GenericData.Fixed(schema.getField("fixed_field").schema(), fixedBytes));

            // Create and set struct field
            GenericData.Record structRecord = new GenericData.Record(schema.getField("struct_field").schema());
            structRecord.put("nested_int", i);
            structRecord.put("nested_string", "nested" + i);
            record.put("struct_field", structRecord);

            // Set list field
            List<String> list = Arrays.asList("item1_" + i, "item2_" + i, "item3_" + i);
            record.put("list_field", list);

            // Set map field
            Map<String, Integer> map = new HashMap<>();
            map.put("key1_" + i, i);
            map.put("key2_" + i, i * 2);
            record.put("map_field", map);

            records.add(record);
        }

        return records;
    }

    // prepare n avro record and return with a list
    List<GenericData.Record> prepareAvroRecordV2(int n) {
        Schema schema = new Schema.Parser().parse(SCHEMA_STR_V2);
        List<GenericData.Record> records = new ArrayList<>();

        for (int i = 0; i < n; i++) {
            GenericData.Record record = new GenericData.Record(schema);

            // Set primitive fields
            record.put("boolean_field", i % 2 == 0);
            record.put("int_field", i);
            record.put("long_field", (long) i);
            record.put("float_field", (float) i);
            record.put("double_field", (double) i);
            record.put("double_field_v2", (double) i);

            // Set decimal field (as bytes with scale 2)
            ByteBuffer decimalBytes = ByteBuffer.wrap(BigDecimal.valueOf(i, 2).unscaledValue().toByteArray());
            record.put("decimal_field", decimalBytes);

            // Set date and time fields
            record.put("date_field", i);  // Days since epoch
            record.put("time_field", i * 1000000L);  // Microseconds since midnight
            record.put("timestamp_field", System.currentTimeMillis() * 1000 + i);  // Microseconds since epoch
            record.put("timestamptz_field", System.currentTimeMillis() * 1000 + i);  // UTC timestamp in microseconds

            // Set string field
            record.put("string_field", "string" + i);

            // Set UUID field
            byte[] uuidBytes = new byte[16];
            Arrays.fill(uuidBytes, (byte) i);
            record.put("uuid_field", new GenericData.Fixed(schema.getField("uuid_field").schema(), uuidBytes));

            // Set binary field
            record.put("binary_field", ByteBuffer.wrap(("binary" + i).getBytes()));

            // Set fixed field
            byte[] fixedBytes = new byte[4];
            Arrays.fill(fixedBytes, (byte) i);
            record.put("fixed_field", new GenericData.Fixed(schema.getField("fixed_field").schema(), fixedBytes));

            // Create and set struct field
            GenericData.Record structRecord = new GenericData.Record(schema.getField("struct_field").schema());
            structRecord.put("nested_int", i);
            structRecord.put("nested_string", "nested" + i);
            record.put("struct_field", structRecord);

            // Set list field
            List<String> list = Arrays.asList("item1_" + i, "item2_" + i, "item3_" + i);
            record.put("list_field", list);

            // Set map field
            Map<String, Integer> map = new HashMap<>();
            map.put("key1_" + i, i);
            map.put("key2_" + i, i * 2);
            record.put("map_field", map);

            records.add(record);
        }

        return records;
    }


    @Test
    public void testFormatS3TableName() {
        assertThrows(IllegalArgumentException.class, () -> TableNameFormatUtils.formatS3TableName(""));
        assertThrows(IllegalArgumentException.class, () -> TableNameFormatUtils.formatS3TableName(null));
        assertEquals("test", TableNameFormatUtils.formatS3TableName("test"));
        assertEquals("public___default", TableNameFormatUtils.formatS3TableNamespaceName("public/default"));
        assertEquals("public___default", TableNameFormatUtils.formatS3TableNamespaceName("public/default/"));
        assertEquals("default___test_v1__v2__partition__0",
            TableNameFormatUtils.formatS3TableName("default/test.v1-v2-partition-0"));
        assertEquals("test_v1", TableNameFormatUtils.formatS3TableName("test.v1"));
        assertEquals("test_v1_v2", TableNameFormatUtils.formatS3TableName("test.v1.v2"));
        assertEquals("test____8080", TableNameFormatUtils.formatS3TableName("test:8080"));
        // Test cases with uppercase letters - uppercase should be converted to lowercase with underscore suffix
        assertEquals("t_est", TableNameFormatUtils.formatS3TableName("Test"));
        assertEquals("t_e_s_t", TableNameFormatUtils.formatS3TableName("TEST"));
        assertEquals("t_estc_ase", TableNameFormatUtils.formatS3TableName("TestCase"));
        assertEquals("public___default___t_est", TableNameFormatUtils.formatS3TableName("public/default/Test"));
        assertEquals("public___default___test", TableNameFormatUtils.formatS3TableName("public/default/test"));
        assertEquals("m_yt_able", TableNameFormatUtils.formatS3TableName("MyTable"));
        assertEquals("m_yt_ablen_ame", TableNameFormatUtils.formatS3TableName("MyTableName"));
        // Test edge cases
        assertEquals("a_b_c", TableNameFormatUtils.formatS3TableName("ABC"));
        assertEquals("a_bc", TableNameFormatUtils.formatS3TableName("AbC"));
        assertEquals("ab_c", TableNameFormatUtils.formatS3TableName("aBC"));
        assertEquals("abc", TableNameFormatUtils.formatS3TableName("abc"));
    }

    @Test
    public void testOverrideTableIdentifier() {
        Catalog catalog = mock(Catalog.class);
        TableOptions tableOptions = mock(TableOptions.class);

        // test hive table
        Properties properties = new Properties();
        properties.put("iceberg.type", "hive");
        LakehouseConfiguration configuration = new LakehouseConfiguration(properties);
        TableIdentifier identifier = TableIdentifier.of(Namespace.of("public", "default"), "test-table-v1");
        IcebergTable table = new IcebergTable(catalog, identifier, tableOptions, configuration);
        assertEquals(table.getIdentifier(),
            TableIdentifier.of(Namespace.of("ursa_public_default"), "test-table-v1"));

        identifier = TableIdentifier.of(Namespace.of("public-v1", "default-v2"), "test-table-v1");
        table = new IcebergTable(catalog, identifier, tableOptions, configuration);
        assertEquals(table.getIdentifier(),
            TableIdentifier.of(Namespace.of("ursa_public__v1_default__v2"), "test-table-v1"));

        identifier = TableIdentifier.of(Namespace.of("public/v1", "default.v2", "ta:8080"), "test-table-v1");
        table = new IcebergTable(catalog, identifier, tableOptions, configuration);
        assertEquals(table.getIdentifier(),
            TableIdentifier.of(Namespace.of("ursa_public___v1_default_v2_ta____8080"), "test-table-v1"));

        // test s3Table - with the new lowercase conversion for uppercase letters
        Properties properties1 = new Properties();
        properties1.put("iceberg.catalog-backend", "s3table");
        LakehouseConfiguration configuration1 = new LakehouseConfiguration(properties1);
        identifier = TableIdentifier.of(Namespace.of("public/v1", "default.v2", "ta:8080"), "test-table-v1");
        table = new IcebergTable(catalog, identifier, tableOptions, configuration1);
        assertEquals(table.getIdentifier(),
            TableIdentifier.of(Namespace.of("ursa_public___v1_default_v2_ta____8080"), "test__table__v1"));

        identifier = TableIdentifier.of(Namespace.of("public/v1", "default.v2", "ta:8080"), "test-table/v1.v2:8080");
        table = new IcebergTable(catalog, identifier, tableOptions, configuration1);
        assertEquals(table.getIdentifier(),
            TableIdentifier.of(Namespace.of("ursa_public___v1_default_v2_ta____8080"),
                "test__table___v1_v2____8080"));
        // Test uppercase table names with s3Table
        identifier = TableIdentifier.of(Namespace.of("public", "default"), "Test");
        table = new IcebergTable(catalog, identifier, tableOptions, configuration1);
        assertEquals(table.getIdentifier(),
            TableIdentifier.of(Namespace.of("ursa_public_default"), "t_est"));
        identifier = TableIdentifier.of(Namespace.of("public", "default"), "TestTable");
        table = new IcebergTable(catalog, identifier, tableOptions, configuration1);
        assertEquals(table.getIdentifier(),
            TableIdentifier.of(Namespace.of("ursa_public_default"), "t_estt_able"));
    }

    @Test
    void testPartitionSpecTransform() {
        Schema avroSchema = new Schema.Parser().parse(SCHEMA_STR);
        org.apache.iceberg.Schema icebergSchema = AvroSchemaUtilExtended.toIceberg(avroSchema);
        Properties properties = new Properties();
        properties.put("iceberg.table-props.test", "test");
        properties.put("iceberg.table-props.test2", "test2");
        properties.put("iceberg.table-props.commit.retry.num-retries", "3");
        properties.put("iceberg.write-props.write.metadata.previous-versions-max", "1000");
        properties.put("iceberg.ta", "testa");
        properties.put("xx", "XX");
        properties.put("storagePath", baseDir.toAbsolutePath().toString());


        String partitionSpecStr = "{\"spec-id\":0,\"fields\":[{\"name\":\"string_field\",\"transform\":\"truncate[4]\",\"source-id\":10},{\"name\":\"timestamp_field\",\"transform\":\"year\",\"source-id\":8}]}\n";
        PartitionSpec spec = PartitionSpecParser.fromJson(icebergSchema, partitionSpecStr);

        PartitionSpec.Builder builder = PartitionSpec.builderFor(icebergSchema);

        LakehouseConfiguration configuration = new LakehouseConfiguration(properties);
        TableOptions tableOptions = TableOptions.builder()
            .schema(icebergSchema)
            .properties(configuration.getIcebergTableProperties())
            .partitionSpec(new IcebergPartitionSpec(spec, null))
            .build();
        TableIdentifier tableIdentifier = TableIdentifier.of("test");
        IcebergTable table = new IcebergTable(configuration, tableOptions, tableIdentifier);
        table.createIfAbsent();
        table.updateTablePartitionSpecIfNeed();
        table.getTable().spec().equals(spec);

    }

    @Test
    public void testWriteAndDelete() throws IOException, LakehouseException {
        var avroSchema = new Schema.Parser().parse(SCHEMA_STR);
        var icebergSchema = AvroSchemaUtilExtended.toIceberg(avroSchema);
        var records = prepareAvroRecord(10);
        var icebergRecords = records.stream()
                .map(avroRecord -> AvroToIcebergConverter.convert(avroRecord, icebergSchema)).toList();

        Properties properties = new Properties();
        properties.put("storagePath", baseDir.toAbsolutePath().toString());
        var configuration = new LakehouseConfiguration(properties);
        var tableOptions = TableOptions.builder().schema(icebergSchema).build();
        var tableIdentifier = TableIdentifier.of("test");
        var table = new IcebergTable(configuration, tableOptions, tableIdentifier);
        table.createIfAbsent();
        TaskWriter<Record> taskWriter = Utilities.createTableWriter(table.getTable(), table.getTable().schema(),
                1, new IcebergSinkConfig(configuration.getProperties()));
        try {
            for (Record icebergRecord : icebergRecords) {
                taskWriter.write(icebergRecord);
            }
            WriteResult result = taskWriter.complete();
            ParquetFileStat stat = ParquetFileStat.fromWriteResults(List.of(result), null);
            table.commitExternal(Collections.singletonList(stat));
            var r = new ArrayList<>(icebergRecords);
            for (Record record : IcebergGenerics.read(table.getTable()).build()) {
                r.remove(record);
            }
            Assertions.assertTrue(r.isEmpty());

            var df = result.dataFiles()[0];
            var deleteFileStat = new ParquetFileStat("",
                    df.location().substring(configuration.getBucketPath().length()),
                    df.fileSizeInBytes(),
                    null,
                    Collections.emptyMap(),
                    Map.of(
                            "totalMessage", "10"
                    )
            );

            log.info("Start to delete files: {}", result.dataFiles());
            table.delete(List.of(deleteFileStat));
            log.info("Delete files finished");

            for (Record record : IcebergGenerics.read(table.getTable()).build()) {
                log.info("Record: {}", record);
                fail("The record should be empty.");
            }

            // Duplicate delete should not throw an exception
            table.delete(List.of(deleteFileStat));
            log.info("Data files after delete: {}", table.getTable().currentSnapshot().addedDataFiles(table.getTable()
                    .io()));
        } finally {
            table.dropTable();
        }

    }

    @Test
    void testUpdateTablePartition() {
        Schema avroSchema = new Schema.Parser().parse(SCHEMA_STR);
        org.apache.iceberg.Schema icebergSchema = AvroSchemaUtilExtended.toIceberg(avroSchema);
        Properties properties = new Properties();
        properties.put("iceberg.table-props.test", "test");
        properties.put("iceberg.table-props.test2", "test2");
        properties.put("iceberg.table-props.commit.retry.num-retries", "3");
        properties.put("iceberg.write-props.write.metadata.previous-versions-max", "1000");
        properties.put("iceberg.ta", "testa");
        properties.put("xx", "XX");
        properties.put("storagePath", baseDir.toAbsolutePath().toString());

        LakehouseConfiguration configuration = new LakehouseConfiguration(properties);
        PartitionSpec spec = PartitionSpec.builderFor(icebergSchema)
            .identity("boolean_field")
            .identity("int_field")
            .build();

        IcebergExpression expression = new IcebergExpression(null, Expressions.ref("boolean_field"));
        IcebergExpression expression1 = new IcebergExpression(null, Expressions.ref("int_field"));
        List<IcebergExpression> expressions = new ArrayList<>();
        expressions.add(expression);
        expressions.add(expression1);

        TableOptions tableOptions = TableOptions.builder()
            .schema(icebergSchema)
            .properties(configuration.getIcebergTableProperties())
            .partitionSpec(new IcebergPartitionSpec(spec, expressions))
            .build();
        TableIdentifier tableIdentifier = TableIdentifier.of("test");
        IcebergTable table = new IcebergTable(configuration, tableOptions, tableIdentifier);
        table.createIfAbsent();
        table.updateTablePartitionSpecIfNeed();
        table.getTable().spec().equals(spec);

        PartitionSpec spec1 = PartitionSpec.builderFor(icebergSchema)
            .identity("float_field")
            .build();
        IcebergExpression expression3 = new IcebergExpression(null, Expressions.ref("float_field"));
        tableOptions = TableOptions.builder()
            .schema(icebergSchema)
            .properties(configuration.getIcebergTableProperties())
            .partitionSpec(new IcebergPartitionSpec(spec1, List.of(expression3)))
            .build();
        tableIdentifier = TableIdentifier.of("test");
        table = new IcebergTable(configuration, tableOptions, tableIdentifier);
        table.createIfAbsent();
        table.updateTablePartitionSpecIfNeed();
        table.getTable().spec().equals(spec1);
    }

    @Test
    void testCreateIcebergTableWithProperties() {
        Schema avroSchema = new Schema.Parser().parse(SCHEMA_STR);
        org.apache.iceberg.Schema icebergSchema = AvroSchemaUtilExtended.toIceberg(avroSchema);
        Properties properties = new Properties();
        properties.put("iceberg.table-props.test", "test");
        properties.put("iceberg.table-props.test2", "test2");
        properties.put("iceberg.table-props.commit.retry.num-retries", "3");
        properties.put("iceberg.write-props.write.metadata.previous-versions-max", "1000");
        properties.put("iceberg.ta", "testa");
        properties.put("xx", "XX");
        properties.put("storagePath", baseDir.toAbsolutePath().toString());

        LakehouseConfiguration configuration = new LakehouseConfiguration(properties);
        TableOptions tableOptions = TableOptions.builder()
            .schema(icebergSchema)
            .properties(configuration.getIcebergTableProperties())
            .build();
        TableIdentifier tableIdentifier = TableIdentifier.of("test");
        IcebergTable table = new IcebergTable(configuration, tableOptions, tableIdentifier);
        table.createIfAbsent();

        Map<String, String> tableProperties = table.getTable().properties();
        assertEquals("test", tableProperties.get("test"));
        assertEquals("test2", tableProperties.get("test2"));
        assertEquals("3", tableProperties.get("commit.retry.num-retries"));
        assertEquals("1000", tableProperties.get("write.metadata.previous-versions-max"));
        assertEquals("zstd", tableProperties.get(PARQUET_COMPRESSION));
        // Check ursa.keys is created and contains the managed keys
        assertTrue(tableProperties.containsKey("ursa.keys"));
        String ursaKeys = tableProperties.get("ursa.keys");
        assertTrue(ursaKeys.contains("test"));
        assertTrue(ursaKeys.contains("test2"));
        assertTrue(ursaKeys.contains("commit.retry.num-retries"));
        assertTrue(ursaKeys.contains("write.metadata.previous-versions-max"));
        assertEquals(7, tableProperties.size()); // 7 properties + ursa.keys

        table.updateTableProperties(null);
        tableProperties = table.getTable().properties();
        // Configuration properties should still be there
        assertEquals("test", tableProperties.get("test"));
        assertEquals("test2", tableProperties.get("test2"));
        assertEquals("3", tableProperties.get("commit.retry.num-retries"));
        assertEquals("1000", tableProperties.get("write.metadata.previous-versions-max"));
        assertEquals("zstd", tableProperties.get(PARQUET_COMPRESSION));
        assertEquals(7, tableProperties.size());

        table.updateTableProperties(new HashMap<>());
        tableProperties = table.getTable().properties();
        // Configuration properties should still be there
        assertEquals("test", tableProperties.get("test"));
        assertEquals("test2", tableProperties.get("test2"));
        assertEquals("3", tableProperties.get("commit.retry.num-retries"));
        assertEquals("1000", tableProperties.get("write.metadata.previous-versions-max"));
        assertEquals("zstd", tableProperties.get(PARQUET_COMPRESSION));
        assertEquals(7, tableProperties.size());

        // update with nothing changed
        Map<String, String> newProp0 = new HashMap<>();
        // add new properties
        newProp0.put("iceberg.table-props.commit.retry.num-retries", "3");
        table.updateTableProperties(newProp0);
        tableProperties = table.getTable().properties();
        // All configuration properties should still be there (cannot be removed)
        assertEquals("test", tableProperties.get("test"));
        assertEquals("test2", tableProperties.get("test2"));
        assertEquals("3", tableProperties.get("commit.retry.num-retries"));
        assertEquals("1000", tableProperties.get("write.metadata.previous-versions-max"));
        assertEquals("zstd", tableProperties.get(PARQUET_COMPRESSION));
        assertEquals(7, tableProperties.size()); // Still 7 because config properties cannot be removed

        Properties properties1 = new Properties();
        properties1.put("iceberg.table-props.test", "test");
        properties1.put("iceberg.table-props.commit.retry.num-retries", "3");
        properties1.put("iceberg.write-props.write.metadata.previous-versions-max", "1000");
        properties1.put("iceberg.ta", "testa");
        properties1.put("xx", "XX");
        properties1.put("storagePath", baseDir.toAbsolutePath().toString());
        configuration = new LakehouseConfiguration(properties1);
        table = new IcebergTable(configuration, tableOptions, tableIdentifier);

        // test add, update and remove properties
        Map<String, String> newProp = new HashMap<>();
        // add new properties
        newProp.put("iceberg.table-props.test3", "test3");
        newProp.put("iceberg.write-props.test.test4", "test4");
        // update properties
        newProp.put("iceberg.table-props." + METADATA_DELETE_AFTER_COMMIT_ENABLED, "false");
        newProp.put("iceberg.write-props." + PARQUET_COMPRESSION, "gzip");
        table.updateTableProperties(newProp);
        tableProperties = table.getTable().properties();
        assertEquals("test", tableProperties.get("test"));
        assertEquals("test3", tableProperties.get("test3"));
        assertEquals("test4", tableProperties.get("test.test4"));
        assertEquals("3", tableProperties.get("commit.retry.num-retries"));
        assertEquals("1000", tableProperties.get("write.metadata.previous-versions-max"));
        assertEquals("false", tableProperties.get(METADATA_DELETE_AFTER_COMMIT_ENABLED));
        assertEquals("gzip", tableProperties.get(PARQUET_COMPRESSION));
        // Note: test2 should be removed because it's no longer in configuration and was only from parameter
        assertFalse(tableProperties.containsKey("test2"));
        assertEquals(9, tableProperties.size()); // 6 properties + ursa.keys
    }

    @Test
    void testThirdPartyPropertiesPreservation() {
        Schema avroSchema = new Schema.Parser().parse(SCHEMA_STR);
        org.apache.iceberg.Schema icebergSchema = AvroSchemaUtilExtended.toIceberg(avroSchema);
        Properties properties = new Properties();
        properties.put("iceberg.table-props.config.prop", "config.value");
        properties.put("storagePath", baseDir.toAbsolutePath().toString());

        LakehouseConfiguration configuration = new LakehouseConfiguration(properties);
        TableOptions tableOptions = TableOptions.builder()
            .schema(icebergSchema)
            .properties(configuration.getIcebergTableProperties())
            .build();
        TableIdentifier tableIdentifier = TableIdentifier.of("test");
        IcebergTable table = new IcebergTable(configuration, tableOptions, tableIdentifier);
        table.createIfAbsent();

        // Simulate third-party adding properties directly to the table
        UpdateProperties updateProperties = table.getTable().updateProperties();
        updateProperties.set("third.party.prop1", "value1");
        updateProperties.set("third.party.prop2", "value2");
        updateProperties.commit();

        Map<String, String> tableProperties = table.getTable().properties();
        assertEquals("config.value", tableProperties.get("config.prop"));
        assertEquals("value1", tableProperties.get("third.party.prop1"));
        assertEquals("value2", tableProperties.get("third.party.prop2"));
        assertTrue(tableProperties.containsKey("ursa.keys"));
        String ursaKeys = tableProperties.get("ursa.keys");
        assertTrue(ursaKeys.contains("config.prop"));
        assertFalse(ursaKeys.contains("third.party.prop1"));
        assertFalse(ursaKeys.contains("third.party.prop2"));

        // Update our properties - should not affect third-party properties
        Map<String, String> newProp = new HashMap<>();
        newProp.put("iceberg.table-props.param.prop", "param.value");
        table.updateTableProperties(newProp);

        tableProperties = table.getTable().properties();
        // Configuration properties should still be there
        assertEquals("config.value", tableProperties.get("config.prop"));
        // Parameter properties should be added
        assertEquals("param.value", tableProperties.get("param.prop"));
        // Third-party properties should be preserved
        assertEquals("value1", tableProperties.get("third.party.prop1"));
        assertEquals("value2", tableProperties.get("third.party.prop2"));
        assertTrue(tableProperties.containsKey("ursa.keys"));
        ursaKeys = tableProperties.get("ursa.keys");
        assertTrue(ursaKeys.contains("config.prop"));
        assertTrue(ursaKeys.contains("param.prop"));
        assertFalse(ursaKeys.contains("third.party.prop1"));
        assertFalse(ursaKeys.contains("third.party.prop2"));
    }

    @Test
    void testParameterPropertiesCanBeRemoved() {
        Schema avroSchema = new Schema.Parser().parse(SCHEMA_STR);
        org.apache.iceberg.Schema icebergSchema = AvroSchemaUtilExtended.toIceberg(avroSchema);
        Properties properties = new Properties();
        properties.put("iceberg.table-props.config.prop1", "config.value1");
        properties.put("iceberg.table-props.config.prop2", "config.value2");
        properties.put("storagePath", baseDir.toAbsolutePath().toString());

        LakehouseConfiguration configuration = new LakehouseConfiguration(properties);
        TableOptions tableOptions = TableOptions.builder()
            .schema(icebergSchema)
            .properties(configuration.getIcebergTableProperties())
            .build();
        TableIdentifier tableIdentifier = TableIdentifier.of("test");
        IcebergTable table = new IcebergTable(configuration, tableOptions, tableIdentifier);
        table.createIfAbsent();

        // Add parameter properties
        Map<String, String> paramProp = new HashMap<>();
        paramProp.put("iceberg.table-props.param.prop1", "param.value1");
        paramProp.put("iceberg.table-props.param.prop2", "param.value2");
        paramProp.put("iceberg.table-props.param.prop3", "param.value3");
        table.updateTableProperties(paramProp);

        // Add third-party property
        UpdateProperties updateProperties = table.getTable().updateProperties();
        updateProperties.set("third.party.prop", "third.value");
        updateProperties.commit();

        Map<String, String> tableProperties = table.getTable().properties();
        // Configuration properties (cannot be removed)
        assertEquals("config.value1", tableProperties.get("config.prop1"));
        assertEquals("config.value2", tableProperties.get("config.prop2"));
        // Parameter properties (can be removed)
        assertEquals("param.value1", tableProperties.get("param.prop1"));
        assertEquals("param.value2", tableProperties.get("param.prop2"));
        assertEquals("param.value3", tableProperties.get("param.prop3"));
        // Third-party property
        assertEquals("third.value", tableProperties.get("third.party.prop"));

        // Update with partial parameter properties (remove param.prop3)
        Map<String, String> newParamProp = new HashMap<>();
        newParamProp.put("iceberg.table-props.param.prop1", "param.updated1");
        newParamProp.put("iceberg.table-props.param.prop2", "param.updated2");
        // param.prop3 is not included, so it should be removed
        table.updateTableProperties(newParamProp);

        tableProperties = table.getTable().properties();
        // Configuration properties should still be there (cannot be removed)
        assertEquals("config.value1", tableProperties.get("config.prop1"));
        assertEquals("config.value2", tableProperties.get("config.prop2"));
        // Parameter properties should be updated
        assertEquals("param.updated1", tableProperties.get("param.prop1"));
        assertEquals("param.updated2", tableProperties.get("param.prop2"));
        // param.prop3 should be removed (was only from parameter)
        assertFalse(tableProperties.containsKey("param.prop3"));
        // Third-party property should be preserved
        assertEquals("third.value", tableProperties.get("third.party.prop"));

        String ursaKeys = tableProperties.get("ursa.keys");
        assertTrue(ursaKeys.contains("config.prop1"));
        assertTrue(ursaKeys.contains("config.prop2"));
        assertTrue(ursaKeys.contains("param.prop1"));
        assertTrue(ursaKeys.contains("param.prop2"));
        assertFalse(ursaKeys.contains("param.prop3"));
        assertFalse(ursaKeys.contains("third.party.prop"));
    }

    @Test
    void testRemoveAllParameterProperties() {
        Schema avroSchema = new Schema.Parser().parse(SCHEMA_STR);
        org.apache.iceberg.Schema icebergSchema = AvroSchemaUtilExtended.toIceberg(avroSchema);
        Properties properties = new Properties();
        properties.put("iceberg.table-props.config.prop", "config.value");
        properties.put("storagePath", baseDir.toAbsolutePath().toString());

        LakehouseConfiguration configuration = new LakehouseConfiguration(properties);
        TableOptions tableOptions = TableOptions.builder()
            .schema(icebergSchema)
            .properties(configuration.getIcebergTableProperties())
            .build();
        TableIdentifier tableIdentifier = TableIdentifier.of("test");
        IcebergTable table = new IcebergTable(configuration, tableOptions, tableIdentifier);
        table.createIfAbsent();

        // Add parameter properties
        Map<String, String> paramProp = new HashMap<>();
        paramProp.put("iceberg.table-props.param.prop1", "param.value1");
        paramProp.put("iceberg.table-props.param.prop2", "param.value2");
        table.updateTableProperties(paramProp);

        // Add third-party property
        UpdateProperties updateProperties = table.getTable().updateProperties();
        updateProperties.set("third.party.prop", "third.value");
        updateProperties.commit();

        Map<String, String> tableProperties = table.getTable().properties();
        assertTrue(tableProperties.containsKey("config.prop"));
        assertTrue(tableProperties.containsKey("param.prop1"));
        assertTrue(tableProperties.containsKey("param.prop2"));
        assertTrue(tableProperties.containsKey("third.party.prop"));
        assertTrue(tableProperties.containsKey("ursa.keys"));

        // Update with empty parameters (remove all parameter properties)
        table.updateTableProperties(new HashMap<>());

        tableProperties = table.getTable().properties();
        // Configuration properties should remain (cannot be removed)
        assertTrue(tableProperties.containsKey("config.prop"));
        assertEquals("config.value", tableProperties.get("config.prop"));
        // Parameter properties should be removed
        assertFalse(tableProperties.containsKey("param.prop1"));
        assertFalse(tableProperties.containsKey("param.prop2"));
        // Third-party properties should be preserved
        assertTrue(tableProperties.containsKey("third.party.prop"));
        assertEquals("third.value", tableProperties.get("third.party.prop"));
        // ursa.keys should still exist (contains config properties)
        assertTrue(tableProperties.containsKey("ursa.keys"));

        String ursaKeys = tableProperties.get("ursa.keys");
        assertTrue(ursaKeys.contains("config.prop"));
        assertFalse(ursaKeys.contains("param.prop1"));
        assertFalse(ursaKeys.contains("param.prop2"));
        assertFalse(ursaKeys.contains("third.party.prop"));
    }

    @Test
    void testConfigurationPropertiesCannotBeRemoved() {
        Schema avroSchema = new Schema.Parser().parse(SCHEMA_STR);
        org.apache.iceberg.Schema icebergSchema = AvroSchemaUtilExtended.toIceberg(avroSchema);
        Properties properties = new Properties();
        properties.put("iceberg.table-props.config.prop1", "config.value1");
        properties.put("iceberg.table-props.config.prop2", "config.value2");
        properties.put("storagePath", baseDir.toAbsolutePath().toString());

        LakehouseConfiguration configuration = new LakehouseConfiguration(properties);
        TableOptions tableOptions = TableOptions.builder()
            .schema(icebergSchema)
            .properties(configuration.getIcebergTableProperties())
            .build();
        TableIdentifier tableIdentifier = TableIdentifier.of("test");
        IcebergTable table = new IcebergTable(configuration, tableOptions, tableIdentifier);
        table.createIfAbsent();

        Map<String, String> tableProperties = table.getTable().properties();
        assertTrue(tableProperties.containsKey("config.prop1"));
        assertTrue(tableProperties.containsKey("config.prop2"));

        // Try to "remove" configuration properties by not including them in parameters
        // This should not remove them because they come from configuration
        Map<String, String> paramProp = new HashMap<>();
        paramProp.put("iceberg.table-props.param.prop", "param.value");
        table.updateTableProperties(paramProp);

        tableProperties = table.getTable().properties();
        // Configuration properties should still be there
        assertTrue(tableProperties.containsKey("config.prop1"));
        assertTrue(tableProperties.containsKey("config.prop2"));
        assertEquals("config.value1", tableProperties.get("config.prop1"));
        assertEquals("config.value2", tableProperties.get("config.prop2"));
        // Parameter property should be added
        assertTrue(tableProperties.containsKey("param.prop"));
        assertEquals("param.value", tableProperties.get("param.prop"));

        String ursaKeys = tableProperties.get("ursa.keys");
        assertTrue(ursaKeys.contains("config.prop1"));
        assertTrue(ursaKeys.contains("config.prop2"));
        assertTrue(ursaKeys.contains("param.prop"));
    }

    @Test
    void testUrsaKeysConsistency() {
        Schema avroSchema = new Schema.Parser().parse(SCHEMA_STR);
        org.apache.iceberg.Schema icebergSchema = AvroSchemaUtilExtended.toIceberg(avroSchema);
        Properties properties = new Properties();
        properties.put("iceberg.table-props.config.prop", "config.value");
        properties.put("storagePath", baseDir.toAbsolutePath().toString());

        LakehouseConfiguration configuration = new LakehouseConfiguration(properties);
        TableOptions tableOptions = TableOptions.builder()
            .schema(icebergSchema)
            .properties(configuration.getIcebergTableProperties())
            .build();
        TableIdentifier tableIdentifier = TableIdentifier.of("test");
        IcebergTable table = new IcebergTable(configuration, tableOptions, tableIdentifier);
        table.createIfAbsent();

        // Add third-party property
        UpdateProperties updateProperties = table.getTable().updateProperties();
        updateProperties.set("third.party.prop", "third.value");
        updateProperties.commit();

        Map<String, String> tableProperties = table.getTable().properties();
        String ursaKeys = tableProperties.get("ursa.keys");

        // Verify ursa.keys contains only managed properties
        Set<String> managedKeys = Arrays.stream(ursaKeys.split(","))
            .map(String::trim)
            .collect(Collectors.toSet());

        // Should contain configuration properties
        assertTrue(managedKeys.contains("config.prop"));
        // Should NOT contain third-party properties
        assertFalse(managedKeys.contains("third.party.prop"));

        // Verify all keys in ursa.keys exist in table properties
        for (String key : managedKeys) {
            assertTrue(tableProperties.containsKey(key), "Table should contain property " + key);
        }

        // Verify third-party properties are not in ursa.keys
        assertTrue(tableProperties.containsKey("third.party.prop"));
        assertFalse(managedKeys.contains("third.party.prop"));
    }

    @Test
    void testNoChangeScenario() {
        Schema avroSchema = new Schema.Parser().parse(SCHEMA_STR);
        org.apache.iceberg.Schema icebergSchema = AvroSchemaUtilExtended.toIceberg(avroSchema);
        Properties properties = new Properties();
        properties.put("iceberg.table-props.config.prop", "config.value");
        properties.put("storagePath", baseDir.toAbsolutePath().toString());

        LakehouseConfiguration configuration = new LakehouseConfiguration(properties);
        TableOptions tableOptions = TableOptions.builder()
            .schema(icebergSchema).properties(configuration.getIcebergTableProperties()).build();
        TableIdentifier tableIdentifier = TableIdentifier.of("test");
        IcebergTable table = new IcebergTable(configuration, tableOptions, tableIdentifier);
        table.createIfAbsent();

        // Add parameter property
        Map<String, String> paramProp = new HashMap<>();
        paramProp.put("iceberg.table-props.param.prop", "param.value");
        table.updateTableProperties(paramProp);

        Map<String, String> initialProperties = new HashMap<>(table.getTable().properties());

        // Update with same properties - should not change anything
        Map<String, String> newProp = new HashMap<>();
        newProp.put("iceberg.table-props.param.prop", "param.value");
        table.updateTableProperties(newProp);

        Map<String, String> finalProperties = table.getTable().properties();
        assertEquals(initialProperties, finalProperties);
    }

    @Test
    void testGenerateIcebergTablePropertiesNullInput() {
        Map<String, String> result = IcebergTable.generateIcebergTableProperties(null);
        assertTrue(result.isEmpty());
        assertThrows(UnsupportedOperationException.class, () -> result.put("x", "y"));
    }

    @Test
    void testGenerateIcebergTablePropertiesEmptyInput() {
        Map<String, String> result = IcebergTable.generateIcebergTableProperties(Collections.emptyMap());
        assertTrue(result.isEmpty());
        assertThrows(UnsupportedOperationException.class, () -> result.put("x", "y"));
    }

    @Test
    void testGenerateIcebergTablePropertiesPopulatedInput() {
        Map<String, String> input = new HashMap<>();
        input.put("format", "parquet");
        input.put("compression", "zstd");

        Map<String, String> result = IcebergTable.generateIcebergTableProperties(input);

        // Original input must remain unchanged
        assertFalse(input.containsKey(URSA_KEYS_PROPERTY));

        // Result must contain all original keys
        assertEquals("parquet", result.get("format"));
        assertEquals("zstd", result.get("compression"));

        // Must contain the URSA_PROPERTIES_KEY with comma-separated key names
        assertTrue(result.containsKey(URSA_KEYS_PROPERTY));
        String keys = result.get(URSA_KEYS_PROPERTY);
        assertNotNull(keys);
        // Should contain both keys
        assertTrue(keys.contains("format"));
        assertTrue(keys.contains("compression"));

        // Result must be unmodifiable
        assertThrows(UnsupportedOperationException.class, () -> result.put("x", "y"));
    }

    @Test
    void testSchemaEvolutionWithTableExistingSchema() throws Exception {
        var tableName = RandomStringUtils.secure().nextAlphabetic(6);
        Properties configProperties = new Properties();
        configProperties.put("storagePath", baseDir.toAbsolutePath().toString());
        var configuration = new LakehouseConfiguration(configProperties);
        IcebergTable icebergTable = new IcebergTable(configuration, TableIdentifier.of(tableName));

        org.apache.iceberg.Schema schema = new org.apache.iceberg.Schema(
            Types.NestedField.required(1, "id", Types.IntegerType.get()));
        var tableOption = TableOptions.builder().schema(schema).build();
        icebergTable.create(tableOption);

        icebergTable.evolveSchemaWithVersion(1, schema);

        var schemas = icebergTable.getTable().schemas();
        assertEquals(1, schemas.size());
        assertTrue(schemas.get(0).sameSchema(schema));

        var properties = icebergTable.getTable().properties();
        var mapping = properties.get("lakestream.schema.mapping");
        assertNotNull(mapping);
        assertEquals("{\"1\":0}", mapping);

        var getSchema = icebergTable.getSchemaByVersion(1);
        assertTrue(getSchema.isPresent());
        assertTrue(schemas.get(0).sameSchema(getSchema.get()));
    }

    @Test
    void testSchemaEvolutionWithNewSchema() throws Exception {
        var tableName = RandomStringUtils.secure().nextAlphabetic(6);
        Properties configProperties = new Properties();
        configProperties.put("storagePath", baseDir.toAbsolutePath().toString());
        var configuration = new LakehouseConfiguration(configProperties);
        IcebergTable icebergTable = new IcebergTable(configuration, TableIdentifier.of(tableName));

        org.apache.iceberg.Schema schema = new org.apache.iceberg.Schema(
            Types.NestedField.required(1, "id", Types.IntegerType.get()));
        var tableOption = TableOptions.builder().schema(schema).build();
        icebergTable.create(tableOption);

        org.apache.iceberg.Schema newSchema = new org.apache.iceberg.Schema(
            Types.NestedField.required(1, "id", Types.IntegerType.get()),
            Types.NestedField.optional(2, "name", Types.StringType.get())
        );

        icebergTable.evolveSchemaWithVersion(1, newSchema);

        var schemas = icebergTable.getTable().schemas();
        assertEquals(2, schemas.size());
        assertTrue(schemas.get(0).sameSchema(schema));
        assertTrue(schemas.get(1).sameSchema(newSchema));

        var properties = icebergTable.getTable().properties();
        var mapping = properties.get("lakestream.schema.mapping");
        assertNotNull(mapping);
        assertEquals("{\"1\":1}", mapping);

        var getSchema = icebergTable.getSchemaByVersion(1);
        assertTrue(getSchema.isPresent());
        assertTrue(schemas.get(1).sameSchema(getSchema.get()));
    }

//    @Test
    // todo: in iceberg this case will be successfully to execute and the table schema will transform like:
    //       {1 : id: required int }
    void testSchemaEvolutionWithWrongSchema() throws Exception {
        var tableName = RandomStringUtils.secure().nextAlphabetic(6);
        Properties configProperties = new Properties();
        configProperties.put("storagePath", baseDir.toAbsolutePath().toString());
        var configuration = new LakehouseConfiguration(configProperties);
        IcebergTable icebergTable = new IcebergTable(configuration, TableIdentifier.of(tableName));

        org.apache.iceberg.Schema schema = new org.apache.iceberg.Schema(
            Types.NestedField.required(1, "id", Types.IntegerType.get()));
        var tableOption = TableOptions.builder().schema(schema).build();
        icebergTable.create(tableOption);

        org.apache.iceberg.Schema newSchema = new org.apache.iceberg.Schema(
            Types.NestedField.required(1, "name", Types.StringType.get()));

        icebergTable.evolveSchemaWithVersion(1, newSchema);

        var schemas = icebergTable.getTable().schemas();
        assertEquals(2, schemas.size());

        for (Map.Entry<Integer, org.apache.iceberg.Schema> integerSchemaEntry : schemas.entrySet()) {
            System.out.println("version: " + integerSchemaEntry.getKey() + ", schema: " + integerSchemaEntry.getValue());
        }
// output:
//        version: 0, schema: table {
//            1: id: required int
//        }
//        version: 1, schema: table {
//            1: id: optional int
//            2: name: optional string
//        }

        var properties = icebergTable.getTable().properties();
        var mapping = properties.get("lakestream.schema.mapping");
        assertNotNull(mapping);
        assertEquals("{\"1\":0}", mapping);

        var getSchema = icebergTable.getSchemaByVersion(1);
        assertTrue(getSchema.isPresent());
        assertTrue(schemas.get(1).sameSchema(getSchema.get()));
    }

    @Test
    void testSaveSchemaMappingPropertiesFailed() throws Exception {
        var tableName = RandomStringUtils.secure().nextAlphabetic(6);
        Properties configProperties = new Properties();
        configProperties.put("storagePath", baseDir.toAbsolutePath().toString());
        var configuration = new LakehouseConfiguration(configProperties);
        IcebergTable icebergTable = spy(new IcebergTable(configuration, TableIdentifier.of(tableName)));

        org.apache.iceberg.Schema schema = new org.apache.iceberg.Schema(
            Types.NestedField.required(1, "id", Types.IntegerType.get()));
        var tableOption = TableOptions.builder().schema(schema).build();
        icebergTable.create(tableOption);

        org.apache.iceberg.Schema newSchema = new org.apache.iceberg.Schema(
            Types.NestedField.required(1, "id", Types.IntegerType.get()),
            Types.NestedField.optional(2, "name", Types.StringType.get())
        );

        doThrow(new SchemaMappingException("inject error")).when(icebergTable)
            .saveSchemaMapping(any(Optional.class), anyMap());

        try {
            icebergTable.evolveSchemaWithVersion(1, newSchema);
            fail("should fail by the inject error");
        } catch (SchemaMappingException e) {
            // expected
        }

        var schemas = icebergTable.getTable().schemas();
        assertEquals(1, schemas.size());
        assertTrue(schemas.get(0).sameSchema(schema));

        var properties = icebergTable.getTable().properties();
        var mapping = properties.get("lakestream.schema.mapping");
        assertNull(mapping);
    }

    @Test
    void testStartTxnWithoutCommitShouldSuccess() {
        var tableName = RandomStringUtils.secure().nextAlphabetic(6);
        Properties configProperties = new Properties();
        configProperties.put("storagePath", baseDir.toAbsolutePath().toString());
        var configuration = new LakehouseConfiguration(configProperties);
        IcebergTable icebergTable = spy(new IcebergTable(configuration, TableIdentifier.of(tableName)));
        org.apache.iceberg.Schema schema = new org.apache.iceberg.Schema(
            Types.NestedField.required(1, "id", Types.IntegerType.get()));
        var tableOption = TableOptions.builder().schema(schema).build();
        icebergTable.create(tableOption);

        icebergTable.startTransaction();
        icebergTable.startTransaction();
    }

    @Test
    void testGetNonExistVersion() throws Exception {
        var tableName = RandomStringUtils.secure().nextAlphabetic(6);
        Properties configProperties = new Properties();
        configProperties.put("storagePath", baseDir.toAbsolutePath().toString());
        var configuration = new LakehouseConfiguration(configProperties);
        IcebergTable icebergTable = spy(new IcebergTable(configuration, TableIdentifier.of(tableName)));

        org.apache.iceberg.Schema schema = new org.apache.iceberg.Schema(
            Types.NestedField.required(1, "id", Types.IntegerType.get()));
        var tableOption = TableOptions.builder().schema(schema).build();
        icebergTable.create(tableOption);

        var getSchema = icebergTable.getSchemaByVersion(1);
        assertTrue(getSchema.isEmpty());
    }

    @Test
    void testESchemaEvolutionWithSameVersion() throws Exception {
        var tableName = RandomStringUtils.secure().nextAlphabetic(6);
        Properties configProperties = new Properties();
        configProperties.put("storagePath", baseDir.toAbsolutePath().toString());
        var configuration = new LakehouseConfiguration(configProperties);
        IcebergTable icebergTable = new IcebergTable(configuration, TableIdentifier.of(tableName));

        org.apache.iceberg.Schema schema = new org.apache.iceberg.Schema(
            Types.NestedField.required(1, "id", Types.IntegerType.get()));
        var tableOption = TableOptions.builder().schema(schema).build();
        icebergTable.create(tableOption);

        org.apache.iceberg.Schema newSchema = new org.apache.iceberg.Schema(
            Types.NestedField.required(1, "id", Types.IntegerType.get()),
            Types.NestedField.optional(2, "name", Types.StringType.get())
        );

        icebergTable.evolveSchemaWithVersion(1, newSchema);


        icebergTable.evolveSchemaWithVersion(1, newSchema);
    }

    @Test
    void testUpdateSchemaConcurrently() throws InterruptedException {
        var tableName = RandomStringUtils.secure().nextAlphabetic(6);
        Properties configProperties = new Properties();
        configProperties.put("storagePath", baseDir.toAbsolutePath().toString());
        var configuration = new LakehouseConfiguration(configProperties);
        IcebergTable icebergTable = new IcebergTable(configuration, TableIdentifier.of(tableName));

        org.apache.iceberg.Schema schema = new org.apache.iceberg.Schema(
            Types.NestedField.required(1, "id", Types.IntegerType.get()));
        var tableOption = TableOptions.builder().schema(schema).build();
        icebergTable.create(tableOption);

        CyclicBarrier barrier = new CyclicBarrier(2);
        CountDownLatch latch = new CountDownLatch(2);
        AtomicInteger failureCount = new AtomicInteger();
        new Thread(() -> {
            try {
                try {
                    barrier.await();
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }

                org.apache.iceberg.Schema newSchema = new org.apache.iceberg.Schema(
                    Types.NestedField.required(1, "id", Types.IntegerType.get()),
                    Types.NestedField.optional(2, "nameA", Types.StringType.get())
                );

                try {
                    icebergTable.evolveSchemaWithVersion(1, newSchema);
                } catch (Throwable e) {
                    failureCount.incrementAndGet();
                }
            } finally {
                latch.countDown();
            }
        }).start();

        new Thread(() -> {
            try {
                try {
                    barrier.await();
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }

                org.apache.iceberg.Schema newSchema = new org.apache.iceberg.Schema(
                    Types.NestedField.required(1, "id", Types.IntegerType.get()),
                    Types.NestedField.optional(2, "nameB", Types.StringType.get()),
                    Types.NestedField.optional(3, "city", Types.StringType.get())
                );

                try {
                    icebergTable.evolveSchemaWithVersion(2, newSchema);
                } catch (Throwable e) {
                    failureCount.incrementAndGet();
                }
            } finally {
                latch.countDown();
            }
        }).start();

        latch.await();

        assertEquals(1, failureCount.get());
    }

    @Test
    void testSchemaEvolution_deleteOptionalFields() throws Exception {
        var tableName = RandomStringUtils.secure().nextAlphabetic(6);
        Properties configProperties = new Properties();
        configProperties.put("storagePath", baseDir.toAbsolutePath().toString());
        var configuration = new LakehouseConfiguration(configProperties);
        IcebergTable icebergTable = new IcebergTable(configuration, TableIdentifier.of(tableName));

        org.apache.iceberg.Schema schema = new org.apache.iceberg.Schema(
            Types.NestedField.required(1, "id", Types.IntegerType.get()),
            Types.NestedField.optional(2, "name", Types.StringType.get()),
            Types.NestedField.optional(3, "age", Types.IntegerType.get())
        );
        var tableOption = TableOptions.builder().schema(schema).build();
        icebergTable.create(tableOption);

        // delete optional field 'age'
        org.apache.iceberg.Schema newSchema = new org.apache.iceberg.Schema(
            Types.NestedField.required(1, "id", Types.IntegerType.get()),
            Types.NestedField.optional(2, "name", Types.StringType.get())
        );

        icebergTable.evolveSchemaWithVersion(1, newSchema);

        var getSchema = icebergTable.getSchemaByVersion(1);
        assertTrue(getSchema.isPresent());

        // in the current logic, we won't delete the optional fields
        assertTrue(schema.sameSchema(getSchema.get()));
    }

    @Test
    void testGetSchemaVersionWithoutInitTable() throws Exception {
        var tableName = RandomStringUtils.secure().nextAlphabetic(6);
        Properties configProperties = new Properties();
        configProperties.put("storagePath", baseDir.toAbsolutePath().toString());
        var configuration = new LakehouseConfiguration(configProperties);
        IcebergTable icebergTable = new IcebergTable(configuration, TableIdentifier.of(tableName));

        try {
            icebergTable.getSchemaByVersion(1);
            fail();
        } catch (NoSuchTableException e) {
            // expected
        }
    }

    @Test
    void testSchemaEvolutionFailureTracked() throws Exception {
        var tableName = RandomStringUtils.secure().nextAlphabetic(6);
        Properties configProperties = new Properties();
        configProperties.put("storagePath", baseDir.toAbsolutePath().toString());
        var configuration = new LakehouseConfiguration(configProperties);
        IcebergTable icebergTable = spy(new IcebergTable(configuration, TableIdentifier.of(tableName)));

        // Create initial table
        org.apache.iceberg.Schema schema = new org.apache.iceberg.Schema(
            Types.NestedField.required(1, "id", Types.IntegerType.get()));
        var tableOption = TableOptions.builder().schema(schema).build();
        icebergTable.create(tableOption);

        // Prepare a schema that will cause evolution failure (incompatible schema)
        org.apache.iceberg.Schema incompatibleSchema = new org.apache.iceberg.Schema(
            Types.NestedField.required(1, "id", Types.StringType.get())); // id type changed from int to string

        // First attempt - should fail and record -1 in schema mapping
        try {
            icebergTable.evolveSchemaWithVersion(1, incompatibleSchema);
            // If it doesn't throw, that's unexpected but we'll verify the mapping was recorded
        } catch (SchemaEvolutionException e) {
            // Expected - schema evolution should fail
        }

        // Verify that the failed schema evolution was recorded with -1
        var schemaMapping = icebergTable.getSchemaMapping();
        assertTrue(schemaMapping.containsKey(1L), "Schema mapping should contain version 1");
        assertEquals(-1, schemaMapping.get(1L), "Schema mapping should record -1 for failed evolution");

        // Second attempt - should throw SchemaEvolutionException immediately
        try {
            icebergTable.evolveSchemaWithVersion(1, incompatibleSchema);
            fail("Should throw SchemaEvolutionException when attempting to evolve a previously failed schema version");
        } catch (SchemaEvolutionException e) {
            assertTrue(e.getMessage().contains("The evolution of schema version ID 1 has failed before."),
                "Error message should indicate the schema version failed before");
        }

        // Try with a different version to ensure the table still works
        org.apache.iceberg.Schema compatibleSchema = new org.apache.iceberg.Schema(
            Types.NestedField.required(1, "id", Types.IntegerType.get()),
            Types.NestedField.optional(2, "name", Types.StringType.get()));
        icebergTable.evolveSchemaWithVersion(2, compatibleSchema);

        var getSchema = icebergTable.getSchemaByVersion(2);
        assertTrue(getSchema.isPresent(), "Version 2 should be available");
        assertTrue(compatibleSchema.sameSchema(getSchema.get()), "Version 2 schema should match");

        // Verify that version 1 still has -1
        assertEquals(-1, schemaMapping.get(1L), "Version 1 should still be marked as -1");
    }

    @Test
    void testCheckNullabilityDefaultIsFalse() {
        // Regression test for the constant change. Iceberg's CheckCompatibility
        // rejects schema transitions where an expected-required field is provided
        // as optional. With check-nullability=true by default, legitimate schema
        // registry changes (e.g. promoting optional->required) throw
        // IllegalArgumentException and propagate as a fatal error, stalling
        // compaction. The safe default for a streaming ingest system is to skip
        // the nullability check and let Iceberg's UpdateSchema do what it supports.
        LakehouseConfiguration configuration = new LakehouseConfiguration();
        assertTrue(configuration.checkIcebergNullability(),
            "iceberg.check-nullability default should be false for streaming ingest");
    }

    @Test
    void testUpdateTableSchemaIfNeeded_NullabilityPromotion_SucceedsWithDefaultConfig()
        throws Exception {
        // With default config (check-nullability=false), a schema that promotes an
        // optional field to required must not throw. Iceberg's UpdateSchema does
        // not actually promote optional->required, but the call must not be fatal.
        var tableName = RandomStringUtils.secure().nextAlphabetic(6);
        Properties configProperties = new Properties();
        configProperties.put("storagePath", baseDir.toAbsolutePath().toString());
        var configuration = new LakehouseConfiguration(configProperties);
        IcebergTable icebergTable = new IcebergTable(configuration, TableIdentifier.of(tableName));

        // Table has 'agee' as optional (prior soft-delete or just a permissive schema)
        org.apache.iceberg.Schema schema = new org.apache.iceberg.Schema(
            Types.NestedField.required(1, "id", Types.IntegerType.get()),
            Types.NestedField.required(2, "agee", Types.IntegerType.get()));
        icebergTable.create(TableOptions.builder().schema(schema).build());

        // Incoming schema promotes 'agee' back to required (bug's optional->required direction)
        org.apache.iceberg.Schema newSchema = new org.apache.iceberg.Schema(
            Types.NestedField.required(1, "id", Types.IntegerType.get()),
            Types.NestedField.optional(2, "agee", Types.IntegerType.get()));

        // Must not throw with the relaxed default.
        icebergTable.updateTableSchemaIfNeeded(newSchema);
    }

    @Test
    void testUpdateTableSchemaIfNeeded_NullabilityPromotion_ThrowsSchemaEvolutionException_WhenCheckEnabled()
        throws Exception {
        // When check-nullability is explicitly enabled, Iceberg throws
        // IllegalArgumentException ("X should be required, but is optional"). The
        // wrapper must surface this as SchemaEvolutionException so the caller
        // (evolveSchemaWithVersion) can mark the version as -1 and continue without
        // quarantining the topic. Before the fix, any inner failure was rewrapped as
        // RuntimeException which skips that non-fatal path.
        var tableName = RandomStringUtils.secure().nextAlphabetic(6);
        Properties configProperties = new Properties();
        configProperties.put("storagePath", baseDir.toAbsolutePath().toString());
        configProperties.put("iceberg.check-nullability", "true");
        var configuration = new LakehouseConfiguration(configProperties);
        assertTrue(configuration.checkIcebergNullability());

        IcebergTable icebergTable = new IcebergTable(configuration, TableIdentifier.of(tableName));

        org.apache.iceberg.Schema schema = new org.apache.iceberg.Schema(
            Types.NestedField.required(1, "id", Types.IntegerType.get()),
            Types.NestedField.optional(2, "agee", Types.IntegerType.get()));
        icebergTable.create(TableOptions.builder().schema(schema).build());

        org.apache.iceberg.Schema newSchema = new org.apache.iceberg.Schema(
            Types.NestedField.required(1, "id", Types.IntegerType.get()),
            Types.NestedField.required(2, "agee", Types.IntegerType.get()));

        var ex = assertThrows(SchemaEvolutionException.class,
            () -> icebergTable.updateTableSchemaIfNeeded(newSchema));
        assertNotNull(ex.getMessage());
        assertTrue(ex.getMessage().contains("should be required"),
            "Expected nullability validation message, got: " + ex.getMessage());
    }

    @Test
    void testUpdateTableSchemaIfNeeded_V3TypeOnV2Table_ThrowsSchemaEvolutionException()
        throws Exception {
        // This specifically exercises the inner try/catch: upgradeTableFormatIfNeeded
        // throws SchemaEvolutionException from INSIDE updateTableSchemaIfNeeded's
        // try block. Before the fix, the outer `catch (Exception)` rewrapped it as
        // RuntimeException, which broke the non-fatal contract upstream. After the
        // fix, the wrapper preserves SchemaEvolutionException type so callers can
        // distinguish it.
        Configuration conf = new Configuration();
        HadoopCatalog catalog = new HadoopCatalog(conf, baseDir.toUri().toString());

        org.apache.iceberg.Schema schema = new org.apache.iceberg.Schema(
            Types.NestedField.required(1, "id", Types.IntegerType.get()));
        TableIdentifier identifier = TableIdentifier.of("logs",
            RandomStringUtils.secure().nextAlphabetic(6));
        TableOptions tableOptions = TableOptions.builder().schema(schema).build();

        LakehouseConfiguration configuration = new LakehouseConfiguration(); // allowIcebergV3 defaults to false
        IcebergTable icebergTable = new IcebergTable(catalog, identifier, tableOptions, configuration);
        icebergTable.createIfAbsent();

        // Schema with a V3-only type on a V2 table with allowIcebergV3 disabled
        // triggers SchemaEvolutionException inside updateTableSchemaIfNeeded's try block.
        org.apache.iceberg.Schema v3Schema = new org.apache.iceberg.Schema(
            Types.NestedField.required(1, "id", Types.IntegerType.get()),
            Types.NestedField.optional(2, "payload", Types.VariantType.get()));

        assertThrows(SchemaEvolutionException.class,
            () -> icebergTable.updateTableSchemaIfNeeded(v3Schema));
    }

    @Test
    void testEvolveSchemaWithVersion_NullabilityPromotion_IsNotFatal_WhenCheckEnabled()
        throws Exception {
        // End-to-end: with check-nullability explicitly enabled, the evolution fails
        // but must stay non-fatal. evolveSchemaWithVersion catches
        // SchemaEvolutionException, records -1 in the schema mapping, commits the
        // transaction, then rethrows SE for upstream logging. Before the fix,
        // updateTableSchemaIfNeeded threw RuntimeException which bypassed this
        // non-fatal path and stalled compaction.
        var tableName = RandomStringUtils.secure().nextAlphabetic(6);
        Properties configProperties = new Properties();
        configProperties.put("storagePath", baseDir.toAbsolutePath().toString());
        configProperties.put("iceberg.check-nullability", "true");
        var configuration = new LakehouseConfiguration(configProperties);
        IcebergTable icebergTable = new IcebergTable(configuration, TableIdentifier.of(tableName));

        org.apache.iceberg.Schema schema = new org.apache.iceberg.Schema(
            Types.NestedField.required(1, "id", Types.IntegerType.get()),
            Types.NestedField.optional(2, "agee", Types.IntegerType.get()));
        icebergTable.create(TableOptions.builder().schema(schema).build());

        org.apache.iceberg.Schema newSchema = new org.apache.iceberg.Schema(
            Types.NestedField.required(1, "id", Types.IntegerType.get()),
            Types.NestedField.required(2, "agee", Types.IntegerType.get()));

        assertThrows(SchemaEvolutionException.class,
            () -> icebergTable.evolveSchemaWithVersion(1, newSchema));

        var schemaMapping = icebergTable.getSchemaMapping();
        assertTrue(schemaMapping.containsKey(1L), "Failed version should be recorded in mapping");
        assertEquals(-1, schemaMapping.get(1L),
            "Failed schema version must be marked -1 so the topic isn't quarantined");
    }

    @Test
    void testMetadataGrowthAfterCommits() throws IOException {
        // 1. Setup Catalog
        Configuration conf = new Configuration();
        HadoopCatalog catalog = new HadoopCatalog(conf, baseDir.toUri().toString());

        // 2. Create Table
        org.apache.iceberg.Schema schema = new org.apache.iceberg.Schema(
            Types.NestedField.required(1, "id", Types.IntegerType.get()),
            Types.NestedField.optional(2, "user", Types.StringType.get())
        );
        TableIdentifier id = TableIdentifier.of("logs", "events");
        Table table = catalog.createTable(id, schema);

        long metadataSize = IcebergTable.getLatestMetadataSize(table);
        assertTrue(metadataSize > 0);
        log.info("Initial metadata size: {} bytes", metadataSize);

        // 3. Perform 3 separate commits
        for (int i = 1; i <= 3; i++) {
            commitData(table, i, "User_" + i);
            long currentMetadataSize = IcebergTable.getLatestMetadataSize(table);
            log.info("--- After Commit {} --- metadata size: {} bytes", i, metadataSize);
            assertTrue(currentMetadataSize > metadataSize, "Metadata size should grow after commit");
            metadataSize = currentMetadataSize;
        }
    }

    @Test
    void testV3TableFormat() {
        Configuration conf = new Configuration();
        HadoopCatalog catalog = new HadoopCatalog(conf, baseDir.toUri().toString());

        org.apache.iceberg.Schema schema = new org.apache.iceberg.Schema(
            Types.NestedField.required(1, "id", Types.IntegerType.get()),
            Types.NestedField.optional(2, "user", Types.StringType.get()),
                Types.NestedField.optional(3, "name", Types.VariantType.get()),
                Types.NestedField.optional(4, "age", Types.VariantType.get())
        );

        TableIdentifier identifier = TableIdentifier.of("logs", "events");
        TableOptions tableOptions = TableOptions.builder()
            .schema(schema)
            .build();
        LakehouseConfiguration configuration = new LakehouseConfiguration();
        IcebergTable icebergTable = new IcebergTable(catalog, identifier, tableOptions, configuration);
        try {
            icebergTable.createIfAbsent();
            fail();
        } catch (Exception e) {
            assertInstanceOf(IllegalStateException.class, e);
            assertTrue(e.getMessage().contains("Invalid schema for v2"));
        }

        Properties properties = new Properties();
        properties.put("allowIcebergV3", "true");
        configuration = new LakehouseConfiguration(properties);
        icebergTable = new IcebergTable(catalog, identifier, tableOptions, configuration);
        try {
            icebergTable.createIfAbsent();
        } catch (Exception e) {
            fail();
        }
    }

    @Test
    void testUpgradeTableFormatVersion() {
        Configuration conf = new Configuration();
        HadoopCatalog catalog = new HadoopCatalog(conf, baseDir.toUri().toString());

        org.apache.iceberg.Schema schema = new org.apache.iceberg.Schema(
                Types.NestedField.required(1, "id", Types.IntegerType.get()),
                Types.NestedField.optional(2, "user", Types.StringType.get())
        );

        TableIdentifier identifier = TableIdentifier.of("logs", "events");
        TableOptions tableOptions = TableOptions.builder()
                .schema(schema)
                .build();
        LakehouseConfiguration configuration = new LakehouseConfiguration();
        IcebergTable icebergTable = new IcebergTable(catalog, identifier, tableOptions, configuration);
        icebergTable.createIfAbsent();

        org.apache.iceberg.Schema schemaV2 = new org.apache.iceberg.Schema(
                Types.NestedField.required(1, "id", Types.IntegerType.get()),
                Types.NestedField.optional(2, "user", Types.StringType.get()),
                Types.NestedField.optional(3, "name", Types.VariantType.get()),
                Types.NestedField.optional(4, "age", Types.VariantType.get())
        );
        Transaction txn = icebergTable.getTable().newTransaction();
        try {
            icebergTable.upgradeTableFormatIfNeeded(true, Optional.of(txn), schemaV2);
            fail();
        } catch (SchemaEvolutionException e) {
            // Schema evolution failed with not allow to upgrade from v2 to v3
        }

        Properties properties = new Properties();
        properties.put("allowIcebergV3", "true");
        configuration = new LakehouseConfiguration(properties);
        icebergTable = new IcebergTable(catalog, identifier, tableOptions, configuration);
        icebergTable.loadTable();
        txn = icebergTable.getTable().newTransaction();
        try {
            assertEquals(2, ((BaseTable) icebergTable.getTable()).operations().current().formatVersion());
            icebergTable.upgradeTableFormatIfNeeded(true, Optional.of(txn), schemaV2);
            txn.commitTransaction();
            icebergTable.getTable().refresh();
            assertEquals(3, ((BaseTable) icebergTable.getTable()).operations().current().formatVersion());
            txn = icebergTable.getTable().newTransaction();
            icebergTable.updateTableSchemaIfNeeded(Optional.of(txn), schemaV2);
            txn.commitTransaction();
            icebergTable.getTable().refresh();
            assertTrue(schemaV2.sameSchema(icebergTable.getTable().schema()));
        } catch (SchemaEvolutionException e) {
            fail();
        }

    }

    private void commitData(Table table, int id, String name) throws IOException {
        // Prepare a local data file path
        String filepath = table.location() + "/data/" + UUID.randomUUID() + ".parquet";
        OutputFile outputFile = table.io().newOutputFile(filepath);

        // Write one record to a Parquet file
        DataWriter<GenericRecord> dataWriter = Parquet.writeData(outputFile)
            .schema(table.schema())
            .createWriterFunc(GenericParquetWriter::create)
            .overwrite()
            .withSpec(PartitionSpec.unpartitioned())
            .build();

        GenericRecord record = GenericRecord.create(table.schema());
        dataWriter.write((GenericRecord) record.copy("id", id, "user", name));
        dataWriter.close();

        // Commit the data file to the table
        DataFile dataFile = dataWriter.toDataFile();
        table.newAppend().appendFile(dataFile).commit();
    }
}
