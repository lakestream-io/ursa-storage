/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.lakehouse.delta;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.delta.kernel.Snapshot;
import io.delta.kernel.types.LongType;
import io.delta.kernel.types.StringType;
import io.delta.kernel.types.StructField;
import io.delta.kernel.types.StructType;
import io.delta.kernel.types.VariantType;
import io.delta.kernel.utils.CloseableIterator;
import io.lakestream.ursa.lakehouse.LakehouseConfiguration;
import io.lakestream.ursa.lakehouse.writer.ParquetFileStat;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

@Tag("lakehouse")
class DeltaVariantTypeTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    @TempDir
    Path tempDir;

    @Test
    void testVariantTypeEndToEnd() throws Exception {
        Properties properties = new Properties();
        properties.put("storagePath", tempDir.toString());
        properties.setProperty("mockUnityCatalog", "true");
        properties.put("mockedUnityCatalogRootStorage", tempDir.toString());
        properties.setProperty("streamTableMode", "external");
        properties.setProperty("tableEvolveSchemaEnabled", "true");

        LakehouseConfiguration config = new LakehouseConfiguration(properties);
        UCExternalTable table =
            (UCExternalTable) ExternalDeltaTableFactory.getDeltaTable(config, "default/example-variant");


        StructType schema = new StructType().add("id", LongType.LONG).add("name", StringType.STRING)
            .add("attributes", VariantType.VARIANT)
            .add("nested", new StructType(List.of(
                new StructField("count", LongType.LONG, true),
                new StructField("details", VariantType.VARIANT, true),
                new StructField("note", StringType.STRING, true)
            )));

        table.createDeltaTable(null, schema);

        Snapshot snapshot = table.getLatestSnapshot();
        assertNotNull(snapshot);
        assertInstanceOf(VariantType.class, snapshot.getSchema().get("attributes").getDataType());
        StructType nestedSnapshotSchema = (StructType) snapshot.getSchema().get("nested").getDataType();
        assertInstanceOf(VariantType.class, nestedSnapshotSchema.get("details").getDataType());

        List<ParquetFileStat> fileStats =
            writeVariantRecords(table.getTableLocation(), config.getHadoopConfiguration(), schema);
        assertEquals(1, fileStats.size());
        assertNotNull(fileStats.get(0).getStats());
        assertFalse(fileStats.get(0).getStats().contains("attributes"));
        assertFalse(fileStats.get(0).getStats().contains("details"));
        assertTrue(fileStats.get(0).getStats().contains("nested"));
        assertTrue(fileStats.get(0).getStats().contains("count"));
        assertTrue(fileStats.get(0).getStats().contains("note"));

        table.commit(Collections.singletonList(ParquetFileStat.fromDeltaFiles(fileStats, Collections.emptyMap())));

        Snapshot committedSnapshot = table.getLatestSnapshot();
        assertNotNull(committedSnapshot);

        List<AddFileAction> addFileActions = new ArrayList<>();
        CloseableIterator<AddFileAction> addActionIterator =
            DeltaTableUtils.getAddActionIterator(table.getLatestSnapshot(), table.getEngine());
        while (addActionIterator.hasNext()) {
            addFileActions.add(addActionIterator.next());
        }
        addActionIterator.close();
        assertEquals(addFileActions.size(), 1);
        assertNotNull(addFileActions.get(0).getStats());
        assertFalse(addFileActions.get(0).getStats().contains("attributes"));
        assertFalse(addFileActions.get(0).getStats().contains("details"));
        assertTrue(addFileActions.get(0).getStats().contains("nested"));
        assertTrue(addFileActions.get(0).getStats().contains("count"));
        assertTrue(addFileActions.get(0).getStats().contains("note"));

        List<Map<String, Object>> records = readVariantRecords(fileStats.get(0).getFileFullPath());
        assertEquals(4, records.size());
        assertEquals(MAPPER.readTree(
                "{\"profile\":{\"age\":30,\"city\":\"NYC\",\"history\":[{\"year\":2023,\"tags\":[\"a\",\"b\"]},"
                    + "{\"year\":2024,\"tags\":[\"c\"],\"scores\":[1,2,{\"deep\":true}]}]}}"),
            MAPPER.readTree((String) records.get(0).get("attributes")));
        assertEquals(
            MAPPER.readTree("[1,{\"nested\":[2,3,{\"items\":[\"test\",{\"flag\":false,\"meta\":{\"depth\":3}}]}]},4]"),
            MAPPER.readTree((String) records.get(1).get("attributes")));
        assertEquals(MAPPER.readTree("\"simple string\""), MAPPER.readTree((String) records.get(2).get("attributes")));
        assertEquals(MAPPER.readTree("null"), MAPPER.readTree((String) records.get(3).get("attributes")));

        assertEquals(MAPPER.readTree(
                "{\"audit\":{\"flags\":[true,false],\"level\":\"gold\"},\"tags\":[\"x\",\"y\"]}"),
            MAPPER.readTree((String) records.get(0).get("nestedDetails")));
        assertEquals(10L, records.get(0).get("nestedCount"));
        assertEquals("first", records.get(0).get("nestedNote"));
        assertEquals(MAPPER.readTree(
                "[{\"type\":\"home\",\"active\":true},{\"type\":\"work\",\"active\":false}]"),
            MAPPER.readTree((String) records.get(1).get("nestedDetails")));
        assertEquals(20L, records.get(1).get("nestedCount"));
        assertEquals("second", records.get(1).get("nestedNote"));
        assertEquals(MAPPER.readTree("\"nested string\""), MAPPER.readTree((String) records.get(2).get("nestedDetails")));
        assertEquals(30L, records.get(2).get("nestedCount"));
        assertEquals("third", records.get(2).get("nestedNote"));
        assertEquals(MAPPER.readTree("null"), MAPPER.readTree((String) records.get(3).get("nestedDetails")));
        assertEquals(40L, records.get(3).get("nestedCount"));
        assertEquals("fourth", records.get(3).get("nestedNote"));
    }

    private List<ParquetFileStat> writeVariantRecords(String location, Configuration hadoopConfig, StructType schema)
        throws IOException {
        ParquetRowWriter parquetRowWriter =
            new ParquetRowWriter(location, hadoopConfig, Collections.emptyList(), schema, 1000);

        parquetRowWriter.write(newVariantRow(schema, 1L, "User1",
            "{\"profile\":{\"age\":30,\"city\":\"NYC\",\"history\":[{\"year\":2023,\"tags\":[\"a\",\"b\"]},"
                + "{\"year\":2024,\"tags\":[\"c\"],\"scores\":[1,2,{\"deep\":true}]}]}}",
            "{\"audit\":{\"flags\":[true,false],\"level\":\"gold\"},\"tags\":[\"x\",\"y\"]}",
            10L,
            "first"));
        parquetRowWriter.write(newVariantRow(schema, 2L, "User2",
            "[1,{\"nested\":[2,3,{\"items\":[\"test\",{\"flag\":false,\"meta\":{\"depth\":3}}]}]},4]",
            "[{\"type\":\"home\",\"active\":true},{\"type\":\"work\",\"active\":false}]",
            20L,
            "second"));
        parquetRowWriter.write(newVariantRow(schema, 3L, "User3", "\"simple string\"", "\"nested string\"", 30L, "third"));
        parquetRowWriter.write(newVariantRow(schema, 4L, "User4", "null", "null", 40L, "fourth"));

        return parquetRowWriter.close();
    }

    private GenericRow newVariantRow(StructType schema, long id, String name, String variantJson,
                                     String nestedVariantJson, long nestedCount, String nestedNote) {
        GenericRow row = new GenericRow(schema, new HashMap<>());
        row.put(schema.indexOf("id"), id);
        row.put(schema.indexOf("name"), name);
        row.put(schema.indexOf("attributes"), DeltaVariantUtils.fromJson(variantJson));
        StructType nestedSchema = (StructType) schema.get("nested").getDataType();
        GenericRow nestedRow = new GenericRow(nestedSchema, new HashMap<>());
        nestedRow.put(nestedSchema.indexOf("count"), nestedCount);
        nestedRow.put(nestedSchema.indexOf("details"), DeltaVariantUtils.fromJson(nestedVariantJson));
        nestedRow.put(nestedSchema.indexOf("note"), nestedNote);
        row.put(schema.indexOf("nested"), nestedRow);
        return row;
    }

    private List<Map<String, Object>> readVariantRecords(String fileFullPath) throws Exception {
        List<Map<String, Object>> records = new ArrayList<>();
        org.apache.hadoop.fs.Path path = new org.apache.hadoop.fs.Path(fileFullPath);
        try (ParquetReader<GenericRecord> reader = AvroParquetReader.<GenericRecord>builder(path).build()) {
            GenericRecord record;
            while ((record = reader.read()) != null) {
                GenericRecord variantRecord = (GenericRecord) record.get("attributes");
                byte[] valueBytes = toByteArray((ByteBuffer) variantRecord.get(DeltaVariantUtils.VALUE));
                byte[] metadataBytes = toByteArray((ByteBuffer) variantRecord.get(DeltaVariantUtils.METADATA));
                GenericRecord nestedRecord = (GenericRecord) record.get("nested");
                GenericRecord nestedVariantRecord = (GenericRecord) nestedRecord.get("details");
                byte[] nestedValueBytes = toByteArray((ByteBuffer) nestedVariantRecord.get(DeltaVariantUtils.VALUE));
                byte[] nestedMetadataBytes =
                    toByteArray((ByteBuffer) nestedVariantRecord.get(DeltaVariantUtils.METADATA));
                records.add(Map.of(
                    "id", record.get("id"),
                    "name", record.get("name").toString(),
                    "attributes", DeltaVariantUtils.deserializeToJsonString(metadataBytes, valueBytes),
                    "nestedCount", nestedRecord.get("count"),
                    "nestedNote", nestedRecord.get("note").toString(),
                    "nestedDetails", DeltaVariantUtils.deserializeToJsonString(nestedMetadataBytes, nestedValueBytes)));
            }
        }
        return records;
    }

    private byte[] toByteArray(ByteBuffer buffer) {
        ByteBuffer duplicate = buffer.duplicate();
        byte[] bytes = new byte[duplicate.remaining()];
        duplicate.get(bytes);
        return bytes;
    }
}
