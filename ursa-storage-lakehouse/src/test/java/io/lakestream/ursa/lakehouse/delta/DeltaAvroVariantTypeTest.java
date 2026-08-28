/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.lakehouse.delta;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.delta.kernel.Snapshot;
import io.delta.kernel.types.StructType;
import io.delta.kernel.types.VariantType;
import io.delta.kernel.utils.CloseableIterator;
import io.lakestream.ursa.lakehouse.LakehouseConfiguration;
import io.lakestream.ursa.lakehouse.utils.AvroSchemaUtilExtended;
import io.lakestream.ursa.lakehouse.writer.ParquetFileStat;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.apache.avro.LogicalType;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

@Tag("lakehouse")
class DeltaAvroVariantTypeTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    @TempDir
    Path tempDir;

    @Test
    void testAvroVariantToDeltaEndToEnd() throws Exception {
        Schema avroSchema = buildAvroVariantSchema();
        StructType deltaSchema = AvroSchemaUtilExtended.toDelta(avroSchema, true);
        assertInstanceOf(VariantType.class, deltaSchema.get("attributes").getDataType());

        List<GenericRecord> avroRecords = buildAvroRecords(avroSchema);

        Properties properties = new Properties();
        properties.put("storagePath", tempDir.toString());
        properties.setProperty("mockUnityCatalog", "true");
        properties.put("mockedUnityCatalogRootStorage", tempDir.toString());
        properties.setProperty("streamTableMode", "external");
        properties.setProperty("tableEvolveSchemaEnabled", "true");

        LakehouseConfiguration config = new LakehouseConfiguration(properties);
        UCExternalTable table =
            (UCExternalTable) ExternalDeltaTableFactory.getDeltaTable(config, "default/avro-variant");

        table.createDeltaTable(null, deltaSchema);

        Snapshot snapshot = table.getLatestSnapshot();
        assertNotNull(snapshot);
        assertInstanceOf(VariantType.class, snapshot.getSchema().get("attributes").getDataType());

        List<ParquetFileStat> fileStats =
            writeVariantRecords(table.getTableLocation(), config.getHadoopConfiguration(), deltaSchema, avroRecords);
        assertEquals(1, fileStats.size());
        assertNotNull(fileStats.get(0).getStats());
        assertFalse(fileStats.get(0).getStats().contains("attributes"));

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

        List<Map<String, Object>> records = readVariantRecords(fileStats.get(0).getFileFullPath());
        assertEquals(5, records.size());
        assertEquals(MAPPER.readTree(
                "{\"profile\":{\"age\":25,\"city\":\"San Francisco\",\"geo\":{\"lat\":37.77,\"lon\":-122.41},"
                    + "\"events\":[{\"type\":\"login\",\"devices\":[\"ios\",\"web\"]},"
                    + "{\"type\":\"purchase\",\"items\":[{\"sku\":\"a1\",\"qty\":2},{\"sku\":\"b9\",\"qty\":1}]}]}}"),
            MAPPER.readTree((String) records.get(0).get("attributes")));
        assertEquals(MAPPER.readTree("{\"flags\":{\"active\":true,\"verified\":false},\"metrics\":{\"age\":30,"
                + "\"scores\":[1,2,3,{\"bonus\":[7,8,{\"deep\":\"yes\"}]}]}}"),
            MAPPER.readTree((String) records.get(1).get("attributes")));
        assertEquals(MAPPER.readTree("[1,{\"nested\":[2,3,{\"items\":[4,5,{\"inner\":[\"x\",\"y\",{\"z\":1}]}]}]},6]"),
            MAPPER.readTree((String) records.get(2).get("attributes")));
        assertEquals(MAPPER.readTree("42.5"), MAPPER.readTree((String) records.get(3).get("attributes")));
        assertEquals(MAPPER.readTree("\"simple string value\""),
            MAPPER.readTree((String) records.get(4).get("attributes")));
    }

    private Schema buildAvroVariantSchema() {
        Schema variantSchema = Schema.createRecord("attribute_variant", null, "", false);
        variantSchema.setFields(List.of(new Schema.Field("metadata", Schema.create(Schema.Type.BYTES), null, null),
            new Schema.Field("value", Schema.create(Schema.Type.BYTES), null, null)));
        new LogicalType("variant").addToSchema(variantSchema);
        variantSchema.addProp("variant-metadata-fields", "[\"age\", \"city\", \"active\", \"score\"]");

        return SchemaBuilder.record("AvroVariantRecord").namespace("io.lakestream.test").fields().requiredLong("id")
            .requiredString("name").name("attributes").type().unionOf().nullType().and().type(variantSchema).endUnion()
            .nullDefault().endRecord();
    }

    private List<GenericRecord> buildAvroRecords(Schema avroSchema) {
        List<GenericRecord> records = new ArrayList<>();

        GenericRecord record1 = new GenericData.Record(avroSchema);
        record1.put("id", 1L);
        record1.put("name", "Alice");
        record1.put("attributes", Map.of(
                "profile", Map.of(
                        "age", 25,
                        "city", "San Francisco",
                        "geo", Map.of("lat", 37.77d, "lon", -122.41d),
                        "events", List.of(
                                Map.of("type", "login", "devices", List.of("ios", "web")),
                                Map.of("type", "purchase",
                                        "items", List.of(
                                                Map.of("sku", "a1", "qty", 2),
                                                Map.of("sku", "b9", "qty", 1)
                                        ))
                        ))));
        records.add(record1);

        GenericRecord record2 = new GenericData.Record(avroSchema);
        record2.put("id", 2L);
        record2.put("name", "Bob");
        record2.put("attributes", Map.of(
                "flags", Map.of("active", true, "verified", false),
                "metrics", Map.of(
                        "age", 30,
                        "scores", List.of(1, 2, 3, Map.of("bonus", List.of(7, 8, Map.of("deep", "yes")))))));
        records.add(record2);

        GenericRecord record3 = new GenericData.Record(avroSchema);
        record3.put("id", 3L);
        record3.put("name", "Charlie");
        record3.put("attributes", List.of(
                1,
                Map.of("nested", List.of(
                        2,
                        3,
                        Map.of("items", List.of(
                                4,
                                5,
                                Map.of("inner", List.of("x", "y", Map.of("z", 1)))
                        ))
                )),
                6));
        records.add(record3);

        GenericRecord record4 = new GenericData.Record(avroSchema);
        record4.put("id", 4L);
        record4.put("name", "Diana");
        record4.put("attributes", 42.5d);
        records.add(record4);

        GenericRecord record5 = new GenericData.Record(avroSchema);
        record5.put("id", 5L);
        record5.put("name", "Eve");
        record5.put("attributes", "simple string value");
        records.add(record5);

        return records;
    }

    private List<ParquetFileStat> writeVariantRecords(String location, Configuration hadoopConfig,
                                                      StructType deltaSchema, List<GenericRecord> avroRecords)
        throws IOException {
        ParquetRowWriter parquetRowWriter =
            new ParquetRowWriter(location, hadoopConfig, Collections.emptyList(), deltaSchema, 1000);

        for (GenericRecord avroRecord : avroRecords) {
            GenericRow row = AvroToDeltaConvert.convert(avroRecord, deltaSchema);
            parquetRowWriter.write(row);
        }

        return parquetRowWriter.close();
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
                records.add(Map.of("id", record.get("id"), "name", record.get("name").toString(), "attributes",
                    DeltaVariantUtils.deserializeToJsonString(metadataBytes, valueBytes)));
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
