/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.lakehouse;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import io.lakestream.ursa.lakehouse.writer.UrsaParquetFileWriter;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.UUID;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag("lakehouse")
@Slf4j
public class UrsaParquetFileWriterTest {

    @Test
    public void testGenerateNextFilePath() {
        String compression = "snappy";
        String partitionColumnPath = "";
        String tablePath = "/tmp/delta/data/test_v1";

        // table path end without "/" and partitionColumnPath is empty.
        String suffix = "-c000." + compression.toLowerCase(Locale.ROOT) + ".parquet";
        String prefix = tablePath + "/" + partitionColumnPath;
        String path = UrsaParquetFileWriter.generateNextFilePath(partitionColumnPath, tablePath, compression);
        assertTrue(path.startsWith(prefix));
        assertTrue(path.endsWith(suffix));

        // table path end without "/", and partitionColumnPath is not empty.
        tablePath = "/tmp/delta/data/test_v1";
        partitionColumnPath = "a=1/b=2";
        prefix = tablePath + "/" + partitionColumnPath;
        path = UrsaParquetFileWriter.generateNextFilePath(partitionColumnPath, tablePath, compression);
        assertTrue(path.startsWith(prefix));
        assertTrue(path.endsWith(suffix));

        // table path end with "/" and partitionColumnPath is empty.
        tablePath = "/tmp/delta/data/test_v1/";
        partitionColumnPath = "";
        prefix = tablePath + partitionColumnPath;
        path = UrsaParquetFileWriter.generateNextFilePath(partitionColumnPath, tablePath, compression);
        assertTrue(path.startsWith(prefix));
        assertTrue(path.endsWith(suffix));

        // table path end with "/" and partitionColumnPath is not empty;
        tablePath = "/tmp/delta/data/test_v1/";
        partitionColumnPath = "a=1/b=2";
        prefix = tablePath + partitionColumnPath;
        path = UrsaParquetFileWriter.generateNextFilePath(partitionColumnPath, tablePath, compression);
        assertTrue(path.startsWith(prefix));
        assertTrue(path.endsWith(suffix));
    }

    @Test
    public void testOpenNewFile() {
        String path = "/tmp/test_delta-" + UUID.randomUUID();
        Configuration configuration = new Configuration();
        String compression = "snappy";

        List<Schema.Field> fields = new ArrayList<>();
        Schema schema = Schema.createRecord("people", "", "", false);
        fields.add(new Schema.Field("name", Schema.create(Schema.Type.STRING)));
        fields.add(new Schema.Field("age", Schema.create(Schema.Type.INT)));
        fields.add(new Schema.Field("phone", Schema.create(Schema.Type.STRING)));
        fields.add(new Schema.Field("address", Schema.create(Schema.Type.STRING)));
        fields.add(new Schema.Field("score", Schema.create(Schema.Type.DOUBLE)));
        schema.setFields(fields);

        GenericData.Record record = new GenericData.Record(schema);
        record.put("name", "hang");
        record.put("phone", "110");
        record.put("address", "GuangZhou, China");
        record.put("age", 18);
        record.put("score", 59.9);

        try {
            ParquetWriter<GenericRecord> writer = UrsaParquetFileWriter
                    .openNewFile(path, schema, 10 * 1024 * 1024, configuration, compression);
            assertEquals(writer.getDataSize(), 0);
            writer.write(record);
            writer.close();
            Map<String, String> metadata = writer.getFooter().getFileMetaData().getKeyValueMetaData();
            assertEquals(metadata.get("parquet.avro.schema"), record.getSchema().toString());
            assertEquals(metadata.get("writer.model.name"), "avro");

        } catch (IOException e) {
            fail();
        }

        new File(path).deleteOnExit();
    }

    @Test
    public void testWriter() throws Exception {
        String path = "/tmp/test_delta-" + UUID.randomUUID();
        Configuration configuration = new Configuration();
        String compression = "snappy";

        List<Schema.Field> fields = new ArrayList<>();
        Schema schema = Schema.createRecord("people", "", "", false);
        fields.add(new Schema.Field("name", Schema.create(Schema.Type.STRING)));
        fields.add(new Schema.Field("age", Schema.create(Schema.Type.INT)));
        fields.add(new Schema.Field("phone", Schema.create(Schema.Type.STRING)));
        fields.add(new Schema.Field("address", Schema.create(Schema.Type.STRING)));
        fields.add(new Schema.Field("score", Schema.create(Schema.Type.DOUBLE)));
        schema.setFields(fields);

        List<GenericRecord> recordList = new ArrayList<>();
        try {
            ParquetWriter<GenericRecord> writer = UrsaParquetFileWriter
                    .openNewFile(path, schema, 10 * 1024 * 1024, configuration, compression);
            for (int i = 0; i < 100; ++i) {
                GenericData.Record record = new GenericData.Record(schema);
                record.put("name", "hang");
                record.put("phone", "110");
                record.put("address", "GuangZhou, China");
                record.put("age", i);
                record.put("score", 59.9 + i);
                recordList.add(record);
                writer.write(record);
            }
            writer.close();

            // open the parquet file to check value.
            GroupReadSupport readSupport = new GroupReadSupport();
            ParquetReader<Group> reader = ParquetReader.builder(readSupport, new Path(path)).build();
            Group line;
            int cnt = 0;
            while ((line = reader.read()) != null) {
                GenericRecord record1 = recordList.get(cnt);
                assertEquals(line.getDouble("score", 0), record1.get("score"));
                assertEquals(line.getString("address", 0), record1.get("address"));
                assertEquals(line.getString("phone", 0), record1.get("phone"));
                assertEquals(line.getString("name", 0), record1.get("name"));
                assertEquals(line.getInteger("age", 0), record1.get("age"));
                cnt++;
            }
        } catch (IOException e) {
            fail();
        }

        new File(path).deleteOnExit();
    }

    @Test
    public void testGetFileSize() {
        String path = "/tmp/test_delta-" + UUID.randomUUID();
        Configuration configuration = new Configuration();
        String compression = "snappy";

        List<Schema.Field> fields = new ArrayList<>();
        Schema schema = Schema.createRecord("people", "", "", false);
        fields.add(new Schema.Field("name", Schema.create(Schema.Type.STRING)));
        fields.add(new Schema.Field("age", Schema.create(Schema.Type.INT)));
        fields.add(new Schema.Field("phone", Schema.create(Schema.Type.STRING)));
        fields.add(new Schema.Field("address", Schema.create(Schema.Type.STRING)));
        fields.add(new Schema.Field("score", Schema.create(Schema.Type.DOUBLE)));
        schema.setFields(fields);

        try {
            ParquetWriter<GenericRecord> writer = UrsaParquetFileWriter
                    .openNewFile(path, schema, 10 * 1024 * 1024, configuration, compression);
            assertEquals(writer.getDataSize(), 0);
            GenericData.Record record = new GenericData.Record(schema);
            record.put("name", "hang");
            record.put("phone", "110");
            record.put("address", "GuangZhou, China");
            record.put("age", 18);
            record.put("score", 59.9);
            writer.write(record);
            writer.close();

            UrsaParquetFileWriter deltaParquetFileWriter =
                    new UrsaParquetFileWriter(configuration, "", compression, null, "",
                        10 * 1024 * 1024, new HashMap<>());
            assertEquals(new File(path).length(), deltaParquetFileWriter.getFileSize(path));
        } catch (IOException e) {
            fail();
        } finally {
            new File(path).deleteOnExit();
        }
    }
}
