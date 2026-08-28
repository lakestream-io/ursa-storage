/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.lakehouse.v2.io.parquet;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

import io.lakestream.ursa.lakehouse.LakehouseConfiguration;
import io.lakestream.ursa.lakehouse.v2.LocalFileTestBase;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag("lakehouse")
public class ParquetFileReaderTest {

    static LocalFileTestBase localFileTestBase = new LocalFileTestBase();

    @BeforeAll
    static void beforeAll() throws Exception {
        localFileTestBase.beforeAll();
    }

    @AfterAll
    static void afterAll() throws Exception {
        localFileTestBase.afterAll();
    }

    private String generateFile(LakehouseConfiguration configuration) throws IOException {
        Random random = new Random();
        List<GenericRecord> records = new ArrayList<>();

        // Create Avro schema for the test records
        String schemaJson = "{"
                            + "\"type\": \"record\","
                            + "\"name\": \"TestRecord\","
                            + "\"fields\": ["
                            + "  {\"name\": \"id\", \"type\": \"int\"},"
                            + "  {\"name\": \"name\", \"type\": \"string\"},"
                            + "  {\"name\": \"value\", \"type\": \"double\"},"
                            + "  {\"name\": \"timestamp\", \"type\": \"long\"}"
                            + "]}";
        Schema schema = new Schema.Parser().parse(schemaJson);

        // Generate records
        for (int i = 0; i < 100; i++) {
            GenericRecord record = new GenericData.Record(schema);
            record.put("id", i);
            record.put("name", "record-" + i);
            record.put("value", random.nextDouble() * 100);
            record.put("timestamp", System.currentTimeMillis());
            records.add(record);
        }

        ParquetFileWriter<GenericRecord> writer = new ParquetFileWriter<>(
            localFileTestBase.getPath().toUri(), configuration);
        writer.setSecondaryIndexKey("secondaryIndex");

        var secondaryIndex = "record-";
        for (int i = 0; i < records.size(); i++) {
            writer.write(records.get(i), Map.of("secondaryIndex", secondaryIndex + (i * 10)));
        }
        var wr = writer.close();
        return ((ParquetWriteResult) wr.get(0)).getDataFile();
    }

    @Test
    public void testSeek() throws Exception{
        var configuration = new LakehouseConfiguration();
        String filename = generateFile(configuration);
        var uri = Paths.get(localFileTestBase.getPath().toString(), filename).toUri();
        ParquetFileReader<GenericRecord> reader = new ParquetFileReader<>(uri, configuration);

        assertEquals(100, reader.lastRowInFile);
        assertEquals(0, reader.currentReadRow);
        assertEquals(0, reader.currentRowGroupIndex);
        assertEquals(0, reader.lastRowOfCurrentRowGroup);

        reader.seek(10);
        assertEquals(100, reader.lastRowInFile);
        assertEquals(10, reader.currentReadRow);
        assertEquals(0, reader.currentRowGroupIndex);
        assertEquals(100, reader.lastRowOfCurrentRowGroup);

        var record = reader.read();
        assertEquals(10, record.getRecord().get("id"));

        reader.seek(90);
        assertEquals(100, reader.lastRowInFile);
        assertEquals(90, reader.currentReadRow);
        assertEquals(0, reader.currentRowGroupIndex);
        assertEquals(100, reader.lastRowOfCurrentRowGroup);
        record = reader.read();
        assertEquals(90, record.getRecord().get("id"));

        reader.seek(0);
        assertEquals(100, reader.lastRowInFile);
        assertEquals(0, reader.currentReadRow);
        assertEquals(0, reader.currentRowGroupIndex);
        assertEquals(100, reader.lastRowOfCurrentRowGroup);
        record = reader.read();
        assertEquals(0, record.getRecord().get("id"));

        reader.seek(0);
        for (int i = 0; i < 5; i++) {
            record = reader.read();
            assertEquals(i, record.getRecord().get("id"));
        }
        assertEquals(100, reader.lastRowInFile);
        assertEquals(5, reader.currentReadRow);
        assertEquals(0, reader.currentRowGroupIndex);
        assertEquals(100, reader.lastRowOfCurrentRowGroup);

        // seek to an invalid position
        try {
            reader.seek(-1);
            fail("Should have thrown exception");
        } catch (Exception e) {
            // should have an exception
        }

        try {
            reader.seek(100);
            fail("Should have thrown exception");
        } catch (Exception e) {
            // should have an exception
        }

        reader.close();
    }

    @Test
    void testSeekWithSecondaryIndex() throws Exception {
        var configuration = new LakehouseConfiguration();
        String filename = generateFile(configuration);
        var uri = Paths.get(localFileTestBase.getPath().toString(), filename).toUri();
        ParquetFileReader<GenericRecord> reader = new ParquetFileReader<>(uri, configuration);

        // Seek by secondary index
        int row = reader.seekBySecondaryIndex("record-10");
        assertEquals(1, row); // First occurrence of record-1
        assertEquals(100, reader.lastRowInFile);
        assertEquals(1, reader.currentReadRow);
        assertEquals(0, reader.currentRowGroupIndex);
        assertEquals(100, reader.lastRowOfCurrentRowGroup);

        row = reader.seekBySecondaryIndex("record-90");
        assertEquals(9, row); // First occurrence of record-9
        assertEquals(100, reader.lastRowInFile);
        assertEquals(9, reader.currentReadRow);
        assertEquals(0, reader.currentRowGroupIndex);
        assertEquals(100, reader.lastRowOfCurrentRowGroup);

        // seek back
        row = reader.seekBySecondaryIndex("record-0");
        assertEquals(0, row); // First occurrence of record-0
        assertEquals(100, reader.lastRowInFile);
        assertEquals(0, reader.currentReadRow);
        assertEquals(0, reader.currentRowGroupIndex);
        assertEquals(100, reader.lastRowOfCurrentRowGroup);

        try {
            reader.seekBySecondaryIndex("nonexistent");
            fail("shouldn't find this secondary index");
        } catch (Exception e) {
            // expected
        }

        reader.close();
    }
}
