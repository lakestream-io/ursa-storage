/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.lakehouse.v2.io.parquet;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.fail;

import io.lakestream.ursa.lakehouse.LakehouseConfiguration;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

@Tag("lakehouse")
public class IndexFileTest {

    @TempDir
    Path path;

    @Test
    public void testReadWrite() throws IOException {
        var fileName = "file-" + RandomStringUtils.secure().nextNumeric(4);
        IndexFileWriter writer = new IndexFileWriter(path.toUri(), fileName, new LakehouseConfiguration());

        Map<String, String> data = Collections.singletonMap("key", "value");
        Map<String, String> data2 = Collections.singletonMap("key2", "value2");

        writer.write(data);
        writer.write(data2);
        writer.close();

        IndexFileReader reader = new IndexFileReader(path.toUri(), fileName, new LakehouseConfiguration());
        var result = reader.read(0);
        assertEquals(data, result);
        result = reader.read(1);
        assertEquals(data2, result);
        try {
            reader.read(2);
            fail("shouldn't read this line");
        } catch (Exception e) {
            // expected
        }

        // seek back
        result = reader.read(0);
        assertEquals(data, result);

        reader.close();
    }

    @Test
    void testSeekWithSecondaryIndex() throws IOException {
        var fileName = "file-" + RandomStringUtils.secure().nextNumeric(4);
        IndexFileWriter writer = new IndexFileWriter(path.toUri(), fileName, new LakehouseConfiguration());
        writer.setSecondaryIndexKey("secondaryKey");

        Map<String, String> data1 = Map.of("secondaryKey", "value1", "data", "data1");
        Map<String, String> data2 = Map.of("secondaryKey", "value2", "data", "data2");
        Map<String, String> data3 = Map.of("secondaryKey", "value3", "data", "data3");

        writer.write(data1);
        writer.write(data2);
        writer.write(data3);
        writer.close();

        IndexFileReader reader = new IndexFileReader(path.toUri(), fileName, new LakehouseConfiguration());

        // Seek by secondary index
        int row = reader.seekBySecondaryIndex("value1");
        assertEquals(0, row); // First occurrence of value1

        row = reader.seekBySecondaryIndex("value2");
        assertEquals(1, row); // First occurrence of value2

        try {
            reader.seekBySecondaryIndex("nonexistent");
            fail("shouldn't find this secondary index");
        } catch (Exception e) {
            // expected
        }

        reader.close();
    }

    @Test
    void testSeekWithSecondaryIndexEnabledAllowApproximateMatching() throws Exception {
        var fileName = "file-" + RandomStringUtils.secure().nextNumeric(4);
        Properties properties = new Properties();
        properties.put("allowApproximateMatching", "true");
        var configuration = new LakehouseConfiguration(properties);
        IndexFileWriter writer = new IndexFileWriter(path.toUri(), fileName, configuration);
        writer.setSecondaryIndexKey("secondaryKey");


        Map<String, String> data1 = Map.of("secondaryKey", "9", "data", "data1");
        Map<String, String> data2 = Map.of("secondaryKey", "10", "data", "data2");
        Map<String, String> data3 = Map.of("secondaryKey", "20", "data", "data3");
        Map<String, String> data4 = Map.of("secondaryKey", "100", "data", "data4");

        writer.write(data1);
        writer.write(data2);
        writer.write(data3);
        writer.write(data4);
        writer.close();

        IndexFileReader reader = new IndexFileReader(path.toUri(), fileName, configuration);

        // Seek by secondary index
        assertThrows(IOException.class, () -> reader.seekBySecondaryIndex("4"));

        int row = reader.seekBySecondaryIndex("15");
        assertEquals(1, row); // First occurrence of value1

        row = reader.seekBySecondaryIndex("25");
        assertEquals(2, row); // First occurrence of value2

        row = reader.seekBySecondaryIndex("110");
        assertEquals(3, row);

        row = reader.seekBySecondaryIndex("9");
        assertEquals(0, row);

        row = reader.seekBySecondaryIndex("10");
        assertEquals(1, row);

        reader.close();
    }

    @Test
    void testConfiguration() {
        Properties properties = new Properties();
        properties.put("ursaIndexFileReaderBufferSize", "1024");
        properties.put("ursaIndexFileWriterBufferSize", "1024");
        properties.put("ursaIndexFileWriterMaxBufferedRecords", "10");
        LakehouseConfiguration configuration = new LakehouseConfiguration(properties);
        var fileName = "file-" + RandomStringUtils.secure().nextNumeric(4);
        IndexFileWriter writer = new IndexFileWriter(path.toUri(), fileName, configuration);
        assertEquals(1024, writer.getWriteBufferSize());
        assertEquals(10, writer.getMaxBufferedRecords());

        IndexFileReader reader = new IndexFileReader(path.toUri(), fileName, configuration);
        assertEquals(1024, reader.getReadBufferSize());
    }

    @Test
    void testReadWithoutCache() throws Exception {
        Properties properties = new Properties();
        properties.put("ursaIndexFileWriterMaxBufferedRecords", "5");
        var fileName = "file-" + RandomStringUtils.secure().nextNumeric(4);
        var writer = new IndexFileWriter(path.toUri(), fileName, new LakehouseConfiguration(properties));

        for (int i = 0; i < 12; i++) {
            writer.write(Map.of("key" + i, "value" + i));
        }
        writer.close();

        var reader = new IndexFileReader(path.toUri(), fileName, new LakehouseConfiguration(properties));
        var result = reader.read(0);
        assertEquals(Map.of("key0", "value0"), result);

        result = reader.read(6);
        assertEquals(Map.of("key6", "value6"), result);

        result = reader.read(11);
        assertEquals(Map.of("key11", "value11"), result);

        result = reader.read(1);
        assertEquals(Map.of("key1", "value1"), result);

        try {
            reader.read(12);
        } catch (Exception e) {
            assertEquals("Reached end of index file or no more metadata available.", e.getMessage());
        }

        result = reader.read(11);
        assertEquals(Map.of("key11", "value11"), result);

        reader.close();
    }

}
