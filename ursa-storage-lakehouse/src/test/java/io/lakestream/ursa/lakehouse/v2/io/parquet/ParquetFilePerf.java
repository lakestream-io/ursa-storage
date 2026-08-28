/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.lakehouse.v2.io.parquet;

import io.lakestream.ursa.lakehouse.LakehouseConfiguration;
import java.nio.file.Path;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.io.TempDir;

@Slf4j
public class ParquetFilePerf {

    @TempDir
    Path path;

    void perf() throws Exception {

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

        var configuration = new LakehouseConfiguration();
        ParquetFileWriter writer = new ParquetFileWriter(path.toUri(), configuration);
        // Generate records
        long start = System.currentTimeMillis();
        for (int i = 0; i < 10000000; i++) {
            GenericRecord record = new GenericData.Record(schema);
            record.put("id", i);
            record.put("name", "record-" + i);
            record.put("value", i * 100);
            record.put("timestamp", System.currentTimeMillis());
            writer.write(record, Map.of("metadata", "test-metadata-" + i));
        }

        log.info("Time taken to write 10 million records: {} ms", System.currentTimeMillis() - start);

        var result = writer.close();
        ParquetWriteResult result1 = (ParquetWriteResult) result.get(0);

        ParquetFileReader reader = new ParquetFileReader(result1.getDirectory().resolve(result1.getDataFile()), configuration);

        start = System.currentTimeMillis();
        var interval = System.currentTimeMillis();
        for (int i = 0; i < 10000000; i++) {
            reader.read();
            if (i != 0 && i % 100000 == 0) {
                log.info("Read {} records in {} ms", i, System.currentTimeMillis() - interval);
                interval = System.currentTimeMillis();
            }
        }
        log.info("Read 10 million records in {} ms", System.currentTimeMillis() - start);

        reader.close();

    }
}
