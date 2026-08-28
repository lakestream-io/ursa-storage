/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.delta.kernel.defaults.internal.parquet;

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.delta.kernel.data.Row;
import io.delta.kernel.defaults.engine.hadoopio.HadoopFileIO;
import io.delta.kernel.defaults.internal.data.DefaultRowBasedColumnarBatch;
import io.delta.kernel.types.IntegerType;
import io.delta.kernel.types.StringType;
import io.delta.kernel.types.StructType;
import io.lakestream.ursa.lakehouse.delta.GenericRow;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.hadoop.ParquetOutputFormat;
import org.apache.parquet.hadoop.ParquetReader;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag("lakehouse")
public class CustomKernelParquetWriterTest {

    @Test
    public void multiFileWriterRollsOverBySize() throws IOException {
        StructType schema = new StructType()
                .add("sequence", IntegerType.INTEGER, false)
                .add("payload", StringType.STRING, false);
        Configuration hadoopConfig = new Configuration();
        hadoopConfig.setLong(CustomKernelParquetWriter.TARGET_FILE_SIZE_CONF, 512);
        hadoopConfig.set(ParquetOutputFormat.COMPRESSION, "UNCOMPRESSED");
        Path location = Files.createTempDirectory("custom-kernel-parquet-writer-rollover-");

        try {
            CustomKernelParquetWriter writer = CustomKernelParquetWriter.multiFileWriter(
                    new HadoopFileIO(hadoopConfig), location.toString(), List.of());
            writer.write(new DefaultRowBasedColumnarBatch(schema, rows(schema, 0, 2)));
            writer.write(new DefaultRowBasedColumnarBatch(schema, rows(schema, 2, 2)));
            writer.write(new DefaultRowBasedColumnarBatch(schema, rows(schema, 4, 2)));
            writer.close();

            List<Path> parquetFiles = listParquetFiles(location);
            assertEquals(3, parquetFiles.size());

            List<Integer> sequences = new ArrayList<>();
            for (Path parquetFile : parquetFiles) {
                ParquetReader<GenericRecord> reader =
                        AvroParquetReader.<GenericRecord>builder(new org.apache.hadoop.fs.Path(
                                parquetFile.toString())).build();
                GenericRecord record;
                int rowCount = 0;
                while ((record = reader.read()) != null) {
                    sequences.add((Integer) record.get("sequence"));
                    rowCount++;
                }
                reader.close();
                assertEquals(2, rowCount);
            }

            sequences.sort(Integer::compareTo);
            assertEquals(List.of(0, 1, 2, 3, 4, 5), sequences);
        } finally {
            deleteDirectory(location);
        }
    }

    private List<Row> rows(StructType schema, int start, int size) {
        List<Row> rows = new ArrayList<>();
        for (int i = 0; i < size; i++) {
            int sequence = start + i;
            Map<Integer, Object> values = new HashMap<>();
            values.put(0, sequence);
            values.put(1, "payload-" + sequence + "-" + "0123456789abcdef".repeat(256));
            rows.add(new GenericRow(schema, values));
        }
        return rows;
    }

    private List<Path> listParquetFiles(Path directory) throws IOException {
        try (Stream<Path> stream = Files.list(directory)) {
            return stream.filter(path -> path.getFileName().toString().endsWith(".parquet")).toList();
        }
    }

    private void deleteDirectory(Path directory) throws IOException {
        if (!Files.exists(directory)) {
            return;
        }
        try (Stream<Path> stream = Files.walk(directory)) {
            List<Path> paths = stream.sorted(Comparator.reverseOrder()).toList();
            for (Path path : paths) {
                Files.delete(path);
            }
        }
    }
}
