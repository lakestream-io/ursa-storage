/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.lakehouse.v2.io.parquet;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.lakestream.ursa.lakehouse.LakehouseConfiguration;
import io.lakestream.ursa.lakehouse.serde.proto.Data;
import io.lakestream.ursa.metrics.InstrumentProvider;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.metrics.SdkMeterProvider;
import io.opentelemetry.sdk.metrics.data.MetricData;
import io.opentelemetry.sdk.testing.exporter.InMemoryMetricReader;
import java.io.EOFException;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

@Timeout(value = 10, unit = TimeUnit.SECONDS)
@Tag("lakehouse")
public class UrsaParquetFileWriterReaderTest {

    protected URI uri;
    protected LakehouseConfiguration configuration;

    @BeforeEach
    public void setup() throws IOException {
        configuration = new LakehouseConfiguration();
        var path = Files.createTempDirectory("ursa-parquet-file-writer-test");
        uri = path.toUri();
    }

    @Test
    public void testWriteAvro() throws Exception {
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

        // write records
        ParquetFileWriter<GenericRecord> writer = new ParquetFileWriter<GenericRecord>(uri, configuration);
        for (GenericRecord record : records) {
            writer.write(record, new HashMap<>());
        }
        var results = writer.close();

        var wr = (ParquetWriteResult) results.get(0);
        assertTrue(wr.getDataFileSize() > 0);
        assertEquals(100, wr.getNumberOfRecords());
        assertEquals(Utils.ensureIsDirectory(uri), wr.getDirectory());

        // read records
        URI readUri = wr.getDirectory().resolve(wr.getDataFile());
        ParquetFileReader<GenericRecord> reader = new ParquetFileReader<>(readUri, configuration);
        ParquetFileReader.Record<GenericRecord> record = null;
        List<GenericRecord> readRecords = new ArrayList<>();
        try {
            while ((record = reader.read()) != null) {
                readRecords.add(record.getRecord());
            }
        } catch (EOFException e) {
            // ignore
        }

        assertEquals(100, readRecords.size());
        assertEquals(records, readRecords);
        reader.close();
    }

    @Test
    public void testWriteProtobuf() throws Exception {
        List<Data> records = new ArrayList<>();

        for (int i = 0; i < 100; i++) {
            Data data = Data.newBuilder()
                .setIndex(i)
                .setName("record-" + i)
                .putTags(i, "tag" + i)
                .build();
            records.add(data);
        }

        ParquetFileWriter<Data> writer = new ParquetFileWriter<Data>(uri, configuration);
        for (Data record : records) {
            writer.write(record, new HashMap<>());
        }
        var results = writer.close();

        var wr = (ParquetWriteResult) results.get(0);
        assertTrue(wr.getDataFileSize() > 0);
        assertEquals(100, wr.getNumberOfRecords());
        assertEquals(Utils.ensureIsDirectory(uri), wr.getDirectory());

        // read records
        URI readUri = wr.getDirectory().resolve(wr.getDataFile());
        ParquetFileReader<Data> reader = new ParquetFileReader<>(readUri, configuration);
        ParquetFileReader.Record<Data> record = null;
        List<Data> readRecords = new ArrayList<>();
        try {
            while ((record = reader.read()) != null) {
                readRecords.add(record.getRecord());
            }
        } catch (EOFException e) {
            // ignore
        }
        reader.close();
        assertEquals(100, readRecords.size());
        assertEquals(records, readRecords);
    }

    @Test
    void testMetrics() throws Exception {
        InMemoryMetricReader inMemoryMetricReader = InMemoryMetricReader.create();
        SdkMeterProvider meterProvider = SdkMeterProvider.builder()
            .registerMetricReader(inMemoryMetricReader)
            .build();
        OpenTelemetry otel = OpenTelemetrySdk.builder()
            .setMeterProvider(meterProvider)
            .build();
        InstrumentProvider instrumentProvider = new InstrumentProvider(otel);

        ParquetFileWriter<Data> writer = new ParquetFileWriter<Data>(uri, configuration, instrumentProvider);
        writer.setSecondaryIndexKey("key");
        Data data = Data.newBuilder().build();
        writer.write(data, Map.of("key", "value"));
        var results = writer.close();
        var wr = (ParquetWriteResult) results.get(0);
        URI readUri = wr.getDirectory().resolve(wr.getDataFile());
        ParquetFileReader<Data> reader = new ParquetFileReader<>(readUri, configuration, instrumentProvider);
        reader.seek(0);
        reader.seekBySecondaryIndex("value");
        reader.read();
        reader.close();

        Collection<MetricData> metrics = inMemoryMetricReader.collectAllMetrics();
        assertTrue(metrics.stream().anyMatch(
            metric -> metric.getName().equals("ursa.storage.lakehouse.parquet.write_record.duration")));
        assertTrue(metrics.stream().anyMatch(
            metric -> metric.getName().equals("ursa.storage.lakehouse.parquet.write_metadata.duration")));
        assertTrue(metrics.stream().anyMatch(
            metric -> metric.getName().equals("ursa.storage.lakehouse.parquet.read_record.duration")));
        assertTrue(metrics.stream().anyMatch(
            metric -> metric.getName().equals("ursa.storage.lakehouse.parquet.read_metadata.duration")));
        assertTrue(metrics.stream().anyMatch(
            metric -> metric.getName().equals("ursa.storage.lakehouse.parquet.seek_by_offset.duration")));
        assertTrue(metrics.stream().anyMatch(
            metric -> metric.getName().equals("ursa.storage.lakehouse.parquet.seek_by_secondary_index.duration")));
    }

}
