/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.lakehouse.writer;

import io.lakestream.ursa.lakehouse.LakehouseConfiguration;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;

/**
 * partitioned delta parquet file writer.
 */
@Slf4j
public class PartitionedParquetFileWriter {
    private final Map<String, UrsaParquetFileWriter> writerMap = new ConcurrentHashMap<>();
    private final String tablePath;
    private Schema schema;
    private final List<String> partitionColumns;
    private final Configuration configuration;
    private final String compression;
    private final long rowGroupSize;

    public PartitionedParquetFileWriter(LakehouseConfiguration config,
                                        Configuration hadoopConfig,
                                        String tablePath,
                                        List<String> partitionColumns,
                                        Schema schema) {
        this.configuration = hadoopConfig;
        this.tablePath = tablePath;
        this.partitionColumns = partitionColumns;
        this.schema = schema;
        this.compression = config.getCompressType();
        this.rowGroupSize = config.getRowGroupSize();
    }

    public List<ParquetFileStat> flushAndClose() throws IOException {
        List<ParquetFileStat> fileStats = new ArrayList<>();
        for (UrsaParquetFileWriter writer : writerMap.values()) {
            List<ParquetFileStat> stats = writer.flushAndClose();
            if (stats == null) {
                continue;
            }
            fileStats.addAll(stats);
        }
        return fileStats;
    }

    public void removeWriter(String key) {
        writerMap.remove(key);
    }

    public void writeToParquetFile(GenericRecord record) throws IOException {
        String partitionValue = getPartitionValuePath(record, partitionColumns);
        UrsaParquetFileWriter writer = writerMap.get(partitionValue);
        if (writer == null) {
            writer = new UrsaParquetFileWriter(configuration, tablePath,
                compression, schema, partitionValue, rowGroupSize, getPartitionValues(record, partitionColumns));
            writerMap.put(partitionValue, writer);
        }
        writer.writeToParquetFile(record);
    }

    public static Map<String, String> getPartitionValues(GenericRecord genericRecord,
                                                         List<String> partitionColumns) {
        Map<String, String> partitionValues = new ConcurrentHashMap<>();
        if (partitionColumns == null || partitionColumns.isEmpty()) {
            return partitionValues;
        }
        for (String partitionColumn : partitionColumns) {
            Schema.Field field = genericRecord.getSchema().getField(partitionColumn);
            if (field == null) {
                continue;
            }
            partitionValues.put(partitionColumn, String.valueOf(genericRecord.get(field.name())));
        }
        return partitionValues;
    }

    public static String getPartitionValuePath(List<Object> values, List<String> partitionColumns) {
        if (partitionColumns == null || partitionColumns.isEmpty()) {
            return "";
        }
        StringBuilder pathBuilder = new StringBuilder();
        boolean first = true;
        for (int i = 0; i < partitionColumns.size(); i++) {
            if (!first) {
                pathBuilder.append("/");
            }
            pathBuilder.append(partitionColumns.get(i))
                .append("=")
                .append(values.get(i));
            first = false;
        }
        return pathBuilder.toString();
    }

    public static String getPartitionValuePath(GenericRecord genericRecord, List<String> partitionColumns) {
        if (partitionColumns == null || partitionColumns.isEmpty()) {
            return "";
        }
        StringBuilder pathBuilder = new StringBuilder();
        boolean first = true;
        for (String column : partitionColumns) {
            Schema.Field field = genericRecord.getSchema().getField(column);
            if (field == null) {
                return "";
            }
            if (!first) {
                pathBuilder.append("/");
            }
            pathBuilder.append(field.name())
                .append("=")
                .append(genericRecord.get(field.name()));
            first = false;
        }
        return pathBuilder.toString();
    }

    public void updateSchema(Schema schema) {
        for (UrsaParquetFileWriter writer : writerMap.values()) {
            writer.updateSchema(schema);
        }
        this.schema = schema;
    }

}
