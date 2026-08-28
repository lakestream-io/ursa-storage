/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.lakehouse.writer;

import static org.apache.parquet.hadoop.ParquetWriter.DEFAULT_PAGE_SIZE;

import io.lakestream.ursa.lakehouse.utils.JsonUtils;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import lombok.Data;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.column.statistics.BinaryStatistics;
import org.apache.parquet.column.statistics.Statistics;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.ColumnPath;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.io.api.Binary;

/**
 * DeltaParquetFile writer.
 */
@Slf4j
@Data
public class UrsaParquetFileWriter {

    protected String tablePath;
    protected Schema schema;
    protected ParquetWriter<GenericRecord> writer;
    protected String partitionColumnPath;
    protected Map<String, String> partitionValues;
    protected Configuration configuration;
    protected long lastRollFileTimestamp;
    protected String currentFileFullPath;
    protected final AtomicBoolean isClosed = new AtomicBoolean(true);
    protected final String compression;
    protected final long rowGroupSize;
    protected long numRecords;
    protected String fileStats = "";

    public UrsaParquetFileWriter(Configuration configuration, String tablePath, String compression,
                                 Schema schema, long rowGroupSize) {
        this.configuration = configuration;
        this.tablePath = tablePath;
        this.schema = schema;
        this.lastRollFileTimestamp = System.currentTimeMillis();
        this.partitionColumnPath = null;
        this.currentFileFullPath = "";
        this.compression = compression;
        this.rowGroupSize = rowGroupSize;
        this.partitionValues = Collections.emptyMap();
    }

    public UrsaParquetFileWriter(Configuration configuration, String tablePath, String compression, Schema schema,
                                 String partitionColumnPath, long rowGroupSize,
                                 Map<String, String> partitionValues) {
        this(configuration, tablePath, compression, schema, rowGroupSize);
        this.partitionColumnPath = partitionColumnPath;
        this.partitionValues = partitionValues;
    }


    public long getFileSize(String fileFullPath) throws IOException {
        Path path = new Path(fileFullPath);
        HadoopInputFile hadoopInputFile = HadoopInputFile.fromPath(path, configuration);
        return hadoopInputFile.getLength();
    }

    public void updateSchema(Schema schema) {
        this.schema = schema;
    }

    public List<ParquetFileStat> flushAndClose() throws IOException {
        if (isClosed.get()) {
            return Collections.emptyList();
        }
        String closedFileFullPath = currentFileFullPath;
        String filePath = currentFileFullPath.substring(
            currentFileFullPath.indexOf(tablePath) + tablePath.length() + 1);
        String fileStats = close();
        lastRollFileTimestamp = System.currentTimeMillis();
        ParquetFileStat fileStat =
            new ParquetFileStat(filePath, closedFileFullPath, getFileSize(closedFileFullPath),
                fileStats, partitionValues, new HashMap<>());
        return Collections.singletonList(fileStat);
    }

    public void writeToParquetFile(GenericRecord record) throws IOException {
        if (isClosed.get() || StringUtils.isBlank(currentFileFullPath)) {
            currentFileFullPath = generateNextFilePath(partitionColumnPath, tablePath, compression);
            writer = openNewFile(currentFileFullPath, schema, rowGroupSize, configuration, compression);
            isClosed.set(false);
            fileStats = "";
        }
        writer.write(record);
        numRecords++;
    }


    protected String close() throws IOException {
        if (isClosed.get()) {
            return fileStats;
        }
        try {
            if (writer != null) {
                try {
                    log.info("start to close internal parquet writer, filePath: {}, records: {}",
                            currentFileFullPath, numRecords);
                    writer.close();
                    fileStats = parseFileStats(writer, numRecords);
                } catch (IOException e) {
                    log.error("close internal parquet writer failed. filePath: {}", currentFileFullPath, e);
                    throw e;
                }
            }
        } finally {
            numRecords = 0;
            writer = null;
            isClosed.set(true);
            currentFileFullPath = "";
        }
        return fileStats;
    }

    public static String parseFileStats(ParquetWriter writer, long numRecords) {
        if (writer == null) {
            return "";
        }
        return parseFileStats(writer.getFooter().getBlocks(), numRecords);
    }

    public static String parseFileStats(List<BlockMetaData> blocks, long numRecords) {
        if (!CollectionUtils.isEmpty(blocks)) {
            BlockMetaData blockMetaData = blocks.get(0);
            Map<String, Object> minValues = new HashMap<>();
            Map<String, Object> maxValues = new HashMap<>();
            Map<String, Object> nullCount = new HashMap<>();

            Map<String, Object> nestedMinValues = new HashMap<>();
            Map<String, Object> nestedMaxValues = new HashMap<>();
            Map<String, Object> nestedNullCount = new HashMap<>();

            for (ColumnChunkMetaData column : blockMetaData.getColumns()) {
                ColumnPath path = column.getPath();
                List<String> paths = Arrays.asList(path.toArray());
                Statistics statistics = column.getStatistics();
                Object min;
                Object max;
                if (statistics instanceof BinaryStatistics) {
                    min = statistics.genericGetMin() == null ? "" :
                        ((Binary) statistics.genericGetMin()).toStringUsingUTF8();
                    max = statistics.genericGetMax() == null ? "" :
                        ((Binary) statistics.genericGetMax()).toStringUsingUTF8();
                } else {
                    min = statistics.genericGetMin();
                    max = statistics.genericGetMax();
                }
                long numNulls = statistics.getNumNulls();
                //ignore the array type and map type.
                if (paths.size() > 1) {
                    if (paths.contains("array") || paths.contains("key_value")) {
                        continue;
                    }
                    Map<String, Object> minTargetMap = nestedMinValues;
                    Map<String, Object> maxTargetMap = nestedMaxValues;
                    Map<String, Object> nullTargetMap = nestedNullCount;
                    for (int i = 0; i < paths.size(); i++) {
                        String parentPath = paths.get(i);
                        if (i == paths.size() - 1) {
                            minTargetMap.put(parentPath, min);
                            maxTargetMap.put(parentPath, max);
                            nullTargetMap.put(parentPath, numNulls);
                        } else {
                            minTargetMap =
                                (Map<String, Object>) minTargetMap.computeIfAbsent(parentPath, k -> new HashMap());
                            maxTargetMap =
                                (Map<String, Object>) maxTargetMap.computeIfAbsent(parentPath, k -> new HashMap());
                            nullTargetMap =
                                (Map<String, Object>) nullTargetMap.computeIfAbsent(parentPath, k -> new HashMap());
                        }
                    }
                } else {
                    String fieldName = paths.get(0);
                    minValues.put(fieldName, min);
                    maxValues.put(fieldName, max);
                    nullCount.put(fieldName, numNulls);
                }
            }
            minValues.putAll(nestedMinValues);
            maxValues.putAll(nestedMaxValues);
            nullCount.putAll(nestedNullCount);
            String rowJson = JsonUtils.toJson(new StatsWrapper(numRecords, minValues, maxValues, nullCount));
            return JsonUtils.toJson(rowJson);
        }
        return "";
    }

    public static String parseFileStatsOnlyRecordNum(long numRecords) {
        String rowJson = JsonUtils.toJson(
                new StatsWrapper(numRecords, Collections.emptyMap(), Collections.emptyMap(), Collections.emptyMap()));
        return JsonUtils.toJson(rowJson);
    }

    @Getter
    public static class StatsWrapper {
        private final long numRecords;
        private final Map<String, Object> minValues;
        private final Map<String, Object> maxValues;
        private final Map<String, Object> nullCount;

        public StatsWrapper(long numRecords, Map<String, Object> minValues, Map<String, Object> maxValues,
                            Map<String, Object> nullCount) {
            this.numRecords = numRecords;
            this.minValues = minValues;
            this.maxValues = maxValues;
            this.nullCount = nullCount;
        }
    }

    public static String generateParquetFile(String compression) {
        return "part-0000-" + UUID.randomUUID() + "-c000." + compression.toLowerCase(Locale.ROOT) + ".parquet";
    }

    public static String generateNextFilePath(String partitionColumnPath, String tablePath, String compression) {
        StringBuilder sb = new StringBuilder();
        String parquetFile = generateParquetFile(compression);
        sb.append(tablePath);
        if (!tablePath.endsWith("/")) {
            sb.append("/");
        }
        if (!StringUtils.isBlank(partitionColumnPath)) {
            sb.append(partitionColumnPath).append("/");
        }
        return sb.append(parquetFile).toString();
    }

    public static ParquetWriter<GenericRecord> openNewFile(String currentFileFullPath, Schema schema, long rowGroupSize,
                                                           Configuration configuration, String compression)
            throws IOException {
        ParquetWriter<GenericRecord> writer = AvroParquetWriter.<GenericRecord>builder(new Path(currentFileFullPath))
            .withRowGroupSize(rowGroupSize)
            .withPageSize(DEFAULT_PAGE_SIZE)
            .withSchema(schema)
            .withConf(configuration)
            .withCompressionCodec(CompressionCodecName.valueOf(compression.toUpperCase(Locale.ROOT)))
            .withValidation(false)
            .withDictionaryEncoding(false)
            .build();

        log.info("open: {} parquet writer succeed. {}", currentFileFullPath, writer);
        return writer;
    }
}
