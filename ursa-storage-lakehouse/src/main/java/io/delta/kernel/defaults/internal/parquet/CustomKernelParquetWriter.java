/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.delta.kernel.defaults.internal.parquet;

import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static io.delta.kernel.defaults.internal.DefaultKernelUtils.getDataType;
import static io.delta.kernel.defaults.internal.parquet.ParquetIOUtils.createParquetOutputFile;
import static io.delta.kernel.internal.util.Preconditions.checkArgument;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Collections.emptyMap;
import static java.util.Objects.requireNonNull;
import static java.util.function.UnaryOperator.identity;

import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.Multimap;
import io.delta.kernel.Meta;
import io.delta.kernel.data.ColumnarBatch;
import io.delta.kernel.defaults.engine.fileio.FileIO;
import io.delta.kernel.defaults.engine.fileio.InputFile;
import io.delta.kernel.defaults.engine.fileio.OutputFile;
import io.delta.kernel.defaults.internal.parquet.ParquetColumnWriters.ColumnWriter;
import io.delta.kernel.expressions.Column;
import io.delta.kernel.expressions.Literal;
import io.delta.kernel.internal.fs.Path;
import io.delta.kernel.statistics.DataFileStatistics;
import io.delta.kernel.types.BinaryType;
import io.delta.kernel.types.BooleanType;
import io.delta.kernel.types.ByteType;
import io.delta.kernel.types.DataType;
import io.delta.kernel.types.DateType;
import io.delta.kernel.types.DecimalType;
import io.delta.kernel.types.DoubleType;
import io.delta.kernel.types.FloatType;
import io.delta.kernel.types.IntegerType;
import io.delta.kernel.types.LongType;
import io.delta.kernel.types.ShortType;
import io.delta.kernel.types.StringType;
import io.delta.kernel.types.StructType;
import io.delta.kernel.types.TimestampNTZType;
import io.delta.kernel.types.TimestampType;
import io.delta.kernel.utils.DataFileStatus;
import io.delta.kernel.utils.FileStatus;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.column.ParquetProperties.WriterVersion;
import org.apache.parquet.column.statistics.BinaryStatistics;
import org.apache.parquet.column.statistics.IntStatistics;
import org.apache.parquet.column.statistics.LongStatistics;
import org.apache.parquet.column.statistics.Statistics;
import org.apache.parquet.format.converter.ParquetMetadataConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetOutputFormat;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.io.api.RecordConsumer;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;

/**
 * This class is follow io.delta.kernel.defaults.internal.parquet.ParquetFileWriter, the delta ParquetFileWriter use
 * iterator to write data, it's not convenient for the streaming mode. So we use CustomKernelParquetWriter to support
 * write the data directly to improve memory usage.
 *
 */
public class CustomKernelParquetWriter {
    public static final String TARGET_FILE_SIZE_CONF = "delta.kernel.default.parquet.writer.targetMaxFileSize";
    public static final long DEFAULT_TARGET_FILE_SIZE = 128 * 1024 * 1024; // 128MB

    private final FileIO fileIO;
    private final boolean writeAsSingleFile;
    private final String location;
    private final boolean atomicWrite;
    private final long targetMaxFileSize;
    private final List<Column> statsColumns;

    private long currentFileNumber; // used to generate the unique file names.
    private ParquetWriter writer;
    private BatchWriteSupport batchWriteSupport;
    private org.apache.parquet.io.OutputFile outPutFile;
    private long currentFileRowCount;
    private StructType dataSchema;
    private final List<DataFileStatus> closedDataFileStatuses;

    /**
     * Create writer to write data into one or more files depending upon the {@code
     * delta.kernel.default.parquet.writer.targetMaxFileSize} value and the given data.
     *
     * @param fileIO       File IO implementation to use for reading and writing files.
     * @param location     Location to write the data. Should be a directory.
     * @param statsColumns List of columns to collect statistics for. The statistics collection is
     *                     optional.
     */
    public static CustomKernelParquetWriter multiFileWriter(FileIO fileIO, String location, List<Column> statsColumns) {
        return new CustomKernelParquetWriter(fileIO, location, /* writeAsSingleFile = */ false, /* atomicWrite = */
                false, statsColumns);
    }

    /**
     * Create writer to write the data exactly into one file.
     *
     * @param fileIO       File IO implementation to use for reading and writing files.
     * @param location     Location to write the data. Shouldn't be a directory.
     * @param atomicWrite  If true, write the file is written atomically (i.e. either the entire
     *                     content is written or none, but won't create a file with the partial contents).
     * @param statsColumns List of columns to collect statistics for. The statistics collection is
     *                     optional.
     */
    public static CustomKernelParquetWriter singleFileWriter(FileIO fileIO, String location, boolean atomicWrite,
                                                             List<Column> statsColumns) {
        return new CustomKernelParquetWriter(fileIO, location, /* writeAsSingleFile = */ true, atomicWrite,
                statsColumns);
    }

    /**
     * Private constructor to create the writer. Use {@link #multiFileWriter} or {@link
     * #singleFileWriter} to create the writer.
     */
    private CustomKernelParquetWriter(FileIO fileIO, String location, boolean writeAsSingleFile, boolean atomicWrite,
                                      List<Column> statsColumns) {
        this.fileIO = requireNonNull(fileIO, "fileIO is null");
        this.writeAsSingleFile = writeAsSingleFile;
        this.location = requireNonNull(location, "location is null");
        this.atomicWrite = atomicWrite;
        this.statsColumns = requireNonNull(statsColumns, "statsColumns is null");
        this.targetMaxFileSize =
                fileIO.getConf(TARGET_FILE_SIZE_CONF).map(Long::valueOf).orElse(DEFAULT_TARGET_FILE_SIZE);
        checkArgument(targetMaxFileSize > 0, "Invalid target Parquet file size: %s", targetMaxFileSize);
        this.closedDataFileStatuses = new ArrayList<>();
    }

    BatchWriteSupport createOrGetWriteSupport(StructType inputSchema) {
        MessageType parquetSchema = ParquetSchemaUtils.toParquetSchema(inputSchema);
        return new BatchWriteSupport(inputSchema, parquetSchema);
    }

    public void write(ColumnarBatch dataBatch) throws IOException {
        if (dataSchema == null) {
            dataSchema = dataBatch.getSchema();
            this.batchWriteSupport = createOrGetWriteSupport(dataBatch.getSchema());
        }
        ColumnWriter[] columnWriters = ParquetColumnWriters.createColumnVectorWriters(dataBatch);
        batchWriteSupport.setColumnVectorWriters(columnWriters);

        int size = dataBatch.getSize();
        if (writer == null) {
            outPutFile = createParquetOutputFile(generateNextOutputFile(), atomicWrite);
            writer = createWriter(outPutFile, batchWriteSupport);
        }
        for (int i = 0; i < size; i++) {
            writer.write(i);
            currentFileRowCount++;
        }
        if (!writeAsSingleFile && currentFileRowCount > 0 && writer.getDataSize() >= targetMaxFileSize) {
            closedDataFileStatuses.add(closeCurrentWriter());
        }
    }

    public DataFileStatus close() throws IOException {
        if (writer != null) {
            return closeCurrentWriter();
        }
        return closedDataFileStatuses.isEmpty() ? null : closedDataFileStatuses.get(closedDataFileStatuses.size() - 1);
    }

    public List<DataFileStatus> closeAll() throws IOException {
        List<DataFileStatus> result = new ArrayList<>(closedDataFileStatuses);
        if (writer != null) {
            result.add(closeCurrentWriter());
        }
        return result;
    }

    private DataFileStatus closeCurrentWriter() throws IOException {
        writer.close();
        DataFileStatus status = constructDataFileStatus(outPutFile.getPath(), dataSchema, currentFileRowCount);
        writer = null;
        outPutFile = null;
        currentFileRowCount = 0;
        return status;
    }

    /**
     * Implementation of {@link WriteSupport} to write the {@link ColumnarBatch} to Parquet files.
     * {@link ParquetWriter} makes use of this interface to consume the data row by row and write to
     * the Parquet file. Call backs from the {@link ParquetWriter} includes:
     *
     * <ul>
     *   <li>{@link #init(Configuration)}: Called once to init and get {@link WriteContext} which
     *       includes the schema and extra properties.
     *   <li>{@link #prepareForWrite(RecordConsumer)}: Called once to prepare for writing the data.
     *       {@link RecordConsumer} is a way for this batch support to write data for each column in
     *       the current row.
     *   <li>{@link #write(Integer)}: Called for each row to write the data. In this method, column
     *       values are passed to the {@link RecordConsumer} through series of calls.
     * </ul>
     */
    private static class BatchWriteSupport extends WriteSupport<Integer> {
        final StructType inputSchema;
        final MessageType parquetSchema;

        private ColumnWriter[] columnWriters;
        private RecordConsumer recordConsumer;

        BatchWriteSupport(StructType inputSchema, // WriteSupport created for this specific schema
                          MessageType parquetSchema) { // Parquet equivalent schema
            this.inputSchema = requireNonNull(inputSchema, "inputSchema is null");
            this.parquetSchema = requireNonNull(parquetSchema, "parquetSchema is null");
        }

        void setColumnVectorWriters(ColumnWriter[] columnWriters) {
            this.columnWriters = requireNonNull(columnWriters, "columnVectorWriters is null");
        }

        @Override
        public String getName() {
            return "delta-kernel-default-parquet-writer";
        }

        @Override
        public WriteContext init(Configuration configuration) {
            Map<String, String> extraProps = Collections.singletonMap("io.delta.kernel.default-parquet-writer",
                    "Kernel-Defaults-" + Meta.KERNEL_VERSION);
            return new WriteContext(parquetSchema, extraProps);
        }

        @Override
        public void prepareForWrite(RecordConsumer recordConsumer) {
            this.recordConsumer = recordConsumer;
        }

        @Override
        public void write(Integer rowId) {
            // Use java asserts which are disabled in prod to reduce the overhead
            // and enabled in tests with `-ea` argument.
            assert (recordConsumer != null) : "Parquet record consumer is null";
            assert (columnWriters != null) : "Column writers are not set";
            recordConsumer.startMessage();
            for (int i = 0; i < columnWriters.length; i++) {
                columnWriters[i].writeRowValue(recordConsumer, rowId);
            }
            recordConsumer.endMessage();
        }
    }

    /**
     * Generate the next file path to write the data.
     */
    private OutputFile generateNextOutputFile() {
        if (writeAsSingleFile) {
            checkArgument(currentFileNumber++ == 0, "expected to write just one file");
            return fileIO.newOutputFile(location);
        }
        String fileName = String.format("%s-%03d.parquet", UUID.randomUUID(), currentFileNumber++);
        String filePath = new Path(location, fileName).toString();
        return fileIO.newOutputFile(filePath);
    }

    /**
     * Helper method to create {@link ParquetWriter} for given file path and write support. It makes
     * use of configuration options in `configuration` to configure the writer. Different available
     * configuration options are defined in {@link ParquetOutputFormat}.
     */
    private ParquetWriter<Integer> createWriter(org.apache.parquet.io.OutputFile outputFile,
                                                WriteSupport<Integer> writeSupport) throws IOException {
        ParquetRowDataBuilder rowDataBuilder = new ParquetRowDataBuilder(outputFile, writeSupport);
        Optional<String> compressionOpt = fileIO.getConf(ParquetOutputFormat.COMPRESSION);
        if (compressionOpt.isPresent()) {
            rowDataBuilder.withCompressionCodec(CompressionCodecName.fromConf(compressionOpt.get()));
        } else {
            rowDataBuilder.withCompressionCodec(CompressionCodecName.ZSTD);
        }
        fileIO.getConf(ParquetOutputFormat.BLOCK_SIZE).map(Long::parseLong).ifPresent(rowDataBuilder::withRowGroupSize);

        fileIO.getConf(ParquetOutputFormat.PAGE_SIZE).map(Integer::parseInt).ifPresent(rowDataBuilder::withPageSize);

        fileIO.getConf(ParquetOutputFormat.DICTIONARY_PAGE_SIZE).map(Integer::parseInt)
                .ifPresent(rowDataBuilder::withDictionaryPageSize);

        fileIO.getConf(ParquetOutputFormat.MAX_PADDING_BYTES).map(Integer::parseInt)
                .ifPresent(rowDataBuilder::withMaxPaddingSize);

        fileIO.getConf(ParquetOutputFormat.ENABLE_DICTIONARY).map(Boolean::parseBoolean)
                .ifPresent(rowDataBuilder::withDictionaryEncoding);

        fileIO.getConf(ParquetOutputFormat.VALIDATION).map(Boolean::parseBoolean)
                .ifPresent(rowDataBuilder::withValidation);

        fileIO.getConf(ParquetOutputFormat.WRITER_VERSION).map(WriterVersion::fromString)
                .ifPresent(rowDataBuilder::withWriterVersion);

        return rowDataBuilder.build();
    }

    private static class ParquetRowDataBuilder extends ParquetWriter.Builder<Integer, ParquetRowDataBuilder> {
        private final WriteSupport<Integer> writeSupport;

        protected ParquetRowDataBuilder(org.apache.parquet.io.OutputFile outputFile,
                                        WriteSupport<Integer> writeSupport) {
            super(outputFile);
            this.writeSupport = requireNonNull(writeSupport, "writeSupport is null");
        }

        @Override
        protected ParquetRowDataBuilder self() {
            return this;
        }

        @Override
        protected WriteSupport<Integer> getWriteSupport(Configuration conf) {
            return writeSupport;
        }
    }

    /**
     * Construct the {@link DataFileStatus} for the given file path. It reads the file status and
     * Parquet footer to compute the statistics for the file.
     *
     * <p>Potential improvement in future to directly compute the statistics while writing the file if
     * this becomes a sufficiently large part of the write operation time.
     *
     * @param path       the path of the file
     * @param dataSchema the schema of the data in the file
     * @param numRows    the number of rows in the file. If no column stats are required, this is used to
     *                   construct the {@link DataFileStatistics}. Otherwise, the stats are read from the file.
     * @return the {@link DataFileStatus} for the file
     */
    private DataFileStatus constructDataFileStatus(String path, StructType dataSchema, long numRows) {
        try {
            // Get the FileStatus to figure out the file size and modification time
            FileStatus fileStatus = fileIO.getFileStatus(path);
            String resolvedPath = fileIO.resolvePath(path);

            DataFileStatistics stats;
            if (statsColumns.isEmpty()) {
                stats = new DataFileStatistics(numRows, emptyMap() /* minValues */, emptyMap() /* maxValues */,
                        emptyMap() /* nullCount */, Optional.empty());
            } else {
                stats = readDataFileStatistics(fileIO.newInputFile(resolvedPath, fileStatus.getSize()), dataSchema,
                        statsColumns);
            }

            return new DataFileStatus(resolvedPath, fileStatus.getSize(), fileStatus.getModificationTime(),
                    Optional.ofNullable(stats));
        } catch (IOException ioe) {
            throw new UncheckedIOException("Failed to read the stats for: " + path, ioe);
        }
    }

    public static DataFileStatistics readDataFileStatistics(
            InputFile kernelInputFile, StructType dataSchema, List<Column> statsColumns)
            throws IOException {
        // Read the Parquet footer to compute the statistics
        org.apache.parquet.io.InputFile parquetFile =
                ParquetIOUtils.createParquetInputFile(kernelInputFile);
        ParquetMetadata footer =
                ParquetFileReader.readFooter(parquetFile, ParquetMetadataConverter.NO_FILTER);
        ImmutableMultimap.Builder<Column, ColumnChunkMetaData> metadataForColumn =
                ImmutableMultimap.builder();

        long rowCount = 0;
        for (BlockMetaData blockMetaData : footer.getBlocks()) {
            rowCount += blockMetaData.getRowCount();
            for (ColumnChunkMetaData columnChunkMetaData : blockMetaData.getColumns()) {
                Column column = new Column(columnChunkMetaData.getPath().toArray());
                metadataForColumn.put(column, columnChunkMetaData);
            }
        }

        return constructFileStats(metadataForColumn.build(), dataSchema, statsColumns, rowCount);
    }

    private static DataFileStatistics constructFileStats(
            Multimap<Column, ColumnChunkMetaData> metadataForColumn,
            StructType dataSchema,
            List<Column> statsColumns,
            long rowCount) {
        Map<Column, Optional<Statistics<?>>> statsForColumn =
                metadataForColumn.keySet().stream()
                        .collect(
                                toImmutableMap(identity(), key -> mergeMetadataList(metadataForColumn.get(key))));

        Map<Column, Literal> minValues = new HashMap<>();
        Map<Column, Literal> maxValues = new HashMap<>();
        Map<Column, Long> nullCounts = new HashMap<>();
        for (Column statsColumn : statsColumns) {
            Optional<Statistics<?>> stats = statsForColumn.get(statsColumn);
            DataType columnType = getDataType(dataSchema, statsColumn);
            if (stats == null || !stats.isPresent() || !isStatsSupportedDataType(columnType)) {
                continue;
            }
            Statistics<?> statistics = stats.get();

            Long numNulls = statistics.isNumNullsSet() ? statistics.getNumNulls() : null;
            nullCounts.put(statsColumn, numNulls);

            if (numNulls != null && rowCount == numNulls) {
                // If all values are null, then min and max are also null
                minValues.put(statsColumn, Literal.ofNull(columnType));
                maxValues.put(statsColumn, Literal.ofNull(columnType));
                continue;
            }

            Literal minValue = decodeMinMaxStat(columnType, statistics, true /* decodeMin */);
            minValues.put(statsColumn, minValue);

            Literal maxValue = decodeMinMaxStat(columnType, statistics, false /* decodeMin */);
            maxValues.put(statsColumn, maxValue);
        }

        return new DataFileStatistics(rowCount, minValues, maxValues, nullCounts, Optional.empty());
    }

    private static Literal decodeMinMaxStat(
            DataType dataType, Statistics<?> statistics, boolean decodeMin) {
        Object statValue = decodeMin ? statistics.genericGetMin() : statistics.genericGetMax();
        if (statValue == null) {
            return null;
        }

        if (dataType instanceof BooleanType) {
            return Literal.ofBoolean((Boolean) statValue);
        } else if (dataType instanceof ByteType) {
            return Literal.ofByte(((Number) statValue).byteValue());
        } else if (dataType instanceof ShortType) {
            return Literal.ofShort(((Number) statValue).shortValue());
        } else if (dataType instanceof IntegerType) {
            return Literal.ofInt(((Number) statValue).intValue());
        } else if (dataType instanceof LongType) {
            return Literal.ofLong(((Number) statValue).longValue());
        } else if (dataType instanceof FloatType) {
            return Literal.ofFloat(((Number) statValue).floatValue());
        } else if (dataType instanceof DoubleType) {
            return Literal.ofDouble(((Number) statValue).doubleValue());
        } else if (dataType instanceof DecimalType decimalType) {
            LogicalTypeAnnotation logicalType = statistics.type().getLogicalTypeAnnotation();
            checkArgument(
                    logicalType instanceof LogicalTypeAnnotation.DecimalLogicalTypeAnnotation,
                    "Physical decimal column has invalid Parquet Logical Type: %s",
                    logicalType);
            int scale = ((LogicalTypeAnnotation.DecimalLogicalTypeAnnotation) logicalType).getScale();

            // Check the scale is same in both the Delta data type and the Parquet Logical Type
            checkArgument(
                    scale == decimalType.getScale(),
                    "Physical decimal type has different scale than the logical type: %s",
                    scale);

            // Decimal is stored either as int, long or binary. Decode the stats accordingly.
            BigDecimal decimalStatValue;
            if (statistics instanceof IntStatistics) {
                decimalStatValue = BigDecimal.valueOf((Integer) statValue).movePointLeft(scale);
            } else if (statistics instanceof LongStatistics) {
                decimalStatValue = BigDecimal.valueOf((Long) statValue).movePointLeft(scale);
            } else if (statistics instanceof BinaryStatistics) {
                BigInteger base = new BigInteger(getBinaryStat(statistics, decodeMin));
                decimalStatValue = new BigDecimal(base, scale);
            } else {
                throw new UnsupportedOperationException(
                        "Unsupported stats type for Decimal: " + statistics.getClass());
            }
            return Literal.ofDecimal(
                    decimalStatValue, decimalType.getPrecision(), decimalType.getScale());
        } else if (dataType instanceof DateType) {
            checkArgument(
                    statistics instanceof IntStatistics,
                    "Column with DATE type contained invalid statistics: %s",
                    statistics);
            return Literal.ofDate((Integer) statValue); // stats are stored as epoch days in Parquet
        } else if (dataType instanceof TimestampType) {
            // Kernel Parquet writer always writes timestamps in INT64 format
            checkArgument(
                    statistics instanceof LongStatistics,
                    "Column with TIMESTAMP type contained invalid statistics: %s",
                    statistics);
            return Literal.ofTimestamp((Long) statValue);
        } else if (dataType instanceof TimestampNTZType) {
            checkArgument(
                    statistics instanceof LongStatistics,
                    "Column with TIMESTAMP_NTZ type contained invalid statistics: %s",
                    statistics);
            return Literal.ofTimestampNtz((Long) statValue);
        } else if (dataType instanceof StringType) {
            byte[] binaryStat = getBinaryStat(statistics, decodeMin);
            return Literal.ofString(new String(binaryStat, UTF_8));
        } else if (dataType instanceof BinaryType) {
            return Literal.ofBinary(getBinaryStat(statistics, decodeMin));
        }

        throw new IllegalArgumentException("Unsupported stats data type: " + statValue);
    }

    private static Optional<Statistics<?>> mergeMetadataList(
            Collection<ColumnChunkMetaData> metadataList) {
        if (hasInvalidStatistics(metadataList)) {
            return Optional.empty();
        }

        return metadataList.stream()
                .<Statistics<?>>map(ColumnChunkMetaData::getStatistics)
                .reduce(
                        (statsA, statsB) -> {
                            statsA.mergeStatistics(statsB);
                            return statsA;
                        });
    }

    private static boolean hasInvalidStatistics(Collection<ColumnChunkMetaData> metadataList) {
        // If any row group does not have stats collected, stats for the file will not be valid
        return metadataList.stream()
                .anyMatch(
                        metadata -> {
                            Statistics<?> stats = metadata.getStatistics();
                            if (stats == null || stats.isEmpty() || !stats.isNumNullsSet()) {
                                return true;
                            }

                            // Columns with NaN values are marked by `hasNonNullValue` = false by the Parquet
                            // reader
                            // See issue: https://issues.apache.org/jira/browse/PARQUET-1246
                            return !stats.hasNonNullValue() && stats.getNumNulls() != metadata.getValueCount();
                        });
    }

    private static boolean isStatsSupportedDataType(DataType dataType) {
        return dataType instanceof BooleanType
                || dataType instanceof ByteType
                || dataType instanceof ShortType
                || dataType instanceof IntegerType
                || dataType instanceof LongType
                || dataType instanceof FloatType
                || dataType instanceof DoubleType
                || dataType instanceof DecimalType
                || dataType instanceof DateType
                || dataType instanceof TimestampType
                || dataType instanceof TimestampNTZType
                || dataType instanceof StringType
                || dataType instanceof BinaryType;
    }

    private static byte[] getBinaryStat(Statistics<?> statistics, boolean decodeMin) {
        return decodeMin ? statistics.getMinBytes() : statistics.getMaxBytes();
    }
}
