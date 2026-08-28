/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.lakehouse.v2.io.parquet;

import com.google.common.annotations.VisibleForTesting;
import com.google.protobuf.Message;
import io.lakestream.ursa.lakehouse.LakehouseConfiguration;
import io.lakestream.ursa.metrics.InstrumentProvider;
import java.io.EOFException;
import java.io.IOException;
import java.net.URI;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.HadoopReadOptions;
import org.apache.parquet.avro.AvroReadSupport;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.conf.ParquetConfiguration;
import org.apache.parquet.filter2.compat.FilterCompat;
import org.apache.parquet.hadoop.api.InitContext;
import org.apache.parquet.hadoop.api.ReadSupport;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.io.ColumnIOFactory;
import org.apache.parquet.io.MessageColumnIO;
import org.apache.parquet.io.RecordReader;
import org.apache.parquet.io.api.RecordMaterializer;
import org.apache.parquet.proto.ProtoReadSupport;
import org.apache.parquet.schema.MessageType;

@Slf4j
public class ParquetFileReader<T> implements AutoCloseable {
    private final Path path;
    private final LakehouseConfiguration conf;
    private final Configuration configuration;
    private final IndexFileReader indexFileReader;

    private final org.apache.parquet.hadoop.ParquetFileReader parquetReader;
    private final ReadSupport<T> readSupport;
    private final RecordMaterializer<T> recordConverter;
    private final MessageType schema;
    private final MessageColumnIO columnIO;

    // parquet file metadata index to speed up the reading
    private final List<BlockMetaData> rowGroups;
    // the index to seek the row group according to the row number
    private final NavigableMap<Long, Pair<BlockMetaData, Integer>> parquetRowGroupIndexes;
    long lastRowInFile;

    int currentRowGroupIndex = 0;
    long currentReadRow = 0;
    long lastRowOfCurrentRowGroup = 0;
    private PageReadStore pageReadStore;
    private RecordReader<T> recordReader;

    private final ParquetFileReaderMetrics metrics;

    @VisibleForTesting
    ParquetFileReader(URI file, LakehouseConfiguration conf) throws IOException {
        this(file, conf, InstrumentProvider.NOOP);
    }

    public ParquetFileReader(URI file, LakehouseConfiguration conf, InstrumentProvider provider) throws IOException {
        this.path = new Path(file);
        this.conf = conf;
        this.configuration = conf.getHadoopConfiguration();
        this.indexFileReader = new IndexFileReader(path.getParent().toUri(),
            path.getName().replace(".parquet", ".index"), conf);
        this.parquetReader = org.apache.parquet.hadoop.ParquetFileReader
            .open(HadoopInputFile.fromPath(path, configuration));
        this.rowGroups = Collections.unmodifiableList(parquetReader.getRowGroups());
        this.schema = parquetReader.getFileMetaData().getSchema();
        this.columnIO = new ColumnIOFactory().getColumnIO(schema);

        var extraFileMetadata = parquetReader.getFileMetaData().getKeyValueMetaData();
        this.readSupport = initalizeReadSupport(extraFileMetadata);
        this.recordConverter = initializeRecordConverter(extraFileMetadata);

        this.parquetRowGroupIndexes = buildParquetRowGroupsIndex();
        this.metrics = ParquetFileReaderMetrics.getInstance(provider);
    }

    private ReadSupport<T> initalizeReadSupport(Map<String, String> extraFileMetadata) {
        var schemaType = extraFileMetadata.get("schemaType");
        if (ParquetFileWriter.SchemaType.AVRO.name().toLowerCase(Locale.ROOT).equals(schemaType)) {
            return new AvroReadSupport<T>();
        } else if (ParquetFileWriter.SchemaType.PROTOBUF.name().toLowerCase(Locale.ROOT).equals(schemaType)) {
            return new ProtoReadSupport();
        } else {
            throw new IllegalArgumentException("Unsupported schema type in parquet file");
        }
    }

    private RecordMaterializer<T> initializeRecordConverter(Map<String, String> extraFileMetadata) {
        var readOptions = HadoopReadOptions.builder(configuration, path)
            .withRecordFilter(FilterCompat.NOOP)
            .build();

        ParquetConfiguration conf = Objects.requireNonNull(readOptions).getConfiguration();
        for (String property : readOptions.getPropertyNames()) {
            conf.set(property, readOptions.getProperty(property));
        }

        var readContext = readSupport.init(new InitContext(conf, toSetMultiMap(extraFileMetadata), schema));
        var requestedSchema = readContext.getRequestedSchema();

        parquetReader.setRequestedSchema(requestedSchema);
        return readSupport.prepareForRead(conf, extraFileMetadata, schema, readContext);
    }

    private NavigableMap<Long, Pair<BlockMetaData, Integer>> buildParquetRowGroupsIndex() {
        long rowNumber = 0;
        TreeMap<Long, Pair<BlockMetaData, Integer>> map = new TreeMap<>();
        for (int i = 0; i < rowGroups.size(); i++) {
            var rowGroup = rowGroups.get(i);
            map.put(rowNumber, Pair.of(rowGroup, i));
            rowNumber += rowGroup.getRowCount();
        }
        lastRowInFile = rowNumber;
        return Collections.unmodifiableNavigableMap(map);
    }

    public Map<String, String> getFirstMetadata() throws IOException {
        return indexFileReader.read(0);
    }

    public String getFileExtraMetadata(String key) {
        return parquetReader.getFileMetaData().getKeyValueMetaData().get(key);
    }

    @SuppressWarnings("unchecked")
    public Record<T> read() throws IOException {
        if (recordReader == null || currentReadRow >= lastRowOfCurrentRowGroup) {
            if (!initRecordReader()) {
                throw new EOFException();
            }
            return read();
        }

        long startReadRecord = System.nanoTime();
        Object record = recordReader.read();  // payload --> message
        if (record == null) {
            throw new EOFException();
        }
        if (record instanceof Message.Builder pbRecord) {
            record = pbRecord.build();
        }
        metrics.getReadRecord().recordSuccess(System.nanoTime() - startReadRecord);

        long startReadMetadata = System.nanoTime();
        var metadata = indexFileReader.read(Math.toIntExact(currentReadRow));
        metrics.getReadMetadata().recordSuccess(System.nanoTime() - startReadMetadata);

        currentReadRow++;
        return new Record<T>((T) record, metadata);
    }

    private boolean initRecordReader() throws IOException {
        if (pageReadStore != null) {
            currentRowGroupIndex++;
            pageReadStore.close();
            if (currentRowGroupIndex >= rowGroups.size()) {
                return false;
            }
        }

        lastRowOfCurrentRowGroup = parquetRowGroupIndexes.floorEntry(currentReadRow).getKey()
                                   + rowGroups.get(currentRowGroupIndex).getRowCount();
        pageReadStore = parquetReader.readRowGroup(currentRowGroupIndex);
        recordReader = columnIO.getRecordReader(pageReadStore, recordConverter, FilterCompat.NOOP);
        return true;
    }

    public void seek(int row) throws IOException {
        long startSeek = System.nanoTime();
        try {
            seek0(row);
            metrics.getSeekByOffset().recordSuccess(System.nanoTime() - startSeek);
        } catch (IOException e) {
            metrics.getSeekByOffset().recordFailure(System.nanoTime() - startSeek);
            throw e;
        }
    }

    private void seek0(int row) throws IOException {
        if (row < 0 || row >= lastRowInFile) {
            throw new IOException(String.format("Row %d is out of bounds", row));
        }

        if (row == currentReadRow) {
            return;
        }

        long rowsToSkip;
        if (row > currentReadRow && row < lastRowOfCurrentRowGroup) {
            rowsToSkip = row - currentReadRow;
            currentReadRow = row;
        } else {
            // Set the target row
            currentReadRow = row;

            // Find the row group containing the target row
            var mapEntry = parquetRowGroupIndexes.floorEntry(currentReadRow);
            if (mapEntry == null) {
                throw new IOException("Invalid position to seek: row " + row + " is out of bounds");
            }

            var rowGroup = mapEntry.getValue();
            long rowGroupStartRow = mapEntry.getKey();

            // Close the current page store if it exists
            if (pageReadStore != null) {
                pageReadStore.close();
                pageReadStore = null;
            }

            // Set the current row group index
            currentRowGroupIndex = rowGroup.getRight();

            // Initialize the record reader for the new row group
            initRecordReader();

            // Calculate how many rows to skip within this row group
            rowsToSkip = currentReadRow - rowGroupStartRow;
        }

        // Skip rows within the row group to reach the exact position
        for (long i = 0; i < rowsToSkip; i++) {
            recordReader.read();
        }
    }

    public int seekBySecondaryIndex(String secondaryIndexValue) throws IOException {
        long startSeek = System.nanoTime();
        var row = indexFileReader.seekBySecondaryIndex(secondaryIndexValue);
        metrics.getSeekBySecondaryIndex().recordSuccess(System.nanoTime() - startSeek);
        seek(row);
        return row;
    }

    @Override
    public void close() throws IOException {
        if (pageReadStore != null) {
            pageReadStore.close();
            pageReadStore = null;
            recordReader = null;
        }
        if (parquetReader != null) {
            parquetReader.close();
        }
        if (indexFileReader != null) {
            indexFileReader.close();
        }

        currentRowGroupIndex = 0;
        currentReadRow = 0;
        lastRowOfCurrentRowGroup = 0;
    }

    private static <K, V> Map<K, Set<V>> toSetMultiMap(Map<K, V> map) {
        Map<K, Set<V>> setMultiMap = new HashMap<>();
        for (Map.Entry<K, V> entry : map.entrySet()) {
            setMultiMap.put(entry.getKey(), Collections.singleton(entry.getValue()));
        }
        return Collections.unmodifiableMap(setMultiMap);
    }

    public static boolean hasNecessaryValuesInMetadata(URI file, Configuration configuration) throws IOException {
        var parquetReader = org.apache.parquet.hadoop.ParquetFileReader
            .open(HadoopInputFile.fromPath(new Path(file), configuration));
        var extraMetadata =  parquetReader.getFileMetaData().getKeyValueMetaData();
        return extraMetadata.containsKey("schemaType");
    }

    @Getter
    @AllArgsConstructor
    public static class Record<T> {
        private T record;
        private Map<String, String> metadata;
    }
}
