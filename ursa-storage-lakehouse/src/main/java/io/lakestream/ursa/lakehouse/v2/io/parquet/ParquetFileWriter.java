/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.lakehouse.v2.io.parquet;

import com.google.common.annotations.VisibleForTesting;
import com.google.protobuf.Message;
import io.lakestream.ursa.lakehouse.LakehouseConfiguration;
import io.lakestream.ursa.lakehouse.v2.IWriteResult;
import io.lakestream.ursa.metrics.InstrumentProvider;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.UUID;
import lombok.Getter;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.proto.ProtoParquetWriter;

public class ParquetFileWriter<T> {
    @Getter
    private ParquetWriter<Object> parquetWriter;
    private Object currentSchema; // Can be Schema or Class<? extends Message>
    private String currentFile;
    private long numberOfRecordsInCurrentFile = 0;
    private IndexFileWriter indexFileWriter;
    private final List<ParquetWriteResult> writeResults;
    private final LakehouseConfiguration conf;
    private final Configuration configuration;
    private final URI directory;
    private String fileName;
    @Getter
    private final Map<String, Object> extraMetadata = new HashMap<>();
    private final Map<String, String> fileExtraMetadata = new HashMap<>();
    private final ParquetFileWriterMetrics metrics;
    private final ParquetConfig parquetConfig;

    @VisibleForTesting
    ParquetFileWriter(URI directory, LakehouseConfiguration conf) {
        this(directory, conf, InstrumentProvider.NOOP);
    }

    public ParquetFileWriter(URI directory, LakehouseConfiguration conf, InstrumentProvider provider) {
        this.parquetWriter = null;
        this.currentFile = null;
        this.writeResults = new ArrayList<>();
        this.conf = conf;
        this.configuration = conf.getHadoopConfiguration();
        this.directory = Utils.ensureIsDirectory(directory);
        this.metrics = ParquetFileWriterMetrics.getInstance(provider);
        this.parquetConfig = new ParquetConfig(conf);
    }

    // The secondary index key must in the metadata map and unique for each record.
    public void setSecondaryIndexKey(String secondaryIndexKey) {
        if (indexFileWriter == null) {
            initializeIndexFileWriter();
        }
        indexFileWriter.setSecondaryIndexKey(secondaryIndexKey);
    }

    // this is allow to add extra metadata to the file, which will be written into the parquet file footer.
    // you need to set this before writing any records.
    public void addExtraMetadataAtFile(String key, String value) {
        fileExtraMetadata.put(key, value);
    }

    public void write(T t, Map<String, String> metadata) throws IOException {
        if (t instanceof GenericRecord genericRecord) {
            writeWithSchema(genericRecord, genericRecord.getSchema(), metadata, SchemaType.AVRO);
        } else if (t instanceof Message message) {
            writeWithSchema(message, message.getClass(), metadata, SchemaType.PROTOBUF);
        } else {
            throw new IllegalArgumentException("Unsupported type: " + t.getClass().getName());
        }
    }

    private <R> void writeWithSchema(Object record, R schema, Map<String, String> metadata, SchemaType type)
        throws IOException {
        if (currentSchema == null || !currentSchema.equals(schema)) {
            closeCurrentWriter();
            initializeWriter(schema, type);
        }
        long startWriteRecord = System.nanoTime();
        parquetWriter.write(record);
        metrics.getWriteRecord().recordSuccess(System.nanoTime() - startWriteRecord);

        long startWriteMetadata = System.nanoTime();
        indexFileWriter.write(metadata);
        metrics.getWriteMetadata().recordSuccess(System.nanoTime() - startWriteMetadata);

        numberOfRecordsInCurrentFile++;
    }

    private <R> void initializeWriter(R schema, SchemaType type) throws IOException {
        if (parquetWriter == null) {
            this.currentSchema = schema;

            this.currentFile = getDataFileName();
            var indexFileName = getIndexFileName();
            initializeIndexFileWriter();
            var extraMetadata = new HashMap<String, String>();
            extraMetadata.put("rowMetadata", indexFileName);
            extraMetadata.put("schemaType", type.name().toLowerCase(Locale.ROOT));
            extraMetadata.putAll(fileExtraMetadata);

            Path path = new Path(directory.resolve(currentFile));
            this.parquetWriter = createParquetWriter(path, schema, type, extraMetadata);
            numberOfRecordsInCurrentFile = 0;
        }
    }

    private void initializeIndexFileWriter() {
        if (indexFileWriter == null) {
            var fileName = getFileName();
            var indexFileName = fileName + ".index";
            this.indexFileWriter = new IndexFileWriter(directory, indexFileName, conf);
        }
    }

    private ParquetWriter<Object> createParquetWriter(Path path, Object schema, SchemaType type,
                                                      Map<String, String> extraMetadata) throws IOException {
        return switch (type) {
            case AVRO -> AvroParquetWriter.builder(path)
                .withMinRowCountForPageSizeCheck(parquetConfig.getMinRowCountForPageSizeCheck())
                .withMaxRowCountForPageSizeCheck(parquetConfig.getMaxRowCountForPageSizeCheck())
                .withRowGroupSize(parquetConfig.getRowGroupSize())
                .withRowGroupRowCountLimit(parquetConfig.getRowGroupRowCountLimit())
                .withPageSize(parquetConfig.getPageSize())
                .withPageRowCountLimit(parquetConfig.getPageRowCountLimit())
                .withSchema((Schema) schema)
                .withConf(configuration)
                .withExtraMetaData(extraMetadata)
                .build();
            case PROTOBUF -> ProtoParquetWriter.builder(path)
                .withMinRowCountForPageSizeCheck(parquetConfig.getMinRowCountForPageSizeCheck())
                .withMaxRowCountForPageSizeCheck(parquetConfig.getMaxRowCountForPageSizeCheck())
                .withRowGroupSize(parquetConfig.getRowGroupSize())
                .withRowGroupRowCountLimit(parquetConfig.getRowGroupRowCountLimit())
                .withPageSize(parquetConfig.getPageSize())
                .withPageRowCountLimit(parquetConfig.getPageRowCountLimit())
                .withMessage((Class<? extends Message>) schema)
                .withConf(configuration)
                .withExtraMetaData(extraMetadata)
                .build();
        };
    }

    private String getFileName() {
        if (fileName == null) {
            fileName = "ursa-" + UUID.randomUUID();
        }
        return fileName;
    }

    private String getDataFileName() {
        return getFileName() + ".parquet";
    }

    private String getIndexFileName() {
        return getFileName() + ".index";
    }

    private void closeCurrentWriter() throws IOException {
        if (parquetWriter != null) {
            indexFileWriter.close();
            parquetWriter.close();
            writeResults.add(new ParquetWriteResult(this.directory, this.currentFile, getCurrentFileSize(),
                numberOfRecordsInCurrentFile, indexFileWriter.getIndexFileName(), parquetWriter.getFooter(),
                Map.copyOf(Collections.unmodifiableMap(this.extraMetadata))));
            extraMetadata.clear();
            this.fileName = null;
            this.parquetWriter = null;
            this.indexFileWriter = null;
        }
    }

    private long getCurrentFileSize() throws IOException {
        return HadoopInputFile.fromPath(new Path(directory.resolve(currentFile)), configuration).getLength();
    }

    public List<IWriteResult> close() throws IOException {
        closeCurrentWriter();
        return Collections.unmodifiableList(writeResults);
    }

    enum SchemaType {
        AVRO, PROTOBUF

    }
}
