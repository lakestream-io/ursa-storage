/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.kafka.reader;

import io.lakestream.api.EntryHeader;
import io.lakestream.api.EntryIndex;
import io.lakestream.ursa.compaction.common.ManagedTableFileIndex;
import io.lakestream.ursa.lakestream.reader.CompactedObjectReader;
import io.lakestream.ursa.storage.Entry;
import io.lakestream.ursa.storage.OwnedResultFutures;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.io.EOFException;
import java.io.IOException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.util.HadoopInputFile;

final class KafkaLakehouseReader implements CompactedObjectReader {

    private static final String SERDE_TYPE_KEY = "entrySerDeType";
    private static final String SUPPORTED_SERDE_TYPE = "KAFKA_BATCHED_RAW_PARQUET";
    private static final String PAYLOAD_FIELD = "payload";
    private static final String ENTRY_METADATA_KEY = "metadata";

    private final String logName;
    private final ReaderConfiguration configuration;
    private final Executor executor;
    private final AtomicBoolean closed = new AtomicBoolean();

    KafkaLakehouseReader(String logName, ReaderConfiguration configuration, Executor executor) {
        this.logName = TopicPaths.canonicalLogName(logName);
        this.configuration = configuration;
        this.executor = executor;
    }

    @Override
    public Optional<ManagedTableFileIndex> getManagedTableFileIndex(EntryIndex entryIndex) {
        Map<String, String> metadata = entryIndex.extraData().orElse(new HashMap<>());
        String serialized = metadata.get(ManagedTableFileIndex.NAME);
        return serialized == null
                ? Optional.empty()
                : Optional.of(ManagedTableFileIndex.deserializeFromString(serialized));
    }

    @Override
    public CompletableFuture<ReadResult> readMessagesWithEntryIndexAsync(
            EntryIndex entryIndex, long startOffset, long baseOffset, long maxNumOfMessages, long maxSize) {
        Optional<ManagedTableFileIndex> fileIndex;
        try {
            fileIndex = getManagedTableFileIndex(entryIndex);
        } catch (RuntimeException error) {
            return CompletableFuture.failedFuture(error);
        }
        if (fileIndex.isEmpty()) {
            return unsupportedV1();
        }
        try {
            String filePath = fileIndex.get().get(startOffset);
            long fileBaseOffset = fileIndex.get().getFileBaseOffset(startOffset).orElse(baseOffset);
            return readV2(filePath, startOffset, fileBaseOffset, maxNumOfMessages, maxSize);
        } catch (RuntimeException error) {
            return CompletableFuture.failedFuture(error);
        }
    }

    @Override
    public CompletableFuture<ReadResult> readMessagesAsync(
            String path, long startOffset, long baseOffset, long maxNumOfMessages, long maxSize) {
        return unsupportedV1();
    }

    private CompletableFuture<ReadResult> readV2(
            String path, long startOffset, long baseOffset, long maxNumOfMessages, long maxSize) {
        if (closed.get()) {
            return CompletableFuture.failedFuture(new IllegalStateException("Kafka lakehouse reader is closed"));
        }
        if (startOffset < baseOffset) {
            return CompletableFuture.failedFuture(new IllegalArgumentException("startOffset is before file base"));
        }
        final int messageLimit;
        try {
            messageLimit = Math.toIntExact(maxNumOfMessages);
        } catch (RuntimeException error) {
            return CompletableFuture.failedFuture(error);
        }
        CompletableFuture<ReadResult> read;
        try {
            read = CompletableFuture.supplyAsync(
                    () -> readV2Sync(path, startOffset, messageLimit, maxSize), executor);
        } catch (RuntimeException submissionFailure) {
            return CompletableFuture.failedFuture(submissionFailure);
        }
        return OwnedResultFutures.transfer(read, result -> {
            if (result != null) {
                OwnedResultFutures.closeLogEntries(result.entries());
            }
        });
    }

    private ReadResult readV2Sync(String path, long startOffset, int maxNumOfMessages, long maxSize) {
        List<Entry> entries = new ArrayList<>();
        try {
            URI parquetFile = URI.create(TopicPaths.storagePath(configuration.storagePath(), logName) + "/" + path);
            Path hadoopPath = new Path(parquetFile);
            HadoopInputFile inputFile = HadoopInputFile.fromPath(hadoopPath, configuration.hadoopConfiguration());
            validateFormat(inputFile);
            try (IndexFileReader indexReader = new IndexFileReader(parquetFile, configuration);
                 org.apache.parquet.hadoop.ParquetReader<GenericRecord> parquetReader =
                         AvroParquetReader.<GenericRecord>builder(inputFile).build()) {
                int row = indexReader.seekBySecondaryIndex(String.valueOf(startOffset));
                skipRows(parquetReader, row);
                long bytes = 0;
                int messages = 0;
                while (messages < maxNumOfMessages && bytes < maxSize) {
                    GenericRecord record = parquetReader.read();
                    if (record == null) {
                        break;
                    }
                    Map<String, String> metadata = indexReader.read(row++);
                    String encodedEntryMetadata = metadata.get(ENTRY_METADATA_KEY);
                    if (encodedEntryMetadata == null) {
                        throw new IOException("Kafka lakehouse row is missing entry metadata");
                    }
                    EntryHeader header = EntryMetadataParser.parse(encodedEntryMetadata);
                    ByteBuf payload = copyPayload(record.get(PAYLOAD_FIELD));
                    entries.add(new Entry(header, payload));
                    messages += header.numberOfMessages();
                    bytes += payload.readableBytes();
                }
            }
        } catch (Throwable error) {
            release(entries, error);
            throw new KafkaLakehouseReadException("Failed to read Kafka V2 lakehouse file", error);
        }
        return transferEntries(entries);
    }

    /**
     * Transfers the storage entries to the public result after file reading has succeeded.
     *
     * <p>{@link Entry#toLogEntries(List)} owns cleanup when conversion fails, so this operation is
     * deliberately outside the file-read cleanup block in {@link #readV2Sync}. Releasing
     * {@code entries} again here would double-release entries converted before the failure.
     */
    static ReadResult transferEntries(List<Entry> entries) {
        try {
            return new ReadResult(true, Entry.toLogEntries(entries));
        } catch (Throwable error) {
            throw new KafkaLakehouseReadException("Failed to convert Kafka V2 lakehouse entries", error);
        }
    }

    private void validateFormat(HadoopInputFile inputFile) throws IOException {
        try (ParquetFileReader reader = ParquetFileReader.open(inputFile)) {
            String serdeType = reader.getFileMetaData().getKeyValueMetaData().get(SERDE_TYPE_KEY);
            if (!SUPPORTED_SERDE_TYPE.equalsIgnoreCase(serdeType)) {
                throw new IOException("Unsupported Kafka lakehouse serde type: " + serdeType
                        + "; only " + SUPPORTED_SERDE_TYPE + " is supported");
            }
        }
    }

    private static void skipRows(org.apache.parquet.hadoop.ParquetReader<GenericRecord> reader, int rows)
            throws IOException {
        for (int i = 0; i < rows; i++) {
            if (reader.read() == null) {
                throw new EOFException("Kafka lakehouse secondary index points past end of parquet file");
            }
        }
    }

    private static ByteBuf copyPayload(Object payload) throws IOException {
        if (payload instanceof ByteBuffer buffer) {
            return Unpooled.copiedBuffer(buffer.duplicate());
        }
        if (payload instanceof byte[] bytes) {
            return Unpooled.copiedBuffer(bytes);
        }
        throw new IOException("Kafka lakehouse payload is not binary");
    }

    private static void release(List<Entry> entries, Throwable error) {
        for (Entry entry : entries) {
            try {
                entry.payload().release();
            } catch (RuntimeException | Error cleanupFailure) {
                error.addSuppressed(cleanupFailure);
            }
        }
    }

    private static CompletableFuture<ReadResult> unsupportedV1() {
        return CompletableFuture.failedFuture(new UnsupportedOperationException(
                "Kafka-only lakehouse reader currently supports V2 KAFKA_BATCHED_RAW_PARQUET files only; "
                        + "V1 compacted parquet requires the legacy ursa-storage-lakehouse runtime"));
    }

    @Override
    public boolean hasSpaceInCache() {
        return false;
    }

    @Override
    public CompletableFuture<Entry> preFetchMessagesAsync(
            String path, long startOffset, long baseOffset, long maxNumOfMessages, long maxSize, long estimatedSize) {
        return CompletableFuture.failedFuture(new UnsupportedOperationException(
                "Kafka-only lakehouse reader does not provide a prefetch cache"));
    }

    @Override
    public void close() {
        closed.set(true);
    }

    private static final class KafkaLakehouseReadException extends RuntimeException {
        private KafkaLakehouseReadException(String message, Throwable cause) {
            super(message, cause);
        }
    }
}
