/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.lakehouse.v2;

import static io.lakestream.ursa.lakehouse.v2.LakehouseReaderMetrics.READER_SERDE_TYPE;

import com.google.common.annotations.VisibleForTesting;
import io.lakestream.ursa.exception.ExceptionCode;
import io.lakestream.ursa.exception.ExceptionWithCode;
import io.lakestream.ursa.exception.LakehouseOptException;
import io.lakestream.ursa.exception.MessageSerDeException;
import io.lakestream.ursa.lakehouse.LakehouseConfiguration;
import io.lakestream.ursa.lakehouse.utils.TopicNames;
import io.lakestream.ursa.lakehouse.v2.io.parquet.ParquetFileReader;
import io.lakestream.ursa.materialization.serde.EntryDecoder;
import io.lakestream.ursa.materialization.serde.EntrySerdeFactory;
import io.lakestream.ursa.materialization.serde.GenericEntry;
import io.lakestream.ursa.materialization.serde.LakehouseEntryMetadata;
import io.lakestream.ursa.materialization.serde.MaterializationRecord;
import io.lakestream.ursa.materialization.serde.ResultConsumer;
import io.lakestream.ursa.materialization.serde.exception.FatalException;
import io.lakestream.ursa.metrics.InstrumentProvider;
import io.opentelemetry.api.common.Attributes;
import java.io.EOFException;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class LakehouseReader implements AutoCloseable {

    private static final String SERDE_TYPE = "entrySerDeType";

    // Shared thread pool for all LakehouseReader instances.
    private static final ScheduledExecutorService IDLE_TIMEOUT_SCHEDULER =
        Executors.newScheduledThreadPool(1, r -> {
            Thread t = new Thread(r, "LakehouseReader-IdleTimeout");
            t.setDaemon(true);
            return t;
        });
    // Default idle timeout in seconds (1 minutes)
    private static final long DEFAULT_IDLE_TIMEOUT_SECONDS = 60;
    public static final String READER_IDLE_TIMEOUT_SECONDS = "lakehouseReaderIdleTimeoutSeconds";
    private final InstrumentProvider provider;
    private final LakehouseReaderMetrics metrics;
    private final Attributes attributes = Attributes.empty();
    private final String topic;
    private final EntrySerdeFactory serdeFactory;
    private volatile String currentReadPath;
    private volatile ParquetFileReader<Object> reader;
    private final LakehouseConfiguration configuration;
    private volatile ScheduledFuture<?> idleTimeoutFuture;
    private final long idleTimeoutSeconds;

    @VisibleForTesting
    public LakehouseReader(String topic, EntrySerdeFactory entrySerdeFactory,
                                 LakehouseConfiguration configuration) {
        this(topic, entrySerdeFactory, configuration, InstrumentProvider.NOOP);
    }

    public LakehouseReader(String topic, EntrySerdeFactory entrySerdeFactory,
                                 LakehouseConfiguration configuration, InstrumentProvider provider) {
        this.provider = provider;
        this.topic = topic;
        this.serdeFactory = entrySerdeFactory;
        this.configuration = configuration;
        this.metrics = LakehouseReaderMetrics.getInstance(provider);
        // Get idle timeout from configuration, use default if not set
        this.idleTimeoutSeconds = configuration.getProperties().containsKey(READER_IDLE_TIMEOUT_SECONDS)
            ? Long.parseLong(configuration.getProperties().getProperty(READER_IDLE_TIMEOUT_SECONDS))
            : DEFAULT_IDLE_TIMEOUT_SECONDS;
    }

    public synchronized List<GenericEntry> readWithMessageId(String path, Object startMessageId,
                                                             int toRead) throws ExceptionWithCode {
        try {
            var reader = getReader(path);
            long start = System.nanoTime();
            reader.seekBySecondaryIndex(startMessageId.toString());
            metrics.getSeek().withAttributes(attributes)
                .recordSuccess(System.nanoTime() - start);
            return doRead(reader, toRead);
        } catch (Throwable t) {
            if (t instanceof MessageSerDeException mse) {
                throw mse;
            }
            throw new LakehouseOptException(ExceptionCode.LAKEHOUSE_READ_ERROR, t);
        }
    }

    public CompletableFuture<List<GenericEntry>> readAsync(String path, long startOffset, long firstOffsetInFile,
                                                           int toRead, long maxSizeBytes) {
        CompletableFuture<List<GenericEntry>> future = new CompletableFuture<>();
        CompletableFuture.runAsync(() -> {
            try {
                future.complete(read(path, startOffset, firstOffsetInFile, toRead, maxSizeBytes));
            } catch (Throwable e) {
                future.completeExceptionally(e);
            }
        });
        return future;
    }

    // todo: support size limitation
    public synchronized List<GenericEntry> read(String path, long startOffset, long firstOffsetInFile,
                                   int toRead, long maxSizeBytes) throws ExceptionWithCode {
        try {
            if (startOffset < firstOffsetInFile) {
                throw new IllegalArgumentException("startOffset < firstOffsetInFile");
            }
            var reader = getReader(path);
            long start = System.nanoTime();
            var serdeType = reader.getFileExtraMetadata(SERDE_TYPE);
            if (EntrySerdeFactory.SerdeType.KAFKA_BATCHED_RAW_PARQUET.name().equalsIgnoreCase(serdeType)) {
                reader.seekBySecondaryIndex(String.valueOf(startOffset));
            } else {
                reader.seek(Math.toIntExact(startOffset - firstOffsetInFile));
            }
            metrics.getSeek().withAttributes(attributes)
                .recordSuccess(System.nanoTime() - start);
            return doRead(reader, toRead);
        } catch (Throwable t) {
            if (t instanceof MessageSerDeException mse) {
                throw mse;
            }
            throw new LakehouseOptException(ExceptionCode.LAKEHOUSE_READ_ERROR, t);
        }
    }

    // todo: support size limitation
    private List<GenericEntry> doRead(ParquetFileReader reader, int toRead) throws ExceptionWithCode {
        long readStart = System.nanoTime();

        var readNumber = new AtomicInteger(toRead);
        var firstException = new AtomicReference<Throwable>(null);
        List<GenericEntry> entries = new ArrayList<>(toRead);
        final AtomicBoolean hasNext = new AtomicBoolean(true);

        var serdeType = reader.getFileExtraMetadata(SERDE_TYPE);
        EntryDecoder<Object> decoder = serdeFactory.getDecoder(EntrySerdeFactory.SerdeType.valueOf(serdeType));
        var finalAttributes = attributes.toBuilder().put(READER_SERDE_TYPE, serdeType).build();

        AtomicLong decodeStart = new AtomicLong();
        decoder.decode(topic, new Iterator<MaterializationRecord<Object>>() {
            @Override
            public boolean hasNext() {
                return readNumber.get() > 0 && hasNext.get();
            }

            @Override
            public MaterializationRecord<Object> next() {
                if (!hasNext()) {
                    throw new NoSuchElementException();
                }
                try {
                    long readRecordStart = System.nanoTime();
                    var record = reader.read();
                    metrics.getReadRecord().withAttributes(finalAttributes)
                        .recordSuccess(System.nanoTime() - readRecordStart);

                    var data = record.getRecord();
                    var metadata = record.getMetadata();
                    Optional<LakehouseEntryMetadata> lakehouseEntryMetadata = Optional.empty();
                    if (metadata.get("metadata") != null) {
                        String entryMetadata = metadata.get("metadata").toString();
                        lakehouseEntryMetadata = Optional.of(LakehouseEntryMetadata.of(entryMetadata));
                    }

                    decodeStart.set(System.nanoTime());
                    return new MaterializationRecord<>(data, lakehouseEntryMetadata);
                } catch (IOException e) {
                    if (e instanceof EOFException) {
                        hasNext.set(false);
                        return null;
                    }
                    throw new FatalException(new LakehouseOptException(ExceptionCode.LAKEHOUSE_READ_ERROR,
                        "Failed to read record from file", e));
                }
            }
        }, new ResultConsumer<GenericEntry>() {
            @Override
            public void onResult(GenericEntry genericEntry) {
                metrics.getDecode().withAttributes(finalAttributes)
                    .recordSuccess(System.nanoTime() - decodeStart.get());
                if (entries.size() != toRead) {
                    entries.add(genericEntry);
                } else {
                    genericEntry.entry().payload().release();
                }
                readNumber.decrementAndGet();
            }

            @Override
            public void onErrorWithCtx(Object ctx, Throwable throwable) {
                metrics.getDecode().withAttributes(finalAttributes)
                    .recordFailure(System.nanoTime() - decodeStart.get());
                if (firstException.get() == null) {
                    firstException.set(throwable);
                    hasNext.set(false);
                    for (GenericEntry entry : entries) {
                        entry.entry().payload().release();
                    }
                    entries.clear();
                }
            }
        });

        if (firstException.get() != null) {
            metrics.getReadAll().withAttributes(finalAttributes)
                .recordFailure(System.nanoTime() - readStart);
            if (firstException.get() instanceof ExceptionWithCode ewc) {
                throw ewc;
            } else {
                throw new LakehouseOptException(ExceptionCode.LAKEHOUSE_READ_ERROR, firstException.get());
            }
        }

        metrics.getReadAll().withAttributes(finalAttributes)
            .recordSuccess(System.nanoTime() - readStart);
        return entries;
    }

    // todo: now we only keep one reader here. If a topic reading in the different paths, it will impact each other
    //       like the open source offloader one. consider using a more smarter cache to allow multiple readers
    protected synchronized ParquetFileReader<Object> getReader(String path) throws IOException {
        if (path.equals(currentReadPath)) {
            resetIdleTimeout();
            return reader;
        }
        if (reader != null) {
            reader.close();
            reader = null;
        }

        currentReadPath = path;
        reader = new ParquetFileReader<>(URI.create(
            TopicNames.storagePath(configuration.getStoragePath(), topic) + "/" + path), configuration, provider);
        scheduleIdleTimeout();
        return reader;
    }

    public boolean isAbleToRead(String path) throws IOException {
        var uri = URI.create(TopicNames.storagePath(configuration.getStoragePath(), topic) + "/" + path);
        return ParquetFileReader.hasNecessaryValuesInMetadata(uri, configuration.getHadoopConfiguration());
    }

    private void scheduleIdleTimeout() {
        if (idleTimeoutSeconds <= 0) {
            return; // Idle timeout disabled
        }

        cancelIdleTimeout();

        idleTimeoutFuture = IDLE_TIMEOUT_SCHEDULER.schedule(() -> {
            try {
                synchronized (this) {
                    if (reader != null) {
                        log.info("Closing idle ParquetFileReader for topic: {} after {} seconds",
                            topic, idleTimeoutSeconds);
                        reader.close();
                        reader = null;
                        currentReadPath = null;
                    }
                }
            } catch (IOException e) {
                log.error("Error closing idle reader for topic: {}", topic, e);
            }
        }, idleTimeoutSeconds, TimeUnit.SECONDS);
    }

    private void resetIdleTimeout() {
        if (idleTimeoutSeconds > 0) {
            scheduleIdleTimeout();
        }
    }

    private void cancelIdleTimeout() {
        if (idleTimeoutFuture != null) {
            idleTimeoutFuture.cancel(false);
            idleTimeoutFuture = null;
        }
    }

    @VisibleForTesting
    void setReader(String path, ParquetFileReader<Object> reader) {
        this.reader = reader;
        currentReadPath = path;
        scheduleIdleTimeout();
    }

    public synchronized void close() throws IOException {
        cancelIdleTimeout();
        if (reader != null) {
            reader.close();
            reader = null;
            currentReadPath = null;
        }
    }
}
