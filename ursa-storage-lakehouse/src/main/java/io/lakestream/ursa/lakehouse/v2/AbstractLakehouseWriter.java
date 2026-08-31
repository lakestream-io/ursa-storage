/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.lakehouse.v2;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.lakestream.api.EntryHeader;
import io.lakestream.ursa.exception.ExceptionCode;
import io.lakestream.ursa.exception.ExceptionWithCode;
import io.lakestream.ursa.exception.LakehouseOptException;
import io.lakestream.ursa.exception.MessageSerDeException;
import io.lakestream.ursa.lakehouse.LakehouseConfiguration;
import io.lakestream.ursa.lakehouse.compact.FailureMessage;
import io.lakestream.ursa.lakehouse.compact.FailureMessageHandler;
import io.lakestream.ursa.lakehouse.iceberg.IcebergTable;
import io.lakestream.ursa.lakehouse.utils.TopicNames;
import io.lakestream.ursa.lakehouse.v2.io.parquet.ParquetFileWriter;
import io.lakestream.ursa.materialization.serde.EntryEncoder;
import io.lakestream.ursa.materialization.serde.EntryEncoderContext;
import io.lakestream.ursa.materialization.serde.EntrySerdeFactory;
import io.lakestream.ursa.materialization.serde.GenericEntry;
import io.lakestream.ursa.materialization.serde.MaterializationRecord;
import io.lakestream.ursa.materialization.serde.MissingSchemaVersionTracker;
import io.lakestream.ursa.materialization.serde.ResultConsumer;
import io.lakestream.ursa.materialization.serde.TableSchemaService;
import io.lakestream.ursa.materialization.serde.exception.FatalException;
import io.lakestream.ursa.metrics.InstrumentProvider;
import io.lakestream.ursa.storage.Entry;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.opentelemetry.api.common.Attributes;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public abstract class AbstractLakehouseWriter implements LakehouseRecordWriter<GenericEntry> {

    static final String SERDETYPE = "entrySerDeType";
    protected EntrySerdeFactory.SerdeType serializeType;

    protected final LakehouseWriterMetrics metrics;
    protected Attributes attributes;

    public static final String BATCH_MESSAGE_COUNT = "batchedMessages";
    public static final String LAST_ENTRY_ID_IN_FILE = "lastEntryIdInFile";
    public static final String LAST_BATCH_ID_IN_FILE = "lastBatchIdInFile";

    protected final String topic;
    /** Topic used only for source-schema lookup; destination identity remains {@link #topic}. */
    protected final String schemaTopic;
    protected final LakehouseConfiguration configuration;
    protected EntryEncoder<Object> encoder;
    protected ParquetFileWriter<Object> writer;
    protected EntrySerdeFactory entrySerdeFactory;
    private FailureMessageHandler failureMessageHandler;
    protected final int partition;
    private final ObjectMapper objectMapper = new ObjectMapper();
    private final MissingSchemaVersionTracker missingSchemaVersionTracker = new MissingSchemaVersionTracker();
    private final Optional<Long> baseSchemaVersion;
    // Unity Catalog rejects Iceberg UUID types; UnityCatalogSchemaToType rewrites them to STRING.
    // Computed once: catalog backend doesn't change for the lifetime of a writer.
    private final boolean isUnityCatalog;

    protected AbstractLakehouseWriter(String topic, EntrySerdeFactory entrySerdeFactory,
            LakehouseConfiguration configuration, InstrumentProvider provider) {
        this(topic, topic, entrySerdeFactory, configuration, provider);
    }

    protected AbstractLakehouseWriter(String topic, String schemaTopic,
            EntrySerdeFactory entrySerdeFactory,
            LakehouseConfiguration configuration, InstrumentProvider provider) {
        this.topic = topic;
        this.schemaTopic = schemaTopic;
        this.partition = TopicNames.partitionIndex(topic);
        this.configuration = configuration;
        this.entrySerdeFactory = entrySerdeFactory;
        this.metrics = LakehouseWriterMetrics.getInstance(provider);
        this.attributes = Attributes.empty();
        this.baseSchemaVersion = configuration.getBaseSchemaVersion();
        // Production always returns non-null, but mocks in tests may return null; String.valueOf
        // turns a null into "null" which never matches "UNITYCATALOG", avoiding the NPE without a
        // redundant null check.
        this.isUnityCatalog = IcebergTable.ICEBERG_CATALOG_TYPE_UNITYCATALOG.equalsIgnoreCase(
            String.valueOf(configuration.getIcebergCatalogBackendType(configuration.getCatalogName())));
    }

    public void write(GenericEntry genericEntry) throws ExceptionWithCode {
        long writeStartTime = System.nanoTime();
        AtomicBoolean writeNumberOfMessagesForThisEntry = new AtomicBoolean(false);
        List<CompletableFuture<Void>> failureMessagesFuture = new ArrayList<>();
        EntryEncoderContext context;
        EntryEncoder<Object> currentEncoder;
        TableSchemaService tableSchemaService;
        try {
            EntryEncoderContext.EntryEncoderContextBuilder contextBuilder = EntryEncoderContext.builder()
                    .isVariantEnabled(configuration.isVariantTypeEnabled() || configuration.isAllowIcebergV3())
                    .isPersistExtraMetadata(configuration.isPersistExtraMetadata()
                            && configuration.isSchemaEvolutionEnabled())
                    .isPersistKey(configuration.isPersistKey() && configuration.isSchemaEvolutionEnabled())
                    .isUnityCatalog(isUnityCatalog)
                    .missingSchemaVersionTracker(missingSchemaVersionTracker)
                    .baseSchemaVersion(baseSchemaVersion);

            genericEntry.metadata().ifPresent(metadata -> {
                contextBuilder.schemaVersion(metadata.getSchemaVersion());
                if (metadata.getEntryHeader() != null) {
                    contextBuilder.publishTime(metadata.getEntryHeader().writtenTimestamp());
                    contextBuilder.messageOffset(String.valueOf(metadata.getEntryHeader().offset()));
                }
            });

            context = contextBuilder.build();
            // Resolve the encoder before transferring ownership. A missing encoder is therefore a
            // pre-transfer failure and the input reference is still ours to release below.
            currentEncoder = Objects.requireNonNull(encoder, "encoder");
            tableSchemaService = getLakehouseTableSchemaService();
        } catch (Throwable t) {
            genericEntry.entry().payload().release();
            metrics.getWriteAll().withAttributes(attributes)
                    .recordFailure(System.nanoTime() - writeStartTime);
            throw writeFailure(t);
        }
        try {
            currentEncoder.encode(schemaTopic, genericEntry, new ResultConsumer<MaterializationRecord<Object>>() {
                @Override
                public void onResult(MaterializationRecord<Object> entry) {
                    try {
                        long start = System.nanoTime();
                        beforeWrite(entry);
                        metrics.getBeforeWrite().withAttributes(attributes)
                                .recordSuccess(System.nanoTime() - start);
                        doWrite(writeNumberOfMessagesForThisEntry, genericEntry, entry, writeStartTime);
                    } catch (Throwable e) {
                        // wrap the single message into a GenericEntry to pass the error context
                        if (e instanceof MessageSerDeException) {
                            String recordStr;
                            try {
                                recordStr = objectMapper.writeValueAsString(entry.record());
                            } catch (IOException ioException) {
                                recordStr = entry.record().toString();
                            }
                            ByteBuf payload = Unpooled.wrappedBuffer(recordStr.getBytes(StandardCharsets.UTF_8));
                            var entry1 = new Entry(EntryHeader.NOT_FOUND, payload);
                            GenericEntry ge = new GenericEntry(entry1, entry.metadata());
                            onErrorWithCtx(ge, e);
                            return;
                        }
                        throw new FatalException(e);
                    }
                }

                // the onErrorWithCtx should be called only from the encoder now.
                // In the encoder process, if there is any error, it will call the
                // onErrorWithCtx method.
                @Override
                public void onErrorWithCtx(Object ctx, Throwable throwable) {
                    try {
                        if (throwable instanceof FatalException) {
                            throw (FatalException) throwable;
                        }
                        log.error("failed to write entry", throwable);
                        if (throwable instanceof MessageSerDeException) {
                            if (failureMessageHandler != null && ctx instanceof GenericEntry ge) {
                                // todo: we can check the exception, if it is the ExceptionWithCode,
                                // we can pass the exception code to the failure message.
                                var failureReason = throwable.getMessage() != null
                                    ? throwable.getMessage() : "Unknown error";
                                var messageId = String.valueOf(ge.entry().header().offset());
                                ByteBuf retainedPayload = ge.entry().payload().retain();
                                FailureMessage message;
                                try {
                                    message = FailureMessage.builder()
                                        .topic(topic)
                                        .payload(retainedPayload)
                                        .messageId(messageId)
                                        .failureReason(failureReason)
                                        .build();
                                } catch (Throwable buildFailure) {
                                    retainedPayload.release();
                                    throw buildFailure;
                                }
                                if (log.isDebugEnabled()) {
                                    log.debug("Failed to write entry to table for topic: {},"
                                            + " partition: {}, error: {}", topic, partition, failureReason);
                                }
                                try {
                                    failureMessagesFuture.add(failureMessageHandler.sendFailureMessage(message));
                                } finally {
                                    message.release();
                                }
                                return;
                            }
                        }
                        throw new FatalException(throwable);
                    } finally {
                        if (ctx instanceof GenericEntry ge) {
                            ge.entry().payload().release();
                        }
                    }
                }
            }, tableSchemaService, context);
        } catch (Throwable t) {
            metrics.getWriteAll().withAttributes(attributes)
                    .recordFailure(System.nanoTime() - writeStartTime);
            throw writeFailure(t);
        }
        // wait for all failure messages to be sent
        try {
            CompletableFuture.allOf(failureMessagesFuture.toArray(new CompletableFuture[0]))
                    .get(30, TimeUnit.SECONDS);
        } catch (Exception e) {
            throw new LakehouseOptException(ExceptionCode.INTERNAL_ERROR, "Failed to send failure messages", e);
        }
        metrics.getWriteAll().withAttributes(attributes)
                .recordSuccess(System.nanoTime() - writeStartTime);
    }

    private static ExceptionWithCode writeFailure(Throwable failure) {
        if (failure instanceof FatalException && failure.getCause() instanceof ExceptionWithCode ewc) {
            return ewc;
        }
        if (failure instanceof FatalException) {
            return new LakehouseOptException(ExceptionCode.INTERNAL_ERROR, "Fatal error during write", failure);
        }
        if (failure instanceof LakehouseOptException lakehouseOptException) {
            return lakehouseOptException;
        }
        return new LakehouseOptException(ExceptionCode.INTERNAL_ERROR, "Error during write", failure);
    }

    protected void doWrite(AtomicBoolean writeNumberOfMessagesForThisEntry, GenericEntry genericEntry,
            MaterializationRecord<Object> objectLakehouseEntry, long writeStartTime)
            throws LakehouseOptException, MessageSerDeException {

        try {
            metrics.getEncode().withAttributes(attributes)
                    .recordSuccess(System.nanoTime() - writeStartTime);
            var record = objectLakehouseEntry.record();
            var metadataToWrite = new HashMap<String, String>();
            if (objectLakehouseEntry.metadata().isPresent()) {
                var lakehouseEntryMetadata = objectLakehouseEntry.metadata().get();
                if (lakehouseEntryMetadata.isNeedToPersistent()) {
                    metadataToWrite.put("metadata", lakehouseEntryMetadata.serializeTo());
                }
                if (serializeType == EntrySerdeFactory.SerdeType.KAFKA_BATCHED_RAW_PARQUET) {
                    metadataToWrite.put("offset", String.valueOf(genericEntry.entry().header().offset()));
                    writer.setSecondaryIndexKey("offset");
                }

                // add the real message numbers
                if (!writeNumberOfMessagesForThisEntry.get()) {
                    AtomicLong messageCounter = (AtomicLong) writer.getExtraMetadata()
                            .computeIfAbsent(BATCH_MESSAGE_COUNT, k -> new AtomicLong(0));
                    messageCounter.addAndGet(lakehouseEntryMetadata.getNumberOfMessagesInBatch());
                    writeNumberOfMessagesForThisEntry.set(true);
                }
            }

            long writeStart = System.nanoTime();
            writer.write(record, Collections.unmodifiableMap(metadataToWrite));
            metrics.getWriteRecord().withAttributes(attributes)
                    .recordSuccess(System.nanoTime() - writeStart);

            if (objectLakehouseEntry.metadata().isPresent()) {
                var lakehouseEntryMetadata = objectLakehouseEntry.metadata().get();
                // update the end offset in the current writer
                var lakehouseEntryOffset = lakehouseEntryMetadata.getLakehouseEntryOffset();
                if (lakehouseEntryOffset != null) {
                    writer.getExtraMetadata().put(LAST_ENTRY_ID_IN_FILE, lakehouseEntryOffset.entryId());
                    writer.getExtraMetadata().put(LAST_BATCH_ID_IN_FILE, lakehouseEntryOffset.batchId());
                }
            }

        } catch (Throwable e) {
            metrics.getWriteRecord().withAttributes(attributes)
                    .recordFailure(System.nanoTime() - writeStartTime);
            if (e instanceof LakehouseOptException lakehouseOptException) {
                throw lakehouseOptException;
            } else {
                throw new LakehouseOptException(ExceptionCode.INTERNAL_ERROR, "Failed to write the entry", e);
            }
        }
    }

    protected abstract void beforeWrite(MaterializationRecord<Object> entry) throws LakehouseOptException;

    public TableSchemaService getLakehouseTableSchemaService() {
        return null;
    }

    @Override
    public void registerFailureMessageHandler(FailureMessageHandler failureMessageHandler) {
        this.failureMessageHandler = failureMessageHandler;
    }

    public List<IWriteResult> close() throws ExceptionWithCode {
        logCloseWarnings();
        try {
            return writer.close();
        } catch (IOException e) {
            throw new LakehouseOptException(ExceptionCode.LAKEHOUSE_WRITE_ERROR, e);
        }
    }

    /**
     * Logs DLT failure summary (via handler) and missing schema version warnings.
     * Subclasses that override {@link #close()} must call this method to ensure
     * diagnostic logs are emitted.
     */
    protected void logCloseWarnings() {
        if (failureMessageHandler != null) {
            failureMessageHandler.close();
        }
        int missingSchemaVersionCount = missingSchemaVersionTracker.getTotalCount();
        if (missingSchemaVersionCount > 0) {
            log.warn("Detected {} message(s) without schema version for topic: {}, partition: {}."
                            + " These messages were treated as bytes schema."
                            + " Sample messageIds (up to 10): {}",
                    missingSchemaVersionCount, topic, partition,
                    missingSchemaVersionTracker.getRecordedMessageIds());
        }
    }

}
