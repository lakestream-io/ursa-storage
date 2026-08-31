/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.materialization.serde.kafka;

import io.confluent.kafka.schemaregistry.client.SchemaMetadata;
import io.lakestream.api.EntryHeader;
import io.lakestream.ursa.exception.ExceptionWithCode;
import io.lakestream.ursa.materialization.serde.GenericEntry;
import io.lakestream.ursa.materialization.serde.MaterializationRecord;
import io.lakestream.ursa.materialization.serde.ResultConsumer;
import io.lakestream.ursa.materialization.serde.SchemaCache;
import io.lakestream.ursa.materialization.serde.SchemaKey;
import io.lakestream.ursa.materialization.serde.SchemaService;
import io.lakestream.ursa.materialization.util.KafkaMessage;
import io.lakestream.ursa.storage.Entry;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.List;

/** Encodes materialized rows back into one-record native Kafka {@code MemoryRecords} entries. */
public abstract class KafkaEntryDecoder<T> {

    protected final KafkaSchemaService schemaService;
    protected static final SchemaCache SCHEMA_CACHE = SchemaCache.INSTANCE;

    protected KafkaEntryDecoder(SchemaService schemaService) {
        if (schemaService == null) {
            this.schemaService = null;
        } else if (schemaService instanceof KafkaSchemaService kafkaSchemaService) {
            this.schemaService = kafkaSchemaService;
        } else {
            throw new IllegalArgumentException("SchemaService must be a KafkaSchemaService");
        }
    }

    public void decode(String topic, Iterator<MaterializationRecord<T>> iterator,
                       ResultConsumer<GenericEntry> consumer) {
        while (iterator.hasNext()) {
            MaterializationRecord<T> materialized = iterator.next();
            if (materialized == null) {
                break;
            }
            try {
                ByteBuffer value = processLakehouseEntry(topic, materialized);
                byte[] valueBytes = null;
                if (value != null) {
                    ByteBuffer copy = value.duplicate();
                    valueBytes = new byte[copy.remaining()];
                    copy.get(valueBytes);
                }
                var metadata = materialized.metadata().orElseThrow(
                        () -> new IllegalArgumentException("Materialization record metadata is required"));
                EntryHeader sourceHeader = metadata.getEntryHeader();
                var payload = KafkaMemoryRecords.encode(new KafkaMessage(
                        0L, sourceHeader.writtenTimestamp(), null, valueBytes, List.of()));
                EntryHeader entryHeader = new EntryHeader(
                        sourceHeader.offset(), 1, sourceHeader.writtenTimestamp(),
                        payload.readableBytes(), sourceHeader.cumulativeSize());
                Entry entry = new Entry(entryHeader, payload);
                consumer.onResult(new GenericEntry(entry, materialized.metadata()));
            } catch (Throwable throwable) {
                consumer.onErrorWithCtx(materialized, throwable);
            }
        }
    }

    protected ByteBuffer processLakehouseEntry(String topic, MaterializationRecord<T> lakehouseEntry)
            throws ExceptionWithCode {
        var metadata = lakehouseEntry.metadata().orElseThrow(
                () -> new IllegalArgumentException("Materialization record metadata is required"));
        long schemaVersion = metadata.getSchemaVersion() == null
                ? KafkaSchemaService.PRIMITIVE_SCHEMA_ID : metadata.getSchemaVersion();
        SchemaKey schemaKey = SchemaKey.builder()
                .topicName(topic)
                .messageType(SchemaKey.MessageType.KAFKA)
                .schemaVersion(schemaVersion)
                .build();
        SchemaMetadata schemaMetadata = schemaService.fetchSchemaByVersion(
                topic, Math.toIntExact(schemaVersion));
        return transform(lakehouseEntry.record(), schemaMetadata, schemaKey);
    }

    protected abstract ByteBuffer transform(T object, SchemaMetadata schemaMetadata,
                                            SchemaKey schemaKey) throws ExceptionWithCode;
}
