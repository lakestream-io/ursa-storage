/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.materialization.serde.kafka;

import io.confluent.kafka.schemaregistry.client.SchemaMetadata;
import io.confluent.kafka.schemaregistry.protobuf.MessageIndexes;
import io.lakestream.api.EntryHeader;
import io.lakestream.ursa.exception.ExceptionCode;
import io.lakestream.ursa.exception.ExceptionWithCode;
import io.lakestream.ursa.exception.MessageSerDeException;
import io.lakestream.ursa.materialization.serde.EntryEncoderContext;
import io.lakestream.ursa.materialization.serde.EntryFormat;
import io.lakestream.ursa.materialization.serde.GenericEntry;
import io.lakestream.ursa.materialization.serde.KafkaEntry;
import io.lakestream.ursa.materialization.serde.LakehouseEntryMetadata;
import io.lakestream.ursa.materialization.serde.MaterializationRecord;
import io.lakestream.ursa.materialization.serde.ResultConsumer;
import io.lakestream.ursa.materialization.serde.SchemaCache;
import io.lakestream.ursa.materialization.serde.SchemaKey;
import io.lakestream.ursa.materialization.serde.SchemaService;
import io.lakestream.ursa.materialization.serde.TableSchemaService;
import io.lakestream.ursa.materialization.serde.exception.FatalException;
import io.lakestream.ursa.materialization.util.KafkaMessage;
import io.lakestream.ursa.materialization.util.PBRecordReader;
import io.lakestream.ursa.storage.Entry;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Optional;

/** Decodes Kafka records from either a raw broker WAL batch or a normalized direct-Kafka entry. */
public abstract class KafkaEntryEncoder<T> {

    protected final KafkaSchemaService schemaService;
    protected static final SchemaCache SCHEMA_CACHE = SchemaCache.INSTANCE;

    protected KafkaEntryEncoder(SchemaService schemaService) {
        if (schemaService instanceof KafkaSchemaService kafkaSchemaService) {
            this.schemaService = kafkaSchemaService;
        } else {
            throw new IllegalArgumentException("SchemaService must be a KafkaSchemaService");
        }
    }

    record ProcessResult<T>(T value, long schemaVersion) {
    }

    public void encode(String topic, GenericEntry genericEntry,
                       ResultConsumer<MaterializationRecord<T>> consumer,
                       TableSchemaService tableSchemaService, EntryEncoderContext context) {
        var entry = genericEntry.entry();
        try {
            List<KafkaMessage> messages = decodeMessages(entry, context.entryFormat());
            for (KafkaMessage message : messages) {
                try {
                    encodeMessage(topic, entry, message, consumer, tableSchemaService, context);
                } catch (FatalException fatal) {
                    throw fatal;
                } catch (Throwable messageFailure) {
                    reportMessageFailure(genericEntry, message, consumer, messageFailure);
                }
            }
        } catch (Throwable throwable) {
            if (throwable instanceof FatalException fatal) {
                throw fatal;
            }
            // The encoder consumes its input. Transfer one reference to the error callback.
            entry.payload().retain();
            consumer.onErrorWithCtx(genericEntry, asDeserializationFailure(throwable));
        } finally {
            entry.payload().release();
        }
    }

    private void encodeMessage(
            String topic,
            Entry entry,
            KafkaMessage message,
            ResultConsumer<MaterializationRecord<T>> consumer,
            TableSchemaService tableSchemaService,
            EntryEncoderContext context) throws ExceptionWithCode {
        EntryEncoderContext.EntryEncoderContextBuilder messageContextBuilder = context.toBuilder()
                .keyBytes(message.key())
                .messageOffset(Long.toString(message.offset()))
                .publishTime(entry.header().writtenTimestamp());
        if (message.timestamp() >= 0) {
            messageContextBuilder.eventTime(message.timestamp());
        }
        ProcessResult<T> processResult = processKafkaMessage(
                topic, message, tableSchemaService, messageContextBuilder.build());
        LakehouseEntryMetadata metadata = new LakehouseEntryMetadata();
        metadata.setEntryHeader(entry.header());
        metadata.setSchemaVersion(processResult.schemaVersion());
        metadata.setNumberOfMessagesInBatch(entry.header().numberOfMessages());
        metadata.setLakehouseEntryOffset(message.offset(), 0);
        metadata.setNeedToPersistent(true);
        consumer.onResult(new MaterializationRecord<>(processResult.value(), metadata));
    }

    private static <T> void reportMessageFailure(
            GenericEntry originalEntry,
            KafkaMessage message,
            ResultConsumer<MaterializationRecord<T>> consumer,
            Throwable failure) {
        var payload = new KafkaEntry(message.key(), message.value()).toByteBuf();
        EntryHeader originalHeader = originalEntry.entry().header();
        EntryHeader messageHeader = new EntryHeader(
                message.offset(), 1, originalHeader.writtenTimestamp(),
                payload.readableBytes(), originalHeader.cumulativeSize());
        GenericEntry failureContext = new GenericEntry(
                new Entry(messageHeader, payload), originalEntry.metadata());
        try {
            consumer.onErrorWithCtx(failureContext, asDeserializationFailure(failure));
        } catch (Throwable callbackFailure) {
            throw new FatalException(callbackFailure);
        }
    }

    private static ExceptionWithCode asDeserializationFailure(Throwable failure) {
        return failure instanceof ExceptionWithCode exceptionWithCode
                ? exceptionWithCode
                : new MessageSerDeException(ExceptionCode.MESSAGE_DESERIALIZE_FROM_SOURCE_ERROR, failure);
    }

    private static List<KafkaMessage> decodeMessages(Entry entry, EntryFormat entryFormat) {
        if (entryFormat == EntryFormat.KAFKA) {
            KafkaEntry kafkaEntry = KafkaEntry.fromByteBuf(entry.payload().duplicate());
            return List.of(new KafkaMessage(entry.header().offset(), kafkaEntry.key(), kafkaEntry.value()));
        }
        return KafkaStorageEntryDecoder.decode(
                entry.payload().duplicate(), entry.header().offset(), entry.header().numberOfMessages());
    }

    protected ProcessResult<T> processKafkaMessage(String topic, KafkaMessage kafkaMessage,
                                                    TableSchemaService tableSchemaService,
                                                    EntryEncoderContext context) throws ExceptionWithCode {
        try {
            byte[] data = kafkaMessage.getData();
            SchemaMetadata schemaMetadata = schemaService.getPrimitiveSchemaMetadata(topic);
            SchemaKey.SchemaKeyBuilder schemaKeyBuilder = SchemaKey.builder()
                    .topicName(topic)
                    .schemaVersion(schemaMetadata.getVersion())
                    .messageType(SchemaKey.MessageType.KAFKA);
            Object value = data;
            Optional<List<Integer>> messageIndexes = Optional.empty();
            if (data != null && schemaService.hasSchema(topic)) {
                int schemaId = getSchemaId(data);
                if (schemaId != KafkaSchemaService.PRIMITIVE_SCHEMA_ID) {
                    schemaMetadata = schemaService.fetchSchema(topic, schemaId);
                    if ("PROTOBUF".equalsIgnoreCase(schemaMetadata.getSchemaType())) {
                        messageIndexes = Optional.of(parseMessageIndexes(data));
                    }
                    schemaKeyBuilder.schemaVersion(schemaMetadata.getVersion());
                    value = schemaService.getDeserializer(schemaMetadata.getSchemaType())
                            .deserialize(topic, data);
                }
            }
            if (data != null && schemaMetadata.getId() == KafkaSchemaService.PRIMITIVE_SCHEMA_ID) {
                schemaService.registerPrimitiveSchema(topic, schemaMetadata);
                if (context.missingSchemaVersionTracker() != null) {
                    context.missingSchemaVersionTracker().record("offset:" + kafkaMessage.offset());
                }
            }

            Optional<String> protobufMessageName = PBRecordReader.resolveProtobufMessageName(
                    schemaMetadata.getSchemaType(), schemaMetadata.getSchema(), messageIndexes);
            EntryEncoderContext messageContext = context.toBuilder()
                    .schemaVersion((long) schemaMetadata.getVersion())
                    .protobufMessageName(protobufMessageName)
                    .build();
            SchemaKey schemaKey = schemaKeyBuilder.build();
            T transformedValue = transform(
                    value, schemaMetadata, schemaKey, tableSchemaService, messageContext);
            return new ProcessResult<>(transformedValue, schemaKey.getSchemaVersion());
        } catch (Throwable throwable) {
            if (throwable instanceof FatalException fatal) {
                throw fatal;
            }
            if (throwable instanceof ExceptionWithCode exceptionWithCode) {
                throw exceptionWithCode;
            }
            throw new MessageSerDeException(
                    ExceptionCode.MESSAGE_DESERIALIZE_FROM_SOURCE_ERROR, throwable);
        }
    }

    protected abstract T transform(Object object, SchemaMetadata schemaMetadata, SchemaKey schemaKey,
                                   TableSchemaService tableSchemaService,
                                   EntryEncoderContext context) throws MessageSerDeException;

    private static List<Integer> parseMessageIndexes(byte[] data) {
        ByteBuffer message = ByteBuffer.wrap(data);
        message.get();
        message.getInt();
        return MessageIndexes.readFrom(message).indexes();
    }

    private static int getSchemaId(byte[] payload) {
        ByteBuffer buffer = ByteBuffer.wrap(payload);
        if (buffer.remaining() < 5 || buffer.get() != 0) {
            return KafkaSchemaService.PRIMITIVE_SCHEMA_ID;
        }
        return buffer.getInt();
    }
}
