/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.materialization.serde.kafka;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaMetadata;
import io.lakestream.api.EntryHeader;
import io.lakestream.ursa.exception.ExceptionCode;
import io.lakestream.ursa.exception.MessageSerDeException;
import io.lakestream.ursa.materialization.serde.EntryEncoderContext;
import io.lakestream.ursa.materialization.serde.EntryFormat;
import io.lakestream.ursa.materialization.serde.GenericEntry;
import io.lakestream.ursa.materialization.serde.KafkaEntry;
import io.lakestream.ursa.materialization.serde.MaterializationRecord;
import io.lakestream.ursa.materialization.serde.ResultConsumer;
import io.lakestream.ursa.materialization.serde.SchemaKey;
import io.lakestream.ursa.materialization.serde.TableSchemaService;
import io.lakestream.ursa.materialization.serde.exception.FatalException;
import io.lakestream.ursa.storage.Entry;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.SimpleRecord;
import org.junit.jupiter.api.Test;

class KafkaEntryEncoderTest {

    @Test
    void releasesConsumedEntryOnSuccess() {
        ByteBuf payload = new KafkaEntry(null, "value".getBytes(StandardCharsets.UTF_8)).toByteBuf();
        GenericEntry entry = genericEntry(payload);
        AtomicReference<MaterializationRecord<String>> result = new AtomicReference<>();

        encoder(false).encode("events", entry, new ResultConsumer<>() {
            @Override
            public void onResult(MaterializationRecord<String> record) {
                result.set(record);
            }

            @Override
            public void onErrorWithCtx(Object context, Throwable throwable) {
                throw new AssertionError(throwable);
            }
        }, null, EntryEncoderContext.builder().entryFormat(EntryFormat.KAFKA).build());

        assertThat(payload.refCnt()).isZero();
        assertThat(result.get().record()).isEqualTo("value");
        assertThat(result.get().metadata().orElseThrow().getSchemaVersion()).isZero();
    }

    @Test
    void createsOwnedSingleRecordErrorContext() {
        ByteBuf payload = new KafkaEntry(null, "value".getBytes(StandardCharsets.UTF_8)).toByteBuf();
        GenericEntry entry = genericEntry(payload);
        AtomicReference<GenericEntry> errorContext = new AtomicReference<>();

        encoder(true).encode("events", entry, new ResultConsumer<>() {
            @Override
            public void onResult(MaterializationRecord<String> record) {
                throw new AssertionError("unexpected result");
            }

            @Override
            public void onErrorWithCtx(Object context, Throwable throwable) {
                errorContext.set((GenericEntry) context);
            }
        }, null, EntryEncoderContext.builder().entryFormat(EntryFormat.KAFKA).build());

        assertThat(errorContext.get()).isNotSameAs(entry);
        assertThat(errorContext.get().entry().header().offset()).isEqualTo(7L);
        assertThat(KafkaEntry.fromByteBuf(errorContext.get().entry().payload().duplicate()).value())
                .isEqualTo(bytes("value"));
        assertThat(payload.refCnt()).isZero();
        assertThat(errorContext.get().entry().payload().refCnt()).isOne();
        errorContext.get().entry().payload().release();
    }

    @Test
    void rawRecordRemainsPrimitiveWhenTopicAlsoHasRegisteredSchemas() {
        ByteBuf payload = new KafkaEntry(null, "raw-value".getBytes(StandardCharsets.UTF_8)).toByteBuf();
        AtomicReference<MaterializationRecord<String>> result = new AtomicReference<>();

        encoder(false, true).encode("events", genericEntry(payload), new ResultConsumer<>() {
            @Override
            public void onResult(MaterializationRecord<String> record) {
                result.set(record);
            }

            @Override
            public void onErrorWithCtx(Object context, Throwable throwable) {
                throw new AssertionError(throwable);
            }
        }, null, EntryEncoderContext.builder().entryFormat(EntryFormat.KAFKA).build());

        assertThat(result.get().record()).isEqualTo("raw-value");
        assertThat(payload.refCnt()).isZero();
    }

    @Test
    void decodesMemoryRecordsIntoOneResultPerRecordWithAbsoluteOffsetsAndEventTimes() {
        MemoryRecords firstBatch = MemoryRecords.withRecords(
                0L,
                Compression.gzip().build(),
                new SimpleRecord(KafkaBrokerEntryFixtures.RECORD_TIMESTAMP,
                        bytes("key-1"), bytes("value-1")),
                new SimpleRecord(KafkaBrokerEntryFixtures.RECORD_TIMESTAMP,
                        bytes("key-2"), bytes("value-2")));
        MemoryRecords secondBatch = MemoryRecords.withRecords(
                0L,
                Compression.NONE,
                new SimpleRecord(KafkaBrokerEntryFixtures.RECORD_TIMESTAMP,
                        bytes("key-3"), bytes("value-3")));
        ByteBuf payload = KafkaBrokerEntryFixtures.rawEntry(firstBatch, secondBatch);
        EntryHeader header = new EntryHeader(100L, 3, 1_700_000_000_500L,
                payload.readableBytes(), payload.readableBytes());
        GenericEntry entry = new GenericEntry(new Entry(header, payload));
        List<MaterializationRecord<String>> results = new ArrayList<>();
        List<EntryEncoderContext> contexts = new ArrayList<>();

        inspectingEncoder(contexts).encode("events", entry, new ResultConsumer<>() {
            @Override
            public void onResult(MaterializationRecord<String> record) {
                results.add(record);
            }

            @Override
            public void onErrorWithCtx(Object context, Throwable throwable) {
                throw new AssertionError(throwable);
            }
        }, null, EntryEncoderContext.builder().entryFormat(EntryFormat.URSA).build());

        assertThat(payload.refCnt()).isZero();
        assertThat(results).extracting(MaterializationRecord::record)
                .containsExactly("value-1", "value-2", "value-3");
        assertThat(results).allSatisfy(result -> assertThat(result.metadata().orElseThrow()
                .getNumberOfMessagesInBatch()).isEqualTo(3));
        assertThat(results).extracting(result -> result.metadata().orElseThrow()
                        .getLakehouseEntryOffset().entryId())
                .containsExactly(100L, 101L, 102L);
        assertThat(contexts).extracting(EntryEncoderContext::messageOffset)
                .containsExactly("100", "101", "102");
        assertThat(contexts).extracting(EntryEncoderContext::eventTime)
                .containsOnly(KafkaBrokerEntryFixtures.RECORD_TIMESTAMP);
        assertThat(contexts).extracting(EntryEncoderContext::publishTime)
                .containsOnly(1_700_000_000_500L);
        assertThat(contexts).extracting(EntryEncoderContext::keyBytesArray)
                .containsExactly(bytes("key-1"), bytes("key-2"), bytes("key-3"));
    }

    @Test
    void continuesWithLaterBatchRecordsAfterOneRecordFails() {
        MemoryRecords records = MemoryRecords.withRecords(
                Compression.NONE,
                new SimpleRecord(KafkaBrokerEntryFixtures.RECORD_TIMESTAMP, bytes("one")),
                new SimpleRecord(KafkaBrokerEntryFixtures.RECORD_TIMESTAMP, bytes("bad")),
                new SimpleRecord(KafkaBrokerEntryFixtures.RECORD_TIMESTAMP, bytes("three")));
        ByteBuf payload = KafkaBrokerEntryFixtures.rawEntry(records);
        EntryHeader header = new EntryHeader(20L, 3, 30L,
                payload.readableBytes(), payload.readableBytes());
        List<String> results = new ArrayList<>();
        AtomicReference<GenericEntry> errorContext = new AtomicReference<>();

        selectivelyFailingEncoder().encode("events", new GenericEntry(new Entry(header, payload)),
                new ResultConsumer<>() {
                    @Override
                    public void onResult(MaterializationRecord<String> record) {
                        results.add(record.record());
                    }

                    @Override
                    public void onErrorWithCtx(Object context, Throwable throwable) {
                        errorContext.set((GenericEntry) context);
                    }
                }, null, EntryEncoderContext.builder().entryFormat(EntryFormat.URSA).build());

        assertThat(results).containsExactly("one", "three");
        assertThat(payload.refCnt()).isZero();
        assertThat(errorContext.get().entry().header().offset()).isEqualTo(21L);
        assertThat(KafkaEntry.fromByteBuf(errorContext.get().entry().payload().duplicate()).value())
                .isEqualTo(bytes("bad"));
        errorContext.get().entry().payload().release();
    }

    @Test
    void callbackOwnsRetainedDecodeErrorContextEvenWhenItThrows() {
        ByteBuf payload = Unpooled.buffer(Integer.BYTES).writeInt(1);
        GenericEntry entry = genericEntry(payload);
        RuntimeException callbackFailure = new RuntimeException("callback failed");

        assertThatThrownBy(() -> encoder(false).encode("events", entry, new ResultConsumer<>() {
            @Override
            public void onResult(MaterializationRecord<String> record) {
                throw new AssertionError("unexpected result");
            }

            @Override
            public void onErrorWithCtx(Object context, Throwable throwable) {
                ((GenericEntry) context).entry().payload().release();
                throw callbackFailure;
            }
        }, null, EntryEncoderContext.builder().entryFormat(EntryFormat.URSA).build()))
                .isSameAs(callbackFailure);

        assertThat(payload.refCnt()).isZero();
    }

    @Test
    void transfersOwnedOriginalContextWhenDecodeErrorCallbackReturns() {
        ByteBuf payload = Unpooled.buffer(Integer.BYTES).writeInt(1);
        GenericEntry entry = genericEntry(payload);
        AtomicReference<GenericEntry> failureContext = new AtomicReference<>();

        encoder(false).encode("events", entry, new ResultConsumer<>() {
            @Override
            public void onResult(MaterializationRecord<String> record) {
                throw new AssertionError("unexpected result");
            }

            @Override
            public void onErrorWithCtx(Object context, Throwable throwable) {
                failureContext.set((GenericEntry) context);
            }
        }, null, EntryEncoderContext.builder().entryFormat(EntryFormat.URSA).build());

        assertThat(failureContext.get()).isSameAs(entry);
        assertThat(payload.refCnt()).isOne();
        failureContext.get().entry().payload().release();
        assertThat(payload.refCnt()).isZero();
    }

    @Test
    void callbackOwnsSingleRecordErrorContextEvenWhenItThrows() {
        ByteBuf payload = new KafkaEntry(null, bytes("value")).toByteBuf();
        GenericEntry entry = genericEntry(payload);
        AtomicReference<ByteBuf> failurePayload = new AtomicReference<>();
        RuntimeException callbackFailure = new RuntimeException("callback failed");

        assertThatThrownBy(() -> encoder(true).encode("events", entry, new ResultConsumer<>() {
            @Override
            public void onResult(MaterializationRecord<String> record) {
                throw new AssertionError("unexpected result");
            }

            @Override
            public void onErrorWithCtx(Object context, Throwable throwable) {
                ByteBuf ownedPayload = ((GenericEntry) context).entry().payload();
                failurePayload.set(ownedPayload);
                ownedPayload.release();
                throw callbackFailure;
            }
        }, null, EntryEncoderContext.builder().entryFormat(EntryFormat.KAFKA).build()))
                .isInstanceOf(FatalException.class)
                .hasCause(callbackFailure);

        assertThat(payload.refCnt()).isZero();
        assertThat(failurePayload.get().refCnt()).isZero();
    }

    private static GenericEntry genericEntry(ByteBuf payload) {
        EntryHeader header = new EntryHeader(7L, 1, 11L,
                payload.readableBytes(), payload.readableBytes());
        return new GenericEntry(new Entry(header, payload));
    }

    private static KafkaEntryEncoder<String> encoder(boolean fail) {
        return encoder(fail, false);
    }

    private static KafkaEntryEncoder<String> encoder(boolean fail, boolean hasSchema) {
        KafkaSchemaService schemaService = new KafkaSchemaService(new MockSchemaRegistryClient()) {
            @Override
            public boolean hasSchema(String topic) {
                return hasSchema;
            }
        };
        return new KafkaEntryEncoder<>(schemaService) {
            @Override
            protected String transform(Object object, SchemaMetadata schemaMetadata, SchemaKey schemaKey,
                                       TableSchemaService tableSchemaService, EntryEncoderContext context)
                    throws MessageSerDeException {
                if (fail) {
                    throw new MessageSerDeException(ExceptionCode.MESSAGE_PARSE_FAILED, "boom");
                }
                return new String((byte[]) object, StandardCharsets.UTF_8);
            }
        };
    }

    private static KafkaEntryEncoder<String> inspectingEncoder(List<EntryEncoderContext> contexts) {
        KafkaSchemaService schemaService = new KafkaSchemaService(new MockSchemaRegistryClient());
        return new KafkaEntryEncoder<>(schemaService) {
            @Override
            protected String transform(Object object, SchemaMetadata schemaMetadata, SchemaKey schemaKey,
                                       TableSchemaService tableSchemaService, EntryEncoderContext context) {
                contexts.add(context);
                return new String((byte[]) object, StandardCharsets.UTF_8);
            }
        };
    }

    private static KafkaEntryEncoder<String> selectivelyFailingEncoder() {
        KafkaSchemaService schemaService = new KafkaSchemaService(new MockSchemaRegistryClient());
        return new KafkaEntryEncoder<>(schemaService) {
            @Override
            protected String transform(Object object, SchemaMetadata schemaMetadata, SchemaKey schemaKey,
                                       TableSchemaService tableSchemaService, EntryEncoderContext context)
                    throws MessageSerDeException {
                String value = new String((byte[]) object, StandardCharsets.UTF_8);
                if ("bad".equals(value)) {
                    throw new MessageSerDeException(ExceptionCode.MESSAGE_PARSE_FAILED, "bad record");
                }
                return value;
            }
        };
    }

    private static byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }
}
