/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.test;

import static org.assertj.core.api.Assertions.assertThat;

import io.confluent.kafka.schemaregistry.avro.AvroSchemaProvider;
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.lakestream.api.EntryHeader;
import io.lakestream.ursa.lakehouse.delta.GenericRow;
import io.lakestream.ursa.lakehouse.utils.LakehouseFieldNames;
import io.lakestream.ursa.lakehouse.v2.serde.delta.KafkaEntryToDeltaRecordEncoder;
import io.lakestream.ursa.lakehouse.v2.serde.iceberg.KafkaEntryToIcebergRecordEncoder;
import io.lakestream.ursa.materialization.serde.EntryEncoderContext;
import io.lakestream.ursa.materialization.serde.GenericEntry;
import io.lakestream.ursa.materialization.serde.MaterializationRecord;
import io.lakestream.ursa.materialization.serde.ResultConsumer;
import io.lakestream.ursa.materialization.serde.kafka.KafkaSchemaService;
import io.lakestream.ursa.storage.Entry;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.iceberg.data.Record;
import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.SimpleRecord;
import org.junit.jupiter.api.Test;

class KafkaLakehouseIngestionTest {

    private static final long RECORD_TIMESTAMP = 1_700_000_000_000L;
    private static final Schema VALUE_SCHEMA = new Schema.Parser().parse("""
        {
          "type": "record",
          "name": "Order",
          "namespace": "io.lakestream.ursa.test",
          "fields": [
            {"name": "id", "type": "long"},
            {"name": "description", "type": "string"}
          ]
        }
        """);

    @Test
    void convertsRawKafkaMemoryRecordsAndTransfersBufferOwnership() {
        var registry = new MockSchemaRegistryClient(List.of(new AvroSchemaProvider()));
        var schemaService = new KafkaSchemaService(registry, false);
        var key = "order-7".getBytes(StandardCharsets.UTF_8);
        var value = "created".getBytes(StandardCharsets.UTF_8);
        ByteBuf payload = rawMemoryRecords(key, value);

        MaterializationRecord<Record> materialized = encode(
            "raw-orders-" + UUID.randomUUID(), payload, schemaService);

        assertThat(bytes(materialized.record().getField("payload"))).isEqualTo(value);
        assertThat(bytes(materialized.record().getField(LakehouseFieldNames.INTERNAL_KEY))).isEqualTo(key);
        assertThat(materialized.metadata().orElseThrow().getSchemaVersion()).isZero();
        assertThat(payload.refCnt()).isZero();
    }

    @Test
    void decodesConfluentAvroFrameIntoIcebergRecord() {
        String topic = "avro-orders-" + UUID.randomUUID();
        var registry = new MockSchemaRegistryClient(List.of(new AvroSchemaProvider()));
        var serializer = new KafkaAvroSerializer(registry);
        serializer.configure(Map.of("schema.registry.url", "mock://ursa-storage-test"), false);

        var value = new GenericData.Record(VALUE_SCHEMA);
        value.put("id", 42L);
        value.put("description", "schema-aware");
        byte[] serialized = serializer.serialize(topic, value);
        ByteBuf payload = rawMemoryRecords(null, serialized);

        MaterializationRecord<Record> materialized = encode(
            topic, payload, new KafkaSchemaService(registry, false));

        assertThat(materialized.record().getField("id")).isEqualTo(42L);
        assertThat(materialized.record().getField("description").toString()).isEqualTo("schema-aware");
        assertThat(materialized.metadata().orElseThrow().getSchemaVersion()).isEqualTo(1L);
        assertThat(payload.refCnt()).isZero();
        serializer.close();
    }

    @Test
    void decodesConfluentAvroMemoryRecordsIntoDeltaRow() {
        String topic = "delta-avro-orders-" + UUID.randomUUID();
        var registry = new MockSchemaRegistryClient(List.of(new AvroSchemaProvider()));
        var serializer = new KafkaAvroSerializer(registry);
        serializer.configure(Map.of("schema.registry.url", "mock://ursa-storage-test"), false);

        var value = new GenericData.Record(VALUE_SCHEMA);
        value.put("id", 84L);
        value.put("description", "delta-schema-aware");
        ByteBuf payload = rawMemoryRecords(null, serializer.serialize(topic, value));
        var header = new EntryHeader(11L, 1, 1234L, payload.readableBytes(), payload.readableBytes());
        var entry = new GenericEntry(new Entry(header, payload));
        var result = new AtomicReference<MaterializationRecord<GenericRow>>();
        var encoder = new KafkaEntryToDeltaRecordEncoder(new KafkaSchemaService(registry, false));

        encoder.encode(topic, entry, resultConsumer(result), null,
            EntryEncoderContext.builder().build());

        GenericRow row = result.get().record();
        assertThat(row.getLong(row.getSchema().indexOf("id"))).isEqualTo(84L);
        assertThat(row.getString(row.getSchema().indexOf("description")))
            .isEqualTo("delta-schema-aware");
        assertThat(payload.refCnt()).isZero();
        serializer.close();
    }

    @Test
    void derivesSchemaSubjectFromCanonicalPartitionName() {
        var registry = new MockSchemaRegistryClient(List.of(new AvroSchemaProvider()));
        var schemaService = new KafkaSchemaService(registry);

        assertThat(schemaService.getSubject("default/orders-partition-3"))
            .isEqualTo("default/orders-value");
    }

    private static MaterializationRecord<Record> encode(
            String topic, ByteBuf payload, KafkaSchemaService schemaService) {
        var header = new EntryHeader(11L, 1, 1234L, payload.readableBytes(), payload.readableBytes());
        var entry = new GenericEntry(new Entry(header, payload));
        var result = new AtomicReference<MaterializationRecord<Record>>();
        var encoder = new KafkaEntryToIcebergRecordEncoder(schemaService);
        var context = EntryEncoderContext.builder()
            .isPersistKey(true)
            .build();

        encoder.encode(topic, entry, resultConsumer(result), null, context);

        return result.get();
    }

    private static <T> ResultConsumer<MaterializationRecord<T>> resultConsumer(
            AtomicReference<MaterializationRecord<T>> result) {
        return new ResultConsumer<>() {
            @Override
            public void onResult(MaterializationRecord<T> record) {
                result.set(record);
            }

            @Override
            public void onErrorWithCtx(Object errorContext, Throwable throwable) {
                throw new AssertionError(throwable);
            }
        };
    }

    static ByteBuf rawMemoryRecords(byte[] key, byte[] value) {
        MemoryRecords records = MemoryRecords.withRecords(
            0L, Compression.NONE, new SimpleRecord(RECORD_TIMESTAMP, key, value));
        ByteBuffer recordsBuffer = records.buffer().duplicate();
        ByteBuf payload = Unpooled.buffer(recordsBuffer.remaining());
        payload.writeBytes(recordsBuffer);
        return payload;
    }

    private static byte[] bytes(Object value) {
        ByteBuffer buffer = ((ByteBuffer) value).duplicate();
        byte[] result = new byte[buffer.remaining()];
        buffer.get(result);
        return result;
    }
}
