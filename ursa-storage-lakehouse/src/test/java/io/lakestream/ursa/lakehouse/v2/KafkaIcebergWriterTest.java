/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.lakehouse.v2;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

import io.confluent.kafka.schemaregistry.annotations.Schema;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;
import io.lakestream.api.EntryHeader;
import io.lakestream.ursa.lakehouse.v2.serde.iceberg.KafkaEntryToIcebergRecordEncoder;
import io.lakestream.ursa.materialization.serde.EntryEncoderContext;
import io.lakestream.ursa.materialization.serde.GenericEntry;
import io.lakestream.ursa.materialization.serde.MaterializationRecord;
import io.lakestream.ursa.materialization.serde.ResultConsumer;
import io.lakestream.ursa.materialization.serde.kafka.KafkaSchemaService;
import io.lakestream.ursa.materialization.util.kafka.json.KafkaJsonSchemaSerializer;
import io.lakestream.ursa.storage.Entry;
import io.lakestream.ursa.test.containers.util.KafkaStandalone;
import io.netty.buffer.Unpooled;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import lombok.Cleanup;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.iceberg.data.Record;
import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.SimpleRecord;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

@Slf4j
public class KafkaIcebergWriterTest {

    private static KafkaStandalone kafkaStandalone;

    @BeforeAll
    static void startKafkaStandalone() {
        kafkaStandalone = new KafkaStandalone(true);
        kafkaStandalone.start();
    }

    @AfterAll
    static void stopKafkaStandalone() {
        if (kafkaStandalone != null) {
            kafkaStandalone.stop();
        }
    }

    record ProducedMessage<T>(T content, byte[] key, byte[] serializedValue, long offset) { }

    @Test
    void simpleTest() throws Exception {
        var topic = "topic-" + RandomStringUtils.secure().nextAlphabetic(4);

        var numberOfMessages = 10;

        List<ProducedMessage<byte[]>> messages = new ArrayList<>();
        for (int i = 0; i < numberOfMessages; i++) {
            var value = ("message" + i).getBytes(StandardCharsets.UTF_8);
            messages.add(new ProducedMessage<>(value, null, value, i));
        }

        var result = encodeEntries(topic, messages);

        assertEquals(numberOfMessages, result.size());

        for (int i = 0; i < result.size(); i++) {
            var record = result.get(i).record();
            var expectedValue = messages.get(i).content();
            var actualValue = (ByteBuffer) record.getField("payload");
            assertEquals(new String(expectedValue, StandardCharsets.UTF_8),
                new String(actualValue.array(), StandardCharsets.UTF_8));
        }

    }

    @Schema(
        value = """
            {
              "$schema": "http://json-schema.org/draft-07/schema#",
              "title": "JsonValue",
              "type": "object",
              "properties": {
                "id": {
                  "type": "integer"
                },
                "name": {
                  "type": "string"
                },
                "tags": {
                  "type": "object",
                  "additionalProperties": {
                    "type": "string"
                  }
                },
                "nested": {
                  "type": "object",
                  "title": "JsonNestedValue",
                  "properties": {
                    "enabled": {
                      "type": "boolean"
                    }
                  }
                }
              }
            }
            """,
        refs = {}
    )
    @Data
    static class JsonValue {
        private int id;
        private String name;
        private Map<String, String> tags;
        private JsonNestedValue nested;
    }

    @Data
    static class JsonNestedValue {
        private boolean enabled;
    }

    @Test
    void testJsonIntegration() throws Exception {
        var topic = "json-topic-" + RandomStringUtils.secure().nextAlphabetic(4);
        var numberOfMessages = 5;

        @Cleanup
        var serializer = new KafkaJsonSchemaSerializer<Object>(
                kafkaStandalone.getSchemaRegistryClient(),
                Map.of("schema.registry.url", "unused", "auto.register.schemas", true));
        List<ProducedMessage<JsonValue>> messages = new ArrayList<>();
        for (int i = 0; i < numberOfMessages; i++) {
            var nested = new JsonNestedValue();
            nested.setEnabled(i % 2 == 0);

            var value = new JsonValue();
            value.setId(i);
            value.setName("json-" + i);
            value.setTags(Map.of("k" + i, "v" + i));
            value.setNested(nested);

            byte[] key = ("key-" + i).getBytes(StandardCharsets.UTF_8);
            messages.add(new ProducedMessage<>(value, key, serializer.serialize(topic, value), i));
        }

        var result = encodeEntries(topic, messages);

        assertEquals(numberOfMessages, result.size());
        for (int i = 0; i < result.size(); i++) {
            var expected = messages.get(i).content();
            var actual = result.get(i).record();

            assertEquals(expected.getId(), ((Number) actual.getField("id")).intValue());
            assertEquals(expected.getName(), actual.getField("name"));
            assertEquals(expected.getTags(), actual.getField("tags"));

            var nested = (Record) actual.getField("nested");
            assertEquals(expected.getNested().isEnabled(), nested.getField("enabled"));
        }
    }

    private static final org.apache.avro.Schema AVRO_VALUE_SCHEMA = new org.apache.avro.Schema.Parser().parse("""
        {
          "type": "record",
          "name": "AvroValue",
          "namespace": "io.lakestream.ursa.lakehouse.v2",
          "fields": [
            {
              "name": "id",
              "type": "int"
            },
            {
              "name": "name",
              "type": "string"
            },
            {
              "name": "tags",
              "type": {
                "type": "map",
                "values": "string"
              }
            },
            {
              "name": "nested",
              "type": {
                "type": "record",
                "name": "AvroNestedValue",
                "fields": [
                  {
                    "name": "enabled",
                    "type": "boolean"
                  }
                ]
              }
            }
          ]
        }
        """);


    @Test
    void testAvroIntegration() throws Exception {
        var topic = "avro-topic-" + RandomStringUtils.secure().nextAlphabetic(4);
        var numberOfMessages = 5;

        @Cleanup
        var serializer = new KafkaAvroSerializer(kafkaStandalone.getSchemaRegistryClient());
        serializer.configure(Map.of(KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, "unused"), false);
        List<ProducedMessage<GenericRecord>> messages = new ArrayList<>();
        for (int i = 0; i < numberOfMessages; i++) {
            var nestedSchema = AVRO_VALUE_SCHEMA.getField("nested").schema();
            var nested = new GenericData.Record(nestedSchema);
            nested.put("enabled", i % 2 == 0);

            var value = new GenericData.Record(AVRO_VALUE_SCHEMA);
            value.put("id", i);
            value.put("name", "avro-" + i);
            value.put("tags", Map.of("k" + i, "v" + i));
            value.put("nested", nested);

            byte[] key = ("key-" + i).getBytes(StandardCharsets.UTF_8);
            messages.add(new ProducedMessage<>(value, key, serializer.serialize(topic, value), i));
        }

        var result = encodeEntries(topic, messages);

        assertEquals(numberOfMessages, result.size());
        for (int i = 0; i < result.size(); i++) {
            var expected = messages.get(i).content();
            var actual = result.get(i).record();

            assertEquals(expected.get("id"), actual.getField("id"));
            assertEquals(expected.get("name").toString(), actual.getField("name"));
            assertEquals(expected.get("tags"), actual.getField("tags"));

            var expectedNested = (GenericRecord) expected.get("nested");
            var actualNested = (Record) actual.getField("nested");
            assertEquals(expectedNested.get("enabled"), actualNested.getField("enabled"));
        }
    }

    private ArrayList<MaterializationRecord<Record>> encodeEntries(
            String topic, List<? extends ProducedMessage<?>> messages) {
        var schemaService = new KafkaSchemaService(kafkaStandalone.getSchemaRegistryClient(), false);
        KafkaEntryToIcebergRecordEncoder encoder = new KafkaEntryToIcebergRecordEncoder(schemaService);

        var result = new ArrayList<MaterializationRecord<Record>>();
        for (ProducedMessage<?> message : messages) {
            GenericEntry entry = rawEntry(message);
            var encodeContext = EntryEncoderContext.builder().build();
            encoder.encode(topic, entry, new ResultConsumer<MaterializationRecord<Record>>() {
                @Override
                public void onResult(MaterializationRecord<Record> recordLakehouseEntry) {
                    result.add(recordLakehouseEntry);
                }

                @Override
                public void onErrorWithCtx(Object ctx, Throwable throwable) {
                    fail(throwable);
                }
            }, null, encodeContext);
        }
        return result;
    }

    private static GenericEntry rawEntry(ProducedMessage<?> message) {
        MemoryRecords records = MemoryRecords.withRecords(
                0L, Compression.NONE,
                new SimpleRecord(1_700_000_000_000L, message.key(), message.serializedValue()));
        ByteBuffer recordsBuffer = records.buffer().duplicate();
        var payload = Unpooled.buffer(recordsBuffer.remaining());
        payload.writeBytes(recordsBuffer);
        var header = new EntryHeader(
                message.offset(), 1, 1_700_000_000_000L, payload.readableBytes(), message.offset());
        return new GenericEntry(new Entry(header, payload));
    }
}
