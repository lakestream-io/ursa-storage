/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.lakehouse.schema;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaMetadata;
import io.lakestream.ursa.lakehouse.exception.SchemaNotFoundException;
import java.util.Properties;
import org.junit.jupiter.api.Test;

class KafkaSchemaRegistryTest {

    private static final String AVRO_SCHEMA =
            "{\"type\":\"record\",\"name\":\"Order\",\"fields\":[{\"name\":\"id\",\"type\":\"long\"}]}";

    @Test
    void usesInMemoryRegistryForRawRecordsWhenUrlIsNotConfigured() {
        KafkaSchemaRegistry registry = new KafkaSchemaRegistry(new Properties());

        assertInstanceOf(MockSchemaRegistryClient.class, registry.client());
    }

    @Test
    void valueSubjectUsesLogicalTopicVerbatim() {
        assertEquals("orders-value", KafkaSchemaRegistry.valueSubject("orders"));
        // A logical Kafka topic may legitimately end with a partition-like suffix; it must be preserved.
        assertEquals("orders-partition-3-value", KafkaSchemaRegistry.valueSubject("orders-partition-3"));
        // Streams created without Kafka lifecycle metadata may still carry a namespace prefix.
        assertEquals("orders-value", KafkaSchemaRegistry.valueSubject("default/orders"));
        assertThrows(IllegalArgumentException.class, () -> KafkaSchemaRegistry.valueSubject(" "));
    }

    @Test
    void fetchLatestResolvesSubjectFromLogicalTopic() throws Exception {
        KafkaSchemaRegistry registry = new KafkaSchemaRegistry(new Properties());
        registry.client().register("orders-value", new AvroSchema(AVRO_SCHEMA));

        SchemaMetadata metadata = registry.fetchLatest("orders");

        assertEquals("AVRO", metadata.getSchemaType());
        assertEquals(1, metadata.getVersion());
    }

    @Test
    void fetchLatestReportsMissingSubjectForUnresolvedStreamName() throws Exception {
        KafkaSchemaRegistry registry = new KafkaSchemaRegistry(new Properties());
        registry.client().register("orders-value", new AvroSchema(AVRO_SCHEMA));

        // The UUID-qualified stream name is not a subject; callers must resolve the logical topic first.
        assertThrows(SchemaNotFoundException.class,
                () -> registry.fetchLatest("default/orders-topic-id-PN--gt4BTtCOfFofVthDgw-partition-0"));
    }
}
