/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.lakehouse.schema;

import static org.junit.jupiter.api.Assertions.assertInstanceOf;

import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import java.util.Properties;
import org.junit.jupiter.api.Test;

class KafkaSchemaRegistryTest {

    @Test
    void usesInMemoryRegistryForRawRecordsWhenUrlIsNotConfigured() {
        KafkaSchemaRegistry registry = new KafkaSchemaRegistry(new Properties());

        assertInstanceOf(MockSchemaRegistryClient.class, registry.client());
    }
}
