/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.lakehouse.reader;

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.lakestream.ursa.lakehouse.schema.KafkaSchemaRegistry;
import java.util.Properties;
import lombok.experimental.UtilityClass;

@UtilityClass
final class KafkaSchemaRegistryClients {

    static SchemaRegistryClient create(Properties properties) {
        return KafkaSchemaRegistry.createClient(properties);
    }
}
