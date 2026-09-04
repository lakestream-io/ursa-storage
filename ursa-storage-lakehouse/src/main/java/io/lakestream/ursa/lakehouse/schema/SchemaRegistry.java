/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.lakehouse.schema;

import io.confluent.kafka.schemaregistry.client.SchemaMetadata;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.lakestream.ursa.lakehouse.exception.FetchSchemaFailedException;
import io.lakestream.ursa.lakehouse.exception.SchemaNotFoundException;

public interface SchemaRegistry {

    String name();

    SchemaRegistryClient client();

    /**
     * Fetches the latest registered schema for a logical source topic.
     *
     * <p>The argument is the logical topic name as known to the source system (for Kafka, the topic the
     * producer registered its schema under), not the UUID-qualified storage stream or log name. Callers
     * must resolve it from the stream properties first, for example with
     * {@code KafkaSourceMetadata.topicName(streamName, streamProperties)}.
     */
    SchemaMetadata fetchLatest(String logicalTopic) throws FetchSchemaFailedException, SchemaNotFoundException;
}
