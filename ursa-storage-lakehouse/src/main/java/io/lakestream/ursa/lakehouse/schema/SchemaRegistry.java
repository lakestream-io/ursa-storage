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

    SchemaMetadata fetchLatest(String topic) throws FetchSchemaFailedException, SchemaNotFoundException;
}
