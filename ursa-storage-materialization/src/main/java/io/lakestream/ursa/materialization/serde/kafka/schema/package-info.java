/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
/**
 * Schema Registry wire format support for JSON Schema and Protobuf Kafka records.
 *
 * <p>Only the Apache-2.0 {@code kafka-schema-registry-client} is used to talk to the registry. Schema
 * text is kept opaque ({@link io.lakestream.ursa.materialization.serde.kafka.schema.RawParsedSchema}),
 * the payload framing is decoded by
 * {@link io.lakestream.ursa.materialization.serde.kafka.schema.SchemaRegistryWireFormat}, and Protobuf
 * schema text is turned into descriptors by
 * {@link io.lakestream.ursa.materialization.serde.kafka.schema.ProtobufSchemaDescriptors}. None of the
 * Confluent Community License provider or serializer artifacts are required at runtime.
 */
package io.lakestream.ursa.materialization.serde.kafka.schema;
