/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.materialization.serde.kafka.schema;

import io.confluent.kafka.schemaregistry.AbstractSchemaProvider;
import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.SchemaProvider;
import io.confluent.kafka.schemaregistry.avro.AvroSchemaProvider;
import io.confluent.kafka.schemaregistry.client.rest.entities.Schema;
import java.util.List;
import java.util.Objects;

/**
 * {@link SchemaProvider} that wraps registry responses in {@link RawParsedSchema} instances.
 *
 * <p>Register these providers on a {@code CachedSchemaRegistryClient} so that schema ids of type
 * {@code JSON} and {@code PROTOBUF} can be resolved without the Confluent Community License provider jars.
 * Avro keeps using the Apache-2.0 {@link AvroSchemaProvider}.
 */
public final class RawSchemaProvider extends AbstractSchemaProvider {

    public static final String JSON_TYPE = "JSON";
    public static final String PROTOBUF_TYPE = "PROTOBUF";

    private final String schemaType;

    private RawSchemaProvider(String schemaType) {
        this.schemaType = schemaType;
    }

    public static RawSchemaProvider json() {
        return new RawSchemaProvider(JSON_TYPE);
    }

    public static RawSchemaProvider protobuf() {
        return new RawSchemaProvider(PROTOBUF_TYPE);
    }

    /** Avro plus raw JSON Schema and Protobuf providers: everything the materialization path can read. */
    public static List<SchemaProvider> defaultProviders() {
        return List.of(new AvroSchemaProvider(), json(), protobuf());
    }

    @Override
    public String schemaType() {
        return schemaType;
    }

    @Override
    public ParsedSchema parseSchemaOrElseThrow(Schema schema, boolean isNew, boolean normalize) {
        Objects.requireNonNull(schema, "schema");
        if (schema.getSchema() == null) {
            throw new IllegalArgumentException("Schema of type " + schemaType + " has no schema text");
        }
        return new RawParsedSchema(schemaType, schema.getSchema(), schema.getReferences(),
                schema.getMetadata(), schema.getRuleSet(), schema.getVersion());
    }
}
