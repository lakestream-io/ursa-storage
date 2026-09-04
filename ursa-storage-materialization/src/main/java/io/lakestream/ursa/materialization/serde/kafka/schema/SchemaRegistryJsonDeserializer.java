/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.materialization.serde.kafka.schema;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;

/**
 * Deserializes schema-registry-framed JSON Schema payloads into Jackson trees.
 *
 * <p>The schema id is only validated structurally here; the caller resolves the schema itself through the
 * registry (see {@code KafkaSchemaService}) and converts the document with the table schema derived from the
 * JSON Schema text. Payload validation against the JSON Schema is not performed, matching the default
 * behaviour of the Schema Registry JSON deserializer.
 */
public final class SchemaRegistryJsonDeserializer implements Deserializer<JsonNode> {

    private final ObjectMapper objectMapper;

    public SchemaRegistryJsonDeserializer() {
        this(new ObjectMapper());
    }

    public SchemaRegistryJsonDeserializer(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    @Override
    public JsonNode deserialize(String topic, byte[] data) {
        if (data == null) {
            return null;
        }
        ByteBuffer buffer = ByteBuffer.wrap(data);
        int schemaId = SchemaRegistryWireFormat.readSchemaId(buffer);
        try {
            return objectMapper.readTree(new ByteArrayInputStream(data, buffer.position(), buffer.remaining()));
        } catch (IOException e) {
            throw new SerializationException("Error deserializing JSON message for schema id " + schemaId, e);
        }
    }
}
