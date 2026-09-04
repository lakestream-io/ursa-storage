/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.materialization.serde.kafka.schema;

import com.google.protobuf.CodedInputStream;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.DynamicMessage;
import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;

/**
 * Deserializes schema-registry-framed Protobuf payloads into {@link DynamicMessage}s.
 *
 * <p>The schema text is fetched by id through the registry client, compiled with
 * {@link ProtobufSchemaDescriptors}, and the message type is selected with the wire-format message indexes.
 */
public final class SchemaRegistryProtobufDeserializer implements Deserializer<DynamicMessage> {

    private final SchemaRegistryClient schemaRegistryClient;
    private final Function<String, String> subjectResolver;

    /**
     * @param schemaRegistryClient client used to resolve schema ids
     * @param subjectResolver maps a topic to the registry subject used for id lookups; sharing the resolver
     *                        with the surrounding schema service keeps the client-side caches warm
     */
    public SchemaRegistryProtobufDeserializer(SchemaRegistryClient schemaRegistryClient,
                                              Function<String, String> subjectResolver) {
        this.schemaRegistryClient = Objects.requireNonNull(schemaRegistryClient, "schemaRegistryClient");
        this.subjectResolver = Objects.requireNonNull(subjectResolver, "subjectResolver");
    }

    @Override
    public DynamicMessage deserialize(String topic, byte[] data) {
        if (data == null) {
            return null;
        }
        ByteBuffer buffer = ByteBuffer.wrap(data);
        int schemaId = SchemaRegistryWireFormat.readSchemaId(buffer);
        List<Integer> messageIndexes = SchemaRegistryWireFormat.readMessageIndexes(buffer);
        ParsedSchema schema = fetchSchema(topic, schemaId);
        if (!RawSchemaProvider.PROTOBUF_TYPE.equals(schema.schemaType())) {
            throw new SerializationException("Schema id " + schemaId + " is of type " + schema.schemaType()
                    + ", expected " + RawSchemaProvider.PROTOBUF_TYPE);
        }
        Descriptor descriptor;
        try {
            descriptor = ProtobufSchemaDescriptors.messageByIndexes(schema.canonicalString(), messageIndexes);
        } catch (IllegalArgumentException e) {
            throw new SerializationException("Error resolving Protobuf message for schema id " + schemaId, e);
        }
        try {
            return DynamicMessage.parseFrom(descriptor,
                    CodedInputStream.newInstance(data, buffer.position(), buffer.remaining()));
        } catch (IOException e) {
            throw new SerializationException("Error deserializing Protobuf message for schema id " + schemaId
                    + " as " + descriptor.getFullName(), e);
        }
    }

    private ParsedSchema fetchSchema(String topic, int schemaId) {
        try {
            return schemaRegistryClient.getSchemaBySubjectAndId(subjectResolver.apply(topic), schemaId);
        } catch (IOException | RestClientException e) {
            throw new SerializationException("Error retrieving Protobuf schema for id " + schemaId, e);
        }
    }
}
