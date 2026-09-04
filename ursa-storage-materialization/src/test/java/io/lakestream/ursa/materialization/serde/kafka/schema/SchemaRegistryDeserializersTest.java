/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.materialization.serde.kafka.schema;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Timestamp;
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import java.nio.charset.StandardCharsets;
import java.util.List;
import org.apache.kafka.common.errors.SerializationException;
import org.junit.jupiter.api.Test;

class SchemaRegistryDeserializersTest {

    private static final String TOPIC = "events";
    private static final String SUBJECT = TOPIC + "-value";

    private final MockSchemaRegistryClient client = new MockSchemaRegistryClient(RawSchemaProvider.defaultProviders());

    @Test
    void deserializesJsonPayloadsToJacksonTrees() throws Exception {
        int id = client.register(SUBJECT, new RawParsedSchema("JSON", "{\"type\":\"object\"}", null, null, null, null));
        byte[] payload = SchemaRegistryWireFormat.frame(id, null,
                "{\"id\":7,\"name\":\"x\",\"nested\":{\"ok\":true}}".getBytes(StandardCharsets.UTF_8));

        JsonNode node = new SchemaRegistryJsonDeserializer().deserialize(TOPIC, payload);

        assertThat(node).isInstanceOf(ObjectNode.class);
        assertThat(node.get("id").intValue()).isEqualTo(7);
        assertThat(node.get("name").textValue()).isEqualTo("x");
        assertThat(node.get("nested").get("ok").booleanValue()).isTrue();
        assertThat(new SchemaRegistryJsonDeserializer().deserialize(TOPIC, null)).isNull();
    }

    @Test
    void rejectsJsonPayloadsWithBadFramingOrBody() {
        SchemaRegistryJsonDeserializer deserializer = new SchemaRegistryJsonDeserializer();
        assertThatThrownBy(() -> deserializer.deserialize(TOPIC, "{}".getBytes(StandardCharsets.UTF_8)))
                .isInstanceOf(SerializationException.class);
        assertThatThrownBy(() -> deserializer.deserialize(TOPIC,
                SchemaRegistryWireFormat.frame(1, null, "{not json".getBytes(StandardCharsets.UTF_8))))
                .isInstanceOf(SerializationException.class)
                .hasMessageContaining("schema id 1");
    }

    @Test
    void deserializesProtobufPayloadsUsingMessageIndexes() throws Exception {
        int id = client.register(SUBJECT,
                new RawParsedSchema("PROTOBUF", ProtobufSchemaDescriptorsTest.SCHEMA, null, null, null, null));
        Descriptor userType = ProtobufSchemaDescriptors.messageByName(ProtobufSchemaDescriptorsTest.SCHEMA, "User");
        Descriptor orderType = ProtobufSchemaDescriptors.messageByName(ProtobufSchemaDescriptorsTest.SCHEMA, "Order");
        Descriptor geoType = ProtobufSchemaDescriptors.messageByName(
                ProtobufSchemaDescriptorsTest.SCHEMA, "User.Address.Geo");

        DynamicMessage user = DynamicMessage.newBuilder(userType)
                .setField(userType.findFieldByName("name"), "alice")
                .setField(userType.findFieldByName("age"), 30)
                .setField(userType.findFieldByName("email"), "a@example.com")
                .setField(userType.findFieldByName("created_at"), Timestamp.newBuilder().setSeconds(12).build())
                .build();
        DynamicMessage order = DynamicMessage.newBuilder(orderType)
                .setField(orderType.findFieldByName("item"), "book")
                .setField(orderType.findFieldByName("price"), 9.5d)
                .build();
        DynamicMessage geo = DynamicMessage.newBuilder(geoType)
                .setField(geoType.findFieldByName("lat"), 1.5d)
                .build();

        SchemaRegistryProtobufDeserializer deserializer =
                new SchemaRegistryProtobufDeserializer(client, topic -> topic + "-value");

        DynamicMessage decodedUser = deserializer.deserialize(TOPIC, SchemaRegistryWireFormat.frame(id,
                SchemaRegistryWireFormat.writeMessageIndexes(List.of(0)), user.toByteArray()));
        assertThat(decodedUser.getDescriptorForType().getFullName()).isEqualTo("test.schema.User");
        assertThat(decodedUser.getField(userType.findFieldByName("name"))).isEqualTo("alice");
        assertThat(decodedUser.getField(userType.findFieldByName("age"))).isEqualTo(30);
        assertThat(decodedUser.getField(userType.findFieldByName("email"))).isEqualTo("a@example.com");
        assertThat(((DynamicMessage) decodedUser.getField(userType.findFieldByName("created_at")))
                .getField(Timestamp.getDescriptor().findFieldByName("seconds"))).isEqualTo(12L);

        DynamicMessage decodedOrder = deserializer.deserialize(TOPIC, SchemaRegistryWireFormat.frame(id,
                SchemaRegistryWireFormat.writeMessageIndexes(List.of(1)), order.toByteArray()));
        assertThat(decodedOrder.getDescriptorForType().getFullName()).isEqualTo("test.schema.Order");
        assertThat(decodedOrder.getField(orderType.findFieldByName("price"))).isEqualTo(9.5d);

        DynamicMessage decodedGeo = deserializer.deserialize(TOPIC, SchemaRegistryWireFormat.frame(id,
                SchemaRegistryWireFormat.writeMessageIndexes(List.of(0, 0, 0)), geo.toByteArray()));
        assertThat(decodedGeo.getDescriptorForType().getFullName()).isEqualTo("test.schema.User.Address.Geo");
        assertThat(decodedGeo.getField(geoType.findFieldByName("lat"))).isEqualTo(1.5d);

        assertThat(deserializer.deserialize(TOPIC, null)).isNull();
    }

    @Test
    void rejectsProtobufPayloadsWithUnknownSchemaOrWrongType() throws Exception {
        int jsonId = client.register(SUBJECT, new RawParsedSchema("JSON", "{\"type\":\"object\"}", null, null, null, null));
        SchemaRegistryProtobufDeserializer deserializer =
                new SchemaRegistryProtobufDeserializer(client, topic -> topic + "-value");
        byte[] indexes = SchemaRegistryWireFormat.writeMessageIndexes(List.of(0));

        assertThatThrownBy(() -> deserializer.deserialize(TOPIC, SchemaRegistryWireFormat.frame(jsonId, indexes,
                new byte[0])))
                .isInstanceOf(SerializationException.class)
                .hasMessageContaining("expected PROTOBUF");
        assertThatThrownBy(() -> deserializer.deserialize(TOPIC, SchemaRegistryWireFormat.frame(9999, indexes,
                new byte[0])))
                .isInstanceOf(SerializationException.class)
                .hasMessageContaining("9999");

        int protoId = client.register("other-value",
                new RawParsedSchema("PROTOBUF", ProtobufSchemaDescriptorsTest.SCHEMA, null, null, null, null));
        assertThatThrownBy(() -> deserializer.deserialize("other", SchemaRegistryWireFormat.frame(protoId,
                SchemaRegistryWireFormat.writeMessageIndexes(List.of(5)), new byte[0])))
                .isInstanceOf(SerializationException.class)
                .hasMessageContaining("resolving Protobuf message");
    }
}
