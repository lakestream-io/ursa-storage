/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.materialization.serde.kafka.schema;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.SchemaProvider;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaMetadata;
import io.confluent.kafka.schemaregistry.client.rest.entities.Schema;
import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaReference;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.Test;

class RawSchemaProviderTest {

    private static final String JSON_SCHEMA = "{\"type\":\"object\",\"properties\":{\"id\":{\"type\":\"integer\"}}}";
    private static final String PROTO_SCHEMA = "syntax = \"proto3\";\nmessage Event { string id = 1; }\n";

    @Test
    void providersCoverEveryReadableSchemaType() {
        List<SchemaProvider> providers = RawSchemaProvider.defaultProviders();
        assertThat(providers).extracting(SchemaProvider::schemaType).containsExactly("AVRO", "JSON", "PROTOBUF");
    }

    @Test
    void parsesRegistryResponsesIntoRawSchemas() {
        SchemaReference reference = new SchemaReference("other.proto", "other", 3);
        Schema response = new Schema("events-value", 2, 17, "PROTOBUF", List.of(reference), PROTO_SCHEMA);

        Optional<ParsedSchema> parsed = RawSchemaProvider.protobuf().parseSchema(response, false, false);

        assertThat(parsed).isPresent();
        ParsedSchema schema = parsed.get();
        assertThat(schema).isInstanceOf(RawParsedSchema.class);
        assertThat(schema.schemaType()).isEqualTo("PROTOBUF");
        assertThat(schema.canonicalString()).isEqualTo(PROTO_SCHEMA);
        assertThat(schema.rawSchema()).isEqualTo(PROTO_SCHEMA);
        assertThat(schema.references()).containsExactly(reference);
        assertThat(schema.version()).isEqualTo(2);
        assertThat(schema.copy(5).version()).isEqualTo(5);
        assertThat(schema.copy()).isEqualTo(schema).hasSameHashCodeAs(schema);
        assertThat(schema).isNotEqualTo(new RawParsedSchema("JSON", PROTO_SCHEMA, List.of(), null, null, 2));
    }

    @Test
    void rejectsResponsesWithoutSchemaText() {
        Schema response = new Schema("events-value", 1, 1, "JSON", List.of(), null);
        assertThat(RawSchemaProvider.json().parseSchema(response, false, false)).isEmpty();
        assertThatThrownBy(() -> RawSchemaProvider.json().parseSchemaOrElseThrow(response, false, false))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void unsupportedOperationsFailLoudly() {
        RawParsedSchema schema = new RawParsedSchema("JSON", JSON_SCHEMA, null, null, null, null);
        assertThatThrownBy(() -> schema.isBackwardCompatible(schema)).isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(() -> schema.copy(Map.of(), Map.of())).isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void worksWithTheSchemaRegistryClient() throws Exception {
        MockSchemaRegistryClient client = new MockSchemaRegistryClient(RawSchemaProvider.defaultProviders());

        int jsonId = client.register("json-value", new RawParsedSchema("JSON", JSON_SCHEMA, null, null, null, null));
        int protoId = client.register("proto-value",
                new RawParsedSchema("PROTOBUF", PROTO_SCHEMA, null, null, null, null));
        int avroId = client.register("avro-value", new AvroSchema("\"string\""));

        ParsedSchema json = client.getSchemaBySubjectAndId("json-value", jsonId);
        assertThat(json.schemaType()).isEqualTo("JSON");
        assertThat(json.canonicalString()).isEqualTo(JSON_SCHEMA);

        ParsedSchema proto = client.getSchemaBySubjectAndId("proto-value", protoId);
        assertThat(proto.schemaType()).isEqualTo("PROTOBUF");
        assertThat(proto.canonicalString()).isEqualTo(PROTO_SCHEMA);
        assertThat(client.getVersion("proto-value", proto)).isEqualTo(1);

        SchemaMetadata metadata = client.getSchemaMetadata("proto-value", 1);
        assertThat(metadata.getSchemaType()).isEqualTo("PROTOBUF");
        assertThat(metadata.getSchema()).isEqualTo(PROTO_SCHEMA);

        assertThat(client.getSchemaBySubjectAndId("avro-value", avroId)).isInstanceOf(AvroSchema.class);
    }
}
