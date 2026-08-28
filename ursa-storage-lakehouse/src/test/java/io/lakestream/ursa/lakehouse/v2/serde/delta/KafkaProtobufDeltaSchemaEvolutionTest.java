/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.lakehouse.v2.serde.delta;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.confluent.kafka.schemaregistry.client.SchemaMetadata;
import io.delta.kernel.types.StructType;
import io.lakestream.ursa.lakehouse.v2.delta.DeltaTableSchemaService;
import io.lakestream.ursa.materialization.serde.EntryEncoderContext;
import io.lakestream.ursa.materialization.serde.SchemaKey;
import io.lakestream.ursa.materialization.serde.TableSchemaService;
import io.lakestream.ursa.materialization.serde.kafka.KafkaSchemaService;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentSkipListMap;
import lombok.Getter;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag("lakehouse")
public class KafkaProtobufDeltaSchemaEvolutionTest {
    private static final String USER_MESSAGE_NAME = "test.evolution.UserMessage";
    private static final String ORDER_MESSAGE_NAME = "test.evolution.OrderMessage";

    private static final String PROTO_V1 = "syntax = \"proto3\";\n"
        + "package test.evolution;\n"
        + "option java_package = \"io.lakestream.ursa.lakehouse.test\";\n"
        + "option java_outer_classname = \"EvolutionProtoV1\";\n"
        + "message UserMessage {\n"
        + "    string name = 1;\n"
        + "    int32 age = 2;\n"
        + "}";

    private static final String PROTO_V2 = "syntax = \"proto3\";\n"
        + "package test.evolution;\n"
        + "option java_package = \"io.lakestream.ursa.lakehouse.test\";\n"
        + "option java_outer_classname = \"EvolutionProtoV2\";\n"
        + "message UserMessage {\n"
        + "    string name = 1;\n"
        + "    int32 age = 2;\n"
        + "    string email = 3;\n"
        + "}";

    private static final String MULTI_MESSAGE_PROTO = "syntax = \"proto3\";\n"
        + "package test.evolution;\n"
        + "option java_package = \"io.lakestream.ursa.lakehouse.test\";\n"
        + "option java_outer_classname = \"MultiMessageProto\";\n"
        + "message UserMessage {\n"
        + "    string name = 1;\n"
        + "    int32 age = 2;\n"
        + "}\n"
        + "message OrderMessage {\n"
        + "    string item = 1;\n"
        + "    double price = 2;\n"
        + "}";

    private static final String MULTI_MESSAGE_PROTO_REORDERED = "syntax = \"proto3\";\n"
        + "package test.evolution;\n"
        + "option java_package = \"io.lakestream.ursa.lakehouse.test\";\n"
        + "option java_outer_classname = \"MultiMessageProtoReordered\";\n"
        + "message OrderMessage {\n"
        + "    string item = 1;\n"
        + "    double price = 2;\n"
        + "}\n"
        + "message UserMessage {\n"
        + "    string name = 1;\n"
        + "    int32 age = 2;\n"
        + "}";

    private KafkaEntryToDeltaRecordEncoder encoder;
    private MockedDeltaTableSchemaService mockedTableSchemaService;
    private KafkaSchemaService schemaService;
    private final EntryEncoderContext context = EntryEncoderContext.builder()
        .isVariantEnabled(false).build();

    static class MockedDeltaTableSchemaService extends DeltaTableSchemaService {
        public MockedDeltaTableSchemaService() {
            super(null);
        }

        @Getter
        final Set<Long> successfullyEvolvedVersions = new HashSet<>();
        @Getter
        private final SortedMap<Long, StructType> schemaWithVersions = new ConcurrentSkipListMap<>();

        @Override
        public Set<Long> evolveTableSchema(SortedMap<Long, StructType> schemaWithVersions) {
            this.schemaWithVersions.putAll(schemaWithVersions);
            if (successfullyEvolvedVersions.isEmpty()) {
                return schemaWithVersions.keySet();
            }
            return successfullyEvolvedVersions;
        }

        @Override
        public StructType getTableSchema(Long schemaVersion) {
            return schemaWithVersions.get(schemaVersion);
        }

        @Override
        public Long getLatestSchemaVersion() throws Exception {
            return schemaWithVersions.isEmpty() ? -1L : schemaWithVersions.lastKey();
        }
    }

    @BeforeEach
    void setUp() {
        schemaService = mock(KafkaSchemaService.class);
        encoder = new KafkaEntryToDeltaRecordEncoder(schemaService);
        mockedTableSchemaService = new MockedDeltaTableSchemaService();
    }

    private EntryEncoderContext protobufContext(String messageName) {
        return EntryEncoderContext.builder()
            .isVariantEnabled(context.isVariantEnabled())
            .isPersistExtraMetadata(context.isPersistExtraMetadata())
            .protobufMessageName(Optional.of(messageName))
            .build();
    }

    // ========== convertSchemaMetadataToDeltaSchema0 tests ==========

    @Test
    void testConvertSchemaMetadataToDeltaSchema0_Protobuf_SingleMessage() {
        var schemaMetadata = new SchemaMetadata(1, 1, "PROTOBUF", null, PROTO_V1);

        StructType deltaSchema = encoder.convertSchemaMetadataToDeltaSchema0(
            schemaMetadata, protobufContext(USER_MESSAGE_NAME));

        assertNotNull(deltaSchema);
        assertNotNull(deltaSchema.get("name"));
        assertNotNull(deltaSchema.get("age"));
    }

    @Test
    void testConvertSchemaMetadataToDeltaSchema0_Protobuf_EvolvedSchema() {
        StructType deltaSchemaV1 = encoder.convertSchemaMetadataToDeltaSchema0(
            new SchemaMetadata(1, 1, "PROTOBUF", null, PROTO_V1), protobufContext(USER_MESSAGE_NAME));
        StructType deltaSchemaV2 = encoder.convertSchemaMetadataToDeltaSchema0(
            new SchemaMetadata(2, 2, "PROTOBUF", null, PROTO_V2), protobufContext(USER_MESSAGE_NAME));

        assertEquals(2, deltaSchemaV1.length());
        assertEquals(3, deltaSchemaV2.length());
        assertNotNull(deltaSchemaV2.get("email"));
    }

    @Test
    void testConvertSchemaMetadataToDeltaSchema0_Protobuf_MultiMessage_SelectByName() {
        var schemaMetadata = new SchemaMetadata(1, 1, "PROTOBUF", null, MULTI_MESSAGE_PROTO);

        StructType userSchema = encoder.convertSchemaMetadataToDeltaSchema0(
            schemaMetadata, protobufContext(USER_MESSAGE_NAME));
        assertNotNull(userSchema.get("name"));
        assertNotNull(userSchema.get("age"));

        StructType orderSchema = encoder.convertSchemaMetadataToDeltaSchema0(
            schemaMetadata, protobufContext(ORDER_MESSAGE_NAME));
        assertNotNull(orderSchema.get("item"));
        assertNotNull(orderSchema.get("price"));
    }

    @Test
    void testConvertSchemaMetadataToDeltaSchema0_Protobuf_ReorderedProto() {
        StructType fromOriginal = encoder.convertSchemaMetadataToDeltaSchema0(
            new SchemaMetadata(1, 1, "PROTOBUF", null, MULTI_MESSAGE_PROTO),
            protobufContext(USER_MESSAGE_NAME));
        StructType fromReordered = encoder.convertSchemaMetadataToDeltaSchema0(
            new SchemaMetadata(2, 2, "PROTOBUF", null, MULTI_MESSAGE_PROTO_REORDERED),
            protobufContext(USER_MESSAGE_NAME));

        assertEquals(fromOriginal.length(), fromReordered.length());
        assertNotNull(fromOriginal.get("name"));
        assertNotNull(fromReordered.get("name"));
        assertNotNull(fromOriginal.get("age"));
        assertNotNull(fromReordered.get("age"));
    }

    @Test
    void testConvertSchemaMetadataToDeltaSchema0_Avro_StillWorks() {
        String avroSchema = "{\"type\":\"record\",\"name\":\"Test\","
            + "\"fields\":[{\"name\":\"id\",\"type\":\"int\"}]}";
        var schemaMetadata = new SchemaMetadata(1, 1, "AVRO", null, avroSchema);

        StructType deltaSchema = encoder.convertSchemaMetadataToDeltaSchema0(
            schemaMetadata, context);

        assertNotNull(deltaSchema);
        assertNotNull(deltaSchema.get("id"));
    }

    // ========== evolveDeltaSchema tests ==========

    @Test
    void testEvolveDeltaSchema_Protobuf_NullTableSchemaService() throws Exception {
        var schemaMetadata = new SchemaMetadata(1, 1, "PROTOBUF", null, PROTO_V1);
        var schemaKey = SchemaKey.builder()
            .topicName("test-topic")
            .schemaVersion(1L)
            .build();

        // Should not throw, just return early
        encoder.evolveDeltaSchema(null, schemaKey, protobufContext(USER_MESSAGE_NAME));
    }

    @Test
    void testEvolveDeltaSchema_Protobuf_NonDeltaService() {
        var nonDeltaService = mock(TableSchemaService.class);
        var schemaKey = SchemaKey.builder()
            .topicName("test-topic")
            .schemaVersion(1L)
            .build();

        assertThrows(IllegalArgumentException.class, () ->
            encoder.evolveDeltaSchema(nonDeltaService, schemaKey,
                protobufContext(USER_MESSAGE_NAME)));
    }

    @Test
    void testEvolveDeltaSchema_Protobuf_BasicEvolution() throws Exception {
        var topicSchemas = new TreeMap<Long, SchemaMetadata>();
        topicSchemas.put(1L, new SchemaMetadata(1, 1, "PROTOBUF", null, PROTO_V1));
        when(schemaService.getSchemaWithVersions("test-topic", 1L)).thenReturn(topicSchemas);

        var schemaKey = SchemaKey.builder()
            .topicName("test-topic")
            .schemaVersion(1L)
            .build();

        encoder.evolveDeltaSchema(mockedTableSchemaService, schemaKey,
            protobufContext(USER_MESSAGE_NAME));

        assertEquals(1, mockedTableSchemaService.getSchemaWithVersions().size());
        StructType evolved = mockedTableSchemaService.getTableSchema(1L);
        assertNotNull(evolved);
        assertNotNull(evolved.get("name"));
        assertNotNull(evolved.get("age"));
    }

    @Test
    void testEvolveDeltaSchema_Protobuf_MultiVersionEvolution() throws Exception {
        var topicSchemas = new TreeMap<Long, SchemaMetadata>();
        topicSchemas.put(1L, new SchemaMetadata(1, 1, "PROTOBUF", null, PROTO_V1));
        topicSchemas.put(2L, new SchemaMetadata(2, 2, "PROTOBUF", null, PROTO_V2));
        when(schemaService.getSchemaWithVersions("test-topic", 2L)).thenReturn(topicSchemas);

        var schemaKey = SchemaKey.builder()
            .topicName("test-topic")
            .schemaVersion(2L)
            .build();

        encoder.evolveDeltaSchema(mockedTableSchemaService, schemaKey,
            protobufContext(USER_MESSAGE_NAME));

        assertEquals(2, mockedTableSchemaService.getSchemaWithVersions().size());
        StructType evolvedV1 = mockedTableSchemaService.getTableSchema(1L);
        StructType evolvedV2 = mockedTableSchemaService.getTableSchema(2L);
        assertEquals(2, evolvedV1.length());
        assertEquals(3, evolvedV2.length());
        assertNotNull(evolvedV2.get("email"));
    }

    @Test
    void testEvolveDeltaSchema_Protobuf_ReorderedMultiMessage() throws Exception {
        var topicSchemas = new TreeMap<Long, SchemaMetadata>();
        topicSchemas.put(1L, new SchemaMetadata(1, 1, "PROTOBUF", null, MULTI_MESSAGE_PROTO));
        topicSchemas.put(2L, new SchemaMetadata(2, 2, "PROTOBUF",
            null, MULTI_MESSAGE_PROTO_REORDERED));
        when(schemaService.getSchemaWithVersions("test-topic", 2L)).thenReturn(topicSchemas);

        var schemaKey = SchemaKey.builder()
            .topicName("test-topic")
            .schemaVersion(2L)
            .build();

        encoder.evolveDeltaSchema(mockedTableSchemaService, schemaKey,
            protobufContext(USER_MESSAGE_NAME));

        assertEquals(2, mockedTableSchemaService.getSchemaWithVersions().size());

        // Both versions should resolve to UserMessage
        StructType evolvedV1 = mockedTableSchemaService.getTableSchema(1L);
        StructType evolvedV2 = mockedTableSchemaService.getTableSchema(2L);
        assertNotNull(evolvedV1.get("name"));
        assertNotNull(evolvedV1.get("age"));
        assertNotNull(evolvedV2.get("name"));
        assertNotNull(evolvedV2.get("age"));
    }

    @Test
    void testEvolveDeltaSchema_Protobuf_ExistingSchemaSkipsEvolution() throws Exception {
        // Pre-populate
        var existingSchema = encoder.convertSchemaMetadataToDeltaSchema0(
            new SchemaMetadata(1, 1, "PROTOBUF", null, PROTO_V1),
            protobufContext(USER_MESSAGE_NAME));
        mockedTableSchemaService.getSchemaWithVersions().put(1L, existingSchema);

        var schemaKey = SchemaKey.builder()
            .topicName("test-topic")
            .schemaVersion(1L)
            .build();

        encoder.evolveDeltaSchema(mockedTableSchemaService, schemaKey,
            protobufContext(USER_MESSAGE_NAME));

        // Should not add new entries
        assertEquals(1, mockedTableSchemaService.getSchemaWithVersions().size());
    }

    @Test
    void testConvertSchemaMetadataToDeltaSchema_Protobuf_SelectByName() {
        var schemaMetadata = new SchemaMetadata(1, 1, "PROTOBUF", null, MULTI_MESSAGE_PROTO);

        StructType userSchema = encoder.convertSchemaMetadataToDeltaSchema(
            schemaMetadata, protobufContext(USER_MESSAGE_NAME));
        assertNotNull(userSchema.get("name"));
        assertNotNull(userSchema.get("age"));

        StructType orderSchema = encoder.convertSchemaMetadataToDeltaSchema(
            schemaMetadata, protobufContext(ORDER_MESSAGE_NAME));
        assertNotNull(orderSchema.get("item"));
        assertNotNull(orderSchema.get("price"));
    }
}
