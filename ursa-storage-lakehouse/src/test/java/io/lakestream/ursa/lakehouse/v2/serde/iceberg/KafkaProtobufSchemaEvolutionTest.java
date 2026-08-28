/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.lakehouse.v2.serde.iceberg;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.confluent.kafka.schemaregistry.client.SchemaMetadata;
import io.lakestream.ursa.lakehouse.v2.iceberg.IcebergTableSchemaService;
import io.lakestream.ursa.materialization.serde.EntryEncoderContext;
import io.lakestream.ursa.materialization.serde.SchemaEvolutionManager;
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
import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag("lakehouse")
public class KafkaProtobufSchemaEvolutionTest {
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

    // Same messages but reordered (simulates Kafka protobuf serializer reordering)
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

    private KafkaEntryToIcebergRecordEncoder encoder;
    private MockedIcebergTableSchemaService mockedTableSchemaService;
    private KafkaSchemaService schemaService;
    private final EntryEncoderContext context = EntryEncoderContext.builder()
        .isVariantEnabled(false).build();

    static class MockedIcebergTableSchemaService extends IcebergTableSchemaService {
        public MockedIcebergTableSchemaService() {
            super(null, null);
        }

        @Getter
        final Set<Long> successfullyEvolvedVersions = new HashSet<>();
        @Getter
        private final SortedMap<Long, Schema> schemaWithVersions = new ConcurrentSkipListMap<>();

        @Override
        public Set<Long> evolveTableSchema(SortedMap<Long, Schema> schemaWithVersions) {
            this.schemaWithVersions.putAll(schemaWithVersions);
            if (successfullyEvolvedVersions.isEmpty()) {
                return schemaWithVersions.keySet();
            }
            return successfullyEvolvedVersions;
        }

        @Override
        public Schema getTableSchema(Long schemaVersion) {
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
        encoder = new KafkaEntryToIcebergRecordEncoder(schemaService);
        mockedTableSchemaService = new MockedIcebergTableSchemaService();
    }

    private EntryEncoderContext protobufContext(String messageName) {
        return EntryEncoderContext.builder()
            .isVariantEnabled(context.isVariantEnabled())
            .isPersistExtraMetadata(context.isPersistExtraMetadata())
            .protobufMessageName(Optional.of(messageName))
            .build();
    }

    // ========== convertSchemaMetadataToIcebergSchema0 tests ==========

    @Test
    void testConvertSchemaMetadataToIcebergSchema0_Protobuf_SingleMessage() {
        var schemaMetadata = new SchemaMetadata(1, 1, "PROTOBUF", null, PROTO_V1);

        Schema icebergSchema = encoder.convertSchemaMetadataToIcebergSchema0(
            schemaMetadata, protobufContext(USER_MESSAGE_NAME));

        assertNotNull(icebergSchema);
        assertNotNull(icebergSchema.findField("name"));
        assertNotNull(icebergSchema.findField("age"));
    }

    @Test
    void testConvertSchemaMetadataToIcebergSchema0_Protobuf_EvolvedSchema() {
        var schemaMetadataV1 = new SchemaMetadata(1, 1, "PROTOBUF", null, PROTO_V1);
        var schemaMetadataV2 = new SchemaMetadata(2, 2, "PROTOBUF", null, PROTO_V2);

        Schema icebergSchemaV1 = encoder.convertSchemaMetadataToIcebergSchema0(
            schemaMetadataV1, protobufContext(USER_MESSAGE_NAME));
        Schema icebergSchemaV2 = encoder.convertSchemaMetadataToIcebergSchema0(
            schemaMetadataV2, protobufContext(USER_MESSAGE_NAME));

        assertEquals(2, icebergSchemaV1.columns().size());
        assertEquals(3, icebergSchemaV2.columns().size());
        assertNotNull(icebergSchemaV2.findField("email"));
    }

    @Test
    void testConvertSchemaMetadataToIcebergSchema0_Protobuf_MultiMessage_SelectFirst() {
        var schemaMetadata = new SchemaMetadata(1, 1, "PROTOBUF", null, MULTI_MESSAGE_PROTO);

        Schema icebergSchema = encoder.convertSchemaMetadataToIcebergSchema0(
            schemaMetadata, protobufContext(USER_MESSAGE_NAME));

        assertNotNull(icebergSchema);
        assertNotNull(icebergSchema.findField("name"));
        assertNotNull(icebergSchema.findField("age"));
    }

    @Test
    void testConvertSchemaMetadataToIcebergSchema0_Protobuf_MultiMessage_SelectSecond() {
        var schemaMetadata = new SchemaMetadata(1, 1, "PROTOBUF", null, MULTI_MESSAGE_PROTO);

        Schema icebergSchema = encoder.convertSchemaMetadataToIcebergSchema0(
            schemaMetadata, protobufContext(ORDER_MESSAGE_NAME));

        assertNotNull(icebergSchema);
        assertNotNull(icebergSchema.findField("item"));
        assertNotNull(icebergSchema.findField("price"));
    }

    @Test
    void testConvertSchemaMetadataToIcebergSchema0_Protobuf_ReorderedProto_SameResultByName() {
        var originalMetadata = new SchemaMetadata(1, 1, "PROTOBUF", null, MULTI_MESSAGE_PROTO);
        var reorderedMetadata = new SchemaMetadata(2, 2, "PROTOBUF",
            null, MULTI_MESSAGE_PROTO_REORDERED);

        Schema schemaFromOriginal = encoder.convertSchemaMetadataToIcebergSchema0(
            originalMetadata, protobufContext(USER_MESSAGE_NAME));
        Schema schemaFromReordered = encoder.convertSchemaMetadataToIcebergSchema0(
            reorderedMetadata, protobufContext(USER_MESSAGE_NAME));

        // Both should produce schemas with the same fields for UserMessage
        assertEquals(schemaFromOriginal.columns().size(), schemaFromReordered.columns().size());
        assertNotNull(schemaFromOriginal.findField("name"));
        assertNotNull(schemaFromReordered.findField("name"));
        assertNotNull(schemaFromOriginal.findField("age"));
        assertNotNull(schemaFromReordered.findField("age"));
    }

    @Test
    void testConvertSchemaMetadataToIcebergSchema0_Protobuf_WithVariantEnabled() {
        var variantContext = EntryEncoderContext.builder()
            .isVariantEnabled(true)
            .isPersistExtraMetadata(true)
            .protobufMessageName(Optional.of(USER_MESSAGE_NAME))
            .build();
        var schemaMetadata = new SchemaMetadata(1, 1, "PROTOBUF", null, PROTO_V1);

        Schema icebergSchema = encoder.convertSchemaMetadataToIcebergSchema0(
            schemaMetadata, variantContext);

        assertNotNull(icebergSchema);
        // Should include the extra metadata field
        assertNotNull(icebergSchema.findField("__meta"));
    }

    @Test
    void testConvertSchemaMetadataToIcebergSchema0_Avro_StillWorks() {
        String avroSchema = "{\"type\":\"record\",\"name\":\"Test\","
            + "\"fields\":[{\"name\":\"id\",\"type\":\"int\"}]}";
        var schemaMetadata = new SchemaMetadata(1, 1, "AVRO", null, avroSchema);

        Schema icebergSchema = encoder.convertSchemaMetadataToIcebergSchema0(
            schemaMetadata, context);

        assertNotNull(icebergSchema);
        assertNotNull(icebergSchema.findField("id"));
    }

    // ========== evolveIcebergSchema tests ==========

    @Test
    void testEvolveIcebergSchema_Protobuf_NullTableSchemaService() throws Exception {
        var schemaMetadata = new SchemaMetadata(1, 1, "PROTOBUF", null, PROTO_V1);
        var schemaKey = SchemaKey.builder()
            .topicName("test-topic")
            .schemaVersion(1L)
            .convertedType(SchemaKey.ConvertedType.PROTOBUF_TO_ICEBERG_RECORD)
            .build();

        Schema inputSchema = new Schema(
            Types.NestedField.optional(1, "name", Types.StringType.get()),
            Types.NestedField.optional(2, "age", Types.IntegerType.get()));

        Schema result = encoder.evolveIcebergSchema(null, schemaKey, inputSchema,
            protobufContext(USER_MESSAGE_NAME));

        assertEquals(inputSchema, result);
    }

    @Test
    void testEvolveIcebergSchema_Protobuf_NonIcebergService() {
        var schemaMetadata = new SchemaMetadata(1, 1, "PROTOBUF", null, PROTO_V1);
        var nonIcebergService = mock(TableSchemaService.class);
        var schemaKey = SchemaKey.builder()
            .topicName("test-topic")
            .schemaVersion(1L)
            .build();
        Schema inputSchema = new Schema(
            Types.NestedField.optional(1, "name", Types.StringType.get()));

        assertThrows(IllegalArgumentException.class, () ->
            encoder.evolveIcebergSchema(nonIcebergService, schemaKey, inputSchema,
                protobufContext(USER_MESSAGE_NAME)));
    }

    @Test
    void testEvolveIcebergSchema_Protobuf_BasicEvolution() throws Exception {
        // Set up schema service to return protobuf schemas
        var topicSchemas = new TreeMap<Long, SchemaMetadata>();
        topicSchemas.put(1L, new SchemaMetadata(1, 1, "PROTOBUF", null, PROTO_V1));
        when(schemaService.getSchemaWithVersions("test-topic", 1L)).thenReturn(topicSchemas);

        var schemaMetadata = new SchemaMetadata(1, 1, "PROTOBUF", null, PROTO_V1);
        var schemaKey = SchemaKey.builder()
            .topicName("test-topic")
            .schemaVersion(1L)
            .convertedType(SchemaKey.ConvertedType.PROTOBUF_TO_ICEBERG_RECORD)
            .build();

        Schema inputSchema = new Schema(
            Types.NestedField.optional(1, "name", Types.StringType.get()),
            Types.NestedField.optional(2, "age", Types.IntegerType.get()));

        Schema result = encoder.evolveIcebergSchema(mockedTableSchemaService, schemaKey, inputSchema,
            protobufContext(USER_MESSAGE_NAME));

        assertNotNull(result);
        assertEquals(1, mockedTableSchemaService.getSchemaWithVersions().size());
        assertTrue(mockedTableSchemaService.getSchemaWithVersions().containsKey(1L));
    }

    @Test
    void testEvolveIcebergSchema_Protobuf_MultiVersionEvolution() throws Exception {
        // Set up schema service to return two protobuf schema versions
        var topicSchemas = new TreeMap<Long, SchemaMetadata>();
        topicSchemas.put(1L, new SchemaMetadata(1, 1, "PROTOBUF", null, PROTO_V1));
        topicSchemas.put(2L, new SchemaMetadata(2, 2, "PROTOBUF", null, PROTO_V2));
        when(schemaService.getSchemaWithVersions("test-topic", 2L)).thenReturn(topicSchemas);

        var schemaMetadata = new SchemaMetadata(2, 2, "PROTOBUF", null, PROTO_V2);
        var schemaKey = SchemaKey.builder()
            .topicName("test-topic")
            .schemaVersion(2L)
            .convertedType(SchemaKey.ConvertedType.PROTOBUF_TO_ICEBERG_RECORD)
            .build();

        Schema inputSchema = new Schema(
            Types.NestedField.optional(1, "name", Types.StringType.get()),
            Types.NestedField.optional(2, "age", Types.IntegerType.get()),
            Types.NestedField.optional(3, "email", Types.StringType.get()));

        Schema result = encoder.evolveIcebergSchema(mockedTableSchemaService, schemaKey, inputSchema,
            protobufContext(USER_MESSAGE_NAME));

        assertNotNull(result);
        // Both versions should be evolved
        assertEquals(2, mockedTableSchemaService.getSchemaWithVersions().size());
        assertTrue(mockedTableSchemaService.getSchemaWithVersions().containsKey(1L));
        assertTrue(mockedTableSchemaService.getSchemaWithVersions().containsKey(2L));

        // V1 should have 2 columns, V2 should have 3
        Schema evolvedV1 = mockedTableSchemaService.getTableSchema(1L);
        Schema evolvedV2 = mockedTableSchemaService.getTableSchema(2L);
        assertEquals(2, evolvedV1.columns().size());
        assertEquals(3, evolvedV2.columns().size());
    }

    @Test
    void testEvolveIcebergSchema_Protobuf_ReorderedMultiMessage() throws Exception {
        // V1: UserMessage is at index 0
        // V2 (reordered): UserMessage is at index 1
        // Both should produce the same schema when resolved by name
        var topicSchemas = new TreeMap<Long, SchemaMetadata>();
        topicSchemas.put(1L, new SchemaMetadata(1, 1, "PROTOBUF", null, MULTI_MESSAGE_PROTO));
        topicSchemas.put(2L, new SchemaMetadata(2, 2, "PROTOBUF",
            null, MULTI_MESSAGE_PROTO_REORDERED));
        when(schemaService.getSchemaWithVersions("test-topic", 2L)).thenReturn(topicSchemas);

        // Current message uses the reordered proto where UserMessage is at index 1
        var schemaMetadata = new SchemaMetadata(2, 2, "PROTOBUF",
            null, MULTI_MESSAGE_PROTO_REORDERED);
        var schemaKey = SchemaKey.builder()
            .topicName("test-topic")
            .schemaVersion(2L)
            .convertedType(SchemaKey.ConvertedType.PROTOBUF_TO_ICEBERG_RECORD)
            .build();

        Schema inputSchema = new Schema(
            Types.NestedField.optional(1, "name", Types.StringType.get()),
            Types.NestedField.optional(2, "age", Types.IntegerType.get()));

        // In reordered proto, UserMessage is at index 1
        Schema result = encoder.evolveIcebergSchema(mockedTableSchemaService, schemaKey, inputSchema,
            protobufContext(USER_MESSAGE_NAME));

        assertNotNull(result);
        assertEquals(2, mockedTableSchemaService.getSchemaWithVersions().size());

        // Both versions should resolve to UserMessage fields (name, age)
        Schema evolvedV1 = mockedTableSchemaService.getTableSchema(1L);
        Schema evolvedV2 = mockedTableSchemaService.getTableSchema(2L);
        assertNotNull(evolvedV1.findField("name"));
        assertNotNull(evolvedV1.findField("age"));
        assertNotNull(evolvedV2.findField("name"));
        assertNotNull(evolvedV2.findField("age"));
    }

    @Test
    void testEvolveIcebergSchema_Protobuf_ExistingSchemaSkipsEvolution() throws Exception {
        // Pre-populate with existing schema
        var existingSchema = new Schema(
            Types.NestedField.optional(1, "name", Types.StringType.get()),
            Types.NestedField.optional(2, "age", Types.IntegerType.get()));
        mockedTableSchemaService.getSchemaWithVersions().put(1L, existingSchema);

        var schemaMetadata = new SchemaMetadata(1, 1, "PROTOBUF", null, PROTO_V1);
        var schemaKey = SchemaKey.builder()
            .topicName("test-topic")
            .schemaVersion(1L)
            .convertedType(SchemaKey.ConvertedType.PROTOBUF_TO_ICEBERG_RECORD)
            .build();

        Schema result = encoder.evolveIcebergSchema(mockedTableSchemaService, schemaKey, existingSchema,
            protobufContext(USER_MESSAGE_NAME));

        // Should return existing schema without calling evolveTableSchema
        assertTrue(existingSchema.sameSchema(result));
        assertEquals(1, mockedTableSchemaService.getSchemaWithVersions().size());
    }

    // ========== SchemaEvolutionManager integration tests ==========

    @Test
    void testSchemaEvolutionManager_ProtobufMessageNamePropagated() throws Exception {
        var topicSchemas = new TreeMap<Long, SchemaMetadata>();
        topicSchemas.put(1L, new SchemaMetadata(1, 1, "PROTOBUF", null, PROTO_V1));
        when(schemaService.getSchemaWithVersions("test-topic", 1L)).thenReturn(topicSchemas);

        var schemaKey = SchemaKey.builder()
            .topicName("test-topic")
            .schemaVersion(1L)
            .build();

        var protobufContext = protobufContext(USER_MESSAGE_NAME);

        Schema result = SchemaEvolutionManager.<SchemaMetadata, Schema>evolveSchema(
            mockedTableSchemaService,
            schemaService,
            schemaKey,
            (metadata, ctx) -> {
                assertTrue(ctx.protobufMessageName().isPresent());
                assertEquals(USER_MESSAGE_NAME, ctx.protobufMessageName().get());
                return encoder.convertSchemaMetadataToIcebergSchema0(metadata, ctx);
            },
            protobufContext);

        assertNotNull(result);
    }

    @Test
    void testSchemaEvolutionManager_EmptyProtobufMessageNameForNonProtobuf() throws Exception {
        String avroSchema = "{\"type\":\"record\",\"name\":\"Test\","
            + "\"fields\":[{\"name\":\"id\",\"type\":\"int\"}]}";
        var topicSchemas = new TreeMap<Long, SchemaMetadata>();
        topicSchemas.put(1L, new SchemaMetadata(1, 1, "AVRO", null, avroSchema));
        when(schemaService.getSchemaWithVersions("test-topic", 1L)).thenReturn(topicSchemas);

        var schemaKey = SchemaKey.builder()
            .topicName("test-topic")
            .schemaVersion(1L)
            .build();

        Schema result = SchemaEvolutionManager.<SchemaMetadata, Schema>evolveSchema(
            mockedTableSchemaService,
            schemaService,
            schemaKey,
            (metadata, ctx) -> {
                assertFalse(ctx.protobufMessageName().isPresent());
                return encoder.convertSchemaMetadataToIcebergSchema0(metadata, ctx);
            },
            context);

        assertNotNull(result);
    }

    @Test
    void testConvertSchemaMetadataToIcebergSchema_Protobuf_SelectByName() {
        var schemaMetadata = new SchemaMetadata(1, 1, "PROTOBUF", null, MULTI_MESSAGE_PROTO);

        // Select UserMessage by name
        Schema userSchema = encoder.convertSchemaMetadataToIcebergSchema(
            schemaMetadata, protobufContext(USER_MESSAGE_NAME));
        assertNotNull(userSchema.findField("name"));
        assertNotNull(userSchema.findField("age"));

        // Select OrderMessage by name
        Schema orderSchema = encoder.convertSchemaMetadataToIcebergSchema(
            schemaMetadata, protobufContext(ORDER_MESSAGE_NAME));
        assertNotNull(orderSchema.findField("item"));
        assertNotNull(orderSchema.findField("price"));
    }

    private static void assertFalse(boolean condition) {
        org.junit.jupiter.api.Assertions.assertFalse(condition);
    }
}
