/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.clickhouse.serde;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.protobuf.ByteString;
import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

/**
 * Comprehensive unit tests for {@link ClickHouseRowConverter} — the schema-decoded-record →
 * ClickHouse-row mapping that the Kafka ClickHouse encoder delegates to. Covers every value
 * branch for each supported source schema type: Avro records, JSON documents, protobuf messages and
 * primitive scalars.
 */
class ClickHouseRowConverterTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    // ---------------------------------------------------------------------------------------------
    // Avro (fromAvro + the shared toJavaValue normalisation)
    // ---------------------------------------------------------------------------------------------
    @Nested
    class Avro {

        @Test
        void mapsAllScalarTypesPreservingSchemaFieldOrder() {
            Schema schema = SchemaBuilder.record("Event").fields()
                    .requiredString("name")
                    .requiredInt("i32")
                    .requiredLong("i64")
                    .requiredFloat("f32")
                    .requiredDouble("f64")
                    .requiredBoolean("active")
                    .endRecord();
            GenericRecord record = new GenericData.Record(schema);
            record.put("name", new Utf8("alice"));
            record.put("i32", 30);
            record.put("i64", 9_000_000_000L);
            record.put("f32", 1.5f);
            record.put("f64", 2.5d);
            record.put("active", true);

            Map<String, Object> row = ClickHouseRowConverter.fromAvro(record);

            assertThat(row.keySet()).containsExactly("name", "i32", "i64", "f32", "f64", "active");
            assertThat(row.get("name")).isEqualTo("alice");
            assertThat(row.get("i32")).isEqualTo(30);
            assertThat(row.get("i64")).isEqualTo(9_000_000_000L);
            assertThat(row.get("f32")).isEqualTo(1.5f);
            assertThat(row.get("f64")).isEqualTo(2.5d);
            assertThat(row.get("active")).isEqualTo(true);
        }

        @Test
        void normalisesUtf8AndCharSequenceToString() {
            Schema schema = SchemaBuilder.record("S").fields()
                    .requiredString("utf8")
                    .requiredString("str")
                    .endRecord();
            GenericRecord record = new GenericData.Record(schema);
            record.put("utf8", new Utf8("u"));
            record.put("str", "s");

            Map<String, Object> row = ClickHouseRowConverter.fromAvro(record);

            assertThat(row.get("utf8")).isInstanceOf(String.class).isEqualTo("u");
            assertThat(row.get("str")).isInstanceOf(String.class).isEqualTo("s");
        }

        @Test
        void convertsByteBufferToByteArrayRespectingPosition() {
            Schema schema = SchemaBuilder.record("B").fields().requiredBytes("data").endRecord();
            GenericRecord record = new GenericData.Record(schema);
            ByteBuffer buffer = ByteBuffer.wrap(new byte[] {1, 2, 3, 4});
            buffer.position(2); // only the remaining {3, 4} must be read
            record.put("data", buffer);

            Map<String, Object> row = ClickHouseRowConverter.fromAvro(record);

            assertThat(row.get("data")).isInstanceOf(byte[].class);
            assertThat((byte[]) row.get("data")).containsExactly(3, 4);
        }

        @Test
        void leavesNullAsNull() {
            Schema schema = SchemaBuilder.record("N").fields()
                    .name("name").type().nullable().stringType().noDefault()
                    .endRecord();
            GenericRecord record = new GenericData.Record(schema);
            record.put("name", null);

            Map<String, Object> row = ClickHouseRowConverter.fromAvro(record);

            assertThat(row).containsKey("name");
            assertThat(row.get("name")).isNull();
        }

        @Test
        void stringifiesComplexValues() {
            Schema nestedSchema = SchemaBuilder.record("Nested").fields().requiredString("city").endRecord();
            Schema enumSchema = SchemaBuilder.enumeration("Color").symbols("RED", "GREEN");
            Schema schema = SchemaBuilder.record("Complex").fields()
                    .name("nested").type(nestedSchema).noDefault()
                    .name("tags").type().array().items().stringType().noDefault()
                    .name("color").type(enumSchema).noDefault()
                    .endRecord();
            GenericRecord nested = new GenericData.Record(nestedSchema);
            nested.put("city", "Munich");
            GenericData.EnumSymbol green = new GenericData.EnumSymbol(enumSchema, "GREEN");
            List<String> tags = List.of("a", "b");
            GenericRecord record = new GenericData.Record(schema);
            record.put("nested", nested);
            record.put("tags", tags);
            record.put("color", green);

            Map<String, Object> row = ClickHouseRowConverter.fromAvro(record);

            // Records, arrays and enums are handed to ClickHouse as their string form.
            assertThat(row.get("nested")).isInstanceOf(String.class).isEqualTo(nested.toString());
            assertThat(row.get("tags")).isInstanceOf(String.class).isEqualTo(tags.toString());
            assertThat(row.get("color")).isInstanceOf(String.class).isEqualTo("GREEN");
        }
    }

    // ---------------------------------------------------------------------------------------------
    // JSON (fromJson + jsonToJavaValue)
    // ---------------------------------------------------------------------------------------------
    @Nested
    class Json {

        @Test
        void mapsEveryScalarNodeTypeToItsJavaType() {
            ObjectNode node = MAPPER.createObjectNode();
            node.put("i", 42);                                      // IntNode
            node.put("l", 9_999_999_999L);                          // LongNode
            node.put("d", 1.5d);                                    // DoubleNode
            node.set("f", MAPPER.getNodeFactory().numberNode(2.5f)); // FloatNode
            node.put("b", false);                                   // BooleanNode
            node.put("s", "bob");                                   // TextNode
            node.putNull("nil");                                    // NullNode
            node.set("sh", MAPPER.getNodeFactory().numberNode((short) 7)); // ShortNode
            node.set("bi", MAPPER.getNodeFactory()
                    .numberNode(new BigInteger("123456789012345678901234567890"))); // BigIntegerNode
            node.set("bd", MAPPER.getNodeFactory().numberNode(new BigDecimal("1.50"))); // DecimalNode

            Map<String, Object> row = ClickHouseRowConverter.fromJson(node);

            assertThat(row.get("i")).isEqualTo(42);
            assertThat(row.get("l")).isEqualTo(9_999_999_999L);
            assertThat(row.get("d")).isEqualTo(1.5d);
            // float nodes are widened to double by the converter.
            assertThat(row.get("f")).isEqualTo((double) 2.5f);
            assertThat(row.get("b")).isEqualTo(false);
            assertThat(row.get("s")).isEqualTo("bob");
            assertThat(row.get("nil")).isNull();
            assertThat(row.get("sh")).isEqualTo((short) 7);
            assertThat(row.get("bi")).isEqualTo(new BigInteger("123456789012345678901234567890"));
            assertThat(row.get("bd")).isEqualTo(new BigDecimal("1.50"));
        }

        @Test
        void stringifiesNestedObjectAndArray() throws Exception {
            var node = MAPPER.readTree("{\"address\":{\"city\":\"Munich\"},\"tags\":[1,2]}");

            Map<String, Object> row = ClickHouseRowConverter.fromJson(node);

            // Nested objects / arrays are handed to ClickHouse as their JSON string form.
            assertThat(row.get("address")).isEqualTo("{\"city\":\"Munich\"}");
            assertThat(row.get("tags")).isEqualTo("[1,2]");
        }

        @Test
        void preservesDocumentFieldOrder() throws Exception {
            var node = MAPPER.readTree("{\"z\":1,\"a\":2,\"m\":3}");

            Map<String, Object> row = ClickHouseRowConverter.fromJson(node);

            assertThat(row.keySet()).containsExactly("z", "a", "m");
        }
    }

    // ---------------------------------------------------------------------------------------------
    // Protobuf (fromProtobuf + protobufToJavaValue)
    // ---------------------------------------------------------------------------------------------
    @Nested
    class Protobuf {

        @Test
        void mapsAllScalarJavaTypesPreservingFieldOrder() throws Exception {
            Descriptors.Descriptor event = eventDescriptor();
            DynamicMessage message = DynamicMessage.newBuilder(event)
                    .setField(event.findFieldByName("i32"), 1)
                    .setField(event.findFieldByName("i64"), 2L)
                    .setField(event.findFieldByName("f32"), 1.5f)
                    .setField(event.findFieldByName("f64"), 2.5d)
                    .setField(event.findFieldByName("flag"), true)
                    .setField(event.findFieldByName("name"), "alice")
                    .setField(event.findFieldByName("payload"), ByteString.copyFromUtf8("blob"))
                    .build();

            Map<String, Object> row = ClickHouseRowConverter.fromProtobuf(message);

            assertThat(row.keySet())
                    .containsExactly("i32", "i64", "f32", "f64", "flag", "name", "payload",
                            "color", "nested", "tags");
            assertThat(row.get("i32")).isEqualTo(1);
            assertThat(row.get("i64")).isEqualTo(2L);
            assertThat(row.get("f32")).isEqualTo(1.5f);
            assertThat(row.get("f64")).isEqualTo(2.5d);
            assertThat(row.get("flag")).isEqualTo(true);
            assertThat(row.get("name")).isEqualTo("alice");
            assertThat(row.get("payload")).isEqualTo("blob".getBytes(StandardCharsets.UTF_8));
        }

        @Test
        void convertsEnumToItsName() throws Exception {
            Descriptors.Descriptor event = eventDescriptor();
            Descriptors.EnumDescriptor color =
                    event.getFile().findEnumTypeByName("Color");
            DynamicMessage message = DynamicMessage.newBuilder(event)
                    .setField(event.findFieldByName("color"), color.findValueByName("GREEN"))
                    .build();

            Map<String, Object> row = ClickHouseRowConverter.fromProtobuf(message);

            assertThat(row.get("color")).isEqualTo("GREEN");
        }

        @Test
        void stringifiesNestedMessage() throws Exception {
            Descriptors.Descriptor event = eventDescriptor();
            Descriptors.Descriptor nestedType = event.getFile().findMessageTypeByName("Nested");
            DynamicMessage nested = DynamicMessage.newBuilder(nestedType)
                    .setField(nestedType.findFieldByName("city"), "Munich")
                    .build();
            DynamicMessage message = DynamicMessage.newBuilder(event)
                    .setField(event.findFieldByName("nested"), nested)
                    .build();

            Map<String, Object> row = ClickHouseRowConverter.fromProtobuf(message);

            assertThat(row.get("nested")).isInstanceOf(String.class);
            assertThat((String) row.get("nested")).contains("Munich");
        }

        @Test
        void stringifiesRepeatedField() throws Exception {
            Descriptors.Descriptor event = eventDescriptor();
            DynamicMessage message = DynamicMessage.newBuilder(event)
                    .addRepeatedField(event.findFieldByName("tags"), "x")
                    .addRepeatedField(event.findFieldByName("tags"), "y")
                    .build();

            Map<String, Object> row = ClickHouseRowConverter.fromProtobuf(message);

            assertThat(row.get("tags")).isInstanceOf(String.class).isEqualTo("[x, y]");
        }

        /** proto3 {@code Event} with one field per JavaType, plus an enum, a nested message and a list. */
        private Descriptors.Descriptor eventDescriptor() throws Exception {
            DescriptorProtos.EnumDescriptorProto color = DescriptorProtos.EnumDescriptorProto.newBuilder()
                    .setName("Color")
                    .addValue(DescriptorProtos.EnumValueDescriptorProto.newBuilder()
                            .setName("RED").setNumber(0))
                    .addValue(DescriptorProtos.EnumValueDescriptorProto.newBuilder()
                            .setName("GREEN").setNumber(1))
                    .build();
            DescriptorProtos.DescriptorProto nested = DescriptorProtos.DescriptorProto.newBuilder()
                    .setName("Nested")
                    .addField(field("city", 1, DescriptorProtos.FieldDescriptorProto.Type.TYPE_STRING,
                            DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL, null))
                    .build();
            DescriptorProtos.DescriptorProto eventProto = DescriptorProtos.DescriptorProto.newBuilder()
                    .setName("Event")
                    .addField(field("i32", 1, DescriptorProtos.FieldDescriptorProto.Type.TYPE_INT32,
                            DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL, null))
                    .addField(field("i64", 2, DescriptorProtos.FieldDescriptorProto.Type.TYPE_INT64,
                            DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL, null))
                    .addField(field("f32", 3, DescriptorProtos.FieldDescriptorProto.Type.TYPE_FLOAT,
                            DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL, null))
                    .addField(field("f64", 4, DescriptorProtos.FieldDescriptorProto.Type.TYPE_DOUBLE,
                            DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL, null))
                    .addField(field("flag", 5, DescriptorProtos.FieldDescriptorProto.Type.TYPE_BOOL,
                            DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL, null))
                    .addField(field("name", 6, DescriptorProtos.FieldDescriptorProto.Type.TYPE_STRING,
                            DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL, null))
                    .addField(field("payload", 7, DescriptorProtos.FieldDescriptorProto.Type.TYPE_BYTES,
                            DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL, null))
                    .addField(field("color", 8, DescriptorProtos.FieldDescriptorProto.Type.TYPE_ENUM,
                            DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL, ".Color"))
                    .addField(field("nested", 9, DescriptorProtos.FieldDescriptorProto.Type.TYPE_MESSAGE,
                            DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL, ".Nested"))
                    .addField(field("tags", 10, DescriptorProtos.FieldDescriptorProto.Type.TYPE_STRING,
                            DescriptorProtos.FieldDescriptorProto.Label.LABEL_REPEATED, null))
                    .build();
            DescriptorProtos.FileDescriptorProto file = DescriptorProtos.FileDescriptorProto.newBuilder()
                    .setName("event.proto")
                    .setSyntax("proto3")
                    .addEnumType(color)
                    .addMessageType(nested)
                    .addMessageType(eventProto)
                    .build();
            Descriptors.FileDescriptor fileDescriptor =
                    Descriptors.FileDescriptor.buildFrom(file, new Descriptors.FileDescriptor[0]);
            return fileDescriptor.findMessageTypeByName("Event");
        }

        private DescriptorProtos.FieldDescriptorProto field(String name, int number,
                DescriptorProtos.FieldDescriptorProto.Type type,
                DescriptorProtos.FieldDescriptorProto.Label label, String typeName) {
            DescriptorProtos.FieldDescriptorProto.Builder b =
                    DescriptorProtos.FieldDescriptorProto.newBuilder()
                            .setName(name).setNumber(number).setLabel(label).setType(type);
            if (typeName != null) {
                b.setTypeName(typeName);
            }
            return b.build();
        }
    }

    // ---------------------------------------------------------------------------------------------
    // Primitive (fromPrimitive + the shared toJavaValue normalisation)
    // ---------------------------------------------------------------------------------------------
    @Nested
    class Primitive {

        @Test
        void normalisesUtf8ToStringInValueColumn() {
            Map<String, Object> row = ClickHouseRowConverter.fromPrimitive(new Utf8("hello"));

            assertThat(row.keySet()).containsExactly("value");
            assertThat(row.get("value")).isEqualTo("hello");
        }

        @Test
        void keepsScalarsAndConvertsByteBuffer() {
            assertThat(ClickHouseRowConverter.fromPrimitive(7L).get("value")).isEqualTo(7L);
            assertThat(ClickHouseRowConverter.fromPrimitive(true).get("value")).isEqualTo(true);

            byte[] raw = {9, 8, 7};
            assertThat(ClickHouseRowConverter.fromPrimitive(raw).get("value")).isEqualTo(raw);

            ByteBuffer buffer = ByteBuffer.wrap(new byte[] {1, 2, 3, 4});
            buffer.position(1);
            Object value = ClickHouseRowConverter.fromPrimitive(buffer).get("value");
            assertThat(value).isInstanceOf(byte[].class);
            assertThat((byte[]) value).containsExactly(2, 3, 4);
        }

        @Test
        void leavesNullAsNull() {
            Map<String, Object> row = ClickHouseRowConverter.fromPrimitive(null);

            assertThat(row.keySet()).containsExactly("value");
            assertThat(row.get("value")).isNull();
        }

        @Test
        void stringifiesComplexValue() {
            Map<String, Object> row = ClickHouseRowConverter.fromPrimitive(List.of("a", "b"));

            assertThat(row.get("value")).isInstanceOf(String.class).isEqualTo("[a, b]");
        }
    }
}
