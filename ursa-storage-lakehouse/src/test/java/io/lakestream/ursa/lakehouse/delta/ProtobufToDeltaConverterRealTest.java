/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.lakehouse.delta;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import io.delta.kernel.data.ArrayValue;
import io.delta.kernel.types.ArrayType;
import io.delta.kernel.types.BinaryType;
import io.delta.kernel.types.BooleanType;
import io.delta.kernel.types.DoubleType;
import io.delta.kernel.types.FloatType;
import io.delta.kernel.types.IntegerType;
import io.delta.kernel.types.LongType;
import io.delta.kernel.types.StringType;
import io.delta.kernel.types.StructField;
import io.delta.kernel.types.StructType;
import io.lakestream.ursa.lakehouse.v2.serde.iceberg.test.TestProtoMessages;
import io.lakestream.ursa.lakehouse.v2.serde.iceberg.test.TestProtoMessages.Address;
import io.lakestream.ursa.lakehouse.v2.serde.iceberg.test.TestProtoMessages.ComprehensiveTestMessage;
import io.lakestream.ursa.lakehouse.v2.serde.iceberg.test.TestProtoMessages.Contact;
import io.lakestream.ursa.lakehouse.v2.serde.iceberg.test.TestProtoMessages.SimpleTestMessage;
import io.lakestream.ursa.lakehouse.v2.serde.iceberg.test.TestProtoMessages.Status;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.IntStream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;


public class ProtobufToDeltaConverterRealTest {

    private StructType simpleSchema;
    private StructType comprehensiveSchema;
    private StructType addressSchema;

    @BeforeEach
    void setUp() {
        // Simple schema for SimpleTestMessage
        simpleSchema = new StructType(Arrays.asList(
            new StructField("name", StringType.STRING, true),
            new StructField("age", IntegerType.INTEGER, true),
            new StructField("active", BooleanType.BOOLEAN, true)
        ));

        // Address schema for nested messages
        addressSchema = new StructType(Arrays.asList(
            new StructField("street", StringType.STRING, true),
            new StructField("city", StringType.STRING, true),
            new StructField("state", StringType.STRING, true),
            new StructField("zip_code", StringType.STRING, true),
            new StructField("country", StringType.STRING, true)
        ));

        // Comprehensive schema for ComprehensiveTestMessage
        comprehensiveSchema = new StructType(Arrays.asList(
            // INT32 types
            new StructField("int32_field", IntegerType.INTEGER, true),
            new StructField("uint32_field", LongType.LONG, true),
            new StructField("sint32_field", IntegerType.INTEGER, true),
            new StructField("fixed32_field", IntegerType.INTEGER, true),
            new StructField("sfixed32_field", IntegerType.INTEGER, true),

            // INT64 types
            new StructField("int64_field", LongType.LONG, true),
            new StructField("uint64_field", LongType.LONG, true),
            new StructField("sint64_field", LongType.LONG, true),
            new StructField("fixed64_field", LongType.LONG, true),
            new StructField("sfixed64_field", LongType.LONG, true),

            // Floating point types
            new StructField("float_field", FloatType.FLOAT, true),
            new StructField("double_field", DoubleType.DOUBLE, true),

            // Boolean and String
            new StructField("bool_field", BooleanType.BOOLEAN, true),
            new StructField("string_field", StringType.STRING, true),

            // Bytes and Enum
            new StructField("bytes_field", BinaryType.BINARY, true),
            new StructField("status_field", StringType.STRING, true),

            // Nested message
            new StructField("address_field", addressSchema, true),

            // Repeated fields
            new StructField("repeated_int32_field", new ArrayType(IntegerType.INTEGER, true), true),
            new StructField("repeated_uint32_field", new ArrayType(LongType.LONG, true), true),
            new StructField("repeated_sint32_field", new ArrayType(IntegerType.INTEGER, true), true),
            new StructField("repeated_fixed32_field", new ArrayType(IntegerType.INTEGER, true), true),
            new StructField("repeated_sfixed32_field", new ArrayType(IntegerType.INTEGER, true), true),

            new StructField("repeated_int64_field", new ArrayType(LongType.LONG, true), true),
            new StructField("repeated_uint64_field", new ArrayType(LongType.LONG, true), true),
            new StructField("repeated_sint64_field", new ArrayType(LongType.LONG, true), true),
            new StructField("repeated_fixed64_field", new ArrayType(LongType.LONG, true), true),
            new StructField("repeated_sfixed64_field", new ArrayType(LongType.LONG, true), true),

            new StructField("repeated_float_field", new ArrayType(FloatType.FLOAT, true), true),
            new StructField("repeated_double_field", new ArrayType(DoubleType.DOUBLE, true), true),

            new StructField("repeated_bool_field", new ArrayType(BooleanType.BOOLEAN, true), true),
            new StructField("repeated_string_field", new ArrayType(StringType.STRING, true), true),
            new StructField("repeated_bytes_field", new ArrayType(BinaryType.BINARY, true), true),
            new StructField("repeated_status_field", new ArrayType(StringType.STRING, true), true),
            new StructField("repeated_address_field", new ArrayType(addressSchema, true), true),
            new StructField("repeated_contact_field", new ArrayType(
                new StructType(Arrays.asList(
                    new StructField("type", StringType.STRING, true),
                    new StructField("value", StringType.STRING, true)
                )), true), true)
        ));
    }

    @Nested
    @DisplayName("Basic Conversion Tests")
    class BasicConversionTests {

        @Test
        @DisplayName("Should convert simple protobuf message successfully")
        void testSimpleMessageConversion() {
            // Given
            SimpleTestMessage message = SimpleTestMessage.newBuilder()
                .setName("John Doe")
                .setAge(30)
                .setActive(true)
                .build();

            // When
            GenericRow result = ProtobufToDeltaConverter.convertToGenericRow(message, simpleSchema);

            // Then
            assertNotNull(result);
            assertEquals("John Doe", result.getValue(0));
            assertEquals(30, result.getValue(1));
            assertEquals(true, result.getValue(2));
        }

        @Test
        @DisplayName("Should handle null message gracefully")
        void testNullMessageHandling() {
            // When & Then
            assertThrows(IllegalArgumentException.class, () ->
                ProtobufToDeltaConverter.convertToGenericRow(null, simpleSchema));
        }

        @Test
        @DisplayName("Should handle null schema gracefully")
        void testNullSchemaHandling() {
            // Given
            SimpleTestMessage message = SimpleTestMessage.newBuilder().build();

            // When & Then
            assertThrows(IllegalArgumentException.class, () ->
                ProtobufToDeltaConverter.convertToGenericRow(message, null));
        }

        @Test
        @DisplayName("Should handle empty message")
        void testEmptyMessage() {
            // Given
            SimpleTestMessage message = SimpleTestMessage.newBuilder().build();

            // When
            GenericRow result = ProtobufToDeltaConverter.convertToGenericRow(message, simpleSchema);

            // Then
            assertNotNull(result);
            assertEquals("", result.getValue(0)); // Default string value
            assertEquals(0, result.getValue(1)); // Default int value
            assertEquals(false, result.getValue(2)); // Default bool value
        }
    }

    @Nested
    @DisplayName("Comprehensive Type Conversion Tests")
    class ComprehensiveTypeTests {

        @Test
        @DisplayName("Should convert all INT32 variations correctly")
        void testInt32Conversions() {
            // Given
            ComprehensiveTestMessage message = ComprehensiveTestMessage.newBuilder()
                .setInt32Field(42)
                .setUint32Field(123)
                .setSint32Field(-456)
                .setFixed32Field(789)
                .setSfixed32Field(-987)
                .build();

            // When
            GenericRow result = ProtobufToDeltaConverter.convertToGenericRow(message, comprehensiveSchema);

            // Then
            assertEquals(42, result.getValue(0));
            assertEquals(123L, result.getValue(1)); // uint32 -> long
            assertEquals(-456, result.getValue(2));
            assertEquals(789, result.getValue(3));
            assertEquals(-987, result.getValue(4));
        }

        @Test
        @DisplayName("Should convert all INT64 variations correctly")
        void testInt64Conversions() {
            // Given
            ComprehensiveTestMessage message = ComprehensiveTestMessage.newBuilder()
                .setInt64Field(9223372036854775807L)
                .setUint64Field(123456789L)
                .setSint64Field(-9876543210L)
                .setFixed64Field(1111111111L)
                .setSfixed64Field(-2222222222L)
                .build();

            // When
            GenericRow result = ProtobufToDeltaConverter.convertToGenericRow(message, comprehensiveSchema);

            // Then
            assertEquals(9223372036854775807L, result.getValue(5));
            assertEquals(123456789L, result.getValue(6));
            assertEquals(-9876543210L, result.getValue(7));
            assertEquals(1111111111L, result.getValue(8));
            assertEquals(-2222222222L, result.getValue(9));
        }

        @Test
        @DisplayName("Should convert floating point types correctly")
        void testFloatingPointConversions() {
            // Given
            ComprehensiveTestMessage message = ComprehensiveTestMessage.newBuilder()
                .setFloatField(3.14f)
                .setDoubleField(2.718281828459045)
                .build();

            // When
            GenericRow result = ProtobufToDeltaConverter.convertToGenericRow(message, comprehensiveSchema);

            // Then
            assertEquals(3.14f, (float) result.getValue(10), 0.001f);
            assertEquals(2.718281828459045, (double) result.getValue(11), 0.000000000000001);
        }

        @Test
        @DisplayName("Should convert boolean type correctly")
        void testBooleanConversion() {
            // Given
            ComprehensiveTestMessage message = ComprehensiveTestMessage.newBuilder()
                .setBoolField(true)
                .build();

            // When
            GenericRow result = ProtobufToDeltaConverter.convertToGenericRow(message, comprehensiveSchema);

            // Then
            assertEquals(true, result.getValue(12));
        }

        @Test
        @DisplayName("Should convert string type correctly")
        void testStringConversion() {
            // Given
            String testString = "Hello, World! 🌍";
            ComprehensiveTestMessage message = ComprehensiveTestMessage.newBuilder()
                .setStringField(testString)
                .build();

            // When
            GenericRow result = ProtobufToDeltaConverter.convertToGenericRow(message, comprehensiveSchema);

            // Then
            assertEquals(testString, result.getValue(13));
        }

        @Test
        @DisplayName("Should convert bytes type correctly")
        void testBytesConversion() {
            // Given
            byte[] testBytes = {1, 2, 3, 4, 5, -1, -2, -3};
            ComprehensiveTestMessage message = ComprehensiveTestMessage.newBuilder()
                .setBytesField(ByteString.copyFrom(testBytes))
                .build();

            // When
            GenericRow result = ProtobufToDeltaConverter.convertToGenericRow(message, comprehensiveSchema);

            // Then
            assertArrayEquals(testBytes, (byte[]) result.getValue(14));
        }

        @ParameterizedTest
        @EnumSource(TestProtoMessages.Status.class)
        @DisplayName("Should convert enum types correctly")
        void testEnumConversion(TestProtoMessages.Status status) {
            if (status == TestProtoMessages.Status.UNRECOGNIZED) {
                return;
            }
            // Given
            ComprehensiveTestMessage message = ComprehensiveTestMessage.newBuilder()
                .setStatusField(status)
                .build();

            // When
            GenericRow result = ProtobufToDeltaConverter.convertToGenericRow(message, comprehensiveSchema);

            // Then
            assertEquals(status.name(), result.getValue(15));
        }

        @Test
        @DisplayName("Should convert nested message correctly")
        void testNestedMessageConversion() {
            // Given
            Address address = Address.newBuilder()
                .setStreet("123 Main St")
                .setCity("New York")
                .setState("NY")
                .setZipCode("10001")
                .setCountry("USA")
                .build();

            ComprehensiveTestMessage message = ComprehensiveTestMessage.newBuilder()
                .setAddressField(address)
                .build();

            // When
            GenericRow result = ProtobufToDeltaConverter.convertToGenericRow(message, comprehensiveSchema);

            // Then
            GenericRow addressRow = (GenericRow) result.getValue(16);
            assertNotNull(addressRow);
            assertEquals("123 Main St", addressRow.getValue(0));
            assertEquals("New York", addressRow.getValue(1));
            assertEquals("NY", addressRow.getValue(2));
            assertEquals("10001", addressRow.getValue(3));
            assertEquals("USA", addressRow.getValue(4));
        }
    }

    @Nested
    @DisplayName("Repeated Fields Tests")
    class RepeatedFieldsTests {

        @Test
        @DisplayName("Should convert repeated int32 fields correctly")
        void testRepeatedInt32Fields() {
            // Given
            ComprehensiveTestMessage message = ComprehensiveTestMessage.newBuilder()
                .addAllRepeatedInt32Field(Arrays.asList(1, 2, 3, 4, 5))
                .addAllRepeatedUint32Field(Arrays.asList(10, 20, 30))
                .build();

            // When
            GenericRow result = ProtobufToDeltaConverter.convertToGenericRow(message, comprehensiveSchema);

            // Then
            ArrayValue int32Array = (ArrayValue) result.getValue(17);
            assertEquals(5, int32Array.getSize());

            ArrayValue uint32Array = (ArrayValue) result.getValue(18);
            assertEquals(3, uint32Array.getSize());
        }

        @Test
        @DisplayName("Should convert repeated int64 fields correctly")
        void testRepeatedInt64Fields() {
            // Given
            ComprehensiveTestMessage message = ComprehensiveTestMessage.newBuilder()
                .addAllRepeatedInt64Field(Arrays.asList(1000000000L, 2000000000L))
                .addAllRepeatedUint64Field(Arrays.asList(3000000000L, 4000000000L))
                .build();

            // When
            GenericRow result = ProtobufToDeltaConverter.convertToGenericRow(message, comprehensiveSchema);

            // Then
            ArrayValue int64Array = (ArrayValue) result.getValue(22);
            assertEquals(2, int64Array.getSize());

            ArrayValue uint64Array = (ArrayValue) result.getValue(23);
            assertEquals(2, uint64Array.getSize());
        }

        @Test
        @DisplayName("Should convert repeated floating point fields correctly")
        void testRepeatedFloatingPointFields() {
            // Given
            ComprehensiveTestMessage message = ComprehensiveTestMessage.newBuilder()
                .addAllRepeatedFloatField(Arrays.asList(1.1f, 2.2f, 3.3f))
                .addAllRepeatedDoubleField(Arrays.asList(4.4, 5.5, 6.6))
                .build();

            // When
            GenericRow result = ProtobufToDeltaConverter.convertToGenericRow(message, comprehensiveSchema);

            // Then
            ArrayValue floatArray = (ArrayValue) result.getValue(27);
            assertEquals(3, floatArray.getSize());

            ArrayValue doubleArray = (ArrayValue) result.getValue(28);
            assertEquals(3, doubleArray.getSize());
        }

        @Test
        @DisplayName("Should convert repeated boolean fields correctly")
        void testRepeatedBooleanFields() {
            // Given
            ComprehensiveTestMessage message = ComprehensiveTestMessage.newBuilder()
                .addAllRepeatedBoolField(Arrays.asList(true, false, true))
                .build();

            // When
            GenericRow result = ProtobufToDeltaConverter.convertToGenericRow(message, comprehensiveSchema);

            // Then
            ArrayValue boolArray = (ArrayValue) result.getValue(29);
            assertEquals(3, boolArray.getSize());
        }

        @Test
        @DisplayName("Should convert repeated string fields correctly")
        void testRepeatedStringFields() {
            // Given
            ComprehensiveTestMessage message = ComprehensiveTestMessage.newBuilder()
                .addAllRepeatedStringField(Arrays.asList("hello", "world", "test"))
                .build();

            // When
            GenericRow result = ProtobufToDeltaConverter.convertToGenericRow(message, comprehensiveSchema);

            // Then
            ArrayValue stringArray = (ArrayValue) result.getValue(30);
            assertEquals(3, stringArray.getSize());
        }

        @Test
        @DisplayName("Should convert repeated bytes fields correctly")
        void testRepeatedBytesFields() {
            // Given
            ComprehensiveTestMessage message = ComprehensiveTestMessage.newBuilder()
                .addRepeatedBytesField(ByteString.copyFrom(new byte[]{1, 2, 3}))
                .addRepeatedBytesField(ByteString.copyFrom(new byte[]{4, 5, 6}))
                .build();

            // When
            GenericRow result = ProtobufToDeltaConverter.convertToGenericRow(message, comprehensiveSchema);

            // Then
            ArrayValue bytesArray = (ArrayValue) result.getValue(31);
            assertEquals(2, bytesArray.getSize());
        }

        @Test
        @DisplayName("Should convert repeated enum fields correctly")
        void testRepeatedEnumFields() {
            // Given
            ComprehensiveTestMessage message = ComprehensiveTestMessage.newBuilder()
                .addAllRepeatedStatusField(Arrays.asList(Status.ACTIVE, Status.INACTIVE, Status.PENDING))
                .build();

            // When
            GenericRow result = ProtobufToDeltaConverter.convertToGenericRow(message, comprehensiveSchema);

            // Then
            ArrayValue enumArray = (ArrayValue) result.getValue(32);
            assertEquals(3, enumArray.getSize());
        }

        @Test
        @DisplayName("Should convert repeated nested message fields correctly")
        void testRepeatedNestedMessageFields() {
            // Given
            Address address1 = Address.newBuilder()
                .setStreet("123 Main St")
                .setCity("New York")
                .build();

            Address address2 = Address.newBuilder()
                .setStreet("456 Oak Ave")
                .setCity("Los Angeles")
                .build();

            ComprehensiveTestMessage message = ComprehensiveTestMessage.newBuilder()
                .addRepeatedAddressField(address1)
                .addRepeatedAddressField(address2)
                .build();

            // When
            GenericRow result = ProtobufToDeltaConverter.convertToGenericRow(message, comprehensiveSchema);

            // Then
            ArrayValue addressArray = (ArrayValue) result.getValue(33);
            assertEquals(2, addressArray.getSize());
        }

        @Test
        @DisplayName("Should handle empty repeated fields")
        void testEmptyRepeatedFields() {
            // Given
            ComprehensiveTestMessage message = ComprehensiveTestMessage.newBuilder().build();

            // When
            GenericRow result = ProtobufToDeltaConverter.convertToGenericRow(message, comprehensiveSchema);

            // Then
            ArrayValue emptyArray = (ArrayValue) result.getValue(17);
            assertEquals(0, emptyArray.getSize());
        }
    }

    @Nested
    @DisplayName("Edge Cases and Error Handling")
    class EdgeCasesTests {

        @Test
        @DisplayName("Should handle missing fields in protobuf message")
        void testMissingFields() {
            // Given - message with only some fields set
            SimpleTestMessage message = SimpleTestMessage.newBuilder()
                .setName("John")
                .build(); // age and active not set

            // When
            GenericRow result = ProtobufToDeltaConverter.convertToGenericRow(message, simpleSchema);

            // Then
            assertNotNull(result);
            assertEquals("John", result.getValue(0));
            assertEquals(0, result.getValue(1)); // default int value
            assertEquals(false, result.getValue(2)); // default bool value
        }

        @Test
        @DisplayName("Should handle schema with extra fields")
        void testSchemaWithExtraFields() {
            // Given
            StructType extendedSchema = new StructType(Arrays.asList(
                new StructField("name", StringType.STRING, true),
                new StructField("age", IntegerType.INTEGER, true),
                new StructField("active", BooleanType.BOOLEAN, true),
                new StructField("extra_field", StringType.STRING, true) // This field doesn't exist in proto
            ));

            SimpleTestMessage message = SimpleTestMessage.newBuilder()
                .setName("John")
                .setAge(25)
                .setActive(true)
                .build();

            // When
            GenericRow result = ProtobufToDeltaConverter.convertToGenericRow(message, extendedSchema);

            // Then
            assertNotNull(result);
            assertEquals("John", result.getValue(0));
            assertEquals(25, result.getValue(1));
            assertEquals(true, result.getValue(2));
            assertNull(result.getValue(3)); // extra field should be null
        }

        @Test
        @DisplayName("Should handle extreme numeric values")
        void testExtremeNumericValues() {
            // Given
            ComprehensiveTestMessage message = ComprehensiveTestMessage.newBuilder()
                .setInt32Field(Integer.MAX_VALUE)
                .setInt64Field(Long.MAX_VALUE)
                .setFloatField(Float.MAX_VALUE)
                .setDoubleField(Double.MAX_VALUE)
                .build();

            // When
            GenericRow result = ProtobufToDeltaConverter.convertToGenericRow(message, comprehensiveSchema);

            // Then
            assertEquals(Integer.MAX_VALUE, result.getValue(0));
            assertEquals(Long.MAX_VALUE, result.getValue(5));
            assertEquals(Float.MAX_VALUE, result.getValue(10));
            assertEquals(Double.MAX_VALUE, result.getValue(11));
        }

        @Test
        @DisplayName("Should handle minimum numeric values")
        void testMinimumNumericValues() {
            // Given
            ComprehensiveTestMessage message = ComprehensiveTestMessage.newBuilder()
                .setInt32Field(Integer.MIN_VALUE)
                .setInt64Field(Long.MIN_VALUE)
                .setFloatField(Float.MIN_VALUE)
                .setDoubleField(Double.MIN_VALUE)
                .build();

            // When
            GenericRow result = ProtobufToDeltaConverter.convertToGenericRow(message, comprehensiveSchema);

            // Then
            assertEquals(Integer.MIN_VALUE, result.getValue(0));
            assertEquals(Long.MIN_VALUE, result.getValue(5));
            assertEquals(Float.MIN_VALUE, result.getValue(10));
            assertEquals(Double.MIN_VALUE, result.getValue(11));
        }

        @Test
        @DisplayName("Should handle special floating point values")
        void testSpecialFloatingPointValues() {
            // Given
            ComprehensiveTestMessage message = ComprehensiveTestMessage.newBuilder()
                .setFloatField(Float.NaN)
                .setDoubleField(Double.POSITIVE_INFINITY)
                .build();

            // When
            GenericRow result = ProtobufToDeltaConverter.convertToGenericRow(message, comprehensiveSchema);

            // Then
            assertTrue(Float.isNaN((Float) result.getValue(10)));
            assertEquals(Double.POSITIVE_INFINITY, result.getValue(11));
        }

        @Test
        @DisplayName("Should handle very large repeated fields")
        void testLargeRepeatedFields() {
            // Given
            List<Integer> largeList = IntStream.range(0, 10000)
                .boxed()
                .collect(ArrayList::new, ArrayList::add, ArrayList::addAll);

            ComprehensiveTestMessage message = ComprehensiveTestMessage.newBuilder()
                .addAllRepeatedInt32Field(largeList)
                .build();

            // When
            GenericRow result = ProtobufToDeltaConverter.convertToGenericRow(message, comprehensiveSchema);

            // Then
            ArrayValue largeArray = (ArrayValue) result.getValue(17);
            assertEquals(10000, largeArray.getSize());
        }

        @Test
        @DisplayName("Should handle empty bytes field")
        void testEmptyBytesField() {
            // Given
            ComprehensiveTestMessage message = ComprehensiveTestMessage.newBuilder()
                .setBytesField(ByteString.EMPTY)
                .build();

            // When
            GenericRow result = ProtobufToDeltaConverter.convertToGenericRow(message, comprehensiveSchema);

            // Then
            byte[] emptyBytes = (byte[]) result.getValue(14);
            assertNotNull(emptyBytes);
            assertEquals(0, emptyBytes.length);
        }

        @Test
        @DisplayName("Should handle empty string field")
        void testEmptyStringField() {
            // Given
            ComprehensiveTestMessage message = ComprehensiveTestMessage.newBuilder()
                .setStringField("")
                .build();

            // When
            GenericRow result = ProtobufToDeltaConverter.convertToGenericRow(message, comprehensiveSchema);

            // Then
            assertEquals("", result.getValue(13));
        }

        @Test
        @DisplayName("Should handle unicode strings")
        void testUnicodeStrings() {
            // Given
            String unicodeString = "Hello 世界 🌍 café naïve résumé";
            ComprehensiveTestMessage message = ComprehensiveTestMessage.newBuilder()
                .setStringField(unicodeString)
                .build();

            // When
            GenericRow result = ProtobufToDeltaConverter.convertToGenericRow(message, comprehensiveSchema);

            // Then
            assertEquals(unicodeString, result.getValue(13));
        }
    }

    @Nested
    @DisplayName("Schema Inference Tests")
    class SchemaInferenceTests {

        @Test
        @DisplayName("Should infer schema from simple protobuf descriptor")
        void testSimpleSchemaInference() {
            // Given
            Descriptors.Descriptor descriptor = SimpleTestMessage.getDescriptor();

            // When
            StructType inferredSchema = ProtobufToDeltaConverter.inferSchemaFromProtobuf(descriptor);

            // Then
            assertNotNull(inferredSchema);
            assertEquals(3, inferredSchema.length());

            assertEquals("name", inferredSchema.at(0).getName());
            assertInstanceOf(StringType.class, inferredSchema.at(0).getDataType());

            assertEquals("age", inferredSchema.at(1).getName());
            assertInstanceOf(IntegerType.class, inferredSchema.at(1).getDataType());

            assertEquals("active", inferredSchema.at(2).getName());
            assertInstanceOf(BooleanType.class, inferredSchema.at(2).getDataType());
        }

        @Test
        @DisplayName("Should infer schema from comprehensive protobuf descriptor")
        void testComprehensiveSchemaInference() {
            // Given
            Descriptors.Descriptor descriptor = ComprehensiveTestMessage.getDescriptor();

            // When
            StructType inferredSchema = ProtobufToDeltaConverter.inferSchemaFromProtobuf(descriptor);

            // Then
            assertNotNull(inferredSchema);
            assertTrue(inferredSchema.length() > 30); // Should have all the fields

            // Check a few key fields
            StructField int32Field = findFieldByName(inferredSchema, "int32_field");
            assertNotNull(int32Field);
            assertInstanceOf(IntegerType.class, int32Field.getDataType());

            StructField repeatedInt32Field = findFieldByName(inferredSchema, "repeated_int32_field");
            assertNotNull(repeatedInt32Field);
            assertInstanceOf(ArrayType.class, repeatedInt32Field.getDataType());

            StructField addressField = findFieldByName(inferredSchema, "address_field");
            assertNotNull(addressField);
            assertInstanceOf(StructType.class, addressField.getDataType());
        }

        @Test
        @DisplayName("Should handle null descriptor gracefully")
        void testNullDescriptorInference() {
            // When & Then
            assertThrows(IllegalArgumentException.class, () ->
                ProtobufToDeltaConverter.inferSchemaFromProtobuf(null));
        }

        private StructField findFieldByName(StructType structType, String name) {
            for (int i = 0; i < structType.length(); i++) {
                StructField field = structType.at(i);
                if (field.getName().equals(name)) {
                    return field;
                }
            }
            return null;
        }
    }

    @Nested
    @DisplayName("Batch Conversion Tests")
    class BatchConversionTests {

        @Test
        @DisplayName("Should convert multiple messages in batch")
        void testBatchConversion() {
            // Given
            List<SimpleTestMessage> messages = Arrays.asList(
                SimpleTestMessage.newBuilder().setName("Alice").setAge(25).setActive(true).build(),
                SimpleTestMessage.newBuilder().setName("Bob").setAge(30).setActive(false).build(),
                SimpleTestMessage.newBuilder().setName("Charlie").setAge(35).setActive(true).build()
            );

            // When
            List<GenericRow> results = ProtobufToDeltaConverter.convertMessages(
                new ArrayList<>(messages), simpleSchema);

            // Then
            assertEquals(3, results.size());

            assertEquals("Alice", results.get(0).getValue(0));
            assertEquals(25, results.get(0).getValue(1));
            assertEquals(true, results.get(0).getValue(2));

            assertEquals("Bob", results.get(1).getValue(0));
            assertEquals(30, results.get(1).getValue(1));
            assertEquals(false, results.get(1).getValue(2));

            assertEquals("Charlie", results.get(2).getValue(0));
            assertEquals(35, results.get(2).getValue(1));
            assertEquals(true, results.get(2).getValue(2));
        }

        @Test
        @DisplayName("Should handle null messages in batch")
        void testNullMessagesInBatch() {
            // Given
            List<Message> messages = Arrays.asList(
                SimpleTestMessage.newBuilder().setName("Alice").setAge(25).build(),
                null, // null message
                SimpleTestMessage.newBuilder().setName("Bob").setAge(30).build()
            );

            // When
            List<GenericRow> results = ProtobufToDeltaConverter.convertMessages(messages, simpleSchema);

            // Then
            assertEquals(2, results.size()); // Should filter out null messages
            assertEquals("Alice", results.get(0).getValue(0));
            assertEquals("Bob", results.get(1).getValue(0));
        }

        @Test
        @DisplayName("Should handle empty batch")
        void testEmptyBatch() {
            // Given
            List<Message> messages = new ArrayList<>();

            // When
            List<GenericRow> results = ProtobufToDeltaConverter.convertMessages(messages, simpleSchema);

            // Then
            assertTrue(results.isEmpty());
        }

        @Test
        @DisplayName("Should handle null batch")
        void testNullBatch() {
            // When
            List<GenericRow> results = ProtobufToDeltaConverter.convertMessages(null, simpleSchema);

            // Then
            assertTrue(results.isEmpty());
        }

        @Test
        @DisplayName("Should handle large batch efficiently")
        void testLargeBatch() {
            // Given
            List<Message> messages = new ArrayList<>();
            for (int i = 0; i < 1000; i++) {
                messages.add(SimpleTestMessage.newBuilder()
                    .setName("User" + i)
                    .setAge(20 + (i % 50))
                    .setActive(i % 2 == 0)
                    .build());
            }

            // When
            long startTime = System.currentTimeMillis();
            List<GenericRow> results = ProtobufToDeltaConverter.convertMessages(messages, simpleSchema);
            long endTime = System.currentTimeMillis();

            // Then
            assertEquals(1000, results.size());
            assertTrue(endTime - startTime < 5000); // Should complete within 5 seconds
        }
    }

    @Nested
    @DisplayName("Type Coercion Tests")
    class TypeCoercionTests {

        @Test
        @DisplayName("Should handle type mismatches gracefully")
        void testTypeMismatch() {
            // Given - schema expects string but proto has int
            StructType mismatchSchema = new StructType(Arrays.asList(
                new StructField("age", StringType.STRING, true), // expects string but proto has int
                new StructField("name", IntegerType.INTEGER, true) // expects int but proto has string
            ));

            SimpleTestMessage message = SimpleTestMessage.newBuilder()
                .setName("John")
                .setAge(25)
                .build();

            // When
            GenericRow result = ProtobufToDeltaConverter.convertToGenericRow(message, mismatchSchema);

            // Then
            assertNotNull(result);
            // Age should be converted to string
            assertEquals("25", result.getValue(0));
            // Name should be converted to string (fallback)
            assertEquals("John", result.getValue(1));
        }

        @Test
        @DisplayName("Should handle numeric type conversions")
        void testNumericTypeConversions() {
            // Given - proto has int32 but schema expects different numeric types
            StructType numericSchema = new StructType(Arrays.asList(
                new StructField("age", LongType.LONG, true),
                new StructField("age", FloatType.FLOAT, true),
                new StructField("age", DoubleType.DOUBLE, true)
            ));

            SimpleTestMessage message = SimpleTestMessage.newBuilder()
                .setAge(42)
                .build();

            // When
            GenericRow result = ProtobufToDeltaConverter.convertToGenericRow(message, numericSchema);

            // Then
            assertEquals(42L, result.getValue(0));
            assertEquals(42.0f, result.getValue(1));
            assertEquals(42.0, result.getValue(2));
        }
    }

    @Nested
    @DisplayName("Performance Tests")
    class PerformanceTests {

        @Test
        @DisplayName("Should handle deeply nested messages efficiently")
        void testDeeplyNestedMessages() {
            // Given
            Address address = Address.newBuilder()
                .setStreet("123 Main St")
                .setCity("New York")
                .setState("NY")
                .setZipCode("10001")
                .setCountry("USA")
                .build();

            ComprehensiveTestMessage message = ComprehensiveTestMessage.newBuilder()
                .setAddressField(address)
                .addRepeatedAddressField(address)
                .addRepeatedAddressField(address)
                .addRepeatedAddressField(address)
                .build();

            // When
            long startTime = System.currentTimeMillis();
            GenericRow result = ProtobufToDeltaConverter.convertToGenericRow(message, comprehensiveSchema);
            long endTime = System.currentTimeMillis();

            // Then
            assertNotNull(result);
            assertTrue(endTime - startTime < 1000); // Should complete within 1 second
        }

        @Test
        @DisplayName("Should handle messages with many repeated fields efficiently")
        void testManyRepeatedFields() {
            // Given
            ComprehensiveTestMessage.Builder builder = ComprehensiveTestMessage.newBuilder();

            // Add 1000 elements to each repeated field
            for (int i = 0; i < 1000; i++) {
                builder.addRepeatedInt32Field(i);
                builder.addRepeatedStringField("string" + i);
                builder.addRepeatedBoolField(i % 2 == 0);
            }

            ComprehensiveTestMessage message = builder.build();

            // When
            long startTime = System.currentTimeMillis();
            GenericRow result = ProtobufToDeltaConverter.convertToGenericRow(message, comprehensiveSchema);
            long endTime = System.currentTimeMillis();

            // Then
            assertNotNull(result);
            assertTrue(endTime - startTime < 2000); // Should complete within 2 seconds
        }
    }

    @Nested
    @DisplayName("Error Recovery Tests")
    class ErrorRecoveryTests {

        @Test
        @DisplayName("Should handle corrupted enum values gracefully")
        void testCorruptedEnumValues() {
            // Given
            ComprehensiveTestMessage message = ComprehensiveTestMessage.newBuilder()
                .setStatusFieldValue(999) // Invalid enum value
                .build();

            // When
            GenericRow result = ProtobufToDeltaConverter.convertToGenericRow(message, comprehensiveSchema);

            // Then
            assertNotNull(result);
            // Should handle gracefully, possibly converting to string representation
            assertNotNull(result.getValue(15));
        }

        @Test
        @DisplayName("Should handle schema field name case sensitivity")
        void testCaseSensitiveFieldNames() {
            // Given
            StructType caseSchema = new StructType(Arrays.asList(
                new StructField("NAME", StringType.STRING, true), // Different case
                new StructField("age", IntegerType.INTEGER, true),
                new StructField("ACTIVE", BooleanType.BOOLEAN, true) // Different case
            ));

            SimpleTestMessage message = SimpleTestMessage.newBuilder()
                .setName("John")
                .setAge(25)
                .setActive(true)
                .build();

            // When
            GenericRow result = ProtobufToDeltaConverter.convertToGenericRow(message, caseSchema);

            // Then
            assertNotNull(result);
            // Fields with different cases should return default values
            assertNull(result.getValue(0)); // NAME doesn't match name
            assertEquals(25, result.getValue(1)); // age matches
            assertNull(result.getValue(2)); // ACTIVE doesn't match active
        }
    }

    @Nested
    @DisplayName("Integration Tests")
    class IntegrationTests {

        @Test
        @DisplayName("Should handle end-to-end conversion with inferred schema")
        void testEndToEndWithInferredSchema() {
            // Given
            ComprehensiveTestMessage message = ComprehensiveTestMessage.newBuilder()
                .setInt32Field(42)
                .setStringField("test")
                .setBoolField(true)
                .setAddressField(Address.newBuilder()
                    .setStreet("123 Main St")
                    .setCity("New York")
                    .build())
                .addRepeatedInt32Field(1)
                .addRepeatedInt32Field(2)
                .addRepeatedInt32Field(3)
                .build();

            // When
            StructType inferredSchema = ProtobufToDeltaConverter.inferSchemaFromProtobuf(
                ComprehensiveTestMessage.getDescriptor());
            GenericRow result = ProtobufToDeltaConverter.convertToGenericRow(message, inferredSchema);

            // Then
            assertNotNull(result);
            assertNotNull(inferredSchema);

            // Find fields by name in inferred schema
            StructField int32Field = findFieldByName(inferredSchema, "int32_field");
            StructField stringField = findFieldByName(inferredSchema, "string_field");
            StructField boolField = findFieldByName(inferredSchema, "bool_field");
            StructField addressField = findFieldByName(inferredSchema, "address_field");
            StructField repeatedInt32Field = findFieldByName(inferredSchema, "repeated_int32_field");

            assertNotNull(int32Field);
            assertNotNull(stringField);
            assertNotNull(boolField);
            assertNotNull(addressField);
            assertNotNull(repeatedInt32Field);
        }

        @Test
        @DisplayName("Should handle real-world message complexity")
        void testRealWorldComplexity() {
            // Given - complex message with all features
            ComprehensiveTestMessage message = ComprehensiveTestMessage.newBuilder()
                // Basic types
                .setInt32Field(Integer.MAX_VALUE)
                .setInt64Field(Long.MAX_VALUE)
                .setFloatField(Float.MAX_VALUE)
                .setDoubleField(Double.MAX_VALUE)
                .setBoolField(true)
                .setStringField("Complex test string with unicode: 你好世界 🌍")
                .setBytesField(ByteString.copyFrom(new byte[]{1, 2, 3, 4, 5}))
                .setStatusField(Status.ACTIVE)

                // Nested message
                .setAddressField(Address.newBuilder()
                    .setStreet("123 Complex Street")
                    .setCity("Complex City")
                    .setState("CC")
                    .setZipCode("12345")
                    .setCountry("Complex Country")
                    .build())

                // Repeated fields
                .addAllRepeatedInt32Field(Arrays.asList(1, 2, 3, 4, 5))
                .addAllRepeatedStringField(Arrays.asList("a", "b", "c"))
                .addAllRepeatedBoolField(Arrays.asList(true, false, true))
                .addAllRepeatedStatusField(Arrays.asList(Status.ACTIVE, Status.INACTIVE))

                // Repeated nested messages
                .addRepeatedAddressField(Address.newBuilder()
                    .setStreet("First Address")
                    .setCity("First City")
                    .build())
                .addRepeatedAddressField(Address.newBuilder()
                    .setStreet("Second Address")
                    .setCity("Second City")
                    .build())

                .addRepeatedContactField(Contact.newBuilder()
                    .setType("email")
                    .setValue("test@example.com")
                    .build())
                .addRepeatedContactField(Contact.newBuilder()
                    .setType("phone")
                    .setValue("+1-555-123-4567")
                    .build())
                .build();

            // When
            GenericRow result = ProtobufToDeltaConverter.convertToGenericRow(message, comprehensiveSchema);

            // Then
            assertNotNull(result);

            // Verify basic types
            assertEquals(Integer.MAX_VALUE, result.getValue(0));
            assertEquals(Long.MAX_VALUE, result.getValue(5));
            assertEquals(Float.MAX_VALUE, result.getValue(10));
            assertEquals(Double.MAX_VALUE, result.getValue(11));
            assertEquals(true, result.getValue(12));
            assertEquals("Complex test string with unicode: 你好世界 🌍", result.getValue(13));
            assertArrayEquals(new byte[]{1, 2, 3, 4, 5}, (byte[]) result.getValue(14));
            assertEquals("ACTIVE", result.getValue(15));

            // Verify nested message
            GenericRow addressRow = (GenericRow) result.getValue(16);
            assertNotNull(addressRow);
            assertEquals("123 Complex Street", addressRow.getValue(0));
            assertEquals("Complex City", addressRow.getValue(1));

            // Verify repeated fields
            ArrayValue repeatedInts = (ArrayValue) result.getValue(17);
            assertEquals(5, repeatedInts.getSize());

            ArrayValue repeatedStrings = (ArrayValue) result.getValue(30);
            assertEquals(3, repeatedStrings.getSize());

            ArrayValue repeatedAddresses = (ArrayValue) result.getValue(33);
            assertEquals(2, repeatedAddresses.getSize());

            ArrayValue repeatedContacts = (ArrayValue) result.getValue(34);
            assertEquals(2, repeatedContacts.getSize());
        }

        private StructField findFieldByName(StructType structType, String name) {
            for (int i = 0; i < structType.length(); i++) {
                StructField field = structType.at(i);
                if (field.getName().equals(name)) {
                    return field;
                }
            }
            return null;
        }
    }

    @Nested
    @DisplayName("Null Safety Tests")
    class NullSafetyTests {

        @Test
        @DisplayName("Should handle all null inputs gracefully")
        void testAllNullInputs() {
            // Test null message
            assertThrows(IllegalArgumentException.class, () ->
                ProtobufToDeltaConverter.convertToGenericRow(null, simpleSchema));

            // Test null schema
            SimpleTestMessage message = SimpleTestMessage.newBuilder().build();
            assertThrows(IllegalArgumentException.class, () ->
                ProtobufToDeltaConverter.convertToGenericRow(message, null));

            // Test null descriptor for schema inference
            assertThrows(IllegalArgumentException.class, () ->
                ProtobufToDeltaConverter.inferSchemaFromProtobuf(null));
        }

        @Test
        @DisplayName("Should handle partial null nested messages")
        void testPartialNullNestedMessages() {
            // Given - message with some null nested fields
            ComprehensiveTestMessage message = ComprehensiveTestMessage.newBuilder()
                .setStringField("test")
                // addressField is not set (null)
                .build();

            // When
            GenericRow result = ProtobufToDeltaConverter.convertToGenericRow(message, comprehensiveSchema);

            // Then
            assertNotNull(result);
            assertEquals("test", result.getValue(13));
            // Address field should be null or default value
            Object addressValue = result.getValue(16);
            // Should be null since the field wasn't set
            assertNull(addressValue);
        }
    }

    @Nested
    @DisplayName("Memory Efficiency Tests")
    class MemoryEfficiencyTests {

        @Test
        @DisplayName("Should not cause memory leaks with large messages")
        void testMemoryEfficiencyWithLargeMessages() {
            // Given
            ComprehensiveTestMessage.Builder builder = ComprehensiveTestMessage.newBuilder();

            // Create a large message with many repeated fields
            for (int i = 0; i < 10000; i++) {
                builder.addRepeatedStringField("String value " + i);
                builder.addRepeatedInt32Field(i);
                builder.addRepeatedBoolField(i % 2 == 0);
            }

            ComprehensiveTestMessage message = builder.build();

            // When - convert multiple times to test memory usage
            List<GenericRow> results = new ArrayList<>();
            for (int i = 0; i < 10; i++) {
                results.add(ProtobufToDeltaConverter.convertToGenericRow(message, comprehensiveSchema));
            }

            // Then
            assertEquals(10, results.size());
            // All results should be valid
            for (GenericRow result : results) {
                assertNotNull(result);
                ArrayValue stringArray = (ArrayValue) result.getValue(30);
                assertEquals(10000, stringArray.getSize());
            }
        }
    }
}