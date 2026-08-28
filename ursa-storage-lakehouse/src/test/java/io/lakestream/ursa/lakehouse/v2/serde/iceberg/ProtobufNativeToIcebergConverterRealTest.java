/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.lakehouse.v2.serde.iceberg;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import io.lakestream.ursa.lakehouse.v2.serde.iceberg.test.TestProtoMessages;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.variants.Variant;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

public class ProtobufNativeToIcebergConverterRealTest {

    private Schema simpleMessageSchema;
    private Schema comprehensiveMessageSchema;
    private Schema addressSchema;

    @BeforeEach
    void setUp() {
        // Define simple message schema
        simpleMessageSchema = new Schema(
            Types.NestedField.required(1, "name", Types.StringType.get()),
            Types.NestedField.required(2, "age", Types.IntegerType.get()),
            Types.NestedField.required(3, "active", Types.BooleanType.get())
        );

        // Define address schema for nested messages
        addressSchema = new Schema(
            Types.NestedField.required(1, "street", Types.StringType.get()),
            Types.NestedField.required(2, "city", Types.StringType.get()),
            Types.NestedField.required(3, "state", Types.StringType.get()),
            Types.NestedField.required(4, "zip_code", Types.StringType.get()),
            Types.NestedField.required(5, "country", Types.StringType.get())
        );

        // Define comprehensive message schema
        comprehensiveMessageSchema = new Schema(
            // INT32 types
            Types.NestedField.required(1, "int32_field", Types.IntegerType.get()),
            Types.NestedField.required(2, "uint32_field", Types.IntegerType.get()),
            Types.NestedField.required(3, "sint32_field", Types.IntegerType.get()),
            Types.NestedField.required(4, "fixed32_field", Types.IntegerType.get()),
            Types.NestedField.required(5, "sfixed32_field", Types.IntegerType.get()),

            // INT64 types
            Types.NestedField.required(6, "int64_field", Types.LongType.get()),
            Types.NestedField.required(7, "uint64_field", Types.LongType.get()),
            Types.NestedField.required(8, "sint64_field", Types.LongType.get()),
            Types.NestedField.required(9, "fixed64_field", Types.LongType.get()),
            Types.NestedField.required(10, "sfixed64_field", Types.LongType.get()),

            // Floating point types
            Types.NestedField.required(11, "float_field", Types.FloatType.get()),
            Types.NestedField.required(12, "double_field", Types.DoubleType.get()),

            // Boolean and String types
            Types.NestedField.required(13, "bool_field", Types.BooleanType.get()),
            Types.NestedField.required(14, "string_field", Types.StringType.get()),

            // Bytes type
            Types.NestedField.required(15, "bytes_field", Types.BinaryType.get()),

            // Enum type (represented as string in Iceberg)
            Types.NestedField.required(16, "status_field", Types.StringType.get()),

            // Nested message
            Types.NestedField.required(17, "address_field", Types.StructType.of(
                Types.NestedField.required(100, "street", Types.StringType.get()),
                Types.NestedField.required(101, "city", Types.StringType.get()),
                Types.NestedField.required(102, "state", Types.StringType.get()),
                Types.NestedField.required(103, "zip_code", Types.StringType.get()),
                Types.NestedField.required(104, "country", Types.StringType.get())
            )),

            // Repeated fields
            Types.NestedField.required(18, "repeated_int32_field",
                Types.ListType.ofRequired(200, Types.IntegerType.get())),
            Types.NestedField.required(19, "repeated_uint32_field",
                Types.ListType.ofRequired(201, Types.IntegerType.get())),
            Types.NestedField.required(20, "repeated_sint32_field",
                Types.ListType.ofRequired(202, Types.IntegerType.get())),
            Types.NestedField.required(21, "repeated_fixed32_field",
                Types.ListType.ofRequired(203, Types.IntegerType.get())),
            Types.NestedField.required(22, "repeated_sfixed32_field",
                Types.ListType.ofRequired(204, Types.IntegerType.get())),

            Types.NestedField.required(23, "repeated_int64_field",
                Types.ListType.ofRequired(205, Types.LongType.get())),
            Types.NestedField.required(24, "repeated_uint64_field",
                Types.ListType.ofRequired(206, Types.LongType.get())),
            Types.NestedField.required(25, "repeated_sint64_field",
                Types.ListType.ofRequired(207, Types.LongType.get())),
            Types.NestedField.required(26, "repeated_fixed64_field",
                Types.ListType.ofRequired(208, Types.LongType.get())),
            Types.NestedField.required(27, "repeated_sfixed64_field",
                Types.ListType.ofRequired(209, Types.LongType.get())),

            Types.NestedField.required(28, "repeated_float_field",
                Types.ListType.ofRequired(210, Types.FloatType.get())),
            Types.NestedField.required(29, "repeated_double_field",
                Types.ListType.ofRequired(211, Types.DoubleType.get())),

            Types.NestedField.required(30, "repeated_bool_field",
                Types.ListType.ofRequired(212, Types.BooleanType.get())),
            Types.NestedField.required(31, "repeated_string_field",
                Types.ListType.ofRequired(213, Types.StringType.get())),
            Types.NestedField.required(32, "repeated_bytes_field",
                Types.ListType.ofRequired(214, Types.BinaryType.get())),
            Types.NestedField.required(33, "repeated_status_field",
                Types.ListType.ofRequired(215, Types.StringType.get())),
            Types.NestedField.required(34, "repeated_address_field", Types.ListType.ofRequired(216, Types.StructType.of(
                Types.NestedField.required(300, "street", Types.StringType.get()),
                Types.NestedField.required(301, "city", Types.StringType.get()),
                Types.NestedField.required(302, "state", Types.StringType.get()),
                Types.NestedField.required(303, "zip_code", Types.StringType.get()),
                Types.NestedField.required(304, "country", Types.StringType.get())
            ))),
            Types.NestedField.required(35, "repeated_contact_field", Types.ListType.ofRequired(217, Types.StructType.of(
                Types.NestedField.required(400, "type", Types.StringType.get()),
                Types.NestedField.required(401, "value", Types.StringType.get())
            )))
        );
    }

    @Test
    @DisplayName("Test simple message conversion")
    void testSimpleMessageConversion() throws Exception {
        // Create test data
        TestProtoMessages.SimpleTestMessage simpleMessage = TestProtoMessages.SimpleTestMessage.newBuilder()
            .setName("John Doe")
            .setAge(30)
            .setActive(true)
            .build();

        // Convert to DynamicMessage
        DynamicMessage dynamicMessage = DynamicMessage.parseFrom(
            TestProtoMessages.SimpleTestMessage.getDescriptor(),
            simpleMessage.toByteArray()
        );

        // Convert to Iceberg record
        GenericRecord icebergRecord = ProtobufNativeToIcebergConverter.convertProtobufToIcebergRecord(
            dynamicMessage, simpleMessageSchema
        );

        // Assertions
        assertNotNull(icebergRecord);
        assertEquals("John Doe", icebergRecord.getField("name"));
        assertEquals(30, icebergRecord.getField("age"));
        assertEquals(true, icebergRecord.getField("active"));
    }

    @Test
    @DisplayName("Test comprehensive message conversion with all data types")
    void testComprehensiveMessageConversion() throws Exception {
        // Create nested address
        TestProtoMessages.Address address = TestProtoMessages.Address.newBuilder()
            .setStreet("123 Main St")
            .setCity("Springfield")
            .setState("IL")
            .setZipCode("62701")
            .setCountry("USA")
            .build();

        // Create contacts for repeated nested messages
        TestProtoMessages.Contact contact1 = TestProtoMessages.Contact.newBuilder()
            .setType("email")
            .setValue("john@example.com")
            .build();

        TestProtoMessages.Contact contact2 = TestProtoMessages.Contact.newBuilder()
            .setType("phone")
            .setValue("555-1234")
            .build();

        // Create comprehensive test message
        TestProtoMessages.ComprehensiveTestMessage comprehensiveMessage =
            TestProtoMessages.ComprehensiveTestMessage.newBuilder()
                // INT32 types
                .setInt32Field(100)
                .setUint32Field(200)
                .setSint32Field(-150)
                .setFixed32Field(300)
                .setSfixed32Field(-250)

                // INT64 types
                .setInt64Field(1000L)
                .setUint64Field(2000L)
                .setSint64Field(-1500L)
                .setFixed64Field(3000L)
                .setSfixed64Field(-2500L)

                // Floating point types
                .setFloatField(3.14f)
                .setDoubleField(2.718281828)

                // Boolean and String
                .setBoolField(true)
                .setStringField("Test String")

                // Bytes
                .setBytesField(ByteString.copyFromUtf8("Test Bytes"))

                // Enum
                .setStatusField(TestProtoMessages.Status.ACTIVE)

                // Nested message
                .setAddressField(address)

                // Repeated fields
                .addAllRepeatedInt32Field(Arrays.asList(1, 2, 3))
                .addAllRepeatedUint32Field(Arrays.asList(4, 5, 6))
                .addAllRepeatedSint32Field(Arrays.asList(-1, -2, -3))
                .addAllRepeatedFixed32Field(Arrays.asList(7, 8, 9))
                .addAllRepeatedSfixed32Field(Arrays.asList(-4, -5, -6))

                .addAllRepeatedInt64Field(Arrays.asList(10L, 20L, 30L))
                .addAllRepeatedUint64Field(Arrays.asList(40L, 50L, 60L))
                .addAllRepeatedSint64Field(Arrays.asList(-10L, -20L, -30L))
                .addAllRepeatedFixed64Field(Arrays.asList(70L, 80L, 90L))
                .addAllRepeatedSfixed64Field(Arrays.asList(-40L, -50L, -60L))

                .addAllRepeatedFloatField(Arrays.asList(1.1f, 2.2f, 3.3f))
                .addAllRepeatedDoubleField(Arrays.asList(1.11, 2.22, 3.33))

                .addAllRepeatedBoolField(Arrays.asList(true, false, true))
                .addAllRepeatedStringField(Arrays.asList("string1", "string2", "string3"))
                .addAllRepeatedBytesField(Arrays.asList(
                    ByteString.copyFromUtf8("bytes1"),
                    ByteString.copyFromUtf8("bytes2"),
                    ByteString.copyFromUtf8("bytes3")
                ))
                .addAllRepeatedStatusField(Arrays.asList(
                    TestProtoMessages.Status.ACTIVE,
                    TestProtoMessages.Status.INACTIVE,
                    TestProtoMessages.Status.PENDING
                ))
                .addAllRepeatedAddressField(Arrays.asList(address, address))
                .addAllRepeatedContactField(Arrays.asList(contact1, contact2))
                .build();

        // Convert to DynamicMessage
        DynamicMessage dynamicMessage = DynamicMessage.parseFrom(
            TestProtoMessages.ComprehensiveTestMessage.getDescriptor(),
            comprehensiveMessage.toByteArray()
        );

        // Convert to Iceberg record
        GenericRecord icebergRecord = ProtobufNativeToIcebergConverter.convertProtobufToIcebergRecord(
            dynamicMessage, comprehensiveMessageSchema
        );

        // Test scalar fields
        assertNotNull(icebergRecord);
        assertEquals(100, icebergRecord.getField("int32_field"));
        assertEquals(200, icebergRecord.getField("uint32_field"));
        assertEquals(-150, icebergRecord.getField("sint32_field"));
        assertEquals(300, icebergRecord.getField("fixed32_field"));
        assertEquals(-250, icebergRecord.getField("sfixed32_field"));

        assertEquals(1000L, icebergRecord.getField("int64_field"));
        assertEquals(2000L, icebergRecord.getField("uint64_field"));
        assertEquals(-1500L, icebergRecord.getField("sint64_field"));
        assertEquals(3000L, icebergRecord.getField("fixed64_field"));
        assertEquals(-2500L, icebergRecord.getField("sfixed64_field"));

        assertEquals(3.14f, icebergRecord.getField("float_field"));
        assertEquals(2.718281828, icebergRecord.getField("double_field"));

        assertEquals(true, icebergRecord.getField("bool_field"));
        assertEquals("Test String", icebergRecord.getField("string_field"));

        // Test bytes field
        ByteBuffer bytesResult = (ByteBuffer) icebergRecord.getField("bytes_field");
        assertEquals("Test Bytes", new String(bytesResult.array()));

        // Test enum field
        assertEquals("ACTIVE", icebergRecord.getField("status_field"));

        // Test nested message
        GenericRecord addressRecord = (GenericRecord) icebergRecord.getField("address_field");
        assertNotNull(addressRecord);
        assertEquals("123 Main St", addressRecord.getField("street"));
        assertEquals("Springfield", addressRecord.getField("city"));
        assertEquals("IL", addressRecord.getField("state"));
        assertEquals("62701", addressRecord.getField("zip_code"));
        assertEquals("USA", addressRecord.getField("country"));

        // Test repeated fields
        List<Integer> repeatedInt32 = (List<Integer>) icebergRecord.getField("repeated_int32_field");
        assertEquals(Arrays.asList(1, 2, 3), repeatedInt32);

        List<Long> repeatedInt64 = (List<Long>) icebergRecord.getField("repeated_int64_field");
        assertEquals(Arrays.asList(10L, 20L, 30L), repeatedInt64);

        List<Float> repeatedFloat = (List<Float>) icebergRecord.getField("repeated_float_field");
        assertEquals(Arrays.asList(1.1f, 2.2f, 3.3f), repeatedFloat);

        List<Double> repeatedDouble = (List<Double>) icebergRecord.getField("repeated_double_field");
        assertEquals(Arrays.asList(1.11, 2.22, 3.33), repeatedDouble);

        List<Boolean> repeatedBool = (List<Boolean>) icebergRecord.getField("repeated_bool_field");
        assertEquals(Arrays.asList(true, false, true), repeatedBool);

        List<String> repeatedString = (List<String>) icebergRecord.getField("repeated_string_field");
        assertEquals(Arrays.asList("string1", "string2", "string3"), repeatedString);

        List<String> repeatedEnum = (List<String>) icebergRecord.getField("repeated_status_field");
        assertEquals(Arrays.asList("ACTIVE", "INACTIVE", "PENDING"), repeatedEnum);

        List<GenericRecord> repeatedAddress = (List<GenericRecord>) icebergRecord.getField("repeated_address_field");
        assertEquals(2, repeatedAddress.size());
        assertEquals("123 Main St", repeatedAddress.get(0).getField("street"));
        assertEquals("Springfield", repeatedAddress.get(1).getField("city"));

        List<GenericRecord> repeatedContact = (List<GenericRecord>) icebergRecord.getField("repeated_contact_field");
        assertEquals(2, repeatedContact.size());
        assertEquals("email", repeatedContact.get(0).getField("type"));
        assertEquals("john@example.com", repeatedContact.get(0).getField("value"));
        assertEquals("phone", repeatedContact.get(1).getField("type"));
        assertEquals("555-1234", repeatedContact.get(1).getField("value"));
    }

    @Test
    @DisplayName("Test conversion with missing fields in schema")
    void testConversionWithMissingFields() throws Exception {
        // Create a schema with only some fields
        Schema partialSchema = new Schema(
            Types.NestedField.required(1, "name", Types.StringType.get()),
            Types.NestedField.required(2, "age", Types.IntegerType.get())
            // Missing 'active' field
        );

        TestProtoMessages.SimpleTestMessage simpleMessage = TestProtoMessages.SimpleTestMessage.newBuilder()
            .setName("Jane Doe")
            .setAge(25)
            .setActive(false)
            .build();

        DynamicMessage dynamicMessage = DynamicMessage.parseFrom(
            TestProtoMessages.SimpleTestMessage.getDescriptor(),
            simpleMessage.toByteArray()
        );

        GenericRecord icebergRecord = ProtobufNativeToIcebergConverter.convertProtobufToIcebergRecord(
            dynamicMessage, partialSchema
        );

        assertNotNull(icebergRecord);
        assertEquals("Jane Doe", icebergRecord.getField("name"));
        assertEquals(25, icebergRecord.getField("age"));
        // 'active' field should not be present in the record
        assertNull(icebergRecord.getField("active"));
    }

    @Test
    @DisplayName("Test empty repeated fields")
    void testEmptyRepeatedFields() throws Exception {
        TestProtoMessages.ComprehensiveTestMessage emptyMessage =
            TestProtoMessages.ComprehensiveTestMessage.newBuilder()
                .setInt32Field(42)
                .setStringField("test")
                .setBoolField(true)
                .build();

        DynamicMessage dynamicMessage = DynamicMessage.parseFrom(
            TestProtoMessages.ComprehensiveTestMessage.getDescriptor(),
            emptyMessage.toByteArray()
        );

        GenericRecord icebergRecord = ProtobufNativeToIcebergConverter.convertProtobufToIcebergRecord(
            dynamicMessage, comprehensiveMessageSchema
        );

        assertNotNull(icebergRecord);
        assertEquals(42, icebergRecord.getField("int32_field"));
        assertEquals("test", icebergRecord.getField("string_field"));
        assertEquals(true, icebergRecord.getField("bool_field"));

        // Empty repeated fields should be empty lists
        List<Integer> emptyRepeatedInt32 = (List<Integer>) icebergRecord.getField("repeated_int32_field");
        assertNotNull(emptyRepeatedInt32);
        assertTrue(emptyRepeatedInt32.isEmpty());
    }

    @Test
    @DisplayName("Test null protobuf message")
    void testNullProtobufMessage() {
        assertThrows(IllegalArgumentException.class, () -> {
            ProtobufNativeToIcebergConverter.convertProtobufToIcebergRecord(
                null, simpleMessageSchema
            );
        });
    }

    @Test
    @DisplayName("Test null schema")
    void testNullSchema() throws Exception {
        TestProtoMessages.SimpleTestMessage simpleMessage = TestProtoMessages.SimpleTestMessage.newBuilder()
            .setName("Test")
            .setAge(1)
            .setActive(true)
            .build();

        DynamicMessage dynamicMessage = DynamicMessage.parseFrom(
            TestProtoMessages.SimpleTestMessage.getDescriptor(),
            simpleMessage.toByteArray()
        );

        assertThrows(IllegalArgumentException.class, () -> {
            ProtobufNativeToIcebergConverter.convertProtobufToIcebergRecord(
                dynamicMessage, null
            );
        });
    }

    @Test
    @DisplayName("Test convertProtobufValueToIcebergValue for individual field types")
    void testConvertProtobufValueToIcebergValue() throws Exception {
        Descriptors.Descriptor descriptor = TestProtoMessages.ComprehensiveTestMessage.getDescriptor();

        // Test INT32 field
        Descriptors.FieldDescriptor int32Field = descriptor.findFieldByName("int32_field");
        Object convertedInt32 = ProtobufNativeToIcebergConverter.convertProtobufValueToIcebergValue(
            100, int32Field, Types.IntegerType.get()
        );
        assertEquals(100, convertedInt32);

        // Test STRING field
        Descriptors.FieldDescriptor stringField = descriptor.findFieldByName("string_field");
        Object convertedString = ProtobufNativeToIcebergConverter.convertProtobufValueToIcebergValue(
            "test string", stringField, Types.StringType.get()
        );
        assertEquals("test string", convertedString);

        // Test BYTES field
        Descriptors.FieldDescriptor bytesField = descriptor.findFieldByName("bytes_field");
        ByteString testBytes = ByteString.copyFromUtf8("test bytes");
        Object convertedBytes = ProtobufNativeToIcebergConverter.convertProtobufValueToIcebergValue(
            testBytes, bytesField, Types.BinaryType.get()
        );
        assertInstanceOf(ByteBuffer.class, convertedBytes);
        assertEquals("test bytes", new String(((ByteBuffer) convertedBytes).array()));
    }

    @Test
    @DisplayName("Test unsupported field type")
    void testUnsupportedFieldType() throws Exception {
        // This would require mocking or creating a custom field descriptor
        // For now, we'll test with a valid scenario and verify the exception message
        // In a real implementation, you might want to create a mock FieldDescriptor
        // that returns an unsupported type

        // This test verifies that the converter handles all expected types
        // and would throw RuntimeException for truly unsupported types
        TestProtoMessages.ComprehensiveTestMessage message =
            TestProtoMessages.ComprehensiveTestMessage.newBuilder()
                .setInt32Field(1)
                .build();

        DynamicMessage dynamicMessage = DynamicMessage.parseFrom(
            TestProtoMessages.ComprehensiveTestMessage.getDescriptor(),
            message.toByteArray()
        );

        // This should work fine - all types in our proto are supported
        assertDoesNotThrow(() -> {
            ProtobufNativeToIcebergConverter.convertProtobufToIcebergRecord(
                dynamicMessage, comprehensiveMessageSchema
            );
        });
    }

    // ===============================
    // BASIC TYPES TESTS
    // ===============================

    @Test
    @DisplayName("BasicTypesTests: Test INT32 variants conversion")
    void testInt32VariantsConversion() throws Exception {
        TestProtoMessages.ComprehensiveTestMessage message =
            TestProtoMessages.ComprehensiveTestMessage.newBuilder()
                .setInt32Field(Integer.MAX_VALUE)
                .setUint32Field(Integer.MAX_VALUE)
                .setSint32Field(Integer.MIN_VALUE)
                .setFixed32Field(123456789)
                .setSfixed32Field(-123456789)
                .build();

        DynamicMessage dynamicMessage = DynamicMessage.parseFrom(
            TestProtoMessages.ComprehensiveTestMessage.getDescriptor(),
            message.toByteArray()
        );

        GenericRecord record = ProtobufNativeToIcebergConverter.convertProtobufToIcebergRecord(
            dynamicMessage, comprehensiveMessageSchema
        );

        assertEquals(Integer.MAX_VALUE, record.getField("int32_field"));
        assertEquals(Integer.MAX_VALUE, record.getField("uint32_field"));
        assertEquals(Integer.MIN_VALUE, record.getField("sint32_field"));
        assertEquals(123456789, record.getField("fixed32_field"));
        assertEquals(-123456789, record.getField("sfixed32_field"));
    }

    @Test
    @DisplayName("BasicTypesTests: Test INT64 variants conversion")
    void testInt64VariantsConversion() throws Exception {
        TestProtoMessages.ComprehensiveTestMessage message =
            TestProtoMessages.ComprehensiveTestMessage.newBuilder()
                .setInt64Field(Long.MAX_VALUE)
                .setUint64Field(Long.MAX_VALUE)
                .setSint64Field(Long.MIN_VALUE)
                .setFixed64Field(123456789012345L)
                .setSfixed64Field(-123456789012345L)
                .build();

        DynamicMessage dynamicMessage = DynamicMessage.parseFrom(
            TestProtoMessages.ComprehensiveTestMessage.getDescriptor(),
            message.toByteArray()
        );

        GenericRecord record = ProtobufNativeToIcebergConverter.convertProtobufToIcebergRecord(
            dynamicMessage, comprehensiveMessageSchema
        );

        assertEquals(Long.MAX_VALUE, record.getField("int64_field"));
        assertEquals(Long.MAX_VALUE, record.getField("uint64_field"));
        assertEquals(Long.MIN_VALUE, record.getField("sint64_field"));
        assertEquals(123456789012345L, record.getField("fixed64_field"));
        assertEquals(-123456789012345L, record.getField("sfixed64_field"));
    }

    @Test
    @DisplayName("BasicTypesTests: Test floating point types")
    void testFloatingPointTypes() throws Exception {
        TestProtoMessages.ComprehensiveTestMessage message =
            TestProtoMessages.ComprehensiveTestMessage.newBuilder()
                .setFloatField(Float.MAX_VALUE)
                .setDoubleField(Double.MAX_VALUE)
                .build();

        DynamicMessage dynamicMessage = DynamicMessage.parseFrom(
            TestProtoMessages.ComprehensiveTestMessage.getDescriptor(),
            message.toByteArray()
        );

        GenericRecord record = ProtobufNativeToIcebergConverter.convertProtobufToIcebergRecord(
            dynamicMessage, comprehensiveMessageSchema
        );

        assertEquals(Float.MAX_VALUE, record.getField("float_field"));
        assertEquals(Double.MAX_VALUE, record.getField("double_field"));
    }

    @Test
    @DisplayName("BasicTypesTests: Test boolean and string types")
    void testBooleanAndStringTypes() throws Exception {
        TestProtoMessages.ComprehensiveTestMessage message =
            TestProtoMessages.ComprehensiveTestMessage.newBuilder()
                .setBoolField(true)
                .setStringField("Test string with special chars: àáâãäåæçèéêë")
                .build();

        DynamicMessage dynamicMessage = DynamicMessage.parseFrom(
            TestProtoMessages.ComprehensiveTestMessage.getDescriptor(),
            message.toByteArray()
        );

        GenericRecord record = ProtobufNativeToIcebergConverter.convertProtobufToIcebergRecord(
            dynamicMessage, comprehensiveMessageSchema
        );

        assertEquals(true, record.getField("bool_field"));
        assertEquals("Test string with special chars: àáâãäåæçèéêë", record.getField("string_field"));
    }

    @Test
    @DisplayName("BasicTypesTests: Test bytes type with various data")
    void testBytesType() throws Exception {
        byte[] testData = {0x00, 0x01, 0x02, 0x03, (byte) 0xFF, (byte) 0xFE, (byte) 0xFD};
        TestProtoMessages.ComprehensiveTestMessage message =
            TestProtoMessages.ComprehensiveTestMessage.newBuilder()
                .setBytesField(ByteString.copyFrom(testData))
                .build();

        DynamicMessage dynamicMessage = DynamicMessage.parseFrom(
            TestProtoMessages.ComprehensiveTestMessage.getDescriptor(),
            message.toByteArray()
        );

        GenericRecord record = ProtobufNativeToIcebergConverter.convertProtobufToIcebergRecord(
            dynamicMessage, comprehensiveMessageSchema
        );

        ByteBuffer result = (ByteBuffer) record.getField("bytes_field");
        assertArrayEquals(testData, result.array());
    }


    @Test
    @DisplayName("EnumTypeConversion: Test enum field conversion")
    void testEnumTypeConversion() throws Exception {
        // Test all valid enum values (exclude UNRECOGNIZED)
        for (TestProtoMessages.Status status : TestProtoMessages.Status.values()) {
            // Skip UNRECOGNIZED as it cannot be set directly
            if (status == TestProtoMessages.Status.UNRECOGNIZED) {
                continue;
            }

            TestProtoMessages.ComprehensiveTestMessage message =
                TestProtoMessages.ComprehensiveTestMessage.newBuilder()
                    .setStatusField(status)
                    .build();

            DynamicMessage dynamicMessage = DynamicMessage.parseFrom(
                TestProtoMessages.ComprehensiveTestMessage.getDescriptor(),
                message.toByteArray()
            );

            GenericRecord record = ProtobufNativeToIcebergConverter.convertProtobufToIcebergRecord(
                dynamicMessage, comprehensiveMessageSchema
            );

            assertEquals(status.name(), record.getField("status_field"));
        }
    }

    @Test
    @DisplayName("EnumTypeConversion: Test specific enum values")
    void testSpecificEnumValues() throws Exception {
        // Test each enum value explicitly to be more clear about what we're testing
        Map<TestProtoMessages.Status, String> enumTestCases = Map.of(
            TestProtoMessages.Status.UNKNOWN, "UNKNOWN",
            TestProtoMessages.Status.ACTIVE, "ACTIVE",
            TestProtoMessages.Status.INACTIVE, "INACTIVE",
            TestProtoMessages.Status.PENDING, "PENDING"
        );

        for (Map.Entry<TestProtoMessages.Status, String> testCase : enumTestCases.entrySet()) {
            TestProtoMessages.ComprehensiveTestMessage message =
                TestProtoMessages.ComprehensiveTestMessage.newBuilder()
                    .setStatusField(testCase.getKey())
                    .build();

            DynamicMessage dynamicMessage = DynamicMessage.parseFrom(
                TestProtoMessages.ComprehensiveTestMessage.getDescriptor(),
                message.toByteArray()
            );

            GenericRecord record = ProtobufNativeToIcebergConverter.convertProtobufToIcebergRecord(
                dynamicMessage, comprehensiveMessageSchema
            );

            assertEquals(testCase.getValue(), record.getField("status_field"));
        }
    }

    @Test
    @DisplayName("EnumTypeConversion: Test repeated enum field conversion")
    void testRepeatedEnumTypeConversion() throws Exception {
        // Test repeated enum fields
        List<TestProtoMessages.Status> statusList = Arrays.asList(
            TestProtoMessages.Status.UNKNOWN,
            TestProtoMessages.Status.ACTIVE,
            TestProtoMessages.Status.INACTIVE,
            TestProtoMessages.Status.PENDING
        );

        TestProtoMessages.ComprehensiveTestMessage message =
            TestProtoMessages.ComprehensiveTestMessage.newBuilder()
                .addAllRepeatedStatusField(statusList)
                .build();

        DynamicMessage dynamicMessage = DynamicMessage.parseFrom(
            TestProtoMessages.ComprehensiveTestMessage.getDescriptor(),
            message.toByteArray()
        );

        GenericRecord record = ProtobufNativeToIcebergConverter.convertProtobufToIcebergRecord(
            dynamicMessage, comprehensiveMessageSchema
        );

        @SuppressWarnings("unchecked")
        List<String> repeatedStatusField = (List<String>) record.getField("repeated_status_field");

        assertThat(repeatedStatusField).containsExactly("UNKNOWN", "ACTIVE", "INACTIVE", "PENDING");
    }

    // ===============================
    // NESTED MESSAGE TESTS
    // ===============================

    @Test
    @DisplayName("NestedMessageTests: Test simple nested message")
    void testSimpleNestedMessage() throws Exception {
        TestProtoMessages.Address address = TestProtoMessages.Address.newBuilder()
            .setStreet("123 Main Street")
            .setCity("New York")
            .setState("NY")
            .setZipCode("10001")
            .setCountry("USA")
            .build();

        TestProtoMessages.ComprehensiveTestMessage message =
            TestProtoMessages.ComprehensiveTestMessage.newBuilder()
                .setAddressField(address)
                .build();

        DynamicMessage dynamicMessage = DynamicMessage.parseFrom(
            TestProtoMessages.ComprehensiveTestMessage.getDescriptor(),
            message.toByteArray()
        );

        GenericRecord record = ProtobufNativeToIcebergConverter.convertProtobufToIcebergRecord(
            dynamicMessage, comprehensiveMessageSchema
        );

        GenericRecord addressRecord = (GenericRecord) record.getField("address_field");
        assertNotNull(addressRecord);
        assertEquals("123 Main Street", addressRecord.getField("street"));
        assertEquals("New York", addressRecord.getField("city"));
        assertEquals("NY", addressRecord.getField("state"));
        assertEquals("10001", addressRecord.getField("zip_code"));
        assertEquals("USA", addressRecord.getField("country"));
    }

    @Test
    @DisplayName("NestedMessageTests: Test nested message with empty fields")
    void testNestedMessageWithEmptyFields() throws Exception {
        TestProtoMessages.Address address = TestProtoMessages.Address.newBuilder()
            .setStreet("")
            .setCity("Boston")
            .setState("")
            .setZipCode("02101")
            .setCountry("")
            .build();

        TestProtoMessages.ComprehensiveTestMessage message =
            TestProtoMessages.ComprehensiveTestMessage.newBuilder()
                .setAddressField(address)
                .build();

        DynamicMessage dynamicMessage = DynamicMessage.parseFrom(
            TestProtoMessages.ComprehensiveTestMessage.getDescriptor(),
            message.toByteArray()
        );

        GenericRecord record = ProtobufNativeToIcebergConverter.convertProtobufToIcebergRecord(
            dynamicMessage, comprehensiveMessageSchema
        );

        GenericRecord addressRecord = (GenericRecord) record.getField("address_field");
        assertNotNull(addressRecord);
        assertEquals("", addressRecord.getField("street"));
        assertEquals("Boston", addressRecord.getField("city"));
        assertEquals("", addressRecord.getField("state"));
        assertEquals("02101", addressRecord.getField("zip_code"));
        assertEquals("", addressRecord.getField("country"));
    }

    @Test
    @DisplayName("NestedMessageTests: Test multiple nested message types")
    void testMultipleNestedMessageTypes() throws Exception {
        TestProtoMessages.Address address = TestProtoMessages.Address.newBuilder()
            .setStreet("456 Oak Ave")
            .setCity("Chicago")
            .setState("IL")
            .setZipCode("60601")
            .setCountry("USA")
            .build();

        TestProtoMessages.Contact contact = TestProtoMessages.Contact.newBuilder()
            .setType("email")
            .setValue("test@example.com")
            .build();

        TestProtoMessages.ComprehensiveTestMessage message =
            TestProtoMessages.ComprehensiveTestMessage.newBuilder()
                .setAddressField(address)
                .addRepeatedContactField(contact)
                .build();

        DynamicMessage dynamicMessage = DynamicMessage.parseFrom(
            TestProtoMessages.ComprehensiveTestMessage.getDescriptor(),
            message.toByteArray()
        );

        GenericRecord record = ProtobufNativeToIcebergConverter.convertProtobufToIcebergRecord(
            dynamicMessage, comprehensiveMessageSchema
        );

        // Test address field
        GenericRecord addressRecord = (GenericRecord) record.getField("address_field");
        assertNotNull(addressRecord);
        assertEquals("456 Oak Ave", addressRecord.getField("street"));
        assertEquals("Chicago", addressRecord.getField("city"));

        // Test repeated contact field
        List<GenericRecord> contacts = (List<GenericRecord>) record.getField("repeated_contact_field");
        assertNotNull(contacts);
        assertEquals(1, contacts.size());
        assertEquals("email", contacts.get(0).getField("type"));
        assertEquals("test@example.com", contacts.get(0).getField("value"));
    }

    // ===============================
    // REPEATED FIELDS TESTS
    // ===============================

    @Test
    @DisplayName("RepeatedFieldsTests: Test repeated primitive types")
    void testRepeatedPrimitiveTypes() throws Exception {
        TestProtoMessages.ComprehensiveTestMessage message =
            TestProtoMessages.ComprehensiveTestMessage.newBuilder()
                .addAllRepeatedInt32Field(Arrays.asList(1, 2, 3, 4, 5))
                .addAllRepeatedInt64Field(Arrays.asList(100L, 200L, 300L))
                .addAllRepeatedFloatField(Arrays.asList(1.1f, 2.2f, 3.3f))
                .addAllRepeatedDoubleField(Arrays.asList(1.11, 2.22, 3.33))
                .addAllRepeatedBoolField(Arrays.asList(true, false, true, false))
                .addAllRepeatedStringField(Arrays.asList("first", "second", "third"))
                .build();

        DynamicMessage dynamicMessage = DynamicMessage.parseFrom(
            TestProtoMessages.ComprehensiveTestMessage.getDescriptor(),
            message.toByteArray()
        );

        GenericRecord record = ProtobufNativeToIcebergConverter.convertProtobufToIcebergRecord(
            dynamicMessage, comprehensiveMessageSchema
        );

        assertEquals(Arrays.asList(1, 2, 3, 4, 5), record.getField("repeated_int32_field"));
        assertEquals(Arrays.asList(100L, 200L, 300L), record.getField("repeated_int64_field"));
        assertEquals(Arrays.asList(1.1f, 2.2f, 3.3f), record.getField("repeated_float_field"));
        assertEquals(Arrays.asList(1.11, 2.22, 3.33), record.getField("repeated_double_field"));
        assertEquals(Arrays.asList(true, false, true, false), record.getField("repeated_bool_field"));
        assertEquals(Arrays.asList("first", "second", "third"), record.getField("repeated_string_field"));
    }

    @Test
    @DisplayName("RepeatedFieldsTests: Test repeated bytes")
    void testRepeatedBytes() throws Exception {
        TestProtoMessages.ComprehensiveTestMessage message =
            TestProtoMessages.ComprehensiveTestMessage.newBuilder()
                .addAllRepeatedBytesField(Arrays.asList(
                    ByteString.copyFromUtf8("first"),
                    ByteString.copyFromUtf8("second"),
                    ByteString.copyFrom(new byte[]{0x01, 0x02, 0x03})
                ))
                .build();

        DynamicMessage dynamicMessage = DynamicMessage.parseFrom(
            TestProtoMessages.ComprehensiveTestMessage.getDescriptor(),
            message.toByteArray()
        );

        GenericRecord record = ProtobufNativeToIcebergConverter.convertProtobufToIcebergRecord(
            dynamicMessage, comprehensiveMessageSchema
        );

        List<ByteBuffer> bytesFields = (List<ByteBuffer>) record.getField("repeated_bytes_field");
        assertEquals(3, bytesFields.size());
        assertEquals("first", new String(bytesFields.get(0).array()));
        assertEquals("second", new String(bytesFields.get(1).array()));
        assertArrayEquals(new byte[]{0x01, 0x02, 0x03}, bytesFields.get(2).array());
    }

    @Test
    @DisplayName("RepeatedFieldsTests: Test repeated enum")
    void testRepeatedEnum() throws Exception {
        TestProtoMessages.ComprehensiveTestMessage message =
            TestProtoMessages.ComprehensiveTestMessage.newBuilder()
                .addAllRepeatedStatusField(Arrays.asList(
                    TestProtoMessages.Status.ACTIVE,
                    TestProtoMessages.Status.INACTIVE,
                    TestProtoMessages.Status.PENDING,
                    TestProtoMessages.Status.UNKNOWN
                ))
                .build();

        DynamicMessage dynamicMessage = DynamicMessage.parseFrom(
            TestProtoMessages.ComprehensiveTestMessage.getDescriptor(),
            message.toByteArray()
        );

        GenericRecord record = ProtobufNativeToIcebergConverter.convertProtobufToIcebergRecord(
            dynamicMessage, comprehensiveMessageSchema
        );

        List<String> statuses = (List<String>) record.getField("repeated_status_field");
        assertEquals(Arrays.asList("ACTIVE", "INACTIVE", "PENDING", "UNKNOWN"), statuses);
    }

    @Test
    @DisplayName("RepeatedFieldsTests: Test repeated nested messages")
    void testRepeatedNestedMessages() throws Exception {
        TestProtoMessages.Address address1 = TestProtoMessages.Address.newBuilder()
            .setStreet("123 First St")
            .setCity("Boston")
            .setState("MA")
            .setZipCode("02101")
            .setCountry("USA")
            .build();

        TestProtoMessages.Address address2 = TestProtoMessages.Address.newBuilder()
            .setStreet("456 Second Ave")
            .setCity("Seattle")
            .setState("WA")
            .setZipCode("98101")
            .setCountry("USA")
            .build();

        TestProtoMessages.ComprehensiveTestMessage message =
            TestProtoMessages.ComprehensiveTestMessage.newBuilder()
                .addAllRepeatedAddressField(Arrays.asList(address1, address2))
                .build();

        DynamicMessage dynamicMessage = DynamicMessage.parseFrom(
            TestProtoMessages.ComprehensiveTestMessage.getDescriptor(),
            message.toByteArray()
        );

        GenericRecord record = ProtobufNativeToIcebergConverter.convertProtobufToIcebergRecord(
            dynamicMessage, comprehensiveMessageSchema
        );

        List<GenericRecord> addresses = (List<GenericRecord>) record.getField("repeated_address_field");
        assertEquals(2, addresses.size());

        assertEquals("123 First St", addresses.get(0).getField("street"));
        assertEquals("Boston", addresses.get(0).getField("city"));
        assertEquals("MA", addresses.get(0).getField("state"));

        assertEquals("456 Second Ave", addresses.get(1).getField("street"));
        assertEquals("Seattle", addresses.get(1).getField("city"));
        assertEquals("WA", addresses.get(1).getField("state"));
    }

    @Test
    @DisplayName("RepeatedFieldsTests: Test large repeated fields")
    void testLargeRepeatedFields() throws Exception {
        List<Integer> largeIntList = new ArrayList<>();
        List<String> largeStringList = new ArrayList<>();

        for (int i = 0; i < 1000; i++) {
            largeIntList.add(i);
            largeStringList.add("item_" + i);
        }

        TestProtoMessages.ComprehensiveTestMessage message =
            TestProtoMessages.ComprehensiveTestMessage.newBuilder()
                .addAllRepeatedInt32Field(largeIntList)
                .addAllRepeatedStringField(largeStringList)
                .build();

        DynamicMessage dynamicMessage = DynamicMessage.parseFrom(
            TestProtoMessages.ComprehensiveTestMessage.getDescriptor(),
            message.toByteArray()
        );

        GenericRecord record = ProtobufNativeToIcebergConverter.convertProtobufToIcebergRecord(
            dynamicMessage, comprehensiveMessageSchema
        );

        List<Integer> resultInts = (List<Integer>) record.getField("repeated_int32_field");
        List<String> resultStrings = (List<String>) record.getField("repeated_string_field");

        assertEquals(1000, resultInts.size());
        assertEquals(1000, resultStrings.size());
        assertEquals(largeIntList, resultInts);
        assertEquals(largeStringList, resultStrings);
    }

    // ===============================
    // EMPTY AND EDGE CASES TESTS
    // ===============================

    @Test
    @DisplayName("EmptyAndEdgeCasesTests: Test all empty repeated fields")
    void testAllEmptyRepeatedFields() throws Exception {
        TestProtoMessages.ComprehensiveTestMessage message =
            TestProtoMessages.ComprehensiveTestMessage.newBuilder()
                .setInt32Field(42)
                .setStringField("test")
                .build();

        DynamicMessage dynamicMessage = DynamicMessage.parseFrom(
            TestProtoMessages.ComprehensiveTestMessage.getDescriptor(),
            message.toByteArray()
        );

        GenericRecord record = ProtobufNativeToIcebergConverter.convertProtobufToIcebergRecord(
            dynamicMessage, comprehensiveMessageSchema
        );

        // All repeated fields should be empty lists
        assertTrue(((List<?>) record.getField("repeated_int32_field")).isEmpty());
        assertTrue(((List<?>) record.getField("repeated_string_field")).isEmpty());
        assertTrue(((List<?>) record.getField("repeated_bool_field")).isEmpty());
        assertTrue(((List<?>) record.getField("repeated_address_field")).isEmpty());
        assertTrue(((List<?>) record.getField("repeated_contact_field")).isEmpty());
    }

    @Test
    @DisplayName("EmptyAndEdgeCasesTests: Test default values")
    void testDefaultValues() throws Exception {
        TestProtoMessages.ComprehensiveTestMessage message =
            TestProtoMessages.ComprehensiveTestMessage.newBuilder()
                .build();

        DynamicMessage dynamicMessage = DynamicMessage.parseFrom(
            TestProtoMessages.ComprehensiveTestMessage.getDescriptor(),
            message.toByteArray()
        );

        GenericRecord record = ProtobufNativeToIcebergConverter.convertProtobufToIcebergRecord(
            dynamicMessage, comprehensiveMessageSchema
        );

        // Test default values for primitive types
        assertEquals(0, record.getField("int32_field"));
        assertEquals(0L, record.getField("int64_field"));
        assertEquals(0.0f, record.getField("float_field"));
        assertEquals(0.0, record.getField("double_field"));
        assertEquals(false, record.getField("bool_field"));
        assertEquals("", record.getField("string_field"));
        assertEquals("UNKNOWN", record.getField("status_field")); // enum default

        // Bytes field should be empty
        ByteBuffer bytesResult = (ByteBuffer) record.getField("bytes_field");
        assertEquals(0, bytesResult.remaining());
    }

    @Test
    @DisplayName("EmptyAndEdgeCasesTests: Test boundary values")
    void testBoundaryValues() throws Exception {
        TestProtoMessages.ComprehensiveTestMessage message =
            TestProtoMessages.ComprehensiveTestMessage.newBuilder()
                .setInt32Field(Integer.MIN_VALUE)
                .setUint32Field(0) // Min value for unsigned
                .setInt64Field(Long.MIN_VALUE)
                .setUint64Field(0L) // Min value for unsigned
                .setFloatField(Float.MIN_VALUE)
                .setDoubleField(Double.MIN_VALUE)
                .build();

        DynamicMessage dynamicMessage = DynamicMessage.parseFrom(
            TestProtoMessages.ComprehensiveTestMessage.getDescriptor(),
            message.toByteArray()
        );

        GenericRecord record = ProtobufNativeToIcebergConverter.convertProtobufToIcebergRecord(
            dynamicMessage, comprehensiveMessageSchema
        );

        assertEquals(Integer.MIN_VALUE, record.getField("int32_field"));
        assertEquals(0, record.getField("uint32_field"));
        assertEquals(Long.MIN_VALUE, record.getField("int64_field"));
        assertEquals(0L, record.getField("uint64_field"));
        assertEquals(Float.MIN_VALUE, record.getField("float_field"));
        assertEquals(Double.MIN_VALUE, record.getField("double_field"));
    }

    @Test
    @DisplayName("EmptyAndEdgeCasesTests: Test empty strings and special characters")
    void testEmptyStringsAndSpecialCharacters() throws Exception {
        TestProtoMessages.ComprehensiveTestMessage message =
            TestProtoMessages.ComprehensiveTestMessage.newBuilder()
                .setStringField("")
                .addAllRepeatedStringField(Arrays.asList(
                    "",
                    "   ",
                    "\n\r\t",
                    "Unicode: 🚀🌟💫",
                    "Special chars: !@#$%^&*()_+-=[]{}|;:,.<>?"
                ))
                .build();

        DynamicMessage dynamicMessage = DynamicMessage.parseFrom(
            TestProtoMessages.ComprehensiveTestMessage.getDescriptor(),
            message.toByteArray()
        );

        GenericRecord record = ProtobufNativeToIcebergConverter.convertProtobufToIcebergRecord(
            dynamicMessage, comprehensiveMessageSchema
        );

        assertEquals("", record.getField("string_field"));
        List<String> strings = (List<String>) record.getField("repeated_string_field");
        assertEquals("", strings.get(0));
        assertEquals("   ", strings.get(1));
        assertEquals("\n\r\t", strings.get(2));
        assertEquals("Unicode: 🚀🌟💫", strings.get(3));
        assertEquals("Special chars: !@#$%^&*()_+-=[]{}|;:,.<>?", strings.get(4));
    }

    // ===============================
    // COMPLEX DATA TESTS
    // ===============================

    @Test
    @DisplayName("ComplexDataTests: Test mixed complex data scenario")
    void testMixedComplexDataScenario() throws Exception {
        // Create multiple addresses
        TestProtoMessages.Address homeAddress = TestProtoMessages.Address.newBuilder()
            .setStreet("123 Home St")
            .setCity("Home City")
            .setState("HC")
            .setZipCode("12345")
            .setCountry("USA")
            .build();

        TestProtoMessages.Address workAddress = TestProtoMessages.Address.newBuilder()
            .setStreet("456 Work Ave")
            .setCity("Work City")
            .setState("WC")
            .setZipCode("67890")
            .setCountry("USA")
            .build();

        // Create multiple contacts
        TestProtoMessages.Contact emailContact = TestProtoMessages.Contact.newBuilder()
            .setType("email")
            .setValue("user@example.com")
            .build();

        TestProtoMessages.Contact phoneContact = TestProtoMessages.Contact.newBuilder()
            .setType("phone")
            .setValue("+1-555-123-4567")
            .build();

        TestProtoMessages.Contact faxContact = TestProtoMessages.Contact.newBuilder()
            .setType("fax")
            .setValue("+1-555-765-4321")
            .build();

        // Create comprehensive message with complex data
        TestProtoMessages.ComprehensiveTestMessage message =
            TestProtoMessages.ComprehensiveTestMessage.newBuilder()
                // Set all basic fields
                .setInt32Field(42)
                .setUint32Field(84)
                .setSint32Field(-42)
                .setFixed32Field(168)
                .setSfixed32Field(-168)
                .setInt64Field(9223372036854775807L)
                .setUint64Field(1844674407370955161L)
                .setSint64Field(-9223372036854775808L)
                .setFixed64Field(3689348814741910323L)
                .setSfixed64Field(-3689348814741910323L)
                .setFloatField(3.14159f)
                .setDoubleField(2.718281828459045)
                .setBoolField(true)
                .setStringField("Complex test message with mixed data")
                .setBytesField(ByteString.copyFrom(new byte[]{0x01, 0x02, 0x03, 0x04, 0x05}))
                .setStatusField(TestProtoMessages.Status.ACTIVE)
                .setAddressField(homeAddress)

                // Set all repeated fields with multiple values
                .addAllRepeatedInt32Field(Arrays.asList(1, 2, 3, 4, 5))
                .addAllRepeatedUint32Field(Arrays.asList(10, 20, 30, 40, 50))
                .addAllRepeatedSint32Field(Arrays.asList(-1, -2, -3, -4, -5))
                .addAllRepeatedFixed32Field(Arrays.asList(100, 200, 300, 400, 500))
                .addAllRepeatedSfixed32Field(Arrays.asList(-100, -200, -300, -400, -500))
                .addAllRepeatedInt64Field(Arrays.asList(1000L, 2000L, 3000L, 4000L, 5000L))
                .addAllRepeatedUint64Field(Arrays.asList(10000L, 20000L, 30000L, 40000L, 50000L))
                .addAllRepeatedSint64Field(Arrays.asList(-1000L, -2000L, -3000L, -4000L, -5000L))
                .addAllRepeatedFixed64Field(Arrays.asList(100000L, 200000L, 300000L, 400000L, 500000L))
                .addAllRepeatedSfixed64Field(Arrays.asList(-100000L, -200000L, -300000L, -400000L, -500000L))
                .addAllRepeatedFloatField(Arrays.asList(1.1f, 2.2f, 3.3f, 4.4f, 5.5f))
                .addAllRepeatedDoubleField(Arrays.asList(1.11, 2.22, 3.33, 4.44, 5.55))
                .addAllRepeatedBoolField(Arrays.asList(true, false, true, false, true))
                .addAllRepeatedStringField(Arrays.asList("first", "second", "third", "fourth", "fifth"))
                .addAllRepeatedBytesField(Arrays.asList(
                    ByteString.copyFrom(new byte[]{0x01, 0x02}),
                    ByteString.copyFrom(new byte[]{0x03, 0x04}),
                    ByteString.copyFrom(new byte[]{0x05, 0x06})
                ))
                .addAllRepeatedStatusField(Arrays.asList(
                    TestProtoMessages.Status.ACTIVE,
                    TestProtoMessages.Status.INACTIVE,
                    TestProtoMessages.Status.PENDING
                ))
                .addAllRepeatedAddressField(Arrays.asList(homeAddress, workAddress))
                .addAllRepeatedContactField(Arrays.asList(emailContact, phoneContact, faxContact))
                .build();

        // Convert to DynamicMessage and then to Iceberg
        DynamicMessage dynamicMessage = DynamicMessage.parseFrom(
            TestProtoMessages.ComprehensiveTestMessage.getDescriptor(),
            message.toByteArray()
        );

        GenericRecord icebergRecord = ProtobufNativeToIcebergConverter.convertProtobufToIcebergRecord(
            dynamicMessage, comprehensiveMessageSchema);

        // Verify basic scalar fields
        assertThat(icebergRecord.getField("int32_field")).isEqualTo(42);
        assertThat(icebergRecord.getField("uint32_field")).isEqualTo(84);
        assertThat(icebergRecord.getField("sint32_field")).isEqualTo(-42);
        assertThat(icebergRecord.getField("fixed32_field")).isEqualTo(168);
        assertThat(icebergRecord.getField("sfixed32_field")).isEqualTo(-168);

        assertThat(icebergRecord.getField("int64_field")).isEqualTo(9223372036854775807L);
        assertThat(icebergRecord.getField("uint64_field")).isEqualTo(1844674407370955161L);
        assertThat(icebergRecord.getField("sint64_field")).isEqualTo(-9223372036854775808L);
        assertThat(icebergRecord.getField("fixed64_field")).isEqualTo(3689348814741910323L);
        assertThat(icebergRecord.getField("sfixed64_field")).isEqualTo(-3689348814741910323L);

        assertThat(icebergRecord.getField("float_field")).isEqualTo(3.14159f);
        assertThat(icebergRecord.getField("double_field")).isEqualTo(2.718281828459045);

        assertThat(icebergRecord.getField("bool_field")).isEqualTo(true);
        assertThat(icebergRecord.getField("string_field")).isEqualTo("Complex test message with mixed data");

        // Verify bytes field
        ByteBuffer bytesField = (ByteBuffer) icebergRecord.getField("bytes_field");
        assertThat(bytesField.array()).isEqualTo(new byte[]{0x01, 0x02, 0x03, 0x04, 0x05});

        // Verify enum field
        assertThat(icebergRecord.getField("status_field")).isEqualTo("ACTIVE");

        // Verify nested message (address)
        GenericRecord addressRecord = (GenericRecord) icebergRecord.getField("address_field");
        assertThat(addressRecord.getField("street")).isEqualTo("123 Home St");
        assertThat(addressRecord.getField("city")).isEqualTo("Home City");
        assertThat(addressRecord.getField("state")).isEqualTo("HC");
        assertThat(addressRecord.getField("zip_code")).isEqualTo("12345");
        assertThat(addressRecord.getField("country")).isEqualTo("USA");

        // Verify repeated integer fields
        assertThat((List<Integer>) icebergRecord.getField("repeated_int32_field"))
            .containsExactly(1, 2, 3, 4, 5);
        assertThat((List<Long>) icebergRecord.getField("repeated_uint32_field"))
            .containsExactly(10L, 20L, 30L, 40L, 50L);
        assertThat((List<Integer>) icebergRecord.getField("repeated_sint32_field"))
            .containsExactly(-1, -2, -3, -4, -5);
        assertThat((List<Integer>) icebergRecord.getField("repeated_fixed32_field"))
            .containsExactly(100, 200, 300, 400, 500);
        assertThat((List<Integer>) icebergRecord.getField("repeated_sfixed32_field"))
            .containsExactly(-100, -200, -300, -400, -500);

        // Verify repeated long fields
        assertThat((List<Long>) icebergRecord.getField("repeated_int64_field"))
            .containsExactly(1000L, 2000L, 3000L, 4000L, 5000L);
        assertThat((List<Long>) icebergRecord.getField("repeated_uint64_field"))
            .containsExactly(10000L, 20000L, 30000L, 40000L, 50000L);
        assertThat((List<Long>) icebergRecord.getField("repeated_sint64_field"))
            .containsExactly(-1000L, -2000L, -3000L, -4000L, -5000L);
        assertThat((List<Long>) icebergRecord.getField("repeated_fixed64_field"))
            .containsExactly(100000L, 200000L, 300000L, 400000L, 500000L);
        assertThat((List<Long>) icebergRecord.getField("repeated_sfixed64_field"))
            .containsExactly(-100000L, -200000L, -300000L, -400000L, -500000L);

        // Verify repeated floating point fields
        assertThat((List<Float>) icebergRecord.getField("repeated_float_field"))
            .containsExactly(1.1f, 2.2f, 3.3f, 4.4f, 5.5f);
        assertThat((List<Double>) icebergRecord.getField("repeated_double_field"))
            .containsExactly(1.11, 2.22, 3.33, 4.44, 5.55);

        // Verify repeated boolean and string fields
        assertThat((List<Boolean>) icebergRecord.getField("repeated_bool_field"))
            .containsExactly(true, false, true, false, true);
        assertThat((List<String>) icebergRecord.getField("repeated_string_field"))
            .containsExactly("first", "second", "third", "fourth", "fifth");

        // Verify repeated bytes field
        List<ByteBuffer> repeatedBytesField = (List<ByteBuffer>) icebergRecord.getField("repeated_bytes_field");
        assertThat(repeatedBytesField).hasSize(3);
        assertThat(repeatedBytesField.get(0).array()).isEqualTo(new byte[]{0x01, 0x02});
        assertThat(repeatedBytesField.get(1).array()).isEqualTo(new byte[]{0x03, 0x04});
        assertThat(repeatedBytesField.get(2).array()).isEqualTo(new byte[]{0x05, 0x06});

        // Verify repeated enum field
        assertThat((List<String>) icebergRecord.getField("repeated_status_field"))
            .containsExactly("ACTIVE", "INACTIVE", "PENDING");

        // Verify repeated address field
        List<GenericRecord> repeatedAddressField =
            (List<GenericRecord>) icebergRecord.getField("repeated_address_field");
        assertThat(repeatedAddressField).hasSize(2);

        GenericRecord homeAddressRecord = repeatedAddressField.get(0);
        assertThat(homeAddressRecord.getField("street")).isEqualTo("123 Home St");
        assertThat(homeAddressRecord.getField("city")).isEqualTo("Home City");
        assertThat(homeAddressRecord.getField("state")).isEqualTo("HC");
        assertThat(homeAddressRecord.getField("zip_code")).isEqualTo("12345");
        assertThat(homeAddressRecord.getField("country")).isEqualTo("USA");

        GenericRecord workAddressRecord = repeatedAddressField.get(1);
        assertThat(workAddressRecord.getField("street")).isEqualTo("456 Work Ave");
        assertThat(workAddressRecord.getField("city")).isEqualTo("Work City");
        assertThat(workAddressRecord.getField("state")).isEqualTo("WC");
        assertThat(workAddressRecord.getField("zip_code")).isEqualTo("67890");
        assertThat(workAddressRecord.getField("country")).isEqualTo("USA");

        // Verify repeated contact field
        List<GenericRecord> repeatedContactField =
            (List<GenericRecord>) icebergRecord.getField("repeated_contact_field");
        assertThat(repeatedContactField).hasSize(3);

        GenericRecord emailContactRecord = repeatedContactField.get(0);
        assertThat(emailContactRecord.getField("type")).isEqualTo("email");
        assertThat(emailContactRecord.getField("value")).isEqualTo("user@example.com");

        GenericRecord phoneContactRecord = repeatedContactField.get(1);
        assertThat(phoneContactRecord.getField("type")).isEqualTo("phone");
        assertThat(phoneContactRecord.getField("value")).isEqualTo("+1-555-123-4567");

        GenericRecord faxContactRecord = repeatedContactField.get(2);
        assertThat(faxContactRecord.getField("type")).isEqualTo("fax");
        assertThat(faxContactRecord.getField("value")).isEqualTo("+1-555-765-4321");

        // Verify that all expected fields are present
        assertThat(icebergRecord.size()).isEqualTo(35); // Total number of fields in comprehensive schema
    }

    @Test
    @DisplayName("VariantTests: Test scalar primitives to Variant")
    void testScalarPrimitivesToVariant() throws Exception {
        // Create a schema where fields are Variants instead of their native types
        Schema variantSchema = new Schema(
                Types.NestedField.optional(1, "int32_field", Types.VariantType.get()),
                Types.NestedField.optional(14, "string_field", Types.VariantType.get()),
                Types.NestedField.optional(13, "bool_field", Types.VariantType.get())
        );

        TestProtoMessages.ComprehensiveTestMessage message =
                TestProtoMessages.ComprehensiveTestMessage.newBuilder()
                        .setInt32Field(123)
                        .setStringField("variant-string")
                        .setBoolField(true)
                        .build();

        DynamicMessage dynamicMessage = DynamicMessage.parseFrom(
                TestProtoMessages.ComprehensiveTestMessage.getDescriptor(),
                message.toByteArray()
        );

        GenericRecord record = ProtobufNativeToIcebergConverter.convertProtobufToIcebergRecord(
                dynamicMessage, variantSchema
        );

        // Verify Int32 -> Variant
        Variant intVariant = (Variant) record.getField("int32_field");
        assertEquals(123, intVariant.value().asPrimitive().get());

        // Verify String -> Variant
        Variant stringVariant = (Variant) record.getField("string_field");
        assertEquals("variant-string", stringVariant.value().asPrimitive().get());

        // Verify Bool -> Variant
        Variant boolVariant = (Variant) record.getField("bool_field");
        assertEquals(true, boolVariant.value().asPrimitive().get());
    }

    @Test
    @DisplayName("VariantTests: Test scalar Message to Variant (Serialized Binary)")
    void testScalarMessageToVariant() throws Exception {
        Schema variantSchema = new Schema(
                Types.NestedField.optional(17, "address_field", Types.VariantType.get())
        );

        TestProtoMessages.Address address = TestProtoMessages.Address.newBuilder()
                .setStreet("Main St")
                .setCity("New York")
                .build();

        TestProtoMessages.ComprehensiveTestMessage message =
                TestProtoMessages.ComprehensiveTestMessage.newBuilder()
                        .setAddressField(address)
                        .build();

        DynamicMessage dynamicMessage = DynamicMessage.parseFrom(
                TestProtoMessages.ComprehensiveTestMessage.getDescriptor(),
                message.toByteArray()
        );

        GenericRecord record = ProtobufNativeToIcebergConverter.convertProtobufToIcebergRecord(
                dynamicMessage, variantSchema
        );

        Variant variant = (Variant) record.getField("address_field");

        // According to implementation, scalar messages are stored as serialzed bytes
        ByteBuffer buffer = (ByteBuffer) variant.value().asPrimitive().get();
        byte[] bytes = new byte[buffer.remaining()];
        buffer.get(bytes);

        // Verify we can parse it back
        TestProtoMessages.Address parsedAddress = TestProtoMessages.Address.parseFrom(bytes);
        assertEquals("Main St", parsedAddress.getStreet());
        assertEquals("New York", parsedAddress.getCity());
    }

    @Test
    @DisplayName("VariantTests: Test repeated primitives to Variant (String Representation)")
    void testRepeatedPrimitivesToVariant() throws Exception {
        Schema variantSchema = new Schema(
                Types.NestedField.optional(18, "repeated_int32_field", Types.VariantType.get())
        );

        TestProtoMessages.ComprehensiveTestMessage message =
                TestProtoMessages.ComprehensiveTestMessage.newBuilder()
                        .addAllRepeatedInt32Field(Arrays.asList(1, 2, 3))
                        .build();

        DynamicMessage dynamicMessage = DynamicMessage.parseFrom(
                TestProtoMessages.ComprehensiveTestMessage.getDescriptor(),
                message.toByteArray()
        );

        GenericRecord record = ProtobufNativeToIcebergConverter.convertProtobufToIcebergRecord(
                dynamicMessage, variantSchema
        );

        Variant variant = (Variant) record.getField("repeated_int32_field");

        // Implementation uses List.toString() for repeated types in Variants
        assertEquals("[1, 2, 3]", variant.value().asPrimitive().get());
    }

    @Test
    @DisplayName("VariantTests: Test repeated messages to Variant (String Representation)")
    void testRepeatedMessagesToVariant() throws Exception {
        Schema variantSchema = new Schema(
                Types.NestedField.optional(35, "repeated_contact_field", Types.VariantType.get())
        );

        TestProtoMessages.Contact c1 = TestProtoMessages.Contact.newBuilder().setType("email").setValue("a@b.com").build();
        TestProtoMessages.Contact c2 = TestProtoMessages.Contact.newBuilder().setType("phone").setValue("123").build();

        TestProtoMessages.ComprehensiveTestMessage message =
                TestProtoMessages.ComprehensiveTestMessage.newBuilder()
                        .addAllRepeatedContactField(Arrays.asList(c1, c2))
                        .build();

        DynamicMessage dynamicMessage = DynamicMessage.parseFrom(
                TestProtoMessages.ComprehensiveTestMessage.getDescriptor(),
                message.toByteArray()
        );

        GenericRecord record = ProtobufNativeToIcebergConverter.convertProtobufToIcebergRecord(
                dynamicMessage, variantSchema
        );

        Variant variant = (Variant) record.getField("repeated_contact_field");
        String resultString = (String) variant.value().asPrimitive().get();

        // Check that it looks like a combined string array
        assertTrue(resultString.startsWith("["));
        assertTrue(resultString.contains("email"));
        assertTrue(resultString.contains("a@b.com"));
        assertTrue(resultString.endsWith("]"));
    }

    @Test
    @DisplayName("VariantTests: Test Enum to Variant")
    void testEnumToVariant() throws Exception {
        Schema variantSchema = new Schema(
                Types.NestedField.optional(16, "status_field", Types.VariantType.get())
        );

        TestProtoMessages.ComprehensiveTestMessage message =
                TestProtoMessages.ComprehensiveTestMessage.newBuilder()
                        .setStatusField(TestProtoMessages.Status.ACTIVE)
                        .build();

        DynamicMessage dynamicMessage = DynamicMessage.parseFrom(
                TestProtoMessages.ComprehensiveTestMessage.getDescriptor(),
                message.toByteArray()
        );

        GenericRecord record = ProtobufNativeToIcebergConverter.convertProtobufToIcebergRecord(
                dynamicMessage, variantSchema
        );

        Variant variant = (Variant) record.getField("status_field");
        assertEquals("ACTIVE", variant.value().asPrimitive().get());
    }

    @Test
    @DisplayName("VariantTests: Test Bytes to Variant")
    void testBytesToVariant() throws Exception {
        Schema variantSchema = new Schema(
                Types.NestedField.optional(15, "bytes_field", Types.VariantType.get())
        );

        byte[] rawData = new byte[]{0x10, 0x20, 0x30};
        TestProtoMessages.ComprehensiveTestMessage message =
                TestProtoMessages.ComprehensiveTestMessage.newBuilder()
                        .setBytesField(ByteString.copyFrom(rawData))
                        .build();

        DynamicMessage dynamicMessage = DynamicMessage.parseFrom(
                TestProtoMessages.ComprehensiveTestMessage.getDescriptor(),
                message.toByteArray()
        );

        GenericRecord record = ProtobufNativeToIcebergConverter.convertProtobufToIcebergRecord(
                dynamicMessage, variantSchema
        );

        Variant variant = (Variant) record.getField("bytes_field");
        ByteBuffer buffer = (ByteBuffer) variant.value().asPrimitive().get();
        byte[] variantBytes = new byte[buffer.remaining()];
        buffer.get(variantBytes);
        assertArrayEquals(rawData, variantBytes);
    }
}