/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.lakehouse.v2.serde.iceberg;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

public class ProtobufNativeToIcebergConverterTest {

    @Mock
    private DynamicMessage mockProtobufMessage;

    @Mock
    private Descriptors.Descriptor mockDescriptor;

    @Mock
    private Descriptors.FieldDescriptor mockFieldDescriptor;

    @Mock
    private Descriptors.EnumValueDescriptor mockEnumValueDescriptor;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
    }

    @Nested
    @DisplayName("Integer Type Tests")
    class IntegerTypeTests {

        @Test
        @DisplayName("Convert INT32 single value")
        void testConvertInt32SingleValue() {
            // Given
            when(mockFieldDescriptor.getType()).thenReturn(Descriptors.FieldDescriptor.Type.INT32);
            when(mockFieldDescriptor.isRepeated()).thenReturn(false);
            Integer protobufValue = 42;
            Types.IntegerType icebergType = Types.IntegerType.get();

            // When
            Object result = ProtobufNativeToIcebergConverter.convertProtobufValueToIcebergValue(
                protobufValue, mockFieldDescriptor, icebergType);

            // Then
            assertEquals(42, result);
            assertInstanceOf(Integer.class, result);
        }

        @Test
        @DisplayName("Convert INT32 repeated values")
        void testConvertInt32RepeatedValues() {
            // Given
            when(mockFieldDescriptor.getType()).thenReturn(Descriptors.FieldDescriptor.Type.INT32);
            when(mockFieldDescriptor.isRepeated()).thenReturn(true);
            List<Integer> protobufValue = Arrays.asList(1, 2, 3, 4, 5);
            Types.ListType icebergType = Types.ListType.ofRequired(1, Types.IntegerType.get());

            // When
            Object result = ProtobufNativeToIcebergConverter.convertProtobufValueToIcebergValue(
                protobufValue, mockFieldDescriptor, icebergType);

            // Then
            assertInstanceOf(List.class, result);
            @SuppressWarnings("unchecked")
            List<Integer> resultList = (List<Integer>) result;
            assertEquals(Arrays.asList(1, 2, 3, 4, 5), resultList);
        }

        @Test
        @DisplayName("Convert UINT32 single value")
        void testConvertUInt32SingleValue() {
            // Given
            when(mockFieldDescriptor.getType()).thenReturn(Descriptors.FieldDescriptor.Type.UINT32);
            when(mockFieldDescriptor.isRepeated()).thenReturn(true);
            Long protobufValue = (long) Integer.MAX_VALUE + 1; // Max uint32
            Types.IntegerType icebergType = Types.IntegerType.get();

            // When
            Object result = ProtobufNativeToIcebergConverter.convertProtobufValueToIcebergValue(
                List.of(protobufValue), mockFieldDescriptor, icebergType);

            // Then
            // overflow
            assertEquals(List.of(protobufValue.intValue()), result); // Converted to int
            assertInstanceOf(List.class, result);

            when(mockFieldDescriptor.getType()).thenReturn(Descriptors.FieldDescriptor.Type.UINT32);
            when(mockFieldDescriptor.isRepeated()).thenReturn(false);

            // When
            result = ProtobufNativeToIcebergConverter.convertProtobufValueToIcebergValue(
                    protobufValue, mockFieldDescriptor, icebergType);

            // Then
            // overflow
            assertEquals(protobufValue.intValue(), result); // Converted to int
            assertInstanceOf(Integer.class, result);
        }

        @Test
        @DisplayName("Convert INT64 single value")
        void testConvertInt64SingleValue() {
            // Given
            when(mockFieldDescriptor.getType()).thenReturn(Descriptors.FieldDescriptor.Type.INT64);
            when(mockFieldDescriptor.isRepeated()).thenReturn(false);
            Long protobufValue = 9223372036854775807L; // Max long
            Types.LongType icebergType = Types.LongType.get();

            // When
            Object result = ProtobufNativeToIcebergConverter.convertProtobufValueToIcebergValue(
                protobufValue, mockFieldDescriptor, icebergType);

            // Then
            assertEquals(9223372036854775807L, result);
            assertInstanceOf(Long.class, result);
        }

        @Test
        @DisplayName("Convert INT64 repeated values")
        void testConvertInt64RepeatedValues() {
            // Given
            when(mockFieldDescriptor.getType()).thenReturn(Descriptors.FieldDescriptor.Type.INT64);
            when(mockFieldDescriptor.isRepeated()).thenReturn(true);
            List<Long> protobufValue = Arrays.asList(1L, 2L, 3L);
            Types.ListType icebergType = Types.ListType.ofRequired(1, Types.LongType.get());

            // When
            Object result = ProtobufNativeToIcebergConverter.convertProtobufValueToIcebergValue(
                protobufValue, mockFieldDescriptor, icebergType);

            // Then
            assertInstanceOf(List.class, result);
            @SuppressWarnings("unchecked")
            List<Long> resultList = (List<Long>) result;
            assertEquals(Arrays.asList(1L, 2L, 3L), resultList);
        }
    }

    @Nested
    @DisplayName("Float Type Tests")
    class FloatTypeTests {

        @Test
        @DisplayName("Convert FLOAT single value")
        void testConvertFloatSingleValue() {
            // Given
            when(mockFieldDescriptor.getType()).thenReturn(Descriptors.FieldDescriptor.Type.FLOAT);
            when(mockFieldDescriptor.isRepeated()).thenReturn(false);
            Float protobufValue = 3.14f;
            Types.FloatType icebergType = Types.FloatType.get();

            // When
            Object result = ProtobufNativeToIcebergConverter.convertProtobufValueToIcebergValue(
                protobufValue, mockFieldDescriptor, icebergType);

            // Then
            assertEquals(3.14f, result);
            assertInstanceOf(Float.class, result);
        }

        @Test
        @DisplayName("Convert FLOAT repeated values")
        void testConvertFloatRepeatedValues() {
            // Given
            when(mockFieldDescriptor.getType()).thenReturn(Descriptors.FieldDescriptor.Type.FLOAT);
            when(mockFieldDescriptor.isRepeated()).thenReturn(true);
            List<Float> protobufValue = Arrays.asList(1.1f, 2.2f, 3.3f);
            Types.ListType icebergType = Types.ListType.ofRequired(1, Types.FloatType.get());

            // When
            Object result = ProtobufNativeToIcebergConverter.convertProtobufValueToIcebergValue(
                protobufValue, mockFieldDescriptor, icebergType);

            // Then
            assertInstanceOf(List.class, result);
            @SuppressWarnings("unchecked")
            List<Float> resultList = (List<Float>) result;
            assertEquals(Arrays.asList(1.1f, 2.2f, 3.3f), resultList);
        }

        @Test
        @DisplayName("Convert DOUBLE single value")
        void testConvertDoubleSingleValue() {
            // Given
            when(mockFieldDescriptor.getType()).thenReturn(Descriptors.FieldDescriptor.Type.DOUBLE);
            when(mockFieldDescriptor.isRepeated()).thenReturn(false);
            Double protobufValue = 3.141592653589793;
            Types.DoubleType icebergType = Types.DoubleType.get();

            // When
            Object result = ProtobufNativeToIcebergConverter.convertProtobufValueToIcebergValue(
                protobufValue, mockFieldDescriptor, icebergType);

            // Then
            assertEquals(3.141592653589793, result);
            assertInstanceOf(Double.class, result);
        }

        @Test
        @DisplayName("Convert DOUBLE repeated values")
        void testConvertDoubleRepeatedValues() {
            // Given
            when(mockFieldDescriptor.getType()).thenReturn(Descriptors.FieldDescriptor.Type.DOUBLE);
            when(mockFieldDescriptor.isRepeated()).thenReturn(true);
            List<Double> protobufValue = Arrays.asList(1.1, 2.2, 3.3);
            Types.ListType icebergType = Types.ListType.ofRequired(1, Types.DoubleType.get());

            // When
            Object result = ProtobufNativeToIcebergConverter.convertProtobufValueToIcebergValue(
                protobufValue, mockFieldDescriptor, icebergType);

            // Then
            assertInstanceOf(List.class, result);
            @SuppressWarnings("unchecked")
            List<Double> resultList = (List<Double>) result;
            assertEquals(Arrays.asList(1.1, 2.2, 3.3), resultList);
        }
    }

    @Nested
    @DisplayName("Boolean Type Tests")
    class BooleanTypeTests {

        @Test
        @DisplayName("Convert BOOL single value - true")
        void testConvertBoolSingleValueTrue() {
            // Given
            when(mockFieldDescriptor.getType()).thenReturn(Descriptors.FieldDescriptor.Type.BOOL);
            when(mockFieldDescriptor.isRepeated()).thenReturn(false);
            Boolean protobufValue = true;
            Types.BooleanType icebergType = Types.BooleanType.get();

            // When
            Object result = ProtobufNativeToIcebergConverter.convertProtobufValueToIcebergValue(
                protobufValue, mockFieldDescriptor, icebergType);

            // Then
            assertEquals(true, result);
            assertInstanceOf(Boolean.class, result);
        }

        @Test
        @DisplayName("Convert BOOL single value - false")
        void testConvertBoolSingleValueFalse() {
            // Given
            when(mockFieldDescriptor.getType()).thenReturn(Descriptors.FieldDescriptor.Type.BOOL);
            when(mockFieldDescriptor.isRepeated()).thenReturn(false);
            Boolean protobufValue = false;
            Types.BooleanType icebergType = Types.BooleanType.get();

            // When
            Object result = ProtobufNativeToIcebergConverter.convertProtobufValueToIcebergValue(
                protobufValue, mockFieldDescriptor, icebergType);

            // Then
            assertEquals(false, result);
        }

        @Test
        @DisplayName("Convert BOOL repeated values")
        void testConvertBoolRepeatedValues() {
            // Given
            when(mockFieldDescriptor.getType()).thenReturn(Descriptors.FieldDescriptor.Type.BOOL);
            when(mockFieldDescriptor.isRepeated()).thenReturn(true);
            List<Boolean> protobufValue = Arrays.asList(true, false, true);
            Types.ListType icebergType = Types.ListType.ofRequired(1, Types.BooleanType.get());

            // When
            Object result = ProtobufNativeToIcebergConverter.convertProtobufValueToIcebergValue(
                protobufValue, mockFieldDescriptor, icebergType);

            // Then
            assertInstanceOf(List.class, result);
            @SuppressWarnings("unchecked")
            List<Boolean> resultList = (List<Boolean>) result;
            assertEquals(Arrays.asList(true, false, true), resultList);
        }
    }

    @Nested
    @DisplayName("String Type Tests")
    class StringTypeTests {

        @Test
        @DisplayName("Convert STRING single value")
        void testConvertStringSingleValue() {
            // Given
            when(mockFieldDescriptor.getType()).thenReturn(Descriptors.FieldDescriptor.Type.STRING);
            when(mockFieldDescriptor.isRepeated()).thenReturn(false);
            String protobufValue = "Hello, World!";
            Types.StringType icebergType = Types.StringType.get();

            // When
            Object result = ProtobufNativeToIcebergConverter.convertProtobufValueToIcebergValue(
                protobufValue, mockFieldDescriptor, icebergType);

            // Then
            assertEquals("Hello, World!", result);
            assertInstanceOf(String.class, result);
        }

        @Test
        @DisplayName("Convert STRING repeated values")
        void testConvertStringRepeatedValues() {
            // Given
            when(mockFieldDescriptor.getType()).thenReturn(Descriptors.FieldDescriptor.Type.STRING);
            when(mockFieldDescriptor.isRepeated()).thenReturn(true);
            List<String> protobufValue = Arrays.asList("Hello", "World", "!");
            Types.ListType icebergType = Types.ListType.ofRequired(1, Types.StringType.get());

            // When
            Object result = ProtobufNativeToIcebergConverter.convertProtobufValueToIcebergValue(
                protobufValue, mockFieldDescriptor, icebergType);

            // Then
            assertInstanceOf(List.class, result);
            @SuppressWarnings("unchecked")
            List<String> resultList = (List<String>) result;
            assertEquals(Arrays.asList("Hello", "World", "!"), resultList);
        }

        @Test
        @DisplayName("Convert STRING empty value")
        void testConvertStringEmptyValue() {
            // Given
            when(mockFieldDescriptor.getType()).thenReturn(Descriptors.FieldDescriptor.Type.STRING);
            when(mockFieldDescriptor.isRepeated()).thenReturn(false);
            String protobufValue = "";
            Types.StringType icebergType = Types.StringType.get();

            // When
            Object result = ProtobufNativeToIcebergConverter.convertProtobufValueToIcebergValue(
                protobufValue, mockFieldDescriptor, icebergType);

            // Then
            assertEquals("", result);
        }
    }

    @Nested
    @DisplayName("Bytes Type Tests")
    class BytesTypeTests {

        @Test
        @DisplayName("Convert BYTES single value")
        void testConvertBytesSingleValue() {
            // Given
            when(mockFieldDescriptor.getType()).thenReturn(Descriptors.FieldDescriptor.Type.BYTES);
            when(mockFieldDescriptor.isRepeated()).thenReturn(false);
            ByteString protobufValue = ByteString.copyFromUtf8("Hello Bytes");
            Types.BinaryType icebergType = Types.BinaryType.get();

            // When
            Object result = ProtobufNativeToIcebergConverter.convertProtobufValueToIcebergValue(
                protobufValue, mockFieldDescriptor, icebergType);

            // Then
            assertInstanceOf(ByteBuffer.class, result);
            ByteBuffer resultBuffer = (ByteBuffer) result;
            assertEquals("Hello Bytes", new String(resultBuffer.array()));
        }

        @Test
        @DisplayName("Convert BYTES repeated values")
        void testConvertBytesRepeatedValues() {
            // Given
            when(mockFieldDescriptor.getType()).thenReturn(Descriptors.FieldDescriptor.Type.BYTES);
            when(mockFieldDescriptor.isRepeated()).thenReturn(true);
            List<ByteString> protobufValue = Arrays.asList(
                ByteString.copyFromUtf8("Hello"),
                ByteString.copyFromUtf8("World")
            );
            Types.ListType icebergType = Types.ListType.ofRequired(1, Types.BinaryType.get());

            // When
            Object result = ProtobufNativeToIcebergConverter.convertProtobufValueToIcebergValue(
                protobufValue, mockFieldDescriptor, icebergType);

            // Then
            assertInstanceOf(List.class, result);
            @SuppressWarnings("unchecked")
            List<ByteBuffer> resultList = (List<ByteBuffer>) result;
            assertEquals(2, resultList.size());
            assertEquals("Hello", new String(resultList.get(0).array()));
            assertEquals("World", new String(resultList.get(1).array()));
        }

        @Test
        @DisplayName("Convert BYTES empty value")
        void testConvertBytesEmptyValue() {
            // Given
            when(mockFieldDescriptor.getType()).thenReturn(Descriptors.FieldDescriptor.Type.BYTES);
            when(mockFieldDescriptor.isRepeated()).thenReturn(false);
            ByteString protobufValue = ByteString.EMPTY;
            Types.BinaryType icebergType = Types.BinaryType.get();

            // When
            Object result = ProtobufNativeToIcebergConverter.convertProtobufValueToIcebergValue(
                protobufValue, mockFieldDescriptor, icebergType);

            // Then
            assertInstanceOf(ByteBuffer.class, result);
            ByteBuffer resultBuffer = (ByteBuffer) result;
            assertEquals(0, resultBuffer.remaining());
        }
    }

    @Nested
    @DisplayName("Enum Type Tests")
    class EnumTypeTests {

        @Test
        @DisplayName("Convert ENUM single value")
        void testConvertEnumSingleValue() {
            // Given
            when(mockFieldDescriptor.getType()).thenReturn(Descriptors.FieldDescriptor.Type.ENUM);
            when(mockFieldDescriptor.isRepeated()).thenReturn(false);
            when(mockEnumValueDescriptor.getName()).thenReturn("ENUM_VALUE_1");
            Types.StringType icebergType = Types.StringType.get();

            // When
            Object result = ProtobufNativeToIcebergConverter.convertProtobufValueToIcebergValue(
                mockEnumValueDescriptor, mockFieldDescriptor, icebergType);

            // Then
            assertEquals("ENUM_VALUE_1", result);
            assertInstanceOf(String.class, result);
        }

        @Test
        @DisplayName("Convert ENUM repeated values")
        void testConvertEnumRepeatedValues() {
            // Given
            when(mockFieldDescriptor.getType()).thenReturn(Descriptors.FieldDescriptor.Type.ENUM);
            when(mockFieldDescriptor.isRepeated()).thenReturn(true);

            Descriptors.EnumValueDescriptor enum1 = mock(Descriptors.EnumValueDescriptor.class);
            Descriptors.EnumValueDescriptor enum2 = mock(Descriptors.EnumValueDescriptor.class);
            when(enum1.getName()).thenReturn("VALUE_1");
            when(enum2.getName()).thenReturn("VALUE_2");

            List<Descriptors.EnumValueDescriptor> protobufValue = Arrays.asList(enum1, enum2);
            Types.ListType icebergType = Types.ListType.ofRequired(1, Types.StringType.get());

            // When
            Object result = ProtobufNativeToIcebergConverter.convertProtobufValueToIcebergValue(
                protobufValue, mockFieldDescriptor, icebergType);

            // Then
            assertInstanceOf(List.class, result);
            @SuppressWarnings("unchecked")
            List<String> resultList = (List<String>) result;
            assertEquals(Arrays.asList("VALUE_1", "VALUE_2"), resultList);
        }
    }

    @Nested
    @DisplayName("Message Type Tests")
    class MessageTypeTests {

        @Test
        @DisplayName("Convert MESSAGE single value")
        void testConvertMessageSingleValue() {
            // Given
            when(mockFieldDescriptor.getType()).thenReturn(Descriptors.FieldDescriptor.Type.MESSAGE);
            when(mockFieldDescriptor.isRepeated()).thenReturn(false);

            DynamicMessage nestedMessage = mock(DynamicMessage.class);
            Descriptors.Descriptor nestedDescriptor = mock(Descriptors.Descriptor.class);
            when(mockFieldDescriptor.getMessageType()).thenReturn(nestedDescriptor);

            Types.StructType structType = Types.StructType.of(
                Types.NestedField.required(1, "nested_field", Types.StringType.get())
            );

            // When
            Object result = ProtobufNativeToIcebergConverter.convertProtobufValueToIcebergValue(
                nestedMessage, mockFieldDescriptor, structType);

            // Then
            assertInstanceOf(GenericRecord.class, result);
        }

        @Test
        @DisplayName("Convert MESSAGE repeated values")
        void testConvertMessageRepeatedValues() {
            // Given
            when(mockFieldDescriptor.getType()).thenReturn(Descriptors.FieldDescriptor.Type.MESSAGE);
            when(mockFieldDescriptor.isRepeated()).thenReturn(true);

            DynamicMessage nestedMessage1 = mock(DynamicMessage.class);
            DynamicMessage nestedMessage2 = mock(DynamicMessage.class);
            List<DynamicMessage> protobufValue = Arrays.asList(nestedMessage1, nestedMessage2);

            Descriptors.Descriptor nestedDescriptor = mock(Descriptors.Descriptor.class);
            when(mockFieldDescriptor.getMessageType()).thenReturn(nestedDescriptor);

            Types.ListType listType = Types.ListType.ofRequired(1,
                Types.StructType.of(
                    Types.NestedField.required(1, "nested_field", Types.StringType.get())
                )
            );

            // When
            Object result = ProtobufNativeToIcebergConverter.convertProtobufValueToIcebergValue(
                protobufValue, mockFieldDescriptor, listType);

            // Then
            assertInstanceOf(List.class, result);
            @SuppressWarnings("unchecked")
            List<GenericRecord> resultList = (List<GenericRecord>) result;
            assertEquals(2, resultList.size());
        }
    }

    @Nested
    @DisplayName("Full Record Conversion Tests")
    class FullRecordConversionTests {

        @Test
        @DisplayName("Convert complete protobuf message to Iceberg record")
        void testConvertCompleteProtobufMessage() {
            // Given
            Schema icebergSchema = new Schema(
                Types.NestedField.required(1, "id", Types.IntegerType.get()),
                Types.NestedField.optional(2, "name", Types.StringType.get()),
                Types.NestedField.optional(3, "active", Types.BooleanType.get())
            );

            Descriptors.FieldDescriptor idField = mock(Descriptors.FieldDescriptor.class);
            Descriptors.FieldDescriptor nameField = mock(Descriptors.FieldDescriptor.class);
            Descriptors.FieldDescriptor activeField = mock(Descriptors.FieldDescriptor.class);

            when(idField.getName()).thenReturn("id");
            when(idField.getType()).thenReturn(Descriptors.FieldDescriptor.Type.INT32);
            when(idField.isRepeated()).thenReturn(false);

            when(nameField.getName()).thenReturn("name");
            when(nameField.getType()).thenReturn(Descriptors.FieldDescriptor.Type.STRING);
            when(nameField.isRepeated()).thenReturn(false);

            when(activeField.getName()).thenReturn("active");
            when(activeField.getType()).thenReturn(Descriptors.FieldDescriptor.Type.BOOL);
            when(activeField.isRepeated()).thenReturn(false);

            when(mockDescriptor.getFields()).thenReturn(Arrays.asList(idField, nameField, activeField));
            when(mockProtobufMessage.getField(idField)).thenReturn(123);
            when(mockProtobufMessage.getField(nameField)).thenReturn("Test Name");
            when(mockProtobufMessage.getField(activeField)).thenReturn(true);

            // When
            GenericRecord result = ProtobufNativeToIcebergConverter.convertProtobufToIcebergRecord(
                mockProtobufMessage, icebergSchema, mockDescriptor);

            // Then
            assertNotNull(result);
            assertEquals(123, result.getField("id"));
            assertEquals("Test Name", result.getField("name"));
            assertEquals(true, result.getField("active"));
        }
    }

    @Nested
    @DisplayName("Edge Case Tests")
    class EdgeCaseTests {

        @Test
        @DisplayName("Handle unsupported field type")
        void testUnsupportedFieldType() {
            // Given
            when(mockFieldDescriptor.getType()).thenReturn(Descriptors.FieldDescriptor.Type.GROUP);
            Types.StringType icebergType = Types.StringType.get();

            // When & Then
            RuntimeException exception = assertThrows(RuntimeException.class, () -> {
                ProtobufNativeToIcebergConverter.convertProtobufValueToIcebergValue(
                    "test", mockFieldDescriptor, icebergType);
            });

            assertTrue(exception.getMessage().contains("Unsupported field type"));
        }

        @Test
        @DisplayName("Handle field not found in Iceberg schema")
        void testFieldNotFoundInIcebergSchema() {
            // Given
            Schema icebergSchema = new Schema(
                Types.NestedField.required(1, "existing_field", Types.StringType.get())
            );

            Descriptors.FieldDescriptor missingField = mock(Descriptors.FieldDescriptor.class);
            when(missingField.getName()).thenReturn("missing_field");
            when(missingField.getType()).thenReturn(Descriptors.FieldDescriptor.Type.STRING);

            when(mockProtobufMessage.getField(missingField)).thenReturn("test value");
            when(mockDescriptor.getFields()).thenReturn(List.of(missingField));

            // When
            GenericRecord result = ProtobufNativeToIcebergConverter.convertProtobufToIcebergRecord(
                mockProtobufMessage, icebergSchema, mockDescriptor);

            // Then
            assertNotNull(result);
            // The missing field should be ignored, not cause an error
            assertNull(result.getField("missing_field"));
        }

        @Test
        @DisplayName("Handle empty repeated fields")
        void testEmptyRepeatedFields() {
            // Given
            when(mockFieldDescriptor.getType()).thenReturn(Descriptors.FieldDescriptor.Type.STRING);
            when(mockFieldDescriptor.isRepeated()).thenReturn(true);
            List<String> emptyList = List.of();
            Types.ListType icebergType = Types.ListType.ofRequired(1, Types.StringType.get());

            // When
            Object result = ProtobufNativeToIcebergConverter.convertProtobufValueToIcebergValue(
                emptyList, mockFieldDescriptor, icebergType);

            // Then
            assertInstanceOf(List.class, result);
            @SuppressWarnings("unchecked")
            List<String> resultList = (List<String>) result;
            assertTrue(resultList.isEmpty());
        }
    }
}