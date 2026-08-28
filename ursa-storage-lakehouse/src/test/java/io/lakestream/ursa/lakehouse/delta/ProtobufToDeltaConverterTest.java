/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.lakehouse.delta;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors;
import com.google.protobuf.ListValue;
import com.google.protobuf.Message;
import com.google.protobuf.Struct;
import com.google.protobuf.Value;
import io.delta.kernel.data.ArrayValue;
import io.delta.kernel.types.ArrayType;
import io.delta.kernel.types.BinaryType;
import io.delta.kernel.types.BooleanType;
import io.delta.kernel.types.DataType;
import io.delta.kernel.types.DoubleType;
import io.delta.kernel.types.FloatType;
import io.delta.kernel.types.IntegerType;
import io.delta.kernel.types.LongType;
import io.delta.kernel.types.StringType;
import io.delta.kernel.types.StructField;
import io.delta.kernel.types.StructType;
import io.delta.kernel.types.VariantType;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class ProtobufToDeltaConverterTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    @Mock
    private Message mockMessage;

    @Mock
    private Descriptors.Descriptor mockDescriptor;

    @Mock
    private Descriptors.FieldDescriptor mockFieldDescriptor;

    @Mock
    private Descriptors.EnumValueDescriptor mockEnumDescriptor;

    @Mock
    private Message mockNestedMessage;

    @Mock
    private Descriptors.Descriptor mockNestedDescriptor;

    private StructType testSchema;

    @BeforeEach
    void setUp() {
        // Create a test schema with various data types
        List<StructField> fields = Arrays.asList(
            new StructField("stringField", StringType.STRING, true),
            new StructField("intField", IntegerType.INTEGER, true),
            new StructField("longField", LongType.LONG, true),
            new StructField("floatField", FloatType.FLOAT, true),
            new StructField("doubleField", DoubleType.DOUBLE, true),
            new StructField("boolField", BooleanType.BOOLEAN, true),
            new StructField("binaryField", BinaryType.BINARY, true),
            new StructField("arrayField", new ArrayType(StringType.STRING, true), true),
            new StructField("structField", new StructType(List.of(
                    new StructField("nestedString", StringType.STRING, true)
            )), true),
            new StructField("timestampField", LongType.LONG, true)
        );
        testSchema = new StructType(fields);
    }

    @Test
    void testConvertToGenericRow_AllFieldsPresent() {
        // Setup mock message with all field types
        when(mockMessage.getDescriptorForType()).thenReturn(mockDescriptor);

        // Mock string field
        Descriptors.FieldDescriptor stringField = mock(Descriptors.FieldDescriptor.class);
        when(mockDescriptor.findFieldByName("stringField")).thenReturn(stringField);
        when(stringField.getType()).thenReturn(Descriptors.FieldDescriptor.Type.STRING);
        when(stringField.isRepeated()).thenReturn(false);
        when(mockMessage.getField(stringField)).thenReturn("test string");

        // Mock int field
        Descriptors.FieldDescriptor intField = mock(Descriptors.FieldDescriptor.class);
        when(mockDescriptor.findFieldByName("intField")).thenReturn(intField);
        when(intField.getType()).thenReturn(Descriptors.FieldDescriptor.Type.INT32);
        when(intField.isRepeated()).thenReturn(false);
        when(mockMessage.getField(intField)).thenReturn(42);

        // Mock long field
        Descriptors.FieldDescriptor longField = mock(Descriptors.FieldDescriptor.class);
        when(mockDescriptor.findFieldByName("longField")).thenReturn(longField);
        when(longField.getType()).thenReturn(Descriptors.FieldDescriptor.Type.INT64);
        when(longField.isRepeated()).thenReturn(false);
        when(mockMessage.getField(longField)).thenReturn(123L);

        // Mock float field
        Descriptors.FieldDescriptor floatField = mock(Descriptors.FieldDescriptor.class);
        when(mockDescriptor.findFieldByName("floatField")).thenReturn(floatField);
        when(floatField.getType()).thenReturn(Descriptors.FieldDescriptor.Type.FLOAT);
        when(floatField.isRepeated()).thenReturn(false);
        when(mockMessage.getField(floatField)).thenReturn(3.14f);

        // Mock double field
        Descriptors.FieldDescriptor doubleField = mock(Descriptors.FieldDescriptor.class);
        when(mockDescriptor.findFieldByName("doubleField")).thenReturn(doubleField);
        when(doubleField.getType()).thenReturn(Descriptors.FieldDescriptor.Type.DOUBLE);
        when(doubleField.isRepeated()).thenReturn(false);
        when(mockMessage.getField(doubleField)).thenReturn(2.718);

        // Mock boolean field
        Descriptors.FieldDescriptor boolField = mock(Descriptors.FieldDescriptor.class);
        when(mockDescriptor.findFieldByName("boolField")).thenReturn(boolField);
        when(boolField.getType()).thenReturn(Descriptors.FieldDescriptor.Type.BOOL);
        when(boolField.isRepeated()).thenReturn(false);
        when(mockMessage.getField(boolField)).thenReturn(true);

        // Mock binary field
        Descriptors.FieldDescriptor binaryField = mock(Descriptors.FieldDescriptor.class);
        when(mockDescriptor.findFieldByName("binaryField")).thenReturn(binaryField);
        when(binaryField.getType()).thenReturn(Descriptors.FieldDescriptor.Type.BYTES);
        when(binaryField.isRepeated()).thenReturn(false);
        ByteString byteString = ByteString.copyFromUtf8("test bytes");
        when(mockMessage.getField(binaryField)).thenReturn(byteString);

        // Mock array field
        Descriptors.FieldDescriptor arrayField = mock(Descriptors.FieldDescriptor.class);
        when(mockDescriptor.findFieldByName("arrayField")).thenReturn(arrayField);
        when(arrayField.getType()).thenReturn(Descriptors.FieldDescriptor.Type.STRING);
        when(arrayField.isRepeated()).thenReturn(true);
        when(mockMessage.getField(arrayField)).thenReturn(Arrays.asList("item1", "item2"));

        // Mock struct field - CRITICAL: Need to add hasField mock for MESSAGE types
        Descriptors.FieldDescriptor structField = mock(Descriptors.FieldDescriptor.class);
        when(mockDescriptor.findFieldByName("structField")).thenReturn(structField);
        when(structField.getType()).thenReturn(Descriptors.FieldDescriptor.Type.MESSAGE);
        when(structField.isRepeated()).thenReturn(false);
        when(mockMessage.hasField(structField)).thenReturn(true); // MISSING in original test
        when(mockMessage.getField(structField)).thenReturn(mockNestedMessage);

        // Mock nested message
        when(mockNestedMessage.getDescriptorForType()).thenReturn(mockNestedDescriptor);
        Descriptors.FieldDescriptor nestedStringField = mock(Descriptors.FieldDescriptor.class);
        when(mockNestedDescriptor.findFieldByName("nestedString")).thenReturn(nestedStringField);
        when(nestedStringField.getType()).thenReturn(Descriptors.FieldDescriptor.Type.STRING);
        when(nestedStringField.isRepeated()).thenReturn(false);
        when(mockNestedMessage.getField(nestedStringField)).thenReturn("nested value");

        // Mock timestamp field
        Descriptors.FieldDescriptor timestampField = mock(Descriptors.FieldDescriptor.class);
        when(mockDescriptor.findFieldByName("timestampField")).thenReturn(timestampField);
        when(timestampField.getType()).thenReturn(Descriptors.FieldDescriptor.Type.INT64);
        when(timestampField.isRepeated()).thenReturn(false);
        when(mockMessage.getField(timestampField)).thenReturn(1234567890L);

        // Execute conversion
        GenericRow result = ProtobufToDeltaConverter.convertToGenericRow(mockMessage, testSchema);

        // Verify results
        assertNotNull(result);

        // Test basic field types
        assertEquals("test string", result.getString(0));
        assertEquals(42, result.getInt(1));
        assertEquals(123L, result.getLong(2));
        assertEquals(3.14f, result.getFloat(3), 0.001f);
        assertEquals(2.718, result.getDouble(4), 0.001);
        assertTrue(result.getBoolean(5));
        assertArrayEquals("test bytes".getBytes(), result.getBinary(6));

        // Test array field
        ArrayValue arrayValue = result.getArray(7);
        assertNotNull(arrayValue);
        assertEquals(2, arrayValue.getSize());

        // Test the array elements through ColumnVector
        var columnVector = arrayValue.getElements();
        assertEquals(2, columnVector.getSize());
        assertEquals("item1", columnVector.getString(0));
        assertEquals("item2", columnVector.getString(1));

        // Test nested struct
        var nestedRow = result.getStruct(8);
        assertNotNull(nestedRow);
        assertEquals("nested value", nestedRow.getString(0));

        // Test timestamp
        assertEquals(1234567890L, result.getLong(9));
    }


    @Test
    void testConvertToGenericRow_MissingFields() {
        // Setup mock message with missing fields
        when(mockMessage.getDescriptorForType()).thenReturn(mockDescriptor);
        when(mockDescriptor.findFieldByName(anyString())).thenReturn(null);

        GenericRow result = ProtobufToDeltaConverter.convertToGenericRow(mockMessage, testSchema);

        assertNotNull(result);
        assertEquals(testSchema, result.getSchema());

        // All fields should be null or default values
        assertTrue(result.isNullAt(0)); // string field
        assertTrue(result.isNullAt(1)); // int field
        assertTrue(result.isNullAt(2)); // long field
        assertTrue(result.isNullAt(3)); // float field
        assertTrue(result.isNullAt(4)); // double field
        assertTrue(result.isNullAt(5)); // bool field
        assertTrue(result.isNullAt(6)); // binary field
        assertNotNull(result.getArray(7)); // array field should have empty array
        assertEquals(0, result.getArray(7).getSize());
        assertTrue(result.isNullAt(8)); // struct field
        assertTrue(result.isNullAt(9)); // timestamp field
    }

    @Test
    void testConvertSingleValue_EnumToString() {
        // Setup enum field
        when(mockEnumDescriptor.getName()).thenReturn("ENUM_VALUE");

        // Test enum to string conversion
        GenericRow result = createGenericRowWithSingleFieldEnum(mockEnumDescriptor, StringType.STRING);
        assertEquals("ENUM_VALUE", result.getString(0));
    }

    @Test
    void testConvertSingleValue_EnumToInteger() {
        // Setup enum field
        when(mockEnumDescriptor.getNumber()).thenReturn(1);

        // Test enum to integer conversion
        GenericRow result = createGenericRowWithSingleFieldEnum(mockEnumDescriptor, IntegerType.INTEGER);
        assertEquals(1, result.getInt(0));
    }

    @Test
    void testConvertSingleValue_NullValue() {
        // Test null value conversion
        GenericRow result = createGenericRowWithSingleField(null, StringType.STRING);
        assertTrue(result.isNullAt(0));
    }

    @Test
    void testConvertSingleValue_IntegerTypes() {
        // Test INT32
        GenericRow result = createGenericRowWithSingleFieldType(42, IntegerType.INTEGER,
            Descriptors.FieldDescriptor.Type.INT32, null);
        assertEquals(42, result.getInt(0));

        // Test SINT32
        result = createGenericRowWithSingleFieldType(42, IntegerType.INTEGER,
            Descriptors.FieldDescriptor.Type.SINT32, null);
        assertEquals(42, result.getInt(0));

        // Test SFIXED32
        result = createGenericRowWithSingleFieldType(42, IntegerType.INTEGER,
            Descriptors.FieldDescriptor.Type.SFIXED32, null);
        assertEquals(42, result.getInt(0));
    }

    @Test
    void testConvertSingleValue_LongTypes() {
        // Test INT64
        GenericRow result = createGenericRowWithSingleFieldType(123L, LongType.LONG,
            Descriptors.FieldDescriptor.Type.INT64, null);
        assertEquals(123L, result.getLong(0));

        // Test SINT64
        result = createGenericRowWithSingleFieldType(123L, LongType.LONG,
            Descriptors.FieldDescriptor.Type.SINT64, null);
        assertEquals(123L, result.getLong(0));

        // Test SFIXED64
        result = createGenericRowWithSingleFieldType(123L, LongType.LONG,
            Descriptors.FieldDescriptor.Type.SFIXED64, null);
        assertEquals(123L, result.getLong(0));

        // Test UINT32
        result = createGenericRowWithSingleFieldType(123L, LongType.LONG,
            Descriptors.FieldDescriptor.Type.UINT32, null);
        assertEquals(123L, result.getLong(0));

        // Test UINT64
        result = createGenericRowWithSingleFieldType(123L, LongType.LONG,
            Descriptors.FieldDescriptor.Type.UINT64, null);
        assertEquals(123L, result.getLong(0));
    }

    @Test
    void testConvertProtobufTimestamp() {
        // Create mock timestamp message
        Message timestampMessage = mock(Message.class);
        Descriptors.Descriptor timestampDescriptor = mock(Descriptors.Descriptor.class);
        when(timestampMessage.getDescriptorForType()).thenReturn(timestampDescriptor);

        Descriptors.FieldDescriptor secondsField = mock(Descriptors.FieldDescriptor.class);
        Descriptors.FieldDescriptor nanosField = mock(Descriptors.FieldDescriptor.class);

        when(timestampDescriptor.findFieldByName("seconds")).thenReturn(secondsField);
        when(timestampDescriptor.findFieldByName("nanos")).thenReturn(nanosField);
        when(timestampMessage.hasField(secondsField)).thenReturn(true);
        when(timestampMessage.hasField(nanosField)).thenReturn(true);
        when(timestampMessage.getField(secondsField)).thenReturn(1234567890L);
        when(timestampMessage.getField(nanosField)).thenReturn(123456789);

        // Mock the timestamp message descriptor to return the correct full name
//        when(timestampDescriptor.getFullName()).thenReturn("google.protobuf.Timestamp");

        // Setup field descriptor for message type
        GenericRow result = createGenericRowWithSingleFieldType(timestampMessage, LongType.LONG,
            Descriptors.FieldDescriptor.Type.MESSAGE, timestampDescriptor);

        // Expected: 1234567890 * 1000 + 123456789 / 1000000 = 1234567890123
        assertEquals(1234567890123L, result.getLong(0));
    }

    private GenericRow createGenericRowWithSingleFieldType(Object value, DataType dataType,
                                                           Descriptors.FieldDescriptor.Type fieldType,
                                                           Descriptors.Descriptor messageType) {
        lenient().when(mockMessage.getDescriptorForType()).thenReturn(mockDescriptor);
        lenient().when(mockDescriptor.findFieldByName("testField")).thenReturn(mockFieldDescriptor);
        lenient().when(mockFieldDescriptor.getType()).thenReturn(fieldType);
        lenient().when(mockFieldDescriptor.isRepeated()).thenReturn(false);
        lenient().when(mockMessage.getField(mockFieldDescriptor)).thenReturn(value);

        // For MESSAGE types, we need to set up additional mocking
        if (fieldType == Descriptors.FieldDescriptor.Type.MESSAGE) {
            lenient().when(mockMessage.hasField(mockFieldDescriptor)).thenReturn(value != null);
            if (messageType != null) {
                lenient().when(mockFieldDescriptor.getMessageType()).thenReturn(messageType);
            }
        }

        StructType schema = new StructType(List.of(
                new StructField("testField", dataType, true)
        ));

        return ProtobufToDeltaConverter.convertToGenericRow(mockMessage, schema);
    }

    private GenericRow createGenericRowWithRepeatedFieldType(List<Object> values, DataType elementType,
                                                             Descriptors.FieldDescriptor.Type fieldType,
                                                             Descriptors.Descriptor messageType) {
        lenient().when(mockMessage.getDescriptorForType()).thenReturn(mockDescriptor);
        lenient().when(mockDescriptor.findFieldByName("testField")).thenReturn(mockFieldDescriptor);
        lenient().when(mockFieldDescriptor.getType()).thenReturn(fieldType);
        lenient().when(mockFieldDescriptor.isRepeated()).thenReturn(true);
        lenient().when(mockMessage.getField(mockFieldDescriptor)).thenReturn(values);

        if (fieldType == Descriptors.FieldDescriptor.Type.MESSAGE && messageType != null) {
            lenient().when(mockFieldDescriptor.getMessageType()).thenReturn(messageType);
        }

        StructType schema = new StructType(List.of(
                new StructField("testField", new ArrayType(elementType, true), true)
        ));
        return ProtobufToDeltaConverter.convertToGenericRow(mockMessage, schema);
    }

    private GenericRow createGenericRowWithRepeatedVariantFieldType(List<Object> values,
                                                                    Descriptors.FieldDescriptor.Type fieldType,
                                                                    Descriptors.Descriptor messageType,
                                                                    boolean isMapField) {
        lenient().when(mockMessage.getDescriptorForType()).thenReturn(mockDescriptor);
        lenient().when(mockDescriptor.findFieldByName("testField")).thenReturn(mockFieldDescriptor);
        lenient().when(mockFieldDescriptor.getType()).thenReturn(fieldType);
        lenient().when(mockFieldDescriptor.isRepeated()).thenReturn(true);
        lenient().when(mockFieldDescriptor.isMapField()).thenReturn(isMapField);
        lenient().when(mockMessage.getField(mockFieldDescriptor)).thenReturn(values);

        if (fieldType == Descriptors.FieldDescriptor.Type.MESSAGE && messageType != null) {
            lenient().when(mockFieldDescriptor.getMessageType()).thenReturn(messageType);
        }

        StructType schema = new StructType(List.of(
                new StructField("testField", VariantType.VARIANT, true)
        ));

        return ProtobufToDeltaConverter.convertToGenericRow(mockMessage, schema);
    }

//    @Test
//    void testConvertProtobufTimestamp() {
//        // Create mock timestamp message
//        Message timestampMessage = mock(Message.class);
//        Descriptors.Descriptor timestampDescriptor = mock(Descriptors.Descriptor.class);
//        when(timestampMessage.getDescriptorForType()).thenReturn(timestampDescriptor);
//
//        Descriptors.FieldDescriptor secondsField = mock(Descriptors.FieldDescriptor.class);
//        Descriptors.FieldDescriptor nanosField = mock(Descriptors.FieldDescriptor.class);
//
//        when(timestampDescriptor.findFieldByName("seconds")).thenReturn(secondsField);
//        when(timestampDescriptor.findFieldByName("nanos")).thenReturn(nanosField);
//        when(timestampMessage.getField(secondsField)).thenReturn(1234567890L);
//        when(timestampMessage.getField(nanosField)).thenReturn(123456789);
//
//        // Setup field descriptor for message type
//        GenericRow result = createGenericRowWithSingleFieldType(timestampMessage, LongType.LONG,
//            Descriptors.FieldDescriptor.Type.MESSAGE);
//
//        // Expected: 1234567890 * 1000 + 123456789 / 1000000 = 1234567890123
//        assertEquals(1234567890123L, result.getLong(0));
//    }

    @Test
    void testConvertSingleValue_FallbackToString() {
        // Test fallback to string conversion for unknown types
        Object testValue = new Object() {
            @Override
            public String toString() {
                return "fallback string";
            }
        };

        GenericRow result = createGenericRowWithSingleFieldType(testValue, StringType.STRING,
            Descriptors.FieldDescriptor.Type.STRING, null);
        assertEquals("fallback string", result.getString(0));
    }

    @Test
    void testInferSchemaFromProtobuf() {
        // Setup mock descriptor with various field types
        List<Descriptors.FieldDescriptor> fields = new ArrayList<>();

        // String field
        Descriptors.FieldDescriptor stringField = mock(Descriptors.FieldDescriptor.class);
        when(stringField.getName()).thenReturn("stringField");
        when(stringField.getType()).thenReturn(Descriptors.FieldDescriptor.Type.STRING);
        when(stringField.isRepeated()).thenReturn(false);
        when(stringField.isRequired()).thenReturn(true);
        fields.add(stringField);

        // Integer field
        Descriptors.FieldDescriptor intField = mock(Descriptors.FieldDescriptor.class);
        when(intField.getName()).thenReturn("intField");
        when(intField.getType()).thenReturn(Descriptors.FieldDescriptor.Type.INT32);
        when(intField.isRepeated()).thenReturn(false);
        when(intField.isRequired()).thenReturn(false);
        fields.add(intField);

        // Array field
        Descriptors.FieldDescriptor arrayField = mock(Descriptors.FieldDescriptor.class);
        when(arrayField.getName()).thenReturn("arrayField");
        when(arrayField.getType()).thenReturn(Descriptors.FieldDescriptor.Type.STRING);
        when(arrayField.isRepeated()).thenReturn(true);
        when(arrayField.isRequired()).thenReturn(false);
        fields.add(arrayField);

        // Nested message field
        Descriptors.FieldDescriptor nestedField = mock(Descriptors.FieldDescriptor.class);
        when(nestedField.getName()).thenReturn("nestedField");
        when(nestedField.getType()).thenReturn(Descriptors.FieldDescriptor.Type.MESSAGE);
        when(nestedField.isRepeated()).thenReturn(false);
        when(nestedField.isRequired()).thenReturn(false);
        when(nestedField.getMessageType()).thenReturn(mockNestedDescriptor);
        fields.add(nestedField);

        // Timestamp field
        Descriptors.FieldDescriptor timestampField = mock(Descriptors.FieldDescriptor.class);
        when(timestampField.getName()).thenReturn("timestampField");
        when(timestampField.getType()).thenReturn(Descriptors.FieldDescriptor.Type.MESSAGE);
        when(timestampField.isRepeated()).thenReturn(false);
        when(timestampField.isRequired()).thenReturn(false);
        Descriptors.Descriptor timestampDescriptor = mock(Descriptors.Descriptor.class);
        when(timestampDescriptor.getFullName()).thenReturn("google.protobuf.Timestamp");
        when(timestampField.getMessageType()).thenReturn(timestampDescriptor);
        fields.add(timestampField);

        when(mockDescriptor.getFields()).thenReturn(fields);
        when(mockNestedDescriptor.getFields()).thenReturn(new ArrayList<>());
        when(mockNestedDescriptor.getFullName()).thenReturn("test.NestedMessage"); // Add this to prevent NPE

        StructType schema = ProtobufToDeltaConverter.inferSchemaFromProtobuf(mockDescriptor);

        assertNotNull(schema);
        assertEquals(5, schema.length());

        // String field
        StructField field0 = schema.at(0);
        assertEquals("stringField", field0.getName());
        assertEquals(StringType.STRING, field0.getDataType());
        assertFalse(field0.isNullable()); // required field

        // Integer field
        StructField field1 = schema.at(1);
        assertEquals("intField", field1.getName());
        assertEquals(IntegerType.INTEGER, field1.getDataType());
        assertTrue(field1.isNullable()); // optional field

        // Array field
        StructField field2 = schema.at(2);
        assertEquals("arrayField", field2.getName());
        assertInstanceOf(ArrayType.class, field2.getDataType());
        ArrayType arrayType = (ArrayType) field2.getDataType();
        assertEquals(StringType.STRING, arrayType.getElementType());

        // Nested message field
        StructField field3 = schema.at(3);
        assertEquals("nestedField", field3.getName());
        assertInstanceOf(StructType.class, field3.getDataType());

        // Timestamp field
        StructField field4 = schema.at(4);
        assertEquals("timestampField", field4.getName());
        assertEquals(LongType.LONG, field4.getDataType());
    }

    @Test
    void testInferDataTypeFromProtobuf_AllTypes() {
        // Test all protobuf types
        Map<Descriptors.FieldDescriptor.Type, DataType> expectedMappings = new HashMap<>();
        expectedMappings.put(Descriptors.FieldDescriptor.Type.STRING, StringType.STRING);
        expectedMappings.put(Descriptors.FieldDescriptor.Type.INT32, IntegerType.INTEGER);
        expectedMappings.put(Descriptors.FieldDescriptor.Type.SINT32, IntegerType.INTEGER);
        expectedMappings.put(Descriptors.FieldDescriptor.Type.SFIXED32, IntegerType.INTEGER);
        expectedMappings.put(Descriptors.FieldDescriptor.Type.INT64, LongType.LONG);
        expectedMappings.put(Descriptors.FieldDescriptor.Type.SINT64, LongType.LONG);
        expectedMappings.put(Descriptors.FieldDescriptor.Type.SFIXED64, LongType.LONG);
        expectedMappings.put(Descriptors.FieldDescriptor.Type.UINT32, LongType.LONG);
        expectedMappings.put(Descriptors.FieldDescriptor.Type.UINT64, LongType.LONG);
        expectedMappings.put(Descriptors.FieldDescriptor.Type.FLOAT, FloatType.FLOAT);
        expectedMappings.put(Descriptors.FieldDescriptor.Type.DOUBLE, DoubleType.DOUBLE);
        expectedMappings.put(Descriptors.FieldDescriptor.Type.BOOL, BooleanType.BOOLEAN);
        expectedMappings.put(Descriptors.FieldDescriptor.Type.BYTES, BinaryType.BINARY);
        expectedMappings.put(Descriptors.FieldDescriptor.Type.ENUM, StringType.STRING);

        for (Map.Entry<Descriptors.FieldDescriptor.Type, DataType> entry : expectedMappings.entrySet()) {
            Descriptors.FieldDescriptor field = mock(Descriptors.FieldDescriptor.class);
            when(field.getType()).thenReturn(entry.getKey());
            when(field.isRepeated()).thenReturn(false);

            // Use reflection to test private method or create a helper
            // For now, we'll test through the public inferSchemaFromProtobuf method
            List<Descriptors.FieldDescriptor> fields = List.of(field);
            when(field.getName()).thenReturn("testField");
            when(field.isRequired()).thenReturn(true);
            when(mockDescriptor.getFields()).thenReturn(fields);

            StructType schema = ProtobufToDeltaConverter.inferSchemaFromProtobuf(mockDescriptor);
            assertEquals(entry.getValue(), schema.at(0).getDataType());
        }
    }

    @Test
    void testConvertMessages_BatchConversion() {
        // Setup multiple messages
        List<Message> messages = Arrays.asList(mockMessage, mockMessage);

        // Setup mock for string field
        when(mockMessage.getDescriptorForType()).thenReturn(mockDescriptor);
        Descriptors.FieldDescriptor stringField = mock(Descriptors.FieldDescriptor.class);
        when(mockDescriptor.findFieldByName("stringField")).thenReturn(stringField);
        when(stringField.getType()).thenReturn(Descriptors.FieldDescriptor.Type.STRING);
        when(stringField.isRepeated()).thenReturn(false);
        when(mockMessage.getField(stringField)).thenReturn("test");

        // Simple schema with just string field
        StructType simpleSchema = new StructType(List.of(
                new StructField("stringField", StringType.STRING, true)
        ));

        List<GenericRow> results = ProtobufToDeltaConverter.convertMessages(messages, simpleSchema);

        assertNotNull(results);
        assertEquals(2, results.size());
        assertEquals("test", results.get(0).getString(0));
        assertEquals("test", results.get(1).getString(0));
    }

    @Test
    void testGenericArrayValue() {
        List<String> elements = Arrays.asList("a", "b", "c");
        ArrayType arrayType = new ArrayType(StringType.STRING, true);

        // Create GenericArrayValue using reflection or test through main converter
        // For now, test through the main conversion path
        when(mockMessage.getDescriptorForType()).thenReturn(mockDescriptor);

        Descriptors.FieldDescriptor arrayField = mock(Descriptors.FieldDescriptor.class);
        when(mockDescriptor.findFieldByName("arrayField")).thenReturn(arrayField);
        when(arrayField.getType()).thenReturn(Descriptors.FieldDescriptor.Type.STRING);
        when(arrayField.isRepeated()).thenReturn(true);
        when(mockMessage.getField(arrayField)).thenReturn(elements);

        StructType arraySchema = new StructType(List.of(
                new StructField("arrayField", arrayType, true)
        ));

        GenericRow result = ProtobufToDeltaConverter.convertToGenericRow(mockMessage, arraySchema);
        ArrayValue arrayValue = result.getArray(0);

        assertNotNull(arrayValue);
        assertEquals(3, arrayValue.getSize());

        // Test column vector functionality
        assertEquals(3, arrayValue.getElements().getSize());
        assertEquals("a", arrayValue.getElements().getString(0));
        assertEquals("b", arrayValue.getElements().getString(1));
        assertEquals("c", arrayValue.getElements().getString(2));
    }

    // Helper method to create a GenericRow with a single field for testing
    private GenericRow createGenericRowWithSingleField(Object value, DataType dataType) {
        lenient().when(mockMessage.getDescriptorForType()).thenReturn(mockDescriptor);
        lenient().when(mockDescriptor.findFieldByName("testField")).thenReturn(mockFieldDescriptor);
        lenient().when(mockFieldDescriptor.isRepeated()).thenReturn(false);
        lenient().when(mockMessage.getField(mockFieldDescriptor)).thenReturn(value);

        StructType schema = new StructType(List.of(
                new StructField("testField", dataType, true)
        ));

        return ProtobufToDeltaConverter.convertToGenericRow(mockMessage, schema);
    }

    // Helper method to create a GenericRow with a single field for testing with specific field type
//    private GenericRow createGenericRowWithSingleFieldType(Object value, DataType dataType,
//                                                           Descriptors.FieldDescriptor.Type fieldType) {
//        lenient().when(mockMessage.getDescriptorForType()).thenReturn(mockDescriptor);
//        lenient().when(mockDescriptor.findFieldByName("testField")).thenReturn(mockFieldDescriptor);
//        lenient().when(mockFieldDescriptor.getType()).thenReturn(fieldType);
//        lenient().when(mockFieldDescriptor.isRepeated()).thenReturn(false);
//        lenient().when(mockMessage.getField(mockFieldDescriptor)).thenReturn(value);
//
//        StructType schema = new StructType(Arrays.asList(
//            new StructField("testField", dataType, true)
//        ));
//
//        return ProtobufToDeltaConverter.convertToGenericRow(mockMessage, schema);
//    }

    // Helper method specifically for enum testing
    private GenericRow createGenericRowWithSingleFieldEnum(Object value, DataType dataType) {
        lenient().when(mockMessage.getDescriptorForType()).thenReturn(mockDescriptor);
        lenient().when(mockDescriptor.findFieldByName("testField")).thenReturn(mockFieldDescriptor);
        lenient().when(mockFieldDescriptor.getType()).thenReturn(Descriptors.FieldDescriptor.Type.ENUM);
        lenient().when(mockFieldDescriptor.isRepeated()).thenReturn(false);
        lenient().when(mockMessage.getField(mockFieldDescriptor)).thenReturn(value);

        StructType schema = new StructType(List.of(
                new StructField("testField", dataType, true)
        ));

        return ProtobufToDeltaConverter.convertToGenericRow(mockMessage, schema);
    }

    @Test
    void testConvertStringField() {
        String testValue = "test string";
        GenericRow result = createGenericRowWithSingleFieldType(testValue, StringType.STRING,
            Descriptors.FieldDescriptor.Type.STRING, null);

        assertEquals(testValue, result.getString(0));
    }

    @Test
    void testConvertIntegerField() {
        Integer testValue = 42;
        GenericRow result = createGenericRowWithSingleFieldType(testValue, IntegerType.INTEGER,
            Descriptors.FieldDescriptor.Type.INT32, null);

        assertEquals(testValue.intValue(), result.getInt(0));
    }

    @Test
    void testConvertBooleanField() {
        Boolean testValue = true;
        GenericRow result = createGenericRowWithSingleFieldType(testValue, BooleanType.BOOLEAN,
            Descriptors.FieldDescriptor.Type.BOOL, null);

        assertEquals(testValue, result.getBoolean(0));
    }

    @Test
    void testConvertLongField() {
        Long testValue = 1234567890123L;
        GenericRow result = createGenericRowWithSingleFieldType(testValue, LongType.LONG,
            Descriptors.FieldDescriptor.Type.INT64, null);

        assertEquals(testValue.longValue(), result.getLong(0));
    }

    @Test
    void testConvertFloatField() {
        Float testValue = 3.14f;
        GenericRow result = createGenericRowWithSingleFieldType(testValue, FloatType.FLOAT,
            Descriptors.FieldDescriptor.Type.FLOAT, null);

        assertEquals(testValue, result.getFloat(0), 0.001f);
    }

    @Test
    void testConvertDoubleField() {
        Double testValue = 3.14159;
        GenericRow result = createGenericRowWithSingleFieldType(testValue, DoubleType.DOUBLE,
            Descriptors.FieldDescriptor.Type.DOUBLE, null);

        assertEquals(testValue, result.getDouble(0), 0.001);
    }

    @Test
    void testConvertNullField() {
        GenericRow result = createGenericRowWithSingleFieldType(null, StringType.STRING,
            Descriptors.FieldDescriptor.Type.STRING, null);

        assertTrue(result.isNullAt(0));
    }

    @Test
    void testConvertMissingField() {
        // Setup mock to return null for field descriptor (field not found)
        lenient().when(mockMessage.getDescriptorForType()).thenReturn(mockDescriptor);
        lenient().when(mockDescriptor.findFieldByName("testField")).thenReturn(null); // Field not found

        StructType schema = new StructType(List.of(
                new StructField("testField", StringType.STRING, true)
        ));

        GenericRow result = ProtobufToDeltaConverter.convertToGenericRow(mockMessage, schema);

        assertTrue(result.isNullAt(0));
    }

    @Test
    void testConvertRepeatedField() {
        // Test array conversion
        java.util.List<String> testList = Arrays.asList("item1", "item2", "item3");

        lenient().when(mockMessage.getDescriptorForType()).thenReturn(mockDescriptor);
        lenient().when(mockDescriptor.findFieldByName("testField")).thenReturn(mockFieldDescriptor);
        lenient().when(mockFieldDescriptor.getType()).thenReturn(Descriptors.FieldDescriptor.Type.STRING);
        lenient().when(mockFieldDescriptor.isRepeated()).thenReturn(true);
        lenient().when(mockMessage.getField(mockFieldDescriptor)).thenReturn(testList);

        ArrayType arrayType = new ArrayType(StringType.STRING, true);
        StructType schema = new StructType(List.of(
                new StructField("testField", arrayType, true)
        ));

        GenericRow result = ProtobufToDeltaConverter.convertToGenericRow(mockMessage, schema);

        assertFalse(result.isNullAt(0));
        // You would need to implement additional methods to properly test array values
        // This depends on your ArrayValue implementation
    }

    @Test
    void testConvertVariantStringField() throws Exception {
        GenericRow result = createGenericRowWithSingleFieldType(
                "simple string value",
                VariantType.VARIANT,
                Descriptors.FieldDescriptor.Type.STRING,
                null);

        assertVariantJsonEquals("\"simple string value\"", (GenericRow) result.getValue(0));
    }

    @Test
    void testConvertVariantIntegerField() throws Exception {
        GenericRow result = createGenericRowWithSingleFieldType(
                42L,
                VariantType.VARIANT,
                Descriptors.FieldDescriptor.Type.INT64,
                null);

        assertVariantJsonEquals("42", (GenericRow) result.getValue(0));
    }

    @Test
    void testConvertVariantBooleanField() throws Exception {
        GenericRow result = createGenericRowWithSingleFieldType(
                true,
                VariantType.VARIANT,
                Descriptors.FieldDescriptor.Type.BOOL,
                null);

        assertVariantJsonEquals("true", (GenericRow) result.getValue(0));
    }

    @Test
    void testConvertVariantDoubleField() throws Exception {
        GenericRow result = createGenericRowWithSingleFieldType(
                3.14159d,
                VariantType.VARIANT,
                Descriptors.FieldDescriptor.Type.DOUBLE,
                null);

        assertVariantJsonEquals("3.14159", (GenericRow) result.getValue(0));
    }

    @Test
    void testConvertVariantBytesField() throws Exception {
        ByteString bytes = ByteString.copyFromUtf8("hello-variant");
        GenericRow result = createGenericRowWithSingleFieldType(
                bytes,
                VariantType.VARIANT,
                Descriptors.FieldDescriptor.Type.BYTES,
                null);

        assertVariantJsonEquals("\"aGVsbG8tdmFyaWFudA==\"", (GenericRow) result.getValue(0));
    }

    @Test
    void testConvertVariantEnumField() throws Exception {
        when(mockEnumDescriptor.getName()).thenReturn("ACTIVE");

        GenericRow result = createGenericRowWithSingleFieldType(
                mockEnumDescriptor,
                VariantType.VARIANT,
                Descriptors.FieldDescriptor.Type.ENUM,
                null);

        assertVariantJsonEquals("\"ACTIVE\"", (GenericRow) result.getValue(0));
    }

    @Test
    void testConvertVariantMissingMessageFieldReturnsNull() {
        lenient().when(mockMessage.getDescriptorForType()).thenReturn(mockDescriptor);
        lenient().when(mockDescriptor.findFieldByName("testField")).thenReturn(mockFieldDescriptor);
        lenient().when(mockFieldDescriptor.getType()).thenReturn(Descriptors.FieldDescriptor.Type.MESSAGE);
        lenient().when(mockFieldDescriptor.isRepeated()).thenReturn(false);
        lenient().when(mockMessage.hasField(mockFieldDescriptor)).thenReturn(false);

        StructType schema = new StructType(List.of(
                new StructField("testField", VariantType.VARIANT, true)
        ));

        GenericRow result = ProtobufToDeltaConverter.convertToGenericRow(mockMessage, schema);
        assertTrue(result.isNullAt(0));
    }

    @Test
    void testConvertVariantRepeatedPrimitiveField() throws Exception {
        GenericRow result = createGenericRowWithRepeatedVariantFieldType(
                Arrays.asList(1L, 2L, 3L),
                Descriptors.FieldDescriptor.Type.INT64,
                null,
                false);

        assertVariantJsonEquals("[1,2,3]", (GenericRow) result.getValue(0));
    }

    @Test
    void testConvertVariantRepeatedMessageField() throws Exception {
        Struct first = Struct.newBuilder()
                .putFields("type", Value.newBuilder().setStringValue("home").build())
                .putFields("active", Value.newBuilder().setBoolValue(true).build())
                .build();
        Struct second = Struct.newBuilder()
                .putFields("type", Value.newBuilder().setStringValue("work").build())
                .putFields("active", Value.newBuilder().setBoolValue(false).build())
                .build();

        GenericRow result = createGenericRowWithRepeatedVariantFieldType(
                Arrays.asList(first, second),
                Descriptors.FieldDescriptor.Type.MESSAGE,
                null,
                false);

        assertVariantJsonEquals("[{\"active\":true,\"type\":\"home\"},{\"active\":false,\"type\":\"work\"}]",
                (GenericRow) result.getValue(0));
    }

    @Test
    void testConvertVariantMapField() throws Exception {
        Message entry1 = mock(Message.class);
        Message entry2 = mock(Message.class);
        Descriptors.Descriptor entryDescriptor = mock(Descriptors.Descriptor.class);
        Descriptors.FieldDescriptor keyField = mock(Descriptors.FieldDescriptor.class);
        Descriptors.FieldDescriptor valueField = mock(Descriptors.FieldDescriptor.class);

        lenient().when(entry1.getDescriptorForType()).thenReturn(entryDescriptor);
        lenient().when(entry2.getDescriptorForType()).thenReturn(entryDescriptor);
        lenient().when(entryDescriptor.findFieldByName("key")).thenReturn(keyField);
        lenient().when(entryDescriptor.findFieldByName("value")).thenReturn(valueField);
        lenient().when(entry1.getField(keyField)).thenReturn("k1");
        lenient().when(entry1.getField(valueField)).thenReturn("v1");
        lenient().when(entry2.getField(keyField)).thenReturn("k2");
        lenient().when(entry2.getField(valueField)).thenReturn("v2");

        GenericRow result = createGenericRowWithRepeatedVariantFieldType(
                Arrays.asList(entry1, entry2),
                Descriptors.FieldDescriptor.Type.MESSAGE,
                entryDescriptor,
                true);

        assertVariantJsonEquals("{\"k1\":\"v1\",\"k2\":\"v2\"}", (GenericRow) result.getValue(0));
    }

    @Test
    void testConvertVariantStructMessageField() throws Exception {
        Struct struct = Struct.newBuilder()
                .putFields("profile", Value.newBuilder().setStructValue(
                        Struct.newBuilder()
                                .putFields("age", Value.newBuilder().setNumberValue(30).build())
                                .putFields("city", Value.newBuilder().setStringValue("NYC").build())
                                .putFields("history", Value.newBuilder().setListValue(
                                        ListValue.newBuilder()
                                                .addValues(Value.newBuilder().setStructValue(
                                                        Struct.newBuilder()
                                                                .putFields("year", Value.newBuilder().setNumberValue(2023).build())
                                                                .putFields("ok", Value.newBuilder().setBoolValue(true).build())
                                                                .build()).build())
                                                .addValues(Value.newBuilder().setStructValue(
                                                        Struct.newBuilder()
                                                                .putFields("year", Value.newBuilder().setNumberValue(2024).build())
                                                                .putFields("scores", Value.newBuilder().setListValue(
                                                                        ListValue.newBuilder()
                                                                                .addValues(Value.newBuilder().setNumberValue(1).build())
                                                                                .addValues(Value.newBuilder().setNumberValue(2).build())
                                                                                .addValues(Value.newBuilder().setStructValue(
                                                                                        Struct.newBuilder()
                                                                                                .putFields("deep", Value.newBuilder().setBoolValue(true).build())
                                                                                                .build()).build())
                                                                                .build()).build())
                                                                .build()).build())
                                                .build()).build())
                                .build()).build())
                .build();

        GenericRow result = createGenericRowWithSingleFieldType(
                struct,
                VariantType.VARIANT,
                Descriptors.FieldDescriptor.Type.MESSAGE,
                null);

        assertVariantJsonEquals(
                "{\"profile\":{\"age\":30,\"city\":\"NYC\",\"history\":[{\"year\":2023,\"ok\":true},"
                    + "{\"year\":2024,\"scores\":[1,2,{\"deep\":true}]}]}}",
                (GenericRow) result.getValue(0));
    }

    @Test
    void testConvertVariantDeepNestedStructMessageField() throws Exception {
        Struct struct = Struct.newBuilder()
                .putFields("profile", Value.newBuilder().setStructValue(
                        Struct.newBuilder()
                                .putFields("user", Value.newBuilder().setStructValue(
                                        Struct.newBuilder()
                                                .putFields("id", Value.newBuilder().setNumberValue(7).build())
                                                .putFields("name", Value.newBuilder().setStringValue("alice").build())
                                                .putFields("active", Value.newBuilder().setBoolValue(true).build())
                                                .putFields("addresses", Value.newBuilder().setListValue(
                                                        ListValue.newBuilder()
                                                                .addValues(Value.newBuilder().setStructValue(
                                                                        Struct.newBuilder()
                                                                                .putFields("type", Value.newBuilder()
                                                                                        .setStringValue("home").build())
                                                                                .putFields("geo", Value.newBuilder()
                                                                                        .setStructValue(Struct.newBuilder()
                                                                                                .putFields("latE6", Value.newBuilder()
                                                                                                        .setNumberValue(37770000).build())
                                                                                                .putFields("lonE6", Value.newBuilder()
                                                                                                        .setNumberValue(-122410000).build())
                                                                                                .putFields("history", Value.newBuilder()
                                                                                                        .setListValue(ListValue.newBuilder()
                                                                                                                .addValues(Value.newBuilder().setStructValue(
                                                                                                                        Struct.newBuilder()
                                                                                                                                .putFields("year", Value.newBuilder()
                                                                                                                                        .setNumberValue(2022).build())
                                                                                                                                .putFields("ok", Value.newBuilder()
                                                                                                                                        .setBoolValue(true).build())
                                                                                                                                .build()).build())
                                                                                                                .addValues(Value.newBuilder().setStructValue(
                                                                                                                        Struct.newBuilder()
                                                                                                                                .putFields("year", Value.newBuilder()
                                                                                                                                        .setNumberValue(2023).build())
                                                                                                                                .putFields("ok", Value.newBuilder()
                                                                                                                                        .setBoolValue(false).build())
                                                                                                                                .putFields("reasons", Value.newBuilder()
                                                                                                                                        .setListValue(ListValue.newBuilder()
                                                                                                                                                .addValues(Value.newBuilder()
                                                                                                                                                        .setStringValue("moved").build())
                                                                                                                                                .addValues(Value.newBuilder()
                                                                                                                                                        .setStringValue("updated").build())
                                                                                                                                                .build()).build())
                                                                                                                                .build()).build())
                                                                                                                .build()).build())
                                                                                                .build()).build())
                                                                                .build()).build())
                                                                .addValues(Value.newBuilder().setStructValue(
                                                                        Struct.newBuilder()
                                                                                .putFields("type", Value.newBuilder()
                                                                                        .setStringValue("work").build())
                                                                                .putFields("geo", Value.newBuilder()
                                                                                        .setStructValue(Struct.newBuilder()
                                                                                                .putFields("latE6", Value.newBuilder()
                                                                                                        .setNumberValue(40710000).build())
                                                                                                .putFields("lonE6", Value.newBuilder()
                                                                                                        .setNumberValue(-74006000).build())
                                                                                                .putFields("zones", Value.newBuilder()
                                                                                                        .setListValue(ListValue.newBuilder()
                                                                                                                .addValues(Value.newBuilder().setStructValue(
                                                                                                                        Struct.newBuilder()
                                                                                                                                .putFields("id", Value.newBuilder()
                                                                                                                                        .setStringValue("A").build())
                                                                                                                                .putFields("flags", Value.newBuilder()
                                                                                                                                        .setListValue(ListValue.newBuilder()
                                                                                                                                                .addValues(Value.newBuilder()
                                                                                                                                                        .setBoolValue(true).build())
                                                                                                                                                .addValues(Value.newBuilder()
                                                                                                                                                        .setBoolValue(false).build())
                                                                                                                                                .build()).build())
                                                                                                                                .build()).build())
                                                                                                                .addValues(Value.newBuilder().setStructValue(
                                                                                                                        Struct.newBuilder()
                                                                                                                                .putFields("id", Value.newBuilder()
                                                                                                                                        .setStringValue("B").build())
                                                                                                                                .putFields("flags", Value.newBuilder()
                                                                                                                                        .setListValue(ListValue.newBuilder()
                                                                                                                                                .addValues(Value.newBuilder()
                                                                                                                                                        .setBoolValue(false).build())
                                                                                                                                                .addValues(Value.newBuilder()
                                                                                                                                                        .setBoolValue(true).build())
                                                                                                                                                .build()).build())
                                                                                                                                .build()).build())
                                                                                                                .build()).build())
                                                                                                .build()).build())
                                                                                .build()).build())
                                                                .build()).build())
                                                .build()).build())
                                .build()).build())
                .putFields("meta", Value.newBuilder().setStructValue(
                        Struct.newBuilder()
                                .putFields("tags", Value.newBuilder().setListValue(
                                        ListValue.newBuilder()
                                                .addValues(Value.newBuilder().setStringValue("gold").build())
                                                .addValues(Value.newBuilder().setStringValue("beta").build())
                                                .build()).build())
                                .putFields("scores", Value.newBuilder().setListValue(
                                        ListValue.newBuilder()
                                                .addValues(Value.newBuilder().setNumberValue(1).build())
                                                .addValues(Value.newBuilder().setNumberValue(2).build())
                                                .addValues(Value.newBuilder().setStructValue(
                                                        Struct.newBuilder()
                                                                .putFields("nested", Value.newBuilder().setListValue(
                                                                        ListValue.newBuilder()
                                                                                .addValues(Value.newBuilder().setNumberValue(3).build())
                                                                                .addValues(Value.newBuilder().setNumberValue(4).build())
                                                                                .addValues(Value.newBuilder().setStructValue(
                                                                                        Struct.newBuilder()
                                                                                                .putFields("final", Value.newBuilder()
                                                                                                        .setStringValue("x").build())
                                                                                                .build()).build())
                                                                                .build()).build())
                                                                .build()).build())
                                                .build()).build())
                                .build()).build())
                .build();

        GenericRow result = createGenericRowWithSingleFieldType(
                struct,
                VariantType.VARIANT,
                Descriptors.FieldDescriptor.Type.MESSAGE,
                null);

        assertVariantJsonEquals(
                "{\"meta\":{\"scores\":[1,2,{\"nested\":[3,4,{\"final\":\"x\"}]}],\"tags\":[\"gold\",\"beta\"]},"
                    + "\"profile\":{\"user\":{\"active\":true,\"addresses\":[{\"geo\":{\"history\":[{\"ok\":true,"
                    + "\"year\":2022},{\"ok\":false,\"reasons\":[\"moved\",\"updated\"],\"year\":2023}],"
                    + "\"latE6\":3.777E7,\"lonE6\":-1.2241E8},\"type\":\"home\"},{\"geo\":{\"latE6\":4.071E7,"
                    + "\"lonE6\":-7.4006E7,\"zones\":[{\"flags\":[true,false],\"id\":\"A\"},"
                    + "{\"flags\":[false,true],\"id\":\"B\"}]},\"type\":\"work\"}],\"id\":7,\"name\":\"alice\"}}}}",
                (GenericRow) result.getValue(0));
    }

    private void assertVariantJsonEquals(String expectedJson, GenericRow variantRow) throws Exception {
        assertNotNull(variantRow);
        byte[] metadataBytes = variantRow.getBinary(variantRow.getSchema().indexOf(DeltaVariantUtils.METADATA));
        byte[] valueBytes = variantRow.getBinary(variantRow.getSchema().indexOf(DeltaVariantUtils.VALUE));
        String actualJson = DeltaVariantUtils.deserializeToJsonString(metadataBytes, valueBytes);
        assertEquals(MAPPER.readTree(expectedJson), MAPPER.readTree(actualJson));
    }

    private void assertVariantJsonEquals(String expectedJson,
                                         io.delta.kernel.data.ColumnVector variantVector,
                                         int rowId) throws Exception {
        byte[] valueBytes = variantVector.getChild(0).getBinary(rowId);
        byte[] metadataBytes = variantVector.getChild(1).getBinary(rowId);
        String actualJson = DeltaVariantUtils.deserializeToJsonString(metadataBytes, valueBytes);
        assertEquals(MAPPER.readTree(expectedJson), MAPPER.readTree(actualJson));
    }

}
