/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.lakehouse.delta;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.EnumValueDescriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Message;
import com.google.protobuf.util.JsonFormat;
import io.delta.kernel.data.ArrayValue;
import io.delta.kernel.data.ColumnVector;
import io.delta.kernel.data.MapValue;
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
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ProtobufToDeltaConverter {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    // Thread-local to handle circular references in schema inference
    private static final ThreadLocal<Set<String>> VISITED_TYPES = ThreadLocal.withInitial(HashSet::new);

    /**
     * Converts a protobuf message to a Delta GenericRow.
     * @param message The protobuf message to convert
     * @param schema The target Delta table schema
     * @return GenericRow compatible with Delta table
     */
    public static GenericRow convertToGenericRow(Message message, StructType schema) {
        if (message == null) {
            throw new IllegalArgumentException("Message cannot be null");
        }
        if (schema == null) {
            throw new IllegalArgumentException("Schema cannot be null");
        }

        Map<Integer, Object> ordinalToValue = new HashMap<>();

        for (int i = 0; i < schema.length(); i++) {
            StructField field = schema.at(i);
            Object value = extractFieldValue(message, field.getName(), field.getDataType());
            ordinalToValue.put(i, value);
        }

        return new GenericRow(schema, ordinalToValue);
    }

    /**
     * Extracts field value from protobuf message based on field name and expected data type.
     */
    private static Object extractFieldValue(Message message, String fieldName, DataType dataType) {
        if (message == null || fieldName == null) {
            return getDefaultValue(dataType);
        }

        Descriptor descriptor = message.getDescriptorForType();
        if (descriptor == null) {
            return getDefaultValue(dataType);
        }

        FieldDescriptor fieldDescriptor = descriptor.findFieldByName(fieldName);

        if (fieldDescriptor == null) {
            // Field not found in protobuf, return default value
            return getDefaultValue(dataType);
        }

        // Check if the field is actually set in the message
        // For MESSAGE types, check hasField to distinguish between null and default
        // For primitive types, we always get the value (which may be the default)
        if (!fieldDescriptor.isRepeated()
            && fieldDescriptor.getType() == FieldDescriptor.Type.MESSAGE
            && !message.hasField(fieldDescriptor)) {
            return getDefaultValue(dataType);
        }

        Object fieldValue = message.getField(fieldDescriptor);
        return convertProtobufValue(fieldValue, fieldDescriptor, dataType);
    }

    /**
     * Converts protobuf field value to appropriate Delta data type.
     */
    private static Object convertProtobufValue(Object value, FieldDescriptor fieldDescriptor, DataType targetType) {
        if (fieldDescriptor == null) {
            return getDefaultValue(targetType);
        }

        if (value == null) {
            return getDefaultValue(targetType);
        }

        // A protobuf field marked as logical_type=variant should be converted as a single variant value,
        // even if the protobuf representation is repeated or map-like.
        if (targetType instanceof VariantType) {
            return convertToVariantValue(value, fieldDescriptor);
        }

        // Handle repeated fields (arrays)
        if (fieldDescriptor.isRepeated()) {
            @SuppressWarnings("unchecked")
            List<Object> list = (List<Object>) value;
            DataType elementType = getArrayElementType(targetType);

            List<Object> convertedList = list.stream()
                .map(item -> convertSingleValue(item, fieldDescriptor, elementType))
                .collect(Collectors.toList());

            return new GenericArrayValue(convertedList, (ArrayType) targetType);
        } else {
            return convertSingleValue(value, fieldDescriptor, targetType);
        }
    }

    /**
     * Converts a single protobuf value to the target data type.
     */
    private static Object convertSingleValue(Object value, FieldDescriptor fieldDescriptor, DataType targetType) {
        if (value == null) {
            return null;
        }

        // Check if fieldDescriptor is null
        if (fieldDescriptor == null) {
            return convertValueToTargetType(value, targetType);
        }

        FieldDescriptor.Type protobufType = fieldDescriptor.getType();

        if (protobufType == FieldDescriptor.Type.STRING && targetType instanceof StringType) {
            return value.toString();
        }

        // Numeric conversions
        if (isIntegerType(protobufType) && targetType instanceof IntegerType) {
            return ((Number) value).intValue();
        }

        if (isLongType(protobufType) && targetType instanceof LongType) {
            Number numValue = (Number) value;
            return numValue.longValue();
        }

        if (protobufType == FieldDescriptor.Type.FLOAT && targetType instanceof FloatType) {
            return ((Number) value).floatValue();
        }

        if (protobufType == FieldDescriptor.Type.DOUBLE && targetType instanceof DoubleType) {
            return ((Number) value).doubleValue();
        }

        // Boolean conversion
        if (protobufType == FieldDescriptor.Type.BOOL && targetType instanceof BooleanType) {
            return value;
        }

        // Bytes conversion
        if (protobufType == FieldDescriptor.Type.BYTES && targetType instanceof BinaryType) {
            if (value instanceof ByteString byteString) {
                return byteString.toByteArray();
            } else if (value instanceof byte[]) {
                return value;
            }
        }

        // Enum conversion
        if (protobufType == FieldDescriptor.Type.ENUM) {
            if (value instanceof EnumValueDescriptor enumValue) {
                if (targetType instanceof StringType) {
                    return enumValue.getName();
                } else if (targetType instanceof IntegerType) {
                    return enumValue.getNumber();
                }
            }
        }

        // Message/Struct conversion
        if (protobufType == FieldDescriptor.Type.MESSAGE) {
            if (value instanceof Message nestedMessage) {

                if (targetType instanceof StructType) {
                    return convertToGenericRow(nestedMessage, (StructType) targetType);
                } else if (isTimestampType(targetType)) {
                    return convertProtobufTimestamp(nestedMessage);
                }
            }
        }

        // Default: try to convert to target type
        return convertValueToTargetType(value, targetType);
    }

    /**
     * Helper method to convert a value to the target type when fieldDescriptor is null.
     */
    private static Object convertValueToTargetType(Object value, DataType targetType) {
        if (value == null) {
            return null;
        }

        try {
            if (targetType instanceof VariantType) {
                if (value instanceof GenericRow genericRow) {
                    return genericRow;
                }
                if (value instanceof Message message) {
                    return DeltaVariantUtils.fromJson(JsonFormat.printer()
                            .omittingInsignificantWhitespace().print(message));
                }
                if (value instanceof ByteString byteString) {
                    return DeltaVariantUtils.fromValue(
                            java.util.Base64.getEncoder().encodeToString(byteString.toByteArray()));
                }
                if (value instanceof byte[] bytes) {
                    return DeltaVariantUtils.fromValue(java.util.Base64.getEncoder().encodeToString(bytes));
                }
                return DeltaVariantUtils.fromValue(value);
            }
            if (targetType instanceof StringType) {
                return value.toString();
            } else if (targetType instanceof IntegerType && value instanceof Number) {
                return ((Number) value).intValue();
            } else if (targetType instanceof LongType && value instanceof Number) {
                return ((Number) value).longValue();
            } else if (targetType instanceof FloatType && value instanceof Number) {
                return ((Number) value).floatValue();
            } else if (targetType instanceof DoubleType && value instanceof Number) {
                return ((Number) value).doubleValue();
            } else if (targetType instanceof BooleanType && value instanceof Boolean) {
                return value;
            } else if (targetType instanceof BinaryType && value instanceof byte[]) {
                return value;
            } else if (targetType instanceof BinaryType && value instanceof ByteString) {
                return ((ByteString) value).toByteArray();
            }
        } catch (Exception e) {
            log.warn("Conversion failed for value: " + value + ", target type: " + targetType, e);
            return value.toString();
        }

        // Default: return string representation
        return value.toString();
    }

    private static GenericRow convertToVariantValue(Object value, FieldDescriptor fieldDescriptor) {
        try {
            if (value instanceof Message message) {
                return DeltaVariantUtils.fromJson(
                        JsonFormat.printer().omittingInsignificantWhitespace().print(message));
            }
            if (value instanceof ByteString byteString) {
                return DeltaVariantUtils.fromValue(
                        java.util.Base64.getEncoder().encodeToString(byteString.toByteArray()));
            }
            if (value instanceof byte[] bytes) {
                return DeltaVariantUtils.fromValue(java.util.Base64.getEncoder().encodeToString(bytes));
            }
            if (fieldDescriptor != null && fieldDescriptor.getType() == FieldDescriptor.Type.ENUM
                    && value instanceof EnumValueDescriptor enumValueDescriptor) {
                return DeltaVariantUtils.fromValue(enumValueDescriptor.getName());
            }
            return DeltaVariantUtils.fromValue(normalizeProtobufVariantValue(value, fieldDescriptor));
        } catch (Exception e) {
            throw new IllegalArgumentException("Failed to convert protobuf value to Delta variant: " + value, e);
        }
    }

    private static Object normalizeProtobufVariantValue(Object value, FieldDescriptor fieldDescriptor) {
        if (value == null) {
            return null;
        }
        if (value instanceof ByteString byteString) {
            return java.util.Base64.getEncoder().encodeToString(byteString.toByteArray());
        }
        if (value instanceof byte[] bytes) {
            return java.util.Base64.getEncoder().encodeToString(bytes);
        }
        if (value instanceof EnumValueDescriptor enumValueDescriptor) {
            return enumValueDescriptor.getName();
        }
        if (fieldDescriptor != null && fieldDescriptor.isMapField() && value instanceof List<?> entries) {
            return normalizeMapEntries(entries);
        }
        if (value instanceof List<?> list) {
            List<Object> normalized = new ArrayList<>(list.size());
            for (Object item : list) {
                normalized.add(normalizeProtobufVariantValue(item, fieldDescriptor));
            }
            return normalized;
        }
        if (value instanceof Message message) {
            return normalizeProtobufMessage(message);
        }
        return value;
    }

    private static Map<String, Object> normalizeMapEntries(List<?> entries) {
        Map<String, Object> result = new LinkedHashMap<>();
        for (Object entry : entries) {
            if (!(entry instanceof Message entryMessage)) {
                continue;
            }
            FieldDescriptor keyField = entryMessage.getDescriptorForType().findFieldByName("key");
            FieldDescriptor valueField = entryMessage.getDescriptorForType().findFieldByName("value");
            Object key = keyField != null ? entryMessage.getField(keyField) : null;
            Object mapValue = valueField != null ? entryMessage.getField(valueField) : null;
            result.put(key == null ? null : key.toString(), normalizeProtobufVariantValue(mapValue, valueField));
        }
        return result;
    }

    private static Object normalizeProtobufMessage(Message message) {
        try {
            return MAPPER.readValue(
                    JsonFormat.printer().omittingInsignificantWhitespace().print(message),
                    Object.class);
        } catch (Exception e) {
            throw new IllegalArgumentException("Failed to normalize protobuf message for Delta variant", e);
        }
    }

    /**
     * Converts protobuf Timestamp to appropriate type based on target.
     */
    private static Object convertProtobufTimestamp(Message timestampMessage) {
        if (timestampMessage == null) {
            return null;
        }

        Descriptor descriptor = timestampMessage.getDescriptorForType();
        if (descriptor == null) {
            return null;
        }

        FieldDescriptor secondsField = descriptor.findFieldByName("seconds");
        FieldDescriptor nanosField = descriptor.findFieldByName("nanos");

        long seconds = 0L;
        int nanos = 0;

        if (secondsField != null && timestampMessage.hasField(secondsField)) {
            Object secondsValue = timestampMessage.getField(secondsField);
            if (secondsValue instanceof Number) {
                seconds = ((Number) secondsValue).longValue();
            }
        }

        if (nanosField != null && timestampMessage.hasField(nanosField)) {
            Object nanosValue = timestampMessage.getField(nanosField);
            if (nanosValue instanceof Number) {
                nanos = ((Number) nanosValue).intValue();
            }
        }

        // Convert to milliseconds since epoch - fix precision loss
        return seconds * 1000L + nanos / 1_000_000L;
    }

    /**
     * Helper method to check if protobuf type is integer.
     */
    private static boolean isIntegerType(FieldDescriptor.Type type) {
        return type == FieldDescriptor.Type.INT32
            || type == FieldDescriptor.Type.SINT32
            || type == FieldDescriptor.Type.SFIXED32;
    }

    /**
     * Helper method to check if protobuf type should be treated as long.
     */
    private static boolean isLongType(FieldDescriptor.Type type) {
        return type == FieldDescriptor.Type.INT64
            || type == FieldDescriptor.Type.SINT64
            || type == FieldDescriptor.Type.SFIXED64
            || type == FieldDescriptor.Type.UINT32
            || type == FieldDescriptor.Type.UINT64;
    }

    /**
     * Gets the element type for array fields.
     */
    private static DataType getArrayElementType(DataType dataType) {
        if (dataType instanceof ArrayType) {
            return ((ArrayType) dataType).getElementType();
        }
        throw new IllegalArgumentException("Expected ArrayType but got: " + dataType);
    }

    /**
     * Checks if target type is timestamp-like.
     */
    private static boolean isTimestampType(DataType dataType) {
        return dataType instanceof LongType; // Assuming timestamps are stored as long (millis)
    }

    /**
     * Returns default value for a given data type.
     */
    private static Object getDefaultValue(DataType dataType) {
        if (dataType == null) {
            return null;
        }

        if (dataType instanceof StringType) {
            return null;
        }
        if (dataType instanceof IntegerType) {
            return null;
        }
        if (dataType instanceof LongType) {
            return null;
        }
        if (dataType instanceof FloatType) {
            return null;
        }
        if (dataType instanceof DoubleType) {
            return null;
        }
        if (dataType instanceof BooleanType) {
            return null;
        }
        if (dataType instanceof BinaryType) {
            return null;
        }
        if (dataType instanceof ArrayType) {
            return new GenericArrayValue(new ArrayList<>(), (ArrayType) dataType);
        }
        if (dataType instanceof StructType) {
            return null;
        }

        return null;
    }

    /**
     * Helper method to infer Delta schema from protobuf descriptor.
     */
    public static StructType inferSchemaFromProtobuf(Descriptor descriptor) {
        if (descriptor == null) {
            throw new IllegalArgumentException("Descriptor cannot be null");
        }

        try {
            VISITED_TYPES.get().clear(); // Clear at the start of schema inference

            List<StructField> fields = descriptor.getFields().stream()
                .map(field -> new StructField(
                    field.getName(),
                    inferDataTypeFromProtobuf(field),
                    !field.isRequired()
                ))
                .collect(Collectors.toList());

            return new StructType(fields);
        } finally {
            VISITED_TYPES.get().clear(); // Clean up
        }
    }

    /**
     * Infers Delta data type from protobuf field descriptor.
     */
    private static DataType inferDataTypeFromProtobuf(FieldDescriptor field) {
        if (field == null) {
            return StringType.STRING; // Default fallback
        }

        DataType baseType;

        switch (field.getType()) {
            case STRING:
                baseType = StringType.STRING;
                break;
            case INT32:
            case SINT32:
            case SFIXED32:
                baseType = IntegerType.INTEGER;
                break;
            case UINT32: // Map UINT32 to Long for Delta compatibility
            case INT64:
            case SINT64:
            case SFIXED64:
            case UINT64:
                baseType = LongType.LONG;
                break;
            case FLOAT:
                baseType = FloatType.FLOAT;
                break;
            case DOUBLE:
                baseType = DoubleType.DOUBLE;
                break;
            case BOOL:
                baseType = BooleanType.BOOLEAN;
                break;
            case BYTES:
                baseType = BinaryType.BINARY;
                break;
            case ENUM:
                baseType = StringType.STRING; // or IntegerType depending on preference
                break;
            case MESSAGE:
                Descriptor messageType = field.getMessageType();
                if (isTimestampMessage(messageType)) {
                    baseType = LongType.LONG; // Store timestamps as long (millis)
                } else if (messageType != null) {
                    String typeName = messageType.getFullName();
                    Set<String> visitedTypes = VISITED_TYPES.get();

                    if (visitedTypes.contains(typeName)) {
                        // Circular reference detected, return string type
                        log.warn("Circular reference detected for type: " + typeName);
                        baseType = StringType.STRING;
                    } else {
                        visitedTypes.add(typeName);
                        try {
                            baseType = inferSchemaFromProtobuf(messageType);
                        } finally {
                            visitedTypes.remove(typeName);
                        }
                    }
                } else {
                    baseType = StringType.STRING; // Fallback
                }
                break;
            default:
                baseType = StringType.STRING;
        }

        // If field is repeated, wrap in ArrayType
        if (field.isRepeated()) {
            return new ArrayType(baseType, true);
        }

        return baseType;
    }

    /**
     * Checks if a message type is a protobuf Timestamp.
     */
    private static boolean isTimestampMessage(Descriptor messageType) {
        if (messageType == null) {
            return false;
        }
        return messageType.getFullName().equals("google.protobuf.Timestamp");
    }

    /**
     * Batch conversion helper.
     */
    public static List<GenericRow> convertMessages(List<Message> messages, StructType schema) {
        if (messages == null) {
            return new ArrayList<>();
        }

        return messages.stream()
            .filter(message -> message != null)
            .map(message -> convertToGenericRow(message, schema))
            .collect(Collectors.toList());
    }

    /**
     * Simple implementation of ArrayValue for GenericRow.
     */
    private static class GenericArrayValue implements ArrayValue {
        private final List<Object> elements;
        private final ArrayType arrayType;

        public GenericArrayValue(List<Object> elements, ArrayType arrayType) {
            this.elements = elements != null ? elements : new ArrayList<>();
            this.arrayType = arrayType;
        }

        @Override
        public int getSize() {
            return elements.size();
        }

        @Override
        public ColumnVector getElements() {
            return new GenericColumnVector(elements, arrayType.getElementType());
        }
    }

    /**
     * Simple implementation of ColumnVector for array elements.
     */
    private static class GenericColumnVector implements ColumnVector {
        private final List<Object> values;
        private final DataType elementType;

        public GenericColumnVector(List<Object> values, DataType elementType) {
            this.values = values != null ? values : new ArrayList<>();
            this.elementType = elementType;
        }

        @Override
        public int getSize() {
            return values.size();
        }

        @Override
        public void close() {
            // No resources to close
        }

        @Override
        public DataType getDataType() {
            return elementType;
        }

        @Override
        public boolean isNullAt(int rowId) {
            if (rowId < 0 || rowId >= values.size()) {
                return true;
            }
            return values.get(rowId) == null;
        }

        @Override
        public boolean getBoolean(int rowId) {
            Object value = values.get(rowId);
            if (value instanceof Boolean) {
                return (Boolean) value;
            }
            throw new IllegalStateException("Value at row " + rowId + " is not a Boolean: " + value);
        }

        @Override
        public byte getByte(int rowId) {
            Object value = values.get(rowId);
            if (value instanceof Number) {
                return ((Number) value).byteValue();
            }
            throw new IllegalStateException("Value at row " + rowId + " is not a Number: " + value);
        }

        @Override
        public short getShort(int rowId) {
            Object value = values.get(rowId);
            if (value instanceof Number) {
                return ((Number) value).shortValue();
            }
            throw new IllegalStateException("Value at row " + rowId + " is not a Number: " + value);
        }

        @Override
        public int getInt(int rowId) {
            Object value = values.get(rowId);
            if (value instanceof Number) {
                return ((Number) value).intValue();
            }
            throw new IllegalStateException("Value at row " + rowId + " is not a Number: " + value);
        }

        @Override
        public long getLong(int rowId) {
            Object value = values.get(rowId);
            if (value instanceof Number) {
                return ((Number) value).longValue();
            }
            throw new IllegalStateException("Value at row " + rowId + " is not a Number: " + value);
        }

        @Override
        public float getFloat(int rowId) {
            Object value = values.get(rowId);
            if (value instanceof Number) {
                return ((Number) value).floatValue();
            }
            throw new IllegalStateException("Value at row " + rowId + " is not a Number: " + value);
        }

        @Override
        public double getDouble(int rowId) {
            Object value = values.get(rowId);
            if (value instanceof Number) {
                return ((Number) value).doubleValue();
            }
            throw new IllegalStateException("Value at row " + rowId + " is not a Number: " + value);
        }

        @Override
        public BigDecimal getDecimal(int rowId) {
            Object value = values.get(rowId);
            if (value instanceof BigDecimal) {
                return (BigDecimal) value;
            } else if (value instanceof Number) {
                return BigDecimal.valueOf(((Number) value).doubleValue());
            }
            throw new IllegalStateException("Value at row " + rowId + " is not a Decimal: " + value);
        }

        @Override
        public String getString(int rowId) {
            Object value = values.get(rowId);
            if (value == null) {
                return null;
            }
            return value.toString();
        }

        @Override
        public byte[] getBinary(int rowId) {
            Object value = values.get(rowId);
            if (value instanceof byte[]) {
                return (byte[]) value;
            }
            throw new IllegalStateException("Value at row " + rowId + " is not a byte array: " + value);
        }

        @Override
        public ArrayValue getArray(int rowId) {
            Object value = values.get(rowId);
            if (value instanceof ArrayValue) {
                return (ArrayValue) value;
            }
            throw new IllegalStateException("Value at row " + rowId + " is not an ArrayValue: " + value);
        }

        @Override
        public MapValue getMap(int rowId) {
            Object value = values.get(rowId);
            if (value instanceof MapValue) {
                return (MapValue) value;
            }
            throw new IllegalStateException("Value at row " + rowId + " is not a MapValue: " + value);
        }

        @Override
        public ColumnVector getChild(int ordinal) {
            StructType childStructType = null;
            if (elementType instanceof StructType structType) {
                childStructType = structType;
            } else if (elementType instanceof VariantType) {
                childStructType = DeltaVariantUtils.variantSchema();
            }

            if (childStructType != null) {
                List<StructField> fields = childStructType.fields();
                if (ordinal < 0 || ordinal >= fields.size()) {
                    throw new IndexOutOfBoundsException("Invalid struct field ordinal: " + ordinal);
                }

                List<Object> fieldData = new ArrayList<>();
                for (Object row : values) {
                    if (row == null) {
                        fieldData.add(null);
                    } else {
                        fieldData.add(((GenericRow) row).getValue(ordinal));
                    }
                }
                return new GenericColumnVector(fieldData, fields.get(ordinal).getDataType());
            }
            throw new UnsupportedOperationException(
                    "Child vectors are not available for vector of type " + getDataType());        }
    }
}
