/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.lakehouse.delta;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.BooleanNode;
import com.fasterxml.jackson.databind.node.NullNode;
import com.fasterxml.jackson.databind.node.NumericNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.ValueNode;
import io.delta.kernel.types.ArrayType;
import io.delta.kernel.types.BinaryType;
import io.delta.kernel.types.ByteType;
import io.delta.kernel.types.DataType;
import io.delta.kernel.types.DateType;
import io.delta.kernel.types.DecimalType;
import io.delta.kernel.types.DoubleType;
import io.delta.kernel.types.FloatType;
import io.delta.kernel.types.IntegerType;
import io.delta.kernel.types.LongType;
import io.delta.kernel.types.MapType;
import io.delta.kernel.types.ShortType;
import io.delta.kernel.types.StringType;
import io.delta.kernel.types.StructField;
import io.delta.kernel.types.StructType;
import io.delta.kernel.types.TimestampNTZType;
import io.delta.kernel.types.TimestampType;
import io.delta.kernel.types.VariantType;
import java.math.RoundingMode;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class JsonToDeltaConvert {

    public static GenericRow convert(JsonNode objectNode, StructType structType) {
        Map<Integer, Object> ordinalToValue = new HashMap<>();
        for (int i = 0; i < structType.length(); i++) {
            StructField field = structType.at(i);
            if (objectNode.has(field.getName())) {
                Object value = objectNode.get(field.getName());
                if (value instanceof NullNode) {
                    continue;
                }
                DataType type = field.getDataType();
                if (type instanceof StructType) {
                    value = convert((ObjectNode) value, (StructType) type);
                } else if (type instanceof ArrayType) {
                    if (value instanceof ArrayNode arrayNode) {
                        List<Object> array = new ArrayList<>();
                        DataType elementType = ((ArrayType) type).getElementType();
                        if (elementType instanceof StructType) {
                            for (JsonNode jsonNode : arrayNode) {
                                array.add(convert(jsonNode, (StructType) elementType));
                            }
                        } else {
                            for (JsonNode jsonNode : arrayNode) {
                                array.add(parseValue(jsonNode, elementType));
                            }
                        }
                        value = new ArrayValueImpl(array, elementType);
                    } else {
                        String base64 = parseValue((JsonNode) value, StringType.STRING).toString();
                        List<Object> array = new ArrayList<>();
                        byte[] byteArray = Base64.getDecoder().decode(base64);
                        for (byte ele : byteArray) {
                            array.add((long) ele);
                        }
                        value = new ArrayValueImpl(array, LongType.LONG);
                    }
                } else if (type instanceof BinaryType) {
                    //The kafka json writer use base64 to encode the bytes to string, so here we use base64 to
                    // decode the string to bytes.
                    // /** Refer {@link io.vertx.core.json.jackson.ByteArrayDeserializer#deserialize} */
                    value = Base64.getDecoder().decode(
                            parseValue((JsonNode) value, StringType.STRING).toString());
                } else if (type instanceof DecimalType) {
                    value = ((JsonNode) value).decimalValue()
                            .setScale(((DecimalType) type).getScale(), RoundingMode.UP);
                } else if (type instanceof MapType) {
                    Map<Object, Object> map = new HashMap<>();
                    Iterator<Map.Entry<String, JsonNode>> fields = ((ObjectNode) value).fields();
                    DataType keyType = ((MapType) type).getKeyType();
                    DataType valueType = ((MapType) type).getValueType();
                    while (fields.hasNext()) {
                        Map.Entry<String, JsonNode> ele = fields.next();
                        if (valueType instanceof StructType) {
                            map.put(ele.getKey(), convert(ele.getValue(), (StructType) valueType));
                        } else {
                            map.put(ele.getKey(), parseValue(ele.getValue(), valueType));
                        }
                    }
                    value = new MapValueImpl(map, keyType, valueType);
                } else if (type instanceof VariantType) {
                    value = DeltaVariantUtils.fromJsonNode((JsonNode) value);
                } else {
                    if (((JsonNode) value).isArray()) {
                        value = parseArrayValue((ArrayNode) value, type);
                    } else {
                        value = parseValue((ValueNode) value, type);
                    }
                }
                ordinalToValue.put(i, value);
            }
        }
        return new GenericRow(structType, ordinalToValue);
    }

    private static Object parseArrayValue(ArrayNode valueNode, DataType type) {
        if (type instanceof DateType) {
            int year = valueNode.get(0).intValue();
            int month = valueNode.get(1).intValue();
            int day = valueNode.get(2).intValue();
            LocalDate localDate = LocalDate.of(year, month, day);
            return (int) localDate.toEpochDay();
        }
        if (type instanceof TimestampNTZType || type instanceof TimestampType) {
            if (valueNode.size() == 3) {
                int hour = valueNode.get(0).intValue();
                int minute = valueNode.get(1).intValue();
                int second = valueNode.get(2).intValue();
                return LocalTime.of(hour, minute, second).atDate(LocalDate.now())
                        .atOffset(ZoneOffset.UTC).toInstant().toEpochMilli() * 1000;
            }
            if (valueNode.size() == 4) {
                int hour = valueNode.get(0).intValue();
                int minute = valueNode.get(1).intValue();
                int second = valueNode.get(2).intValue();
                int nano = valueNode.get(3).intValue();
                LocalTime localTime = LocalTime.of(hour, minute, second, nano);
                return localTime.atDate(LocalDate.now()).atOffset(ZoneOffset.UTC).toInstant().toEpochMilli() * 1000L
                        + nano / 1_000L;
            }
            if (valueNode.size() == 6) {
                int year = valueNode.get(0).intValue();
                int month = valueNode.get(1).intValue();
                int day = valueNode.get(2).intValue();
                int hour = valueNode.get(3).intValue();
                int minute = valueNode.get(4).intValue();
                int second = valueNode.get(5).intValue();
                LocalDateTime localDateTime = LocalDateTime.of(year, month, day, hour, minute, second);
                return localDateTime.atOffset(ZoneOffset.UTC).toInstant().toEpochMilli() * 1000L;
            }
            if (valueNode.size() == 7) {
                int year = valueNode.get(0).intValue();
                int month = valueNode.get(1).intValue();
                int day = valueNode.get(2).intValue();
                int hour = valueNode.get(3).intValue();
                int minute = valueNode.get(4).intValue();
                int second = valueNode.get(5).intValue();
                int nano = valueNode.get(6).intValue();
                LocalDateTime localDateTime = LocalDateTime.of(year, month, day, hour, minute, second, nano);
                return localDateTime.atOffset(ZoneOffset.UTC).toInstant().toEpochMilli() * 1000L
                        + nano / 1_000L;
            }
        }
        throw new IllegalArgumentException("Unknown data type " + type);
    }


    private static Object parseValue(JsonNode valueNode, DataType type) {
        if (valueNode instanceof NumericNode) {
            if (type instanceof IntegerType) {
                return valueNode.intValue();
            } else if (type instanceof LongType) {
                return valueNode.longValue();
            } else if (type instanceof FloatType) {
                return valueNode.floatValue();
            } else if (type instanceof DoubleType) {
                return valueNode.doubleValue();
            } else if (type instanceof ShortType) {
                return valueNode.shortValue();
            } else if (type instanceof ByteType) {
                return (byte) valueNode.intValue();
            } else if (type instanceof TimestampNTZType) {
                return valueNode.longValue() * 1000 * 1000;
            } else if (type instanceof TimestampType) {
                return valueNode.longValue() * 1000 * 1000;
            }
        } else if (valueNode instanceof BooleanNode) {
            return valueNode.booleanValue();
        }
        return valueNode.asText();
    }
}
