/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.lakehouse.iceberg;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.DecimalNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.IOException;
import java.math.RoundingMode;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.variants.Variant;
import org.apache.iceberg.variants.VariantMetadata;
import org.apache.iceberg.variants.Variants;


@Slf4j
public class JsonToIcebergConverter {

    // F# DateTime.Ticks epoch difference from Unix epoch in 100-nanosecond intervals
    // F# epoch: 0001-01-01 00:00:00, Unix epoch: 1970-01-01 00:00:00
    private static final long FSHARP_EPOCH_DIFF_TICKS = 621355968000000000L;
    private static final long TICKS_PER_MILLISECOND = 10000L;

    /**
     * Converts a JSON object to an Iceberg Record.
     *
     * <p>This method handles conversion of various data types including timestamps with a heuristic
     * approach for determining timestamp precision. The timestamp heuristic works as follows:
     * <ul>
     *   <li>Values &lt; 10^12 are interpreted as seconds since epoch</li>
     *   <li>Values between 10^12 and 10^15 are interpreted as milliseconds since epoch</li>
     *   <li>Values &gt;= 10^15 are interpreted as microseconds since epoch</li>
     * </ul>
     *
     * <p>Note: The heuristic may misinterpret edge cases:
     * <ul>
     *   <li>Timestamps in seconds from year ~2033 onwards could be misinterpreted as milliseconds</li>
     *   <li>Very early timestamps in milliseconds could be misinterpreted as seconds</li>
     * </ul>
     *
     * @param schema the Iceberg schema defining the expected structure
     * @param objectNode the JSON object to convert
     * @return the converted Iceberg Record
     * @throws IllegalArgumentException if required fields are missing
     * @throws UnsupportedOperationException if unsupported types are encountered
     */
    public static Record convertToRecord(Schema schema, ObjectNode objectNode) {
        Record record = GenericRecord.create(schema);
        for (Types.NestedField field : schema.columns()) {
            String fieldName = field.name();
            JsonNode jsonValue = objectNode.get(fieldName);

            if (jsonValue == null) {
                if (field.isRequired()) {
                    throw new IllegalArgumentException("Missing required field: " + fieldName);
                }
                record.setField(fieldName, null);
                continue;
            }

            Object value = fromJsonNode(jsonValue, field.type());
            record.setField(fieldName, value);
        }
        return record;
    }

    private static Object fromJsonNode(JsonNode jsonNode, Type type) {
        if (jsonNode == null || jsonNode.isNull()) {
            return null;
        }

        if (type instanceof Types.BooleanType) {
            return jsonNode.asBoolean();
        } else if (type instanceof Types.IntegerType) {
            return jsonNode.asInt();
        } else if (type instanceof Types.LongType) {
            return jsonNode.asLong();
        } else if (type instanceof Types.FloatType) {
            return (float) jsonNode.asDouble();
        } else if (type instanceof Types.DoubleType) {
            return jsonNode.asDouble();
        } else if (type instanceof Types.StringType) {
            return jsonNode.asText();
        } else if (type instanceof Types.DateType) {
            // Handle date conversion (assuming stored as epoch days)
            if (jsonNode instanceof ArrayNode arrayNode) {
                if (arrayNode.size() == 3) {
                    int year = arrayNode.get(0).intValue();
                    int month = arrayNode.get(1).intValue();
                    int day = arrayNode.get(2).intValue();
                    return LocalDate.of(year, month, day);
                }
                throw new UnsupportedOperationException("Unsupported type: " + type  + " for data: " + jsonNode);
            }
            return LocalDate.ofEpochDay(jsonNode.asLong());
        } else if (type instanceof Types.TimeType) {
            if (jsonNode instanceof ArrayNode arrayNode) {
                if (arrayNode.size() == 2 || arrayNode.size() == 3) {
                    int hour = arrayNode.get(0).intValue();
                    int minute = arrayNode.get(1).intValue();
                    int second = arrayNode.size() == 3 ? arrayNode.get(2).intValue() : 0;
                    return LocalTime.of(hour, minute, second);
                }
                throw new UnsupportedOperationException("Unsupported type: " + type  + " for data: " + jsonNode);
            }
            return LocalTime.ofNanoOfDay(jsonNode.longValue());
        } else if (type instanceof Types.TimestampType) {
            Types.TimestampType timestampType = (Types.TimestampType) type;
            boolean withZone = timestampType.shouldAdjustToUTC();
            // Handle timestamp conversion
            if (jsonNode instanceof ArrayNode arrayNode) {
                if (arrayNode.size() == 5 || arrayNode.size() == 6) {
                    int year = arrayNode.get(0).intValue();
                    int month = arrayNode.get(1).intValue();
                    int day = arrayNode.get(2).intValue();
                    int hour = arrayNode.get(3).intValue();
                    int minute = arrayNode.get(4).intValue();
                    int second = arrayNode.size() == 6 ? arrayNode.get(5).intValue() : 0;
                    LocalDateTime localDateTime = LocalDateTime.of(year, month, day, hour, minute, second);
                    return withZone ? localDateTime.atOffset(ZoneOffset.UTC) : localDateTime;
                }
                throw new UnsupportedOperationException("Unsupported type: " + type  + " for data: " + jsonNode);
            } else if (jsonNode instanceof DecimalNode) {
                LocalDateTime localDateTime = LocalDateTime.ofInstant(
                        Instant.ofEpochSecond(jsonNode.longValue()),
                        ZoneOffset.UTC);
                return withZone ? localDateTime.atOffset(ZoneOffset.UTC) : localDateTime;
            } else if (jsonNode.isTextual()) {
                Instant instant = Instant.parse(jsonNode.asText());
                LocalDateTime localDateTime = LocalDateTime.ofInstant(instant, ZoneOffset.UTC);
                return withZone ? localDateTime.atOffset(ZoneOffset.UTC) : localDateTime;
            } else {
                long value = jsonNode.longValue();
                Instant instant = convertTimestampToInstant(value);
                LocalDateTime localDateTime = LocalDateTime.ofInstant(instant, ZoneOffset.UTC);
                return withZone ? localDateTime.atOffset(ZoneOffset.UTC) : localDateTime;
            }
        } else if (type instanceof Types.BinaryType) {
            try {
                return ByteBuffer.wrap(jsonNode.binaryValue());
            } catch (IOException e) {
                throw new RuntimeException("Error converting binary value", e);
            }
        } else if (type instanceof Types.StructType) {
            return convertToStruct((Types.StructType) type, (ObjectNode) jsonNode);
        } else if (type instanceof Types.ListType) {
            return convertToList((Types.ListType) type, jsonNode);
        } else if (type instanceof Types.MapType) {
            return convertToMap((Types.MapType) type, jsonNode);
        } else if (type instanceof Types.DecimalType) {
            int scale = ((Types.DecimalType) type).scale();
            return jsonNode.decimalValue().setScale(scale, RoundingMode.UP);
        } else if (type instanceof Types.VariantType) {
            return convertToVariant(jsonNode);
        } else {
            throw new UnsupportedOperationException("Unsupported type: " + type  + " for data: " + jsonNode);
        }
    }

    private static Variant convertToVariant(JsonNode jsonNode) {
        // For simplicity, we treat the entire JSON node as the variant value.
        // A more sophisticated implementation may be needed based on actual requirements.
        if (jsonNode == null) {
            return Variant.of(VariantMetadata.empty(), Variants.ofNull());
        }

        // --- Primitives ---
        if (jsonNode.isBoolean()) {
            return Variant.of(VariantMetadata.empty(), Variants.of(jsonNode.booleanValue()));
        }

        if (jsonNode.isInt()) {
            return Variant.of(VariantMetadata.empty(), Variants.of(jsonNode.intValue()));
        }

        if (jsonNode.isLong()) {
            return Variant.of(VariantMetadata.empty(), Variants.of(jsonNode.longValue()));
        }

        if (jsonNode.isFloat()) {
            return Variant.of(VariantMetadata.empty(), Variants.of(jsonNode.floatValue()));
        }

        if (jsonNode.isDouble()) {
            return Variant.of(VariantMetadata.empty(), Variants.of(jsonNode.doubleValue()));
        }

        if (jsonNode.isTextual()) {
            return Variant.of(VariantMetadata.empty(), Variants.of(jsonNode.textValue()));
        }

        // --- Complex Types ---
        if (jsonNode.isBinary()) {
            try {
                return Variant.of(VariantMetadata.empty(), Variants.of(ByteBuffer.wrap(jsonNode.binaryValue())));
            } catch (IOException e) {
                throw new RuntimeException("Error converting binary value", e);
            }
        }

        if (jsonNode.isBigDecimal()) {
            return Variant.of(VariantMetadata.empty(), Variants.of(jsonNode.decimalValue()));
        }

        if (jsonNode.isBigInteger()) {
            return Variant.of(VariantMetadata.empty(), Variants.of(jsonNode.bigIntegerValue().intValue()));
        }

        if (jsonNode.isEmpty() || jsonNode.isMissingNode()) {
            return Variant.of(VariantMetadata.empty(), Variants.ofNull());
        }

        if (jsonNode.isFloatingPointNumber()) {
            return Variant.of(VariantMetadata.empty(), Variants.of(jsonNode.doubleValue()));
        }

        if (jsonNode.isShort()) {
            return Variant.of(VariantMetadata.empty(), Variants.of(jsonNode.shortValue()));
        }

        return Variant.of(VariantMetadata.empty(), Variants.of(jsonNode.toString()));
    }

    private static Record convertToStruct(Types.StructType structType, ObjectNode objectNode) {
        return convertToRecord(new Schema(structType.fields()), objectNode);
    }

    private static List<Object> convertToList(Types.ListType listType, JsonNode jsonNode) {
        Type elementType = listType.elementType();
        List<Object> list = new ArrayList<>();
        for (JsonNode elementNode : jsonNode) {
            list.add(fromJsonNode(elementNode, elementType));
        }
        return list;
    }

    private static Map<Object, Object> convertToMap(Types.MapType mapType, JsonNode jsonNode) {
        Type keyType = mapType.keyType();
        Type valueType = mapType.valueType();
        Map<Object, Object> map = new HashMap<>();

        if (jsonNode.isObject()) {
            // Handle JSON object style map (string keys only)
            jsonNode.fields().forEachRemaining(entry -> {
                Object key = fromJsonNode(JsonNodeFactory.instance.textNode(entry.getKey()), keyType);
                Object value = fromJsonNode(entry.getValue(), valueType);
                map.put(key, value);
            });
        } else if (jsonNode.isArray()) {
            // Handle array of {key: ..., value: ...} objects
            for (JsonNode entryNode : jsonNode) {
                Object key = fromJsonNode(entryNode.get("key"), keyType);
                Object value = fromJsonNode(entryNode.get("value"), valueType);
                map.put(key, value);
            }
        } else {
            throw new IllegalArgumentException("Unsupported JSON format for map");
        }

        return map;
    }

    /**
     * Convert timestamp value to Instant with automatic format detection.
     * Supports:
     * - F# DateTime.Ticks (100-nanosecond intervals since 0001-01-01)
     * - Unix seconds
     * - Unix milliseconds
     * - Unix microseconds
     *
     * @param value the timestamp value
     * @return the corresponding Instant
     */
    private static Instant convertTimestampToInstant(long value) {
        // F# DateTime.Ticks detection:
        // Current F# ticks are in the range of 6.3e17
        // This is much larger than any reasonable Unix timestamp even in nanoseconds
        if (value > 1e17) {
            // F# DateTime.Ticks: 100-nanosecond intervals since 0001-01-01
            // Convert to Unix milliseconds
            long unixMillis = (value - FSHARP_EPOCH_DIFF_TICKS) / TICKS_PER_MILLISECOND;
            return Instant.ofEpochMilli(unixMillis);
        }

        // Unix timestamp detection based on magnitude:
        // - Seconds: < 10^12 (year ~33658)
        // - Milliseconds: 10^12 to 10^15 (year 2001 to ~33658000)
        // - Microseconds: 10^15 to 10^18

        if (value < 1e12) {
            // Unix seconds
            return Instant.ofEpochSecond(value);
        } else if (value < 1e15) {
            // Unix milliseconds
            return Instant.ofEpochMilli(value);
        } else {
            // Unix microseconds
            return Instant.ofEpochSecond(
                value / 1_000_000,
                (value % 1_000_000) * 1000);
        }
    }
}
