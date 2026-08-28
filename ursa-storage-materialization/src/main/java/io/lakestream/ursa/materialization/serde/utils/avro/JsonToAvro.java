/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.materialization.serde.utils.avro;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.lakestream.ursa.materialization.serde.exception.ConvertException;
import java.io.IOException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DecoderFactory;

/**
 * Utility class for converting JSON schema definitions to Avro Schema objects.
 * Provides functionality to:
 * - Convert JSON schema to Avro schema
 * - Handle complex type conversions
 * - Support schema evolution
 */
@Slf4j
public class JsonToAvro {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    public static GenericRecord convert(String jsonString, Schema schema) {
        try {
            String formattedJson = formatUnionFieldsRecursive(jsonString, schema);
            var decoder = DecoderFactory.get().jsonDecoder(schema, formattedJson);
            var reader = new GenericDatumReader<GenericRecord>(schema);
            return reader.read(null, decoder);
        } catch (Exception e) {
            throw new ConvertException("Failed to convert JSON to Avro GenericRecord", e);
        }
    }

    private static String formatUnionFieldsRecursive(String jsonData, Schema avroSchema) throws IOException {
        if (jsonData == null || jsonData.trim().isEmpty() || "null".equalsIgnoreCase(jsonData)) {
            return MAPPER.writeValueAsString(MAPPER.getNodeFactory().nullNode());
        }
        JsonNode rootNode = MAPPER.readTree(jsonData);
        JsonNode formattedNode = formatNodeRecursive(rootNode, avroSchema);
        return MAPPER.writeValueAsString(formattedNode);
    }

    private static JsonNode formatNodeRecursive(JsonNode currentNode, Schema currentSchema) {
        if (currentNode == null || currentNode.isNull()) {
            return MAPPER.getNodeFactory().nullNode();
        }
        Schema.Type schemaType = currentSchema.getType();
        if (schemaType == Schema.Type.UNION) {
            Schema actualTypeInUnion = null;
            if (currentNode.isObject()) {
                for (Schema type : currentSchema.getTypes()) {
                    if (type.getType() == Schema.Type.RECORD) {
                        actualTypeInUnion = type;
                        break;
                    }
                }
            }
            if (actualTypeInUnion == null && currentNode.isArray()) {
                for (Schema type : currentSchema.getTypes()) {
                    if (type.getType() == Schema.Type.ARRAY) {
                        actualTypeInUnion = type;
                        break;
                    } else if (type.getType() == Schema.Type.INT
                            && type.getLogicalType() instanceof LogicalTypes.Date) {
                        actualTypeInUnion = type;
                        break;
                    } else if (type.getType() == Schema.Type.LONG
                            && type.getLogicalType() instanceof LogicalTypes.LocalTimestampMillis) {
                        actualTypeInUnion = type;
                        break;
                    } else if (type.getType() == Schema.Type.INT
                            && type.getLogicalType() instanceof LogicalTypes.TimeMillis) {
                        actualTypeInUnion = type;
                        break;
                    }
                }
            }
            if (actualTypeInUnion == null && currentNode.isObject()) {
                for (Schema type : currentSchema.getTypes()) {
                    if (type.getType() == Schema.Type.MAP) {
                        actualTypeInUnion = type;
                        break;
                    }
                }
            }
            if (actualTypeInUnion == null) {
                for (Schema type : currentSchema.getTypes()) {
                    if (type.getType() == Schema.Type.NULL) {
                        continue;
                    }

                    switch (type.getType()) {
                        case STRING:
                            if (currentNode.isTextual()) {
                                actualTypeInUnion = type;
                                break;
                            }
                            break;
                        case INT:
                        case LONG:
                        case DOUBLE:
                        case FLOAT:
                            if (currentNode.isNumber()) {
                                actualTypeInUnion = type;
                                break;
                            }
                            break;
                        case BOOLEAN:
                            if (currentNode.isBoolean()) {
                                actualTypeInUnion = type;
                                break;
                            }
                            break;
                        case BYTES:
                        case FIXED:
                            if (currentNode.isTextual()) {
                                actualTypeInUnion = type;
                                break;
                            }
                            break;
                        case ENUM:
                            if (currentNode.isTextual() && type.getEnumSymbols().contains(currentNode.asText())) {
                                actualTypeInUnion = type;
                                break;
                            }
                            break;
                    }
                    if (actualTypeInUnion != null) {
                        break;
                    }
                }
            }

            if (actualTypeInUnion == null) {
                throw new IllegalArgumentException(
                        "Could not determine actual type for union field. "
                                + "JSON value type: " + currentNode.getNodeType()
                                + ", Avro Union Schema types: " + currentSchema.getTypes()
                                + ", Value: " + currentNode.toPrettyString());
            }

            JsonNode wrappedValue = formatNodeRecursive(currentNode, actualTypeInUnion);
            ObjectNode unionNode = MAPPER.createObjectNode();

            String typeTagName;
            if (actualTypeInUnion.getType() == Schema.Type.RECORD
                    || actualTypeInUnion.getType() == Schema.Type.ENUM
                    || actualTypeInUnion.getType() == Schema.Type.FIXED) {
                typeTagName = actualTypeInUnion.getFullName();
            } else {
                typeTagName = actualTypeInUnion.getType().getName();
            }

            unionNode.set(typeTagName, wrappedValue);
            return unionNode;
        } else if (schemaType == Schema.Type.RECORD) {
            if (!currentNode.isObject()) {
                throw new ClassCastException(
                        "Expected JSON object for Avro RECORD type, but got " + currentNode.getNodeType());
            }
            ObjectNode newObjectNode = MAPPER.createObjectNode();
            for (Schema.Field field : currentSchema.getFields()) {
                String fieldName = field.name();
                JsonNode childNode = currentNode.get(fieldName);
                newObjectNode.set(fieldName, formatNodeRecursive(childNode, field.schema()));
            }
            return newObjectNode;
        } else if (schemaType == Schema.Type.ARRAY) {
            if (!currentNode.isArray()) {
                throw new ClassCastException(
                        "Expected JSON array for Avro ARRAY type, but got " + currentNode.getNodeType());
            }
            ArrayNode newArrayNode = MAPPER.createArrayNode();
            Schema elementType = currentSchema.getElementType();
            for (JsonNode elementNode : currentNode) {
                newArrayNode.add(formatNodeRecursive(elementNode, elementType));
            }
            return newArrayNode;
        } else if (schemaType == Schema.Type.MAP) {
            if (!currentNode.isObject()) {
                throw new ClassCastException(
                        "Expected JSON object for Avro MAP type, but got " + currentNode.getNodeType());
            }
            ObjectNode newMapNode = MAPPER.createObjectNode();
            Schema valueType = currentSchema.getValueType();
            for (Map.Entry<String, JsonNode> entry :
                    (Iterable<Map.Entry<String, JsonNode>>) () -> currentNode.fields()) {
                newMapNode.set(entry.getKey(), formatNodeRecursive(entry.getValue(), valueType));
            }
            return newMapNode;
        } else if (schemaType == Schema.Type.INT && currentSchema.getLogicalType() instanceof LogicalTypes.Date) {
            if (currentNode.isArray()) {
                int year = currentNode.get(0).asInt();
                int month = currentNode.get(1).asInt();
                int day = currentNode.get(2).asInt();
                LocalDate date = LocalDate.of(year, month, day);
                long epochDay = date.toEpochDay();
                return MAPPER.getNodeFactory().numberNode((int) epochDay);
            }
        } else if (schemaType == Schema.Type.LONG
                && currentSchema.getLogicalType() instanceof LogicalTypes.LocalTimestampMillis) {
            if (currentNode.isArray()) {
                int year = currentNode.get(0).intValue();
                int month = currentNode.get(1).intValue();
                int day = currentNode.get(2).intValue();
                int hour = currentNode.get(3).intValue();
                int minute = currentNode.get(4).intValue();
                int second = currentNode.get(5).intValue();
                LocalDateTime localDateTime = LocalDateTime.of(year, month, day, hour, minute, second);
                long epochMilli = localDateTime.toInstant(ZoneOffset.UTC).toEpochMilli();
                return MAPPER.getNodeFactory().numberNode(epochMilli);
            }
        } else if (schemaType == Schema.Type.INT
                && currentSchema.getLogicalType() instanceof LogicalTypes.TimeMillis) {
            if (currentNode.isArray()) {
                int hour = currentNode.get(0).intValue();
                int minute = currentNode.get(1).intValue();
                int second = currentNode.get(2).intValue();
                LocalTime localTime = LocalTime.of(hour, minute, second);
                return MAPPER.getNodeFactory().numberNode((int) TimeUnit.NANOSECONDS.toMillis(localTime.toNanoOfDay()));
            }

        }
        return currentNode;
    }

    public static Schema convert(String jsonSchemaStr) {
        return new Schema.Parser().parse(jsonSchemaStr);
    }

}
