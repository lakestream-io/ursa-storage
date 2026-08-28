/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.materialization.serde.utils.json.schema;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;

@Slf4j
@Getter
public class JsonSchema {
    private String rawSchema;
    private Type type;
    private String title;
    private String description;
    private LinkedList<Field> fields;
    private Boolean additionalProperties;
    private ObjectMapper objectMapper;

    public static JsonSchema of(String rawSchema) {
        return new JsonSchema(rawSchema);
    }

    public JsonSchema(String rawSchema) {
        this.rawSchema = rawSchema;
        this.objectMapper = new ObjectMapper();
        try {
            JsonNode schemaNode = objectMapper.readTree(rawSchema);
            parseSchema(schemaNode);
        } catch (IOException e) {
            throw new IllegalArgumentException("Error parsing JSON schema", e);
        }
    }

    private void parseSchema(JsonNode schemaNode) {
        // Parse top-level properties
        if (schemaNode.has("type")) {
            this.type = Type.fromValue(schemaNode.get("type").asText());
        }

        if (schemaNode.has("title")) {
            this.title = schemaNode.get("title").asText();
        }

        if (schemaNode.has("description")) {
            this.description = schemaNode.get("description").asText();
        }

        List<String> requiredFields = new ArrayList<>();
        if (schemaNode.has("required")) {
            JsonNode requiredNode = schemaNode.get("required");
            if (requiredNode.isArray()) {
                for (JsonNode reqNode : requiredNode) {
                    requiredFields.add(reqNode.asText());
                }
            }
        }

        if (schemaNode.has("additionalProperties")) {
            this.additionalProperties = schemaNode.get("additionalProperties").asBoolean();
        }

        JsonNode jsonNode = schemaNode.get("definitions");

        // Parse fields/properties
        if (schemaNode.has("properties")) {
            this.fields = parseProperties(schemaNode.get("properties"), jsonNode, requiredFields);
        }
    }

    private LinkedList<Field> parseProperties(JsonNode propertiesNode,
                                              JsonNode definitionsNode, List<String> requiredFields) {
        LinkedList<Field> fields = new LinkedList<>();

        Iterator<String> fieldNames = propertiesNode.fieldNames();
        while (fieldNames.hasNext()) {
            String fieldName = fieldNames.next();
            JsonNode fieldNode = propertiesNode.get(fieldName);

            Field field = parseField(fieldName, fieldNode, definitionsNode);
            if (requiredFields != null && requiredFields.contains(fieldName)) {
                field = field.toBuilder().required(true).build();
            }

            fields.add(field);
        }

        return fields;
    }

    private Field parseField(String name, JsonNode fieldNode, JsonNode definitionsNode) {
        Field.FieldBuilder builder = Field.builder()
            .name(name);
        if (fieldNode.has("oneOf")) {
            ArrayNode arrayNode = (ArrayNode) fieldNode.get("oneOf");
            for (int i = 0; i < arrayNode.size(); i++) {
                JsonNode elementNode = arrayNode.get(i);
                if (elementNode.has("type")) {
                    String typeStr = elementNode.get("type").asText();
                    if (!"null".equals(typeStr)) {
                        fieldNode = elementNode;
                        break;
                    }
                } else if (elementNode.has("$ref")) {
                    fieldNode = elementNode;
                    break;
                }
            }
        }

        if (fieldNode.has("$ref")) {
            String refPath = fieldNode.get("$ref").asText();
            String[] pathParts = refPath.split("/");
            String refName = pathParts[pathParts.length - 1];
            JsonNode referencedSchema = definitionsNode.get(refName);
            if (referencedSchema == null) {
                throw new IllegalArgumentException("Referenced schema not found: " + refPath);
            }
            Field referencedField = parseField(name, referencedSchema, definitionsNode);
            builder = referencedField.toBuilder();

            // Preserve any description from the reference node
            if (fieldNode.has("description")) {
                builder.description(fieldNode.get("description").asText());
            }

        } else {
            if (fieldNode.has("type")) {
                String typeStr = fieldNode.get("type").asText();
                try {
                    builder.type(Type.fromValue(typeStr));
                } catch (IllegalArgumentException e) {
                    // If it's not a basic type, we might need to handle complex types differently
                    // For now, we'll ignore invalid types or handle them as needed
                }
            } else if (fieldNode.has("const")) {
                // If there's a const keyword but no type, treat it as a string type
                builder.type(Type.STRING);
            }

            if (fieldNode.has("description")) {
                builder.description(fieldNode.get("description").asText());
            }

            if (fieldNode.has("format")) {
                String formatStr = fieldNode.get("format").asText();
                Format format = Format.fromValue(formatStr);
                if (format != null) {
                    builder.format(format);
                }
            }

            if (fieldNode.has("default")) {
                builder.defaultValue(fieldNode.get("default"));
            }

            if (fieldNode.has("minimum")) {
                builder.minimum(fieldNode.get("minimum").asInt());
            }

            if (fieldNode.has("maximum")) {
                builder.maximum(fieldNode.get("maximum").asInt());
            }

            if (fieldNode.has("minItems")) {
                builder.minItems(fieldNode.get("minItems").asDouble());
            }

            if (fieldNode.has("maxItems")) {
                builder.maxItems(fieldNode.get("maxItems").asDouble());
            }

            if (fieldNode.has("uniqueItems")) {
                builder.uniqueItems(fieldNode.get("uniqueItems").asBoolean());
            }

            if (fieldNode.has("pattern")) {
                builder.pattern(fieldNode.get("pattern").asText());
            }

            if (fieldNode.has("enum")) {
                List<String> enumList = new ArrayList<>();
                JsonNode enumNode = fieldNode.get("enum");
                if (enumNode.isArray()) {
                    for (JsonNode enumValue : enumNode) {
                        enumList.add(enumValue.asText());
                    }
                }
                builder.enumValues(enumList);
            }

            if (fieldNode.has("items")) {
                JsonNode itemsNode = fieldNode.get("items");
                List<Field> itemsList = new ArrayList<>();

                // Handle anyOf in items
                if (itemsNode.has("anyOf") && itemsNode.get("anyOf").isArray()) {
                    JsonNode anyOfNode = itemsNode.get("anyOf");
                    for (JsonNode anyOfItem : anyOfNode) {
                        itemsList.add(parseField("anyOfItem", anyOfItem, definitionsNode));
                    }
                } else {
                    // For simplicity, assuming items is a single object, not an array
                    itemsList.add(parseField("item", itemsNode, definitionsNode)); // Using "item" as placeholder name
                }
                builder.items(itemsList);
            }

            if (fieldNode.has("properties")) {
                List<String> requiredFields = new ArrayList<>();
                if (fieldNode.has("required")) {
                    JsonNode requiredNode = fieldNode.get("required");
                    if (requiredNode.isArray()) {
                        for (JsonNode reqNode : requiredNode) {
                            requiredFields.add(reqNode.asText());
                        }
                    }
                }


                builder.properties(parseProperties(fieldNode.get("properties"), definitionsNode, requiredFields));
            }

            if (fieldNode.has("required")) {
                List<String> reqFields = new ArrayList<>();
                JsonNode requiredNode = fieldNode.get("required");
                if (requiredNode.isArray()) {
                    for (JsonNode reqNode : requiredNode) {
                        reqFields.add(reqNode.asText());
                    }
                }
                builder.requiredFields(reqFields);
            }

            if (fieldNode.has("additionalProperties")) {
                JsonNode additionalPropsNode = fieldNode.get("additionalProperties");
                if (additionalPropsNode.isBoolean()) {
                    // If additionalProperties is a boolean, set it directly
                    builder.additionalProperties(additionalPropsNode.asBoolean());
                } else if (additionalPropsNode.isObject()) {
                    // If additionalProperties is an object, parse it as a schema
                    builder.additionalProperties(true)
                        .additionalPropertiesSchema(parseField("additionalProperty",
                            additionalPropsNode, definitionsNode));
                }
            }

            if (fieldNode.has("multipleOf")) {
                builder.multipleOf(fieldNode.get("multipleOf").asDouble());
            }

            if (fieldNode.has("minLength")) {
                builder.minLength(fieldNode.get("minLength").asInt());
            }

            if (fieldNode.has("maxLength")) {
                builder.maxLength(fieldNode.get("maxLength").asInt());
            }
        }
        return builder.build();
    }

    public Schema toAvroSchema() {
        Schema.Field[] avroFields = new Schema.Field[fields.size()];

        for (int i = 0; i < fields.size(); i++) {
            Field field = fields.get(i);
            // Use the field's own toAvroSchema method
            Schema fieldSchema = field.toAvroSchema();
            // Create Avro field with name, type, and doc
            avroFields[i] = new Schema.Field(
                field.getName(),
                fieldSchema,
                field.getDescription(),
                field.getDefaultValue() != null ? field.getDefaultValue() :
                    (field.isRequired() ? null : null)
            );
        }

        String recordName = this.title != null ? this.title.replaceAll("[^a-zA-Z0-9_]", "") : "JsonSchemaRecord";
        Schema recordSchema = Schema.createRecord(
            recordName,
            this.description,
            null, // namespace
            false
        );

        recordSchema.setFields(Arrays.asList(avroFields));
        return recordSchema;
    }
}
