/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.materialization.serde.utils.json.schema;

import java.util.List;
import lombok.Builder;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;

@Slf4j
@Data
@Builder(toBuilder = true)
public class Field {
    private String name;
    private Type type;
    private String description;
    private Format format;
    private boolean required;
    private Object defaultValue;
    private Integer minimum;
    private Integer maximum;
    private Double minItems;
    private Double maxItems;
    private Boolean uniqueItems;
    private String pattern;
    private List<String> enumValues;
    private List<Field> items;
    private List<Field> properties;
    private List<String> requiredFields;
    private Boolean additionalProperties;
    private Field additionalPropertiesSchema;
    private Double multipleOf;
    private Integer minLength;
    private Integer maxLength;

    /**
     * Converts a JSON Schema field definition to an Avro Schema.
     *
     * Mapping Logic:
     * - JSON Schema primitive types (string, integer, number, boolean) map directly to Avro types
     * - JSON Schema 'object' type maps to Avro Record type with nested fields
     * - JSON Schema 'array' type maps to Avro Array type with item schema
     * - JSON Schema 'anyOf' constructs map to Avro Union types within arrays
     * - Non-required fields are wrapped in union with null type to make them optional
     *
     * Type/Format Mappings:
     * - string -> string (or specific logical types based on format)
     * - integer -> int
     * - number -> double
     * - boolean -> boolean
     * - object -> record
     * - array -> array
     *
     * Format Mappings:
     * - date -> int with logicalType "date"
     * - date-time -> long with logicalType "timestamp-micros"
     * - uuid -> string with logicalType "uuid"
     * - time -> long with logicalType "time-micros"
     * - email, uri, ipv4, ipv6, hostname -> string
     *
     * Special Handling:
     * - Non-required fields are wrapped in union with null to make them optional
     * - 'anyOf' constructs create union types for array items
     * - Avoids nested unions by extracting non-null types from unions when processing anyOf
     */
    public Schema toAvroSchema() {
        Schema fieldSchema;
        switch (this.type) {
            case STRING:
                if (this.format != null) {
                    // Handle logical types based on format
                    switch (this.format) {
                        case DATE:
                            Schema dateSchema = Schema.create(Schema.Type.INT);
                            dateSchema.addProp("logicalType", "date");
                            LogicalTypes.date().addToSchema(dateSchema);
                            fieldSchema = dateSchema;
                            break;
                        case DATE_TIME:
                            Schema dateTimeSchema = Schema.create(Schema.Type.LONG);
                            dateTimeSchema.addProp("logicalType", "timestamp-micros");
                            LogicalTypes.timestampMicros().addToSchema(dateTimeSchema);
                            dateTimeSchema.addProp("adjust-to-utc", true);
                            fieldSchema = dateTimeSchema;
                            break;
                        case UUID:
                            Schema uuidSchema = Schema.create(Schema.Type.STRING);
                            uuidSchema.addProp("logicalType", "uuid");
                            LogicalTypes.uuid().addToSchema(uuidSchema);
                            fieldSchema = uuidSchema;
                            break;
                        case TIME:
                            Schema timeSchema = Schema.create(Schema.Type.LONG);
                            timeSchema.addProp("logicalType", "time-micros");
                            LogicalTypes.timeMicros().addToSchema(timeSchema);
                            fieldSchema = timeSchema;
                            break;
                        case URI:
                        case EMAIL:
                        case IPV4:
                        case IPV6:
                        case HOSTNAME:
                        default:
                            fieldSchema = Schema.create(Schema.Type.STRING);
                            break;
                    }
                } else {
                    fieldSchema = Schema.create(Schema.Type.STRING);
                }
                break;
            case INTEGER:
                fieldSchema = Schema.create(Schema.Type.INT);
                break;
            case NUMBER:
                fieldSchema = Schema.create(Schema.Type.DOUBLE);
                break;
            case BOOLEAN:
                fieldSchema = Schema.create(Schema.Type.BOOLEAN);
                break;
            case ARRAY:
                Schema itemsSchema = Schema.create(Schema.Type.STRING); // default
                if (this.items != null && !this.items.isEmpty()) {
                    // Check if this is an anyOf case (multiple items)
                    if (this.items.size() > 1) {
                        // Create a union schema for anyOf
                        List<Schema> unionSchemas = new java.util.ArrayList<>();
                        for (Field item : this.items) {
                            // For anyOf items, we don't make them optional individually, just use their direct schema
                            // This prevents nested unions when the items themselves might be marked as non-required
                            Schema itemSchema = item.toAvroSchema();
                            // If the item schema is a union that includes null (meaning it's optional),
                            // we need to extract the non-null types to avoid nested unions in the anyOf
                            if (itemSchema.getType() == Schema.Type.UNION) {
                                List<Schema> nonNullTypes = new java.util.ArrayList<>();
                                for (Schema unionType : itemSchema.getTypes()) {
                                    if (unionType.getType() != Schema.Type.NULL) {
                                        nonNullTypes.add(unionType);
                                    }
                                }
                                if (nonNullTypes.size() == 1) {
                                    unionSchemas.add(nonNullTypes.get(0));
                                } else if (nonNullTypes.size() > 1) {
                                    unionSchemas.add(Schema.createUnion(nonNullTypes));
                                }
                            } else {
                                unionSchemas.add(itemSchema);
                            }
                        }
                        itemsSchema = Schema.createUnion(unionSchemas);
                    } else {
                        itemsSchema = this.items.get(0).toAvroSchema(); // Take the first item type
                    }
                }
                fieldSchema = Schema.createArray(itemsSchema);
                break;
            case OBJECT:
                // For nested objects with additionalProperties defined as a schema, create a map
                if (this.additionalProperties != null && this.additionalProperties
                        && this.additionalPropertiesSchema != null) {
                    // When additionalProperties is defined as a schema, the whole object becomes a map
                    Schema valueTypeSchema = this.additionalPropertiesSchema.toAvroSchema();
                    fieldSchema = Schema.createMap(valueTypeSchema);
                } else if (this.properties != null && !this.properties.isEmpty()) {
                    // For nested objects with explicit properties, create a record schema
                    Schema.Field[] nestedAvroFields = new Schema.Field[this.properties.size()];

                    for (int i = 0; i < this.properties.size(); i++) {
                        Field nestedField = this.properties.get(i);
                        Schema nestedFieldSchema = nestedField.toAvroSchema();
                        nestedAvroFields[i] = new Schema.Field(
                            nestedField.getName(),
                            nestedFieldSchema,
                            nestedField.getDescription(),
                            nestedField.getDefaultValue() != null ? nestedField.getDefaultValue() :
                                (nestedField.isRequired() ? null : null)
                        );
                    }

                    String nestedRecordName = this.name.replaceAll("[^a-zA-Z0-9_]", "_");
                    Schema nestedRecordSchema = Schema.createRecord(
                        nestedRecordName,
                        this.description,
                        null,
                        false
                    );

                    nestedRecordSchema.setFields(java.util.Arrays.asList(nestedAvroFields));
                    fieldSchema = nestedRecordSchema;
                } else {
                    fieldSchema = Schema.create(Schema.Type.RECORD);
                }
                break;
            default:
                fieldSchema = Schema.create(Schema.Type.STRING);
                break;
        }

        // If field is not required, wrap it in a union with null to make it optional
        // But avoid creating nested unions if the field schema is already a union (from anyOf)
        if (!this.required) {
            // Check if fieldSchema is already a union (e.g., from anyOf handling)
            if (fieldSchema.getType() == Schema.Type.UNION) {
                // If it's already a union, check if null is already included
                boolean hasNull = false;
                for (Schema unionMember : fieldSchema.getTypes()) {
                    if (unionMember.getType() == Schema.Type.NULL) {
                        hasNull = true;
                        break;
                    }
                }

                if (!hasNull) {
                    // Add null to the existing union
                    List<Schema> newUnionMembers = new java.util.ArrayList<>();
                    newUnionMembers.add(Schema.create(Schema.Type.NULL));
                    newUnionMembers.addAll(fieldSchema.getTypes());
                    return Schema.createUnion(newUnionMembers);
                } else {
                    // Null is already included, return as is
                    return fieldSchema;
                }
            } else {
                // Not a union, create a new union with null
                List<Schema> optionalSchemas = new java.util.ArrayList<>();
                optionalSchemas.add(Schema.create(Schema.Type.NULL));
                optionalSchemas.add(fieldSchema);
                return Schema.createUnion(optionalSchemas);
            }
        } else {
            return fieldSchema;
        }
    }
}
