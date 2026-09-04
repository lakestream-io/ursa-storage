/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.materialization.util;

import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import io.lakestream.ursa.materialization.serde.kafka.schema.ProtobufSchemaDescriptors;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;

@Slf4j
public class PBRecordReader {

    private final Schema schema;

    public PBRecordReader(Schema schema) {
        this.schema = schema;
    }

    public GenericRecord read(Object dynamicMessage) {
        if (!(dynamicMessage instanceof DynamicMessage dm)) {
            throw new IllegalArgumentException("The input message is not a DynamicMessage");
        }
        return convertProtobufToAvroRecord(dm, this.schema, dm.getDescriptorForType());
    }

    public GenericRecord convertProtobufToAvroRecord(DynamicMessage protobufMessage,
                                                     Schema avroSchema,
                                                     Descriptors.Descriptor descriptor) {

        Schema actualRecordSchema = getNonNullSchema(avroSchema);
        GenericRecord avroRecord = new GenericData.Record(actualRecordSchema);

        for (Descriptors.FieldDescriptor field : descriptor.getFields()) {
            Schema.Field avroField = actualRecordSchema.getField(field.getName());
            if (avroField == null) {
                continue;
            }

            Object protobufValue = protobufMessage.getField(field);

            // Handle nulls
            if (protobufValue == null) {
                avroRecord.put(field.getName(), null);
                continue;
            }

            Object avroValue = convertProtobufValueToAvroValue(protobufValue, field, avroField.schema());
            avroRecord.put(field.getName(), avroValue);
        }

        return avroRecord;
    }

    public Object convertProtobufValueToAvroValue(Object protobufValue,
                                                  Descriptors.FieldDescriptor field,
                                                  Schema avroSchema) {

        Schema actualSchema = getNonNullSchema(avroSchema);

        // Map and Repeated handling
        if (field.isRepeated()) {
            List<?> pbList = (List<?>) protobufValue;

            // If Avro expects an ARRAY (Iceberg list<struct>), wrap entries in a List
            if (actualSchema.getType() == Schema.Type.ARRAY) {
                Schema elementSchema = actualSchema.getElementType();
                return pbList.stream()
                        .map(item -> convertSingleValue(item, field, elementSchema))
                        .collect(Collectors.toCollection(ArrayList::new));
            }

            // Fallback: If PB is repeated but Avro expects a single Record (struct)
            // This happens if the schema converter didn't produce an Array wrapper.
            if (actualSchema.getType() == Schema.Type.RECORD && !pbList.isEmpty()) {
                return convertSingleValue(pbList.get(0), field, actualSchema);
            }
        }

        return convertSingleValue(protobufValue, field, actualSchema);
    }

    private Object convertSingleValue(Object value, Descriptors.FieldDescriptor field, Schema schema) {
        if (value == null) {
            return null;
        }
        Schema actualSchema = getNonNullSchema(schema);

        switch (field.getType()) {
            case MESSAGE:
                return convertProtobufToAvroRecord((DynamicMessage) value, actualSchema, field.getMessageType());
            case INT32: case SINT32: case FIXED32: case SFIXED32:
                return ((Number) value).intValue();
            case UINT32: case INT64: case UINT64: case SINT64: case FIXED64: case SFIXED64:
                return ((Number) value).longValue();
            case FLOAT: return ((Number) value).floatValue();
            case DOUBLE: return ((Number) value).doubleValue();
            case BOOL: return (Boolean) value;
            case STRING: return value.toString();
            case BYTES: return ByteBuffer.wrap(((com.google.protobuf.ByteString) value).toByteArray());
            case ENUM: return ((Descriptors.EnumValueDescriptor) value).getName();
            default: return value;
        }
    }

    private Schema getNonNullSchema(Schema schema) {
        if (schema.getType() == Schema.Type.UNION) {
            return schema.getTypes().stream()
                .filter(s -> s.getType() != Schema.Type.NULL)
                .findFirst()
                .orElse(schema);
        }
        return schema;
    }

    public static Schema makeMapEntriesNullable(Schema schema) {
        if (schema.getType() != Schema.Type.RECORD) {
            return schema;
        }

        List<Schema.Field> newFields = new ArrayList<>();
        for (Schema.Field field : schema.getFields()) {
            Schema fieldSchema = field.schema();
            Schema newFieldSchema = processFieldSchema(fieldSchema);
            newFields.add(new Schema.Field(field.name(), newFieldSchema, field.doc(), field.defaultVal()));
        }

        return Schema.createRecord(schema.getName(), schema.getDoc(),
                schema.getNamespace() != null ? schema.getNamespace().replace('$', '.') : null,
                schema.isError(), newFields);
    }

    private static Schema processFieldSchema(Schema schema) {
        if (schema.getType() == Schema.Type.ARRAY) {
            return Schema.createArray(processFieldSchema(schema.getElementType()));
        } else if (schema.getType() == Schema.Type.RECORD) {
            // Check if this is a MapEntry record
            if (schema.getName().endsWith("Entry")) {
                return createNullableMapEntrySchema(schema);
            }
            return makeMapEntriesNullable(schema);
        } else if (schema.getType() == Schema.Type.UNION) {
            List<Schema> types = schema.getTypes().stream()
                    .map(PBRecordReader::processFieldSchema)
                    .collect(Collectors.toList());
            return Schema.createUnion(types);
        }
        return schema;
    }

    private static Schema createNullableMapEntrySchema(Schema mapEntrySchema) {
        List<Schema.Field> entryFields = new ArrayList<>();
        for (Schema.Field field : mapEntrySchema.getFields()) {
            // Wrap key/value in Union [null, type] to make them optional
            Schema nullableType = Schema.createUnion(Schema.create(Schema.Type.NULL), field.schema());
            entryFields.add(new Schema.Field(field.name(), nullableType, field.doc(), null));
        }
        return Schema.createRecord(mapEntrySchema.getName(), mapEntrySchema.getDoc(),
                mapEntrySchema.getNamespace() != null ? mapEntrySchema.getNamespace().replace('$', '.') : null,
                mapEntrySchema.isError(), entryFields);
    }

    public static Schema convertPbSchemaToAvro(String schemas, List<Integer> messageIndexes) {
        var descriptor = ProtobufSchemaDescriptors.messageByIndexes(schemas, messageIndexes);
        // Use ProtobufDataExtend to handle uint32 to long conversion
        // The avro default implementation is to map uint32 to int, which is not compatible with Lakehouse
        var schema = ProtobufDataExtend.get().getSchema(descriptor);
        schema = makeMapEntriesNullable(schema);
        return schema;
    }

    /**
     * Resolve the fully qualified protobuf message name from a schema string and message indexes.
     * The message name is stable across schema versions even if the serializer reorders message
     * definitions, making it safe to use for schema evolution across versions.
     */
    public static String resolveMessageName(String schemaStr, List<Integer> messageIndexes) {
        return ProtobufSchemaDescriptors.messageNameByIndexes(schemaStr, messageIndexes);
    }

    /**
     * Resolve the protobuf message name from schema metadata and message indexes.
     * Returns Optional.empty() for non-protobuf schemas or when messageIndexes are absent.
     */
    public static Optional<String> resolveProtobufMessageName(String schemaType, String schemaStr,
                                                              Optional<List<Integer>> messageIndexes) {
        if ("PROTOBUF".equalsIgnoreCase(schemaType) && messageIndexes.isPresent()) {
            return Optional.of(resolveMessageName(schemaStr, messageIndexes.get()));
        }
        return Optional.empty();
    }

    /**
     * Convert a protobuf schema to Avro using the fully qualified message name instead of
     * positional message indexes. This is used during schema evolution where the message name
     * is stable across versions but positional indexes may differ due to serializer reordering.
     */
    public static Schema convertPbSchemaToAvroByName(String schemaStr, String messageName) {
        var descriptor = ProtobufSchemaDescriptors.messageByName(schemaStr, messageName);
        var schema = ProtobufDataExtend.get().getSchema(descriptor);
        return makeMapEntriesNullable(schema);
    }
}
