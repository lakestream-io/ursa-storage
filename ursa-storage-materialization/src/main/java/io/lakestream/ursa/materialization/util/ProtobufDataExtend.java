/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.materialization.util;

import com.google.protobuf.Descriptors;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.JsonProperties;
import org.apache.avro.LogicalType;
import org.apache.avro.Schema;
import org.apache.avro.protobuf.ProtobufData;

/**
 * Optimized Protobuf-to-Avro converter for Lakehouse (Iceberg/Delta) storage.
 * * Logic:
 * 1. Scalars (int, string, etc.) -> Optional ["null", "type"]
 * 2. Repeated fields -> Required Array (defaults to [])
 * 3. Map entries -> Required Key/Value (standard Map integrity)
 * 4. Custom logical types -> Applied to the inner type of Unions
 */
@Slf4j
public class ProtobufDataExtend extends ProtobufData {
    private static final ProtobufDataExtend INSTANCE = new ProtobufDataExtend();
    private static final Schema NULL = Schema.create(Schema.Type.NULL);

    protected ProtobufDataExtend() {}

    public static ProtobufDataExtend get() {
        return INSTANCE;
    }

    @Override
    public Schema getSchema(Descriptors.Descriptor descriptor) {
        try {
            return super.getSchema(descriptor);
        } catch (org.apache.avro.AvroTypeException e) {
            return reconstructRecord(descriptor);
        }
    }

    private Schema reconstructRecord(Descriptors.Descriptor descriptor) {
        Schema result = Schema.createRecord(descriptor.getName(), null,
                getNamespace(descriptor.getFile(), descriptor.getContainingType()), false);
        List<Schema.Field> fields = new ArrayList<>();

        for (Descriptors.FieldDescriptor f : descriptor.getFields()) {
            Schema fieldSchema = getSchema(f);
            // Since all top-level fields (scalars and repeated) are now Unions starting with NULL,
            // the default must be JsonProperties.NULL_VALUE.
            Object defaultValue = JsonProperties.NULL_VALUE;

            fields.add(new Schema.Field(f.getName(), fieldSchema, null, defaultValue));
        }
        result.setFields(fields);
        return result;
    }

    @Override
    public Schema getSchema(Descriptors.FieldDescriptor f) {
        boolean isMapEntryField = f.getContainingType().getOptions().getMapEntry();

        Schema s = getNonRepeatedSchemaExtend(f, isMapEntryField);

        if (f.isRepeated()) {
            s = Schema.createUnion(Arrays.asList(NULL, Schema.createArray(s)));
        }

        if (hasVariantLogicalType(f)) {
            Schema targetSchema = s;
            if (s.getType() == Schema.Type.UNION) {
                for (Schema subSchema : s.getTypes()) {
                    if (subSchema.getType() != Schema.Type.NULL) {
                        targetSchema = subSchema;
                        break;
                    }
                }
            }
            new LogicalType("variant").addToSchema(targetSchema);
        }
        return s;
    }

    private Schema getNonRepeatedSchemaExtend(Descriptors.FieldDescriptor f, boolean isMapEntryField) {
        Schema baseSchema;
        switch (f.getType()) {
            case BOOL:   baseSchema = Schema.create(Schema.Type.BOOLEAN); break;
            case FLOAT:  baseSchema = Schema.create(Schema.Type.FLOAT); break;
            case DOUBLE: baseSchema = Schema.create(Schema.Type.DOUBLE); break;
            case STRING: baseSchema = Schema.create(Schema.Type.STRING); break;
            case BYTES:  baseSchema = Schema.create(Schema.Type.BYTES); break;
            case INT32:
            case SINT32:
            case FIXED32:
            case SFIXED32:
                baseSchema = Schema.create(Schema.Type.INT); break;
            case UINT32:
            case INT64:
            case UINT64:
            case SINT64:
            case FIXED64:
            case SFIXED64:
                baseSchema = Schema.create(Schema.Type.LONG); break;
            case ENUM:
                baseSchema = getSchema(f.getEnumType()); break;
            case MESSAGE:
                baseSchema = getSchema(f.getMessageType());
                if (f.isMapField()) {
                    return baseSchema;
                }
                if (f.isOptional()) {
                    return Schema.createUnion(Arrays.asList(NULL, baseSchema));
                }
                return baseSchema;
            default:
                throw new RuntimeException("Unexpected type: " + f.getType());
        }

        // Wrap scalars in Union [null, T] if not in a MapEntry
        if (!isMapEntryField && !f.isRepeated()) {
            return Schema.createUnion(Arrays.asList(NULL, baseSchema));
        }

        return baseSchema;
    }

    private boolean hasVariantLogicalType(Descriptors.FieldDescriptor f) {
        // 1. Check registered extensions (from your debug image)
        Map<Descriptors.FieldDescriptor, Object> allFields = f.getOptions().getAllFields();
        for (Map.Entry<Descriptors.FieldDescriptor, Object> entry : allFields.entrySet()) {
            if (entry.getKey().getName().equals("logical_type") && "variant".equals(entry.getValue())) {
                return true;
            }
        }

        // 2. Check unknown fields
        return f.getOptions().getUnknownFields().asMap().values().stream()
                .anyMatch(uf -> uf.getLengthDelimitedList().stream()
                        .anyMatch(bd -> "variant".equals(bd.toStringUtf8())));
    }
}