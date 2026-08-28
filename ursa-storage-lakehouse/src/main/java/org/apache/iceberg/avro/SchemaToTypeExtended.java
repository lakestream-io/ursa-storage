/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package org.apache.iceberg.avro;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;

import com.google.common.collect.Lists;
import java.util.Deque;
import java.util.List;
import org.apache.avro.LogicalType;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;

public class SchemaToTypeExtended extends SchemaToType {
    private static final String METADATA = "metadata";
    private static final String VALUE = "value";

    private static final Schema METADATA_SCHEMA = Schema.create(Schema.Type.BYTES);
    private static final Schema VALUE_SCHEMA = Schema.create(Schema.Type.BYTES);

    private final Deque<String> recordLevels = Lists.newLinkedList();
    private final Deque<String> fieldNames = Lists.newLinkedList();

    public SchemaToTypeExtended(Schema root) {
        super(root);
    }

    public static Type visit(Schema schema, SchemaToTypeExtended visitor, boolean isVariantEnabled) {
        if (isVariantEnabled && isVariantType(schema)) {
            return visitor.variant(
                    schema,
                    visit(METADATA_SCHEMA, visitor),
                    visit(VALUE_SCHEMA, visitor));
        }

        switch (schema.getType()) {
            case RECORD:
                // check to make sure this hasn't been visited before
                String name = schema.getFullName();
                checkState(
                        !visitor.recordLevels.contains(name), "Cannot process recursive Avro record %s", name);

                if (isVariantEnabled && schema.getLogicalType() instanceof VariantLogicalType) {
                    checkArgument(
                            AvroSchemaUtil.isVariantSchema(schema), "Invalid variant record: %s", schema);

                    return visitor.variant(
                            schema,
                            visit(schema.getField(METADATA).schema(), visitor),
                            visit(schema.getField(VALUE).schema(), visitor));
                } else {
                    visitor.recordLevels.push(name);

                    List<Schema.Field> fields = schema.getFields();
                    List<String> names = Lists.newArrayListWithExpectedSize(fields.size());
                    List<Type> results = Lists.newArrayListWithExpectedSize(fields.size());
                    for (Schema.Field field : schema.getFields()) {
                        names.add(field.name());
                        Type result = visitWithName(field.name(), field.schema(), visitor, isVariantEnabled);
                        results.add(result);
                    }

                    visitor.recordLevels.pop();
                    return visitor.record(schema, names, results);
                }

            case UNION:
                List<Schema> types = schema.getTypes();
                List<Type> options = Lists.newArrayListWithExpectedSize(types.size());
                for (Schema type : types) {
                    options.add(visit(type, visitor, isVariantEnabled));
                }
                return visitor.union(schema, options);

            case ARRAY:
                if (schema.getLogicalType() instanceof LogicalMap) {
                    return visitor.array(schema, visit(schema.getElementType(), visitor, isVariantEnabled));
                } else {
                    return visitor.array(schema,
                            visitWithName("element", schema.getElementType(), visitor, isVariantEnabled));
                }

            case MAP:
                return visitor.map(schema, visitWithName("value", schema.getValueType(), visitor, isVariantEnabled));

            default: {
                // keep the compatibility for the null type in v2 and v3
                // https://github.com/apache/iceberg/pull/12455
                var primitiveType = visitor.primitive(schema);
                if (isVariantEnabled) {
                    return primitiveType;
                } else {
                    if (primitiveType.typeId() == Type.TypeID.UNKNOWN) {
                        return null;
                    }
                    return primitiveType;
                }
            }
        }
    }

    private static Type visitWithName(String name,
                                      Schema schema,
                                      SchemaToTypeExtended visitor,
                                      boolean isVariantEnabled) {
        try {
            visitor.fieldNames.addLast(name);
            return visit(schema, visitor, isVariantEnabled);
        } finally {
            visitor.fieldNames.removeLast();
        }
    }

    public static boolean isVariantType(Schema schema) {
        if (schema.getLogicalType() != null) {
            return "variant".equals(schema.getLogicalType().getName());
        }
        String logicalTypeProp = schema.getProp("logicalType");
        return "variant".equals(logicalTypeProp);
    }

    @Override
    public Type logicalType(Schema primitive, LogicalType logical) {
        Type t = super.logicalType(primitive, logical);
        if (t != null) {
            return t;
        }

        if (logical instanceof LogicalTypes.LocalTimestampMillis
                || logical instanceof LogicalTypes.LocalTimestampMicros) {
            return Types.TimestampType.withoutZone();
        }

        return null;
    }

    @Override
    public Type primitive(Schema primitive) {
        // ---------- BigDecimal ----------
        String javaClassStr = primitive.getProp("java-class");
        if (javaClassStr != null && javaClassStr.contains("BigDecimal")) {
            return Types.DecimalType.of(10, 2);
        }

        return super.primitive(primitive);
    }

}
