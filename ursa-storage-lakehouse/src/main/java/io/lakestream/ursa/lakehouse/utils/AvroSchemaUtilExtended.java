/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.lakehouse.utils;

import static org.apache.avro.Schema.Type.ARRAY;
import static org.apache.avro.Schema.Type.MAP;
import static org.apache.avro.Schema.Type.RECORD;

import com.google.common.annotations.VisibleForTesting;
import io.delta.kernel.types.ArrayType;
import io.delta.kernel.types.BinaryType;
import io.delta.kernel.types.BooleanType;
import io.delta.kernel.types.ByteType;
import io.delta.kernel.types.DataType;
import io.delta.kernel.types.DateType;
import io.delta.kernel.types.DecimalType;
import io.delta.kernel.types.DoubleType;
import io.delta.kernel.types.FieldMetadata;
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
import io.lakestream.ursa.exception.BadSchemaException;
import io.lakestream.ursa.exception.ExceptionCode;
import io.lakestream.ursa.exception.RuntimeExceptionWithCode;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.AvroRuntimeException;
import org.apache.avro.LogicalType;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.commons.lang3.StringUtils;
import org.apache.iceberg.avro.AvroSchemaUtil;
import org.apache.iceberg.avro.SchemaToTypeExtended;
import org.apache.iceberg.avro.UnityCatalogSchemaToType;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;

@Slf4j

public class AvroSchemaUtilExtended {
    private static final ThreadLocal<Set<String>> visitedFields = ThreadLocal.withInitial(HashSet::new);

    public static StructType toDelta(Schema avroSchema, boolean isVariantEnabled) {
        log.info("Avro schema: {}, {}", avroSchema, avroSchema.isNullable());
        StructField field = oneAvroFieldToDeltaField(avroSchema.getName(), avroSchema, false, isVariantEnabled);
        if (avroSchema.getType() != Schema.Type.RECORD) {
            throw new UnsupportedOperationException();
        }
        return (StructType) field.getDataType();
    }

    protected static StructField oneAvroFieldToDeltaField(String name, Schema avroSchema, boolean nullable) {
        return oneAvroFieldToDeltaField(name, avroSchema, nullable, true);
    }

    protected static StructField oneAvroFieldToDeltaField(
            String name, Schema avroSchema, boolean nullable, boolean isVariantEnabled) {
        try {
            return oneAvroFieldToDeltaFieldInternal(name, avroSchema, nullable, isVariantEnabled);
        } finally {
            visitedFields.remove();
        }
    }

    public static boolean isVariantType(Schema schema) {
        if (schema.getLogicalType() != null) {
            return "variant".equals(schema.getLogicalType().getName());
        }
        String logicalTypeProp = schema.getProp("logicalType");
        return "variant".equals(logicalTypeProp);
    }

    public static StructField oneAvroFieldToDeltaFieldInternal(
            String name, Schema avroSchema, boolean nullable, boolean isVariantEnabled) {
        if (isVariantEnabled && isVariantType(avroSchema)) {
            return new StructField(name, VariantType.VARIANT, nullable);
        }
        String fullName = avroSchema.getFullName();
        boolean recordAddedToPath = false;
        // Only check for recursion if it's a RECORD type
        if (fullName != null && avroSchema.getType() == Schema.Type.RECORD) {
            if (!visitedFields.get().add(fullName)) {
                throw new IllegalStateException(
                    String.format("Cannot handle recursive Avro schemas: field '%s' is recursive", fullName));
            }
            recordAddedToPath = true;
        }
        try {
            StructField newField = null;
            switch (avroSchema.getType()) {
                case RECORD:
                    List<StructField> fields = new ArrayList<>();
                    avroSchema.getFields().forEach(field -> {
                        fields.add(oneAvroFieldToDeltaFieldInternal(
                                field.name(), field.schema(), false, isVariantEnabled));
                    });
                    newField = new StructField(name, new StructType(fields), nullable);
                    break;
                case MAP:
                    Schema valueSchema = avroSchema.getValueType();
                    StructField deltaValueField = oneAvroFieldToDeltaField(
                            valueSchema.getName(), valueSchema, false, isVariantEnabled);
                    MapType mapType = new MapType(StringType.STRING, deltaValueField.getDataType(), true);
                    newField = new StructField(name, mapType, nullable);
                    break;
                case ARRAY:
                    Schema itemSchema = avroSchema.getElementType();
                    StructField deltaItemField = oneAvroFieldToDeltaField(
                            itemSchema.getName(), itemSchema, false, isVariantEnabled);
                    if (deltaItemField.getDataType() instanceof StructType) {
                        deltaItemField = convertToNullableStructType(deltaItemField);
                    }
                    ArrayType arrayType = new ArrayType(deltaItemField.getDataType(), true);
                    newField = new StructField(name, arrayType, nullable);
                    break;
                case UNION:
                    List<Schema> types = avroSchema.getTypes();
                    boolean isNullable = types.stream().anyMatch(Schema::isNullable);
                    for (Schema schema : types) {
                        if (!schema.isNullable()) {
                            if (schema.getType() == RECORD || schema.getType() == ARRAY || schema.getType() == MAP) {
                                newField = oneAvroFieldToDeltaField(name, schema, isNullable, isVariantEnabled);
                            } else {
                                newField = new StructField(
                                        name, schemaTypeToDeltaType(schema, isVariantEnabled), isNullable);
                            }
                            break;
                        }
                    }
                    break;
                default:
                    newField = new StructField(
                            name, schemaTypeToDeltaType(avroSchema, isVariantEnabled), avroSchema.isNullable());
                    break;
            }
            return newField;
        } finally {
            if (recordAddedToPath) {
                visitedFields.get().remove(fullName);
            }
        }
    }
    protected static StructField convertToNullableStructType(StructField structField) {
        String name = structField.getName();
        FieldMetadata metadata = structField.getMetadata();
        DataType dataType = structField.getDataType();
        List<StructField> fields = ((StructType) dataType).fields();
        List<StructField> newFields = new ArrayList<>(fields.size());
        for (StructField field : fields) {
            if (field.getDataType() instanceof StructType) {
                newFields.add(convertToNullableStructType(field));
            } else {
                newFields.add(new StructField(field.getName(), field.getDataType(), true, field.getMetadata()));
            }
        }
        return new StructField(name, new StructType(newFields), true, metadata);
    }

    public static DataType schemaTypeToDeltaType(Schema schema, boolean isVariantEnabled) {
        LogicalType logicType = schema.getLogicalType();
        if (logicType != null) {
            String name = logicType.getName();
            if ("variant".equals(name) && isVariantEnabled) {
                return VariantType.VARIANT;
            }
            if (logicType instanceof LogicalTypes.Decimal) {
                return new DecimalType(((LogicalTypes.Decimal) logicType).getPrecision(),
                        ((LogicalTypes.Decimal) logicType).getScale());
            } else if (logicType instanceof LogicalTypes.Date) {
                return DateType.DATE;
            } else if (
                    logicType instanceof LogicalTypes.TimeMillis
                            || logicType instanceof LogicalTypes.TimeMicros) {
                return TimestampNTZType.TIMESTAMP_NTZ;
            } else if (
                    logicType instanceof LogicalTypes.TimestampMillis
                            || logicType instanceof LogicalTypes.TimestampMicros) {
                if (AvroSchemaUtil.isTimestamptz(schema)) {
                    return TimestampType.TIMESTAMP;
                } else {
                    return TimestampNTZType.TIMESTAMP_NTZ;
                }
            } else if (logicType instanceof LogicalTypes.LocalTimestampMillis
                    || logicType instanceof LogicalTypes.LocalTimestampMicros) {
                return TimestampNTZType.TIMESTAMP_NTZ;
            } else if (LogicalTypes.uuid().getName().equals(name)) {
                return StringType.STRING;
            }
        }

        if (isVariantEnabled && "variant".equals(schema.getProp("logicalType"))) {
            return VariantType.VARIANT;
        }
        DataType type;
        switch (schema.getType()) {
            case STRING, ENUM:
                type = StringType.STRING;
                break;
            case FIXED:
            case BYTES:
                type = BinaryType.BINARY;
                break;
            case INT:
                String javaClass = schema.getProp("java-class");
                if (StringUtils.isBlank(javaClass)) {
                    type = IntegerType.INTEGER;
                } else if (Byte.class.getName().equals(javaClass)) {
                    type = ByteType.BYTE;
                }  else if (Short.class.getName().equals(javaClass)) {
                    type = ShortType.SHORT;
                } else {
                    type = IntegerType.INTEGER;
                }
                break;
            case LONG:
                type = LongType.LONG;
                break;
            case FLOAT:
                type = FloatType.FLOAT;
                break;
            case DOUBLE:
                type = DoubleType.DOUBLE;
                break;
            case BOOLEAN:
                type = BooleanType.BOOLEAN;
                break;
            default:
                Schema.Type filedType = schema.getType();
                String fieldName = schema.getName();
                log.error("not support schema type {} for field: {}", filedType, fieldName);
                throw new UnsupportedOperationException(
                        "Not support schema type " + filedType + " for filed: " + fieldName);
        }
        return type;
    }

    public static Schema deltaSchemaToAvroSchema(String name, StructType structType, boolean isNullable) {
        log.info("Delta table schema: {}, {}", name, structType);
        List<Schema.Field> schemaFields = new ArrayList<>();
        List<StructField> fields = structType.fields();

        for (StructField field : fields) {
            Schema subSchema = oneDeltaFieldToAvroField(field);
            schemaFields.add(field.isNullable()
                    ? new Schema.Field(field.getName(), subSchema, "", Schema.Field.NULL_DEFAULT_VALUE) :
                    new Schema.Field(field.getName(), subSchema));
        }
        return isNullable ? Schema.createUnion(Schema.create(Schema.Type.NULL),
                Schema.createRecord(name, "", "", false, schemaFields)) :
                Schema.createRecord(name, "", "", false, schemaFields);
    }

    protected static Schema oneDeltaFieldToAvroField(StructField structField) {
        Schema newField = null;
        DataType dataType = structField.getDataType();
        if (dataType instanceof StructType) {
            newField = deltaSchemaToAvroSchema(structField.getName(), (StructType) structField.getDataType(),
                    true);
        } else if (dataType instanceof MapType) {
            MapType mapType = (MapType) structField.getDataType();
            DataType valueType = mapType.getValueType();
            Schema valueSchema = deltaTypeToSchemaType(valueType, false);
            newField = structField.isNullable()
                    ? Schema.createUnion(Schema.create(Schema.Type.NULL), Schema.createMap(valueSchema)) :
                    Schema.createMap(valueSchema);
        } else if (dataType instanceof ArrayType) {
            ArrayType arrayType = (ArrayType) structField.getDataType();
            DataType elementType = arrayType.getElementType();
            Schema elementSchema = deltaTypeToSchemaType(elementType, false);
            newField = structField.isNullable()
                    ? Schema.createUnion(Schema.create(Schema.Type.NULL), Schema.createArray(elementSchema)) :
                    Schema.createArray(elementSchema);
        } else if (dataType instanceof StringType) {
            newField = deltaTypeToSchemaType(structField.getDataType(), structField.isNullable());
        } else if (dataType instanceof BinaryType) {
            newField = deltaTypeToSchemaType(structField.getDataType(), structField.isNullable());
        } else if (dataType instanceof ShortType) {
            newField = deltaTypeToSchemaType(structField.getDataType(), structField.isNullable());
        } else if (dataType instanceof ByteType) {
            newField = deltaTypeToSchemaType(structField.getDataType(), structField.isNullable());
        } else if (dataType instanceof IntegerType) {
            newField = deltaTypeToSchemaType(structField.getDataType(), structField.isNullable());
        } else if (dataType instanceof LongType) {
            newField = deltaTypeToSchemaType(structField.getDataType(), structField.isNullable());
        } else if (dataType instanceof FloatType) {
            newField = deltaTypeToSchemaType(structField.getDataType(), structField.isNullable());
        } else if (dataType instanceof DoubleType) {
            newField = deltaTypeToSchemaType(structField.getDataType(), structField.isNullable());
        } else if (dataType instanceof BooleanType) {
            newField = deltaTypeToSchemaType(structField.getDataType(), structField.isNullable());
        } else {
            log.error("not support schema type {} in convert ", dataType.getClass().getName());
        }
        return newField;
    }

    private static Schema deltaTypeToSchemaType(DataType dataType, boolean isNullable) {
        if (dataType instanceof StructType) {
            return deltaSchemaToAvroSchema("Record", (StructType) dataType, isNullable);
        } else if (dataType instanceof StringType) {
            return isNullable
                    ? Schema.createUnion(Schema.create(Schema.Type.NULL), Schema.create(Schema.Type.STRING)) :
                    Schema.create(Schema.Type.STRING);
        } else if (dataType instanceof BinaryType) {
            return isNullable
                    ? Schema.createUnion(Schema.create(Schema.Type.NULL), Schema.create(Schema.Type.BYTES)) :
                    Schema.create(Schema.Type.BYTES);
        } else if (dataType instanceof ByteType) {
            Schema schema = Schema.create(Schema.Type.INT);
            schema.addProp("java-class", Byte.class.getName());
            return isNullable ? Schema.createUnion(Schema.create(Schema.Type.NULL), schema) : schema;
        } else if (dataType instanceof ShortType) {
            Schema schema = Schema.create(Schema.Type.INT);
            schema.addProp("java-class", Short.class.getName());
            return isNullable ? Schema.createUnion(Schema.create(Schema.Type.NULL), schema) : schema;
        } else if (dataType instanceof IntegerType) {
            return isNullable
                    ? Schema.createUnion(Schema.create(Schema.Type.NULL), Schema.create(Schema.Type.INT)) :
                    Schema.create(Schema.Type.INT);
        } else if (dataType instanceof LongType) {
            return isNullable
                    ? Schema.createUnion(Schema.create(Schema.Type.NULL), Schema.create(Schema.Type.LONG)) :
                    Schema.create(Schema.Type.LONG);
        } else if (dataType instanceof FloatType) {
            return isNullable
                    ? Schema.createUnion(Schema.create(Schema.Type.NULL), Schema.create(Schema.Type.FLOAT)) :
                    Schema.create(Schema.Type.FLOAT);
        } else if (dataType instanceof DoubleType) {
            return isNullable
                    ? Schema.createUnion(Schema.create(Schema.Type.NULL), Schema.create(Schema.Type.DOUBLE)) :
                    Schema.create(Schema.Type.DOUBLE);
        } else if (dataType instanceof BooleanType) {
            return isNullable
                    ? Schema.createUnion(Schema.create(Schema.Type.NULL), Schema.create(Schema.Type.BOOLEAN)) :
                    Schema.create(Schema.Type.BOOLEAN);
        } else {
            throw new AvroRuntimeException("Can't create a: " + dataType.getClass().getName());
        }
    }

    public static void validateNoEmptyRecords(Schema schema) {
        validateNoEmptyRecords(schema, "$", Collections.newSetFromMap(new IdentityHashMap<>()));
    }

    private static void validateNoEmptyRecords(Schema schema, String path, Set<Schema> visited) {
        if (schema == null || !visited.add(schema)) {
            return;
        }

        switch (schema.getType()) {
            case RECORD:
                if (schema.getFields().isEmpty()) {
                    throw new RuntimeExceptionWithCode(new BadSchemaException(
                        ExceptionCode.MESSAGE_BAD_SCHEMA,
                        new IllegalStateException("Schema path " + path + " is an empty record: "
                            + schema.getFullName())));
                }
                for (Schema.Field field : schema.getFields()) {
                    validateNoEmptyRecords(field.schema(), path + "." + field.name(), visited);
                }
                return;
            case UNION:
                List<Schema> unionTypes = schema.getTypes();
                for (int i = 0; i < unionTypes.size(); i++) {
                    Schema unionType = unionTypes.get(i);
                    String branchPath = path + "[" + i + "]";
                    if (unionType.getType() == Schema.Type.NULL) {
                        continue;
                    }
                    validateNoEmptyRecords(unionType, branchPath, visited);
                }
                return;
            case ARRAY:
                validateNoEmptyRecords(schema.getElementType(), path + "[]", visited);
                return;
            case MAP:
                validateNoEmptyRecords(schema.getValueType(), path + "{}", visited);
                return;
            default:
        }
    }

    public static Type convert(Schema schema, boolean isVariantEnabled) {
        return SchemaToTypeExtended.visit(schema, new SchemaToTypeExtended(schema), isVariantEnabled);
    }

    public static Type convert(Schema schema, boolean isVariantEnabled, boolean isUnityCatalog) {
        if (isUnityCatalog) {
            return UnityCatalogSchemaToType.visit(schema, new UnityCatalogSchemaToType(schema), isVariantEnabled);
        }
        return convert(schema, isVariantEnabled);
    }

    @VisibleForTesting
    public static org.apache.iceberg.Schema toIceberg(Schema schema) {
        return toIceberg(schema, false);
    }

    public static org.apache.iceberg.Schema toIceberg(Schema schema, boolean isVariantEnabled) {
        final List<Types.NestedField> fields = convert(schema, isVariantEnabled).asNestedType().asStructType().fields();
        return new org.apache.iceberg.Schema(fields);
    }
    public static org.apache.iceberg.Schema toIceberg(Schema schema, boolean isVariantEnabled, boolean isUnityCatalog) {
        final List<Types.NestedField> fields = convert(schema, isVariantEnabled, isUnityCatalog)
            .asNestedType().asStructType().fields();
        return new org.apache.iceberg.Schema(fields);
    }

    public static org.apache.iceberg.Schema toIceberg(Schema schema,
                                                      Set<String> identifierFieldNames,
                                                      boolean isVariantEnabled) {
        final List<Types.NestedField> fields = convert(schema, isVariantEnabled).asNestedType().asStructType().fields();
        Set<Integer> identifierFieldIds = fields.stream()
            // if the field is required and is in the identifierFieldNames, add it to the identifierFieldIds
            .filter(f -> identifierFieldNames.contains(f.name()) && f.isRequired())
            .map(Types.NestedField::fieldId).collect(Collectors.toSet());
        return new org.apache.iceberg.Schema(fields, identifierFieldIds);
    }
    public static org.apache.iceberg.Schema toIceberg(Schema schema,
                                                      Set<String> identifierFieldNames,
                                                      boolean isVariantEnabled,
                                                      boolean isUnityCatalog) {
        final List<Types.NestedField> fields = convert(schema, isVariantEnabled, isUnityCatalog)
            .asNestedType().asStructType().fields();
        Set<Integer> identifierFieldIds = fields.stream()
            // if the field is required and is in the identifierFieldNames, add it to the identifierFieldIds
            .filter(f -> identifierFieldNames.contains(f.name()) && f.isRequired())
            .map(Types.NestedField::fieldId).collect(Collectors.toSet());
        return new org.apache.iceberg.Schema(fields, identifierFieldIds);
    }
}
