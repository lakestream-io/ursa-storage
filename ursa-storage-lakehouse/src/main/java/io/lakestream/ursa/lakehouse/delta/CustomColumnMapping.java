/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.lakehouse.delta;

import io.delta.kernel.internal.util.ColumnMapping;
import io.delta.kernel.types.ArrayType;
import io.delta.kernel.types.DataType;
import io.delta.kernel.types.FieldMetadata;
import io.delta.kernel.types.MapType;
import io.delta.kernel.types.StructField;
import io.delta.kernel.types.StructType;
import java.util.concurrent.atomic.AtomicInteger;

public class CustomColumnMapping {

    public static StructType assignColumnIdAndPhysicalNameForCreateTable(
        StructType oldSchema, AtomicInteger maxColumnId) {
        StructType newSchema = new StructType();
        for (StructField field : oldSchema.fields()) {
            newSchema =
                newSchema.add(
                    transformAndAssignColumnIdAndPhysicalName(
                        assignColumnIdAndPhysicalNameToField(
                            field, maxColumnId), maxColumnId));
        }
        return newSchema;
    }

    public static StructType assignColumnIdAndPhysicalNameForTableEvolution(
        StructType oldSchema, StructType newSchema, AtomicInteger maxColumnId, boolean softDeleteEnabled,
        boolean makeNewFieldsOptional) {
        StructType finalSchema = new StructType();
        for (StructField field : newSchema.fields()) {
            int index = oldSchema.indexOf(field.getName());
            if (index == -1) {
                StructField newField = makeNewFieldsOptional ? makeOptionalIfNeeded(field) : field;
                finalSchema = finalSchema.add(transformAndAssignColumnIdAndPhysicalName(
                    assignColumnIdAndPhysicalNameToField(
                        newField, maxColumnId), maxColumnId));
            } else {
                StructField oldField = oldSchema.at(index);
                DataType dataType = assignColumnIdAndPhysicalNameForTypeEvolution(
                    oldField.getDataType(), field.getDataType(), maxColumnId, softDeleteEnabled,
                    makeNewFieldsOptional);
                finalSchema =
                    finalSchema.add(new StructField(field.getName(), dataType, field.isNullable(),
                        oldField.getMetadata()));
            }
        }
        if (softDeleteEnabled) {
            for (StructField oldField : oldSchema.fields()) {
                if (newSchema.indexOf(oldField.getName()) == -1) {
                    finalSchema = finalSchema.add(makeOptionalIfNeeded(oldField));
                }
            }
        }
        return finalSchema;
    }

    private static DataType assignColumnIdAndPhysicalNameForTypeEvolution(
        DataType oldType, DataType newType, AtomicInteger maxColumnId, boolean softDeleteEnabled,
        boolean makeNewFieldsOptional) {
        if (oldType instanceof StructType oldStruct && newType instanceof StructType newStruct) {
            return assignColumnIdAndPhysicalNameForTableEvolution(
                oldStruct, newStruct, maxColumnId, softDeleteEnabled, makeNewFieldsOptional);
        }
        if (oldType instanceof ArrayType oldArray && newType instanceof ArrayType newArray) {
            StructField elementField = assignColumnIdAndPhysicalNameForFieldEvolution(
                oldArray.getElementField(), newArray.getElementField(), maxColumnId, softDeleteEnabled,
                makeNewFieldsOptional);
            return new ArrayType(elementField);
        }
        if (oldType instanceof MapType oldMap && newType instanceof MapType newMap) {
            StructField keyField = assignColumnIdAndPhysicalNameForFieldEvolution(
                oldMap.getKeyField(), newMap.getKeyField(), maxColumnId, softDeleteEnabled,
                makeNewFieldsOptional);
            StructField valueField = assignColumnIdAndPhysicalNameForFieldEvolution(
                oldMap.getValueField(), newMap.getValueField(), maxColumnId, softDeleteEnabled,
                makeNewFieldsOptional);
            return new MapType(keyField, valueField);
        }
        return newType;
    }

    private static StructField assignColumnIdAndPhysicalNameForFieldEvolution(
        StructField oldField, StructField newField, AtomicInteger maxColumnId, boolean softDeleteEnabled,
        boolean makeNewFieldsOptional) {
        DataType dataType = assignColumnIdAndPhysicalNameForTypeEvolution(
            oldField.getDataType(), newField.getDataType(), maxColumnId, softDeleteEnabled,
            makeNewFieldsOptional);
        return new StructField(newField.getName(), dataType, newField.isNullable(), oldField.getMetadata());
    }

    private static StructField makeOptionalIfNeeded(StructField field) {
        if (field.isNullable()) {
            return field;
        }
        return new StructField(field.getName(), field.getDataType(), true, field.getMetadata());
    }

    private static StructField transformAndAssignColumnIdAndPhysicalName(
        StructField field,
        AtomicInteger maxColumnId) {
        DataType dataType = field.getDataType();
        if (dataType instanceof StructType) {
            StructType type = (StructType) dataType;
            StructType schema = new StructType();
            for (StructField f : type.fields()) {
                schema =
                    schema.add(
                        transformAndAssignColumnIdAndPhysicalName(
                            assignColumnIdAndPhysicalNameToField(
                                f, maxColumnId), maxColumnId));
            }
            return new StructField(field.getName(), schema, field.isNullable(), field.getMetadata());
        } else if (dataType instanceof ArrayType) {
            ArrayType type = (ArrayType) dataType;
            StructField elementField =
                transformAndAssignColumnIdAndPhysicalName(type.getElementField(), maxColumnId);
            return new StructField(
                field.getName(), new ArrayType(elementField), field.isNullable(), field.getMetadata());
        } else if (dataType instanceof MapType) {
            MapType type = (MapType) dataType;
            StructField key =
                transformAndAssignColumnIdAndPhysicalName(
                    type.getKeyField(), maxColumnId);
            StructField value =
                transformAndAssignColumnIdAndPhysicalName(type.getValueField(), maxColumnId);
            return new StructField(
                field.getName(), new MapType(key, value), field.isNullable(), field.getMetadata());
        }
        return field;
    }

    private static StructField assignColumnIdAndPhysicalNameToField(
        StructField field,
        AtomicInteger maxColumnId) {
        if (!hasColumnId(field)) {
            field =
                field.withNewMetadata(
                    FieldMetadata.builder()
                        .fromMetadata(field.getMetadata())
                        .putLong(ColumnMapping.COLUMN_MAPPING_ID_KEY, maxColumnId.incrementAndGet())
                        .build());
        }
        if (!hasPhysicalName(field)) {
            // re-use old display names as physical names when a table is updated
            String physicalName = field.getName();
            field =
                field.withNewMetadata(
                    FieldMetadata.builder()
                        .fromMetadata(field.getMetadata())
                        .putString(ColumnMapping.COLUMN_MAPPING_PHYSICAL_NAME_KEY, physicalName)
                        .build());
        }
        return field;
    }

    static boolean hasColumnId(StructField field) {
        return field.getMetadata().contains(ColumnMapping.COLUMN_MAPPING_ID_KEY);
    }

    static boolean hasPhysicalName(StructField field) {
        return field.getMetadata().contains(ColumnMapping.COLUMN_MAPPING_PHYSICAL_NAME_KEY);
    }

}
