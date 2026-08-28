/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.lakehouse.delta;

import static java.util.Objects.requireNonNull;

import io.delta.kernel.data.ArrayValue;
import io.delta.kernel.data.MapValue;
import io.delta.kernel.data.Row;
import io.delta.kernel.types.ArrayType;
import io.delta.kernel.types.BinaryType;
import io.delta.kernel.types.BooleanType;
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
import io.delta.kernel.types.StructType;
import io.delta.kernel.types.TimestampNTZType;
import io.delta.kernel.types.TimestampType;
import java.math.BigDecimal;
import java.util.Map;

public class GenericRow implements Row {
    private final StructType schema;
    private final Map<Integer, Object> ordinalToValue;

    /**
     * @param schema the schema of the row
     * @param ordinalToValue a mapping of column ordinal to objects; for each column the object must
     *     be of the return type corresponding to the data type's getter method in the Row interface
     */
    public GenericRow(StructType schema, Map<Integer, Object> ordinalToValue) {
        this.schema = requireNonNull(schema, "schema is null");
        this.ordinalToValue = requireNonNull(ordinalToValue, "ordinalToValue is null");
    }

    public void put(int index, Object value) {
        ordinalToValue.put(index, value);
    }

    @Override
    public StructType getSchema() {
        return schema;
    }

    @Override
    public boolean isNullAt(int ordinal) {
        return getValue(ordinal) == null;
    }

    @Override
    public boolean getBoolean(int ordinal) {
        throwIfUnsafeAccess(ordinal, BooleanType.class, "boolean");
        return (boolean) getValue(ordinal);
    }

    @Override
    public byte getByte(int ordinal) {
        throwIfUnsafeAccess(ordinal, ByteType.class, "byte");
        return (byte) getValue(ordinal);
    }

    @Override
    public short getShort(int ordinal) {
        throwIfUnsafeAccess(ordinal, ShortType.class, "short");
        return (short) getValue(ordinal);
    }

    @Override
    public int getInt(int ordinal) {
        DataType actualDataType = dataType(ordinal);
        if (!(actualDataType instanceof DateType)) {
            throwIfUnsafeAccess(ordinal, IntegerType.class, "integer");
        }
        return (int) getValue(ordinal);
    }

    @Override
    public long getLong(int ordinal) {
        DataType actualDataType = dataType(ordinal);
        if (!(actualDataType instanceof TimestampType) && !(actualDataType instanceof TimestampNTZType)) {
            throwIfUnsafeAccess(ordinal, LongType.class, "long");
        }
        return (long) getValue(ordinal);
    }

    @Override
    public float getFloat(int ordinal) {
        throwIfUnsafeAccess(ordinal, FloatType.class, "float");
        return (float) getValue(ordinal);
    }

    @Override
    public double getDouble(int ordinal) {
        throwIfUnsafeAccess(ordinal, DoubleType.class, "double");
        return (double) getValue(ordinal);
    }

    @Override
    public String getString(int ordinal) {
        throwIfUnsafeAccess(ordinal, StringType.class, "string");
        return (String) getValue(ordinal);
    }

    @Override
    public BigDecimal getDecimal(int ordinal) {
        throwIfUnsafeAccess(ordinal, DecimalType.class, "decimal");
        return (BigDecimal) getValue(ordinal);
    }

    @Override
    public byte[] getBinary(int ordinal) {
        throwIfUnsafeAccess(ordinal, BinaryType.class, "binary");
        return (byte[]) getValue(ordinal);
    }

    @Override
    public Row getStruct(int ordinal) {
        throwIfUnsafeAccess(ordinal, StructType.class, "struct");
        return (Row) getValue(ordinal);
    }

    @Override
    public ArrayValue getArray(int ordinal) {
        // TODO: not sufficient check, also need to check the element type
        throwIfUnsafeAccess(ordinal, ArrayType.class, "array");
        return (ArrayValue) getValue(ordinal);
    }

    @Override
    public MapValue getMap(int ordinal) {
        // TODO: not sufficient check, also need to check the element types
        throwIfUnsafeAccess(ordinal, MapType.class, "map");
        return (MapValue) getValue(ordinal);
    }

    public Object getValue(int ordinal) {
        return ordinalToValue.get(ordinal);
    }

    private void throwIfUnsafeAccess(
            int ordinal, Class<? extends DataType> expDataType, String accessType) {

        DataType actualDataType = dataType(ordinal);
        if (!expDataType.isAssignableFrom(actualDataType.getClass())) {
            String msg =
                    String.format(
                            "Trying to access a `%s` value from vector of type `%s`", accessType, actualDataType);
            throw new UnsupportedOperationException(msg);
        }
    }

    private DataType dataType(int ordinal) {
        if (schema.length() <= ordinal) {
            throw new IllegalArgumentException("invalid ordinal: " + ordinal);
        }

        return schema.at(ordinal).getDataType();
    }
}