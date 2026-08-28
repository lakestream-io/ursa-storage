/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.lakehouse.delta;

import io.delta.kernel.data.ArrayValue;
import io.delta.kernel.data.ColumnVector;
import io.delta.kernel.types.DataType;
import io.delta.kernel.types.StructField;
import io.delta.kernel.types.StructType;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import org.apache.avro.util.Utf8;

public class SimpleColumnVector implements ColumnVector {
    private final List<Object> data;
    private final DataType type;

    public SimpleColumnVector(List<Object> data, DataType type) {
        this.data = data;
        this.type = type;
    }

    @Override
    public DataType getDataType() {
        return type;
    }

    @Override
    public int getSize() {
        return data.size();
    }

    @Override
    public boolean isNullAt(int rowId) {
        return data.get(rowId) == null;
    }

    @Override
    public boolean getBoolean(int rowId) {
        return (boolean) data.get(rowId);
    }

    @Override
    public byte getByte(int rowId) {
        return (byte) data.get(rowId);
    }

    @Override
    public short getShort(int rowId) {
        return (short) data.get(rowId);
    }

    @Override
    public int getInt(int rowId) {
        return (int) data.get(rowId);
    }

    @Override
    public long getLong(int rowId) {
        return (long) data.get(rowId);
    }

    @Override
    public float getFloat(int rowId) {
        return (float) data.get(rowId);
    }

    @Override
    public double getDouble(int rowId) {
        return (double) data.get(rowId);
    }

    @Override
    public String getString(int rowId) {
        Object o = data.get(rowId);
        if (o instanceof Utf8) {
            return o.toString();
        }
        return (String) o;
    }

    @Override
    public byte[] getBinary(int rowId) {
        return (byte[]) data.get(rowId);
    }

    @Override
    public BigDecimal getDecimal(int rowId) {
        return (BigDecimal) data.get(rowId);
    }

    @Override
    public ColumnVector getChild(int ordinal) {
        if (type instanceof StructType) {
            StructType structType = (StructType) type;
            List<StructField> fields = structType.fields();
            if (ordinal < 0 || ordinal >= fields.size()) {
                throw new IndexOutOfBoundsException("Invalid struct field ordinal: " + ordinal);
            }

            List<Object> fieldData = new ArrayList<>();
            for (Object row : data) {
                if (row == null) {
                    fieldData.add(null);
                } else {
                    fieldData.add(((GenericRow) row).getValue(ordinal));
                }
            }
            return new SimpleColumnVector(fieldData, fields.get(ordinal).getDataType());
        }
        throw new UnsupportedOperationException(
                "Child vectors are not available for vector of type " + getDataType());
    }

    @Override
    public ArrayValue getArray(int rowId) {
        return (ArrayValue) data.get(rowId);
    }

    @Override
    public void close() {
        // No-op
    }
}
