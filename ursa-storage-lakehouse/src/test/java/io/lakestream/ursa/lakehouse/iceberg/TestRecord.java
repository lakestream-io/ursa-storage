/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.lakehouse.iceberg;

import java.util.Map;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.types.Types.StructType;

public class TestRecord implements Record {
    private final Schema schema;
    private final Object[] values;

    public TestRecord(Schema schema, Object... values) {
        this.schema = schema;
        this.values = values;
        validate();
    }

    private void validate() {
        if (schema.columns().size() != values.length) {
            throw new IllegalArgumentException("Number of values doesn't match schema");
        }
    }

    @Override
    public StructType struct() {
        return schema.asStruct();
    }

    @Override
    public <T> T get(int pos, Class<T> javaClass) {
        return javaClass.cast(values[pos]);
    }

    @Override
    public int size() {
        return values.length;
    }

    @Override
    public <T> void set(int pos, T value) {
        values[pos] = value;
    }

    @Override
    public Object get(int pos) {
        return values[pos];
    }

    @Override
    public Object getField(String name) {
        int pos = schema.findField(name).fieldId();
        return get(pos - 1); // Using field ID based position
    }

    @Override
    public void setField(String name, Object value) {
        int pos = schema.findField(name).fieldId();
        set(pos - 1, value);
    }

    @Override
    public Record copy() {
        return new TestRecord(schema, values.clone());
    }

    @Override
    public Record copy(Map<String, Object> overwriteValues) {
        Object[] newValues = values.clone();
        for (Map.Entry<String, Object> entry : overwriteValues.entrySet()) {
            int pos = schema.findField(entry.getKey()).fieldId();
            newValues[pos - 1] = entry.getValue();
        }
        return new TestRecord(schema, newValues);
    }
}