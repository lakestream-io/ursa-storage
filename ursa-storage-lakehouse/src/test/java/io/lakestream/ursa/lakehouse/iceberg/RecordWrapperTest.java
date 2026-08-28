/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.lakehouse.iceberg;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.HashMap;
import java.util.Map;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag("lakehouse")
class RecordWrapperTest {

    private TestRecord delegate;
    private RecordWrapper wrapper;
    private final Operation testOp = Operation.UPDATE;

    @BeforeEach
    void setUp() {
        delegate = new TestRecord(42, "test", true);
        wrapper = new RecordWrapper(delegate, testOp);
    }

    @Test
    void shouldDelegateRecordOperations() {
        // Verify field access
        assertThat(wrapper.get(0, Integer.class)).isEqualTo(42);
        assertThat(wrapper.get(1, String.class)).isEqualTo("test");
        assertThat(wrapper.get(2, Boolean.class)).isTrue();

        // Verify structural properties
        assertThat(wrapper.size()).isEqualTo(3);
        assertThat(wrapper.struct()).isEqualTo(delegate.struct());
    }

    @Test
    void shouldReturnCorrectOperation() {
        assertThat(wrapper.op()).isEqualTo(testOp);
    }

    @Test
    void shouldHandleFieldModifications() {
        // Test field modification
        wrapper.set(0, 100);
        wrapper.setField("field1", "modified");

        assertThat(delegate.get(0, Integer.class)).isEqualTo(100);
        assertThat(delegate.getField("field1")).isEqualTo("modified");
    }

    @Test
    void shouldCreateProperCopies() {
        // Test simple copy
        Record copy = wrapper.copy();
        assertThat(copy)
            .isInstanceOf(RecordWrapper.class)
            .isNotSameAs(wrapper);

        RecordWrapper copiedWrapper = (RecordWrapper) copy;
        assertThat(copiedWrapper.op()).isEqualTo(testOp);
        assertThat(copiedWrapper.get(0, Integer.class)).isEqualTo(42);

        // Test copy with overwrites
        Map<String, Object> overwrites = new HashMap<>();
        overwrites.put("field1", "new-value");
        overwrites.put("field2", true);

        Record modifiedCopy = wrapper.copy(overwrites);
        assertThat(modifiedCopy.get(1, String.class)).isEqualTo("new-value");
        assertThat(modifiedCopy.get(2, Boolean.class)).isTrue(); // Changed expectation
        assertThat(((RecordWrapper) modifiedCopy).op()).isEqualTo(testOp);
    }

    @Test
    void shouldMaintainDelegateStructure() {
        assertThat(wrapper.struct().fields()).hasSize(3);
        assertThat(wrapper.struct().field("field0").type())
            .isEqualTo(Types.IntegerType.get());
    }

    // Test implementation of Record for verification
    private static class TestRecord implements Record {
        private final Object[] values;
        private final Types.StructType structType;

        TestRecord(Integer field0, String field1, Boolean field2) {
            this.values = new Object[]{field0, field1, field2};
            this.structType = Types.StructType.of(
                Types.NestedField.required(0, "field0", Types.IntegerType.get()),
                Types.NestedField.required(1, "field1", Types.StringType.get()),
                Types.NestedField.required(2, "field2", Types.BooleanType.get())
            );
        }

        @Override
        public Object get(int pos) {
            return values[pos];
        }

        @Override
        public <T> T get(int pos, Class<T> javaClass) {
            return javaClass.cast(values[pos]);
        }

        @Override
        public void set(int pos, Object value) {
            values[pos] = value;
        }

        @Override
        public int size() {
            return values.length;
        }

        @Override
        public Record copy() {
            return new TestRecord(
                (Integer) values[0],
                (String) values[1],
                (Boolean) values[2]
            );
        }

        @Override
        public Record copy(Map<String, Object> overwriteValues) {
            Integer new0 = getWithType("field0", overwriteValues, Integer.class);
            String new1 = getWithType("field1", overwriteValues, String.class);
            Boolean new2 = getWithType("field2", overwriteValues, Boolean.class);

            return new TestRecord(new0, new1, new2);
        }

        private <T> T getWithType(String fieldName, Map<String, Object> overwrites, Class<T> type) {
            Object value = overwrites.containsKey(fieldName)
                ? overwrites.get(fieldName) : getField(fieldName);

            if (!type.isInstance(value)) {
                throw new IllegalArgumentException(
                    "Invalid type for field " + fieldName + ", expected " + type.getSimpleName()
                );
            }
            return type.cast(value);
        }

        @Override
        public Types.StructType struct() {
            return structType;
        }

        @Override
        public Object getField(String name) {
            return switch (name) {
                case "field0" -> values[0];
                case "field1" -> values[1];
                case "field2" -> values[2];
                default -> throw new IllegalArgumentException("Unknown field: " + name);
            };
        }

        @Override
        public void setField(String name, Object value) {
            switch (name) {
                case "field0" -> values[0] = value;
                case "field1" -> values[1] = value;
                case "field2" -> values[2] = value;
                default -> throw new IllegalArgumentException("Unknown field: " + name);
            }
        }
    }
}
