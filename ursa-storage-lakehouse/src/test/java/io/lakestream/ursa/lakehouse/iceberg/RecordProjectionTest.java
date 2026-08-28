/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.lakehouse.iceberg;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;


@Tag("lakehouse")
public class RecordProjectionTest {

    private final Schema dataSchema = new Schema(
        Types.NestedField.required(1, "id", Types.IntegerType.get()),
        Types.NestedField.optional(2, "data", Types.StringType.get()),
        Types.NestedField.required(3, "nested", Types.StructType.of(
            Types.NestedField.required(4, "field1", Types.StringType.get()),
            Types.NestedField.required(5, "field2", Types.IntegerType.get())
        ))
    );

    private final Schema nestedSchema = new Schema(
        Types.NestedField.required(4, "field1", Types.StringType.get()),
        Types.NestedField.required(5, "field2", Types.IntegerType.get())
    );

    @Test
    void shouldProjectTopLevelFields() {
        Schema projectionSchema = new Schema(
            Types.NestedField.required(1, "id", Types.IntegerType.get())
        );

        TestRecord record = new TestRecord(
            dataSchema,
            42,
            "test",
            new TestNestedRecord(nestedSchema, "nested-value", 100)
        );

        RecordProjection projection = RecordProjection.create(dataSchema, projectionSchema);
        RecordProjection result = projection.wrap(record);

        assertThat(result.size()).isEqualTo(1);
        assertThat(result.get(0, Integer.class)).isEqualTo(42);
        assertThat(result.struct()).isEqualTo(projectionSchema.asStruct());
    }

    @Test
    void shouldProjectNestedFields() {
        Schema projectionSchema = new Schema(
            Types.NestedField.required(3, "nested", Types.StructType.of(
                Types.NestedField.required(4, "field1", Types.StringType.get())
            ))
        );

        TestNestedRecord nestedRecord = new TestNestedRecord(nestedSchema, "nested-value", 100);
        TestRecord record = new TestRecord(dataSchema, 42, "test", nestedRecord);

        RecordProjection projection = RecordProjection.create(dataSchema, projectionSchema);
        RecordProjection result = projection.wrap(record);

        assertThat(result.size()).isEqualTo(1);
        Object nestedProjection = result.get(0, Object.class);
        assertThat(nestedProjection).isInstanceOf(RecordProjection.class);

        RecordProjection nestedResult = (RecordProjection) nestedProjection;
        assertThat(nestedResult.size()).isEqualTo(1);
        assertThat(nestedResult.get(0, String.class)).isEqualTo("nested-value");
        assertThat(nestedResult.struct()).isEqualTo(
            Types.StructType.of(
                Types.NestedField.required(4, "field1", Types.StringType.get())
            )
        );
    }

    @Test
    void shouldThrowWhenProjectingPartialMap() {
        Schema dataSchemaWithMap = new Schema(
            Types.NestedField.required(1, "map", Types.MapType.ofRequired(2, 3,
                Types.StringType.get(),
                Types.StructType.of(
                    Types.NestedField.required(4, "a", Types.StringType.get()),
                    Types.NestedField.required(5, "b", Types.IntegerType.get())
                ))
            )
        );

        Schema projectionSchema = new Schema(
            Types.NestedField.required(1, "map", Types.MapType.ofRequired(2, 3,
                Types.StringType.get(),
                Types.StructType.of(
                    Types.NestedField.required(4, "a", Types.StringType.get())
                ))
            )
        );

        assertThrows(IllegalArgumentException.class,
            () -> RecordProjection.create(dataSchemaWithMap, projectionSchema),
            "Should reject partial map projection");
    }

    @Test
    void shouldHandleOptionalMissingFields() {
        Schema projectionSchema = new Schema(
            Types.NestedField.optional(2, "data", Types.StringType.get()),
            Types.NestedField.optional(6, "missing", Types.StringType.get())
        );

        IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
            () -> RecordProjection.create(dataSchema, projectionSchema));

        assertThat(e.getMessage()).contains("Cannot find field");
    }

    @Test
    void shouldHandleNestedProjectionWithSchemaEvolution() {
        Schema evolvedSchema = new Schema(
            Types.NestedField.required(3, "nested", Types.StructType.of(
                Types.NestedField.required(4, "field1", Types.StringType.get()),
                Types.NestedField.optional(6, "new_field", Types.BooleanType.get())
            ))
        );

        TestNestedRecord nestedRecord = new TestNestedRecord(nestedSchema, "value", 42);
        TestRecord record = new TestRecord(dataSchema, 1, "test", nestedRecord);

        assertThrows(IllegalArgumentException.class,
            () -> RecordProjection.create(dataSchema, evolvedSchema),
            "Should reject projection with new nested field");
    }
}
