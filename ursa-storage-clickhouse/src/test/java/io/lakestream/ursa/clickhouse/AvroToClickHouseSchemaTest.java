/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.clickhouse;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Unit tests for {@link AvroToClickHouseSchema}. Covers every primitive
 * mapping, nullable unions, nested record flattening, arrays, maps, and the
 * unsupported-type rejection paths.
 */
class AvroToClickHouseSchemaTest {

    private static java.util.stream.Stream<Arguments> primitives() {
        return java.util.stream.Stream.of(
                Arguments.of(Schema.create(Schema.Type.INT), "Int32"),
                Arguments.of(Schema.create(Schema.Type.LONG), "Int64"),
                Arguments.of(Schema.create(Schema.Type.FLOAT), "Float32"),
                Arguments.of(Schema.create(Schema.Type.DOUBLE), "Float64"),
                Arguments.of(Schema.create(Schema.Type.BOOLEAN), "UInt8"),
                Arguments.of(Schema.create(Schema.Type.STRING), "String"),
                Arguments.of(Schema.create(Schema.Type.BYTES), "String"));
    }

    @ParameterizedTest
    @MethodSource("primitives")
    void primitiveAvroTypeMapsToExpectedClickHouseType(Schema avro, String expected) {
        assertThat(AvroToClickHouseSchema.avroToClickHouseType(avro)).isEqualTo(expected);
    }

    @Test
    void nullableUnionWrapsInNullable() {
        Schema nullable = SchemaBuilder.unionOf().nullType().and().longType().endUnion();
        assertThat(AvroToClickHouseSchema.avroToClickHouseType(nullable))
                .isEqualTo("Nullable(Int64)");
    }

    @Test
    void nullableUnionWithReversedOrderAlsoSupported() {
        Schema nullable = SchemaBuilder.unionOf().stringType().and().nullType().endUnion();
        assertThat(AvroToClickHouseSchema.avroToClickHouseType(nullable))
                .isEqualTo("Nullable(String)");
    }

    @Test
    void arrayMapsToArrayWithRecursiveElementType() {
        Schema array = SchemaBuilder.array().items().intType();
        assertThat(AvroToClickHouseSchema.avroToClickHouseType(array)).isEqualTo("Array(Int32)");
    }

    @Test
    void mapMapsToMapStringV() {
        Schema map = SchemaBuilder.map().values().longType();
        assertThat(AvroToClickHouseSchema.avroToClickHouseType(map))
                .isEqualTo("Map(String, Int64)");
    }

    @Test
    void nestedRecordIsFlattenedWithDottedColumnNames() {
        Schema address = SchemaBuilder.record("Address").fields()
                .requiredString("city")
                .requiredString("zip")
                .endRecord();
        Schema user = SchemaBuilder.record("User").fields()
                .requiredLong("id")
                .name("address").type(address).noDefault()
                .endRecord();

        ClickHouseSchema schema = AvroToClickHouseSchema.convert(
                user, List.of("id"), ClickHouseTableEngine.MERGE_TREE);

        assertThat(schema.columns()).hasSize(3);
        assertThat(schema.columns().get(0).name()).isEqualTo("id");
        assertThat(schema.columns().get(0).type()).isEqualTo("Int64");
        assertThat(schema.columns().get(1).name()).isEqualTo("address.city");
        assertThat(schema.columns().get(1).type()).isEqualTo("String");
        assertThat(schema.columns().get(2).name()).isEqualTo("address.zip");
        assertThat(schema.columns().get(2).type()).isEqualTo("String");
        assertThat(schema.primaryKey()).containsExactly("id");
    }

    @Test
    void nullableFieldKeepsNullableFlagOnColumn() {
        Schema record = SchemaBuilder.record("R").fields()
                .name("optional").type().nullable().stringType().noDefault()
                .endRecord();

        ClickHouseSchema schema = AvroToClickHouseSchema.convert(
                record, List.of(), ClickHouseTableEngine.MERGE_TREE);

        assertThat(schema.columns()).hasSize(1);
        ClickHouseColumn col = schema.columns().get(0);
        assertThat(col.name()).isEqualTo("optional");
        assertThat(col.type()).isEqualTo("Nullable(String)");
        assertThat(col.nullable()).isTrue();
    }

    @Test
    void fixedTypeIsRejected() {
        Schema fixed = SchemaBuilder.fixed("uuid").size(16);
        assertThatThrownBy(() -> AvroToClickHouseSchema.avroToClickHouseType(fixed))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Unsupported AVRO type")
                .hasMessageContaining("FIXED");
    }

    @Test
    void enumTypeIsRejected() {
        Schema enumeration =
                SchemaBuilder.enumeration("Color").symbols("RED", "GREEN", "BLUE");
        assertThatThrownBy(() -> AvroToClickHouseSchema.avroToClickHouseType(enumeration))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("ENUM");
    }

    @Test
    void multiBranchUnionIsRejected() {
        Schema multi = SchemaBuilder.unionOf().intType().and().longType().and().stringType()
                .endUnion();
        assertThatThrownBy(() -> AvroToClickHouseSchema.avroToClickHouseType(multi))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("union");
    }

    @Test
    void unionWithTwoNonNullBranchesIsRejected() {
        Schema badUnion = SchemaBuilder.unionOf().intType().and().longType().endUnion();
        assertThatThrownBy(() -> AvroToClickHouseSchema.avroToClickHouseType(badUnion))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("UNION[null, T]");
    }

    @Test
    void topLevelNonRecordIsRejectedByConvert() {
        Schema notRecord = Schema.create(Schema.Type.STRING);
        assertThatThrownBy(() -> AvroToClickHouseSchema.convert(
                notRecord, List.of(), ClickHouseTableEngine.MERGE_TREE))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("RECORD");
    }

    @Test
    void convertPreservesEngineAndPrimaryKey() {
        Schema record = SchemaBuilder.record("R").fields()
                .requiredLong("id")
                .endRecord();

        ClickHouseSchema schema = AvroToClickHouseSchema.convert(
                record, List.of("id"), ClickHouseTableEngine.REPLACING_MERGE_TREE);

        assertThat(schema.engine()).isEqualTo(ClickHouseTableEngine.REPLACING_MERGE_TREE);
        assertThat(schema.primaryKey()).containsExactly("id");
    }
}
