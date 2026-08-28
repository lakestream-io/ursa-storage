/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.lakehouse.delta;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.confluent.kafka.schemaregistry.avro.AvroSchemaUtils;
import io.delta.kernel.data.ColumnVector;
import io.delta.kernel.data.Row;
import io.delta.kernel.types.ArrayType;
import io.delta.kernel.types.StructType;
import io.delta.kernel.types.VariantType;
import io.lakestream.ursa.lakehouse.utils.AvroSchemaUtilExtended;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.avro.LogicalType;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericArray;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag("lakehouse")
public class AvroDeltaConvertTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    @Test
    public void testConvertBoolean() {
        Schema schema = Schema.createRecord("TestRecord", null, null, false);
        schema.setFields(List.of(new Schema.Field("flag", Schema.create(Schema.Type.BOOLEAN), null, null)));

        GenericRecord avroRecord = new GenericData.Record(schema);
        avroRecord.put("flag", true);
        StructType deltaSchema = AvroSchemaUtilExtended.toDelta(schema, false);
        GenericRow row = AvroToDeltaConvert.convert(avroRecord, deltaSchema);

        assertNotNull(row);
        assertTrue(row.getBoolean(deltaSchema.indexOf("flag")));
    }

    @Test
    public void testConvertInteger() {
        Schema schema = Schema.createRecord("TestRecord", null, null, false);
        schema.setFields(List.of(new Schema.Field("value", Schema.create(Schema.Type.INT), null, null)));

        GenericRecord avroRecord = new GenericData.Record(schema);
        avroRecord.put("value", 42);

        StructType deltaSchema = AvroSchemaUtilExtended.toDelta(schema, false);

        GenericRow row = AvroToDeltaConvert.convert(avroRecord, deltaSchema);

        assertNotNull(row);
        assertEquals(42, row.getInt(deltaSchema.indexOf("value")));
    }

    @Test
    public void testConvertString() {
        Schema schema = Schema.createRecord("TestRecord", null, null, false);
        schema.setFields(List.of(new Schema.Field("name", Schema.create(Schema.Type.STRING), null, null)));

        GenericRecord avroRecord = new GenericData.Record(schema);
        avroRecord.put("name", "John Doe");

        StructType deltaSchema = AvroSchemaUtilExtended.toDelta(schema, false);

        GenericRow row = AvroToDeltaConvert.convert(avroRecord, deltaSchema);

        assertNotNull(row);
        assertEquals("John Doe", row.getString(deltaSchema.indexOf("name")));
    }

    @Test
    public void testConvertArray() {
        Schema schema = SchemaBuilder.record("TestRecord")
            .fields()
            .name("values").type().array().items().stringType().noDefault()
            .endRecord();

        GenericRecord avroRecord = new GenericData.Record(schema);
        GenericArray<String> stringArray = new GenericData.Array<>(3, schema.getField("values").schema());
        stringArray.add("A");
        stringArray.add("B");
        stringArray.add("C");

        avroRecord.put("values", stringArray);
        StructType deltaSchema = AvroSchemaUtilExtended.toDelta(schema, false);

        GenericRow row = AvroToDeltaConvert.convert(avroRecord, deltaSchema);

        assertNotNull(row);
        assertInstanceOf(ArrayValueImpl.class, row.getArray(0));
        ArrayValueImpl arrayValue = (ArrayValueImpl) row.getArray(deltaSchema.indexOf("values"));
        assertEquals(3, arrayValue.getSize());
        assertEquals("A", arrayValue.getElements().getString(0));
        assertEquals("B", arrayValue.getElements().getString(1));
        assertEquals("C", arrayValue.getElements().getString(2));
    }

    @Test
    public void testConvertJavaTimeTypes() {
        Schema instantSchema = new Schema.Parser().parse("{\"type\":\"long\",\"logicalType\":\"timestamp-millis\"}");
        Schema localDateSchema = new Schema.Parser().parse("{\"type\":\"int\",\"logicalType\":\"date\"}");
        Schema localTimeSchema = new Schema.Parser().parse("{\"type\":\"long\",\"logicalType\":\"time-millis\"}");
        Schema localDateTimeSchema =
            new Schema.Parser().parse("{\"type\":\"long\",\"logicalType\":\"timestamp-millis\"}");

        Schema schema = Schema.createRecord("TimeRecord", null, null, false);
        schema.setFields(List.of(
            new Schema.Field("instant", instantSchema, null, null),
            new Schema.Field("localDate", localDateSchema, null, null),
            new Schema.Field("localTime", localTimeSchema, null, null),
            new Schema.Field("localDateTime", localDateTimeSchema, null, null)
        ));

        GenericRecord avroRecord = new GenericData.Record(schema);

        Instant instant = Instant.now();
        LocalDate localDate = LocalDate.of(2025, 9, 8);
        LocalTime localTime = LocalTime.of(12, 30, 0);
        LocalDateTime localDateTime = LocalDateTime.of(2025, 9, 8, 12, 30, 0);

        avroRecord.put("instant", instant.toEpochMilli());
        avroRecord.put("localDate", (int) localDate.toEpochDay());
        avroRecord.put("localTime", (int) TimeUnit.NANOSECONDS.toMillis(localTime.toNanoOfDay()));
        avroRecord.put("localDateTime", localDateTime.toInstant(ZoneOffset.UTC).toEpochMilli());

        AvroSchemaUtils.getDatumWriter(avroRecord, schema, false);

        StructType deltaSchema = AvroSchemaUtilExtended.toDelta(schema, false);

        Row row = AvroToDeltaConvert.convert(avroRecord, deltaSchema);

        assertNotNull(row);

        assertEquals(instant.toEpochMilli() * 1000, row.getLong(deltaSchema.indexOf("instant")));

        assertEquals((int) localDate.toEpochDay(), row.getInt(deltaSchema.indexOf("localDate")));

        assertEquals(localTime.toNanoOfDay() / 1_000_000, row.getLong(deltaSchema.indexOf("localTime")));

        assertEquals(localDateTime.atZone(ZoneOffset.UTC).toInstant().toEpochMilli() * 1000,
            row.getLong(deltaSchema.indexOf("localDateTime")));
    }

    @Test
    public void testConvertMap() {
        Schema schema = SchemaBuilder.record("TestRecord")
            .fields()
            .name("data").type().map().values().stringType().noDefault()
            .endRecord();

        GenericRecord avroRecord = new GenericData.Record(schema);

        Map<String, String> stringMap = new HashMap();
        stringMap.put("key1", "value1");
        stringMap.put("key2", "value2");

        avroRecord.put("data", stringMap);

        StructType deltaSchema = AvroSchemaUtilExtended.toDelta(schema, false);

        GenericRow row = AvroToDeltaConvert.convert(avroRecord, deltaSchema);

        assertNotNull(row);
        assertInstanceOf(MapValueImpl.class, row.getMap(0));
        MapValueImpl mapValue = (MapValueImpl) row.getMap(deltaSchema.indexOf("data"));
        assertEquals(2, mapValue.getSize());
        assertEquals("key1", mapValue.getKeys().getString(0));
        assertEquals("value1", mapValue.getValues().getString(0));
        assertEquals("key2", mapValue.getKeys().getString(1));
        assertEquals("value2", mapValue.getValues().getString(1));
    }

    @Test
    public void testConvertComplexSchema() {
        Schema nestedRecordSchema = Schema.createRecord("NestedRecord", null, null, false);
        nestedRecordSchema.setFields(Arrays.asList(
            new Schema.Field("nestedField1", Schema.create(Schema.Type.INT), null, null),
            new Schema.Field("nestedField2", Schema.create(Schema.Type.STRING), null, null)
        ));

        Schema schema = SchemaBuilder.record("TestRecord")
            .fields()
            .name("id").type().intType().noDefault()
            .name("name").type().stringType().noDefault()
            .name("age").type().longType().noDefault()
            .name("height").type().doubleType().noDefault()
            .name("nestedArray").type().array().items(nestedRecordSchema).noDefault()
            .name("nestedMap").type().map().values(nestedRecordSchema).noDefault()
            .endRecord();

        GenericRecord nestedRecord1 = new GenericData.Record(nestedRecordSchema);
        nestedRecord1.put("nestedField1", 1);
        nestedRecord1.put("nestedField2", "nestedValue1");

        GenericRecord nestedRecord2 = new GenericData.Record(nestedRecordSchema);
        nestedRecord2.put("nestedField1", 2);
        nestedRecord2.put("nestedField2", "nestedValue2");

        GenericArray<GenericRecord> nestedArray = new GenericData.Array<>(2, schema.getField("nestedArray").schema());
        nestedArray.add(nestedRecord1);
        nestedArray.add(nestedRecord2);

        Map<String, GenericRecord> nestedMap = new HashMap<>();
        nestedMap.put("mapKey1", nestedRecord1);
        nestedMap.put("mapKey2", nestedRecord2);

        GenericRecord avroRecord = new GenericData.Record(schema);
        avroRecord.put("id", 123);
        avroRecord.put("name", "John Doe");
        avroRecord.put("age", 30L);
        avroRecord.put("height", 175.5);
        avroRecord.put("nestedArray", nestedArray);
        avroRecord.put("nestedMap", nestedMap);

        StructType deltaSchema = AvroSchemaUtilExtended.toDelta(schema, false);

        GenericRow row = AvroToDeltaConvert.convert(avroRecord, deltaSchema);

        assertNotNull(row);

        assertEquals(123, row.getInt(deltaSchema.indexOf("id")));
        assertEquals("John Doe", row.getString(deltaSchema.indexOf("name")));
        assertEquals(30L, row.getLong(deltaSchema.indexOf("age")));
        assertEquals(175.5, row.getDouble(deltaSchema.indexOf("height")));

        assertInstanceOf(ArrayValueImpl.class, row.getArray(deltaSchema.indexOf("nestedArray")));
        ArrayValueImpl arrayValue = (ArrayValueImpl) row.getArray(4);
        assertEquals(2, arrayValue.getSize());

        StructType elementType =
            (StructType) ((ArrayType) deltaSchema.get("nestedArray").getDataType()).getElementType();

        ColumnVector nestedField1Column = arrayValue.getElements().getChild(elementType.indexOf("nestedField1"));

        assertEquals(1, nestedField1Column.getInt(0));
        assertEquals(2, nestedField1Column.getInt(1));

        ColumnVector nestedField2Column = arrayValue.getElements().getChild(elementType.indexOf("nestedField2"));
        assertEquals("nestedValue1", nestedField2Column.getString(0));
        assertEquals("nestedValue2", nestedField2Column.getString(1));

        assertInstanceOf(MapValueImpl.class, row.getMap(deltaSchema.indexOf("nestedMap")));
        MapValueImpl mapValue = (MapValueImpl) row.getMap(5);
        assertEquals(2, mapValue.getSize());

        assertEquals("mapKey2", mapValue.getKeys().getString(0));
        ColumnVector child2 = mapValue.getValues().getChild(0);
        assertEquals(2, child2.getInt(0));
        assertEquals(1, child2.getInt(1));

        assertEquals("mapKey1", mapValue.getKeys().getString(1));
        ColumnVector child3 = mapValue.getValues().getChild(1);
        assertEquals("nestedValue2", child3.getString(0));
        assertEquals("nestedValue1", child3.getString(1));
    }

    @Test
    public void testConvertVariantWithNull() throws Exception {
        assertVariantFieldRoundTrip(null);
    }

    @Test
    public void testConvertVariantWithBoolean() throws Exception {
        assertVariantFieldRoundTrip(true);
        assertVariantFieldRoundTrip(false);
    }

    @Test
    public void testConvertVariantWithInteger() throws Exception {
        assertVariantFieldRoundTrip(42);
        assertVariantFieldRoundTrip(-42);
        assertVariantFieldRoundTrip(0);
    }

    @Test
    public void testConvertVariantWithLong() throws Exception {
        assertVariantFieldRoundTrip(1234567890123L);
        assertVariantFieldRoundTrip(-1234567890123L);
    }

    @Test
    public void testConvertVariantWithDouble() throws Exception {
        assertVariantFieldRoundTrip(3.14159d);
        assertVariantFieldRoundTrip(-99.125d);
    }

    @Test
    public void testConvertVariantWithString() throws Exception {
        assertVariantFieldRoundTrip("simple string value");
        assertVariantFieldRoundTrip("unicode-你好-🙂");
        assertVariantFieldRoundTrip("escaped-quote-\"value\"");
    }

    @Test
    public void testConvertVariantWithSimpleArray() throws Exception {
        assertVariantFieldRoundTrip(List.of(1, 2, 3, 4, 5));
    }

    @Test
    public void testConvertVariantWithNestedArray() throws Exception {
        assertVariantFieldRoundTrip(List.of(
                1,
                Map.of("nested", List.of(
                        2,
                        3,
                        Map.of("items", List.of(
                                4,
                                5,
                                Map.of("inner", List.of("x", "y", Map.of("z", 1)))
                        ))
                )),
                6));
    }

    @Test
    public void testConvertVariantWithSimpleMap() throws Exception {
        assertVariantFieldRoundTrip(Map.of("age", 30, "active", true));
    }

    @Test
    public void testConvertVariantWithNestedMap() throws Exception {
        assertVariantFieldRoundTrip(Map.of(
                "profile", Map.of(
                        "age", 25,
                        "city", "San Francisco",
                        "geo", Map.of("latE6", 37770000, "lonE6", -122410000),
                        "events", List.of(
                                Map.of("type", "login", "devices", List.of("ios", "web")),
                                Map.of("type", "purchase",
                                        "items", List.of(
                                                Map.of("sku", "a1", "qty", 2),
                                                Map.of("sku", "b9", "qty", 1)
                                        ))
                        ))));
    }

    @Test
    public void testConvertVariantWithComplexNestedStructure() throws Exception {
        Map<String, Object> value = new java.util.LinkedHashMap<>();
        value.put("string", "value");
        value.put("emptyString", "");
        value.put("boolean", true);
        value.put("number", 42);
        value.put("decimal", 12.75d);
        value.put("nullField", null);
        List<Object> innerArray = new ArrayList<>();
        innerArray.add(1);
        innerArray.add("two");
        innerArray.add(null);
        innerArray.add(Map.of("deep", List.of(3, 4, Map.of("v", "x"))));
        value.put("object", Map.of(
                "inner", Map.of(
                        "flag", false,
                        "arr", innerArray)));
        value.put("topArray", List.of(
                Map.of("kind", "a", "payload", Map.of("x", 1)),
                Map.of("kind", "b", "payload", List.of(1, 2, 3)),
                "done"));
        assertVariantFieldRoundTrip(value);
    }

    @Test
    public void testConvertVariantSchemaMapsToDeltaVariantType() {
        Schema schema = buildVariantRecordSchema();
        StructType deltaSchema = AvroSchemaUtilExtended.toDelta(schema, true);

        assertInstanceOf(VariantType.class, deltaSchema.get("variantField").getDataType());
    }

    private void assertVariantFieldRoundTrip(Object value) throws Exception {
        Schema schema = buildVariantRecordSchema();
        StructType deltaSchema = AvroSchemaUtilExtended.toDelta(schema, true);

        GenericRecord avroRecord = new GenericData.Record(schema);
        avroRecord.put("variantField", value);

        GenericRow row = AvroToDeltaConvert.convert(avroRecord, deltaSchema);

        assertNotNull(row);
        assertInstanceOf(VariantType.class, deltaSchema.get("variantField").getDataType());
        Object variantValue = row.getValue(deltaSchema.indexOf("variantField"));
        if (value == null) {
            assertNull(variantValue);
            return;
        }
        assertVariantJsonEquals(MAPPER.writeValueAsString(value), (GenericRow) variantValue);
    }

    private void assertVariantJsonEquals(String expectedJson, GenericRow variantRow) throws Exception {
        assertNotNull(variantRow);
        byte[] metadataBytes = variantRow.getBinary(variantRow.getSchema().indexOf(DeltaVariantUtils.METADATA));
        byte[] valueBytes = variantRow.getBinary(variantRow.getSchema().indexOf(DeltaVariantUtils.VALUE));
        String actualJson = DeltaVariantUtils.deserializeToJsonString(metadataBytes, valueBytes);
        assertEquals(MAPPER.readTree(expectedJson), MAPPER.readTree(actualJson));
    }

    private Schema buildVariantRecordSchema() {
        Schema variantSchema = Schema.createRecord("variant_value", null, "", false);
        variantSchema.setFields(List.of(
            new Schema.Field("metadata", Schema.create(Schema.Type.BYTES), null, null),
            new Schema.Field("value", Schema.create(Schema.Type.BYTES), null, null)));
        new LogicalType("variant").addToSchema(variantSchema);
        variantSchema.addProp("variant-metadata-fields", "[\"age\", \"city\", \"active\", \"score\"]");

        return SchemaBuilder.record("VariantRecord")
            .fields()
            .name("variantField")
            .type()
            .unionOf()
            .nullType()
            .and()
            .type(variantSchema)
            .endUnion()
            .nullDefault()
            .endRecord();
    }
}
