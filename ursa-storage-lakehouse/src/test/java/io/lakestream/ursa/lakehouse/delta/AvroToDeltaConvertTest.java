/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.lakehouse.delta;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.delta.kernel.data.ArrayValue;
import io.delta.kernel.data.MapValue;
import io.delta.kernel.data.Row;
import io.delta.kernel.types.ArrayType;
import io.delta.kernel.types.BinaryType;
import io.delta.kernel.types.BooleanType;
import io.delta.kernel.types.ByteType;
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
import io.delta.kernel.types.VariantType;
import io.lakestream.ursa.lakehouse.utils.AvroSchemaUtilExtended;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.junit.Test;

public class AvroToDeltaConvertTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    @Test
    public void testConvertPrimitiveTypes() {
        Schema avroSchema = SchemaBuilder.record("TestRecord")
                .fields()
                .name("byteField").type().intType().noDefault()
                .name("shortField").type().intType().noDefault()
                .name("intField").type().intType().noDefault()
                .name("longField").type().longType().noDefault()
                .name("floatField").type().floatType().noDefault()
                .name("doubleField").type().doubleType().noDefault()
                .name("booleanField").type().booleanType().noDefault()
                .name("stringField").type().stringType().noDefault()
                .endRecord();

        GenericRecord avroRecord = new GenericData.Record(avroSchema);
        avroRecord.put("byteField", 10);
        avroRecord.put("shortField", 20);
        avroRecord.put("intField", 30);
        avroRecord.put("longField", 40L);
        avroRecord.put("floatField", 50.5f);
        avroRecord.put("doubleField", 60.6);
        avroRecord.put("booleanField", true);
        avroRecord.put("stringField", "test");

        StructType deltaSchema = new StructType()
                .add("byteField", ByteType.BYTE, true)
                .add("shortField", ShortType.SHORT, true)
                .add("intField", IntegerType.INTEGER, true)
                .add("longField", LongType.LONG, true)
                .add("floatField", FloatType.FLOAT, true)
                .add("doubleField", DoubleType.DOUBLE, true)
                .add("booleanField", BooleanType.BOOLEAN, true)
                .add("stringField", StringType.STRING, true);

        Row result = AvroToDeltaConvert.convert(avroRecord, deltaSchema);

        assertNotNull(result);
        assertEquals((byte) 10, result.getByte(0));
        assertEquals((short) 20, result.getShort(1));
        assertEquals(30, result.getInt(2));
        assertEquals(40L, result.getLong(3));
        assertEquals(50.5f, result.getFloat(4), 0.001f);
        assertEquals(60.6, result.getDouble(5), 0.001);
        assertEquals(true, result.getBoolean(6));
        assertEquals("test", result.getString(7));
    }

    @Test
    public void testConvertBinaryType() {
        Schema avroSchema = SchemaBuilder.record("TestRecord")
                .fields()
                .name("binaryField").type().bytesType().noDefault()
                .endRecord();

        byte[] testBytes = new byte[]{1, 2, 3, 4, 5};
        GenericRecord avroRecord = new GenericData.Record(avroSchema);
        avroRecord.put("binaryField", ByteBuffer.wrap(testBytes));

        StructType deltaSchema = new StructType()
                .add("binaryField", BinaryType.BINARY, true);

        Row result = AvroToDeltaConvert.convert(avroRecord, deltaSchema);

        assertNotNull(result);
        assertArrayEquals(testBytes, result.getBinary(0));
    }

    @Test
    public void testSingleElementUnion() {
        Schema avroSchema = Schema.createRecord("Event", "", "", false,
            List.of(
                new Schema.Field("eventId",
                    Schema.createUnion(Schema.create(Schema.Type.STRING)), "", null),
                new Schema.Field("count",
                    Schema.createUnion(Schema.create(Schema.Type.INT)), "", null)
            ));

        StructType deltaSchema = AvroSchemaUtilExtended.toDelta(avroSchema, false);

        assertEquals(StringType.STRING, deltaSchema.at(0).getDataType());
        assertEquals(IntegerType.INTEGER, deltaSchema.at(1).getDataType());
        assertEquals(false, deltaSchema.at(0).isNullable());
        assertEquals(false, deltaSchema.at(1).isNullable());

        GenericRecord avroRecord = new GenericData.Record(avroSchema);
        avroRecord.put("eventId", "test-123");
        avroRecord.put("count", 42);

        Row result = AvroToDeltaConvert.convert(avroRecord, deltaSchema);

        assertNotNull(result);
        assertEquals("test-123", result.getString(0));
        assertEquals(42, result.getInt(1));
    }

    @Test
    public void testConvertDecimalType() {
        Schema decimalSchema = LogicalTypes.decimal(10, 2).addToSchema(SchemaBuilder.builder().bytesType());
        Schema avroSchema = SchemaBuilder.record("TestRecord")
                .fields()
                .name("decimalField").type(decimalSchema).noDefault()
                .endRecord();

        BigDecimal testDecimal = new BigDecimal("123.45");
        byte[] decimalBytes = testDecimal.unscaledValue().toByteArray();

        GenericRecord avroRecord = new GenericData.Record(avroSchema);
        avroRecord.put("decimalField", ByteBuffer.wrap(decimalBytes));

        StructType deltaSchema = new StructType()
                .add("decimalField", new DecimalType(10, 2), true);

        Row result = AvroToDeltaConvert.convert(avroRecord, deltaSchema);

        assertNotNull(result);
        assertEquals(testDecimal, result.getDecimal(0));
    }

    @Test
    public void testConvertTimestampMillis() {
        Schema timestampSchema = LogicalTypes.timestampMillis().addToSchema(SchemaBuilder.builder().longType());
        Schema avroSchema = SchemaBuilder.record("TestRecord")
                .fields()
                .name("timestampField").type(timestampSchema).noDefault()
                .endRecord();

        long timestampMs = 1640000000000L;
        GenericRecord avroRecord = new GenericData.Record(avroSchema);
        avroRecord.put("timestampField", timestampMs);

        StructType deltaSchema = new StructType()
                .add("timestampField", LongType.LONG, true);

        Row result = AvroToDeltaConvert.convert(avroRecord, deltaSchema);

        assertNotNull(result);
        assertEquals(timestampMs * 1000, result.getLong(0));
    }

    @Test
    public void testConvertTimestampMicros() {
        Schema timestampSchema = LogicalTypes.timestampMicros().addToSchema(SchemaBuilder.builder().longType());
        Schema avroSchema = SchemaBuilder.record("TestRecord")
                .fields()
                .name("timestampField").type(timestampSchema).noDefault()
                .endRecord();

        long timestampUs = 1640000000000000L;
        GenericRecord avroRecord = new GenericData.Record(avroSchema);
        avroRecord.put("timestampField", timestampUs);

        StructType deltaSchema = new StructType()
                .add("timestampField", LongType.LONG, true);

        Row result = AvroToDeltaConvert.convert(avroRecord, deltaSchema);

        assertNotNull(result);
    }

    @Test
    public void testConvertTimestampNTZ() {
        Schema timestampNtzSchema = LogicalTypes.localTimestampMicros().addToSchema(SchemaBuilder.builder().longType());
        Schema avroSchema = SchemaBuilder.record("TestRecord")
                .fields()
                .name("timestampNtzField").type(timestampNtzSchema).noDefault()
                .endRecord();

        long timestampUs = 1640000000000000L;  // microseconds
        GenericRecord avroRecord = new GenericData.Record(avroSchema);
        avroRecord.put("timestampNtzField", timestampUs);

        StructType deltaSchema = new StructType()
                .add("timestampNtzField", TimestampNTZType.TIMESTAMP_NTZ, true);

        Row result = AvroToDeltaConvert.convert(avroRecord, deltaSchema);

        assertNotNull(result);
        // Avro local-timestamp-micros is in microseconds, Delta TimestampNTZ also uses microseconds
        assertEquals(timestampUs, result.getLong(0));
    }

    @Test
    public void testConvertDate() {
        Schema dateSchema = LogicalTypes.date().addToSchema(SchemaBuilder.builder().intType());
        Schema avroSchema = SchemaBuilder.record("TestRecord")
                .fields()
                .name("dateField").type(dateSchema).noDefault()
                .endRecord();

        int daysSinceEpoch = 18250;
        GenericRecord avroRecord = new GenericData.Record(avroSchema);
        avroRecord.put("dateField", daysSinceEpoch);

        StructType deltaSchema = new StructType()
                .add("dateField", IntegerType.INTEGER, true);

        Row result = AvroToDeltaConvert.convert(avroRecord, deltaSchema);

        assertNotNull(result);
        assertEquals(daysSinceEpoch, result.getInt(0));
    }

    @Test
    public void testConvertUUID() {
        Schema uuidFixedSchema = SchemaBuilder.builder().fixed("uuid").size(16);
        Schema uuidSchema = LogicalTypes.uuid().addToSchema(uuidFixedSchema);
        Schema avroSchema = SchemaBuilder.record("TestRecord")
                .fields()
                .name("uuidField").type(uuidSchema).noDefault()
                .endRecord();

        UUID testUUID = UUID.randomUUID();
        ByteBuffer buffer = ByteBuffer.allocate(16);
        buffer.putLong(testUUID.getMostSignificantBits());
        buffer.putLong(testUUID.getLeastSignificantBits());
        buffer.flip();

        GenericData.Fixed fixedUUID = new GenericData.Fixed(uuidFixedSchema, buffer.array());

        GenericRecord avroRecord = new GenericData.Record(avroSchema);
        avroRecord.put("uuidField", fixedUUID);

        StructType deltaSchema = new StructType()
                .add("uuidField", StringType.STRING, true);

        Row result = AvroToDeltaConvert.convert(avroRecord, deltaSchema);

        assertNotNull(result);
        assertEquals(testUUID.toString(), result.getString(0));
    }

    @Test
    public void testConvertArrayType() {
        Schema arraySchema = SchemaBuilder.array().items().intType();
        Schema avroSchema = SchemaBuilder.record("TestRecord")
                .fields()
                .name("arrayField").type(arraySchema).noDefault()
                .endRecord();

        List<Integer> testArray = Arrays.asList(1, 2, 3, 4, 5);
        GenericRecord avroRecord = new GenericData.Record(avroSchema);
        avroRecord.put("arrayField", testArray);

        StructType deltaSchema = new StructType()
                .add("arrayField", new ArrayType(IntegerType.INTEGER, true), true);

        Row result = AvroToDeltaConvert.convert(avroRecord, deltaSchema);

        assertNotNull(result);
        assertTrue(result.getArray(0) instanceof ArrayValue);
    }

    @Test
    public void testConvertMapType() {
        Schema mapSchema = SchemaBuilder.map().values().intType();
        Schema avroSchema = SchemaBuilder.record("TestRecord")
                .fields()
                .name("mapField").type(mapSchema).noDefault()
                .endRecord();

        Map<String, Integer> testMap = new HashMap<>();
        testMap.put("key1", 1);
        testMap.put("key2", 2);

        GenericRecord avroRecord = new GenericData.Record(avroSchema);
        avroRecord.put("mapField", testMap);

        StructType deltaSchema = new StructType()
                .add("mapField", new MapType(StringType.STRING, IntegerType.INTEGER, true), true);

        Row result = AvroToDeltaConvert.convert(avroRecord, deltaSchema);

        assertNotNull(result);
        assertTrue(result.getMap(0) instanceof MapValue);
    }

    @Test
    public void testConvertNestedStructType() {
        Schema innerSchema = SchemaBuilder.record("InnerRecord")
                .fields()
                .name("innerValue").type().stringType().noDefault()
                .endRecord();

        Schema avroSchema = SchemaBuilder.record("OuterRecord")
                .fields()
                .name("nestedField").type(innerSchema).noDefault()
                .endRecord();

        GenericRecord innerRecord = new GenericData.Record(innerSchema);
        innerRecord.put("innerValue", "nested");

        GenericRecord avroRecord = new GenericData.Record(avroSchema);
        avroRecord.put("nestedField", innerRecord);

        StructType innerDeltaSchema = new StructType()
                .add("innerValue", StringType.STRING, true);

        StructType deltaSchema = new StructType()
                .add("nestedField", innerDeltaSchema, true);

        Row result = AvroToDeltaConvert.convert(avroRecord, deltaSchema);

        assertNotNull(result);
        Row nestedRow = result.getStruct(0);
        assertEquals("nested", nestedRow.getString(0));
    }

    @Test
    public void testConvertWithNullValues() {
        Schema avroSchema = SchemaBuilder.record("TestRecord")
                .fields()
                .name("field1").type().optional().intType()
                .name("field2").type().optional().stringType()
                .endRecord();

        GenericRecord avroRecord = new GenericData.Record(avroSchema);
        avroRecord.put("field1", null);
        avroRecord.put("field2", null);

        StructType deltaSchema = new StructType()
                .add("field1", IntegerType.INTEGER, true)
                .add("field2", StringType.STRING, true);

        Row result = AvroToDeltaConvert.convert(avroRecord, deltaSchema);

        assertNotNull(result);
        assertTrue(result.isNullAt(0));
        assertTrue(result.isNullAt(1));
    }

    @Test
    public void testConvertWithMissingFields() {
        Schema avroSchema = SchemaBuilder.record("TestRecord")
                .fields()
                .name("field1").type().intType().noDefault()
                .endRecord();

        GenericRecord avroRecord = new GenericData.Record(avroSchema);
        avroRecord.put("field1", 42);

        StructType deltaSchema = new StructType()
                .add("field1", IntegerType.INTEGER, true)
                .add("field2", StringType.STRING, true);

        Row result = AvroToDeltaConvert.convert(avroRecord, deltaSchema);

        assertNotNull(result);
        assertEquals(42, result.getInt(0));
        assertTrue(result.isNullAt(1));
    }

    @Test
    public void testConvertTimeMillis() {
        Schema timeSchema = LogicalTypes.timeMillis().addToSchema(SchemaBuilder.builder().intType());
        Schema avroSchema = SchemaBuilder.record("TestRecord")
                .fields()
                .name("timeField").type(timeSchema).noDefault()
                .endRecord();

        int timeMs = 3600000;
        GenericRecord avroRecord = new GenericData.Record(avroSchema);
        avroRecord.put("timeField", timeMs);

        StructType deltaSchema = new StructType()
                .add("timeField", LongType.LONG, true);

        Row result = AvroToDeltaConvert.convert(avroRecord, deltaSchema);

        assertNotNull(result);
    }

    @Test
    public void testConvertComplexNestedStructure() {
        Schema innerArraySchema = SchemaBuilder.array().items().intType();
        Schema innerSchema = SchemaBuilder.record("InnerRecord")
                .fields()
                .name("arrayField").type(innerArraySchema).noDefault()
                .name("stringField").type().stringType().noDefault()
                .endRecord();

        Schema outerSchema = SchemaBuilder.record("OuterRecord")
                .fields()
                .name("nestedStruct").type(innerSchema).noDefault()
                .name("outerInt").type().intType().noDefault()
                .endRecord();

        List<Integer> innerArray = Arrays.asList(10, 20, 30);
        GenericRecord innerRecord = new GenericData.Record(innerSchema);
        innerRecord.put("arrayField", innerArray);
        innerRecord.put("stringField", "test");

        GenericRecord outerRecord = new GenericData.Record(outerSchema);
        outerRecord.put("nestedStruct", innerRecord);
        outerRecord.put("outerInt", 100);

        StructType innerDeltaSchema = new StructType()
                .add("arrayField", new ArrayType(IntegerType.INTEGER, true), true)
                .add("stringField", StringType.STRING, true);

        StructType outerDeltaSchema = new StructType()
                .add("nestedStruct", innerDeltaSchema, true)
                .add("outerInt", IntegerType.INTEGER, true);

        Row result = AvroToDeltaConvert.convert(outerRecord, outerDeltaSchema);

        assertNotNull(result);
        assertEquals(100, result.getInt(1));
    }

    @Test
    public void testConvertLocalTimestampMillis() {
        Schema localTimestampSchema = LogicalTypes.localTimestampMillis().addToSchema(SchemaBuilder.builder().longType());
        Schema avroSchema = SchemaBuilder.record("TestRecord")
                .fields()
                .name("localTimestampField").type(localTimestampSchema).noDefault()
                .endRecord();

        long localTimestampMs = 1640000000000L;
        GenericRecord avroRecord = new GenericData.Record(avroSchema);
        avroRecord.put("localTimestampField", localTimestampMs);

        StructType deltaSchema = new StructType()
                .add("localTimestampField", LongType.LONG, true);

        Row result = AvroToDeltaConvert.convert(avroRecord, deltaSchema);

        assertNotNull(result);
        assertEquals(localTimestampMs * 1000, result.getLong(0));
    }

    @Test
    public void testConvertDateType() {
        Schema dateSchema = LogicalTypes.date().addToSchema(SchemaBuilder.builder().intType());
        Schema avroSchema = SchemaBuilder.record("TestRecord")
                .fields()
                .name("dateField").type(dateSchema).noDefault()
                .endRecord();

        // Days since Unix epoch (1970-01-01)
        // 18628 days = 2021-01-01
        int daysSinceEpoch = 18628;
        GenericRecord avroRecord = new GenericData.Record(avroSchema);
        avroRecord.put("dateField", daysSinceEpoch);

        StructType deltaSchema = new StructType()
                .add("dateField", IntegerType.INTEGER, true);

        Row result = AvroToDeltaConvert.convert(avroRecord, deltaSchema);

        assertNotNull(result);
        assertEquals(daysSinceEpoch, result.getInt(0));
    }

    @Test
    public void testConvertTimeMillisType() {
        Schema timeMillisSchema = LogicalTypes.timeMillis().addToSchema(SchemaBuilder.builder().intType());
        Schema avroSchema = SchemaBuilder.record("TestRecord")
                .fields()
                .name("timeField").type(timeMillisSchema).noDefault()
                .endRecord();

        // Milliseconds since midnight
        // 46800000 ms = 13:00:00
        int timeMs = 46800000;
        GenericRecord avroRecord = new GenericData.Record(avroSchema);
        avroRecord.put("timeField", timeMs);

        StructType deltaSchema = new StructType()
                .add("timeField", LongType.LONG, true);

        Row result = AvroToDeltaConvert.convert(avroRecord, deltaSchema);

        assertNotNull(result);
        // Result should be in microseconds
        assertTrue(result.getLong(0) > 0);
    }

    @Test
    public void testConvertTimeMicrosType() {
        Schema timeMicrosSchema = LogicalTypes.timeMicros().addToSchema(SchemaBuilder.builder().longType());
        Schema avroSchema = SchemaBuilder.record("TestRecord")
                .fields()
                .name("timeField").type(timeMicrosSchema).noDefault()
                .endRecord();

        // Microseconds since midnight
        // 46800000000 micros = 13:00:00
        long timeMicros = 46800000000L;
        GenericRecord avroRecord = new GenericData.Record(avroSchema);
        avroRecord.put("timeField", timeMicros);

        StructType deltaSchema = new StructType()
                .add("timeField", LongType.LONG, true);

        Row result = AvroToDeltaConvert.convert(avroRecord, deltaSchema);

        assertNotNull(result);
        assertTrue(result.getLong(0) > 0);
    }

    @Test
    public void testConvertTimestampWithTimezone() {
        Schema timestampSchema = LogicalTypes.timestampMicros().addToSchema(SchemaBuilder.builder().longType());
        Schema avroSchema = SchemaBuilder.record("TestRecord")
                .fields()
                .name("timestampField").type(timestampSchema).noDefault()
                .endRecord();

        // Microseconds since epoch
        long timestampMicros = 1640000000000000L;  // 2021-12-20 10:26:40 UTC
        GenericRecord avroRecord = new GenericData.Record(avroSchema);
        avroRecord.put("timestampField", timestampMicros);

        StructType deltaSchema = new StructType()
                .add("timestampField", LongType.LONG, true);

        Row result = AvroToDeltaConvert.convert(avroRecord, deltaSchema);

        assertNotNull(result);
        assertTrue(result.getLong(0) > 0);
    }

    @Test
    public void testConvertMultipleTimestampTypes() {
        Schema avroSchema = SchemaBuilder.record("TimestampRecord")
                .fields()
                .name("dateField").type(LogicalTypes.date().addToSchema(SchemaBuilder.builder().intType())).noDefault()
                .name("timeMillisField").type(LogicalTypes.timeMillis().addToSchema(SchemaBuilder.builder().intType())).noDefault()
                .name("timestampMillisField").type(LogicalTypes.timestampMillis().addToSchema(SchemaBuilder.builder().longType())).noDefault()
                .name("localTimestampMicrosField").type(LogicalTypes.localTimestampMicros().addToSchema(SchemaBuilder.builder().longType())).noDefault()
                .endRecord();

        GenericRecord avroRecord = new GenericData.Record(avroSchema);
        avroRecord.put("dateField", 18628);
        avroRecord.put("timeMillisField", 46800000);
        avroRecord.put("timestampMillisField", 1640000000000L);
        avroRecord.put("localTimestampMicrosField", 1640000000000000L);

        StructType deltaSchema = new StructType()
                .add("dateField", IntegerType.INTEGER, true)
                .add("timeMillisField", LongType.LONG, true)
                .add("timestampMillisField", LongType.LONG, true)
                .add("localTimestampMicrosField", TimestampNTZType.TIMESTAMP_NTZ, true);

        Row result = AvroToDeltaConvert.convert(avroRecord, deltaSchema);

        assertNotNull(result);
        assertEquals(18628, result.getInt(0));
        assertTrue(result.getLong(1) > 0);  // time-millis converted
        assertEquals(1640000000000L * 1000, result.getLong(2));  // timestamp-millis to microseconds
        assertEquals(1640000000000000L, result.getLong(3));  // local-timestamp-micros unchanged
    }

    @Test
    public void testConvertDateWithNullValue() {
        Schema dateSchema = LogicalTypes.date().addToSchema(SchemaBuilder.builder().intType());
        Schema avroSchema = SchemaBuilder.record("TestRecord")
                .fields()
                .name("dateField").type().optional().type(dateSchema).endRecord();

        GenericRecord avroRecord = new GenericData.Record(avroSchema);
        avroRecord.put("dateField", null);

        StructType deltaSchema = new StructType()
                .add("dateField", IntegerType.INTEGER, true);

        Row result = AvroToDeltaConvert.convert(avroRecord, deltaSchema);

        assertNotNull(result);
        assertTrue(result.isNullAt(0));
    }

    @Test
    public void testConvertTimestampMillisWithNullValue() {
        Schema timestampSchema = LogicalTypes.timestampMillis().addToSchema(SchemaBuilder.builder().longType());
        Schema avroSchema = SchemaBuilder.record("TestRecord")
                .fields()
                .name("timestampField").type().optional().type(timestampSchema).endRecord();

        GenericRecord avroRecord = new GenericData.Record(avroSchema);
        avroRecord.put("timestampField", null);

        StructType deltaSchema = new StructType()
                .add("timestampField", LongType.LONG, true);

        Row result = AvroToDeltaConvert.convert(avroRecord, deltaSchema);

        assertNotNull(result);
        assertTrue(result.isNullAt(0));
    }

    @Test
    public void testConvertLocalTimestampMicrosEdgeCases() {
        Schema localTimestampSchema = LogicalTypes.localTimestampMicros().addToSchema(SchemaBuilder.builder().longType());
        Schema avroSchema = SchemaBuilder.record("TestRecord")
                .fields()
                .name("epochField").type(localTimestampSchema).noDefault()
                .name("minField").type(localTimestampSchema).noDefault()
                .name("maxField").type(localTimestampSchema).noDefault()
                .endRecord();

        // Test edge cases: epoch (0), minimum, and maximum timestamps
        GenericRecord avroRecord = new GenericData.Record(avroSchema);
        avroRecord.put("epochField", 0L);
        avroRecord.put("minField", 1L);
        avroRecord.put("maxField", Long.MAX_VALUE / 2);  // Avoid overflow

        StructType deltaSchema = new StructType()
                .add("epochField", TimestampNTZType.TIMESTAMP_NTZ, true)
                .add("minField", TimestampNTZType.TIMESTAMP_NTZ, true)
                .add("maxField", TimestampNTZType.TIMESTAMP_NTZ, true);

        Row result = AvroToDeltaConvert.convert(avroRecord, deltaSchema);

        assertNotNull(result);
        assertEquals(0L, result.getLong(0));
        assertEquals(1L, result.getLong(1));
        assertEquals(Long.MAX_VALUE / 2, result.getLong(2));
    }

    @Test
    public void testConvertTimestampMillisEdgeCases() {
        Schema timestampSchema = LogicalTypes.timestampMillis().addToSchema(SchemaBuilder.builder().longType());
        Schema avroSchema = SchemaBuilder.record("TestRecord")
                .fields()
                .name("epochField").type(timestampSchema).noDefault()
                .name("testField").type(timestampSchema).noDefault()
                .endRecord();

        GenericRecord avroRecord = new GenericData.Record(avroSchema);
        avroRecord.put("epochField", 0L);
        avroRecord.put("testField", 1640000000000L);

        StructType deltaSchema = new StructType()
                .add("epochField", LongType.LONG, true)
                .add("testField", LongType.LONG, true);

        Row result = AvroToDeltaConvert.convert(avroRecord, deltaSchema);

        assertNotNull(result);
        assertEquals(0L, result.getLong(0));
        assertEquals(1640000000000L * 1000, result.getLong(1));
    }

    @Test
    public void testConvertLocalDateType() {
        // Using Avro's reflect API to create schema from LocalDate.class
        Schema avroSchema = SchemaBuilder.record("TestRecord")
                .fields()
                .name("localDateField").type(LogicalTypes.date().addToSchema(SchemaBuilder.builder().intType())).noDefault()
                .endRecord();

        // Create a LocalDate and convert to days since epoch for Avro
        LocalDate testDate = LocalDate.of(2021, 12, 20);  // 2021-12-20
        int daysSinceEpoch = (int) testDate.toEpochDay();

        GenericRecord avroRecord = new GenericData.Record(avroSchema);
        avroRecord.put("localDateField", daysSinceEpoch);

        StructType deltaSchema = new StructType()
                .add("localDateField", IntegerType.INTEGER, true);

        Row result = AvroToDeltaConvert.convert(avroRecord, deltaSchema);

        assertNotNull(result);
        assertEquals(daysSinceEpoch, result.getInt(0));
        // Verify it's the correct date
        assertEquals(testDate, LocalDate.ofEpochDay(result.getInt(0)));
    }

    @Test
    public void testConvertLocalDateTypeMinMax() {
        Schema dateSchema = LogicalTypes.date().addToSchema(SchemaBuilder.builder().intType());
        Schema avroSchema = SchemaBuilder.record("TestRecord")
                .fields()
                .name("minDateField").type(dateSchema).noDefault()
                .name("maxDateField").type(dateSchema).noDefault()
                .name("epochDateField").type(dateSchema).noDefault()
                .endRecord();

        // Test dates at boundaries
        LocalDate minDate = LocalDate.of(1970, 1, 1);  // Unix epoch
        LocalDate maxDate = LocalDate.of(2262, 4, 11);  // Far future
        LocalDate epochDate = LocalDate.of(1970, 1, 1);

        GenericRecord avroRecord = new GenericData.Record(avroSchema);
        avroRecord.put("minDateField", (int) minDate.toEpochDay());
        avroRecord.put("maxDateField", (int) maxDate.toEpochDay());
        avroRecord.put("epochDateField", (int) epochDate.toEpochDay());

        StructType deltaSchema = new StructType()
                .add("minDateField", IntegerType.INTEGER, true)
                .add("maxDateField", IntegerType.INTEGER, true)
                .add("epochDateField", IntegerType.INTEGER, true);

        Row result = AvroToDeltaConvert.convert(avroRecord, deltaSchema);

        assertNotNull(result);
        assertEquals(minDate, LocalDate.ofEpochDay(result.getInt(0)));
        assertEquals(maxDate, LocalDate.ofEpochDay(result.getInt(1)));
        assertEquals(epochDate, LocalDate.ofEpochDay(result.getInt(2)));
    }

    @Test
    public void testConvertLocalTimeType() {
        // Test LocalTime conversion
        Schema timeMillisSchema = LogicalTypes.timeMillis().addToSchema(SchemaBuilder.builder().intType());
        Schema avroSchema = SchemaBuilder.record("TestRecord")
                .fields()
                .name("localTimeField").type(timeMillisSchema).noDefault()
                .endRecord();

        // LocalTime: 14:30:45
        LocalTime testTime = LocalTime.of(14, 30, 45);
        int timeInMillis = (int) (testTime.toNanoOfDay() / 1_000_000L);

        GenericRecord avroRecord = new GenericData.Record(avroSchema);
        avroRecord.put("localTimeField", timeInMillis);

        StructType deltaSchema = new StructType()
                .add("localTimeField", LongType.LONG, true);

        Row result = AvroToDeltaConvert.convert(avroRecord, deltaSchema);

        assertNotNull(result);
        assertTrue(result.getLong(0) > 0);
    }

    @Test
    public void testConvertLocalTimeTypeWithMicros() {
        // Test LocalTime with microseconds precision
        Schema timeMicrosSchema = LogicalTypes.timeMicros().addToSchema(SchemaBuilder.builder().longType());
        Schema avroSchema = SchemaBuilder.record("TestRecord")
                .fields()
                .name("localTimeMicrosField").type(timeMicrosSchema).noDefault()
                .endRecord();

        // LocalTime: 14:30:45.123456
        LocalTime testTime = LocalTime.of(14, 30, 45, 123456000);  // nanoseconds
        long timeInMicros = testTime.toNanoOfDay() / 1000L;

        GenericRecord avroRecord = new GenericData.Record(avroSchema);
        avroRecord.put("localTimeMicrosField", timeInMicros);

        StructType deltaSchema = new StructType()
                .add("localTimeMicrosField", LongType.LONG, true);

        Row result = AvroToDeltaConvert.convert(avroRecord, deltaSchema);

        assertNotNull(result);
        assertTrue(result.getLong(0) > 0);
    }

    @Test
    public void testConvertLocalDateTimeTypes() {
        // Test combining LocalDate and LocalTime
        Schema dateSchema = LogicalTypes.date().addToSchema(SchemaBuilder.builder().intType());
        Schema timeMillisSchema = LogicalTypes.timeMillis().addToSchema(SchemaBuilder.builder().intType());
        Schema avroSchema = SchemaBuilder.record("DateTimeRecord")
                .fields()
                .name("dateField").type(dateSchema).noDefault()
                .name("timeField").type(timeMillisSchema).noDefault()
                .endRecord();

        LocalDate testDate = LocalDate.of(2021, 12, 20);
        LocalTime testTime = LocalTime.of(14, 30, 45);

        GenericRecord avroRecord = new GenericData.Record(avroSchema);
        avroRecord.put("dateField", (int) testDate.toEpochDay());
        avroRecord.put("timeField", (int) (testTime.toNanoOfDay() / 1_000_000L));

        StructType deltaSchema = new StructType()
                .add("dateField", IntegerType.INTEGER, true)
                .add("timeField", LongType.LONG, true);

        Row result = AvroToDeltaConvert.convert(avroRecord, deltaSchema);

        assertNotNull(result);
        assertEquals(testDate, LocalDate.ofEpochDay(result.getInt(0)));
        assertTrue(result.getLong(1) > 0);
    }

    @Test
    public void testConvertLocalDateWithNullValue() {
        Schema dateSchema = LogicalTypes.date().addToSchema(SchemaBuilder.builder().intType());
        Schema avroSchema = SchemaBuilder.record("TestRecord")
                .fields()
                .name("localDateField").type().optional().type(dateSchema).endRecord();

        GenericRecord avroRecord = new GenericData.Record(avroSchema);
        avroRecord.put("localDateField", null);

        StructType deltaSchema = new StructType()
                .add("localDateField", IntegerType.INTEGER, true);

        Row result = AvroToDeltaConvert.convert(avroRecord, deltaSchema);

        assertNotNull(result);
        assertTrue(result.isNullAt(0));
    }

    @Test
    public void testConvertNestedArrayType() {
        // Test array[array[string]]
        Schema innerArraySchema = SchemaBuilder.array().items().stringType();
        Schema outerArraySchema = SchemaBuilder.array().items(innerArraySchema);
        Schema avroSchema = SchemaBuilder.record("TestRecord")
                .fields()
                .name("nestedArrayField").type(outerArraySchema).noDefault()
                .endRecord();

        List<List<String>> nestedArray = Arrays.asList(
                Arrays.asList("a", "b", "c"),
                Arrays.asList("d", "e"),
                Arrays.asList("f")
        );
        GenericRecord avroRecord = new GenericData.Record(avroSchema);
        avroRecord.put("nestedArrayField", nestedArray);

        ArrayType innerDeltaArrayType = new ArrayType(StringType.STRING, true);
        ArrayType outerDeltaArrayType = new ArrayType(innerDeltaArrayType, true);
        StructType deltaSchema = new StructType()
                .add("nestedArrayField", outerDeltaArrayType, true);

        Row result = AvroToDeltaConvert.convert(avroRecord, deltaSchema);

        assertNotNull(result);
        ArrayValue outerArray = result.getArray(0);
        assertNotNull(outerArray);
        assertEquals(3, outerArray.getSize());
    }

    @Test
    public void testConvertDeeplyNestedArrayType() {
        // Test array[array[array[int]]]
        Schema intArraySchema = SchemaBuilder.array().items().intType();
        Schema twoDArraySchema = SchemaBuilder.array().items(intArraySchema);
        Schema threeDArraySchema = SchemaBuilder.array().items(twoDArraySchema);
        Schema avroSchema = SchemaBuilder.record("TestRecord")
                .fields()
                .name("deeplyNestedArray").type(threeDArraySchema).noDefault()
                .endRecord();

        List<List<List<Integer>>> deeplyNested = Arrays.asList(
                Arrays.asList(
                        Arrays.asList(1, 2, 3),
                        Arrays.asList(4, 5)
                ),
                Arrays.asList(
                        Arrays.asList(6),
                        Arrays.asList(7, 8, 9, 10)
                )
        );
        GenericRecord avroRecord = new GenericData.Record(avroSchema);
        avroRecord.put("deeplyNestedArray", deeplyNested);

        ArrayType innerArrayType = new ArrayType(IntegerType.INTEGER, true);
        ArrayType twoDArrayType = new ArrayType(innerArrayType, true);
        ArrayType threeDArrayType = new ArrayType(twoDArrayType, true);
        StructType deltaSchema = new StructType()
                .add("deeplyNestedArray", threeDArrayType, true);

        Row result = AvroToDeltaConvert.convert(avroRecord, deltaSchema);

        assertNotNull(result);
        ArrayValue outerArray = result.getArray(0);
        assertNotNull(outerArray);
        assertEquals(2, outerArray.getSize());
    }

    @Test
    public void testConvertNestedMapType() {
        // Test map[string, map[string, int]]
        Schema innerMapSchema = SchemaBuilder.map().values().intType();
        Schema outerMapSchema = SchemaBuilder.map().values(innerMapSchema);
        Schema avroSchema = SchemaBuilder.record("TestRecord")
                .fields()
                .name("nestedMapField").type(outerMapSchema).noDefault()
                .endRecord();

        Map<String, Map<String, Integer>> nestedMap = new HashMap<>();
        Map<String, Integer> innerMap1 = new HashMap<>();
        innerMap1.put("a", 1);
        innerMap1.put("b", 2);
        Map<String, Integer> innerMap2 = new HashMap<>();
        innerMap2.put("c", 3);
        innerMap2.put("d", 4);
        nestedMap.put("first", innerMap1);
        nestedMap.put("second", innerMap2);

        GenericRecord avroRecord = new GenericData.Record(avroSchema);
        avroRecord.put("nestedMapField", nestedMap);

        MapType innerDeltaMapType = new MapType(StringType.STRING, IntegerType.INTEGER, true);
        MapType outerDeltaMapType = new MapType(StringType.STRING, innerDeltaMapType, true);
        StructType deltaSchema = new StructType()
                .add("nestedMapField", outerDeltaMapType, true);

        Row result = AvroToDeltaConvert.convert(avroRecord, deltaSchema);

        assertNotNull(result);
        MapValue outerMap = result.getMap(0);
        assertNotNull(outerMap);
    }

    @Test
    public void testConvertMixedNestedStructures() {
        // Test array[map[string, array[int]]]
        Schema intArraySchema = SchemaBuilder.array().items().intType();
        Schema mapWithArraySchema = SchemaBuilder.map().values(intArraySchema);
        Schema arrayOfMapsSchema = SchemaBuilder.array().items(mapWithArraySchema);
        Schema avroSchema = SchemaBuilder.record("TestRecord")
                .fields()
                .name("mixedNested").type(arrayOfMapsSchema).noDefault()
                .endRecord();

        List<Map<String, List<Integer>>> mixedNested = new ArrayList<>();
        Map<String, List<Integer>> map1 = new HashMap<>();
        map1.put("even", Arrays.asList(2, 4, 6));
        map1.put("odd", Arrays.asList(1, 3, 5));
        Map<String, List<Integer>> map2 = new HashMap<>();
        map2.put("prime", Arrays.asList(2, 3, 5, 7));
        mixedNested.add(map1);
        mixedNested.add(map2);

        GenericRecord avroRecord = new GenericData.Record(avroSchema);
        avroRecord.put("mixedNested", mixedNested);

        ArrayType intArrayType = new ArrayType(IntegerType.INTEGER, true);
        MapType mapWithArrayType = new MapType(StringType.STRING, intArrayType, true);
        ArrayType arrayOfMapsType = new ArrayType(mapWithArrayType, true);
        StructType deltaSchema = new StructType()
                .add("mixedNested", arrayOfMapsType, true);

        Row result = AvroToDeltaConvert.convert(avroRecord, deltaSchema);

        assertNotNull(result);
        ArrayValue outerArray = result.getArray(0);
        assertNotNull(outerArray);
        assertEquals(2, outerArray.getSize());
    }

    @Test
    public void testConvertComplexMixedStructure() {
        // Test struct containing nested arrays and maps
        Schema stringArraySchema = SchemaBuilder.array().items().stringType();
        Schema mapOfArraysSchema = SchemaBuilder.map().values(stringArraySchema);
        Schema complexSchema = SchemaBuilder.record("ComplexRecord")
                .fields()
                .name("id").type().intType().noDefault()
                .name("nestedArray").type(SchemaBuilder.array().items(stringArraySchema)).noDefault()
                .name("nestedMap").type(mapOfArraysSchema).noDefault()
                .endRecord();

        List<List<String>> nestedArray = Arrays.asList(
                Arrays.asList("x", "y"),
                Arrays.asList("z")
        );
        Map<String, List<String>> nestedMap = new HashMap<>();
        nestedMap.put("colors", Arrays.asList("red", "green", "blue"));
        nestedMap.put("sizes", Arrays.asList("small", "medium", "large"));

        GenericRecord avroRecord = new GenericData.Record(complexSchema);
        avroRecord.put("id", 123);
        avroRecord.put("nestedArray", nestedArray);
        avroRecord.put("nestedMap", nestedMap);

        ArrayType stringArrayType = new ArrayType(StringType.STRING, true);
        ArrayType arrayOfArraysType = new ArrayType(stringArrayType, true);
        MapType mapOfArraysType = new MapType(StringType.STRING, stringArrayType, true);
        StructType deltaSchema = new StructType()
                .add("id", IntegerType.INTEGER, true)
                .add("nestedArray", arrayOfArraysType, true)
                .add("nestedMap", mapOfArraysType, true);

        Row result = AvroToDeltaConvert.convert(avroRecord, deltaSchema);

        assertNotNull(result);
        assertEquals(123, result.getInt(0));
        assertNotNull(result.getArray(1));
        assertNotNull(result.getMap(2));
    }

    @Test
    public void testConvertNestedArrayWithNulls() {
        // Test array[array[string]] with null values
        Schema innerArraySchema = SchemaBuilder.array().items().stringType();
        Schema outerArraySchema = SchemaBuilder.array().items(innerArraySchema);
        Schema avroSchema = SchemaBuilder.record("TestRecord")
                .fields()
                .name("nestedArrayWithNulls").type(outerArraySchema).noDefault()
                .endRecord();

        List<List<String>> nestedArray = Arrays.asList(
                Arrays.asList("a", null, "c"),
                null,
                Arrays.asList("d", "e")
        );
        GenericRecord avroRecord = new GenericData.Record(avroSchema);
        avroRecord.put("nestedArrayWithNulls", nestedArray);

        ArrayType innerDeltaArrayType = new ArrayType(StringType.STRING, true);
        ArrayType outerDeltaArrayType = new ArrayType(innerDeltaArrayType, true);
        StructType deltaSchema = new StructType()
                .add("nestedArrayWithNulls", outerDeltaArrayType, true);

        Row result = AvroToDeltaConvert.convert(avroRecord, deltaSchema);

        assertNotNull(result);
        ArrayValue outerArray = result.getArray(0);
        assertNotNull(outerArray);
        assertEquals(3, outerArray.getSize());
    }

    @Test
    public void testConvertVariantGenericRecord() throws Exception {
        Schema streetSchema = SchemaBuilder.record("Street")
                .fields()
                .name("name").type().optional().stringType()
                .name("number").type().optional().intType()
                .endRecord();

        Schema addressSchema = SchemaBuilder.record("Address1")
                .prop("logicalType", "variant")
                .fields()
                .name("city").type().optional().stringType()
                .name("street").type(streetSchema).noDefault()
                .name("streetList").type().optional().array().items(streetSchema)
                .endRecord();

        Schema avroSchema = SchemaBuilder.record("Student")
                .fields()
                .name("addressObj1").type(addressSchema).noDefault()
                .endRecord();

        GenericRecord street = new GenericData.Record(streetSchema);
        street.put("name", "main");
        street.put("number", 12);

        GenericRecord street2 = new GenericData.Record(streetSchema);
        street2.put("name", "second");
        street2.put("number", 34);

        GenericRecord address = new GenericData.Record(addressSchema);
        address.put("city", "shanghai");
        address.put("street", street);
        address.put("streetList", List.of(street, street2));

        GenericRecord root = new GenericData.Record(avroSchema);
        root.put("addressObj1", address);

        StructType deltaSchema = new StructType()
                .add("addressObj1", VariantType.VARIANT, true);

        Row result = AvroToDeltaConvert.convert(root, deltaSchema);

        assertNotNull(result);
        Row variant = result.getStruct(0);
        String json = DeltaVariantUtils.deserializeToJsonString(variant.getBinary(1), variant.getBinary(0));
        assertEquals(MAPPER.readTree("{\"city\":\"shanghai\",\"street\":{\"name\":\"main\",\"number\":12},"
                        + "\"streetList\":[{\"name\":\"main\",\"number\":12},{\"name\":\"second\",\"number\":34}]}"),
                MAPPER.readTree(json));
    }
}
