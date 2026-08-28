/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.materialization.serde.utils.json;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.lakestream.ursa.materialization.serde.utils.json.schema.JsonSchema;
import java.io.IOException;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.avro.Conversions;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class AvroJsonConverterTest {

    private AvroJsonConverter converter;
    private ObjectMapper objectMapper;

    @BeforeEach
    public void setUp() {
        converter = new AvroJsonConverter();
        objectMapper = new ObjectMapper();
    }

    @Test
    public void testConvertSimpleRecordWithoutLogicalTypes() throws IOException {
        // Create a simple schema without logical types
        Schema schema = SchemaBuilder.record("TestRecord")
                .fields()
                .name("id").type().intType().noDefault()
                .name("name").type().stringType().noDefault()
                .name("active").type().booleanType().noDefault()
                .endRecord();

        GenericRecord record = new GenericData.Record(schema);
        record.put("id", 123);
        record.put("name", "John Doe");
        record.put("active", true);

        String jsonString = converter.convertToJsonString(record);
        Map<String, Object> result = objectMapper.readValue(jsonString, new TypeReference<Map<String, Object>>() {});

        assertEquals(123, result.get("id"));
        assertEquals("John Doe", result.get("name"));
        assertEquals(true, result.get("active"));
    }

    @Test
    public void testConvertRecordWithDateLogicalType() throws IOException {
        // Create schema with date logical type
        Schema dateSchema = LogicalTypes.date().addToSchema(Schema.create(Schema.Type.INT));
        Schema schema = SchemaBuilder.record("TestRecord")
                .fields()
                .name("id").type().intType().noDefault()
                .name("birthDate").type(dateSchema).noDefault()
                .endRecord();

        GenericRecord record = new GenericData.Record(schema);
        LocalDate testDate = LocalDate.of(1990, 5, 15);
        // Convert LocalDate to days since epoch (as expected by Avro date logical type)
        int daysSinceEpoch = (int) testDate.toEpochDay();
        record.put("id", 1);
        record.put("birthDate", daysSinceEpoch);

        String jsonString = converter.convertToJsonString(record);
        Map<String, Object> result = objectMapper.readValue(jsonString, new TypeReference<Map<String, Object>>() {});

        assertEquals(1, result.get("id"));
        assertEquals("1990-05-15", result.get("birthDate"));
    }

    @Test
    public void testConvertRecordWithTimeMillisLogicalType() throws IOException {
        // Create schema with time-millis logical type
        Schema timeSchema = LogicalTypes.timeMillis().addToSchema(Schema.create(Schema.Type.INT));
        Schema schema = SchemaBuilder.record("TestRecord")
                .fields()
                .name("id").type().intType().noDefault()
                .name("eventTime").type(timeSchema).noDefault()
                .endRecord();

        GenericRecord record = new GenericData.Record(schema);
        LocalTime testTime = LocalTime.of(14, 30, 45, 123_000_000); // 14:30:45.123
        int millisOfDay = (int) (testTime.toNanoOfDay() / 1_000_000); // Convert to milliseconds
        record.put("id", 1);
        record.put("eventTime", millisOfDay);

        String jsonString = converter.convertToJsonString(record);
        Map<String, Object> result = objectMapper.readValue(jsonString, new TypeReference<Map<String, Object>>() {});

        assertEquals(1, result.get("id"));
        assertEquals("14:30:45.123", result.get("eventTime"));
    }

    @Test
    public void testConvertRecordWithTimeMicrosLogicalType() throws IOException {
        // Create schema with time-micros logical type
        Schema timeSchema = LogicalTypes.timeMicros().addToSchema(Schema.create(Schema.Type.LONG));
        Schema schema = SchemaBuilder.record("TestRecord")
                .fields()
                .name("id").type().intType().noDefault()
                .name("eventTime").type(timeSchema).noDefault()
                .endRecord();

        GenericRecord record = new GenericData.Record(schema);
        LocalTime testTime = LocalTime.of(14, 30, 45, 123_456_000); // 14:30:45.123456
        long microsOfDay = testTime.toNanoOfDay() / 1_000; // Convert to microseconds
        record.put("id", 1);
        record.put("eventTime", microsOfDay);

        String jsonString = converter.convertToJsonString(record);
        Map<String, Object> result = objectMapper.readValue(jsonString, new TypeReference<Map<String, Object>>() {});

        assertEquals(1, result.get("id"));
        assertEquals("14:30:45.123456", result.get("eventTime"));
    }

    @Test
    public void testConvertRecordWithTimestampMillisLogicalType() throws IOException {
        // Create schema with timestamp-millis logical type
        Schema tsSchema = LogicalTypes.timestampMillis().addToSchema(Schema.create(Schema.Type.LONG));
        Schema schema = SchemaBuilder.record("TestRecord")
                .fields()
                .name("id").type().intType().noDefault()
                .name("timestamp").type(tsSchema).noDefault()
                .endRecord();

        GenericRecord record = new GenericData.Record(schema);
        LocalDateTime testDateTime = LocalDateTime.of(2023, 6, 15, 14, 30, 45, 123_000_000);
        long millis = testDateTime.atZone(java.time.ZoneOffset.UTC).toInstant().toEpochMilli();
        record.put("id", 1);
        record.put("timestamp", millis);

        String jsonString = converter.convertToJsonString(record);
        Map<String, Object> result = objectMapper.readValue(jsonString, new TypeReference<Map<String, Object>>() {});

        assertEquals(1, result.get("id"));
        // The result should be in ISO format
        assertTrue(result.get("timestamp").toString().startsWith("2023-06-15T14:30:45"));
    }

    @Test
    public void testConvertRecordWithDecimalLogicalType() throws IOException {
        // Create schema with decimal logical type
        Schema decimalSchema = LogicalTypes.decimal(10, 2).addToSchema(Schema.create(Schema.Type.BYTES));
        Schema schema = SchemaBuilder.record("TestRecord")
                .fields()
                .name("id").type().intType().noDefault()
                .name("amount").type(decimalSchema).noDefault()
                .endRecord();

        GenericRecord record = new GenericData.Record(schema);
        BigDecimal testDecimal = new BigDecimal("123.45");
        Conversions.DecimalConversion decimalConversion = new Conversions.DecimalConversion();
        Object byteBuffer = decimalConversion.toBytes(testDecimal, decimalSchema,
                LogicalTypes.decimal(10, 2));

        record.put("id", 1);
        record.put("amount", byteBuffer);

        String jsonString = converter.convertToJsonString(record);
        Map<String, Object> result = objectMapper.readValue(jsonString, new TypeReference<Map<String, Object>>() {});

        assertEquals(1, result.get("id"));
        assertEquals(testDecimal.doubleValue(), Double.parseDouble(result.get("amount").toString()), 0.001);
    }

    @Test
    public void testConvertRecordWithUuidLogicalType() throws IOException {
        // Create schema with uuid logical type
        Schema uuidSchema = LogicalTypes.uuid().addToSchema(Schema.create(Schema.Type.STRING));
        Schema schema = SchemaBuilder.record("TestRecord")
                .fields()
                .name("id").type().intType().noDefault()
                .name("uuid").type(uuidSchema).noDefault()
                .endRecord();

        GenericRecord record = new GenericData.Record(schema);
        String testUuid = "550e8400-e29b-41d4-a716-446655440000";
        record.put("id", 1);
        record.put("uuid", testUuid);

        String jsonString = converter.convertToJsonString(record);
        Map<String, Object> result = objectMapper.readValue(jsonString, new TypeReference<Map<String, Object>>() {});

        assertEquals(1, result.get("id"));
        assertEquals(testUuid, result.get("uuid"));
    }

    @Test
    public void testConvertRecordWithNestedRecord() throws IOException {
        // Create nested schema
        Schema nestedSchema = SchemaBuilder.record("NestedRecord")
                .fields()
                .name("nestedField").type().stringType().noDefault()
                .name("nestedInt").type().intType().noDefault()
                .endRecord();

        Schema schema = SchemaBuilder.record("TestRecord")
                .fields()
                .name("id").type().intType().noDefault()
                .name("nested").type(nestedSchema).noDefault()
                .endRecord();

        GenericRecord nestedRecord = new GenericData.Record(nestedSchema);
        nestedRecord.put("nestedField", "nestedValue");
        nestedRecord.put("nestedInt", 42);

        GenericRecord record = new GenericData.Record(schema);
        record.put("id", 1);
        record.put("nested", nestedRecord);

        String jsonString = converter.convertToJsonString(record);
        Map<String, Object> result = objectMapper.readValue(jsonString, new TypeReference<Map<String, Object>>() {});

        assertEquals(1, result.get("id"));

        @SuppressWarnings("unchecked")
        Map<String, Object> nestedResult = (Map<String, Object>) result.get("nested");
        assertEquals("nestedValue", nestedResult.get("nestedField"));
        assertEquals(42, nestedResult.get("nestedInt"));
    }

    @Test
    public void testConvertRecordWithArrayField() throws IOException {
        Schema arraySchema = Schema.createArray(Schema.create(Schema.Type.STRING));
        Schema schema = SchemaBuilder.record("TestRecord")
                .fields()
                .name("id").type().intType().noDefault()
                .name("tags").type(arraySchema).noDefault()
                .endRecord();

        GenericRecord record = new GenericData.Record(schema);
        record.put("id", 1);
        record.put("tags", Arrays.asList("tag1", "tag2", "tag3"));

        String jsonString = converter.convertToJsonString(record);
        Map<String, Object> result = objectMapper.readValue(jsonString, new TypeReference<Map<String, Object>>() {});

        assertEquals(1, result.get("id"));

        @SuppressWarnings("unchecked")
        List<String> tags = (List<String>) result.get("tags");
        assertEquals(Arrays.asList("tag1", "tag2", "tag3"), tags);
    }

    @Test
    public void testConvertRecordWithMapField() throws IOException {
        Schema mapSchema = Schema.createMap(Schema.create(Schema.Type.INT));
        Schema schema = SchemaBuilder.record("TestRecord")
                .fields()
                .name("id").type().intType().noDefault()
                .name("properties").type(mapSchema).noDefault()
                .endRecord();

        GenericRecord record = new GenericData.Record(schema);
        Map<Utf8, Integer> properties = new HashMap<>();
        properties.put(new Utf8("prop1"), 100);
        properties.put(new Utf8("prop2"), 200);
        record.put("id", 1);
        record.put("properties", properties);

        String jsonString = converter.convertToJsonString(record);
        Map<String, Object> result = objectMapper.readValue(jsonString, new TypeReference<Map<String, Object>>() {});

        assertEquals(1, result.get("id"));

        @SuppressWarnings("unchecked")
        Map<String, Object> props = (Map<String, Object>) result.get("properties");
        assertEquals(Integer.valueOf(100), props.get("prop1"));
        assertEquals(Integer.valueOf(200), props.get("prop2"));
    }

    @Test
    public void testConvertRecordWithMixedLogicalAndRegularTypes() throws IOException {
        // Schema with mixed logical and regular types
        Schema dateSchema = LogicalTypes.date().addToSchema(Schema.create(Schema.Type.INT));
        Schema decimalSchema = LogicalTypes.decimal(8, 2).addToSchema(Schema.create(Schema.Type.BYTES));

        Schema schema = SchemaBuilder.record("TestRecord")
                .fields()
                .name("id").type().intType().noDefault()
                .name("name").type().stringType().noDefault()
                .name("birthDate").type(dateSchema).noDefault()
                .name("salary").type(decimalSchema).noDefault()
                .name("active").type().booleanType().noDefault()
                .endRecord();

        GenericRecord record = new GenericData.Record(schema);
        LocalDate birthDate = LocalDate.of(1990, 1, 1);
        BigDecimal salary = new BigDecimal("50000.50");

        Conversions.DecimalConversion decimalConversion = new Conversions.DecimalConversion();
        Object salaryBytes = decimalConversion.toBytes(salary, decimalSchema,
                LogicalTypes.decimal(8, 2));

        record.put("id", 1);
        record.put("name", "Jane Smith");
        record.put("birthDate", (int) birthDate.toEpochDay());
        record.put("salary", salaryBytes);
        record.put("active", true);

        String jsonString = converter.convertToJsonString(record);
        Map<String, Object> result = objectMapper.readValue(jsonString, new TypeReference<Map<String, Object>>() {});

        assertEquals(1, result.get("id"));
        assertEquals("Jane Smith", result.get("name"));
        assertEquals("1990-01-01", result.get("birthDate"));
        assertEquals(salary.doubleValue(), Double.parseDouble(result.get("salary").toString()), 0.001);
        assertEquals(true, result.get("active"));
    }

    @Test
    public void testConvertNullRecord() throws IOException {
        String result = converter.convertToJsonString(null);
        assertNull(result);

        byte[] bytesResult = converter.convertToJsonBytes(null);
        assertNull(bytesResult);
    }

    @Test
    public void testToMapMethod() {
        Schema schema = SchemaBuilder.record("TestRecord")
                .fields()
                .name("id").type().intType().noDefault()
                .name("name").type().stringType().noDefault()
                .endRecord();

        GenericRecord record = new GenericData.Record(schema);
        record.put("id", 123);
        record.put("name", "Test Name");

        Map<String, Object> result = converter.toMap(record);

        assertEquals(2, result.size());
        assertEquals(123, result.get("id"));
        assertEquals("Test Name", result.get("name"));
    }

    @Test
    void testUnionRecord() throws Exception {
        Schema schema = SchemaBuilder.record("TestRecord")
            .fields()
            .name("id").type().unionOf()
            .nullType().and().intType().endUnion().nullDefault()
            .endRecord();

        GenericRecord record = new GenericData.Record(schema);
        record.put("id", 123);

        Map<String, Object> result = converter.toMap(record);
        assertEquals(1, result.size());
        assertEquals(123, result.get("id"));

        System.out.println(converter.convertToJsonString(record));
    }

    @Test
    void testRecord() throws Exception {
        var schemaString = """
             {
              "$schema": "http://json-schema.org/draft-07/schema#",
              "title": "Comprehensive JSON Schema Test",
              "type": "object",
              "properties": {
                "stringField": {
                  "type": "string"
                },
                "emailField": {
                  "type": "string",
                  "format": "email"
                },
                "uriField": {
                  "type": "string",
                  "format": "uri"
                },
                "hostnameField": {
                  "type": "string",
                  "format": "hostname"
                },
                "ipv4Field": {
                  "type": "string",
                  "format": "ipv4"
                },
                "ipv6Field": {
                  "type": "string",
                  "format": "ipv6"
                },
                "uuidField": {
                  "type": "string",
                  "format": "uuid"
                },
                "dateField": {
                  "type": "string",
                  "format": "date"
                },
                "timeField": {
                  "type": "string",
                  "format": "time"
                },
                "dateTimeField": {
                  "type": "string",
                  "format": "date-time"
                },
                "numberField": {
                  "type": "number"
                },
                "integerField": {
                  "type": "integer"
                },
                "booleanField": {
                  "type": "boolean"
                },
                "arrayField": {
                  "type": "array",
                  "items": {
                    "type": "string"
                  }
                },
                "nestedObject": {
                  "type": "object",
                  "properties": {
                    "innerString": {
                      "type": "string"
                    },
                    "innerNumber": {
                      "type": "number"
                    },
                    "deepNestedObject": {
                      "type": "object",
                      "properties": {
                        "deepString": {
                          "type": "string"
                        },
                        "deepArray": {
                          "type": "array",
                          "items": {
                            "type": "integer"
                          }
                        }
                      }
                    }
                  }
                },
                "mixedTypeArray": {
                  "type": "array",
                  "items": {
                    "anyOf": [
                      { "type": "string" },
                      { "type": "number" },
                      { "type": "object",
                        "properties": {
                          "objProp": { "type": "string" }
                        }
                      }
                    ]
                  }
                },
                "timestamp": {
                  "type": "string",
                  "format": "date-time"
                }
              },
              "required": [
                "stringField",
                "numberField",
                "integerField",
                "booleanField"
              ],
              "additionalProperties": true
            }
            """;

        JsonSchema jsonSchema = new JsonSchema(schemaString);
        System.out.println(jsonSchema.toAvroSchema());

    }
}
