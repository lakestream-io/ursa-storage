/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.lakehouse.iceberg;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;


public class TimestampConversionTest {

    @Test
    public void testJsonTimestampWithZone() {
        // Create Iceberg schema with timestamptz
        org.apache.iceberg.Schema icebergSchema = new org.apache.iceberg.Schema(
                Types.NestedField.required(1, "timestamp_field", Types.TimestampType.withZone())
        );

        // Create JSON data
        ObjectNode jsonNode = JsonNodeFactory.instance.objectNode();
        long timestampMicros = 1698412800000000L; // Example timestamp in microseconds
        jsonNode.put("timestamp_field", timestampMicros);

        // Convert
        Record record = JsonToIcebergConverter.convertToRecord(icebergSchema, jsonNode);

        // Verify the result is OffsetDateTime
        Object value = record.getField("timestamp_field");
        assertTrue(value instanceof OffsetDateTime, "Value should be OffsetDateTime for timestamptz");

        // Verify the value is correct
        OffsetDateTime expected = LocalDateTime.ofInstant(
                java.time.Instant.ofEpochSecond(
                        timestampMicros / 1_000_000,
                        (timestampMicros % 1_000_000) * 1000),
                ZoneOffset.UTC).atOffset(ZoneOffset.UTC);
        assertEquals(expected, value);
    }

    @Test
    public void testJsonTimestampWithoutZone() {
        // Create Iceberg schema with regular timestamp
        org.apache.iceberg.Schema icebergSchema = new org.apache.iceberg.Schema(
                Types.NestedField.required(1, "timestamp_field", Types.TimestampType.withoutZone())
        );

        // Create JSON data
        ObjectNode jsonNode = JsonNodeFactory.instance.objectNode();
        long timestampMicros = 1698412800000000L;
        jsonNode.put("timestamp_field", timestampMicros);

        // Convert
        Record record = JsonToIcebergConverter.convertToRecord(icebergSchema, jsonNode);

        // Verify the result is LocalDateTime
        Object value = record.getField("timestamp_field");
        assertTrue(value instanceof LocalDateTime, "Value should be LocalDateTime for timestamp without zone");

        // Verify the value is correct
        LocalDateTime expected = LocalDateTime.ofInstant(
                java.time.Instant.ofEpochSecond(
                        timestampMicros / 1_000_000,
                        (timestampMicros % 1_000_000) * 1000),
                ZoneOffset.UTC);
        assertEquals(expected, value);
    }

    @Test
    public void testAvroTimestampWithZone() {
        // Create Avro schema with timestamp-millis
        Schema avroSchema = Schema.createRecord("test", null, null, false);
        Schema timestampMillisSchema = LogicalTypes.timestampMillis()
                .addToSchema(Schema.create(Schema.Type.LONG));
        avroSchema.setFields(java.util.List.of(
                new Schema.Field("timestamp_field", timestampMillisSchema)));

        // Create Iceberg schema with timestamptz
        org.apache.iceberg.Schema icebergSchema = new org.apache.iceberg.Schema(
                Types.NestedField.required(1, "timestamp_field", Types.TimestampType.withZone())
        );

        // Create Avro record
        GenericRecord avroRecord = new GenericData.Record(avroSchema);
        long timestampMillis = 1698412800000L; // Timestamp in milliseconds
        avroRecord.put("timestamp_field", timestampMillis);

        // Convert
        Record record = AvroToIcebergConverter.convert(avroRecord, icebergSchema);

        // Verify the result is OffsetDateTime
        Object value = record.getField("timestamp_field");
        assertTrue(value instanceof OffsetDateTime, "Value should be OffsetDateTime for timestamptz");

        // Verify the value
        OffsetDateTime expected = LocalDateTime.ofInstant(
                java.time.Instant.ofEpochMilli(timestampMillis),
                ZoneOffset.UTC).atOffset(ZoneOffset.UTC);
        assertEquals(expected, value);
    }

    @Test
    public void testJsonTimestampMillisWithZone() {
        // Create Iceberg schema with timestamptz
        org.apache.iceberg.Schema icebergSchema = new org.apache.iceberg.Schema(
                Types.NestedField.required(1, "timestamp_field", Types.TimestampType.withZone())
        );

        // Create JSON data with millisecond timestamp (like from timestamp-millis)
        ObjectNode jsonNode = JsonNodeFactory.instance.objectNode();
        long timestampMillis = 1730232695409L; // 2025-10-29T19:51:35.409 in milliseconds
        jsonNode.put("timestamp_field", timestampMillis);

        // Convert
        Record record = JsonToIcebergConverter.convertToRecord(icebergSchema, jsonNode);

        // Verify the result is OffsetDateTime
        Object value = record.getField("timestamp_field");
        assertTrue(value instanceof OffsetDateTime, "Value should be OffsetDateTime for timestamptz");

        // Verify the value is correct
        OffsetDateTime expected = LocalDateTime.ofInstant(
                java.time.Instant.ofEpochMilli(timestampMillis),
                ZoneOffset.UTC).atOffset(ZoneOffset.UTC);
        assertEquals(expected, value);
    }

    @Test
    public void testJsonTimestampMillisWithoutZone() {
        // Create Iceberg schema with regular timestamp
        org.apache.iceberg.Schema icebergSchema = new org.apache.iceberg.Schema(
                Types.NestedField.required(1, "timestamp_field", Types.TimestampType.withoutZone())
        );

        // Create JSON data with millisecond timestamp
        ObjectNode jsonNode = JsonNodeFactory.instance.objectNode();
        long timestampMillis = 1730232695409L; // 2025-10-29T19:51:35.409 in milliseconds
        jsonNode.put("timestamp_field", timestampMillis);

        // Convert
        Record record = JsonToIcebergConverter.convertToRecord(icebergSchema, jsonNode);

        // Verify the result is LocalDateTime
        Object value = record.getField("timestamp_field");
        assertTrue(value instanceof LocalDateTime, "Value should be LocalDateTime for timestamp without zone");

        // Verify the value is correct
        LocalDateTime expected = LocalDateTime.ofInstant(
                java.time.Instant.ofEpochMilli(timestampMillis),
                ZoneOffset.UTC);
        assertEquals(expected, value);
    }

    @Test
    public void testAvroTimestampWithoutZone() {
        // Create Avro schema with timestamp-millis
        Schema avroSchema = Schema.createRecord("test", null, null, false);
        Schema timestampMillisSchema = LogicalTypes.timestampMillis()
                .addToSchema(Schema.create(Schema.Type.LONG));
        avroSchema.setFields(java.util.List.of(
                new Schema.Field("timestamp_field", timestampMillisSchema)));

        // Create Iceberg schema with regular timestamp
        org.apache.iceberg.Schema icebergSchema = new org.apache.iceberg.Schema(
                Types.NestedField.required(1, "timestamp_field", Types.TimestampType.withoutZone())
        );

        // Create Avro record
        GenericRecord avroRecord = new GenericData.Record(avroSchema);
        long timestampMillis = 1698412800000L;
        avroRecord.put("timestamp_field", timestampMillis);

        // Convert
        Record record = AvroToIcebergConverter.convert(avroRecord, icebergSchema);

        // Verify the result is LocalDateTime
        Object value = record.getField("timestamp_field");
        assertTrue(value instanceof LocalDateTime, "Value should be LocalDateTime for timestamp without zone");

        // Verify the value
        LocalDateTime expected = LocalDateTime.ofInstant(
                java.time.Instant.ofEpochMilli(timestampMillis),
                ZoneOffset.UTC);
        assertEquals(expected, value);
    }
}