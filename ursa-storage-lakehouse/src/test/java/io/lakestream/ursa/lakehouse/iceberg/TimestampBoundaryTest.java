/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.lakehouse.iceberg;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;


@Tag("lakehouse")
public class TimestampBoundaryTest {

    @Test
    public void testBoundaryValueExactly10Power12WithZone() {
        // Create Iceberg schema with timestamptz
        Schema icebergSchema = new Schema(
                Types.NestedField.required(1, "timestamp_field", Types.TimestampType.withZone())
        );

        // Create JSON data with exactly 10^12 (boundary between seconds and milliseconds)
        ObjectNode jsonNode = JsonNodeFactory.instance.objectNode();
        long boundaryValue = 1_000_000_000_000L; // Exactly 10^12
        jsonNode.put("timestamp_field", boundaryValue);

        // Convert
        Record record = JsonToIcebergConverter.convertToRecord(icebergSchema, jsonNode);

        // Verify - should be treated as milliseconds (first value >= 10^12)
        Object value = record.getField("timestamp_field");
        assertTrue(value instanceof OffsetDateTime);
        OffsetDateTime expected = LocalDateTime.ofInstant(
                Instant.ofEpochMilli(boundaryValue),
                ZoneOffset.UTC).atOffset(ZoneOffset.UTC);
        assertEquals(expected, value);
    }

    @Test
    public void testBoundaryValueJustBelow10Power12() {
        Schema icebergSchema = new Schema(
                Types.NestedField.required(1, "timestamp_field", Types.TimestampType.withoutZone())
        );

        // One less than 10^12
        ObjectNode jsonNode = JsonNodeFactory.instance.objectNode();
        long justBelowBoundary = 999_999_999_999L;
        jsonNode.put("timestamp_field", justBelowBoundary);

        Record record = JsonToIcebergConverter.convertToRecord(icebergSchema, jsonNode);

        // Should be treated as seconds
        Object value = record.getField("timestamp_field");
        assertTrue(value instanceof LocalDateTime);
        LocalDateTime expected = LocalDateTime.ofInstant(
                Instant.ofEpochSecond(justBelowBoundary),
                ZoneOffset.UTC);
        assertEquals(expected, value);
    }

    @Test
    public void testBoundaryValueExactly10Power15WithZone() {
        Schema icebergSchema = new Schema(
                Types.NestedField.required(1, "timestamp_field", Types.TimestampType.withZone())
        );

        // Exactly 10^15 (boundary between milliseconds and microseconds)
        ObjectNode jsonNode = JsonNodeFactory.instance.objectNode();
        long boundaryValue = 1_000_000_000_000_000L; // Exactly 10^15
        jsonNode.put("timestamp_field", boundaryValue);

        Record record = JsonToIcebergConverter.convertToRecord(icebergSchema, jsonNode);

        // Should be treated as microseconds (first value >= 10^15)
        Object value = record.getField("timestamp_field");
        assertTrue(value instanceof OffsetDateTime);
        OffsetDateTime expected = LocalDateTime.ofInstant(
                Instant.ofEpochSecond(
                        boundaryValue / 1_000_000,
                        (boundaryValue % 1_000_000) * 1000),
                ZoneOffset.UTC).atOffset(ZoneOffset.UTC);
        assertEquals(expected, value);
    }

    @Test
    public void testBoundaryValueJustBelow10Power15() {
        Schema icebergSchema = new Schema(
                Types.NestedField.required(1, "timestamp_field", Types.TimestampType.withoutZone())
        );

        // One less than 10^15
        ObjectNode jsonNode = JsonNodeFactory.instance.objectNode();
        long justBelowBoundary = 999_999_999_999_999L;
        jsonNode.put("timestamp_field", justBelowBoundary);

        Record record = JsonToIcebergConverter.convertToRecord(icebergSchema, jsonNode);

        // Should be treated as milliseconds
        Object value = record.getField("timestamp_field");
        assertTrue(value instanceof LocalDateTime);
        LocalDateTime expected = LocalDateTime.ofInstant(
                Instant.ofEpochMilli(justBelowBoundary),
                ZoneOffset.UTC);
        assertEquals(expected, value);
    }

    @Test
    public void testNegativeTimestampValueAsSeconds() {
        Schema icebergSchema = new Schema(
                Types.NestedField.required(1, "timestamp_field", Types.TimestampType.withoutZone())
        );

        // Negative timestamp (before Unix epoch) - should be treated as seconds
        ObjectNode jsonNode = JsonNodeFactory.instance.objectNode();
        long negativeTimestamp = -31536000L; // -1 year from epoch (1969)
        jsonNode.put("timestamp_field", negativeTimestamp);

        Record record = JsonToIcebergConverter.convertToRecord(icebergSchema, jsonNode);

        // Should be treated as seconds (< 10^12)
        Object value = record.getField("timestamp_field");
        assertTrue(value instanceof LocalDateTime);
        LocalDateTime expected = LocalDateTime.ofInstant(
                Instant.ofEpochSecond(negativeTimestamp),
                ZoneOffset.UTC);
        assertEquals(expected, value);
    }

    @Test
    public void testNegativeTimestampValueAsMilliseconds() {
        Schema icebergSchema = new Schema(
                Types.NestedField.required(1, "timestamp_field", Types.TimestampType.withZone())
        );

        // Negative timestamp in "millisecond range" - edge case that might be problematic
        ObjectNode jsonNode = JsonNodeFactory.instance.objectNode();
        long negativeMillis = -1_000_000_000_000L; // Negative value at boundary
        jsonNode.put("timestamp_field", negativeMillis);

        Record record = JsonToIcebergConverter.convertToRecord(icebergSchema, jsonNode);

        // Should be treated as seconds (< 10^12, even though absolute value is at boundary)
        Object value = record.getField("timestamp_field");
        assertTrue(value instanceof OffsetDateTime);
        OffsetDateTime expected = LocalDateTime.ofInstant(
                Instant.ofEpochSecond(negativeMillis),
                ZoneOffset.UTC).atOffset(ZoneOffset.UTC);
        assertEquals(expected, value);
    }

    @Test
    public void testYear2038Problem32BitSeconds() {
        Schema icebergSchema = new Schema(
                Types.NestedField.required(1, "timestamp_field", Types.TimestampType.withoutZone())
        );

        // Year 2038 problem: maximum value for signed 32-bit integer (2^31 - 1)
        ObjectNode jsonNode = JsonNodeFactory.instance.objectNode();
        long year2038Timestamp = 2_147_483_647L; // January 19, 2038 03:14:07 UTC
        jsonNode.put("timestamp_field", year2038Timestamp);

        Record record = JsonToIcebergConverter.convertToRecord(icebergSchema, jsonNode);

        // Should be treated as seconds (< 10^12)
        Object value = record.getField("timestamp_field");
        assertTrue(value instanceof LocalDateTime);
        LocalDateTime expected = LocalDateTime.ofInstant(
                Instant.ofEpochSecond(year2038Timestamp),
                ZoneOffset.UTC);
        assertEquals(expected, value);
        // Verify this is indeed the Y2038 problematic date
        LocalDateTime y2038 = (LocalDateTime) value;
        assertEquals(2038, y2038.getYear());
        assertEquals(1, y2038.getMonthValue());
        assertEquals(19, y2038.getDayOfMonth());
    }

    @Test
    public void testYear2038ProblemOverflow() {
        Schema icebergSchema = new Schema(
                Types.NestedField.required(1, "timestamp_field", Types.TimestampType.withZone())
        );

        // Just after the Year 2038 problem: 2^31
        ObjectNode jsonNode = JsonNodeFactory.instance.objectNode();
        long afterY2038 = 2_147_483_648L; // One second after the 32-bit limit
        jsonNode.put("timestamp_field", afterY2038);

        Record record = JsonToIcebergConverter.convertToRecord(icebergSchema, jsonNode);

        // Should still be treated as seconds (< 10^12)
        Object value = record.getField("timestamp_field");
        assertTrue(value instanceof OffsetDateTime);
        OffsetDateTime expected = LocalDateTime.ofInstant(
                Instant.ofEpochSecond(afterY2038),
                ZoneOffset.UTC).atOffset(ZoneOffset.UTC);
        assertEquals(expected, value);
    }

    @Test
    public void testYear2038AsMilliseconds() {
        Schema icebergSchema = new Schema(
                Types.NestedField.required(1, "timestamp_field", Types.TimestampType.withoutZone())
        );

        // Year 2038 timestamp represented as milliseconds
        ObjectNode jsonNode = JsonNodeFactory.instance.objectNode();
        long year2038Millis = 2_147_483_647_000L; // Y2038 in milliseconds
        jsonNode.put("timestamp_field", year2038Millis);

        Record record = JsonToIcebergConverter.convertToRecord(icebergSchema, jsonNode);

        // Should be treated as milliseconds (>= 10^12)
        Object value = record.getField("timestamp_field");
        assertTrue(value instanceof LocalDateTime);
        LocalDateTime expected = LocalDateTime.ofInstant(
                Instant.ofEpochMilli(year2038Millis),
                ZoneOffset.UTC);
        assertEquals(expected, value);
        // Verify this is the same Y2038 date
        LocalDateTime y2038 = (LocalDateTime) value;
        assertEquals(2038, y2038.getYear());
    }

    @Test
    public void testFarFutureTimestamp() {
        Schema icebergSchema = new Schema(
                Types.NestedField.required(1, "timestamp_field", Types.TimestampType.withZone())
        );

        // Far future timestamp that could be ambiguous
        // This is year ~2033 in seconds, which might be misinterpreted
        ObjectNode jsonNode = JsonNodeFactory.instance.objectNode();
        long futureSeconds = 2_000_000_000L; // August 18, 2033
        jsonNode.put("timestamp_field", futureSeconds);

        Record record = JsonToIcebergConverter.convertToRecord(icebergSchema, jsonNode);

        // Should be treated as seconds (< 10^12)
        Object value = record.getField("timestamp_field");
        assertTrue(value instanceof OffsetDateTime);
        OffsetDateTime expected = LocalDateTime.ofInstant(
                Instant.ofEpochSecond(futureSeconds),
                ZoneOffset.UTC).atOffset(ZoneOffset.UTC);
        assertEquals(expected, value);
        // Verify the year
        OffsetDateTime future = (OffsetDateTime) value;
        assertEquals(2033, future.getYear());
    }

    @Test
    public void testZeroTimestamp() {
        Schema icebergSchema = new Schema(
                Types.NestedField.required(1, "timestamp_field", Types.TimestampType.withoutZone())
        );

        // Zero timestamp (Unix epoch)
        ObjectNode jsonNode = JsonNodeFactory.instance.objectNode();
        jsonNode.put("timestamp_field", 0L);

        Record record = JsonToIcebergConverter.convertToRecord(icebergSchema, jsonNode);

        // Should be treated as seconds (< 10^12)
        Object value = record.getField("timestamp_field");
        assertTrue(value instanceof LocalDateTime);
        LocalDateTime expected = LocalDateTime.ofInstant(Instant.EPOCH, ZoneOffset.UTC);
        assertEquals(expected, value);
        // Verify this is 1970-01-01
        LocalDateTime epoch = (LocalDateTime) value;
        assertEquals(1970, epoch.getYear());
        assertEquals(1, epoch.getMonthValue());
        assertEquals(1, epoch.getDayOfMonth());
    }
}