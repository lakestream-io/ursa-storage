/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.lakehouse.iceberg;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.variants.PhysicalType;
import org.apache.iceberg.variants.Variant;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;


@Tag("lakehouse")
class JsonToIcebergConverterTest {

    // Complex Iceberg Schema
    private static final Schema COMPLEX_SCHEMA = new Schema(
        Types.NestedField.required(1, "id", Types.LongType.get()),
        Types.NestedField.optional(2, "metadata", Types.StructType.of(
                Types.NestedField.required(3, "timestamp", Types.TimestampType.withoutZone()),
                Types.NestedField.optional(4, "tags", Types.ListType.ofRequired(5, Types.StringType.get()))
            )
        ), Types.NestedField.optional(6, "scores", Types.ListType.ofRequired(7, Types.DoubleType.get())),
        Types.NestedField.optional(8, "properties", Types.MapType.ofRequired(9, 10,
                Types.StringType.get(), Types.StructType.of(
                    Types.NestedField.required(11, "value", Types.StringType.get()),
                    Types.NestedField.optional(12, "unit", Types.StringType.get()))
            )),
        Types.NestedField.optional(13, "nested_map", Types.MapType.ofRequired(14, 15,
            Types.IntegerType.get(), Types.ListType.ofRequired(16, Types.BooleanType.get()))
        ),
        Types.NestedField.optional(17, "binary_data", Types.BinaryType.get()),
        Types.NestedField.optional(18, "logical_types", Types.StructType.of(
            Types.NestedField.required(19, "birth_date", Types.DateType.get()),
            Types.NestedField.optional(20, "event_time", Types.TimestampType.withZone()),
                Types.NestedField.optional(21, "local_time", Types.TimeType.get()))
        )
    );

    @Test
    void testComplexConversion() throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        ObjectNode jsonNode = mapper.createObjectNode();

        // Build complex JSON structure
        jsonNode.put("id", 12345L);
        Instant instant = Instant.now().truncatedTo(ChronoUnit.MICROS);
        ObjectNode metadata = jsonNode.putObject("metadata");
        metadata.put("timestamp", convertToLong(instant)); // Microseconds timestamp
        metadata.putArray("tags").add("test").add("iceberg").add("conversion");

        jsonNode.putArray("scores").add(9.8).add(7.6).add(8.4);

        ObjectNode properties = jsonNode.putObject("properties");
        ObjectNode prop1 = properties.putObject("prop1");
        prop1.put("value", "temperature");
        prop1.put("unit", "Celsius");
        ObjectNode prop2 = properties.putObject("prop2");
        prop2.put("value", "pressure");

        ObjectNode nestedMap = jsonNode.putObject("nested_map");
        nestedMap.putArray("1").add(true).add(false);
        nestedMap.putArray("2").add(false);

        jsonNode.put("binary_data", "SGVsbG8gV29ybGQh"); // "Hello World!" in Base64


        ObjectNode logicalTypes = jsonNode.putObject("logical_types");
        logicalTypes.put("birth_date", 19431); // Days since epoch (2023-03-15)
        logicalTypes.put("event_time", convertToLong(instant));
        logicalTypes.put("local_time", LocalDateTime.ofInstant(instant, ZoneOffset.UTC)
                .toLocalTime().toNanoOfDay());
        // Convert to Iceberg Record
        Record record = JsonToIcebergConverter.convertToRecord(COMPLEX_SCHEMA, jsonNode);

        // Verify conversion
        assertEquals(12345L, record.getField("id"));

        // Verify nested struct
        Record metadataRecord = (Record) record.getField("metadata");
        assertEquals(LocalDateTime.ofInstant(instant, ZoneOffset.UTC), metadataRecord.getField("timestamp"));
        assertEquals(List.of("test", "iceberg", "conversion"), metadataRecord.getField("tags"));

        // Verify list of doubles
        assertEquals(List.of(9.8, 7.6, 8.4), record.getField("scores"));

        // Verify map of structs
        Map<String, Record> propertiesMap = (Map) record.getField("properties");
        assertEquals(2, propertiesMap.size());
        Record prop1Record = propertiesMap.get("prop1");
        assertEquals("temperature", prop1Record.getField("value"));
        assertEquals("Celsius", prop1Record.getField("unit"));

        // Verify nested map with list values
        Map<Integer, List<Boolean>> nestedMapResult = (Map) record.getField("nested_map");
        assertEquals(List.of(true, false), nestedMapResult.get(1));
        assertEquals(List.of(false), nestedMapResult.get(2));

        // Verify binary data
        assertArrayEquals("Hello World!".getBytes(), ((ByteBuffer) record.getField("binary_data")).array());

        // Verify logical types conversion
        Record logicalTypesRecord = (Record) record.getField("logical_types");
        assertEquals(LocalDate.of(2023, 3, 15), logicalTypesRecord.getField("birth_date"));
        // With proper date assertion using epoch days:
        LocalDate expectedDate = LocalDate.ofEpochDay(19431);
        assertEquals(expectedDate, logicalTypesRecord.getField("birth_date"));
        assertEquals(OffsetDateTime.ofInstant(instant, ZoneOffset.UTC), logicalTypesRecord.getField("event_time"));
        assertEquals(LocalDateTime.ofInstant(instant, ZoneOffset.UTC).toLocalTime(), logicalTypesRecord.getField("local_time"));
    }

    private long convertToLong(Instant instant) {
        long seconds = instant.getEpochSecond();
        int nanos = instant.getNano();

        if (seconds < 0 && nanos > 0) {
            long micros = Math.multiplyExact(seconds + 1, 1_000_000L);
            long adjustment = (nanos / 1_000L) - 1_000_000;

            return Math.addExact(micros, adjustment);
        } else {
            long micros = Math.multiplyExact(seconds, 1_000_000L);

            return Math.addExact(micros, nanos / 1_000L);
        }
    }

    @Test
    void testRequiredFieldMissing() {
        ObjectMapper mapper = new ObjectMapper();
        ObjectNode jsonNode = mapper.createObjectNode(); // Missing required "id" field

        assertThrows(IllegalArgumentException.class, () ->
            JsonToIcebergConverter.convertToRecord(COMPLEX_SCHEMA, jsonNode)
        );
    }

    @Test
    void testNullHandling() {
        ObjectMapper mapper = new ObjectMapper();
        ObjectNode jsonNode = mapper.createObjectNode();
        jsonNode.put("id", 12345L);
        jsonNode.putNull("scores"); // Test optional null field

        Record record = JsonToIcebergConverter.convertToRecord(COMPLEX_SCHEMA, jsonNode);
        assertNull(record.getField("scores"));
    }

    @Test
    void testTimestampMillisConversion() {
        // Test to ensure timestamp-millis values are correctly converted
        Schema schema = new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.optional(2, "createdDate", Types.TimestampType.withoutZone()),
            Types.NestedField.optional(3, "expirationDate", Types.TimestampType.withZone())
        );

        ObjectMapper mapper = new ObjectMapper();
        ObjectNode jsonNode = mapper.createObjectNode();
        jsonNode.put("id", 123L);
        // Use actual timestamp values in milliseconds (like from timestamp-millis logical type)
        long createdDateMillis = 1730232695409L; // 2025-10-29T19:51:35.409
        long expirationDateMillis = 1769705147123L; // 2025-10-30T19:51:35.409
        jsonNode.put("createdDate", createdDateMillis);
        jsonNode.put("expirationDate", expirationDateMillis);

        // Convert
        Record record = JsonToIcebergConverter.convertToRecord(schema, jsonNode);

        // Verify
        LocalDateTime expectedCreated = LocalDateTime.ofInstant(
            Instant.ofEpochMilli(createdDateMillis), ZoneOffset.UTC);
        OffsetDateTime expectedExpiration = OffsetDateTime.ofInstant(
            Instant.ofEpochMilli(expirationDateMillis), ZoneOffset.UTC);

        assertEquals(expectedCreated, record.getField("createdDate"));
        assertEquals(expectedExpiration, record.getField("expirationDate"));
    }

    @Test
    void testFSharpDateTimeTicks() {
        // Test F# DateTime.Ticks conversion
        ObjectMapper mapper = new ObjectMapper();

        // Create schema with timestamp field
        Schema timestampSchema = new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.required(2, "created_date", Types.TimestampType.withoutZone()),
            Types.NestedField.optional(3, "created_date_tz", Types.TimestampType.withZone())
        );

        ObjectNode jsonNode = mapper.createObjectNode();
        jsonNode.put("id", 1L);

        // F# DateTime.Ticks for 2025-10-29 (approximate)
        // This represents 100-nanosecond intervals since 0001-01-01
        long fsharpTicks = 638718204000000000L;  // Around October 2025
        jsonNode.put("created_date", fsharpTicks);
        jsonNode.put("created_date_tz", fsharpTicks);

        // Convert to Iceberg Record
        Record record = JsonToIcebergConverter.convertToRecord(timestampSchema, jsonNode);

        // Verify the conversion
        LocalDateTime localDateTime = (LocalDateTime) record.getField("created_date");
        OffsetDateTime offsetDateTime = (OffsetDateTime) record.getField("created_date_tz");

        // The date should be in 2025, not 1970
        assertEquals(2025, localDateTime.getYear());
        assertEquals(1, localDateTime.getMonthValue());
        assertEquals(2025, offsetDateTime.getYear());
        assertEquals(1, offsetDateTime.getMonthValue());
    }

    @Test
    void testMixedTimestampFormats() {
        // Test various timestamp formats to ensure backward compatibility
        ObjectMapper mapper = new ObjectMapper();

        Schema mixedTimestampSchema = new Schema(
            Types.NestedField.required(1, "unix_seconds", Types.TimestampType.withoutZone()),
            Types.NestedField.required(2, "unix_millis", Types.TimestampType.withoutZone()),
            Types.NestedField.required(3, "unix_micros", Types.TimestampType.withoutZone()),
            Types.NestedField.required(4, "fsharp_ticks", Types.TimestampType.withoutZone())
        );

        ObjectNode jsonNode = mapper.createObjectNode();

        // Unix seconds (October 29, 2025)
        long unixSeconds = 1761782400L;
        jsonNode.put("unix_seconds", unixSeconds);

        // Unix milliseconds
        long unixMillis = 1761782400000L;
        jsonNode.put("unix_millis", unixMillis);

        // Unix microseconds
        long unixMicros = 1761782400000000L;
        jsonNode.put("unix_micros", unixMicros);

        // F# DateTime.Ticks
        long fsharpTicks = 638718204000000000L;
        jsonNode.put("fsharp_ticks", fsharpTicks);

        Record record = JsonToIcebergConverter.convertToRecord(mixedTimestampSchema, jsonNode);

        // All should represent dates in 2025
        LocalDateTime secondsTime = (LocalDateTime) record.getField("unix_seconds");
        LocalDateTime millisTime = (LocalDateTime) record.getField("unix_millis");
        LocalDateTime microsTime = (LocalDateTime) record.getField("unix_micros");
        LocalDateTime ticksTime = (LocalDateTime) record.getField("fsharp_ticks");

        assertEquals(2025, secondsTime.getYear());
        assertEquals(2025, millisTime.getYear());
        assertEquals(2025, microsTime.getYear());
        assertEquals(2025, ticksTime.getYear());

        // The unix timestamps should all represent the same time
        assertEquals(secondsTime, millisTime);
        assertEquals(millisTime, microsTime);
    }

    @Test
    void testVariantPrimitives() throws Exception {
        Schema variantSchema = new Schema(
                Types.NestedField.optional(1, "variant_col", Types.VariantType.get())
        );

        ObjectMapper mapper = new ObjectMapper();

        // 1. Test String Variant
        ObjectNode jsonNode = mapper.createObjectNode();
        jsonNode.put("variant_col", "hello iceberg");
        Record record = JsonToIcebergConverter.convertToRecord(variantSchema, jsonNode);
        Variant variant = (Variant) record.getField("variant_col");
        assertEquals("hello iceberg", variant.value().asPrimitive().get());

        // 2. Test Integer/Long Variant
        jsonNode.put("variant_col", 42L);
        record = JsonToIcebergConverter.convertToRecord(variantSchema, jsonNode);
        variant = (Variant) record.getField("variant_col");
        assertEquals(42L, variant.value().asPrimitive().get());

        // 3. Test Boolean Variant
        jsonNode.put("variant_col", true);
        record = JsonToIcebergConverter.convertToRecord(variantSchema, jsonNode);
        variant = (Variant) record.getField("variant_col");
        assertEquals(true, variant.value().asPrimitive().get());

        // 4. Test Double Variant
        jsonNode.put("variant_col", 3.14159);
        record = JsonToIcebergConverter.convertToRecord(variantSchema, jsonNode);
        variant = (Variant) record.getField("variant_col");
        assertEquals(3.14159, variant.value().asPrimitive().get());
    }

    @Test
    void testVariantNullAndMissing() {
        Schema variantSchema = new Schema(
                Types.NestedField.optional(1, "variant_col", Types.VariantType.get())
        );

        ObjectMapper mapper = new ObjectMapper();
        ObjectNode jsonNode = mapper.createObjectNode();

        // 1. Explicit JSON Null
        jsonNode.putNull("variant_col");
        Record record = JsonToIcebergConverter.convertToRecord(variantSchema, jsonNode);
        Variant variant = (Variant) record.getField("variant_col");
        // Note: fromJsonNode returns null for variant if jsonNode.isNull()
        assertNull(variant);

        // 2. Missing field (Optional)
        ObjectNode emptyNode = mapper.createObjectNode();
        record = JsonToIcebergConverter.convertToRecord(variantSchema, emptyNode);
        assertNull(record.getField("variant_col"));
    }

    @Test
    void testVariantComplexTypes() throws Exception {
        Schema variantSchema = new Schema(
                Types.NestedField.optional(1, "variant_col", Types.VariantType.get())
        );

        ObjectMapper mapper = new ObjectMapper();
        ObjectNode jsonNode = mapper.createObjectNode();

        // Testing Object structure
        ObjectNode nested = jsonNode.putObject("variant_col");
        nested.put("key", "value");
        nested.put("num", 123);

        Record record = JsonToIcebergConverter.convertToRecord(variantSchema, jsonNode);
        Variant variant = (Variant) record.getField("variant_col");

        // Based on current implementation, complex types are converted to String
        String expectedJson = "{\"key\":\"value\",\"num\":123}";
        assertEquals(expectedJson, variant.value().asPrimitive().get());

        // Testing Array structure
        jsonNode.putArray("variant_col").add(1).add(2).add(3);
        record = JsonToIcebergConverter.convertToRecord(variantSchema, jsonNode);
        variant = (Variant) record.getField("variant_col");
        assertEquals("[1,2,3]", variant.value().asPrimitive().get());
    }

    @Test
    void testVariantBinary() throws Exception {
        Schema variantSchema = new Schema(
                Types.NestedField.optional(1, "variant_col", Types.VariantType.get())
        );

        ObjectMapper mapper = new ObjectMapper();
        ObjectNode jsonNode = mapper.createObjectNode();

        byte[] data = "iceberg-binary".getBytes();
        jsonNode.put("variant_col", data);

        Record record = JsonToIcebergConverter.convertToRecord(variantSchema, jsonNode);
        Variant variant = (Variant) record.getField("variant_col");

        assertEquals(PhysicalType.BINARY, variant.value().type());
        ByteBuffer buffer = (ByteBuffer) variant.value().asPrimitive().get();
        byte[] bytes = new byte[buffer.remaining()];
        buffer.get(bytes);
        assertArrayEquals(data, bytes);
    }

    @Test
    void testTimestampArrayWithoutSeconds() {
        // Test timestamp arrays with 5 elements (missing seconds component)
        Schema schema = new Schema(
            Types.NestedField.required(1, "timestamp_without_tz", Types.TimestampType.withoutZone()),
            Types.NestedField.required(2, "timestamp_with_tz", Types.TimestampType.withZone())
        );

        ObjectMapper mapper = new ObjectMapper();
        ObjectNode jsonNode = mapper.createObjectNode();

        // Create timestamp arrays without seconds [year, month, day, hour, minute]
        jsonNode.putArray("timestamp_without_tz")
            .add(2024).add(4).add(11).add(14).add(30); // 2024-04-11T14:30:00
        jsonNode.putArray("timestamp_with_tz")
            .add(2025).add(12).add(25).add(9).add(45);  // 2025-12-25T09:45:00

        Record record = JsonToIcebergConverter.convertToRecord(schema, jsonNode);

        // Verify timestamps have seconds defaulted to 0
        LocalDateTime timestampWithoutTz = (LocalDateTime) record.getField("timestamp_without_tz");
        assertEquals(LocalDateTime.of(2024, 4, 11, 14, 30, 0), timestampWithoutTz);

        OffsetDateTime timestampWithTz = (OffsetDateTime) record.getField("timestamp_with_tz");
        assertEquals(OffsetDateTime.of(2025, 12, 25, 9, 45, 0, 0, ZoneOffset.UTC), timestampWithTz);
    }

    @Test
    void testTimeArrayWithoutSeconds() {
        // Test time arrays with 2 elements (missing seconds component)
        Schema schema = new Schema(
            Types.NestedField.required(1, "time_field", Types.TimeType.get())
        );

        ObjectMapper mapper = new ObjectMapper();
        ObjectNode jsonNode = mapper.createObjectNode();

        // Create time array without seconds [hour, minute]
        jsonNode.putArray("time_field").add(14).add(30); // 14:30:00

        Record record = JsonToIcebergConverter.convertToRecord(schema, jsonNode);

        // Verify time has seconds defaulted to 0
        assertEquals(LocalTime.of(14, 30, 0), record.getField("time_field"));
    }

    @Test
    void testTimestampAndTimeArraysWithSeconds() {
        // Verify that the original format with seconds still works correctly
        Schema schema = new Schema(
            Types.NestedField.required(1, "timestamp_field", Types.TimestampType.withoutZone()),
            Types.NestedField.required(2, "time_field", Types.TimeType.get())
        );

        ObjectMapper mapper = new ObjectMapper();
        ObjectNode jsonNode = mapper.createObjectNode();

        // Timestamp with all 6 elements [year, month, day, hour, minute, second]
        jsonNode.putArray("timestamp_field")
            .add(2024).add(4).add(11).add(14).add(30).add(45); // 2024-04-11T14:30:45

        // Time with all 3 elements [hour, minute, second]
        jsonNode.putArray("time_field").add(14).add(30).add(45); // 14:30:45

        Record record = JsonToIcebergConverter.convertToRecord(schema, jsonNode);

        // Verify full format still works
        LocalDateTime timestamp = (LocalDateTime) record.getField("timestamp_field");
        assertEquals(LocalDateTime.of(2024, 4, 11, 14, 30, 45), timestamp);

        assertEquals(LocalTime.of(14, 30, 45), record.getField("time_field"));
    }
}