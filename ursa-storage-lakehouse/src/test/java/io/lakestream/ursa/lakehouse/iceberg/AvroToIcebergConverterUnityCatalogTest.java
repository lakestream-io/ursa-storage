/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.lakehouse.iceberg;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;

public class AvroToIcebergConverterUnityCatalogTest {

    @Test
    public void testUUIDToStringConversionForUnityCatalog() {
        // Create Avro schema with UUID logical type
        Schema uuidLogicalType = LogicalTypes.uuid().addToSchema(
                Schema.createFixed("UUID", null, null, 16));
        Schema avroSchema = Schema.createRecord("TestRecord", null, null, false);
        avroSchema.setFields(List.of(
                new Schema.Field("id", uuidLogicalType),
                new Schema.Field("name", Schema.create(Schema.Type.STRING))
        ));

        // Create Iceberg schema with STRING type for UUID (Unity Catalog compatible)
        org.apache.iceberg.Schema icebergSchema = new org.apache.iceberg.Schema(
                Types.NestedField.optional(1, "id", Types.StringType.get()),
                Types.NestedField.optional(2, "name", Types.StringType.get())
        );

        // Create Avro record with UUID value
        UUID uuid = UUID.randomUUID();
        ByteBuffer buffer = ByteBuffer.allocate(16);
        buffer.putLong(uuid.getMostSignificantBits());
        buffer.putLong(uuid.getLeastSignificantBits());
        buffer.rewind();

        GenericData.Fixed fixed = new GenericData.Fixed(uuidLogicalType, buffer.array());
        GenericRecord avroRecord = new GenericData.Record(avroSchema);
        avroRecord.put("id", fixed);
        avroRecord.put("name", "test");

        // Convert - Unity Catalog schema has STRING type for UUID
        Record icebergRecord = AvroToIcebergConverter.convert(avroRecord, icebergSchema);

        // Assert that UUID is converted to String
        Object idValue = icebergRecord.getField("id");
        assertInstanceOf(String.class, idValue);
        assertEquals(uuid.toString(), idValue);
        assertEquals("test", icebergRecord.getField("name"));
    }

    @Test
    public void testUUIDPreservedWhenNotUnityCatalog() {
        // Create Avro schema with UUID logical type
        Schema uuidLogicalType = LogicalTypes.uuid().addToSchema(
                Schema.createFixed("UUID", null, null, 16));
        Schema avroSchema = Schema.createRecord("TestRecord", null, null, false);
        avroSchema.setFields(List.of(
                new Schema.Field("id", uuidLogicalType),
                new Schema.Field("name", Schema.create(Schema.Type.STRING))
        ));

        // Create Iceberg schema with UUID type (non-Unity Catalog)
        org.apache.iceberg.Schema icebergSchema = new org.apache.iceberg.Schema(
                Types.NestedField.optional(1, "id", Types.UUIDType.get()),
                Types.NestedField.optional(2, "name", Types.StringType.get())
        );

        // Create Avro record with UUID value
        UUID uuid = UUID.randomUUID();
        ByteBuffer buffer = ByteBuffer.allocate(16);
        buffer.putLong(uuid.getMostSignificantBits());
        buffer.putLong(uuid.getLeastSignificantBits());
        buffer.rewind();

        GenericData.Fixed fixed = new GenericData.Fixed(uuidLogicalType, buffer.array());
        GenericRecord avroRecord = new GenericData.Record(avroSchema);
        avroRecord.put("id", fixed);
        avroRecord.put("name", "test");

        // Convert - Non-Unity Catalog schema has UUID type
        Record icebergRecord = AvroToIcebergConverter.convert(avroRecord, icebergSchema);

        // Assert that UUID is preserved as UUID
        Object idValue = icebergRecord.getField("id");
        assertInstanceOf(UUID.class, idValue);
        assertEquals(uuid, idValue);
        assertEquals("test", icebergRecord.getField("name"));
    }

    @Test
    public void testNestedUUIDConversion() {
        // Create nested Avro schema with UUID
        Schema uuidLogicalType = LogicalTypes.uuid().addToSchema(
                Schema.createFixed("UUID", null, null, 16));
        Schema nestedSchema = Schema.createRecord("NestedRecord", null, null, false);
        nestedSchema.setFields(List.of(
                new Schema.Field("nestedId", uuidLogicalType)
        ));
        Schema avroSchema = Schema.createRecord("TestRecord", null, null, false);
        avroSchema.setFields(List.of(
                new Schema.Field("id", uuidLogicalType),
                new Schema.Field("nested", nestedSchema)
        ));

        // Create Iceberg schema with STRING types for UUIDs
        org.apache.iceberg.Schema icebergSchema = new org.apache.iceberg.Schema(
                Types.NestedField.optional(1, "id", Types.StringType.get()),
                Types.NestedField.optional(2, "nested", Types.StructType.of(
                        Types.NestedField.optional(3, "nestedId", Types.StringType.get())
                ))
        );

        // Create Avro records
        UUID uuid1 = UUID.randomUUID();
        UUID uuid2 = UUID.randomUUID();
        ByteBuffer buffer1 = ByteBuffer.allocate(16);
        buffer1.putLong(uuid1.getMostSignificantBits());
        buffer1.putLong(uuid1.getLeastSignificantBits());
        buffer1.rewind();
        ByteBuffer buffer2 = ByteBuffer.allocate(16);
        buffer2.putLong(uuid2.getMostSignificantBits());
        buffer2.putLong(uuid2.getLeastSignificantBits());
        buffer2.rewind();

        GenericData.Fixed fixed1 = new GenericData.Fixed(uuidLogicalType, buffer1.array());
        GenericData.Fixed fixed2 = new GenericData.Fixed(uuidLogicalType, buffer2.array());
        GenericRecord nestedRecord = new GenericData.Record(nestedSchema);
        nestedRecord.put("nestedId", fixed2);
        GenericRecord avroRecord = new GenericData.Record(avroSchema);
        avroRecord.put("id", fixed1);
        avroRecord.put("nested", nestedRecord);

        // Convert - Unity Catalog schema has STRING type for UUID
        Record icebergRecord = AvroToIcebergConverter.convert(avroRecord, icebergSchema);

        // Assert UUIDs are converted to strings
        assertEquals(uuid1.toString(), icebergRecord.getField("id"));
        Record nested = (Record) icebergRecord.getField("nested");
        assertEquals(uuid2.toString(), nested.getField("nestedId"));
    }

    @Test
    public void testArrayOfUUIDsConversion() {
        // Create Avro schema with array of UUIDs
        Schema uuidLogicalType = LogicalTypes.uuid().addToSchema(
                Schema.createFixed("UUID", null, null, 16));
        Schema arraySchema = Schema.createArray(uuidLogicalType);
        Schema avroSchema = Schema.createRecord("TestRecord", null, null, false);
        avroSchema.setFields(List.of(
                new Schema.Field("ids", arraySchema)
        ));

        // Create Iceberg schema with array of STRINGs
        org.apache.iceberg.Schema icebergSchema = new org.apache.iceberg.Schema(
                Types.NestedField.optional(1, "ids", Types.ListType.ofOptional(2, Types.StringType.get()))
        );

        // Create Avro record with array of UUIDs
        UUID uuid1 = UUID.randomUUID();
        UUID uuid2 = UUID.randomUUID();
        ByteBuffer buffer1 = ByteBuffer.allocate(16);
        buffer1.putLong(uuid1.getMostSignificantBits());
        buffer1.putLong(uuid1.getLeastSignificantBits());
        buffer1.rewind();
        ByteBuffer buffer2 = ByteBuffer.allocate(16);
        buffer2.putLong(uuid2.getMostSignificantBits());
        buffer2.putLong(uuid2.getLeastSignificantBits());
        buffer2.rewind();

        GenericData.Fixed fixed1 = new GenericData.Fixed(uuidLogicalType, buffer1.array());
        GenericData.Fixed fixed2 = new GenericData.Fixed(uuidLogicalType, buffer2.array());
        GenericData.Array<GenericData.Fixed> array = new GenericData.Array<>(arraySchema, List.of(fixed1, fixed2));
        GenericRecord avroRecord = new GenericData.Record(avroSchema);
        avroRecord.put("ids", array);

        // Convert - Unity Catalog schema has STRING type for UUID
        Record icebergRecord = AvroToIcebergConverter.convert(avroRecord, icebergSchema);

        // Assert UUIDs in array are converted to strings
        List<?> ids = (List<?>) icebergRecord.getField("ids");
        assertEquals(2, ids.size());
        assertEquals(uuid1.toString(), ids.get(0));
        assertEquals(uuid2.toString(), ids.get(1));
    }

    @Test
    public void testMapWithUUIDValuesConversion() {
        // Create Avro schema with map of UUID values
        Schema uuidLogicalType = LogicalTypes.uuid().addToSchema(
                Schema.createFixed("UUID", null, null, 16));
        Schema mapSchema = Schema.createMap(uuidLogicalType);
        Schema avroSchema = Schema.createRecord("TestRecord", null, null, false);
        avroSchema.setFields(List.of(
                new Schema.Field("uuidMap", mapSchema)
        ));

        // Create Iceberg schema with map of STRING values
        org.apache.iceberg.Schema icebergSchema = new org.apache.iceberg.Schema(
                Types.NestedField.optional(1, "uuidMap", Types.MapType.ofOptional(2, 3,
                        Types.StringType.get(), Types.StringType.get()))
        );

        // Create Avro record with map of UUIDs
        UUID uuid1 = UUID.randomUUID();
        UUID uuid2 = UUID.randomUUID();
        ByteBuffer buffer1 = ByteBuffer.allocate(16);
        buffer1.putLong(uuid1.getMostSignificantBits());
        buffer1.putLong(uuid1.getLeastSignificantBits());
        buffer1.rewind();
        ByteBuffer buffer2 = ByteBuffer.allocate(16);
        buffer2.putLong(uuid2.getMostSignificantBits());
        buffer2.putLong(uuid2.getLeastSignificantBits());
        buffer2.rewind();

        GenericData.Fixed fixed1 = new GenericData.Fixed(uuidLogicalType, buffer1.array());
        GenericData.Fixed fixed2 = new GenericData.Fixed(uuidLogicalType, buffer2.array());
        Map<String, GenericData.Fixed> map = Map.of("key1", fixed1, "key2", fixed2);
        GenericRecord avroRecord = new GenericData.Record(avroSchema);
        avroRecord.put("uuidMap", map);

        // Convert - Unity Catalog schema has STRING type for UUID
        Record icebergRecord = AvroToIcebergConverter.convert(avroRecord, icebergSchema);

        // Assert UUID values in map are converted to strings
        Map<?, ?> uuidMap = (Map<?, ?>) icebergRecord.getField("uuidMap");
        assertEquals(2, uuidMap.size());
        assertEquals(uuid1.toString(), uuidMap.get("key1"));
        assertEquals(uuid2.toString(), uuidMap.get("key2"));
    }

    @Test
    public void testStringBasedUUIDConversion() {
        // Create Avro schema with string-based UUID logical type
        Schema stringUuidSchema = LogicalTypes.uuid().addToSchema(Schema.create(Schema.Type.STRING));
        Schema avroSchema = Schema.createRecord("TestRecord", null, null, false);
        avroSchema.setFields(List.of(
                new Schema.Field("id", stringUuidSchema)
        ));

        // Create Iceberg schema with STRING type
        org.apache.iceberg.Schema icebergSchema = new org.apache.iceberg.Schema(
                Types.NestedField.optional(1, "id", Types.StringType.get())
        );

        // Create Avro record with string UUID
        UUID uuid = UUID.randomUUID();
        GenericRecord avroRecord = new GenericData.Record(avroSchema);
        avroRecord.put("id", uuid.toString());

        // Convert - Unity Catalog schema has STRING type for UUID
        Record icebergRecord = AvroToIcebergConverter.convert(avroRecord, icebergSchema);

        // Assert that string UUID is preserved as string
        assertEquals(uuid.toString(), icebergRecord.getField("id"));
    }

    @Test
    public void testNullableUUIDConversion() {
        // Create Avro schema with nullable UUID
        Schema uuidLogicalType = LogicalTypes.uuid().addToSchema(
                Schema.createFixed("UUID", null, null, 16));
        Schema nullableUuid = Schema.createUnion(Schema.create(Schema.Type.NULL), uuidLogicalType);
        Schema avroSchema = Schema.createRecord("TestRecord", null, null, false);
        avroSchema.setFields(List.of(
                new Schema.Field("id", nullableUuid),
                new Schema.Field("name", Schema.create(Schema.Type.STRING))
        ));

        // Create Iceberg schema
        org.apache.iceberg.Schema icebergSchema = new org.apache.iceberg.Schema(
                Types.NestedField.optional(1, "id", Types.StringType.get()),
                Types.NestedField.optional(2, "name", Types.StringType.get())
        );

        // Test with null value
        GenericRecord avroRecordNull = new GenericData.Record(avroSchema);
        avroRecordNull.put("id", null);
        avroRecordNull.put("name", "test");

        Record icebergRecordNull = AvroToIcebergConverter.convert(avroRecordNull, icebergSchema);
        assertTrue(icebergRecordNull.getField("id") == null);
        assertEquals("test", icebergRecordNull.getField("name"));

        // Test with UUID value
        UUID uuid = UUID.randomUUID();
        ByteBuffer buffer = ByteBuffer.allocate(16);
        buffer.putLong(uuid.getMostSignificantBits());
        buffer.putLong(uuid.getLeastSignificantBits());
        buffer.rewind();

        GenericData.Fixed fixed = new GenericData.Fixed(uuidLogicalType, buffer.array());
        GenericRecord avroRecordWithUuid = new GenericData.Record(avroSchema);
        avroRecordWithUuid.put("id", fixed);
        avroRecordWithUuid.put("name", "test");

        Record icebergRecordWithUuid = AvroToIcebergConverter.convert(avroRecordWithUuid, icebergSchema);
        assertEquals(uuid.toString(), icebergRecordWithUuid.getField("id"));
        assertEquals("test", icebergRecordWithUuid.getField("name"));
    }
}