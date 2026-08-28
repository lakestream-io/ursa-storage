/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.lakehouse.utils;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import org.apache.avro.Schema;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;

public class AvroSchemaUtilExtendedUnityCatalogTest {

    @Test
    public void testToIcebergWithUnityCatalogUUIDConversion() {
        // Create an Avro schema with UUID field
        String avroSchemaStr = "{\n"
            + " \"type\": \"record\",\n"
            + " \"name\": \"TestUUID\",\n"
            + " \"fields\": [\n"
            + "   {\"name\": \"id\", \"type\": {\"type\": \"string\", \"logicalType\": \"uuid\"}},\n"
            + "   {\"name\": \"name\", \"type\": \"string\"}\n"
            + " ]\n"
            + "}";

        Schema avroSchema = new Schema.Parser().parse(avroSchemaStr);

        // Convert without Unity Catalog (should keep UUID type)
        org.apache.iceberg.Schema icebergSchemaNormal = AvroSchemaUtilExtended.toIceberg(avroSchema, false, false);

        // Convert with Unity Catalog (should convert UUID to STRING)
        org.apache.iceberg.Schema icebergSchemaUnity = AvroSchemaUtilExtended.toIceberg(avroSchema, false, true);

        // Verify schemas
        assertNotNull(icebergSchemaNormal);
        assertNotNull(icebergSchemaUnity);

        // Check that both schemas have correct field types
        // For Unity Catalog, UUID should be converted to STRING
        Types.NestedField idFieldUnity = icebergSchemaUnity.findField("id");
        assertNotNull(idFieldUnity);
        assertEquals(Types.StringType.get(), idFieldUnity.type());

        // Name field should remain string in both cases
        Types.NestedField nameFieldUnity = icebergSchemaUnity.findField("name");
        assertNotNull(nameFieldUnity);
        assertEquals(Types.StringType.get(), nameFieldUnity.type());
    }

    @Test
    public void testNestedUUIDConversion() {
        // Create a complex schema with nested UUID
        String avroSchemaStr = "{\n"
            + " \"type\": \"record\",\n"
            + " \"name\": \"NestedTest\",\n"
            + " \"fields\": [\n"
            + "   {\n"
            + "     \"name\": \"nested\",\n"
            + "     \"type\": {\n"
            + "       \"type\": \"record\",\n"
            + "       \"name\": \"InnerRecord\",\n"
            + "       \"fields\": [\n"
            + "         {\"name\": \"uuid_field\", \"type\": {\"type\": \"string\", \"logicalType\": \"uuid\"}}\n"
            + "       ]\n"
            + "     }\n"
            + "   }\n"
            + " ]\n"
            + "}";

        Schema avroSchema = new Schema.Parser().parse(avroSchemaStr);

        // Convert with Unity Catalog
        org.apache.iceberg.Schema icebergSchema = AvroSchemaUtilExtended.toIceberg(avroSchema, false, true);

        // Verify nested UUID is converted to STRING
        Types.NestedField nestedField = icebergSchema.findField("nested");
        assertNotNull(nestedField);

        Types.StructType nestedStruct = nestedField.type().asNestedType().asStructType();
        Types.NestedField uuidField = nestedStruct.field("uuid_field");
        assertNotNull(uuidField);
        assertEquals(Types.StringType.get(), uuidField.type());
    }

    @Test
    public void testArrayOfUUIDConversion() {
        // Create schema with array of UUIDs
        String avroSchemaStr = "{\n"
            + " \"type\": \"record\",\n"
            + " \"name\": \"ArrayTest\",\n"
            + " \"fields\": [\n"
            + "   {\n"
            + "     \"name\": \"uuids\",\n"
            + "     \"type\": {\n"
            + "       \"type\": \"array\",\n"
            + "       \"items\": {\"type\": \"string\", \"logicalType\": \"uuid\"}\n"
            + "     }\n"
            + "   }\n"
            + " ]\n"
            + "}";

        Schema avroSchema = new Schema.Parser().parse(avroSchemaStr);

        // Convert with Unity Catalog
        org.apache.iceberg.Schema icebergSchema = AvroSchemaUtilExtended.toIceberg(avroSchema, false, true);

        // Verify array element type is STRING
        Types.NestedField arrayField = icebergSchema.findField("uuids");
        assertNotNull(arrayField);

        Types.ListType listType = arrayField.type().asListType();
        assertEquals(Types.StringType.get(), listType.elementType());
    }
}