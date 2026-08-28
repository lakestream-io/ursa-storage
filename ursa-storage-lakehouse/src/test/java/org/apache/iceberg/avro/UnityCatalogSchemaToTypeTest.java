/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package org.apache.iceberg.avro;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;

public class UnityCatalogSchemaToTypeTest {

    @Test
    public void testUUIDTypeConversion() {
        // Create an Avro schema with UUID type
        Schema uuidSchema = Schema.create(Schema.Type.STRING);
        uuidSchema.addProp("logicalType", "uuid");
        LogicalTypes.uuid().addToSchema(uuidSchema);

        // Convert using UnityCatalogSchemaToType
        UnityCatalogSchemaToType visitor = new UnityCatalogSchemaToType(uuidSchema);
        Type result = UnityCatalogSchemaToType.visit(uuidSchema, visitor, false);

        // Verify UUID is converted to STRING
        assertNotNull(result);
        assertEquals(Types.StringType.get(), result);
    }

    @Test
    public void testRecordWithUUIDField() {
        // Create a record schema with a UUID field
        String schemaStr = "{\n"
            + " \"type\": \"record\",\n"
            + " \"name\": \"TestUUID\",\n"
            + " \"fields\": [\n"
            + "   {\"name\": \"id\", \"type\": {\"type\": \"string\", \"logicalType\": \"uuid\"}}\n"
            + " ]\n"
            + "}";
        Schema avroSchema = new Schema.Parser().parse(schemaStr);

        // Convert using UnityCatalogSchemaToType
        UnityCatalogSchemaToType visitor = new UnityCatalogSchemaToType(avroSchema);
        Type result = UnityCatalogSchemaToType.visit(avroSchema, visitor, false);

        // Verify the result is a struct with a string field
        assertNotNull(result);
        Types.StructType structType = result.asNestedType().asStructType();
        assertEquals(1, structType.fields().size());

        Types.NestedField idField = structType.field("id");
        assertNotNull(idField);
        assertEquals(Types.StringType.get(), idField.type());
    }

    @Test
    public void testNonUUIDTypesUnchanged() {
        // Test that non-UUID types remain unchanged
        Schema stringSchema = Schema.create(Schema.Type.STRING);
        Schema intSchema = Schema.create(Schema.Type.INT);
        Schema longSchema = Schema.create(Schema.Type.LONG);

        UnityCatalogSchemaToType stringVisitor = new UnityCatalogSchemaToType(stringSchema);
        Type stringResult = UnityCatalogSchemaToType.visit(stringSchema, stringVisitor, false);
        assertEquals(Types.StringType.get(), stringResult);

        UnityCatalogSchemaToType intVisitor = new UnityCatalogSchemaToType(intSchema);
        Type intResult = UnityCatalogSchemaToType.visit(intSchema, intVisitor, false);
        assertEquals(Types.IntegerType.get(), intResult);

        UnityCatalogSchemaToType longVisitor = new UnityCatalogSchemaToType(longSchema);
        Type longResult = UnityCatalogSchemaToType.visit(longSchema, longVisitor, false);
        assertEquals(Types.LongType.get(), longResult);
    }
}