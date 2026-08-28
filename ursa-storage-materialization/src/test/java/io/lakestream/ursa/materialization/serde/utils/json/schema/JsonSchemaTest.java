/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.materialization.serde.utils.json.schema;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.stream.Collectors;
import org.apache.avro.Schema;
import org.intellij.lang.annotations.Language;
import org.junit.jupiter.api.Test;

public class JsonSchemaTest {

    @Test
    public void testBasicJsonSchemaParsing() {
        String jsonSchemaStr = "{\n"
            + "  \"type\": \"object\",\n"
            + "  \"properties\": {\n"
            + "    \"name\": { \"type\": \"string\" },\n"
            + "    \"age\": { \"type\": \"integer\" }\n"
            + "  }\n"
            + "}";

        JsonSchema jsonSchema = new JsonSchema(jsonSchemaStr);
        assertNotNull(jsonSchema);
        assertEquals("object", jsonSchema.getType().getValue());
        assertEquals(2, jsonSchema.getFields().size());
    }

    @Test
    public void testJsonSchemaWithRequiredFields() {
        String jsonSchemaStr = "{\n"
            + "  \"type\": \"object\",\n"
            + "  \"properties\": {\n"
            + "    \"name\": { \"type\": \"string\" },\n"
            + "    \"email\": { \"type\": \"string\" }\n"
            + "  },\n"
            + "  \"required\": [\"name\"]\n"
            + "}";

        JsonSchema jsonSchema = new JsonSchema(jsonSchemaStr);
        assertNotNull(jsonSchema);

        // Check that the 'name' field is marked as required
        var nameField = jsonSchema.getFields().stream()
            .filter(f -> f.getName().equals("name"))
            .findFirst().orElse(null);
        assertNotNull(nameField);
        assertTrue(nameField.isRequired());
    }

    @Test
    public void testJsonSchemaWithFormats() {
        String jsonSchemaStr = "{\n"
            + "  \"type\": \"object\",\n"
            + "  \"properties\": {\n"
            + "    \"email\": { \"type\": \"string\", \"format\": \"email\" },\n"
            + "    \"uri\": { \"type\": \"string\", \"format\": \"uri\" },\n"
            + "    \"uuid\": { \"type\": \"string\", \"format\": \"uuid\" },\n"
            + "    \"date\": { \"type\": \"string\", \"format\": \"date\" },\n"
            + "    \"datetime\": { \"type\": \"string\", \"format\": \"date-time\" }\n"
            + "  }\n"
            + "}";

        JsonSchema jsonSchema = new JsonSchema(jsonSchemaStr);
        assertNotNull(jsonSchema);
        assertEquals(5, jsonSchema.getFields().size());

        // Verify that format fields are parsed correctly
        var emailField = jsonSchema.getFields().stream()
            .filter(f -> f.getName().equals("email"))
            .findFirst().orElse(null);
        assertNotNull(emailField);
        assertEquals(io.lakestream.ursa.materialization.serde.utils.json.schema.Format.EMAIL, emailField.getFormat());
    }

    @Test
    public void testJsonSchemaWithDefAndOneOfCase() {
        String jsonSchemaStr =
            "    {\"$schema\":\"http://json-schema.org/draft-07/schema#\",\"title\":\"TestRecord\","
                + "\"type\":\"object\",\"additionalProperties\":false,\"properties\":{\"id\":{\"type\":\"integer\"},"
                + "\"name\":{\"oneOf\":[{\"type\":\"null\",\"title\":\"Not included\"},{\"type\":\"string\"}]},"
                + "\"location\":{\"oneOf\":[{\"type\":\"null\",\"title\":\"Not included\"},"
                + "{\"$ref\":\"#/definitions/LocationV4\"}]}},\"required\":[\"id\"],"
                + "\"definitions\":{\"LocationV4\":{\"type\":\"object\",\"additionalProperties\":false,"
                + "\"properties\":{\"id\":{\"type\":\"integer\"},\"x\":{\"type\":\"integer\"},"
                + "\"y\":{\"type\":\"integer\"},\"z\":{\"type\":\"integer\"}},\"required\":[\"id\",\"x\",\"y\","
                + "\"z\"]}}}\n";

        JsonSchema jsonSchema = new JsonSchema(jsonSchemaStr);
        Schema avroSchema = jsonSchema.toAvroSchema();

        assertEquals(Schema.Type.RECORD, avroSchema.getType());
        assertEquals("TestRecord", avroSchema.getName());
        assertEquals(3, avroSchema.getFields().size());

        assertEquals(Schema.Type.INT, avroSchema.getField("id").schema().getType());

        Schema nameSchema = avroSchema.getField("name").schema();
        assertEquals(Schema.Type.UNION, nameSchema.getType());
        assertEquals(Schema.Type.STRING, getRealSchema(nameSchema).getType());

        Schema locationSchema = avroSchema.getField("location").schema();
        assertEquals(Schema.Type.UNION, locationSchema.getType());

        Schema realLocationSchema = getRealSchema(locationSchema);
        assertEquals(Schema.Type.RECORD, realLocationSchema.getType());

        assertEquals(4, realLocationSchema.getFields().size());

        assertEquals(Schema.Type.INT, realLocationSchema.getField("id").schema().getType());
        assertEquals(Schema.Type.INT, realLocationSchema.getField("x").schema().getType());
        assertEquals(Schema.Type.INT, realLocationSchema.getField("y").schema().getType());
        assertEquals(Schema.Type.INT, realLocationSchema.getField("z").schema().getType());
    }

    private static Schema getRealSchema(Schema schema) {
        if (schema.isUnion()) {
            List<Schema> types = schema.getTypes();
            for (Schema subSchema : types) {
                if (subSchema.getType() != Schema.Type.NULL) {
                    return subSchema;
                }
            }
        }
        return schema;
    }


    @Test
    public void testJsonSchemaToAvroConversion() {
        String jsonSchemaStr = "{\n"
            + "  \"type\": \"object\",\n"
            + "  \"title\": \"TestRecord\",\n"
            + "  \"properties\": {\n"
            + "    \"name\": { \"type\": \"string\" },\n"
            + "    \"age\": { \"type\": \"integer\" },\n"
            + "    \"active\": { \"type\": \"boolean\" }\n"
            + "  }\n"
            + "}";

        JsonSchema jsonSchema = new JsonSchema(jsonSchemaStr);
        Schema avroSchema = jsonSchema.toAvroSchema();

        assertNotNull(avroSchema);
        assertEquals(Schema.Type.RECORD, avroSchema.getType());
        assertEquals("TestRecord", avroSchema.getName());
        assertEquals(3, avroSchema.getFields().size());

        // Verify field types - since no fields are in required, they all become unions with null
        assertTrue(avroSchema.getField("name") != null);
        assertEquals(Schema.Type.UNION, avroSchema.getField("name").schema().getType());
        assertEquals(Schema.Type.UNION, avroSchema.getField("age").schema().getType());
        assertEquals(Schema.Type.UNION, avroSchema.getField("active").schema().getType());
    }

    @Test
    public void testJsonSchemaWithNestedObjects() {
        String jsonSchemaStr = "{\n"
            + "  \"type\": \"object\",\n"
            + "  \"properties\": {\n"
            + "    \"address\": {\n"
            + "      \"type\": \"object\",\n"
            + "      \"properties\": {\n"
            + "        \"street\": { \"type\": \"string\" },\n"
            + "        \"city\": { \"type\": \"string\" }\n"
            + "      }\n"
            + "    }\n"
            + "  }\n"
            + "}";

        JsonSchema jsonSchema = new JsonSchema(jsonSchemaStr);
        Schema avroSchema = jsonSchema.toAvroSchema();

        assertNotNull(avroSchema);
        assertEquals(Schema.Type.RECORD, avroSchema.getType());

        // Verify nested object - the address field becomes a union because it's not required
        Schema addressSchema = avroSchema.getField("address").schema();
        assertEquals(Schema.Type.UNION, addressSchema.getType());

        // Find the RECORD type within the union (should be the second type after NULL)
        Schema recordType = addressSchema.getTypes().stream()
            .filter(s -> s.getType() == Schema.Type.RECORD)
            .findFirst()
            .orElse(null);
        assertNotNull(recordType);
        assertEquals(2, recordType.getFields().size());
    }

    @Test
    public void testJsonSchemaWithArrays() {
        String jsonSchemaStr = "{\n"
            + "  \"type\": \"object\",\n"
            + "  \"properties\": {\n"
            + "    \"tags\": {\n"
            + "      \"type\": \"array\",\n"
            + "      \"items\": { \"type\": \"string\" }\n"
            + "    }\n"
            + "  }\n"
            + "}";

        JsonSchema jsonSchema = new JsonSchema(jsonSchemaStr);
        Schema avroSchema = jsonSchema.toAvroSchema();

        assertNotNull(avroSchema);
        assertEquals(Schema.Type.RECORD, avroSchema.getType());

        // Verify array field - the tags field becomes a union because it's not required
        Schema tagsSchema = avroSchema.getField("tags").schema();
        assertEquals(Schema.Type.UNION, tagsSchema.getType());

        // Find the ARRAY type within the union (should be the second type after NULL)
        Schema arrayType = tagsSchema.getTypes().stream()
            .filter(s -> s.getType() == Schema.Type.ARRAY)
            .findFirst()
            .orElse(null);
        assertNotNull(arrayType);
        // The element type is also a union because it's not required
        assertEquals(Schema.Type.UNION, arrayType.getElementType().getType());

        // Find the STRING type within the union
        Schema stringType = arrayType.getElementType().getTypes().stream()
            .filter(s -> s.getType() == Schema.Type.STRING)
            .findFirst()
            .orElse(null);
        assertNotNull(stringType);
    }

    @Test
    public void testJsonSchemaWithAnyOf() {
        String jsonSchemaStr = "{\n"
            + "  \"type\": \"object\",\n"
            + "  \"properties\": {\n"
            + "    \"value\": {\n"
            + "      \"type\": \"array\",\n"
            + "      \"items\": {\n"
            + "        \"anyOf\": [\n"
            + "          { \"type\": \"string\" },\n"
            + "          { \"type\": \"number\" },\n"
            + "          { \"type\": \"object\", \"properties\": { \"test\": { \"type\": \"string\" } } }\n"
            + "        ]\n"
            + "      }\n"
            + "    }\n"
            + "  }\n"
            + "}";

        JsonSchema jsonSchema = new JsonSchema(jsonSchemaStr);
        Schema avroSchema = jsonSchema.toAvroSchema();

        assertNotNull(avroSchema);
        assertEquals(Schema.Type.RECORD, avroSchema.getType());

        // Verify anyOf array field creates a union schema - the value field itself is also a union because it's not
        // required
        Schema valueSchema = avroSchema.getField("value").schema();
        assertEquals(Schema.Type.UNION, valueSchema.getType());

        // Find the ARRAY type within the union (should be the second type after NULL)
        Schema arrayType = valueSchema.getTypes().stream()
            .filter(s -> s.getType() == Schema.Type.ARRAY)
            .findFirst()
            .orElse(null);
        assertNotNull(arrayType);

        Schema elementType = arrayType.getElementType();
        assertEquals(Schema.Type.UNION, elementType.getType());
        assertEquals(3, elementType.getTypes().size()); // string, double, record
    }

    @Test
    public void testJsonSchemaWithOptionalFields() {
        String jsonSchemaStr = "{\n"
            + "  \"type\": \"object\",\n"
            + "  \"properties\": {\n"
            + "    \"requiredField\": { \"type\": \"string\" },\n"
            + "    \"optionalField\": { \"type\": \"string\" }\n"
            + "  },\n"
            + "  \"required\": [\"requiredField\"]\n"
            + "}";

        JsonSchema jsonSchema = new JsonSchema(jsonSchemaStr);
        Schema avroSchema = jsonSchema.toAvroSchema();

        assertNotNull(avroSchema);
        assertEquals(Schema.Type.RECORD, avroSchema.getType());

        // Required field should be just the type
        Schema requiredFieldSchema = avroSchema.getField("requiredField").schema();
        assertEquals(Schema.Type.STRING, requiredFieldSchema.getType());

        // Optional field should be a union with null
        Schema optionalFieldSchema = avroSchema.getField("optionalField").schema();
        assertEquals(Schema.Type.UNION, optionalFieldSchema.getType());
        assertTrue(optionalFieldSchema.getTypes().stream()
            .anyMatch(s -> s.getType() == Schema.Type.NULL));
    }

    @Test
    public void testInvalidJsonSchemaParsing() {
        String invalidJsonSchemaStr = "{\n"
            + "  \"type\": \"invalid_type\"\n"
            + "}";

        assertThrows(IllegalArgumentException.class, () -> {
            new JsonSchema(invalidJsonSchemaStr);
        });
    }

    @Test
    public void testJsonSchemaWithAllFormatTypes() {
        String jsonSchemaStr = "{\n"
            + "  \"type\": \"object\",\n"
            + "  \"properties\": {\n"
            + "    \"email\": { \"type\": \"string\", \"format\": \"email\" },\n"
            + "    \"uri\": { \"type\": \"string\", \"format\": \"uri\" },\n"
            + "    \"uuid\": { \"type\": \"string\", \"format\": \"uuid\" },\n"
            + "    \"date\": { \"type\": \"string\", \"format\": \"date\" },\n"
            + "    \"time\": { \"type\": \"string\", \"format\": \"time\" },\n"
            + "    \"datetime\": { \"type\": \"string\", \"format\": \"date-time\" },\n"
            + "    \"ipv4\": { \"type\": \"string\", \"format\": \"ipv4\" },\n"
            + "    \"ipv6\": { \"type\": \"string\", \"format\": \"ipv6\" },\n"
            + "    \"hostname\": { \"type\": \"string\", \"format\": \"hostname\" }\n"
            + "  }\n"
            + "}";

        JsonSchema jsonSchema = new JsonSchema(jsonSchemaStr);
        assertEquals(9, jsonSchema.getFields().size());

        // Verify all format types are correctly parsed
        var fieldsByName = jsonSchema.getFields().stream().collect(
            Collectors.toMap(io.lakestream.ursa.materialization.serde.utils.json.schema.Field::getName, f -> f));

        assertTrue(fieldsByName.containsKey("email"));
        assertTrue(fieldsByName.containsKey("uri"));
        assertTrue(fieldsByName.containsKey("uuid"));
        assertTrue(fieldsByName.containsKey("date"));
        assertTrue(fieldsByName.containsKey("time"));
        assertTrue(fieldsByName.containsKey("datetime"));
        assertTrue(fieldsByName.containsKey("ipv4"));
        assertTrue(fieldsByName.containsKey("ipv6"));
        assertTrue(fieldsByName.containsKey("hostname"));
    }

    @Test
    void testJsonObjectSchema() {
        @Language("JSON5") var jsonSchemaStr = """
            {
              "$schema": "http://json-schema.org/draft-07/schema#",
              "title": "Unleash Impressions GetVariant Event",
              "type": "object",
              "properties": {
                "eventType": {
                  "const": "getVariant"
                },
                "eventId": {
                  "type": "string",
                  "format": "uuid"
                },
                "context": {
                  "type": "object",
                  "additionalProperties": {
                    "type": "string"
                  }
                },
                "enabled": {
                  "type": "boolean"
                },
                "featureName": {
                  "type": "string"
                },
                "variant": {
                  "type": "string",
                  "minLength": 1
                },
                "webhook_received_at": {
                  "type": "string",
                  "format": "date-time"
                }
              },
              "required": [
                "eventType",
                "eventId",
                "context",
                "enabled",
                "featureName",
                "variant"
              ],
              "additionalProperties": true
            }
            """;
        JsonSchema jsonSchema = new JsonSchema(jsonSchemaStr);
        Schema avroSchema = jsonSchema.toAvroSchema();

        assertNotNull(avroSchema);
        assertEquals(Schema.Type.RECORD, avroSchema.getType());
        assertEquals("UnleashImpressionsGetVariantEvent", avroSchema.getName());

        // Verify eventType field (const gets treated as string)
        Schema.Field eventTypeField = avroSchema.getField("eventType");
        assertNotNull(eventTypeField);
        assertEquals(Schema.Type.STRING, eventTypeField.schema().getType());

        // Verify eventId field (string with UUID format)
        Schema.Field eventIdField = avroSchema.getField("eventId");
        assertNotNull(eventIdField);
        // Since eventId is required, it should be the direct type, not a union with null
        assertEquals(Schema.Type.STRING, eventIdField.schema().getType());
        // Verify that it has the UUID logical type
        assertNotNull(eventIdField.schema().getObjectProp("logicalType"));
        assertEquals("uuid", eventIdField.schema().getObjectProp("logicalType"));

        // Verify context field (object with additionalProperties)
        Schema.Field contextField = avroSchema.getField("context");
        assertNotNull(contextField);
        assertEquals(Schema.Type.MAP, contextField.schema().getType());
        // The value type should be STRING inside a union (because additionalProperties doesn't define it as required)
        Schema contextValueType = contextField.schema().getValueType();
        assertEquals(Schema.Type.UNION, contextValueType.getType());
        assertTrue(contextValueType.getTypes().stream()
            .anyMatch(s -> s.getType() == Schema.Type.STRING));

        // Verify enabled field (boolean)
        Schema.Field enabledField = avroSchema.getField("enabled");
        assertNotNull(enabledField);
        assertEquals(Schema.Type.BOOLEAN, enabledField.schema().getType());

        // Verify featureName field (string)
        Schema.Field featureNameField = avroSchema.getField("featureName");
        assertNotNull(featureNameField);
        assertEquals(Schema.Type.STRING, featureNameField.schema().getType());

        // Verify variant field (string)
        Schema.Field variantField = avroSchema.getField("variant");
        assertNotNull(variantField);
        assertEquals(Schema.Type.STRING, variantField.schema().getType());

        // Verify webhook_received_at field (string with date-time format)
        Schema.Field webhookReceivedAtField = avroSchema.getField("webhook_received_at");
        assertNotNull(webhookReceivedAtField);
        // Since webhook_received_at is not in required, it should be a union with null
        assertEquals(Schema.Type.UNION, webhookReceivedAtField.schema().getType());
        // Find the actual type within the union
        Schema webhookReceivedAtType = webhookReceivedAtField.schema().getTypes().stream()
            .filter(s -> s.getType() != Schema.Type.NULL)
            .findFirst()
            .orElse(null);
        assertNotNull(webhookReceivedAtType);
        assertEquals(Schema.Type.LONG, webhookReceivedAtType.getType());
        assertNotNull(webhookReceivedAtType.getObjectProp("logicalType"));
        assertEquals("timestamp-micros", webhookReceivedAtType.getObjectProp("logicalType"));
    }
}

