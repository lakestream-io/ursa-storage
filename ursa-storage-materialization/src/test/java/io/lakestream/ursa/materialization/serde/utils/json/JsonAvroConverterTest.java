/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */

package io.lakestream.ursa.materialization.serde.utils.json;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.lakestream.ursa.materialization.serde.utils.json.schema.JsonSchema;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Map;
import java.util.UUID;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.intellij.lang.annotations.Language;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class JsonAvroConverterTest {
    private JsonAvroConverter converter;
    private ObjectMapper objectMapper;

    @BeforeEach
    void setUp() {
        converter = new JsonAvroConverter();
        objectMapper = new ObjectMapper();
    }

    @Test
    void testConvertObjectNodeToGenericRecord() throws Exception {
        // Define a simple Avro schema
        String schemaString = "{\n"
            + "  \"type\": \"record\",\n"
            + "  \"name\": \"TestRecord\",\n"
            + "  \"fields\": [\n"
            + "    {\n"
            + "      \"name\": \"name\",\n"
            + "      \"type\": \"string\"\n"
            + "    },\n"
            + "    {\n"
            + "      \"name\": \"age\",\n"
            + "      \"type\": \"int\"\n"
            + "    },\n"
            + "    {\n"
            + "      \"name\": \"active\",\n"
            + "      \"type\": \"boolean\"\n"
            + "    }\n"
            + "  ]\n"
            + "}";

        Schema schema = new Schema.Parser().parse(schemaString);

        // Create a JSON object
        ObjectNode jsonNode = objectMapper.createObjectNode();
        jsonNode.put("name", "John Doe");
        jsonNode.put("age", 30);
        jsonNode.put("active", true);

        // Convert to GenericRecord
        GenericData.Record record = converter.convertToGenericDataRecord(jsonNode, schema);

        // Verify the conversion
        assertNotNull(record);
        assertEquals("John Doe", record.get("name").toString());
        assertEquals(30, record.get("age"));
        assertEquals(true, record.get("active"));
    }

    @Test
    void testConvertByteArrayToGenericRecord() throws Exception {
        // Define a simple Avro schema
        String schemaString = "{\n"
            + "  \"type\": \"record\",\n"
            + "  \"name\": \"TestRecord\",\n"
            + "  \"fields\": [\n"
            + "    {\n"
            + "      \"name\": \"name\",\n"
            + "      \"type\": \"string\"\n"
            + "    },\n"
            + "    {\n"
            + "      \"name\": \"count\",\n"
            + "      \"type\": \"long\"\n"
            + "    }\n"
            + "  ]\n"
            + "}";

        Schema schema = new Schema.Parser().parse(schemaString);

        // Create a JSON string and convert to bytes
        String jsonString = "{\n"
            + "  \"name\": \"Jane Smith\",\n"
            + "  \"count\": 42\n"
            + "}";
        byte[] jsonData = jsonString.getBytes();

        // Convert to GenericRecord
        GenericData.Record record = converter.convertToGenericDataRecord(jsonData, schema);

        // Verify the conversion
        assertNotNull(record);
        assertEquals("Jane Smith", record.get("name").toString());
        assertEquals(42L, record.get("count"));
    }

    @Test
    void testConvertWithNestedRecord() throws Exception {
        // Define a schema with nested records
        String schemaString = "{\n"
            + "  \"type\": \"record\",\n"
            + "  \"name\": \"Person\",\n"
            + "  \"fields\": [\n"
            + "    {\n"
            + "      \"name\": \"name\",\n"
            + "      \"type\": \"string\"\n"
            + "    },\n"
            + "    {\n"
            + "      \"name\": \"address\",\n"
            + "      \"type\": {\n"
            + "        \"type\": \"record\",\n"
            + "        \"name\": \"Address\",\n"
            + "        \"fields\": [\n"
            + "          {\n"
            + "            \"name\": \"street\",\n"
            + "            \"type\": \"string\"\n"
            + "          },\n"
            + "          {\n"
            + "            \"name\": \"city\",\n"
            + "            \"type\": \"string\"\n"
            + "          }\n"
            + "        ]\n"
            + "      }\n"
            + "    }\n"
            + "  ]\n"
            + "}";

        Schema schema = new Schema.Parser().parse(schemaString);

        // Create a JSON object with nested structure
        ObjectNode addressNode = objectMapper.createObjectNode();
        addressNode.put("street", "123 Main St");
        addressNode.put("city", "New York");

        ObjectNode jsonNode = objectMapper.createObjectNode();
        jsonNode.put("name", "Alice Johnson");
        jsonNode.set("address", addressNode);

        // Convert to GenericRecord
        GenericData.Record record = converter.convertToGenericDataRecord(jsonNode, schema);

        // Verify the conversion
        assertNotNull(record);
        assertEquals("Alice Johnson", record.get("name").toString());

        GenericRecord addressRecord = (GenericRecord) record.get("address");
        assertNotNull(addressRecord);
        assertEquals("123 Main St", addressRecord.get("street").toString());
        assertEquals("New York", addressRecord.get("city").toString());
    }

    @Test
    void testConvertWithArrayField() throws Exception {
        // Define a schema with an array field
        String schemaString = "{\n"
            + "  \"type\": \"record\",\n"
            + "  \"name\": \"User\",\n"
            + "  \"fields\": [\n"
            + "    {\n"
            + "      \"name\": \"name\",\n"
            + "      \"type\": \"string\"\n"
            + "    },\n"
            + "    {\n"
            + "      \"name\": \"tags\",\n"
            + "      \"type\": {\n"
            + "        \"type\": \"array\",\n"
            + "        \"items\": \"string\"\n"
            + "      }\n"
            + "    }\n"
            + "  ]\n"
            + "}";

        Schema schema = new Schema.Parser().parse(schemaString);

        // Create a JSON object with an array
        ObjectNode jsonNode = objectMapper.createObjectNode();
        jsonNode.put("name", "Bob Wilson");

        ArrayNode tagsArray = objectMapper.createArrayNode();
        tagsArray.add("developer");
        tagsArray.add("java");
        tagsArray.add("avro");
        jsonNode.set("tags", tagsArray);

        // Convert to GenericRecord
        GenericData.Record record = converter.convertToGenericDataRecord(jsonNode, schema);

        // Verify the conversion
        assertNotNull(record);
        assertEquals("Bob Wilson", record.get("name").toString());

        @SuppressWarnings("unchecked")
        java.util.List<Object> tags = (java.util.List<Object>) record.get("tags");
        assertNotNull(tags);
        assertEquals(3, tags.size());
        assertEquals("developer", tags.get(0).toString());
        assertEquals("java", tags.get(1).toString());
        assertEquals("avro", tags.get(2).toString());
    }

    @Test
    void testConvertWithNullValues() throws Exception {
        // Define a schema with optional fields (unions with null)
        String schemaString = "{\n"
            + "  \"type\": \"record\",\n"
            + "  \"name\": \"OptionalFields\",\n"
            + "  \"fields\": [\n"
            + "    {\n"
            + "      \"name\": \"requiredField\",\n"
            + "      \"type\": \"string\"\n"
            + "    },\n"
            + "    {\n"
            + "      \"name\": \"optionalField\",\n"
            + "      \"type\": [\"null\", \"string\"]\n"
            + "    }\n"
            + "  ]\n"
            + "}";

        Schema schema = new Schema.Parser().parse(schemaString);

        // Create a JSON object with null value
        ObjectNode jsonNode = objectMapper.createObjectNode();
        jsonNode.put("requiredField", "some value");
        jsonNode.putNull("optionalField");

        // Convert to GenericRecord
        GenericData.Record record = converter.convertToGenericDataRecord(jsonNode, schema);

        // Verify the conversion
        assertNotNull(record);
        assertEquals("some value", record.get("requiredField").toString());
        assertNull(record.get("optionalField"));
    }

    @Test
    void testObjectWithMap() throws Exception {
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

        var value = objectMapper.createObjectNode();

        value.put("eventType", "getVariant");
        value.put("eventId", UUID.randomUUID().toString());

        var contextNode = objectMapper.createObjectNode();
        contextNode.put("userId", "user-1234");
        contextNode.put("appName", "my-app");
        value.set("context", contextNode);

        value.put("enabled", true);
        value.put("featureName", "dark_mode");
        value.put("variant", "on");
        String now = Instant.now()
            .atOffset(ZoneOffset.UTC)
            .toString();
        value.put("webhook_received_at", now);

        var record = converter.convertToGenericDataRecord(value, avroSchema);

        assertEquals(value.get("eventType").textValue(), record.get("eventType"));
        assertEquals(value.get("eventId").textValue(), record.get("eventId").toString());
        // Check context field - should be a map with string values
        var contextValue = (Map) record.get("context");
        assertNotNull(contextValue);
        assertEquals(contextNode.get("userId").textValue(), contextValue.get("userId"));
        assertEquals(contextNode.get("appName").textValue(), contextValue.get("appName"));
        assertEquals(value.get("enabled").booleanValue(), record.get("enabled"));
        assertEquals(value.get("featureName").textValue(), record.get("featureName").toString());
        assertEquals(value.get("variant").textValue(), record.get("variant").toString());
    }
}
