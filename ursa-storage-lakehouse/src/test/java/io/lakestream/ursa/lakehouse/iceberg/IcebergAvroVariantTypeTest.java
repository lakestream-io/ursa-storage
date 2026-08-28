/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.lakehouse.iceberg;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import com.google.gson.Gson;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.LogicalType;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.Table;
import org.apache.iceberg.avro.AvroSchemaUtil;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetReaders;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.hadoop.HadoopOutputFile;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.variants.Variant;
import org.apache.iceberg.variants.VariantMetadata;
import org.apache.iceberg.variants.Variants;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

@Slf4j
public class IcebergAvroVariantTypeTest {

    private HadoopCatalog catalog;
    private TableIdentifier tableId;
    private Table table;
    private File catalogDir;

    @Before
    public void setup() {
        catalogDir = new File("/tmp/iceberg_avro_catalog_" + System.currentTimeMillis());
        catalogDir.mkdirs();

        catalog = new HadoopCatalog(new Configuration(), catalogDir.getAbsolutePath());
        tableId = TableIdentifier.of("default", "avro_variant_table");
    }

    @After
    public void cleanup() {
        // Uncomment for cleanup
//        if (catalog != null && tableId != null) {
//            try {
//                catalog.dropTable(tableId, true);
//            } catch (Exception e) {
//                // Ignore cleanup errors
//            }
//        }
//        deleteRecursively(catalogDir);
    }

    public static Schema buildVariantRecord(
            String recordName,
            String namespace,
            List<String> metadataFields) {
        // metadata is a map<string>
        Schema metadataSchema = Schema.createMap(Schema.create(Schema.Type.STRING));

        // value is a string (Iceberg will parse JSON or primitive inside)
        Schema valueSchema = Schema.create(Schema.Type.STRING);

        // Create the record
        Schema variantRecord = Schema.createRecord(
                recordName,
                null,
                namespace,
                false
        );

        variantRecord.setFields(List.of(
                new Schema.Field("metadata", metadataSchema, null, (Object) null),
                new Schema.Field("value", valueSchema, null, (Object) null)
        ));

        // Add Iceberg logicalType="variant"
        new LogicalType("variant").addToSchema(variantRecord);

        // Add your custom metadata key
        variantRecord.addProp(
                "variant-metadata-fields",
                new Gson().toJson(metadataFields)    // produces ["age","city",...]
        );

        return variantRecord;
    }


    static Schema record(String name, Schema.Field... fields) {
        return Schema.createRecord(name, null, null, false, Arrays.asList(fields));
    }

    static Schema variant(String name) {
        Schema schema =
                record(
                        name,
                        new Schema.Field("metadata", Schema.create(Schema.Type.BYTES), null, null),
                        new Schema.Field("value", Schema.create(Schema.Type.BYTES), null, null));

        // Add logical type to record
        new LogicalType("variant").addToSchema(schema);

        // Add metadata fields property
        schema.addProp(
                "variant-metadata-fields",
                "[\"age\", \"city\", \"active\", \"score\"]"
        );
        return schema;
    }

    @Test
    public void testAvroVariantToIcebergEndToEnd() throws IOException {
        String s1 = "{\"type\":\"record\",\"name\":\"AvroVariantRecord\",\"namespace\":\"io.lakestream.test\",\"fields\":[{\"name\":\"id\",\"type\":\"long\"},{\"name\":\"name\",\"type\":\"string\"},{\"name\":\"attributes\",\"type\":[\"null\",{\"type\":\"record\",\"name\":\"attribute_variant\",\"namespace\":\"\",\"fields\":[{\"name\":\"metadata\",\"type\":\"bytes\"},{\"name\":\"value\",\"type\":\"bytes\"}],\"logicalType\":\"variant\",\"variant-metadata-fields\":\"[\\\"age\\\", \\\"city\\\", \\\"active\\\", \\\"score\\\"]\"}],\"default\":null}]}";
        var parsedS1 = new Schema.Parser().parse(s1);

        String s2 = "{\"type\":\"record\",\"name\":\"UserEventWithVariantV1\",\"namespace\":\"com.example\",\"fields\":[{\"name\":\"id\",\"type\":[\"null\",\"string\"]},{\"name\":\"age\",\"type\":\"long\"},{\"name\":\"active\",\"type\":\"boolean\"},{\"name\":\"score\",\"type\":\"double\"},{\"name\":\"tags\",\"type\":{\"type\":\"array\",\"items\":\"string\"},\"default\":[]},{\"name\":\"attributes\",\"type\":{\"type\":\"map\",\"values\":\"string\"},\"default\":{}},{\"name\":\"address\",\"type\":{\"type\":\"record\",\"name\":\"Address\",\"fields\":[{\"name\":\"street\",\"type\":\"string\"},{\"name\":\"city\",\"type\":\"string\"}],\"logicalType\":\"variant\",\"variant-metadata-fields\":\"[\\\"street\\\", \\\"city\\\"]\"}}]}";
        var parsedS2 = new Schema.Parser().parse(s2);



        Schema avroSchema = SchemaBuilder.record("AvroVariantRecord")
                .namespace("io.lakestream.test")
                .fields()
                .requiredLong("id")
                .requiredString("name")
                .name("attributes")
                    .type().unionOf().nullType().and().type(variant("attribute_variant")).endUnion()
                    .nullDefault()
                .endRecord();

        // Re-parse to activate the logical type registry
        var avroSchema2 = new Schema.Parser().parse(avroSchema.toString(true));

        log.info("Final Avro Schema: {}", avroSchema2.toString(true));

        // 2. Create Avro GenericRecords with variant data as strings
        GenericRecord avroRecord1 = new GenericData.Record(avroSchema2);
        avroRecord1.put("id", 1L);
        avroRecord1.put("name", "Alice");
        avroRecord1.put("attributes", "{\"age\": 25, \"city\": \"San Francisco\"}");

        GenericRecord avroRecord2 = new GenericData.Record(avroSchema);
        avroRecord2.put("id", 2L);
        avroRecord2.put("name", "Bob");
        avroRecord2.put("attributes", "{\"age\": 30, \"active\": true}");

        GenericRecord avroRecord3 = new GenericData.Record(avroSchema);
        avroRecord3.put("id", 3L);
        avroRecord3.put("name", "Charlie");
        avroRecord3.put("attributes", "[1, 2, 3, 4, 5]");

        GenericRecord avroRecord4 = new GenericData.Record(avroSchema);
        avroRecord4.put("id", 4L);
        avroRecord4.put("name", "Diana");
        avroRecord4.put("attributes", 42.5);

        GenericRecord avroRecord5 = new GenericData.Record(avroSchema);
        avroRecord5.put("id", 5L);
        avroRecord5.put("name", "Eve");
        avroRecord5.put("attributes", "\"simple string value\"");

        // 3. Convert Avro schema to Iceberg schema using AvroSchemaUtil
//        org.apache.iceberg.Schema icebergSchema = AvroSchemaUtilExtended.toIceberg(avroSchema);
        org.apache.iceberg.Schema icebergSchema = AvroSchemaUtil.toIceberg(avroSchema2);
        log.info("Iceberg Schema (before variant conversion): {}", icebergSchema);

        // Extract variant metadata from Avro schema
        VariantMetadata variantMetadata = extractVariantMetadataFromSchema(avroSchema);
        log.info("Extracted Variant Metadata fields: {}",
                Arrays.toString(getMetadataFieldNames(variantMetadata)));

        // Verify the schema conversion
        assertNotNull("Iceberg schema should not be null", icebergSchema);
        assertEquals("Should have 3 fields", 3, icebergSchema.columns().size());
        assertEquals("Field 'attributes' should be VariantType",
                org.apache.iceberg.types.Types.VariantType.get(),
                icebergSchema.findField("attributes").type());

        // 4. Create Iceberg table with format version 3
        Map<String, String> props = new HashMap<>();
        props.put("format-version", "3");
        table = catalog.createTable(tableId, icebergSchema, null, props);
        assertNotNull("Table should be created", table);
        assertEquals("Table should use format version 3", 3,
                ((BaseTable) table).operations().current().formatVersion());

        // 5. Convert Avro GenericRecords to Iceberg Records and write
        File dataFile = new File("/tmp/avro-variant-" + System.currentTimeMillis() + ".parquet");
        OutputFile out = HadoopOutputFile.fromPath(
                new org.apache.hadoop.fs.Path(dataFile.getAbsolutePath()),
                new Configuration()
        );

        FileAppender<Record> appender = Parquet.write(out)
                .schema(icebergSchema)
                .createWriterFunc(fileSchema ->
                        org.apache.iceberg.data.parquet.GenericParquetWriter.create(icebergSchema, fileSchema))
                .build();

        try {
            // Convert and write each Avro record as Iceberg record
//            appender.add(convertAvroToIcebergRecord(avroRecord1, icebergSchema, variantMetadata));
//            appender.add(convertAvroToIcebergRecord(avroRecord2, icebergSchema, variantMetadata));
//            appender.add(convertAvroToIcebergRecord(avroRecord3, icebergSchema, variantMetadata));
//            appender.add(convertAvroToIcebergRecord(avroRecord4, icebergSchema, variantMetadata));
//            appender.add(convertAvroToIcebergRecord(avroRecord5, icebergSchema, variantMetadata));

            appender.add(AvroToIcebergConverter.convert(avroRecord1, icebergSchema));
            appender.add(AvroToIcebergConverter.convert(avroRecord2, icebergSchema));
            appender.add(AvroToIcebergConverter.convert(avroRecord3, icebergSchema));
            appender.add(AvroToIcebergConverter.convert(avroRecord4, icebergSchema));
            appender.add(AvroToIcebergConverter.convert(avroRecord5, icebergSchema));
        } finally {
            appender.close();
        }

        // 6. Commit to Iceberg table
        DataFile dataFileToAdd = DataFiles.builder(table.spec())
                .withPath(dataFile.getAbsolutePath())
                .withFileSizeInBytes(dataFile.length())
                .withRecordCount(5)
                .build();

        table.newAppend()
                .appendFile(dataFileToAdd)
                .commit();

        log.info("✓ Data committed to Iceberg table");

        // 7. Read the Iceberg table and verify values
        InputFile inputFile = table.io().newInputFile(dataFile.getAbsolutePath());

        try (CloseableIterable<org.apache.iceberg.data.GenericRecord> reader = Parquet.read(inputFile)
                .project(icebergSchema)
                .createReaderFunc(fileSchema -> GenericParquetReaders.buildReader(icebergSchema, fileSchema))
                .build()) {

            int recordCount = 0;
            for (org.apache.iceberg.data.GenericRecord record : reader) {
                recordCount++;
                Long id = (Long) record.getField("id");
                String name = (String) record.getField("name");
                Variant attributes = (Variant) record.getField("attributes");

                assertNotNull("ID should not be null", id);
                assertNotNull("Name should not be null", name);
                assertNotNull("Attributes should not be null", attributes);

                log.info("Record {} : name={}, attributes={}", id, name, attributes);

                // Verify specific values
                switch (id.intValue()) {
                    case 1:
                        assertEquals("Alice", name);
                        assertTrue("Should contain variant data", attributes.value() != null);
                        log.info("  → Variant type: {}", attributes.value().type());
                        break;
                    case 2:
                        assertEquals("Bob", name);
                        assertTrue("Should contain variant data", attributes.value() != null);
                        break;
                    case 3:
                        assertEquals("Charlie", name);
                        assertTrue("Should contain variant data (array)", attributes.value() != null);
                        break;
                    case 4:
                        assertEquals("Diana", name);
                        assertTrue("Should contain variant data (number)", attributes.value() != null);
                        break;
                    case 5:
                        assertEquals("Eve", name);
                        assertTrue("Should contain variant data (string)", attributes.value() != null);
                        break;
                }
            }

            assertEquals("Should have read 5 records", 5, recordCount);
        }

        // 8. Verify table metadata
        assertEquals("Table should have 1 snapshot", 1,
                table.snapshots().spliterator().getExactSizeIfKnown());
        assertNotNull("Current snapshot should exist", table.currentSnapshot());

        log.info("✓ Avro to Iceberg variant type test passed successfully!");
    }

    /**
     * Extract VariantMetadata from Avro schema properties.
     */
    private VariantMetadata extractVariantMetadataFromSchema(Schema avroSchema) {
        for (Schema.Field field : avroSchema.getFields()) {
            if (field.schema().getProp("logicalType") != null
                    && field.schema().getProp("logicalType").equals("variant")) {

                String metadataFieldsJson = field.schema().getProp("variant-metadata-fields");
                if (metadataFieldsJson != null) {
                    // Parse JSON array of field names
                    // Simple parsing for array format: ["field1", "field2", ...]
                    String[] fieldNames = parseJsonArray(metadataFieldsJson);
                    if (fieldNames.length > 0) {
                        return Variants.metadata(fieldNames);
                    }
                }
            }
        }

        // Return empty metadata if not found
        return VariantMetadata.empty();
    }

    /**
     * Simple JSON array parser for field names.
     */
    private String[] parseJsonArray(String json) {
        // Remove brackets and whitespace
        String content = json.trim().replaceAll("^\\[|\\]$", "");
        if (content.isEmpty()) {
            return new String[0];
        }

        // Split by comma and clean quotes
        String[] parts = content.split(",");
        List<String> fieldNames = new ArrayList<>();

        for (String part : parts) {
            String cleaned = part.trim().replaceAll("^\"|\"$", "");
            if (!cleaned.isEmpty()) {
                fieldNames.add(cleaned);
            }
        }

        return fieldNames.toArray(new String[0]);
    }

    /**
     * Helper method to get field names from VariantMetadata for debugging.
     */
    private String[] getMetadataFieldNames(VariantMetadata metadata) {
        if (metadata == null) {
            return new String[0];
        }

        // Note: This is a simplified approach for debugging
        // In production, you'd access the internal dictionary properly
        List<String> fields = new ArrayList<>();
        try {
            // Try to get field count and iterate
            for (int i = 0; i < 100; i++) { // Reasonable upper limit
                try {
                    String fieldName = metadata.get(i);
                    if (fieldName != null) {
                        fields.add(fieldName);
                    }
                } catch (Exception e) {
                    break;
                }
            }
        } catch (Exception e) {
            // Return empty if we can't introspect
        }

        return fields.toArray(new String[0]);
    }

    /**
     * Convert Avro schema to Iceberg schema, handling variant logical types.
     * Post-processes AvroSchemaUtil output to convert string fields with variant logical type
     */
    private org.apache.iceberg.Schema convertVariantLogicalTypes(
            Schema avroSchema,
            org.apache.iceberg.Schema icebergSchema) {

        // Check each field in the Avro schema for variant logical type
        boolean hasVariantFields = false;
        for (Schema.Field avroField : avroSchema.getFields()) {
            if (avroField.schema().getProp("logicalType") != null
                    && avroField.schema().getProp("logicalType").equals("variant")) {
                hasVariantFields = true;
                break;
            }
        }

        if (!hasVariantFields) {
            return icebergSchema;
        }

        // Rebuild schema with variant types
        java.util.List<org.apache.iceberg.types.Types.NestedField> newFields = new java.util.ArrayList<>();

        for (org.apache.iceberg.types.Types.NestedField field : icebergSchema.columns()) {
            Schema.Field avroField = avroSchema.getField(field.name());

            if (avroField != null
                    && avroField.schema().getProp("logicalType") != null
                    && avroField.schema().getProp("logicalType").equals("variant")) {
                // Replace string type with variant type
                org.apache.iceberg.types.Types.NestedField variantField;
                if (field.isRequired()) {
                    variantField = org.apache.iceberg.types.Types.NestedField.required(
                            field.fieldId(), field.name(), org.apache.iceberg.types.Types.VariantType.get());
                } else {
                    variantField = org.apache.iceberg.types.Types.NestedField.optional(
                            field.fieldId(), field.name(), org.apache.iceberg.types.Types.VariantType.get());
                }
                newFields.add(variantField);
            } else {
                newFields.add(field);
            }
        }

        return new org.apache.iceberg.Schema(newFields);
    }

    /**
     * Convert Avro GenericRecord to Iceberg GenericRecord.
     * Handles conversion from String to Variant type
     */
    private org.apache.iceberg.data.GenericRecord convertAvroToIcebergRecord(
            GenericRecord avroRecord,
            org.apache.iceberg.Schema icebergSchema,
            VariantMetadata variantMetadata) {

        org.apache.iceberg.data.GenericRecord icebergRecord =
                org.apache.iceberg.data.GenericRecord.create(icebergSchema);

        icebergRecord.setField("id", avroRecord.get("id"));
        icebergRecord.setField("name", avroRecord.get("name"));

        // Convert string to Variant
        String attributesString = (String) avroRecord.get("attributes");
        if (attributesString != null) {
            // Parse the string and create a Variant
            Variant variant = parseStringToVariant(attributesString, variantMetadata);
            icebergRecord.setField("attributes", variant);
        }

        return icebergRecord;
    }

    /**
     * Parse a string representation to Variant.
     * Handles JSON objects, arrays, primitives, and plain strings
     */
    private Variant parseStringToVariant(String value, VariantMetadata metadata) {
        if (value == null || value.isEmpty()) {
            return Variant.of(VariantMetadata.empty(), Variants.of("null"));
        }

        String trimmed = value.trim();

        // Check if it's a JSON object or array
        if ((trimmed.startsWith("{") && trimmed.endsWith("}"))
                || (trimmed.startsWith("[") && trimmed.endsWith("]"))) {
            // It's a JSON object or array
            return Variant.of(metadata, Variants.of(trimmed));
        }

        // Check if it's a quoted string
        if (trimmed.startsWith("\"") && trimmed.endsWith("\"")) {
            // Remove quotes and treat as string
            String unquoted = trimmed.substring(1, trimmed.length() - 1);
            return Variant.of(VariantMetadata.empty(), Variants.of(unquoted));
        }

        // Try to parse as number
        try {
            if (trimmed.contains(".")) {
                double doubleValue = Double.parseDouble(trimmed);
                return Variant.of(VariantMetadata.empty(), Variants.of(doubleValue));
            } else {
                long longValue = Long.parseLong(trimmed);
                return Variant.of(VariantMetadata.empty(), Variants.of(longValue));
            }
        } catch (NumberFormatException e) {
            // Not a number
        }

        // Check for boolean
        if (trimmed.equalsIgnoreCase("true") || trimmed.equalsIgnoreCase("false")) {
            return Variant.of(VariantMetadata.empty(), Variants.of(Boolean.parseBoolean(trimmed)));
        }

        // Check for null
        if (trimmed.equalsIgnoreCase("null")) {
            return Variant.of(VariantMetadata.empty(), Variants.of("null"));
        }

        // Default: treat as plain string
        return Variant.of(VariantMetadata.empty(), Variants.of(trimmed));
    }

    private void deleteRecursively(File file) {
        if (file == null || !file.exists()) {
            return;
        }
        if (file.isDirectory()) {
            File[] files = file.listFiles();
            if (files != null) {
                for (File child : files) {
                    deleteRecursively(child);
                }
            }
        }
        file.delete();
    }
}