/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.lakehouse.iceberg;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.parquet.GenericParquetReaders;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.hadoop.HadoopOutputFile;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.variants.Variant;
import org.apache.iceberg.variants.VariantMetadata;
import org.apache.iceberg.variants.Variants;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;



public class IcebergVariantTypeTest {

    private HadoopCatalog catalog;
    private TableIdentifier tableId;
    private Table table;
    private File catalogDir;

    @Before
    public void setup() {
        catalogDir = new File("/tmp/iceberg_catalog_" + System.currentTimeMillis());
        catalogDir.mkdirs();

        catalog = new HadoopCatalog(new Configuration(), catalogDir.getAbsolutePath());
        tableId = TableIdentifier.of("default", "example_variant");
    }

    @After
    public void cleanup() {
        if (catalog != null && tableId != null) {
            try {
                catalog.dropTable(tableId, true);
            } catch (Exception e) {
                // Ignore cleanup errors
            }
        }
        deleteRecursively(catalogDir);
    }

    @Test
    public void testVariantTypeEndToEnd() throws IOException {
        // 1. Create schema with Variant type
        Schema schema = new Schema(
                Types.NestedField.required(1, "id", Types.LongType.get()),
                Types.NestedField.optional(2, "name", Types.StringType.get()),
                Types.NestedField.optional(3, "attributes", Types.VariantType.get())
        );

        // 2. Create table with format version 3 (required for Variant type)
        Map<String, String> props = new HashMap<>();
        props.put("format-version", "3");

        table = catalog.createTable(tableId, schema, null, props);
        assertNotNull("Table should be created", table);
        assertEquals("Table should use format version 3", 3,
                ((BaseTable) table).operations().current().formatVersion());

        // 3. Write data with Variant values
        File dataFile = new File("/tmp/example-variant-" + System.currentTimeMillis() + ".parquet");
        OutputFile out = HadoopOutputFile.fromPath(
                new org.apache.hadoop.fs.Path(dataFile.getAbsolutePath()),
                new Configuration()
        );

        FileAppender<GenericRecord> appender = Parquet.write(out)
                .schema(schema)
                .createWriterFunc(fileSchema -> GenericParquetWriter.create(schema, fileSchema))
                .build();

        try {
            // Create test records with different Variant data types
            GenericRecord record1 = GenericRecord.create(schema);
            record1.setField("id", 1L);
            record1.setField("name", "User1");
            record1.setField("attributes", Variant.of(Variants.metadata("age", "city"), Variants.of("{\"age\": 30, \"city\": \"NYC\"}")));
            appender.add(record1);

            GenericRecord record2 = GenericRecord.create(schema);
            record2.setField("id", 2L);
            record2.setField("name", "User2");
            record2.setField("attributes", Variant.of(Variants.metadata("tt"), Variants.of("[1, 2, 3, \"test\"]")));
            appender.add(record2);

            GenericRecord record3 = GenericRecord.create(schema);
            record3.setField("id", 3L);
            record3.setField("name", "User3");
            record3.setField("attributes", Variant.of(VariantMetadata.empty(), Variants.of("simple string")));
            appender.add(record3);

            GenericRecord record4 = GenericRecord.create(schema);
            record4.setField("id", 4L);
            record4.setField("name", "User4");
            record4.setField("attributes", Variant.of(VariantMetadata.empty(), Variants.of("null"))); // null variant
            appender.add(record4);

            GenericRecord record5 = GenericRecord.create(schema);
            record5.setField("id", 5L);
            record5.setField("name", "User5");
            record5.setField("attributes", Variant.of(VariantMetadata.empty(), Variants.of(42)));
            appender.add(record5);

            GenericRecord record6 = GenericRecord.create(schema);
            record6.setField("id", 6L);
            record6.setField("name", "User6");
            record6.setField("attributes", Variant.of(VariantMetadata.empty(), Variants.of(12345L)));
            appender.add(record6);

            GenericRecord record7 = GenericRecord.create(schema);
            record7.setField("id", 7L);
            record7.setField("name", "User7");
            record7.setField("attributes", Variant.of(VariantMetadata.empty(), Variants.of(3.14159)));
            appender.add(record7);

            GenericRecord record8 = GenericRecord.create(schema);
            record8.setField("id", 8L);
            record8.setField("name", "User8");
            record8.setField("attributes", Variant.of(VariantMetadata.empty(), Variants.of(true)));
            appender.add(record8);

        } finally {
            appender.close();
        }

        // 4. Add data file to table
        DataFile dataFileToAdd = DataFiles.builder(table.spec())
                .withPath(dataFile.getAbsolutePath())
                .withFileSizeInBytes(dataFile.length())
                .withRecordCount(8)
                .build();

        table.newAppend()
                .appendFile(dataFileToAdd)
                .commit();

        // 5. Read data back and verify
        InputFile inputFile = table.io().newInputFile(dataFile.getAbsolutePath());

        try (CloseableIterable<GenericRecord> reader = Parquet.read(inputFile)
                .project(schema)
                .createReaderFunc(fileSchema -> GenericParquetReaders.buildReader(schema, fileSchema))
                .build()) {

            int recordCount = 0;
            for (GenericRecord record : reader) {
                recordCount++;
                Long id = (Long) record.getField("id");
                String name = (String) record.getField("name");
                Variant attributes = (Variant) record.getField("attributes");

                assertNotNull("ID should not be null", id);
                assertNotNull("Name should not be null", name);

                System.out.println("Record " + id + ": name=" + name + ", attributes=" + attributes);

                switch (id.intValue()) {
                    case 1:
                        assertEquals("User1", name);
                        assertNotNull("Variant attributes should not be null for record 1", attributes);
                        break;
                    case 2:
                        assertEquals("User2", name);
                        assertNotNull("Variant attributes should not be null for record 2", attributes);
                        break;
                    case 3:
                        assertEquals("User3", name);
                        assertNotNull("Variant attributes should not be null for record 3", attributes);
                        break;
                    case 4:
                        assertEquals("User4", name);
                        break;
                    case 5:
                        assertEquals("User5", name);
                        assertNotNull("Variant attributes should not be null for record 5", attributes);
                        break;
                    case 6:
                        assertEquals("User6", name);
                        assertNotNull("Variant attributes should not be null for record 6", attributes);
                        break;
                    case 7:
                        assertEquals("User7", name);
                        assertNotNull("Variant attributes should not be null for record 7", attributes);
                        break;
                    case 8:
                        assertEquals("User8", name);
                        assertNotNull("Variant attributes should not be null for record 8", attributes);
                        break;
                }
            }

            assertEquals("Should have read 8 records", 8, recordCount);
        }

        // 6. Verify table metadata
        assertEquals("Table should have 1 data file", 1,
                table.currentSnapshot().allManifests(table.io()).size());

        System.out.println("✓ Variant type end-to-end test passed successfully!");
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