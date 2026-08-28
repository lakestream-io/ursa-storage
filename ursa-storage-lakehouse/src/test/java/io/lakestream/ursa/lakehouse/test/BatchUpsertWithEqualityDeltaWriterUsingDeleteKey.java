/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.lakehouse.test;

import com.google.common.collect.ImmutableList;
import io.lakestream.ursa.lakehouse.iceberg.IcebergSinkConfig;
import io.lakestream.ursa.lakehouse.iceberg.Operation;
import io.lakestream.ursa.lakehouse.iceberg.RecordWrapper;
import io.lakestream.ursa.lakehouse.iceberg.Utilities;
import io.lakestream.ursa.lakehouse.utils.AvroSchemaUtilExtended;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.avro.util.Utf8;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.RowDelta;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.io.TaskWriter;
import org.apache.iceberg.io.WriteResult;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Tag;

@Tag("lakehouse")
public class BatchUpsertWithEqualityDeltaWriterUsingDeleteKey {

    // Convert Avro GenericRecord to Iceberg Record
    public static Record convertToIcebergRecord(GenericRecord avroRecord, org.apache.iceberg.Schema icebergSchema) {
        Record icebergRecord = org.apache.iceberg.data.GenericRecord.create(icebergSchema);
        for (Types.NestedField field : icebergSchema.columns()) {
            String fieldName = field.name();
            Object value = avroRecord.get(fieldName);
            if (value instanceof Utf8) {
                value = value.toString();
            }
            icebergRecord.setField(fieldName, value);
        }
        return icebergRecord;
    }

    public static void main(String[] args) throws IOException {
        // Avro schema as a JSON string
        String avroSchemaString = """
            {
              "type": "record",
              "name": "User",
              "fields": [
                {"name": "id", "type": "int"},
                {"name": "name", "type": "string"},
                {"name": "age", "type": "int"}
              ]
            }
            """;

        // Parse Avro schema
        Schema avroSchema = new Schema.Parser().parse(avroSchemaString);

        // Iceberg table setup
        Configuration conf = new Configuration();
        String warehousePath = "file:///tmp/iceberg-warehouse"; // Replace with HDFS/S3 path if needed
        HadoopCatalog catalog = new HadoopCatalog(conf, warehousePath);
        TableIdentifier tableIdentifier = TableIdentifier.of("default", "users");

        // Create table if not exists
        Set<String> identifierFieldNames = new HashSet<>();
        identifierFieldNames.add("id");
        org.apache.iceberg.Schema icebergSchema =
                AvroSchemaUtilExtended.toIceberg(avroSchema, identifierFieldNames, false);

        if (!catalog.tableExists(tableIdentifier)) {
            catalog.createTable(tableIdentifier, icebergSchema, PartitionSpec.unpartitioned());
        }

        Table table = catalog.loadTable(tableIdentifier);

        // Sample batch of 1000 records
        List<GenericRecord> avroRecords = new ArrayList<>();
        for (int i = 0; i < 1000; i++) {
            avroRecords.add(
                new GenericRecordBuilder(avroSchema)
                    .set("id", i % 20)
                    .set("name", new Utf8("User " + i))
                    .set("age", 20 + (i % 50)) // Random age
                    .build()
            );
        }

        Properties prop = new Properties();
        prop.put("iceberg.tables.upsert-mode-enabled", "true");
        IcebergSinkConfig config = new IcebergSinkConfig(prop);
        try (TaskWriter<Record> writer = Utilities.createTableWriter(table, table.schema(), 1, config)) {
            for (GenericRecord avroRecord : avroRecords) {
                Record icebergRecord = convertToIcebergRecord(avroRecord, icebergSchema);
                RecordWrapper recordWrapper = new RecordWrapper(icebergRecord, Operation.UPDATE);

                // Add the updated record
                writer.write(recordWrapper);
            }

            // Commit the changes after processing the batch
            WriteResult result = writer.complete();

//            // Append data and delete files to the table
//            AppendFiles appendFiles = table.newAppend();
//            for (DataFile dataFile : result.dataFiles()) {
//                appendFiles.appendFile(dataFile);
//            }
//            appendFiles.commit();


            RowDelta rowDelta = table.newRowDelta()
                    .validateDataFilesExist(ImmutableList.copyOf(result.referencedDataFiles()))
                        .validateDeletedFiles();

            Arrays.stream(result.dataFiles()).forEach(rowDelta::addRows);
            Arrays.stream(result.deleteFiles()).forEach(rowDelta::addDeletes);
            rowDelta.commit();

            System.out.println("Batch upsert completed for 1000 records.");
        } catch (Exception e) {
            System.err.println("Error during batch upsert: " + e.getMessage());
        }
    }
}
