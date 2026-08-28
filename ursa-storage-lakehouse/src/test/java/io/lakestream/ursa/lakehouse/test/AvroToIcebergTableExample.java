/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.lakehouse.test;

import io.lakestream.ursa.lakehouse.iceberg.IcebergSinkConfig;
import io.lakestream.ursa.lakehouse.iceberg.Utilities;
import java.io.IOException;
import java.util.Properties;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.avro.util.Utf8;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Table;
import org.apache.iceberg.avro.AvroSchemaUtil;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.io.TaskWriter;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Tag;

@Tag("lakehouse")
public class AvroToIcebergTableExample {

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

        // Create Avro GenericRecord
        GenericRecord avroRecord = new GenericRecordBuilder(avroSchema)
            .set("id", 1)
            .set("name", new Utf8("John Doe"))
            .set("age", 30)
            .build();

        // Convert Avro schema to Iceberg schema
        org.apache.iceberg.Schema icebergSchema = AvroSchemaUtil.toIceberg(avroSchema);

        // Convert Avro GenericRecord to Iceberg Record
        Record icebergRecord = convertToIcebergRecord(avroRecord, icebergSchema);

        // Iceberg table setup
        Configuration conf = new Configuration();
        String warehousePath = "file:///Users/hangc/Workspace/lakestream/ursa-storage/tmp/iceberg-warehouse"; // Replace with HDFS/S3 path if needed
        HadoopCatalog catalog = new HadoopCatalog(conf, warehousePath);
        TableIdentifier tableIdentifier = TableIdentifier.of("default", "users");

        // Create table if not exists
        if (!catalog.tableExists(tableIdentifier)) {
            catalog.createTable(tableIdentifier, icebergSchema, PartitionSpec.unpartitioned());
        }

        Table table = catalog.loadTable(tableIdentifier);

        // Write the record to the Iceberg table
        Properties prop = new Properties();
        IcebergSinkConfig config = new IcebergSinkConfig(prop);
        try (TaskWriter<Record> writer = Utilities.createTableWriter(table, table.schema(), 1, config)) {
            writer.write(icebergRecord);
            writer.complete();
        }

        System.out.println("Record written to Iceberg table: " + tableIdentifier);
    }
}
