/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.lakehouse.test;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.avro.util.Utf8;
import org.apache.iceberg.avro.AvroSchemaUtil;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Tag;

@Tag("lakehouse")
public class AvroToIcebergExample {

    // Converts an Avro GenericRecord to an Iceberg Record
    public static Record convertToIcebergRecord(GenericRecord avroRecord, org.apache.iceberg.Schema icebergSchema) {
        // Create an Iceberg record based on the Iceberg schema
        Record icebergRecord = org.apache.iceberg.data.GenericRecord.create(icebergSchema);

        // Map fields from the Avro record to the Iceberg record
        for (Types.NestedField field : icebergSchema.columns()) {
            String fieldName = field.name();
            Object value = avroRecord.get(fieldName);

            // Convert Avro-specific types to standard Java types
            if (value instanceof Utf8) {
                value = value.toString();
            }

            // Set the field in the Iceberg record
            icebergRecord.setField(fieldName, value);
        }

        return icebergRecord;
    }

    public static void main(String[] args) {
        // Example Avro schema
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

        // Parse the Avro schema
        Schema avroSchema = new Schema.Parser().parse(avroSchemaString);

        // Create an Avro GenericRecord
        GenericRecord avroRecord = new GenericRecordBuilder(avroSchema)
            .set("id", 1)
            .set("name", new Utf8("John Doe"))
            .set("age", 30)
            .build();

        // Convert Avro schema to Iceberg schema
        org.apache.iceberg.Schema icebergSchema = AvroSchemaUtil.toIceberg(avroSchema);

        // Convert the Avro record to an Iceberg record
        Record icebergRecord = convertToIcebergRecord(avroRecord, icebergSchema);

        // Print the Iceberg record
        System.out.println(icebergRecord);
    }
}

