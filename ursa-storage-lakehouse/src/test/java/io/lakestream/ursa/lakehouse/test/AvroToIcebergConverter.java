/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.lakehouse.test;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Tag;

@Tag("lakehouse")
public class AvroToIcebergConverter {

    // Convert Avro GenericRecord to Iceberg Record
    public static org.apache.iceberg.data.Record convertAvroToIceberg(
        GenericRecord avroRecord,
        Types.StructType icebergSchema
    ) {
        org.apache.iceberg.data.Record icebergRecord = org.apache.iceberg.data.GenericRecord.create(icebergSchema);

        for (Types.NestedField field : icebergSchema.fields()) {
            Object avroValue = avroRecord.get(field.name());
            Object icebergValue = convertValue(avroValue, field.type());
            icebergRecord.setField(field.name(), icebergValue);
        }

        return icebergRecord;
    }

    // Handle nested types recursively
    private static Object convertValue(Object avroValue, Type icebergType) {
        if (avroValue == null) {
            return null;
        }

        if (icebergType.isStructType()) {
            return convertAvroToIceberg(
                (GenericRecord) avroValue,
                icebergType.asStructType()
            );
        } else if (icebergType.isListType()) {
            return convertList(
                (java.util.List<?>) avroValue,
                icebergType.asListType().elementType()
            );
        } else if (icebergType.isMapType()) {
            return convertMap(
                (java.util.Map<?, ?>) avroValue,
                icebergType.asMapType()
            );
        } else {
            // Handle primitives (int, string, etc.)
            return avroValue;
        }
    }

    private static java.util.List<Object> convertList(
        java.util.List<?> avroList,
        Type elementType
    ) {
        java.util.List<Object> icebergList = new java.util.ArrayList<>();
        for (Object item : avroList) {
            icebergList.add(convertValue(item, elementType));
        }
        return icebergList;
    }

    private static java.util.Map<Object, Object> convertMap(
        java.util.Map<?, ?> avroMap,
        Types.MapType mapType
    ) {
        java.util.Map<Object, Object> icebergMap = new java.util.HashMap<>();
        for (java.util.Map.Entry<?, ?> entry : avroMap.entrySet()) {
            Object key = convertValue(entry.getKey(), mapType.keyType());
            Object value = convertValue(entry.getValue(), mapType.valueType());
            icebergMap.put(key, value);
        }
        return icebergMap;
    }

    // Example Usage
    public static void main(String[] args) {
        // 1. Define Avro Schema with nested record
        String avroSchemaStr = """
            {
              "type": "record",
              "name": "User",
              "fields": [
                {"name": "id", "type": "int"},
                {"name": "address", "type": {
                  "type": "record",
                  "name": "Address",
                  "fields": [
                    {"name": "city", "type": "string"},
                    {"name": "zip", "type": "int"}
                  ]
                }},
                {"name": "tags", "type": {"type": "array", "items": "string"}},
                {"name": "metadata", "type": {"type": "map", "values": "double"}}
              ]
            }
            """;
        Schema avroSchema = new Schema.Parser().parse(avroSchemaStr);

        // 2. Define matching Iceberg Schema
        Types.StructType icebergSchema = Types.StructType.of(
            Types.NestedField.required(1, "id", Types.IntegerType.get()),
            Types.NestedField.required(2, "address", Types.StructType.of(
                Types.NestedField.required(3, "city", Types.StringType.get()),
                Types.NestedField.required(4, "zip", Types.IntegerType.get())
            )),
            Types.NestedField.required(5, "tags", Types.ListType.ofRequired(6, Types.StringType.get())),
            Types.NestedField.required(7, "metadata", Types.MapType.ofRequired(8, 9,
                Types.StringType.get(), Types.DoubleType.get()))
        );

        // 3. Create sample Avro record
        GenericRecord avroAddress = new GenericData.Record(avroSchema.getField("address").schema());
        avroAddress.put("city", "New York");
        avroAddress.put("zip", 10001);

        GenericRecord avroUser = new GenericData.Record(avroSchema);
        avroUser.put("id", 123);
        avroUser.put("address", avroAddress);
        avroUser.put("tags", java.util.List.of("admin", "user"));
        avroUser.put("metadata", java.util.Map.of("score", 9.8));

        // 4. Convert to Iceberg Record
        org.apache.iceberg.data.Record icebergUser = convertAvroToIceberg(avroUser, icebergSchema);

        // 5. Verify
        System.out.println("Converted Iceberg Record:");
        System.out.println("ID: " + icebergUser.getField("id"));
        System.out.println("City: " + ((org.apache.iceberg.data.Record) icebergUser.getField("address")).getField("city"));
        System.out.println("Tags: " + icebergUser.getField("tags"));
        System.out.println("Metadata: " + icebergUser.getField("metadata"));
    }
}
