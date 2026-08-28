/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.compact;

import java.util.List;
import java.util.Map;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.avro.reflect.AvroSchema;

// V1
@Data
@NoArgsConstructor
@AllArgsConstructor
@AvroSchema("{\"type\":\"record\",\"name\":\"UserEvent\",\"fields\":["
            + "{\"name\":\"id\",\"type\":\"string\"},"
            + "{\"name\":\"age\",\"type\":\"long\"},"
            + "{\"name\":\"active\",\"type\":\"boolean\"},"
            + "{\"name\":\"score\",\"type\":\"double\"},"
            + "{\"name\":\"tags\",\"type\":{\"type\":\"array\",\"items\":\"string\"}},"
            + "{\"name\":\"attributes\",\"type\":{\"type\":\"map\",\"values\":\"string\"}},"
            + "{\"name\":\"address\",\"type\":{\"type\":\"record\",\"name\":\"Address\",\"fields\":["
            + "{\"name\":\"street\",\"type\":\"string\"},"
            + "{\"name\":\"city\",\"type\":\"string\"}"
            + "]}}"
            + "]}") // To avoid Avro schema conflict with V2
public class UserEventV1 {
    private String id;
    private long age;
    private boolean active;
    private double score;
    private List<String> tags;
    private Map<String, String> attributes;

    private Address address;

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class Address {
        private String street;
        private String city;
    }
}

// V2
@Data
@NoArgsConstructor
@AvroSchema("{\"type\":\"record\",\"name\":\"UserEvent\",\"fields\":["
            + "{\"name\":\"id\",\"type\":\"string\"},"
            + "{\"name\":\"age\",\"type\":\"long\"},"
            + "{\"name\":\"active\",\"type\":\"boolean\"},"
            + "{\"name\":\"score\",\"type\":\"double\"},"
            + "{\"name\":\"tags\",\"type\":{\"type\":\"array\",\"items\":\"string\"}},"
            + "{\"name\":\"attributes\",\"type\":{\"type\":\"map\",\"values\":\"string\"}},"
            + "{\"name\":\"email\",\"type\":[\"null\",\"string\"],\"default\":null},"
            + "{\"name\":\"address\",\"type\":{\"type\":\"record\",\"name\":\"Address\",\"fields\":["
            + "{\"name\":\"street\",\"type\":\"string\"},"
            + "{\"name\":\"city\",\"type\":\"string\"}"
            + "]}}"
            + "]}") // To avoid Avro schema conflict with V1
@AllArgsConstructor // To avoid Avro schema conflict with V1
class UserEventV2 {
    private String id;
    private long age; // evolved from int → long
    private boolean active;
    private double score;
    private List<String> tags;
    private Map<String, String> attributes;
    private String email; // new optional field

    private Address address;

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class Address {
        private String street;
        private String city;
    }
}

// V3
@Data
@NoArgsConstructor
@AvroSchema("{\"type\":\"record\",\"name\":\"UserEvent\",\"fields\":["
            + "{\"name\":\"id\",\"type\":\"string\"},"
            + "{\"name\":\"age\",\"type\":\"long\"},"
            + "{\"name\":\"active\",\"type\":\"boolean\"},"
            + "{\"name\":\"score\",\"type\":\"double\"},"
            + "{\"name\":\"tags\",\"type\":{\"type\":\"array\",\"items\":\"string\"}},"
            + "{\"name\":\"attributes\",\"type\":{\"type\":\"map\",\"values\":\"string\"}},"
            + "{\"name\":\"email\",\"type\":[\"null\",\"string\"],\"default\":null},"
            + "{\"name\":\"address\",\"type\":{\"type\":\"record\",\"name\":\"Address\",\"fields\":["
            + "{\"name\":\"street\",\"type\":\"string\"},"
            + "{\"name\":\"city\",\"type\":\"string\"},"
            + "{\"name\":\"zipCode\",\"type\":[\"null\",\"string\"],\"default\":null}"
            + "]}}"
            + "]}") // To avoid Avro schema conflict with V1 and V2
@AllArgsConstructor
class UserEventV3 {
    private String id;
    private long age;
    private boolean active;
    private double score; // not support rename field
    private List<String> tags;
    private Map<String, String> attributes;
    private String email;

    private Address address;

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class Address {
        private String street;
        private String city;
        private String zipCode; // new field
    }
}

@Data
@NoArgsConstructor
@AllArgsConstructor
// Solution 1: Define the avro schema for the entire class with logicalType "variant"
//@AvroSchema("{\n"
//        + "  \"type\": \"record\",\n"
//        + "  \"name\": \"UserEventWithVariantV1\",\n"
//        + "  \"namespace\": \"com.example\",\n"
//        + "  \"fields\": [\n"
//        + "    {\n"
//        + "      \"name\": \"id\",\n"
//        + "      \"type\": [\"null\", \"string\"],\n"
//        + "      \"default\": null\n"
//        + "    },\n"
//        + "    {\n"
//        + "      \"name\": \"age\",\n"
//        + "      \"type\": \"long\"\n"
//        + "    },\n"
//        + "    {\n"
//        + "      \"name\": \"active\",\n"
//        + "      \"type\": \"boolean\"\n"
//        + "    },\n"
//        + "    {\n"
//        + "      \"name\": \"score\",\n"
//        + "      \"type\": {"
//        + "        \"type\": \"double\",\n"
//        + "         \"logicalType\": \"variant\"\n"
//        + "       }"
//        + "    },\n"
//        + "    {\n"
//        + "      \"name\": \"tags\",\n"
//        + "      \"type\": {\n"
//        + "        \"type\": \"array\",\n"
//        + "        \"items\": \"string\",\n"
//        + "        \"logicalType\": \"variant\"\n"
//        + "      },\n"
//        + "      \"default\": []\n"
//        + "    },\n"
//        + "    {\n"
//        + "      \"name\": \"attributes\",\n"
//        + "      \"type\": {\n"
//        + "        \"type\": \"map\",\n"
//        + "        \"values\": \"string\",\n"
//        + "        \"logicalType\": \"variant\"\n"
//        + "      },\n"
//        + "      \"default\": {}\n"
//        + "    },\n"
//        + "    {\n"
//        + "      \"name\": \"address\",\n"
//        + "      \"type\": {\n"
//        + "        \"type\": \"record\",\n"
//        + "        \"name\": \"Address\",\n"
//        + "        \"fields\": [\n"
//        + "          {\n"
//        + "            \"name\": \"street\",\n"
//        + "            \"type\": \"string\"\n"
//        + "          },\n"
//        + "          {\n"
//        + "            \"name\": \"city\",\n"
//        + "            \"type\": \"string\"\n"
//        + "          }\n"
//        + "        ],\n"
//        + "        \"logicalType\": \"variant\",\n"
//        + "        \"variant-metadata-fields\": \"[\\\"street\\\", \\\"city\\\"]\"\n"
//        + "      }\n"
//        + "    }\n"
//        + "  ]\n"
//        + "}\n")

// Solution 2: Define the avro schema for each field with logicalType "variant"
class UserEventWithVariantV1 {
    private String id;
    private long age;
    private boolean active;
    @AvroSchema("{\"type\": \"double\", \"logicalType\": \"variant\"}")
    private double score;
    @AvroSchema("{\"type\": \"array\", \"items\": \"string\", \"logicalType\": \"variant\"}")
    private List<String> tags;
    @AvroSchema("{\"type\": \"map\", \"values\": \"string\", \"logicalType\": \"variant\"}")
    private Map<String, String> attributes;

    private Address address;

    private static final String ADDRESS_SCHEMA = "{\"type\": \"record\", \"name\": \"Address\", \"fields\": ["
            + "  {\"name\": \"street\", \"type\": \"string\"},"
            + "  {\"name\": \"city\", \"type\": \"string\"}"
            + "],"
            + "\"logicalType\": \"variant\","
            + "\"variant-metadata-fields\": \"[\\\"street\\\", \\\"city\\\"]\" "
            + "}";

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    @AvroSchema(ADDRESS_SCHEMA)
    public static class Address {
        private String street;
        private String city;
    }
}
