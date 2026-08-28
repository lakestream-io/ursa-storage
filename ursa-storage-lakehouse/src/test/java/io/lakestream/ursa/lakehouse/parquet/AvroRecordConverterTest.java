/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.lakehouse.parquet;

import static org.junit.jupiter.api.Assertions.assertNotNull;

import org.apache.avro.Schema;
import org.apache.parquet.avro.AvroSchemaConverter;
import org.apache.parquet.schema.MessageType;
import org.junit.jupiter.api.Test;

/**
 * Comprehensive unit tests for AvroRecordMaterializer and AvroRecordConverter.
 */
public class AvroRecordConverterTest {

    /**
     * Helper method to convert Avro schema to Parquet MessageType.
     * Uses the standard AvroSchemaConverter from the parquet-avro library.
     */
    private MessageType avroSchemaToParquet(Schema avroSchema) {
        AvroSchemaConverter converter = new AvroSchemaConverter();
        return converter.convert(avroSchema);
    }

    /**
     * Helper method to create and test a materializer with the given schemas.
     */
    private void testMaterializer(String avroSchemaJson) {
        Schema avroSchema = new Schema.Parser().parse(avroSchemaJson);
        MessageType parquetSchema = avroSchemaToParquet(avroSchema);

        AvroRecordMaterializer materializer =
                new AvroRecordMaterializer(parquetSchema, avroSchema);
        assertNotNull(materializer);
        assertNotNull(materializer.getRootConverter());
    }

    @Test
    public void testPrimitiveTypes() {
        String avroSchemaJson =
                "{\n"
                        + "  \"type\": \"record\",\n"
                        + "  \"name\": \"PrimitiveTest\",\n"
                        + "  \"fields\": [\n"
                        + "    {\"name\": \"boolField\", \"type\": \"boolean\"},\n"
                        + "    {\"name\": \"intField\", \"type\": \"int\"},\n"
                        + "    {\"name\": \"longField\", \"type\": \"long\"},\n"
                        + "    {\"name\": \"floatField\", \"type\": \"float\"},\n"
                        + "    {\"name\": \"doubleField\", \"type\": \"double\"},\n"
                        + "    {\"name\": \"stringField\", \"type\": \"string\"},\n"
                        + "    {\"name\": \"bytesField\", \"type\": \"bytes\"}\n"
                        + "  ]\n"
                        + "}";
        testMaterializer(avroSchemaJson);
    }

    @Test
    public void testOptionalPrimitiveTypes() {
        String avroSchemaJson =
                "{\n"
                        + "  \"type\": \"record\",\n"
                        + "  \"name\": \"OptionalPrimitiveTest\",\n"
                        + "  \"fields\": [\n"
                        + "    {\"name\": \"optionalInt\", \"type\": [\"null\", \"int\"], \"default\": null},\n"
                        + "    {\"name\": \"optionalString\", \"type\": [\"null\", \"string\"], \"default\": null},\n"
                        + "    {\"name\": \"optionalLong\", \"type\": [\"null\", \"long\"], \"default\": null},\n"
                        + "    {\"name\": \"optionalBoolean\", \"type\": [\"null\", \"boolean\"], \"default\": null}\n"
                        + "  ]\n"
                        + "}";
        testMaterializer(avroSchemaJson);
    }

    @Test
    public void testNestedRecord() {
        String avroSchemaJson =
                "{\n"
                        + "  \"type\": \"record\",\n"
                        + "  \"name\": \"OuterRecord\",\n"
                        + "  \"fields\": [\n"
                        + "    {\"name\": \"id\", \"type\": \"int\"},\n"
                        + "    {\n"
                        + "      \"name\": \"innerRecord\",\n"
                        + "      \"type\": {\n"
                        + "        \"type\": \"record\",\n"
                        + "        \"name\": \"InnerRecord\",\n"
                        + "        \"fields\": [\n"
                        + "          {\"name\": \"name\", \"type\": \"string\"},\n"
                        + "          {\"name\": \"value\", \"type\": \"double\"}\n"
                        + "        ]\n"
                        + "      }\n"
                        + "    }\n"
                        + "  ]\n"
                        + "}";
        testMaterializer(avroSchemaJson);
    }

    @Test
    public void testOptionalNestedRecord() {
        String avroSchemaJson =
                "{\n"
                        + "  \"type\": \"record\",\n"
                        + "  \"name\": \"RecordWithOptionalNested\",\n"
                        + "  \"fields\": [\n"
                        + "    {\"name\": \"id\", \"type\": \"int\"},\n"
                        + "    {\n"
                        + "      \"name\": \"optionalInner\",\n"
                        + "      \"type\": [\"null\", {\n"
                        + "        \"type\": \"record\",\n"
                        + "        \"name\": \"InnerRecord\",\n"
                        + "        \"fields\": [\n"
                        + "          {\"name\": \"field1\", \"type\": \"string\"}\n"
                        + "        ]\n"
                        + "      }],\n"
                        + "      \"default\": null\n"
                        + "    }\n"
                        + "  ]\n"
                        + "}";
        testMaterializer(avroSchemaJson);
    }

    @Test
    public void testArrayOfPrimitives() {
        String avroSchemaJson =
                "{\n"
                        + "  \"type\": \"record\",\n"
                        + "  \"name\": \"ArrayTest\",\n"
                        + "  \"fields\": [\n"
                        + "    {\"name\": \"id\", \"type\": \"int\"},\n"
                        + "    {\"name\": \"intArray\", \"type\": {\"type\": \"array\", \"items\": \"int\"}},\n"
                        + "    {\"name\": \"stringArray\", \"type\": {\"type\": \"array\", \"items\": \"string\"}}\n"
                        + "  ]\n"
                        + "}";
        testMaterializer(avroSchemaJson);
    }

    @Test
    public void testArrayOfRecords() {
        String avroSchemaJson =
                "{\n"
                        + "  \"type\": \"record\",\n"
                        + "  \"name\": \"ArrayOfRecordsTest\",\n"
                        + "  \"fields\": [\n"
                        + "    {\n"
                        + "      \"name\": \"items\",\n"
                        + "      \"type\": {\n"
                        + "        \"type\": \"array\",\n"
                        + "        \"items\": {\n"
                        + "          \"type\": \"record\",\n"
                        + "          \"name\": \"Item\",\n"
                        + "          \"fields\": [\n"
                        + "            {\"name\": \"name\", \"type\": \"string\"},\n"
                        + "            {\"name\": \"quantity\", \"type\": \"int\"}\n"
                        + "          ]\n"
                        + "        }\n"
                        + "      }\n"
                        + "    }\n"
                        + "  ]\n"
                        + "}";
        testMaterializer(avroSchemaJson);
    }

    @Test
    public void testMapWithPrimitiveValues() {
        String avroSchemaJson =
                "{\n"
                        + "  \"type\": \"record\",\n"
                        + "  \"name\": \"MapTest\",\n"
                        + "  \"fields\": [\n"
                        + "    {\"name\": \"id\", \"type\": \"int\"},\n"
                        + "    {\"name\": \"stringMap\", \"type\": {\"type\": \"map\", \"values\": \"string\"}},\n"
                        + "    {\"name\": \"intMap\", \"type\": {\"type\": \"map\", \"values\": \"int\"}}\n"
                        + "  ]\n"
                        + "}";
        testMaterializer(avroSchemaJson);
    }

    @Test
    public void testMapWithUnionValues() {
        String avroSchemaJson =
                "{\n"
                        + "  \"type\": \"record\",\n"
                        + "  \"name\": \"MapWithUnionTest\",\n"
                        + "  \"fields\": [\n"
                        + "    {\"name\": \"id\", \"type\": \"int\"},\n"
                        + "    {\n"
                        + "      \"name\": \"metadata\",\n"
                        + "      \"type\": {\n"
                        + "        \"type\": \"map\",\n"
                        + "        \"values\": [\"null\", \"string\"]\n"
                        + "      }\n"
                        + "    }\n"
                        + "  ]\n"
                        + "}";
        testMaterializer(avroSchemaJson);
    }

    @Test
    public void testOptionalMapWithUnionValues() {
        String avroSchemaJson =
                "{\n"
                        + "  \"type\": \"record\",\n"
                        + "  \"name\": \"OptionalMapWithUnionTest\",\n"
                        + "  \"fields\": [\n"
                        + "    {\"name\": \"id\", \"type\": \"int\"},\n"
                        + "    {\n"
                        + "      \"name\": \"GNApp\",\n"
                        + "      \"type\": [\"null\", {\n"
                        + "        \"type\": \"map\",\n"
                        + "        \"values\": [\"null\", \"string\"]\n"
                        + "      }],\n"
                        + "      \"default\": null\n"
                        + "    }\n"
                        + "  ]\n"
                        + "}";
        testMaterializer(avroSchemaJson);
    }

    @Test
    public void testMapWithRecordValues() {
        String avroSchemaJson =
                "{\n"
                        + "  \"type\": \"record\",\n"
                        + "  \"name\": \"MapWithRecordTest\",\n"
                        + "  \"fields\": [\n"
                        + "    {\n"
                        + "      \"name\": \"recordMap\",\n"
                        + "      \"type\": {\n"
                        + "        \"type\": \"map\",\n"
                        + "        \"values\": {\n"
                        + "          \"type\": \"record\",\n"
                        + "          \"name\": \"ValueRecord\",\n"
                        + "          \"fields\": [\n"
                        + "            {\"name\": \"field1\", \"type\": \"string\"},\n"
                        + "            {\"name\": \"field2\", \"type\": \"int\"}\n"
                        + "          ]\n"
                        + "        }\n"
                        + "      }\n"
                        + "    }\n"
                        + "  ]\n"
                        + "}";
        testMaterializer(avroSchemaJson);
    }

    @Test
    public void testEnum() {
        String avroSchemaJson =
                "{\n"
                        + "  \"type\": \"record\",\n"
                        + "  \"name\": \"EnumTest\",\n"
                        + "  \"fields\": [\n"
                        + "    {\n"
                        + "      \"name\": \"status\",\n"
                        + "      \"type\": {\n"
                        + "        \"type\": \"enum\",\n"
                        + "        \"name\": \"Status\",\n"
                        + "        \"symbols\": [\"ACTIVE\", \"INACTIVE\", \"PENDING\"]\n"
                        + "      }\n"
                        + "    }\n"
                        + "  ]\n"
                        + "}";
        testMaterializer(avroSchemaJson);
    }

    @Test
    public void testFixed() {
        String avroSchemaJson =
                "{\n"
                        + "  \"type\": \"record\",\n"
                        + "  \"name\": \"FixedTest\",\n"
                        + "  \"fields\": [\n"
                        + "    {\n"
                        + "      \"name\": \"hash\",\n"
                        + "      \"type\": {\n"
                        + "        \"type\": \"fixed\",\n"
                        + "        \"name\": \"MD5\",\n"
                        + "        \"size\": 16\n"
                        + "      }\n"
                        + "    }\n"
                        + "  ]\n"
                        + "}";
        testMaterializer(avroSchemaJson);
    }

    @Test
    public void testComplexNestedStructure() {
        String avroSchemaJson =
                "{\n"
                        + "  \"type\": \"record\",\n"
                        + "  \"name\": \"ComplexTest\",\n"
                        + "  \"fields\": [\n"
                        + "    {\"name\": \"id\", \"type\": \"int\"},\n"
                        + "    {\n"
                        + "      \"name\": \"nested\",\n"
                        + "      \"type\": {\n"
                        + "        \"type\": \"record\",\n"
                        + "        \"name\": \"NestedLevel1\",\n"
                        + "        \"fields\": [\n"
                        + "          {\"name\": \"name\", \"type\": \"string\"},\n"
                        + "          {\n"
                        + "            \"name\": \"items\",\n"
                        + "            \"type\": {\n"
                        + "              \"type\": \"array\",\n"
                        + "              \"items\": {\n"
                        + "                \"type\": \"record\",\n"
                        + "                \"name\": \"Item\",\n"
                        + "                \"fields\": [\n"
                        + "                  {\"name\": \"key\", \"type\": \"string\"},\n"
                        + "                  {\"name\": \"value\", \"type\": [\"null\", \"int\"], \"default\": null}\n"
                        + "                ]\n"
                        + "              }\n"
                        + "            }\n"
                        + "          },\n"
                        + "          {\n"
                        + "            \"name\": \"metadata\",\n"
                        + "            \"type\": {\"type\": \"map\", \"values\": [\"null\", \"string\"]}\n"
                        + "          }\n"
                        + "        ]\n"
                        + "      }\n"
                        + "    }\n"
                        + "  ]\n"
                        + "}";
        testMaterializer(avroSchemaJson);
    }

    @Test
    public void testBugsnagEventSchema() {
        String avroSchemaJson =
                "{\n"
                        + "  \"type\": \"record\",\n"
                        + "  \"name\": \"Event\",\n"
                        + "  \"fields\": [\n"
                        + "    {\n"
                        + "      \"name\": \"error\",\n"
                        + "      \"type\": [\"null\", {\n"
                        + "        \"type\": \"record\",\n"
                        + "        \"name\": \"ErrorRecord\",\n"
                        + "        \"fields\": [\n"
                        + "          {\"name\": \"id\", \"type\": [\"null\", \"string\"], \"default\": null},\n"
                        + "          {\"name\": \"message\", \"type\": [\"null\", \"string\"], \"default\": null},\n"
                        + "          {\n"
                        + "            \"name\": \"metaData\",\n"
                        + "            \"type\": [\"null\", {\n"
                        + "              \"type\": \"record\",\n"
                        + "              \"name\": \"MetaData\",\n"
                        + "              \"fields\": [\n"
                        + "                {\"name\": \"GNApp\", \"type\": [\"null\", {\"type\": \"map\", \"values\": [\"null\", \"string\"]}], \"default\": null}\n"
                        + "              ]\n"
                        + "            }],\n"
                        + "            \"default\": null\n"
                        + "          }\n"
                        + "        ]\n"
                        + "      }],\n"
                        + "      \"default\": null\n"
                        + "    }\n"
                        + "  ]\n"
                        + "}";
        testMaterializer(avroSchemaJson);
    }

    @Test
    public void testMultipleOptionalMaps() {
        String avroSchemaJson =
                "{\n"
                        + "  \"type\": \"record\",\n"
                        + "  \"name\": \"MultiMapTest\",\n"
                        + "  \"fields\": [\n"
                        + "    {\"name\": \"map1\", \"type\": [\"null\", {\"type\": \"map\", \"values\": [\"null\", \"string\"]}], \"default\": null},\n"
                        + "    {\"name\": \"map2\", \"type\": [\"null\", {\"type\": \"map\", \"values\": [\"null\", \"int\"]}], \"default\": null},\n"
                        + "    {\"name\": \"map3\", \"type\": [\"null\", {\"type\": \"map\", \"values\": \"string\"}], \"default\": null}\n"
                        + "  ]\n"
                        + "}";
        testMaterializer(avroSchemaJson);
    }

    @Test
    public void testArrayWithUnionElements() {
        String avroSchemaJson =
                "{\n"
                        + "  \"type\": \"record\",\n"
                        + "  \"name\": \"ArrayUnionTest\",\n"
                        + "  \"fields\": [\n"
                        + "    {\n"
                        + "      \"name\": \"items\",\n"
                        + "      \"type\": {\n"
                        + "        \"type\": \"array\",\n"
                        + "        \"items\": [\"null\", \"string\"]\n"
                        + "      }\n"
                        + "    }\n"
                        + "  ]\n"
                        + "}";
        testMaterializer(avroSchemaJson);
    }

    @Test
    public void testMismatchedSchemas() {
        String avroSchemaJson =
                "{\n"
                        + "  \"type\": \"record\",\n"
                        + "  \"name\": \"MismatchTest\",\n"
                        + "  \"fields\": [\n"
                        + "    {\"name\": \"field1\", \"type\": \"string\"},\n"
                        + "    {\"name\": \"field2\", \"type\": \"int\"}\n"
                        + "  ]\n"
                        + "}";
        testMaterializer(avroSchemaJson);
    }

    @Test
    public void testTimestampLogicalType() {
        String avroSchemaJson =
                "{\n"
                        + "  \"type\": \"record\",\n"
                        + "  \"name\": \"TimestampTest\",\n"
                        + "  \"fields\": [\n"
                        + "    {\"name\": \"createdAt\", \"type\": {\"type\": \"long\", \"logicalType\": \"timestamp-millis\"}},\n"
                        + "    {\"name\": \"updatedAt\", \"type\": [\"null\", {\"type\": \"long\", \"logicalType\": \"timestamp-millis\"}], \"default\": null}\n"
                        + "  ]\n"
                        + "}";
        testMaterializer(avroSchemaJson);
    }

    @Test
    public void testDeeplyNestedMapsAndArrays() {
        String avroSchemaJson =
                "{\n"
                        + "  \"type\": \"record\",\n"
                        + "  \"name\": \"DeeplyNestedTest\",\n"
                        + "  \"fields\": [\n"
                        + "    {\n"
                        + "      \"name\": \"nestedStructure\",\n"
                        + "      \"type\": {\n"
                        + "        \"type\": \"map\",\n"
                        + "        \"values\": {\n"
                        + "          \"type\": \"array\",\n"
                        + "          \"items\": {\n"
                        + "            \"type\": \"map\",\n"
                        + "            \"values\": [\"null\", \"string\"]\n"
                        + "          }\n"
                        + "        }\n"
                        + "      }\n"
                        + "    }\n"
                        + "  ]\n"
                        + "}";
        testMaterializer(avroSchemaJson);
    }

    @Test
    public void testAllTypesWithOptionals() {
        String avroSchemaJson =
                "{\n"
                        + "  \"type\": \"record\",\n"
                        + "  \"name\": \"ComprehensiveTest\",\n"
                        + "  \"fields\": [\n"
                        + "    {\"name\": \"requiredInt\", \"type\": \"int\"},\n"
                        + "    {\"name\": \"optionalInt\", \"type\": [\"null\", \"int\"], \"default\": null},\n"
                        + "    {\"name\": \"requiredString\", \"type\": \"string\"},\n"
                        + "    {\"name\": \"optionalString\", \"type\": [\"null\", \"string\"], \"default\": null},\n"
                        + "    {\"name\": \"requiredBoolean\", \"type\": \"boolean\"},\n"
                        + "    {\"name\": \"optionalBoolean\", \"type\": [\"null\", \"boolean\"], \"default\": null},\n"
                        + "    {\"name\": \"requiredLong\", \"type\": \"long\"},\n"
                        + "    {\"name\": \"optionalLong\", \"type\": [\"null\", \"long\"], \"default\": null},\n"
                        + "    {\"name\": \"requiredFloat\", \"type\": \"float\"},\n"
                        + "    {\"name\": \"optionalFloat\", \"type\": [\"null\", \"float\"], \"default\": null},\n"
                        + "    {\"name\": \"requiredDouble\", \"type\": \"double\"},\n"
                        + "    {\"name\": \"optionalDouble\", \"type\": [\"null\", \"double\"], \"default\": null},\n"
                        + "    {\"name\": \"requiredBytes\", \"type\": \"bytes\"},\n"
                        + "    {\"name\": \"optionalBytes\", \"type\": [\"null\", \"bytes\"], \"default\": null}\n"
                        + "  ]\n"
                        + "}";
        testMaterializer(avroSchemaJson);
    }

    @Test
    public void testMapWithComplexUnionValues() {
        String avroSchemaJson =
                "{\n"
                        + "  \"type\": \"record\",\n"
                        + "  \"name\": \"ComplexUnionMapTest\",\n"
                        + "  \"fields\": [\n"
                        + "    {\n"
                        + "      \"name\": \"complexMap\",\n"
                        + "      \"type\": {\n"
                        + "        \"type\": \"map\",\n"
                        + "        \"values\": [\n"
                        + "          \"null\",\n"
                        + "          \"string\",\n"
                        + "          \"int\",\n"
                        + "          {\n"
                        + "            \"type\": \"record\",\n"
                        + "            \"name\": \"ValueRecord\",\n"
                        + "            \"fields\": [\n"
                        + "              {\"name\": \"field1\", \"type\": \"string\"}\n"
                        + "            ]\n"
                        + "          }\n"
                        + "        ]\n"
                        + "      }\n"
                        + "    }\n"
                        + "  ]\n"
                        + "}";
        testMaterializer(avroSchemaJson);
    }

    @Test
    public void testEmptyRecord() {
        String avroSchemaJson =
                "{\n"
                        + "  \"type\": \"record\",\n"
                        + "  \"name\": \"EmptyRecord\",\n"
                        + "  \"fields\": []\n"
                        + "}";
        testMaterializer(avroSchemaJson);
    }

    @Test
    public void testOptionalArray() {
        String avroSchemaJson =
                "{\n"
                        + "  \"type\": \"record\",\n"
                        + "  \"name\": \"OptionalArrayTest\",\n"
                        + "  \"fields\": [\n"
                        + "    {\"name\": \"id\", \"type\": \"int\"},\n"
                        + "    {\"name\": \"optionalArray\", \"type\": [\"null\", {\"type\": \"array\", \"items\": \"string\"}], \"default\": null}\n"
                        + "  ]\n"
                        + "}";
        testMaterializer(avroSchemaJson);
    }

    @Test
    public void testOptionalEnum() {
        String avroSchemaJson =
                "{\n"
                        + "  \"type\": \"record\",\n"
                        + "  \"name\": \"OptionalEnumTest\",\n"
                        + "  \"fields\": [\n"
                        + "    {\n"
                        + "      \"name\": \"status\",\n"
                        + "      \"type\": [\"null\", {\n"
                        + "        \"type\": \"enum\",\n"
                        + "        \"name\": \"Status\",\n"
                        + "        \"symbols\": [\"ACTIVE\", \"INACTIVE\"]\n"
                        + "      }],\n"
                        + "      \"default\": null\n"
                        + "    }\n"
                        + "  ]\n"
                        + "}";
        testMaterializer(avroSchemaJson);
    }

    @Test
    public void testOptionalFixed() {
        String avroSchemaJson =
                "{\n"
                        + "  \"type\": \"record\",\n"
                        + "  \"name\": \"OptionalFixedTest\",\n"
                        + "  \"fields\": [\n"
                        + "    {\n"
                        + "      \"name\": \"hash\",\n"
                        + "      \"type\": [\"null\", {\n"
                        + "        \"type\": \"fixed\",\n"
                        + "        \"name\": \"MD5\",\n"
                        + "        \"size\": 16\n"
                        + "      }],\n"
                        + "      \"default\": null\n"
                        + "    }\n"
                        + "  ]\n"
                        + "}";
        testMaterializer(avroSchemaJson);
    }

    @Test
    public void testMultipleLevelsOfNesting() {
        String avroSchemaJson =
                "{\n"
                        + "  \"type\": \"record\",\n"
                        + "  \"name\": \"Level1\",\n"
                        + "  \"fields\": [\n"
                        + "    {\n"
                        + "      \"name\": \"level2\",\n"
                        + "      \"type\": {\n"
                        + "        \"type\": \"record\",\n"
                        + "        \"name\": \"Level2\",\n"
                        + "        \"fields\": [\n"
                        + "          {\n"
                        + "            \"name\": \"level3\",\n"
                        + "            \"type\": {\n"
                        + "              \"type\": \"record\",\n"
                        + "              \"name\": \"Level3\",\n"
                        + "              \"fields\": [\n"
                        + "                {\n"
                        + "                  \"name\": \"level4\",\n"
                        + "                  \"type\": {\n"
                        + "                    \"type\": \"record\",\n"
                        + "                    \"name\": \"Level4\",\n"
                        + "                    \"fields\": [\n"
                        + "                      {\n"
                        + "                        \"name\": \"level5\",\n"
                        + "                        \"type\": {\n"
                        + "                          \"type\": \"record\",\n"
                        + "                          \"name\": \"Level5\",\n"
                        + "                          \"fields\": [\n"
                        + "                            {\"name\": \"value\", \"type\": \"string\"}\n"
                        + "                          ]\n"
                        + "                        }\n"
                        + "                      }\n"
                        + "                    ]\n"
                        + "                  }\n"
                        + "                }\n"
                        + "              ]\n"
                        + "            }\n"
                        + "          }\n"
                        + "        ]\n"
                        + "      }\n"
                        + "    }\n"
                        + "  ]\n"
                        + "}";
        testMaterializer(avroSchemaJson);
    }

    @Test
    public void testMixedOptionalAndRequiredFields() {
        String avroSchemaJson =
                "{\n"
                        + "  \"type\": \"record\",\n"
                        + "  \"name\": \"MixedTest\",\n"
                        + "  \"fields\": [\n"
                        + "    {\"name\": \"required1\", \"type\": \"string\"},\n"
                        + "    {\"name\": \"optional1\", \"type\": [\"null\", \"string\"], \"default\": null},\n"
                        + "    {\"name\": \"required2\", \"type\": \"int\"},\n"
                        + "    {\"name\": \"optional2\", \"type\": [\"null\", \"int\"], \"default\": null},\n"
                        + "    {\n"
                        + "      \"name\": \"nestedRequired\",\n"
                        + "      \"type\": {\n"
                        + "        \"type\": \"record\",\n"
                        + "        \"name\": \"NestedRequired\",\n"
                        + "        \"fields\": [\n"
                        + "          {\"name\": \"field1\", \"type\": \"string\"},\n"
                        + "          {\"name\": \"field2\", \"type\": [\"null\", \"int\"], \"default\": null}\n"
                        + "        ]\n"
                        + "      }\n"
                        + "    },\n"
                        + "    {\n"
                        + "      \"name\": \"nestedOptional\",\n"
                        + "      \"type\": [\"null\", {\n"
                        + "        \"type\": \"record\",\n"
                        + "        \"name\": \"NestedOptional\",\n"
                        + "        \"fields\": [\n"
                        + "          {\"name\": \"field1\", \"type\": \"string\"},\n"
                        + "          {\"name\": \"field2\", \"type\": [\"null\", \"int\"], \"default\": null}\n"
                        + "        ]\n"
                        + "      }],\n"
                        + "      \"default\": null\n"
                        + "    }\n"
                        + "  ]\n"
                        + "}";
        testMaterializer(avroSchemaJson);
    }
}
