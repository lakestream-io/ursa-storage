/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.lakehouse.catalog.unity;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.databricks.sdk.service.catalog.AwsCredentials;
import com.databricks.sdk.service.catalog.AzureUserDelegationSas;
import com.databricks.sdk.service.catalog.ColumnInfo;
import com.databricks.sdk.service.catalog.ColumnTypeName;
import com.databricks.sdk.service.catalog.GenerateTemporaryTableCredentialResponse;
import io.delta.kernel.types.ArrayType;
import io.delta.kernel.types.BinaryType;
import io.delta.kernel.types.BooleanType;
import io.delta.kernel.types.ByteType;
import io.delta.kernel.types.DataType;
import io.delta.kernel.types.DateType;
import io.delta.kernel.types.DecimalType;
import io.delta.kernel.types.DoubleType;
import io.delta.kernel.types.FloatType;
import io.delta.kernel.types.IntegerType;
import io.delta.kernel.types.LongType;
import io.delta.kernel.types.MapType;
import io.delta.kernel.types.ShortType;
import io.delta.kernel.types.StringType;
import io.delta.kernel.types.StructField;
import io.delta.kernel.types.StructType;
import io.delta.kernel.types.TimestampNTZType;
import io.delta.kernel.types.TimestampType;
import io.lakestream.ursa.lakehouse.LakehouseConfiguration;
import io.lakestream.ursa.lakehouse.utils.AvroSchemaUtilExtended;
import java.util.ArrayList;
import java.util.List;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag("lakehouse")
public class UnityCatalogUtilTest {


    @Test
    public void testParseTopicName() {
        String topicName = "test-topic";
        UnityTableIdentifier identifier = UnityTableIdentifier.parse(topicName);
        assertEquals("default", identifier.getSchema());
        assertEquals("test__topic", identifier.getTable());
        assertEquals("test.default.test__topic", identifier.getTableFullName("test"));

        topicName = "test_topic";
        identifier = UnityTableIdentifier.parse(topicName);
        assertEquals("default", identifier.getSchema());
        assertEquals("test_topic", identifier.getTable());
        assertEquals("test.default.test_topic", identifier.getTableFullName("test"));

        topicName = "test_topic-partition-0";
        identifier = UnityTableIdentifier.parse(topicName);
        assertEquals("default", identifier.getSchema());
        assertEquals("test_topic", identifier.getTable());
        assertEquals("test.default.test_topic", identifier.getTableFullName("test"));


        topicName = "avro/test_topic";
        identifier = UnityTableIdentifier.parse(topicName);
        assertEquals("avro", identifier.getSchema());
        assertEquals("test_topic", identifier.getTable());
        assertEquals("test.avro.test_topic", identifier.getTableFullName("test"));

        topicName = "test_topic_v1";
        identifier = UnityTableIdentifier.parse(topicName);
        assertEquals("default", identifier.getSchema());
        assertEquals("test_topic_v1", identifier.getTable());
        assertEquals("test.default.test_topic_v1", identifier.getTableFullName("test"));

        topicName = "test-topic-v1";
        identifier = UnityTableIdentifier.parse(topicName);
        assertEquals("default", identifier.getSchema());
        assertEquals("test__topic__v1", identifier.getTable());
        assertEquals("test.default.test__topic__v1", identifier.getTableFullName("test"));

        topicName = "test--topic-v1";
        identifier = UnityTableIdentifier.parse(topicName);
        assertEquals("default", identifier.getSchema());
        assertEquals("test____topic__v1", identifier.getTable());
        assertEquals("test.default.test____topic__v1", identifier.getTableFullName("test"));

        topicName = "public/default/test.topic-v1_xx";
        identifier = UnityTableIdentifier.parse(topicName);
        assertEquals("public_default", identifier.getSchema());
        assertEquals("test_topic__v1_xx", identifier.getTable());
        assertEquals("test.public_default.test_topic__v1_xx", identifier.getTableFullName("test"));

        topicName = "public/default/test.topic-v1_xx:m";
        identifier = UnityTableIdentifier.parse(topicName);
        assertEquals("public_default", identifier.getSchema());
        assertEquals("test_topic__v1_xx____m", identifier.getTable());
        assertEquals("test.public_default.test_topic__v1_xx____m", identifier.getTableFullName("test"));
    }

    @Test
    public void testConvertAvroSchemaToColumnsWithPrimitiveType() {
        Schema avroSchema = SchemaBuilder.record("TestRecord")
            .fields()
            .name("name").type().stringType().noDefault()
            .name("age").type().intType().noDefault()
            .name("isStudent").type().booleanType().noDefault()
            .endRecord();

        StructType structType = AvroSchemaUtilExtended.toDelta(avroSchema, false);

        List<ColumnInfo> columns = UnityCatalogUtil.convertDeltaSchemaToColumns(structType);

        assertNotNull(columns);
        assertEquals(3, columns.size());

        ColumnInfo nameColumn = columns.get(0);
        assertEquals("name", nameColumn.getName());
        assertNotEquals(Boolean.TRUE, nameColumn.getNullable());
        assertEquals("string", nameColumn.getTypeText());
        assertEquals(ColumnTypeName.STRING, nameColumn.getTypeName());
        assertEquals(0, nameColumn.getPosition());

        ColumnInfo ageColumn = columns.get(1);
        assertEquals("age", ageColumn.getName());
        assertNotEquals(Boolean.TRUE, ageColumn.getNullable());
        assertEquals("int", ageColumn.getTypeText());
        assertEquals(ColumnTypeName.INT, ageColumn.getTypeName());
        assertEquals(1, ageColumn.getPosition());

        ColumnInfo isStudentColumn = columns.get(2);
        assertEquals("isStudent", isStudentColumn.getName());
        assertNotEquals(Boolean.TRUE, isStudentColumn.getNullable());
        assertEquals("boolean", isStudentColumn.getTypeText());
        assertEquals(ColumnTypeName.BOOLEAN, isStudentColumn.getTypeName());
        assertEquals(2, isStudentColumn.getPosition());
    }

    @Test
    public void testConvertAvroSchemaToColumnsWithNestedStruct() {
        Schema nestedSchema = SchemaBuilder.record("NestedRecord")
            .fields()
            .name("field1").type().stringType().noDefault()
            .name("field2").type().intType().noDefault()
            .endRecord();

        Schema avroSchema = SchemaBuilder.record("TestRecord")
            .fields()
            .name("nestedField").type(nestedSchema).noDefault()
            .endRecord();

        StructType structType = AvroSchemaUtilExtended.toDelta(avroSchema, false);

        List<ColumnInfo> columns = UnityCatalogUtil.convertDeltaSchemaToColumns(structType);

        assertNotNull(columns);
        assertEquals(1, columns.size());

        ColumnInfo nestedColumn = columns.get(0);
        assertEquals("nestedField", nestedColumn.getName());
        assertNotEquals(Boolean.TRUE, nestedColumn.getNullable());
        assertEquals("struct<field1 string NOT NULL, field2 int NOT NULL>", nestedColumn.getTypeText());
        assertEquals(ColumnTypeName.STRUCT, nestedColumn.getTypeName());
        assertEquals(0, nestedColumn.getPosition());
    }

    @Test
    public void testConvertAvroSchemaToColumnsWithArrayType() {
        Schema avroSchema = SchemaBuilder.record("TestRecord")
            .fields()
            .name("scores").type().array().items().intType().noDefault()
            .endRecord();

        StructType structType = AvroSchemaUtilExtended.toDelta(avroSchema, false);

        List<ColumnInfo> columns = UnityCatalogUtil.convertDeltaSchemaToColumns(structType);

        assertNotNull(columns);
        assertEquals(1, columns.size());

        ColumnInfo scoresColumn = columns.get(0);
        assertEquals("scores", scoresColumn.getName());
        assertNotEquals(Boolean.TRUE, scoresColumn.getNullable());
        assertEquals("array<int>", scoresColumn.getTypeText());
        assertEquals(ColumnTypeName.ARRAY, scoresColumn.getTypeName());
        assertEquals(0, scoresColumn.getPosition());
    }

    @Test
    public void testConvertAvroSchemaToColumnsWithMapType() {
        Schema avroSchema = SchemaBuilder.record("TestRecord")
            .fields()
            .name("metadata").type().map().values().stringType().noDefault()
            .endRecord();

        StructType structType = AvroSchemaUtilExtended.toDelta(avroSchema, false);

        List<ColumnInfo> columns = UnityCatalogUtil.convertDeltaSchemaToColumns(structType);

        assertNotNull(columns);
        assertEquals(1, columns.size());

        ColumnInfo metadataColumn = columns.get(0);
        assertEquals("metadata", metadataColumn.getName());
        assertNotEquals(Boolean.TRUE, metadataColumn.getNullable());
        assertEquals("map<string,string>", metadataColumn.getTypeText());
        assertEquals(ColumnTypeName.MAP, metadataColumn.getTypeName());
        assertEquals(0, metadataColumn.getPosition());
    }

    @Test
    public void testConvertAvroSchemaToColumnsWithNullableField() {
        Schema avroSchema = SchemaBuilder.record("TestRecord")
            .fields()
            .name("name").type().nullable().stringType().noDefault()
            .endRecord();

        StructType structType = AvroSchemaUtilExtended.toDelta(avroSchema, false);

        List<ColumnInfo> columns = UnityCatalogUtil.convertDeltaSchemaToColumns(structType);

        assertNotNull(columns);
        assertEquals(1, columns.size());

        ColumnInfo nameColumn = columns.get(0);
        assertEquals("name", nameColumn.getName());
        assertEquals(Boolean.TRUE, nameColumn.getNullable());
        assertEquals("string", nameColumn.getTypeText());
        assertEquals(ColumnTypeName.STRING, nameColumn.getTypeName());
        assertEquals(0, nameColumn.getPosition());
    }

    @Test
    public void testConvertAvroSchemaToColumnsWithArrayOfRecord() {
        Schema elementSchema = SchemaBuilder.record("ElementRecord")
            .fields()
            .name("field1").type().stringType().noDefault()
            .name("field2").type().intType().noDefault()
            .endRecord();

        Schema avroSchema = SchemaBuilder.record("TestRecord")
            .fields()
            .name("arrayField").type().array().items(elementSchema).noDefault()
            .endRecord();

        StructType structType = AvroSchemaUtilExtended.toDelta(avroSchema, false);

        List<ColumnInfo> columns = UnityCatalogUtil.convertDeltaSchemaToColumns(structType);

        assertNotNull(columns);
        assertEquals(1, columns.size());

        ColumnInfo arrayColumn = columns.get(0);
        assertEquals("arrayField", arrayColumn.getName());
        assertNotEquals(Boolean.TRUE, arrayColumn.getNullable());
        assertEquals("array<struct<field1 string, field2 int>>", arrayColumn.getTypeText());
        assertEquals(ColumnTypeName.ARRAY, arrayColumn.getTypeName());
        assertEquals(0, arrayColumn.getPosition());
    }

    @Test
    public void testConvertAvroSchemaToColumnsWithDecimalType() {
        String schemaJsonString = "{"
            + "\"type\": \"bytes\","
            + "\"logicalType\": \"decimal\","
            + "\"precision\": " + 38 + ","
            + "\"scale\": " + 16
            + "}";
        Schema decimalSchema = new Schema.Parser().parse(schemaJsonString);

        List<Schema.Field> fields = new ArrayList<>();
        Schema schema = Schema.createRecord("DecimalRecord", "", "", false);
        fields.add(new Schema.Field("amount", decimalSchema));
        schema.setFields(fields);

        StructType structType = AvroSchemaUtilExtended.toDelta(schema, false);
        List<ColumnInfo> columns = UnityCatalogUtil.convertDeltaSchemaToColumns(structType);

        assertNotNull(columns);
        assertEquals(1, columns.size());

        ColumnInfo decimalColumn = columns.get(0);
        assertEquals("amount", decimalColumn.getName());
        assertNotEquals(Boolean.TRUE, decimalColumn.getNullable());
        assertEquals("Decimal(38, 16)", decimalColumn.getTypeText());
        assertEquals(ColumnTypeName.DECIMAL, decimalColumn.getTypeName());
        assertEquals(0, decimalColumn.getPosition());
    }

    @Test
    public void testGenerateStructTypeText() {
        // Test simple struct
        List<StructField> fields = new ArrayList<>();
        fields.add(new StructField("name", StringType.STRING, false));
        fields.add(new StructField("age", IntegerType.INTEGER, true));
        fields.add(new StructField("active", BooleanType.BOOLEAN, false));
        StructType simpleStruct = new StructType(fields);

        String ddl = UnityCatalogUtil.generateStructTypeText(simpleStruct);
        assertEquals("name string NOT NULL, age int, active boolean NOT NULL", ddl);

        // Test nested struct
        List<StructField> addressFields = new ArrayList<>();
        addressFields.add(new StructField("street", StringType.STRING, true));
        addressFields.add(new StructField("zipcode", IntegerType.INTEGER, false));
        StructType addressStruct = new StructType(addressFields);

        List<StructField> personFields = new ArrayList<>();
        personFields.add(new StructField("name", StringType.STRING, false));
        personFields.add(new StructField("address", addressStruct, true));
        StructType personStruct = new StructType(personFields);

        String nestedDdl = UnityCatalogUtil.generateStructTypeText(personStruct);
        assertEquals("name string NOT NULL, address struct<street string, zipcode int NOT NULL>", nestedDdl);
    }

    @Test
    public void testGenerateStructTypeTextWithArrayAndMap() {
        // Test struct with array field
        List<StructField> fields = new ArrayList<>();
        fields.add(new StructField("scores", new ArrayType(IntegerType.INTEGER, true), false));
        fields.add(new StructField("tags", new ArrayType(StringType.STRING, true), true));
        StructType structWithArray = new StructType(fields);

        String ddl = UnityCatalogUtil.generateStructTypeText(structWithArray);
        assertEquals("scores array<int> NOT NULL, tags array<string>", ddl);

        // Test struct with map field
        List<StructField> mapFields = new ArrayList<>();
        mapFields.add(new StructField("metadata", new MapType(StringType.STRING, StringType.STRING, true), false));
        mapFields.add(new StructField("counts", new MapType(StringType.STRING, IntegerType.INTEGER, true), true));
        StructType structWithMap = new StructType(mapFields);

        ddl = UnityCatalogUtil.generateStructTypeText(structWithMap);
        assertEquals("metadata map<string,string> NOT NULL, counts map<string,int>", ddl);
    }

    @Test
    public void testGenerateStructTypeTextWithArrayOfStruct() {
        // Create struct type for array element
        List<StructField> itemFields = new ArrayList<>();
        itemFields.add(new StructField("id", IntegerType.INTEGER, false));
        itemFields.add(new StructField("value", StringType.STRING, true));
        StructType itemStruct = new StructType(itemFields);

        // Create main struct with array of structs
        List<StructField> fields = new ArrayList<>();
        fields.add(new StructField("items", new ArrayType(itemStruct, true), false));
        StructType struct = new StructType(fields);

        String ddl = UnityCatalogUtil.generateStructTypeText(struct);
        assertEquals("items array<struct<id int NOT NULL, value string>> NOT NULL", ddl);
    }

    @Test
    public void testConvertColumnsToDeltaSchema() {
        List<ColumnInfo> columns = new ArrayList<>();

        ColumnInfo col1 = new ColumnInfo();
        col1.setName("id");
        col1.setTypeText("long");
        col1.setNullable(false);
        columns.add(col1);

        ColumnInfo col2 = new ColumnInfo();
        col2.setName("name");
        col2.setTypeText("string");
        col2.setNullable(true);
        columns.add(col2);

        ColumnInfo col3 = new ColumnInfo();
        col3.setName("amount");
        col3.setTypeText("double");
        col3.setNullable(false);
        columns.add(col3);

        StructType schema = UnityCatalogUtil.convertColumnsToDeltaSchema(columns);

        assertNotNull(schema);
        assertEquals(3, schema.fields().size());

        StructField field1 = schema.fields().get(0);
        assertEquals("id", field1.getName());
        assertTrue(field1.getDataType() instanceof LongType);
        assertFalse(field1.isNullable());

        StructField field2 = schema.fields().get(1);
        assertEquals("name", field2.getName());
        assertTrue(field2.getDataType() instanceof StringType);
        assertTrue(field2.isNullable());

        StructField field3 = schema.fields().get(2);
        assertEquals("amount", field3.getName());
        assertTrue(field3.getDataType() instanceof DoubleType);
        assertFalse(field3.isNullable());
    }

    @Test
    public void testConvertColumnsToDeltaSchemaWithComplexTypes() {
        List<ColumnInfo> columns = new ArrayList<>();

        // Array column
        ColumnInfo arrayCol = new ColumnInfo();
        arrayCol.setName("scores");
        arrayCol.setTypeText("array<int>");
        arrayCol.setNullable(true);
        columns.add(arrayCol);

        // Map column
        ColumnInfo mapCol = new ColumnInfo();
        mapCol.setName("metadata");
        mapCol.setTypeText("map<string,double>");
        mapCol.setNullable(false);
        columns.add(mapCol);

        // Struct column
        ColumnInfo structCol = new ColumnInfo();
        structCol.setName("address");
        structCol.setTypeText("struct<street string, zipcode int>");
        structCol.setNullable(true);
        columns.add(structCol);

        StructType schema = UnityCatalogUtil.convertColumnsToDeltaSchema(columns);

        assertNotNull(schema);
        assertEquals(3, schema.fields().size());

        // Verify array field
        StructField arrayField = schema.fields().get(0);
        assertEquals("scores", arrayField.getName());
        assertTrue(arrayField.getDataType() instanceof ArrayType);
        ArrayType arrayType = (ArrayType) arrayField.getDataType();
        assertTrue(arrayType.getElementType() instanceof IntegerType);

        // Verify map field
        StructField mapField = schema.fields().get(1);
        assertEquals("metadata", mapField.getName());
        assertTrue(mapField.getDataType() instanceof MapType);
        MapType mapType = (MapType) mapField.getDataType();
        assertTrue(mapType.getKeyType() instanceof StringType);
        assertTrue(mapType.getValueType() instanceof DoubleType);

        // Verify struct field
        StructField structField = schema.fields().get(2);
        assertEquals("address", structField.getName());
        assertTrue(structField.getDataType() instanceof StructType);
    }

    @Test
    public void testConvertColumnsToDeltaSchemaWithUnsupportedType() {
        List<ColumnInfo> columns = new ArrayList<>();
        ColumnInfo col = new ColumnInfo();
        col.setName("invalid");
        col.setTypeText("unsupported_type");
        columns.add(col);

        assertThrows(IllegalArgumentException.class, () -> {
            UnityCatalogUtil.convertColumnsToDeltaSchema(columns);
        });
    }

    @Test
    public void testConvertDeltaSchemaToColumnsWithAllTypes() {
        List<StructField> fields = new ArrayList<>();
        fields.add(new StructField("string_col", StringType.STRING, true));
        fields.add(new StructField("boolean_col", BooleanType.BOOLEAN, false));
        fields.add(new StructField("byte_col", ByteType.BYTE, true));
        fields.add(new StructField("short_col", ShortType.SHORT, false));
        fields.add(new StructField("int_col", IntegerType.INTEGER, true));
        fields.add(new StructField("long_col", LongType.LONG, false));
        fields.add(new StructField("float_col", FloatType.FLOAT, true));
        fields.add(new StructField("double_col", DoubleType.DOUBLE, false));
        fields.add(new StructField("binary_col", BinaryType.BINARY, true));
        fields.add(new StructField("date_col", DateType.DATE, false));
        fields.add(new StructField("timestamp_col", TimestampType.TIMESTAMP, true));
        fields.add(new StructField("timestamp_ntz_col", TimestampNTZType.TIMESTAMP_NTZ, false));
        fields.add(new StructField("decimal_col", new DecimalType(10, 2), true));

        StructType schema = new StructType(fields);
        List<ColumnInfo> columns = UnityCatalogUtil.convertDeltaSchemaToColumns(schema);

        assertEquals(13, columns.size());

        // Verify each column type
        assertEquals(ColumnTypeName.STRING, columns.get(0).getTypeName());
        assertEquals(ColumnTypeName.BOOLEAN, columns.get(1).getTypeName());
        assertEquals(ColumnTypeName.BYTE, columns.get(2).getTypeName());
        assertEquals(ColumnTypeName.SHORT, columns.get(3).getTypeName());
        assertEquals(ColumnTypeName.INT, columns.get(4).getTypeName());
        assertEquals(ColumnTypeName.LONG, columns.get(5).getTypeName());
        assertEquals(ColumnTypeName.FLOAT, columns.get(6).getTypeName());
        assertEquals(ColumnTypeName.DOUBLE, columns.get(7).getTypeName());
        assertEquals(ColumnTypeName.BINARY, columns.get(8).getTypeName());
        assertEquals(ColumnTypeName.DATE, columns.get(9).getTypeName());
        assertEquals(ColumnTypeName.TIMESTAMP, columns.get(10).getTypeName());
        assertEquals(ColumnTypeName.TIMESTAMP_NTZ, columns.get(11).getTypeName());
        assertEquals(ColumnTypeName.DECIMAL, columns.get(12).getTypeName());
    }

    @Test
    public void testConvertDeltaSchemaToColumnsWithUnsupportedType() {
        // Create a mock unsupported DataType
        DataType unsupportedType = mock(DataType.class);
        when(unsupportedType.toString()).thenReturn("UnsupportedType");

        List<StructField> fields = new ArrayList<>();
        fields.add(new StructField("unsupported_col", unsupportedType, true));
        StructType schema = new StructType(fields);

        assertThrows(IllegalArgumentException.class, () -> {
            UnityCatalogUtil.convertDeltaSchemaToColumns(schema);
        });
    }

    @Test
    public void testGenerateExternalHadoopConfigWithAwsCredentials() {
        LakehouseConfiguration lakehouseConfig = mock(LakehouseConfiguration.class);
        GenerateTemporaryTableCredentialResponse tempCreds = mock(GenerateTemporaryTableCredentialResponse.class);
        AwsCredentials awsCreds = mock(AwsCredentials.class);

        when(tempCreds.getAwsTempCredentials()).thenReturn(awsCreds);
        when(awsCreds.getAccessKeyId()).thenReturn("test-access-key");
        when(awsCreds.getSecretAccessKey()).thenReturn("test-secret-key");
        when(awsCreds.getSessionToken()).thenReturn("test-session-token");

        Configuration config = UnityCatalogUtil.generateExternalHadoopConfig(lakehouseConfig, tempCreds);

        assertNotNull(config);
        assertEquals("org.apache.hadoop.fs.s3a.S3AFileSystem", config.get("fs.s3a.impl"));
        assertEquals("test-access-key", config.get("fs.s3a.access.key"));
        assertEquals("test-secret-key", config.get("fs.s3a.secret.key"));
        assertEquals("test-session-token", config.get("fs.s3a.session.token"));
        assertEquals("true", config.get("fs.s3a.impl.disable.cache"));
        assertEquals("true", config.get("fs.s3a.path.style.access"));
    }

    @Test
    public void testGenerateExternalHadoopConfigWithAzureCredentials() {
        LakehouseConfiguration lakehouseConfig = mock(LakehouseConfiguration.class);
        GenerateTemporaryTableCredentialResponse tempCreds = mock(GenerateTemporaryTableCredentialResponse.class);
        AzureUserDelegationSas azureSas = mock(AzureUserDelegationSas.class);

        when(tempCreds.getAwsTempCredentials()).thenReturn(null);
        when(tempCreds.getAzureUserDelegationSas()).thenReturn(azureSas);
        when(tempCreds.getUrl()).thenReturn("https://testaccount.dfs.core.windows.net/container/path");
        when(azureSas.getSasToken()).thenReturn("test-sas-token");

        Configuration config = UnityCatalogUtil.generateExternalHadoopConfig(lakehouseConfig, tempCreds);

        assertNotNull(config);
        assertEquals("org.apache.hadoop.fs.azurebfs.AzureBlobFileSystem", config.get("fs.abfss.impl"));
        assertEquals("false", config.get("fs.azure.account.hns.enabled"));
        assertEquals("SAS", config.get("fs.azure.account.auth.type"));
        assertEquals(UnityCatalogSasTokenProvider.class.getName(),
            config.get("fs.azure.sas.token.provider.type"));
    }

    @Test
    public void testGenerateExternalHadoopConfigWithMockCredentials() {
        LakehouseConfiguration lakehouseConfig = mock(LakehouseConfiguration.class);
        Configuration hadoopConfig = new Configuration();
        hadoopConfig.set("test.key", "test.value");
        when(lakehouseConfig.getHadoopConfiguration()).thenReturn(hadoopConfig);

        MockUnityCatalog.MockedGenerateTemporaryTableCredentialResponse mockCreds =
            mock(MockUnityCatalog.MockedGenerateTemporaryTableCredentialResponse.class);

        Configuration result = UnityCatalogUtil.generateExternalHadoopConfig(lakehouseConfig, mockCreds);

        assertNotNull(result);
        assertEquals(hadoopConfig, result);
        assertEquals("test.value", result.get("test.key"));
    }

    @Test
    public void testGenerateExternalHadoopConfigWithUnsupportedCredentials() {
        LakehouseConfiguration lakehouseConfig = mock(LakehouseConfiguration.class);
        GenerateTemporaryTableCredentialResponse tempCreds = mock(GenerateTemporaryTableCredentialResponse.class);

        when(tempCreds.getAwsTempCredentials()).thenReturn(null);
        when(tempCreds.getAzureUserDelegationSas()).thenReturn(null);

        assertThrows(IllegalStateException.class, () -> {
            UnityCatalogUtil.generateExternalHadoopConfig(lakehouseConfig, tempCreds);
        });
    }

    @Test
    public void testMapTypeWithStructValue() {
        // Test map with struct as value type
        List<ColumnInfo> columns = new ArrayList<>();
        ColumnInfo mapCol = new ColumnInfo();
        mapCol.setName("complex_map");
        mapCol.setTypeText("map<string,struct<field1 int, field2 string>>");
        mapCol.setNullable(true);
        columns.add(mapCol);

        StructType schema = UnityCatalogUtil.convertColumnsToDeltaSchema(columns);
        StructField mapField = schema.fields().get(0);
        assertTrue(mapField.getDataType() instanceof MapType);
        MapType mapType = (MapType) mapField.getDataType();
        assertTrue(mapType.getValueType() instanceof StructType);

        // Test the reverse conversion
        List<ColumnInfo> convertedColumns = UnityCatalogUtil.convertDeltaSchemaToColumns(schema);
        assertEquals("map<string,struct<field1 int, field2 string>>", convertedColumns.get(0).getTypeText());
    }

}
