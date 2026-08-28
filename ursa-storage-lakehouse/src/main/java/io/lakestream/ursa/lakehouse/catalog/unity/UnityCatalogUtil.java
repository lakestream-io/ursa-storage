/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.lakehouse.catalog.unity;

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
import io.delta.kernel.types.VariantType;
import io.lakestream.ursa.lakehouse.LakehouseConfiguration;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.azurebfs.AzureBlobFileSystem;
import org.apache.hadoop.fs.azurebfs.services.AuthType;

public class UnityCatalogUtil {


    public static List<ColumnInfo> convertDeltaSchemaToColumns(StructType structType) {
        List<ColumnInfo> columns = new ArrayList<>();
        List<StructField> fields = structType.fields();
        for (int i = 0; i < fields.size(); i++) {
            StructField field = fields.get(i);
            ColumnInfo column = new ColumnInfo();
            column.setName(field.getName());
            column.setNullable(field.isNullable());
            column.setTypeScale(0L);
            column.setTypePrecision(0L);
            DataType dataType = field.getDataType();
            if (dataType instanceof ArrayType) {
                DataType elementType = ((ArrayType) field.getDataType()).getElementType();
                if (elementType instanceof StructType) {
                    String structStr = "struct<" + generateStructTypeText((StructType) elementType) + ">";
                    column.setTypeText("array<" + structStr + ">");
                } else {
                    column.setTypeText("array<" + parseTextType(elementType) + ">");
                }
            } else if (dataType instanceof MapType) {
                String keyName = parseTextType(((MapType) dataType).getKeyType());
                DataType valueType = ((MapType) dataType).getValueType();
                if (valueType instanceof StructType) {
                    String structStr = "struct<" + generateStructTypeText((StructType) valueType) + ">";
                    column.setTypeText("map<" + keyName + "," + structStr + ">");
                } else {
                    String valueName = parseTextType(((MapType) dataType).getValueType());
                    column.setTypeText("map<" + keyName + "," + valueName + ">");
                }
            } else if (dataType instanceof StructType) {
                column.setTypeText("struct<" + generateStructTypeText((StructType) dataType) + ">");
            } else if (dataType instanceof DecimalType) {
                column.setTypePrecision((long) ((DecimalType) dataType).getPrecision());
                column.setTypeScale((long) ((DecimalType) dataType).getScale());
                column.setTypeText(parseTextType(dataType));
            } else {
                column.setTypeText(parseTextType(dataType));
            }
            column.setTypeName(convertDeltaTypeToUnityType(field.getDataType()));
            column.setTypeJson(field.getDataType().toString());
            column.setPosition((long) i);

            columns.add(column);
        }
        return columns;
    }

    private static String parseTextType(DataType dataType) {
        //Handle the delta kernel integer type.
        if (dataType instanceof IntegerType) {
            return "int";
        }
        return dataType.toString();
    }

    public static String generateStructTypeText(StructType schema) {
        StringBuilder ddl = new StringBuilder();
        List<StructField> fields = schema.fields();
        // Process each field in the schema
        for (int i = 0; i < fields.size(); i++) {
            StructField field = fields.get(i);
            // Add field name and type
            ddl.append(field.getName()).append(" ");

            // If the field is a struct, generate DDL for nested StructType
            if (field.getDataType() instanceof StructType) {
                ddl.append("struct<");
                ddl.append(generateStructTypeText((StructType) field.getDataType()));
                ddl.append(">");
            } else if (field.getDataType() instanceof ArrayType) {
                DataType elementType = ((ArrayType) field.getDataType()).getElementType();
                if (elementType instanceof StructType) {
                    String structStr = "struct<" + generateStructTypeText((StructType) elementType) + ">";
                    ddl.append("array<").append(structStr).append(">");
                } else {
                    ddl.append("array<").append(parseTextType(elementType)).append(">");
                }
            } else if (field.getDataType() instanceof MapType) {
                String keyName = parseTextType(((MapType) field.getDataType()).getKeyType());
                String valueName = parseTextType(((MapType) field.getDataType()).getValueType());
                ddl.append("map<").append(keyName).append(",").append(valueName).append(">");
            } else {
                ddl.append(parseTextType(field.getDataType()));
            }
            // Add NOT NULL constraint for individual fields, not structs
            if (!field.isNullable() && !(field.getDataType() instanceof StructType)) {
                ddl.append(" NOT NULL");
            }

            // Add a comma if this is not the last field in the schema
            if (i < fields.size() - 1) {
                ddl.append(", ");
            }
        }
        return ddl.toString();
    }

    private static ColumnTypeName convertDeltaTypeToUnityType(DataType dataType) {
        if (dataType instanceof StringType) {
            return ColumnTypeName.STRING;
        } else if (dataType instanceof BooleanType) {
            return ColumnTypeName.BOOLEAN;
        } else if (dataType instanceof ShortType) {
            return ColumnTypeName.SHORT;
        } else if (dataType instanceof IntegerType) {
            return ColumnTypeName.INT;
        } else if (dataType instanceof LongType) {
            return ColumnTypeName.LONG;
        } else if (dataType instanceof FloatType) {
            return ColumnTypeName.FLOAT;
        } else if (dataType instanceof DoubleType) {
            return ColumnTypeName.DOUBLE;
        } else if (dataType instanceof ByteType) {
            return ColumnTypeName.BYTE;
        } else if (dataType instanceof BinaryType) {
            return ColumnTypeName.BINARY;
        } else if (dataType instanceof TimestampType) {
            return ColumnTypeName.TIMESTAMP;
        } else if (dataType instanceof TimestampNTZType) {
            return ColumnTypeName.TIMESTAMP_NTZ;
        } else if (dataType instanceof ArrayType) {
            return ColumnTypeName.ARRAY;
        } else if (dataType instanceof MapType) {
            return ColumnTypeName.MAP;
        } else if (dataType instanceof StructType) {
            return ColumnTypeName.STRUCT;
        } else if (dataType instanceof DateType) {
            return ColumnTypeName.DATE;
        } else if (dataType instanceof DecimalType) {
            return ColumnTypeName.DECIMAL;
        } else if (dataType instanceof VariantType) {
            return ColumnTypeName.VARIANT;
        }
        throw new IllegalArgumentException("DataType not supported: " + dataType.toString());
    }

    public static StructType convertColumnsToDeltaSchema(List<ColumnInfo> columns) {
        List<StructField> fields = new ArrayList<>();
        for (ColumnInfo column : columns) {
            String name = column.getName();
            DataType dataType = parseColumnType(column.getTypeText());
            fields.add(new StructField(name, dataType, Boolean.TRUE.equals(column.getNullable())));
        }
        return new StructType(fields);
    }

    private static DataType parseColumnType(String typeText) {
        if (typeText.startsWith("array<")) {
            String elementTypeText = extractBetweenBrackets(typeText);
            return new ArrayType(parseColumnType(elementTypeText), true);
        } else if (typeText.startsWith("map<")) {
            String mapContent = extractBetweenBrackets(typeText);
            String[] keyValue = mapContent.split(",", 2);
            if (keyValue.length == 2) {
                DataType keyType = parseColumnType(keyValue[0].trim());
                DataType valueType = parseColumnType(keyValue[1].trim());
                return new MapType(keyType, valueType, true);
            }
        } else if (typeText.startsWith("struct<")) {
            String structContent = extractBetweenBrackets(typeText);
            List<StructField> structFields = parseStructFields(structContent);
            return new StructType(structFields);
        }
        return parseBasicType(typeText);
    }

    private static List<StructField> parseStructFields(String structText) {
        List<StructField> fields = new ArrayList<>();
        String[] fieldPairs = structText.split(", ");
        for (String fieldPair : fieldPairs) {
            String[] parts = fieldPair.trim().split(" ");
            if (parts.length == 2) {
                String fieldName = parts[0].trim();
                DataType fieldType = parseColumnType(parts[1].trim());
                fields.add(new StructField(fieldName, fieldType, true));
            }
        }
        return fields;
    }

    private static DataType parseBasicType(String type) {
        return switch (type.toLowerCase(Locale.ROOT)) {
            case "string" -> StringType.STRING;
            case "boolean" -> BooleanType.BOOLEAN;
            case "short" -> ShortType.SHORT;
            case "int" -> IntegerType.INTEGER;
            case "long" -> LongType.LONG;
            case "float" -> FloatType.FLOAT;
            case "double" -> DoubleType.DOUBLE;
            case "byte" -> ByteType.BYTE;
            case "binary" -> BinaryType.BINARY;
            case "timestamp" -> TimestampType.TIMESTAMP;
            default -> throw new IllegalArgumentException("Unsupported type: " + type);
        };
    }

    private static String extractBetweenBrackets(String text) {
        Pattern pattern = Pattern.compile("<(.*)>");
        Matcher matcher = pattern.matcher(text);
        if (matcher.find()) {
            return matcher.group(1);
        }
        return "";
    }

    public static Configuration generateExternalHadoopConfig(LakehouseConfiguration lakehouseConfiguration,
            GenerateTemporaryTableCredentialResponse temporaryCredentials) {
        if (temporaryCredentials instanceof MockUnityCatalog.MockedGenerateTemporaryTableCredentialResponse) {
            return lakehouseConfiguration.getHadoopConfiguration();
        }

        AwsCredentials awsTempCredentials =
                temporaryCredentials.getAwsTempCredentials();
        if (awsTempCredentials != null) {
            Configuration conf = new Configuration();
            // https://issues.apache.org/jira/browse/HADOOP-19559
            // https://github.com/awslabs/analytics-accelerator-s3#memory-used-by-library
            // hadoop changed the type to the analytics which takes a lot of memory.
            conf.set("fs.s3a.input.stream.type", "classic");
            conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem");
            conf.set("fs.s3a.access.key", awsTempCredentials.getAccessKeyId());
            conf.set("fs.s3a.secret.key", awsTempCredentials.getSecretAccessKey());
            conf.set("fs.s3a.session.token", awsTempCredentials.getSessionToken());
            conf.set("fs.s3a.impl.disable.cache", "true");
            conf.set("fs.s3a.path.style.access", "true");
            return conf;
        }
        AzureUserDelegationSas azureSas = temporaryCredentials.getAzureUserDelegationSas();
        if (azureSas != null) {
            Configuration conf = new Configuration();
            conf.set("fs.abfss.impl", AzureBlobFileSystem.class.getName());
            conf.set("fs.azure.account.hns.enabled", "false");
            conf.set("fs.azure.account.auth.type", AuthType.SAS.name());
            conf.set("fs.azure.sas.token.provider.type", UnityCatalogSasTokenProvider.class.getName());
            UnityCatalogSasTokenProvider.updateToken(parseSubPathPath(temporaryCredentials.getUrl()),
                    azureSas.getSasToken());
            return conf;
        }
        throw new IllegalStateException("Unsupported temporary credentials");
    }

    private static String parseSubPathPath(String fullPath) {
        int startIndex = fullPath.indexOf(".net");
        if (startIndex != -1) {
            return fullPath.substring(startIndex + ".net".length());
        }
        throw new IllegalArgumentException("Unsupported path: " + fullPath);
    }
}