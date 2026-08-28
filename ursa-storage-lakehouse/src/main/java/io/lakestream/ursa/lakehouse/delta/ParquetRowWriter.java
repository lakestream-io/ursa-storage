/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.lakehouse.delta;

import io.delta.kernel.data.Row;
import io.delta.kernel.defaults.engine.hadoopio.HadoopFileIO;
import io.delta.kernel.defaults.internal.data.DefaultRowBasedColumnarBatch;
import io.delta.kernel.defaults.internal.parquet.CustomKernelParquetWriter;
import io.delta.kernel.expressions.Column;
import io.delta.kernel.internal.util.SchemaUtils;
import io.delta.kernel.types.ArrayType;
import io.delta.kernel.types.BinaryType;
import io.delta.kernel.types.BooleanType;
import io.delta.kernel.types.ByteType;
import io.delta.kernel.types.DataType;
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
import io.delta.kernel.types.VariantType;
import io.delta.kernel.utils.DataFileStatus;
import io.lakestream.ursa.lakehouse.writer.ParquetFileStat;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;

public class ParquetRowWriter {

    private final StructType schema;
    private final StructType parquetSchema;
    private final List<String> partitionKeys;
    private final String location;
    private final Configuration hadoopConfig;
    private final Map<String, List<Row>> partitionRows;
    private final Map<String, CustomKernelParquetWriter> writerMap;
    private final int batchSize;
    //The variant type didn't support stats, so we need to truncate it.
    private final StructType statsSchema;


    public ParquetRowWriter(String location, Configuration hadoopConfig, List<String> partitionKeys,
                            StructType schema, int batchSize) {
        this.location = location;
        this.hadoopConfig = hadoopConfig;
        this.partitionKeys = partitionKeys;
        this.schema = schema;
        this.parquetSchema = toParquetSchema(schema);
        this.statsSchema = toStatsSchema(schema);
        this.partitionRows = new ConcurrentHashMap<>();
        this.writerMap = new ConcurrentHashMap<>();
        this.batchSize = batchSize;
    }

    public void write(GenericRow genericRow) throws IOException {
        String partitionValuePath = getPartitionValuePath(genericRow, partitionKeys);
        List<Row> buffers = partitionRows.computeIfAbsent(partitionValuePath, (path) -> new ArrayList<>());
        buffers.add(toParquetRow(genericRow, schema));
        if (buffers.size() >= batchSize) {
            CustomKernelParquetWriter writer =
                    writerMap.computeIfAbsent(partitionValuePath, path -> initWriter(parquetSchema, path));
            DefaultRowBasedColumnarBatch defaultRowBasedColumnarBatch =
                    new DefaultRowBasedColumnarBatch(parquetSchema, new ArrayList<>(buffers));
            buffers.clear();
            writer.write(defaultRowBasedColumnarBatch);
        }
    }

    public List<ParquetFileStat> close() throws IOException {
        for (Map.Entry<String, List<Row>> entry : partitionRows.entrySet()) {
            List<Row> rows = entry.getValue();
            if (!rows.isEmpty()) {
                String dataPath = entry.getKey();
                CustomKernelParquetWriter writer =
                        writerMap.computeIfAbsent(dataPath, path -> initWriter(parquetSchema, path));
                DefaultRowBasedColumnarBatch defaultRowBasedColumnarBatch =
                        new DefaultRowBasedColumnarBatch(parquetSchema, new ArrayList<>(rows));
                rows.clear();
                writer.write(defaultRowBasedColumnarBatch);
            }
        }
        try {
            List<ParquetFileStat> result = new ArrayList<>();
            for (Map.Entry<String, CustomKernelParquetWriter> entry : writerMap.entrySet()) {
                String path = entry.getKey();
                CustomKernelParquetWriter writer = entry.getValue();
                for (DataFileStatus ele : writer.closeAll()) {
                    String stats = null;
                    if (ele.getStatistics().isPresent() && statsSchema.length() > 0) {
                        stats = ele.getStatistics().get().serializeAsJson(statsSchema);
                    }
                    String fileName = Paths.get(ele.getPath()).getFileName().toString();
                    String filePath = Objects.equals(path, "") ? fileName : path + "/" + fileName;
                    String fullPath = location.endsWith("/") ? location + filePath : location + "/" + filePath;
                    Map<String, String> partitionValues = parsePartitionValues(path);
                    ParquetFileStat parquetFileStat =
                            new ParquetFileStat(filePath, fullPath, ele.getSize(), stats, partitionValues,
                                    Collections.emptyMap());
                    result.add(parquetFileStat);
                }
            }
            return result;
        } catch (IOException e) {
            for (CustomKernelParquetWriter writer : writerMap.values()) {
                try {
                    writer.close();
                } catch (IOException ignore) {

                }
            }
            throw e;
        }
    }

    private CustomKernelParquetWriter initWriter(StructType schema, String path) {
        List<Column> statsColumns =
                SchemaUtils.collectLeafColumns(schema, Collections.emptySet(), -1);
        String finalLocation;
        if (StringUtils.isBlank(path)) {
            finalLocation = location;
        } else {
            finalLocation = location.endsWith("/") ? location + path : location + "/" + path;
        }
        return CustomKernelParquetWriter.multiFileWriter(new HadoopFileIO(hadoopConfig), finalLocation, statsColumns);
    }

    StructType toParquetSchema(StructType logicalSchema) {
        StructType result = new StructType();
        for (StructField field : logicalSchema.fields()) {
            result = result.add(field.getName(), toParquetType(field.getDataType()), field.isNullable());
        }
        return result;
    }

    StructType toStatsSchema(StructType logicalSchema) {
        StructType result = new StructType();
        for (StructField field : logicalSchema.fields()) {
            DataType dataType = field.getDataType();
            if (dataType instanceof VariantType) {
                continue;
            }
            if (dataType instanceof StructType structType) {
                StructType nestedStatsSchema = toStatsSchema(structType);
                if (nestedStatsSchema.length() == 0) {
                    continue;
                }
                result = result.add(field.getName(), nestedStatsSchema, field.isNullable());
            } else {
                result = result.add(field.getName(), dataType, field.isNullable());
            }
        }
        return result;
    }

    private DataType toParquetType(DataType logicalType) {
        if (logicalType instanceof VariantType) {
            return DeltaVariantUtils.variantSchema();
        } else if (logicalType instanceof StructType structType) {
            return toParquetSchema(structType);
        }
        return logicalType;
    }

    private GenericRow toParquetRow(GenericRow logicalRow, StructType logicalSchema) {
        StructType physicalSchema = toParquetSchema(logicalSchema);
        Map<Integer, Object> ordinalToValue = new HashMap<>();
        for (int i = 0; i < logicalSchema.length(); i++) {
            Object logicalValue = logicalRow.getValue(i);
            if (logicalValue == null) {
                ordinalToValue.put(i, null);
                continue;
            }
            ordinalToValue.put(i, toParquetValue(logicalSchema.at(i).getDataType(), logicalValue));
        }
        return new GenericRow(physicalSchema, ordinalToValue);
    }

    private Object toParquetValue(DataType logicalType, Object logicalValue) {
        if (logicalValue == null) {
            return null;
        }
        if (logicalType instanceof VariantType) {
            return toVariantParquetValue(logicalValue);
        } else if (logicalType instanceof StructType structType) {
            return toParquetRow((GenericRow) logicalValue, structType);
        }
        return logicalValue;
    }

    private GenericRow toVariantParquetValue(Object logicalValue) {
        if (logicalValue instanceof GenericRow genericRow) {
            return genericRow;
        }
        return DeltaVariantUtils.fromValue(logicalValue);
    }

    public String getPartitionValuePath(GenericRow row, List<String> partitionColumns) {
        if (partitionColumns == null || partitionColumns.isEmpty()) {
            return "";
        }
        StringBuilder pathBuilder = new StringBuilder();
        boolean first = true;
        for (String column : partitionColumns) {
            StructField field = row.getSchema().get(column);
            if (field == null) {
                return "";
            }
            if (!first) {
                pathBuilder.append("/");
            }
            pathBuilder.append(field.getName())
                    .append("=");
            Object value = getValue(row, field.getName());
            pathBuilder.append(value);
            first = false;
        }
        return pathBuilder.toString();
    }

    private Object getValue(GenericRow row, String fieldName) {
        StructType schema = row.getSchema();
        StructField structField = schema.get(fieldName);
        if (structField == null) {
            throw new IllegalArgumentException("The field '" + fieldName + "' is not defined");
        }
        DataType dataType = structField.getDataType();
        int index = schema.indexOf(fieldName);
        if (dataType instanceof StringType) {
            return row.getString(index);
        } else if (dataType instanceof ByteType) {
            return row.getByte(index);
        } else if (dataType instanceof IntegerType) {
            return row.getInt(index);
        } else if (dataType instanceof LongType) {
            return row.getLong(index);
        } else if (dataType instanceof FloatType) {
            return row.getFloat(index);
        } else if (dataType instanceof DoubleType) {
            return row.getDouble(index);
        } else if (dataType instanceof BooleanType) {
            return row.getBoolean(index);
        } else if (dataType instanceof StructType) {
            return row.getStruct(index);
        } else if (dataType instanceof ArrayType) {
            return row.getArray(index);
        } else if (dataType instanceof MapType) {
            return row.getMap(index);
        } else if (dataType instanceof BinaryType) {
            return row.getBinary(index);
        } else if (dataType instanceof ShortType) {
            return row.getShort(index);
        } else if (dataType instanceof DecimalType) {
            return row.getDecimal(index);
        }
        throw new IllegalArgumentException("Unsupported data type: " + dataType);
    }

    public Map<String, String> parsePartitionValues(String input) {
        Map<String, String> map = new HashMap<>();
        String[] pairs = input.split("/");
        for (String pair : pairs) {
            String[] keyValue = pair.split("=", 2);
            if (keyValue.length == 2) {
                map.put(keyValue[0], keyValue[1]);
            }
        }
        return map;
    }

}
