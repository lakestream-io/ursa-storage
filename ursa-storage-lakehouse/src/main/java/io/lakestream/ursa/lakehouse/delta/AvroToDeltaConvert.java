/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.lakehouse.delta;

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
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.avro.LogicalType;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericFixed;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.util.Utf8;

public class AvroToDeltaConvert {

    public static GenericRow convert(GenericRecord avroRecord, StructType structType) {
        Schema originSchema = avroRecord.getSchema();
        Map<Integer, Object> ordinalToValue = new HashMap<>();
        for (int i = 0; i < structType.length(); i++) {
            StructField field = structType.at(i);
            String columnName = field.getName();
            Schema.Field originalField = originSchema.getField(columnName);
            if (originalField == null) {
                continue;
            }
            DataType columnType = field.getDataType();
            Object avroValue = avroRecord.get(columnName);
            if (columnType instanceof ArrayType) {
                ordinalToValue.put(i,
                    avroValue != null ? convertArray((List<?>) avroValue, (ArrayType) columnType) : null);
            } else if (columnType instanceof MapType) {
                ordinalToValue.put(i,
                    avroValue != null ? convertMap((Map<Object, Object>) avroValue, (MapType) columnType) : null);
            } else {
                Object o = convertValue(avroValue, originalField.schema(), columnType);
                ordinalToValue.put(i, o);
            }
        }
        return new GenericRow(structType, ordinalToValue);
    }

    private static ArrayValueImpl convertArray(List<?> avroArray, ArrayType arrayType) {
        List<Object> convertedValues = new ArrayList<>();
        for (Object element : avroArray) {
            convertedValues.add(convertElement(element, arrayType.getElementType()));
        }
        return new ArrayValueImpl(convertedValues, arrayType.getElementType());
    }

    private static MapValueImpl convertMap(Map<Object, Object> avroMap, MapType mapType) {
        Map<Object, Object> convertedValues = new HashMap<>();
        for (Map.Entry<Object, Object> entry : avroMap.entrySet()) {
            convertedValues.put(entry.getKey(), convertElement(entry.getValue(), mapType.getValueType()));
        }
        return new MapValueImpl(convertedValues, mapType.getKeyType(), mapType.getValueType());
    }

    public static Object convertValue(Object avroValue, Schema avroSchema, DataType deltaSchema) {
        if (avroValue == null) {
            return null;
        }
        LogicalType logicalType = getRealSchema(avroSchema).getLogicalType();
        if (logicalType != null) {
            Object logicalResult = convertLogicalType(avroValue, logicalType, deltaSchema);
            // Apply element type conversion to handle final type conversions (e.g., UUID to String)
            return convertElement(logicalResult, deltaSchema);
        }
        return convertElement(avroValue, deltaSchema);
    }


    private static Object convertElement(Object element, DataType targetType) {
        if (element == null) {
            return null;
        }
        if (targetType instanceof VariantType) {
            if (element instanceof GenericRow genericRow) {
                return genericRow;
            }
            if (element instanceof GenericRecord genericRecord) {
                return DeltaVariantUtils.fromJson(avroRecordToJson(genericRecord));
            }
            if (element instanceof GenericData.Array<?> array) {
                return DeltaVariantUtils.fromJson(avroArrayToJson(array));
            }
            if (element instanceof Utf8 utf8) {
                return DeltaVariantUtils.fromValue(utf8.toString());
            }
            return DeltaVariantUtils.fromValue(element);
        }
        if (targetType instanceof ByteType) {
            if (element instanceof Number) {
                return ((Number) element).byteValue();
            }
            return element;
        } else if (targetType instanceof ShortType) {
            if (element instanceof Number) {
                return ((Number) element).shortValue();
            }
            return element;
        } else if (targetType instanceof BooleanType) {
            return element;
        } else if (targetType instanceof IntegerType) {
            if (element instanceof Number) {
                return ((Number) element).intValue();
            }
            return element;
        } else if (targetType instanceof StringType) {
            // BUG FIX: Convert to string, including UUID objects
            if (element instanceof UUID) {
                return element.toString();
            }
            return element.toString();
        } else if (targetType instanceof LongType) {
            if (element instanceof Number) {
                return ((Number) element).longValue();
            }
            return element;
        } else if (targetType instanceof FloatType) {
            if (element instanceof Number) {
                return ((Number) element).floatValue();
            }
            return element;
        } else if (targetType instanceof DoubleType) {
            if (element instanceof Number) {
                return ((Number) element).doubleValue();
            }
            return element;
        } else if (targetType instanceof DecimalType) {
            return new BigDecimal(element.toString());
        } else if (targetType instanceof BinaryType) {
            if (element instanceof GenericData.Fixed) {
                return ((GenericData.Fixed) element).bytes();
            }
            if (element instanceof ByteBuffer) {
                return ((ByteBuffer) element).array();
            }
            return element;
        } else if (targetType instanceof TimestampNTZType || targetType instanceof TimestampType) {
            if (element instanceof Number) {
                // Handle timestamp as microseconds since epoch
                return ((Number) element).longValue();
            }
            return element;
        } else if (targetType instanceof DateType) {
            if (element instanceof Number) {
                // Handle date as days since epoch
                return ((Number) element).intValue();
            }
            return element;
        } else if (targetType instanceof StructType) {
            return convert((GenericRecord) element, (StructType) targetType);
        } else if (targetType instanceof ArrayType) {
            return convertArray((List<?>) element, (ArrayType) targetType);
        } else if (targetType instanceof MapType) {
            return convertMap((Map<Object, Object>) element, (MapType) targetType);
        } else {
            throw new UnsupportedOperationException("Unsupported element type: " + targetType);
        }
    }

    private static Object convertLogicalType(Object avroValue, LogicalType logicalType, DataType deltaType) {
        if (avroValue == null) {
            return null;
        }
        if (avroValue instanceof LocalDate date) {
            avroValue = (int) date.toEpochDay();
        } else if (avroValue instanceof LocalTime time) {
            if ("time-micros".equals(logicalType.getName())) {
                avroValue = time.toNanoOfDay() / 1000;
            } else {
                avroValue = (int) (time.toNanoOfDay() / 1_000_000);
            }
        } else if (avroValue instanceof Instant || avroValue instanceof LocalDateTime) {
            Instant instant = (avroValue instanceof LocalDateTime ldt)
                ? ldt.toInstant(ZoneOffset.UTC)
                : (Instant) avroValue;

            if (logicalType.getName().endsWith("-micros")) {
                avroValue = (instant.getEpochSecond() * 1_000_000L) + (instant.getNano() / 1000);
            } else {
                avroValue = instant.toEpochMilli();
            }
        }
        switch (logicalType.getName()) {
            case "date":
                return avroValue;
            case "time-millis":
                // Convert milliseconds to nanoseconds for LocalTime
                return LocalTime.ofNanoOfDay((Integer) avroValue * 1_000_000L).atDate(LocalDate.now())
                    .atOffset(ZoneOffset.UTC).toInstant().toEpochMilli() * 1000;
            case "time-micros":
                return LocalTime.ofNanoOfDay(((Long) avroValue) * 1000L).atDate(LocalDate.now())
                    .atOffset(ZoneOffset.UTC).toInstant().toEpochMilli() * 1000;
            case "timestamp-millis":
                // Convert milliseconds since epoch to microseconds
                return (Long) avroValue * 1000;
            case "timestamp-micros":
                return Instant.ofEpochSecond(((Long) avroValue) / 1_000_000, (((Long) avroValue) % 1_000_000) * 1000)
                    .toEpochMilli() * 1000;
            case "local-timestamp-millis":
                // BUG FIX: Convert milliseconds to microseconds, not through Instant
                return (Long) avroValue * 1000;
            case "local-timestamp-micros":
                // BUG FIX: Return microseconds directly without converting to Instant
                // Avro local-timestamp-micros is already in microseconds since epoch
                return (Long) avroValue;
            case "decimal":
                if (avroValue instanceof ByteBuffer buffer) {
                    byte[] bytes = new byte[buffer.remaining()];
                    buffer.duplicate().get(bytes);
                    DecimalType decimalType = (DecimalType) deltaType;
                    return new BigDecimal(new BigInteger(bytes), decimalType.getScale());
                }
                if (avroValue instanceof GenericFixed fixed) {
                    byte[] bytes = fixed.bytes();
                    DecimalType decimalType = (DecimalType) deltaType;
                    return new BigDecimal(new BigInteger(bytes), decimalType.getScale());
                }
                return avroValue;
            case "uuid":
                if (avroValue instanceof GenericFixed) {
                    byte[] bytes = ((GenericFixed) avroValue).bytes();
                    ByteBuffer buffer = ByteBuffer.wrap(bytes);
                    long mostSigBits = buffer.getLong();
                    long leastSigBits = buffer.getLong();
                    return new UUID(mostSigBits, leastSigBits);
                } else if (avroValue instanceof CharSequence) {
                    // Handle UUID with string base type
                    return UUID.fromString(avroValue.toString());
                }
                return avroValue;
            case "variant":
                if (avroValue instanceof GenericRecord genericRecord) {
                    return DeltaVariantUtils.fromJson(avroRecordToJson(genericRecord));
                }
                if (avroValue instanceof GenericData.Array<?> array) {
                    return DeltaVariantUtils.fromJson(avroArrayToJson(array));
                }
                if (avroValue instanceof Utf8 utf8) {
                    return DeltaVariantUtils.fromValue(utf8.toString());
                }
                return DeltaVariantUtils.fromValue(avroValue);
            default:
                return avroValue;
        }
    }

    private static String avroRecordToJson(GenericRecord record) {
        try (ByteArrayOutputStream outputStream = new ByteArrayOutputStream()) {
            DatumWriter<GenericRecord> writer = new GenericDatumWriter<>(record.getSchema());
            var encoder = EncoderFactory.get().jsonEncoder(record.getSchema(), outputStream);
            writer.write(record, encoder);
            encoder.flush();
            return outputStream.toString(StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new IllegalArgumentException("Failed to serialize Avro record to Delta variant JSON", e);
        }
    }

    private static String avroArrayToJson(GenericData.Array<?> array) {
        try (ByteArrayOutputStream outputStream = new ByteArrayOutputStream()) {
            DatumWriter<GenericData.Array<?>> writer = new GenericDatumWriter<>(array.getSchema());
            var encoder = EncoderFactory.get().jsonEncoder(array.getSchema(), outputStream);
            writer.write(array, encoder);
            encoder.flush();
            return outputStream.toString(StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new IllegalArgumentException("Failed to serialize Avro array to Delta variant JSON", e);
        }
    }

    private static Schema getRealSchema(Schema schema) {
        if (schema.isUnion()) {
            List<Schema> types = schema.getTypes();
            for (Schema subSchema : types) {
                if (subSchema.getType() != Schema.Type.NULL) {
                    return subSchema;
                }
            }
        }
        return schema;
    }

}
