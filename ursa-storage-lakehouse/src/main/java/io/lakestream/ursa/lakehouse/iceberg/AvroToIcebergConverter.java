/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.lakehouse.iceberg;

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
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.LogicalType;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericArray;
import org.apache.avro.generic.GenericFixed;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.types.Types.DecimalType;
import org.apache.iceberg.variants.Variant;
import org.apache.iceberg.variants.VariantMetadata;
import org.apache.iceberg.variants.VariantValue;
import org.apache.iceberg.variants.Variants;

// the logic should follow here: https://iceberg.apache.org/spec/#avro
@Slf4j
public class AvroToIcebergConverter {

    public static Record convert(GenericRecord avroRecord, org.apache.iceberg.Schema icebergSchema) {
        Record icebergRecord = org.apache.iceberg.data.GenericRecord.create(icebergSchema.asStruct());

        avroRecord.getSchema().getFields().forEach(avroField -> {
            String fieldName = avroField.name();
            Object avroValue = avroRecord.get(fieldName);
            if (icebergSchema.findField(fieldName) == null) {
                throw new IllegalArgumentException("Field not found in Iceberg schema: " + fieldName);
            }

            Type icebergFieldType = icebergSchema.findField(fieldName).type();

            Object icebergValue = convertValue(
                    avroValue,
                    avroField.schema(),
                    icebergFieldType
            );

            icebergRecord.setField(fieldName, icebergValue);
        });

        return icebergRecord;
    }

    public static Object convertValue(Object avroValue, Schema avroSchema, Type icebergType) {
        if (avroValue == null) {
            return null;
        }

        if (icebergType != null && icebergType.isVariantType()) {
            VariantMetadata metadata = extractVariantMetadataFromSchema(avroSchema);
            VariantValue variantValue = convertToVariantValue(avroValue);
            return Variant.of(metadata, variantValue);
        }

        LogicalType logicalType = avroSchema.getLogicalType();
        if (logicalType != null) {
            return convertLogicalType(avroValue, logicalType, icebergType);
        }

        return switch (avroSchema.getType()) {
            case RECORD -> convertNestedRecord((GenericRecord) avroValue, (Types.StructType) icebergType);
            case ARRAY -> convertArray(avroValue, avroSchema, icebergType);
            case MAP -> convertMap(avroValue, avroSchema, icebergType);
            case UNION -> handleUnion(avroValue, avroSchema, icebergType);
            case ENUM -> avroValue.toString();
            case FIXED -> convertFixed((GenericFixed) avroValue, icebergType);
            case BYTES -> convertBytes(avroValue);
            case INT -> ((Number) avroValue).intValue();
            case STRING -> avroValue.toString();
            default -> avroValue;
        };
    }

    protected static Object convertLogicalType(Object avroValue, LogicalType logicalType, Type icebergType) {
        if (avroValue == null) {
            return null;
        }

        switch (logicalType.getName()) {
            case "date":
                if (avroValue instanceof LocalDate) {
                    return avroValue;
                }
                return LocalDate.ofEpochDay((Integer) avroValue);
            case "time-millis":
                if (avroValue instanceof LocalTime) {
                    return avroValue;
                }
                // Convert milliseconds to nanoseconds for LocalTime
                return LocalTime.ofNanoOfDay((Integer) avroValue * 1_000_000L);
            case "time-micros":
                return LocalTime.ofNanoOfDay(((Long) avroValue) * 1000L);
            case "timestamp-millis":
                if (avroValue instanceof LocalDateTime) {
                    return avroValue;
                }
                // Convert milliseconds since epoch to appropriate timestamp type
                if (icebergType instanceof Types.TimestampType timestampType) {
                    LocalDateTime localDateTime = LocalDateTime.ofInstant(
                            Instant.ofEpochMilli((Long) avroValue),
                            ZoneOffset.UTC);
                    return timestampType.shouldAdjustToUTC() ? localDateTime.atOffset(ZoneOffset.UTC) : localDateTime;
                }
                return LocalDateTime.ofInstant(
                        Instant.ofEpochMilli((Long) avroValue),
                        ZoneOffset.UTC);
            case "timestamp-micros":
                if (avroValue instanceof Instant instant) {
                    LocalDateTime localDateTime = LocalDateTime.ofInstant(instant, ZoneOffset.UTC);
                    if (icebergType instanceof Types.TimestampType timestampType) {
                        return timestampType.shouldAdjustToUTC()
                            ? localDateTime.atOffset(ZoneOffset.UTC)
                            : localDateTime;
                    }
                    return localDateTime;
                }
                // Convert microseconds since epoch to appropriate timestamp type
                if (icebergType instanceof Types.TimestampType timestampType) {
                    LocalDateTime localDateTime = LocalDateTime.ofInstant(
                            Instant.ofEpochSecond(
                                    ((Long) avroValue) / 1_000_000,
                                    (((Long) avroValue) % 1_000_000) * 1000),
                            ZoneOffset.UTC);
                    return timestampType.shouldAdjustToUTC() ? localDateTime.atOffset(ZoneOffset.UTC) : localDateTime;
                }
                return LocalDateTime.ofInstant(
                        Instant.ofEpochSecond(
                                ((Long) avroValue) / 1_000_000,
                                (((Long) avroValue) % 1_000_000) * 1000),
                        ZoneOffset.UTC);
            case "local-timestamp-millis":
                if (avroValue instanceof LocalDateTime) {
                    return avroValue;
                }
                return LocalDateTime.ofInstant(Instant.ofEpochMilli((Long) avroValue), ZoneOffset.UTC);
            case "local-timestamp-micros":
                if (avroValue instanceof LocalDateTime) {
                    return avroValue;
                }
                long epochSeconds = (Long) avroValue / (1_000_000L);
                long nanoAdjustment = (((Long) avroValue).intValue() % (1_000_000L)) * 1_000L;
                return LocalDateTime.ofInstant(Instant.ofEpochSecond(epochSeconds, nanoAdjustment), ZoneOffset.UTC);
            case "decimal":
                if (avroValue instanceof ByteBuffer buffer) {
                    byte[] bytes = new byte[buffer.remaining()];
                    buffer.get(bytes);
                    DecimalType decimalType = (DecimalType) icebergType;
                    return new BigDecimal(new BigInteger(bytes), decimalType.scale());
                }
                return avroValue;
            case "uuid":
                // Check if the Iceberg type is STRING (Unity Catalog case)
                if (icebergType.typeId() == Type.TypeID.STRING) {
                    // Convert UUID to STRING representation for Unity Catalog
                    if (avroValue instanceof GenericFixed) {
                        byte[] bytes = ((GenericFixed) avroValue).bytes();
                        ByteBuffer buffer = ByteBuffer.wrap(bytes);
                        long mostSigBits = buffer.getLong();
                        long leastSigBits = buffer.getLong();
                        UUID uuid = new UUID(mostSigBits, leastSigBits);
                        return uuid.toString();
                    } else if (avroValue instanceof CharSequence) {
                        // Already a string, ensure it's a valid UUID format
                        return UUID.fromString(avroValue.toString()).toString();
                    }
                    return avroValue.toString();
                }
                // Regular UUID conversion (Iceberg type is UUID)
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
            default:
                return avroValue;
        }
    }

    protected static VariantValue convertToVariantValue(Object avroValue) {
        if (avroValue == null) {
            return Variants.ofNull();
        }

        // --- Primitives ---
        if (avroValue instanceof Boolean b) {
            return Variants.of(b);
        }
        if (avroValue instanceof Integer i) {
            return Variants.of(i);
        }
        if (avroValue instanceof Long l)    {
            return Variants.of(l);
        }
        if (avroValue instanceof Float f)   {
            return Variants.of(f);
        }
        if (avroValue instanceof Double d)  {
            return Variants.of(d);
        }

        // --- Binary Types ---
        if (avroValue instanceof GenericFixed fixed) {
            return Variants.of(ByteBuffer.wrap(fixed.bytes()));
        }
        if (avroValue instanceof byte[] bytes) {
            return Variants.of(ByteBuffer.wrap(bytes));
        }
        if (avroValue instanceof ByteBuffer bb) {
            return Variants.of(bb);
        }

        // --- Complex Types (The Fix) ---

        // 1. Handle GenericRecord (Structs) -> Serialize to JSON String
        if (avroValue instanceof GenericRecord r) {
            try {
                String json = avroRecordToJson(r);
                return Variants.of(json); // Store as JSON String
            } catch (IOException e) {
                throw new RuntimeException("Failed to convert Avro Record to JSON for Variant", e);
            }
        }

        // 2. Handle Maps -> Serialize to JSON String
        if (avroValue instanceof Map<?, ?> m) {
            // Note: For simple maps, toString() might work, but a JSON library
            // (like Jackson) is safer. For now, we fallback to toString if no library is available.
            return Variants.of(m.toString());
        }

        // 3. Handle Arrays/Collections
        // Note: Arrays of primitives might be supported, but arrays of Objects
        // will hit the same error. Recursion here is risky unless elements are strictly primitives.
        // It is safer to treat Arrays of Objects as part of the JSON serialization above.
        if (avroValue instanceof GenericArray<?> || avroValue instanceof Collection<?>) {
            // If you have an array at the root, you might want to serialize it to JSON too.
            // For now, we'll return it as a string representation to avoid the crash.
            return Variants.of(avroValue.toString());
        }

        // --- Strings / Fallback ---
        if (avroValue instanceof CharSequence cs) {
            return Variants.of(cs.toString());
        }

        return Variants.of(avroValue.toString());
    }

    /**
     * Helper to correctly serialize an Avro Record to a valid JSON string.
     */
    private static String avroRecordToJson(GenericRecord record) throws IOException {
        try (ByteArrayOutputStream outputStream = new ByteArrayOutputStream()) {
            DatumWriter<GenericRecord> writer = new SpecificDatumWriter<>(record.getSchema());
            // Use Avro's built-in JSON Encoder
            org.apache.avro.io.JsonEncoder encoder = EncoderFactory.get().jsonEncoder(record.getSchema(), outputStream);
            writer.write(record, encoder);
            encoder.flush();
            return outputStream.toString("UTF-8");
        }
    }

    private static boolean isVariantType(Schema schema) {
        if (schema.getLogicalType() != null) {
            return "variant".equals(schema.getLogicalType().getName());
        }
        String logicalTypeProp = schema.getProp("logicalType");
        return "variant".equals(logicalTypeProp);
    }

    /**
     * Extract VariantMetadata from Avro schema properties.
     */
    protected static VariantMetadata extractVariantMetadataFromSchema(Schema avroSchema) {
        // First, check if this schema directly has variant logical type
        if (isVariantType(avroSchema)) {

            String metadataFieldsJson = avroSchema.getProp("variant-metadata-fields");
            if (metadataFieldsJson != null) {
                String[] fieldNames = parseJsonArray(metadataFieldsJson);
                if (fieldNames.length > 0) {
                    return Variants.metadata(fieldNames);
                }
            }
        }

        // If this is a record schema, check its fields for variant logical type
        if (avroSchema.getType() == Schema.Type.RECORD) {
            for (Schema.Field field : avroSchema.getFields()) {
                Schema fieldSchema = field.schema();

                // Handle union types (nullable fields)
                if (fieldSchema.getType() == Schema.Type.UNION) {
                    for (Schema unionSchema : fieldSchema.getTypes()) {
                        if (unionSchema.getLogicalType() != null
                                && "variant".equals(unionSchema.getLogicalType().getName())) {
                            fieldSchema = unionSchema;
                            break;
                        }
                    }
                }

                if (fieldSchema.getLogicalType() != null
                        && "variant".equals(fieldSchema.getLogicalType().getName())) {

                    String metadataFieldsJson = fieldSchema.getProp("variant-metadata-fields");
                    if (metadataFieldsJson != null) {
                        String[] fieldNames = parseJsonArray(metadataFieldsJson);
                        if (fieldNames.length > 0) {
                            return Variants.metadata(fieldNames);
                        }
                    }
                }
            }
        }

        // Return empty metadata if not found
        return VariantMetadata.empty();
    }

    /**
     * Simple JSON array parser for field names.
     * FIXED: Enhanced with better error handling and support for both single and double quotes.
     */
    private static String[] parseJsonArray(String json) {
        if (json == null || json.isEmpty()) {
            return new String[0];
        }

        try {
            // Remove brackets and whitespace
            String content = json.trim();
            if (content.startsWith("[")) {
                content = content.substring(1);
            }
            if (content.endsWith("]")) {
                content = content.substring(0, content.length() - 1);
            }
            content = content.trim();

            if (content.isEmpty()) {
                return new String[0];
            }

            // Split by comma and clean quotes
            String[] parts = content.split(",");
            List<String> fieldNames = new ArrayList<>();

            for (String part : parts) {
                String cleaned = part.trim();
                // Remove surrounding quotes (both single and double)
                if ((cleaned.startsWith("\"") && cleaned.endsWith("\""))
                        || (cleaned.startsWith("'") && cleaned.endsWith("'"))) {
                    cleaned = cleaned.substring(1, cleaned.length() - 1);
                }

                if (!cleaned.isEmpty()) {
                    fieldNames.add(cleaned);
                }
            }

            return fieldNames.toArray(new String[0]);
        } catch (Exception e) {
            // Log warning and return empty array on parse failure
            if (log.isDebugEnabled()) {
                log.debug("Failed to parse JSON array for variant metadata: {}, error: {}", json, e.getMessage());
            }
            return new String[0];
        }
    }

    private static Record convertNestedRecord(GenericRecord avroRecord, Types.StructType structType) {
        org.apache.iceberg.Schema nestedSchema = new org.apache.iceberg.Schema(structType.fields());
        Record nestedRecord = org.apache.iceberg.data.GenericRecord.create(nestedSchema.asStruct());

        avroRecord.getSchema().getFields().forEach(avroField -> {
            String fieldName = avroField.name();
            Object fieldValue = avroRecord.get(fieldName);
            Type fieldType = structType.fieldType(fieldName);

            // Skip fields that don't exist in the Iceberg schema
            if (fieldType != null) {
                nestedRecord.setField(
                        fieldName,
                        convertValue(fieldValue, avroField.schema(), fieldType)
                );
            } else {
                log.info("Skipping field '{}' - not present in Iceberg schema", fieldName);
            }
        });

        return nestedRecord;
    }

    private static List<?> convertArray(Object avroValue, Schema avroSchema, Type icebergType) {
        Types.ListType listType = (Types.ListType) icebergType;
        return ((Collection<?>) avroValue).stream()
                .map(element -> convertValue(
                        element,
                        avroSchema.getElementType(),
                        listType.elementType()
                ))
                .collect(Collectors.toList());
    }

    private static Map<String, ?> convertMap(Object avroValue, Schema avroSchema, Type icebergType) {
        Types.MapType mapType = (Types.MapType) icebergType;
        Map<String, Object> icebergMap = new HashMap<>();

        ((Map<?, ?>) avroValue).forEach((key, value) -> {
            String stringKey = key.toString();
            Object convertedValue = convertValue(
                    value,
                    avroSchema.getValueType(),
                    mapType.valueType()
            );
            icebergMap.put(stringKey, convertedValue);
        });

        return icebergMap;
    }

    private static Object handleUnion(Object avroValue, Schema avroSchema, Type icebergType) {
        // Iceberg's SchemaToType.union() handles two cases differently:
        //
        // 1. "Option" unions (exactly 2 types, one is NULL, e.g. ["null","string"]):
        //    Iceberg unwraps these to the non-null type (e.g. StringType).
        //    We resolve the value's branch and convert it directly.
        //
        // 2. All other unions (single-element like ["string"], or multi-type like
        //    ["null","string","record"]): Iceberg creates a tagged-union StructType
        //    with a "tag" field and "field0", "field1", ... for each non-null branch.
        //    We must produce a matching GenericRecord.
        //
        // Failing to distinguish these cases causes ClassCastException: the Parquet
        // writer expects StructLike for case 2, but gets a raw value (e.g. String).
        if (isOptionSchema(avroSchema)) {
            // Case 1: standard nullable — resolve the correct branch and convert
            Schema resolvedSchema = avroSchema.getTypes().get(0).getType() == Schema.Type.NULL
                    ? avroSchema.getTypes().get(1) : avroSchema.getTypes().get(0);
            return convertValue(avroValue, resolvedSchema, icebergType);
        }

        // Case 2: non-option union — build a tagged union struct
        return convertTaggedUnion(avroValue, avroSchema, (Types.StructType) icebergType);
    }

    private static boolean isOptionSchema(Schema schema) {
        if (schema.getType() != Schema.Type.UNION || schema.getTypes().size() != 2) {
            return false;
        }
        return schema.getTypes().get(0).getType() == Schema.Type.NULL
                || schema.getTypes().get(1).getType() == Schema.Type.NULL;
    }

    private static Record convertTaggedUnion(Object avroValue, Schema avroSchema, Types.StructType structType) {
        // Determine which branch index the value belongs to (skipping NULL branches)
        List<Schema> types = avroSchema.getTypes();
        int resolvedIndex = -1;
        int nonNullIndex = 0;
        for (int i = 0; i < types.size(); i++) {
            if (types.get(i).getType() == Schema.Type.NULL) {
                continue;
            }
            if (matchesUnionBranch(avroValue, types.get(i))) {
                resolvedIndex = nonNullIndex;
                break;
            }
            nonNullIndex++;
        }
        if (resolvedIndex == -1) {
            // Fallback: use first non-null branch
            resolvedIndex = 0;
        }

        // Build the tagged-union record: { tag: <branchIndex>, field<N>: <convertedValue> }
        org.apache.iceberg.Schema taggedSchema = new org.apache.iceberg.Schema(structType.fields());
        Record taggedRecord = org.apache.iceberg.data.GenericRecord.create(taggedSchema.asStruct());
        taggedRecord.setField("tag", resolvedIndex);

        // Convert and set the value in the appropriate field slot
        nonNullIndex = 0;
        for (int i = 0; i < types.size(); i++) {
            if (types.get(i).getType() == Schema.Type.NULL) {
                continue;
            }
            String fieldName = "field" + nonNullIndex;
            if (nonNullIndex == resolvedIndex) {
                Type fieldType = structType.fieldType(fieldName);
                taggedRecord.setField(fieldName, convertValue(avroValue, types.get(i), fieldType));
            }
            nonNullIndex++;
        }

        return taggedRecord;
    }

    private static boolean matchesUnionBranch(Object value, Schema branchSchema) {
        return switch (branchSchema.getType()) {
            case RECORD -> value instanceof GenericRecord
                    && ((GenericRecord) value).getSchema().getFullName().equals(branchSchema.getFullName());
            case ARRAY -> value instanceof GenericArray || value instanceof Collection;
            case MAP -> value instanceof Map;
            case STRING, ENUM -> value instanceof CharSequence;
            case INT -> value instanceof Integer;
            case LONG -> value instanceof Long;
            case FLOAT -> value instanceof Float;
            case DOUBLE -> value instanceof Double;
            case BOOLEAN -> value instanceof Boolean;
            case BYTES -> value instanceof ByteBuffer || value instanceof byte[];
            case FIXED -> value instanceof GenericFixed;
            default -> false;
        };
    }

    private static ByteBuffer convertBytes(Object avroValue) {
        if (avroValue instanceof ByteBuffer buffer) {
            byte[] bytes = new byte[buffer.remaining()];
            buffer.get(bytes);
            return ByteBuffer.wrap(bytes);
        } else if (avroValue instanceof GenericFixed) {
            return ByteBuffer.wrap(((GenericFixed) avroValue).bytes());
        } else if (avroValue instanceof byte[] bytes) {
            return ByteBuffer.wrap(bytes);
        }
        return ByteBuffer.wrap(avroValue.toString().getBytes(StandardCharsets.UTF_8));
    }

    private static byte[] convertFixed(GenericFixed fixed, Type icebergType) {
        Types.FixedType fixedType = (Types.FixedType) icebergType;
        byte[] bytes = fixed.bytes();
        if (bytes.length != fixedType.length()) {
            throw new IllegalArgumentException(
                    "Fixed length mismatch: Avro=" + bytes.length + ", Iceberg=" + fixedType.length());
        }
        return bytes;
    }
}
