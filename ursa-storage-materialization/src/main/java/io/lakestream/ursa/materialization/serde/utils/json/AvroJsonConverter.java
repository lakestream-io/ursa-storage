/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */

package io.lakestream.ursa.materialization.serde.utils.json;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Conversions;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;

@Slf4j
public class AvroJsonConverter {

    public static final AvroJsonConverter INSTANCE = new AvroJsonConverter();

    private final ObjectMapper objectMapper = new ObjectMapper();
    private final Conversions.DecimalConversion decimalConversion = new Conversions.DecimalConversion();

    public void convertToJsonBytes(GenericRecord record, ByteBuf outputBuffer) throws IOException {
        if (record == null) {
            return;
        }
        Map<String, Object> map = toMap(record);
        try (OutputStream out = new ByteBufOutputStream(outputBuffer)) {
            objectMapper.writeValue(out, map);
        }
    }

    public byte[] convertToJsonBytes(GenericRecord record) throws IOException {
        if (record == null) {
            return null;
        }
        Map<String, Object> map = toMap(record);
        return objectMapper.writeValueAsBytes(map);
    }

    public String convertToJsonString(GenericRecord record) throws IOException {
        if (record == null) {
            return null;
        }
        Map<String, Object> map = toMap(record);
        ObjectMapper objectMapper = new ObjectMapper();
        return objectMapper.writeValueAsString(map);
    }

    public Map<String, Object> toMap(GenericRecord record) {
        if (record == null) {
            return null;
        }

        Map<String, Object> map = new LinkedHashMap<>();
        record.getSchema().getFields().forEach(field -> {
            var name = field.name();
            Object value = record.get(name);
            map.put(field.name(), unwrapValue(name, value, field.schema()));
        });
        return map;
    }

    private Object unwrapValue(String name, Object value, Schema schema) {
        if (value == null) {
            return null;
        }

        if (schema.isUnion()) {
            for (Schema type : schema.getTypes()) {
                if (type.getType().equals(Schema.Type.NULL)) {
                    continue;
                }
                return unwrapValue(name, value, type);
            }
        }

        if (schema.getLogicalType() != null || schema.getProp("logicalType") != null) {
            return unwrapLogicalTypeValue(name, value, schema);
        }

        if (value instanceof GenericRecord) {
            return toMap((GenericRecord) value);
        }

        if (value instanceof Collection) {
            return value;
        }

        if (value instanceof Map) {
            Map<Object, Object> result = new LinkedHashMap<>();
            ((Map<?, ?>) value).forEach((k, v)
                -> result.put(k.toString(), unwrapValue(name, v, schema)));
            return result;
        }

        if (value instanceof Utf8) {
            return value.toString();
        }

        if (value instanceof GenericData.EnumSymbol) {
            return value.toString();
        }

        if (value instanceof java.nio.ByteBuffer) {
            return value;
        }

        return value;
    }

    private Object unwrapLogicalTypeValue(String name, Object value, Schema schema) {
        if (value == null) {
            return null;
        }

        // todo: there is an issue that we can not get the logical type from the schema, it may because
        // we didnt' set it in the schema before.
        var logicalType = schema.getLogicalType();
        if (logicalType == null) {
            return value;
        }

        switch (logicalType.getName()) {
            case "date":
                if (value instanceof Integer) {
                    return LocalDate.ofEpochDay((Integer) value).toString();
                }
                break;
            case "time-millis":
            case "time-micros":
                long nanos = (logicalType.getName().equals("time-millis"))
                    ? (Integer) value * 1_000_000L
                    : (Long) value * 1000L;
                return LocalTime.ofNanoOfDay(nanos).format(DateTimeFormatter.ISO_LOCAL_TIME);
            case "timestamp-millis":
            case "timestamp-micros":
                Instant instant;
                if (logicalType.getName().equals("timestamp-millis")) {
                    instant = Instant.ofEpochMilli((Long) value);
                } else {
                    long micros = (Long) value;
                    instant = Instant.ofEpochSecond(micros / 1_000_000, (micros % 1_000_000) * 1000);
                }
                return DateTimeFormatter.ISO_INSTANT.format(instant);
            case "decimal":
                // The value for decimal logical type is typically stored as bytes in Avro
                // It could be java.nio.ByteBuffer or byte[]
                if (value instanceof ByteBuffer byteBuffer) {
                    BigDecimal bigDecimal = decimalConversion.fromBytes(byteBuffer, schema, logicalType);
                    return bigDecimal;
                } else if (value instanceof byte[]) {
                    ByteBuffer buffer = ByteBuffer.wrap((byte[]) value);
                    BigDecimal bigDecimal = decimalConversion.fromBytes(buffer, schema, logicalType);
                    return bigDecimal;
                }
                // If we can't convert it, return as-is
                return value;
            case "uuid":
                return value.toString();
            default:
                // For unknown logical types, try to convert using the logical type's fromBytes/fromCharSequence methods
                // if applicable, otherwise return as-is
                return value.toString();
        }

        return value;
    }

}
