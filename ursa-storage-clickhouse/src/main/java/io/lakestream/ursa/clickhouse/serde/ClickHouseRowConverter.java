/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.clickhouse.serde;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;

/**
 * Converts a schema-decoded source record (Avro {@link GenericRecord}, a JSON {@link JsonNode}, a
 * protobuf {@link Message}, or a primitive scalar) into a flat ClickHouse row — a
 * {@code Map<column, value>} where the column order follows the source schema / document field order.
 * Scalar values are mapped to JDBC-friendly Java types; nested or complex values (records, arrays,
 * maps, nested messages) fall back to their string form (the destination column type, owned by
 * {@code ClickHouseTableSchemaService}, decides final coercion).
 */
public final class ClickHouseRowConverter {

    /** Column name for a primitive / single-value source schema (no named fields). */
    public static final String PRIMITIVE_COLUMN = "value";

    private ClickHouseRowConverter() {
    }

    /**
     * Wraps a primitive / single-value payload (Kafka primitive schema, or an Avro-wrapped
     * scalar) in a single {@value #PRIMITIVE_COLUMN}-column row, normalising the value to a
     * JDBC-friendly Java type.
     */
    public static Map<String, Object> fromPrimitive(Object value) {
        Map<String, Object> row = new LinkedHashMap<>();
        row.put(PRIMITIVE_COLUMN, toJavaValue(value));
        return row;
    }

    /** Maps each top-level protobuf field to a row column, preserving the message's field order. */
    public static Map<String, Object> fromProtobuf(Message message) {
        Map<String, Object> row = new LinkedHashMap<>();
        for (Descriptors.FieldDescriptor field : message.getDescriptorForType().getFields()) {
            row.put(field.getName(), protobufToJavaValue(field, message.getField(field)));
        }
        return row;
    }

    /** Maps each top-level Avro field to a row column, preserving the schema field order. */
    public static Map<String, Object> fromAvro(GenericRecord record) {
        Map<String, Object> row = new LinkedHashMap<>();
        for (Schema.Field field : record.getSchema().getFields()) {
            row.put(field.name(), toJavaValue(record.get(field.name())));
        }
        return row;
    }

    /** Maps each top-level JSON field to a row column, preserving the document field order. */
    public static Map<String, Object> fromJson(JsonNode root) {
        Map<String, Object> row = new LinkedHashMap<>();
        Iterator<Map.Entry<String, JsonNode>> fields = root.fields();
        while (fields.hasNext()) {
            Map.Entry<String, JsonNode> field = fields.next();
            row.put(field.getKey(), jsonToJavaValue(field.getValue()));
        }
        return row;
    }

    /** Normalises an Avro field value to a JDBC-friendly Java type. */
    private static Object toJavaValue(Object value) {
        if (value == null) {
            return null;
        }
        if (value instanceof Utf8 || value instanceof CharSequence) {
            return value.toString();
        }
        if (value instanceof ByteBuffer buffer) {
            ByteBuffer dup = buffer.duplicate();
            byte[] bytes = new byte[dup.remaining()];
            dup.get(bytes);
            return bytes;
        }
        if (value instanceof Number || value instanceof Boolean || value instanceof byte[]) {
            return value;
        }
        // Records, arrays, maps, enums, fixed: hand ClickHouse the string form for now.
        return value.toString();
    }

    /** Normalises a protobuf field value to a JDBC-friendly Java type. */
    private static Object protobufToJavaValue(Descriptors.FieldDescriptor field, Object value) {
        if (value == null) {
            return null;
        }
        if (field.isRepeated()) {
            // Repeated (list / map) fields: hand ClickHouse the string form; the column type coerces.
            return value.toString();
        }
        return switch (field.getJavaType()) {
            case INT, LONG, FLOAT, DOUBLE, BOOLEAN -> value;
            case STRING -> value.toString();
            case BYTE_STRING -> ((ByteString) value).toByteArray();
            case ENUM -> ((Descriptors.EnumValueDescriptor) value).getName();
            // Nested message: no flattening — hand ClickHouse the string form.
            case MESSAGE -> value.toString();
        };
    }

    /** Normalises a JSON node to a Java scalar, mirroring the materializer's JSON fallback. */
    private static Object jsonToJavaValue(JsonNode node) {
        if (node == null || node.isNull()) {
            return null;
        }
        if (node.isBoolean()) {
            return node.booleanValue();
        }
        if (node.isInt()) {
            return node.intValue();
        }
        if (node.isLong()) {
            return node.longValue();
        }
        if (node.isFloat() || node.isDouble()) {
            return node.doubleValue();
        }
        if (node.isShort()) {
            return node.shortValue();
        }
        if (node.isBigInteger()) {
            return node.bigIntegerValue();
        }
        if (node.isBigDecimal()) {
            return node.decimalValue();
        }
        if (node.isTextual()) {
            return node.textValue();
        }
        return node.toString();
    }
}
