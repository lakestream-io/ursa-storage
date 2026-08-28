/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.clickhouse;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import org.apache.avro.Schema;

/**
 * Translates AVRO {@link Schema} records into typed
 * {@link ClickHouseSchema} descriptors.
 *
 * <p>Type mapping rules:
 * <ul>
 *   <li>{@link Schema.Type#INT INT} &rarr; {@code Int32}</li>
 *   <li>{@link Schema.Type#LONG LONG} &rarr; {@code Int64}</li>
 *   <li>{@link Schema.Type#FLOAT FLOAT} &rarr; {@code Float32}</li>
 *   <li>{@link Schema.Type#DOUBLE DOUBLE} &rarr; {@code Float64}</li>
 *   <li>{@link Schema.Type#BOOLEAN BOOLEAN} &rarr; {@code UInt8}
 *       (ClickHouse stores booleans as 0/1).</li>
 *   <li>{@link Schema.Type#STRING STRING} &rarr; {@code String}</li>
 *   <li>{@link Schema.Type#BYTES BYTES} &rarr; {@code String} — bytes-as-string
 *       simplification for v1. Production deployments that need binary
 *       fidelity should map this to {@code FixedString(n)} or base64-encode
 *       on the producer side. T13's e2e test can refine if needed.</li>
 *   <li>{@link Schema.Type#RECORD RECORD} (nested) &rarr; flattened with
 *       dotted names (e.g. {@code address.city}).</li>
 *   <li>{@link Schema.Type#ARRAY ARRAY&lt;T&gt;} &rarr; {@code Array(T)}.</li>
 *   <li>{@link Schema.Type#UNION UNION[null, T]} &rarr; {@code Nullable(T)}.</li>
 *   <li>{@link Schema.Type#MAP MAP&lt;V&gt;} &rarr; {@code Map(String, V)}.</li>
 *   <li>{@link Schema.Type#FIXED FIXED}, {@link Schema.Type#ENUM ENUM},
 *       complex unions and {@link Schema.Type#NULL NULL} as a top-level type
 *       throw {@link IllegalArgumentException}.</li>
 * </ul>
 *
 * <p>The helper is stateless and side-effect free; all methods are static.
 */
public final class AvroToClickHouseSchema {

    private AvroToClickHouseSchema() {
    }

    /**
     * Translates a top-level AVRO {@link Schema.Type#RECORD record} schema into
     * a {@link ClickHouseSchema} carrying the supplied primary key and engine.
     *
     * <p>Each record field maps to one {@link ClickHouseColumn}. Nested records
     * are flattened with dotted column names so {@code address.city} becomes a
     * single ClickHouse column.
     *
     * @param avroSchema AVRO record schema (required)
     * @param primaryKey ordered list of column names that form the table's
     *                   {@code ORDER BY} / {@code PRIMARY KEY} clause; may be
     *                   empty
     * @param engine     destination table engine
     * @return a fully populated {@link ClickHouseSchema}
     * @throws IllegalArgumentException if {@code avroSchema} is not a record
     *                                  or contains an unsupported type
     */
    public static ClickHouseSchema convert(Schema avroSchema,
                                           List<String> primaryKey,
                                           ClickHouseTableEngine engine) {
        Objects.requireNonNull(avroSchema, "avroSchema");
        Objects.requireNonNull(primaryKey, "primaryKey");
        Objects.requireNonNull(engine, "engine");
        if (avroSchema.getType() != Schema.Type.RECORD) {
            throw new IllegalArgumentException(
                    "Top-level AVRO schema must be RECORD; got " + avroSchema.getType());
        }
        List<ClickHouseColumn> columns = new ArrayList<>();
        flattenFields(avroSchema, "", columns);
        return new ClickHouseSchema(columns, primaryKey, engine);
    }

    /**
     * Translates a single AVRO {@link Schema} to a ClickHouse type string.
     *
     * <p>Visible for tests and reuse by callers that already track field names
     * separately (e.g. for {@code ALTER TABLE ADD COLUMN}).
     *
     * @param avroSchema field schema (required)
     * @return the ClickHouse type string
     * @throws IllegalArgumentException for FIXED, ENUM, complex unions and any
     *                                  other unsupported type
     */
    public static String avroToClickHouseType(Schema avroSchema) {
        Objects.requireNonNull(avroSchema, "avroSchema");
        return switch (avroSchema.getType()) {
            case INT -> "Int32";
            case LONG -> "Int64";
            case FLOAT -> "Float32";
            case DOUBLE -> "Float64";
            case BOOLEAN -> "UInt8";
            case STRING -> "String";
            case BYTES -> "String";
            case ARRAY -> "Array(" + avroToClickHouseType(avroSchema.getElementType()) + ")";
            case MAP -> "Map(String, " + avroToClickHouseType(avroSchema.getValueType()) + ")";
            case UNION -> unionType(avroSchema);
            case RECORD, FIXED, ENUM, NULL -> throw new IllegalArgumentException(
                    "Unsupported AVRO type for direct ClickHouse mapping: " + avroSchema.getType()
                            + " (record types should be flattened via convert())");
        };
    }

    /**
     * Returns {@code true} when an AVRO field is wrapped in a
     * {@code UNION[null, T]} (or {@code UNION[T, null]}); used by the
     * flattening pass to decide whether a column should be wrapped in
     * {@code Nullable(...)}.
     */
    static boolean isNullable(Schema avroSchema) {
        if (avroSchema.getType() != Schema.Type.UNION) {
            return false;
        }
        for (Schema branch : avroSchema.getTypes()) {
            if (branch.getType() == Schema.Type.NULL) {
                return true;
            }
        }
        return false;
    }

    private static void flattenFields(Schema record, String prefix, List<ClickHouseColumn> out) {
        for (Schema.Field field : record.getFields()) {
            String name = prefix.isEmpty() ? field.name() : prefix + "." + field.name();
            Schema fieldSchema = field.schema();
            Schema effective = unwrapNullable(fieldSchema);
            if (effective.getType() == Schema.Type.RECORD) {
                // Nested record: flatten recursively. The nested level cannot be NULL on its own;
                // null-handling for nested fields lives on individual leaf columns.
                flattenFields(effective, name, out);
            } else {
                String type = avroToClickHouseType(effective);
                boolean nullable = isNullable(fieldSchema);
                String columnType = nullable ? "Nullable(" + type + ")" : type;
                out.add(new ClickHouseColumn(name, columnType, nullable));
            }
        }
    }

    /**
     * For UNIONs that contain {@code null} plus exactly one other branch,
     * returns the non-null branch. Otherwise (no null, or more than two
     * branches) returns the input schema unchanged — the caller's type
     * handler will then either accept it (the {@link #unionType(Schema)}
     * path) or reject it with a clear error.
     */
    private static Schema unwrapNullable(Schema avroSchema) {
        if (avroSchema.getType() != Schema.Type.UNION) {
            return avroSchema;
        }
        List<Schema> branches = avroSchema.getTypes();
        if (branches.size() != 2) {
            return avroSchema;
        }
        Schema first = branches.get(0);
        Schema second = branches.get(1);
        if (first.getType() == Schema.Type.NULL) {
            return second;
        }
        if (second.getType() == Schema.Type.NULL) {
            return first;
        }
        return avroSchema;
    }

    /**
     * Maps a UNION schema to a ClickHouse type string. Only the
     * {@code [null, T]} (or {@code [T, null]}) shape is supported and returns
     * {@code Nullable(T)}. Any other multi-branch union throws
     * {@link IllegalArgumentException}.
     */
    private static String unionType(Schema avroSchema) {
        List<Schema> branches = avroSchema.getTypes();
        if (branches.size() != 2) {
            throw new IllegalArgumentException(
                    "Unsupported AVRO union with " + branches.size() + " branches; "
                            + "ClickHouse only accepts UNION[null, T]");
        }
        Schema first = branches.get(0);
        Schema second = branches.get(1);
        Schema nonNull;
        if (first.getType() == Schema.Type.NULL) {
            nonNull = second;
        } else if (second.getType() == Schema.Type.NULL) {
            nonNull = first;
        } else {
            throw new IllegalArgumentException(
                    "Unsupported AVRO union " + avroSchema + "; ClickHouse only accepts "
                            + "UNION[null, T]");
        }
        if (nonNull.getType() == Schema.Type.NULL) {
            throw new IllegalArgumentException(
                    "AVRO union [null, null] is not a meaningful ClickHouse type");
        }
        return "Nullable(" + avroToClickHouseType(nonNull) + ")";
    }
}
