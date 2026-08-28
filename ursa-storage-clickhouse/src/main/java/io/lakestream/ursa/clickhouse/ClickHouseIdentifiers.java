/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.clickhouse;

import io.lakestream.ursa.exception.ExceptionCode;
import io.lakestream.ursa.materialization.MaterializationException;
import java.util.regex.Pattern;

/**
 * Helpers for safely embedding identifiers and type strings into ClickHouse DDL/DML.
 *
 * <p>Identifiers (namespace, table, column names) can originate from user-defined
 * materialization policies and — for the JSON fallback decoder — from message payload
 * keys. They MUST be escaped before interpolation into SQL, otherwise a backtick in a
 * name breaks out of the quoted identifier and injects arbitrary DDL/DML.
 *
 * <p>Column type strings are produced internally by {@code AvroToClickHouseSchema} from a
 * closed translation set; {@link #validateType(String)} defends against a malformed or
 * tampered persisted schema by rejecting types containing SQL metacharacters rather than
 * emitting them raw.
 */
final class ClickHouseIdentifiers {

    /**
     * ClickHouse column type grammar we emit: letters, digits, underscore, and the
     * punctuation needed for parametric/nested types ({@code Nullable(...)},
     * {@code Decimal(10, 2)}, {@code Array(String)}, {@code DateTime64(3)}). Anything
     * outside this set is treated as untrusted and rejected.
     */
    private static final Pattern SAFE_TYPE = Pattern.compile("[A-Za-z0-9_(),\\s]+");

    private ClickHouseIdentifiers() {
    }

    /**
     * Backtick-quotes an identifier, escaping embedded backticks and backslashes per the
     * ClickHouse rule (both are escaped with a leading backslash inside backtick quotes).
     */
    static String quote(String identifier) {
        if (identifier == null) {
            throw new MaterializationException(ExceptionCode.INTERNAL_ERROR,
                    "ClickHouse identifier must not be null");
        }
        return "`" + identifier.replace("\\", "\\\\").replace("`", "\\`") + "`";
    }

    /**
     * Normalizes a derived table name for ClickHouse. The materialization framework may encode
     * namespace path components into the table name (e.g. {@code public/test_v7.test_v4}); ClickHouse
     * has no path separator, so the namespace path separator {@code '/'} is folded to {@code '.'},
     * yielding a dotted identifier ({@code public.test_v7.test_v4}). The result is always backtick-
     * quoted by {@link #quote(String)} when emitted, so the embedded dots stay part of a single
     * identifier rather than being read as a {@code database.table} boundary.
     *
     * @param name the raw (interpolated) table name
     * @return the name with {@code '/'} replaced by {@code '.'}
     */
    static String sanitizeName(String name) {
        if (name == null || name.isEmpty()) {
            throw new MaterializationException(ExceptionCode.INTERNAL_ERROR,
                    "ClickHouse table name must not be null or empty");
        }
        return name.replace('/', '.');
    }

    /**
     * Validates a column type string against {@link #SAFE_TYPE}, returning it unchanged when
     * safe and throwing otherwise. Type strings are never quoted, so they cannot be escaped;
     * the only safe option for an unexpected value is to reject it.
     */
    static String validateType(String type) {
        if (type == null || !SAFE_TYPE.matcher(type).matches()) {
            throw new MaterializationException(ExceptionCode.MESSAGE_SCHEMA_INCOMPATIBLE,
                    "Refusing to emit unsafe ClickHouse column type: " + type);
        }
        return type;
    }
}
