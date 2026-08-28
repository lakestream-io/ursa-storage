/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.lakehouse.iceberg.exception;

public class SchemaCompatibilityException extends Exception {
    public SchemaCompatibilityException(String message) {
        super(message);
    }

    public SchemaCompatibilityException(String message, Throwable cause) {
        super(message, cause);
    }
}
