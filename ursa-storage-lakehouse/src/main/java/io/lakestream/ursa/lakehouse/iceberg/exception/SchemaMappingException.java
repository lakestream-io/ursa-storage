/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.lakehouse.iceberg.exception;

public class SchemaMappingException extends Exception {
    public SchemaMappingException(String message) {
        super(message);
    }

    public SchemaMappingException(String message, Throwable cause) {
        super(message, cause);
    }
}
