/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.lakehouse.exception;

public class SchemaNotFoundException extends LakehouseException {
    public SchemaNotFoundException(String msg, Throwable t) {
        super(msg, t);
    }

    public SchemaNotFoundException(String msg) {
        super(msg);
    }
}
