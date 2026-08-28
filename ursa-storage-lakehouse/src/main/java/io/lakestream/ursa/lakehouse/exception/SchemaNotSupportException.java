/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.lakehouse.exception;

import java.io.IOException;

public class SchemaNotSupportException extends IOException {

    public SchemaNotSupportException(String msg) {
        super(msg);
    }

    public SchemaNotSupportException(String msg, Throwable t) {
        super(msg, t);
    }
}
