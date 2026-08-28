/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.lakehouse.exception;

public class PersistTagFailedException extends LakehouseException {

    public PersistTagFailedException(String message) {
        super(message);
    }

    public PersistTagFailedException(String message, Throwable cause) {
        super(message, cause);
    }
}
