/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.lakehouse.exception;

public class LakehouseException extends Exception {

    public LakehouseException(String message) {
        super(message);
    }

    public LakehouseException(String message, Throwable cause) {
        super(message, cause);
    }
}
