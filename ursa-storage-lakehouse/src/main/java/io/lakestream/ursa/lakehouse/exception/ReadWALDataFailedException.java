/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.lakehouse.exception;

public class ReadWALDataFailedException extends LakehouseException {

    public ReadWALDataFailedException(String message, Throwable cause) {
        super(message, cause);
    }

    public ReadWALDataFailedException(String message) {
        super(message);
    }
}
