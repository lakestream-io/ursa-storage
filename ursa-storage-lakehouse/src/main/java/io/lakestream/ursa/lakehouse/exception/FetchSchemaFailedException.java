/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.lakehouse.exception;


public class FetchSchemaFailedException extends LakehouseException {

    public FetchSchemaFailedException(String msg, Throwable t) {
        super(msg, t);
    }

    public FetchSchemaFailedException(String msg) {
        super(msg);
    }
}
