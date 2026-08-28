/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.lakehouse.exception;

public class IcebergTableCorruptedException extends LakehouseException {
    public IcebergTableCorruptedException(String msg, Throwable t) {
        super(msg, t);
    }

    public IcebergTableCorruptedException(String msg) {
        super(msg);
    }
}