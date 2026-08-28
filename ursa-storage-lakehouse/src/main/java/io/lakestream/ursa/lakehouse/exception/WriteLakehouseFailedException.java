/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.lakehouse.exception;

public class WriteLakehouseFailedException extends LakehouseException {
    public WriteLakehouseFailedException(String msg, Throwable t) {
        super(msg, t);
    }

    public WriteLakehouseFailedException(String msg) {
        super(msg);
    }
}
