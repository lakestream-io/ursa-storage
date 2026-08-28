/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.lakehouse.exception;

public class CommitLakehouseFailedException extends LakehouseException {
    public CommitLakehouseFailedException(String msg, Throwable t) {
        super(msg, t);
    }

    public CommitLakehouseFailedException(String msg) {
        super(msg);
    }


}
