/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.materialization.exception;

// Fatal exception is used to indicate that the system is in an unrecoverable state.
public class FatalException extends RuntimeException{
    public FatalException(String msg, Throwable t) {
        super(msg, t);
    }

    public FatalException(Throwable t) {
        super(t);
    }

    public FatalException(String msg) {
        super(msg);
    }
}
