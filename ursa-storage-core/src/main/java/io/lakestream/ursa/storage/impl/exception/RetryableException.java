/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.storage.impl.exception;

public class RetryableException extends Exception {
    public RetryableException(String msg, Throwable t) {
        super(msg, t);
    }

    public RetryableException(String msg) {
        super(msg);
    }

    public RetryableException(Throwable t) {
        super(t);
    }
}
