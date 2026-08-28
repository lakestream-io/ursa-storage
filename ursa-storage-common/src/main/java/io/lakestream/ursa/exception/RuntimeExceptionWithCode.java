/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.exception;

public class RuntimeExceptionWithCode extends RuntimeException {
    private final ExceptionWithCode exceptionWithCode;

    public RuntimeExceptionWithCode(ExceptionWithCode exceptionWithCode) {
        super(exceptionWithCode.getMessage(), exceptionWithCode);
        this.exceptionWithCode = exceptionWithCode;
    }

    public ExceptionWithCode getRealException() {
        return exceptionWithCode;
    }
}

