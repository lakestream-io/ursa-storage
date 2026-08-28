/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.exception;

import lombok.Getter;

public class ExceptionWithCode extends Exception {

    @Getter
    private final ExceptionCode exceptionCode;

    public ExceptionWithCode(ExceptionCode exceptionCode, String message) {
        super(message);
        this.exceptionCode = exceptionCode;
    }

    public ExceptionWithCode(ExceptionCode exceptionCode, String message, Throwable cause) {
        super(message, cause);
        this.exceptionCode = exceptionCode;
    }

    ExceptionEntity toExceptionEntity() {
        return new ExceptionEntity(this.exceptionCode, this.getMessage(), this.getCause());
    }

}
