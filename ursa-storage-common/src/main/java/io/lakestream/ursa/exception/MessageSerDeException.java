/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.exception;

public class MessageSerDeException extends ExceptionWithCode {

    public MessageSerDeException(ExceptionCode exceptionCode, String message) {
        super(exceptionCode, message);
    }

    public MessageSerDeException(ExceptionCode exceptionCode, String message, Throwable cause) {
        super(exceptionCode, message, cause);
    }

    public MessageSerDeException(ExceptionCode exceptionCode, Throwable cause) {
        super(exceptionCode, cause.getMessage(), cause);
    }

}
