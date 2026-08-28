/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.exception;

public class CompactionServiceException extends ExceptionWithCode {
    public CompactionServiceException(ExceptionCode exceptionCode, String message) {
        super(exceptionCode, message);
    }

    public CompactionServiceException(ExceptionCode exceptionCode, String message, Throwable cause) {
        super(exceptionCode, message, cause);
    }
}
