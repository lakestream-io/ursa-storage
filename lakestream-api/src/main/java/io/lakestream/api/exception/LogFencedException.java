/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.api.exception;

/**
 * Signals that a log can no longer accept mutations because its writer was fenced.
 */
public class LogFencedException extends RuntimeException {

    public LogFencedException(String message) {
        super(message);
    }

    public LogFencedException(String message, Throwable cause) {
        super(message, cause);
    }
}
