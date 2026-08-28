/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.storage.impl.exception;

public class EntryCacheClosedException extends RuntimeException {
    public EntryCacheClosedException(String message) {
        super(message);
    }

    public EntryCacheClosedException(String message, Throwable cause) {
        super(message, cause);
    }
}