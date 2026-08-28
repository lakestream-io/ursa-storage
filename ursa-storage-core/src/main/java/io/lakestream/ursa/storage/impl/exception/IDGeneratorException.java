/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.storage.impl.exception;

public class IDGeneratorException extends Exception {

    public IDGeneratorException() {
    }

    public IDGeneratorException(String message, Throwable cause) {
        super(message, cause);
    }

    public static class NotSupportException extends IDGeneratorException {

        public NotSupportException() {
            super();
        }
    }
}
