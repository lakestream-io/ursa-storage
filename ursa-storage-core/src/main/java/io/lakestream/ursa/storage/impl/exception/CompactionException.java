/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.storage.impl.exception;

public class CompactionException extends Exception {

    public CompactionException(String message) {
        super(message);
    }

    public CompactionException(String message, Throwable cause) {
        super(message, cause);
    }

    public static class IndexUpdateFailed extends CompactionException {
        public IndexUpdateFailed(String message) {
            super(message);
        }

        public IndexUpdateFailed(String message, Throwable cause) {
            super(message, cause);
        }
    }
}
