/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.storage.impl.exception;

public class BackendStorageException extends Exception {

    public BackendStorageException() {
        super();
    }

    public BackendStorageException(String msg) {
        super(msg);
    }

    public BackendStorageException(String msg, Throwable e) {
        super(msg, e);
    }

    public static class NotSupportException extends BackendStorageException {
    }
}
