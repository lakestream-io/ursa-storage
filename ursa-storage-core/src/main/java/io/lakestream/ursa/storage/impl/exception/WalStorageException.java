/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.storage.impl.exception;

public class WalStorageException extends Exception {

    public WalStorageException() {
        super();
    }

    public WalStorageException(String msg) {
        super(msg);
    }

    public WalStorageException(String msg, Throwable e) {
        super(msg, e);
    }

    public static final WalStorageException NOT_INITIALIZED_EXCEPTION = new NotInitializedException();

    private static class NotInitializedException extends WalStorageException {
        public NotInitializedException() {
            super("WalStorage is not initialized");
        }
    }

    public static final WalStorageException SHUTDOWN_EXCEPTION = new ShutdownException();

    private static class ShutdownException extends WalStorageException {
        public ShutdownException() {
            super("WalStorage is shutdown");
        }
    }
}
