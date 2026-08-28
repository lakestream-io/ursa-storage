/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.utils.lock.exception;

import io.lakestream.ursa.utils.lock.AsyncLock;
import lombok.Getter;

public class LockException extends RuntimeException {
    LockException(String message) {
        super(message);
    }

    LockException(Throwable cause) {
        super("", cause);
    }

    public static LockException wrap(Throwable ex) {
        if (ex instanceof LockException) {
            return (LockException) ex;
        } else {
            return new LockException(ex);
        }
    }

    public static final class LockBusyException extends LockException {
        public LockBusyException() {
            super("lock busy");
        }
    }

    public static final class LockBusyInLocalException extends LockException {
        public LockBusyInLocalException() {
            super("local lock busy");
        }
    }

    public static final class AcquireTimeoutException extends LockException {
        public AcquireTimeoutException() {
            super("lock acquire timeout");
        }
    }

    @Getter
    public static final class IllegalLockStatusException extends LockException {
        private final AsyncLock.LockStatus expect;
        private final AsyncLock.LockStatus actual;

        public IllegalLockStatusException(AsyncLock.LockStatus expect, AsyncLock.LockStatus actual) {
            super("illegal lock status. expect: " + expect.name() + ", actual: " + actual.name());
            this.expect = expect;
            this.actual = actual;
        }
    }

    @Getter
    public static final class UnknownLockStatusException extends LockException {
        private final AsyncLock.LockStatus actual;

        public UnknownLockStatusException(AsyncLock.LockStatus actual) {
            super("unknown lock status: " + actual.name());
            this.actual = actual;
        }
    }
}
