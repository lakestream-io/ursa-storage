/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.utils.lock;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;

public interface AsyncLock {

    /** Represents the different status of a lock. */
    enum LockStatus {
        INIT,
        ACQUIRING,
        ACQUIRED,
        RELEASING,
        RELEASED
    }

    LockStatus getStatus();

    CompletableFuture<Void> lock();

    CompletableFuture<Void> tryLock();

    CompletableFuture<Void> unlock();

    CompletableFuture<Void> lock(ExecutorService executorService);

    CompletableFuture<Void> tryLock(ExecutorService executorService);

    CompletableFuture<Void> unlock(ExecutorService executorService);

    void close();
}
