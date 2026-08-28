/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.lakehouse.utils.lock;

import io.lakestream.ursa.utils.lock.AsyncLock;
import io.lakestream.ursa.utils.lock.NotificationReceiver;
import io.lakestream.ursa.utils.lock.OptionAutoRevalidate;
import io.lakestream.ursa.utils.lock.exception.LockException;
import io.oxia.client.api.AsyncOxiaClient;
import io.oxia.client.api.Notification;
import io.oxia.client.util.Backoff;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Semaphore;
import javax.annotation.concurrent.ThreadSafe;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@ThreadSafe
final class ThreadSimpleLock implements AsyncLock, NotificationReceiver {
    private final SharedSimpleLock distributedLock;
    private final Semaphore memorySemaphore;

    @SafeVarargs
    public ThreadSimpleLock(
            AsyncOxiaClient client,
            String key,
            ScheduledExecutorService executorService,
            Backoff backoff,
            OptionAutoRevalidate optionAutoRevalidate,
            Class<? extends Throwable>... retryableExceptions) {
        this.distributedLock =
                new SharedSimpleLock(
                        client, key, executorService, backoff, optionAutoRevalidate, retryableExceptions);
        this.memorySemaphore = new Semaphore(1);
    }

    @Override
    public LockStatus getStatus() {
        return distributedLock.getStatus();
    }

    @Override
    public CompletableFuture<Void> lock() {
        return lock(ForkJoinPool.commonPool());
    }

    @Override
    public CompletableFuture<Void> tryLock() {
        return tryLock(ForkJoinPool.commonPool());
    }

    @Override
    public CompletableFuture<Void> unlock() {
        return unlock(ForkJoinPool.commonPool());
    }

    @Override
    public CompletableFuture<Void> lock(ExecutorService executorService) {
        try {
            memorySemaphore.acquire();
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            return CompletableFuture.failedFuture(ex);
        }
        return distributedLock
                .lock(executorService)
                .whenComplete(
                        (r, err) -> {
                            if (err != null) {
                                // lock distributed lock failed
                                // rollback the memory lock
                                memorySemaphore.release();
                            }
                        });
    }

    @Override
    public CompletableFuture<Void> tryLock(ExecutorService executorService) {
        if (!memorySemaphore.tryAcquire()) {
            return CompletableFuture.failedFuture(new LockException.LockBusyInLocalException());
        }
        return distributedLock
                .tryLock(executorService)
                .whenComplete(
                        (r, err) -> {
                            if (err != null) {
                                // distributed lock failed, rollback the memory lock
                                memorySemaphore.release();
                            }
                        });
    }

    @Override
    public CompletableFuture<Void> unlock(ExecutorService executorService) {
        return distributedLock
                .unlock()
                .whenComplete(
                        (r, err) -> {
                            if (err == null) {
                                // unlock memory lock only when distributed lock unlocked
                                memorySemaphore.release();
                            }
                        });
    }

    @Override
    public void notifyStateChanged(Notification notification) {
        distributedLock.notifyStateChanged(notification);
    }

    @Override
    public void close() {
        distributedLock.close();
    }
}
