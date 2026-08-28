/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.utils.lock;

import java.io.Closeable;

public interface LockManager extends Closeable {

    /**
     * Gets a shared asynchronous lock for the specified key with default backoff options. Note:
     * "Shared" implies that a single lock key is shared among all threads. If different threads
     * attempt to acquire a lock that has already been acquired by another thread, a
     * IllegalLockStatusException from {@link
     * io.lakestream.ursa.lakehouse.utils.lock.exception.LockException.IllegalLockStatusException} will be
     * raised.
     *
     * @param key the key associated with the lock
     * @return an AsyncLock instance for the specified key
     */
    default AsyncLock getSharedLock(String key) {
        return getSharedLock(key, OptionBackoff.DEFAULT);
    }

    /**
     * Gets a shared asynchronous lock for the specified key with custom backoff options. Note:
     * "Shared" implies that a single lock key is shared among all threads. If different threads
     * attempt to acquire a lock that has already been acquired by another thread, a
     * IllegalLockStatusException from
     * {@link io.lakestream.ursa.lakehouse.utils.lock.exception.LockException.IllegalLockStatusException} will be
     * raised.
     *
     * @param key the key associated with the lock
     * @param optionBackoff the backoff options to be used for lock acquisition retries
     * @return an AsyncLock instance for the specified key
     */
    AsyncLock getSharedLock(String key, OptionBackoff optionBackoff);

    /**
     * Gets a thread simple asynchronous lock for the specified key with default backoff options.
     *
     * @param key the key associated with the lock
     * @return an AsyncLock instance for the specified key
     */
    default AsyncLock getThreadSimpleLock(String key) {
        return getThreadSimpleLock(key, OptionBackoff.DEFAULT);
    }

    /**
     * Gets a thread simple asynchronous lock for the specified key with custom backoff options.
     *
     * @param key the key associated with the lock
     * @param optionBackoff the backoff options to be used for lock acquisition retries
     * @return an AsyncLock instance for the specified key
     */
    AsyncLock getThreadSimpleLock(String key, OptionBackoff optionBackoff);

    /**
     * Removes the lock associated with the specified key.
     *
     * @param key the key associated with the lock
     */
    void removeLock(String key);
}
