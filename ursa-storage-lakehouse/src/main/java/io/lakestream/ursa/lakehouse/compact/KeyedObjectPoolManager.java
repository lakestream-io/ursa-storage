/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.lakehouse.compact;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalListener;
import io.lakestream.ursa.materialization.serde.exception.FatalException;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Function;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class KeyedObjectPoolManager<K, V> implements AutoCloseable {

    private final Cache<K, ObjectPool<K, V>> poolCache;
    private final Function<K, V> creator;
    private final long idleTimeoutSeconds;
    @Setter
    private Consumer<V> closeAction;

    public KeyedObjectPoolManager(Function<K, V> creator, long keyExpireAfterAccessSeconds, long idleTimeoutSeconds) {
        this.creator = creator;
        this.idleTimeoutSeconds = idleTimeoutSeconds;

        this.poolCache = CacheBuilder.newBuilder()
            .expireAfterAccess(keyExpireAfterAccessSeconds, TimeUnit.SECONDS)
            .removalListener((RemovalListener<K, ObjectPool<K, V>>) notification -> {
                ObjectPool<K, V> pool = notification.getValue();
                if (pool != null) {
                    pool.close();
                }
            })
            .build();
    }

    public ObjectPool.PooledObject<V> borrow(K key) {
        try {
            ObjectPool<K, V> pool = poolCache.get(key, () ->
                new ObjectPool<>(creator, closeAction, idleTimeoutSeconds * 1000));
            return pool.borrow(key);
        } catch (Exception e) {
            throw new FatalException("Failed to get a pooled object from the cache with the key " + key.toString(), e);
        }
    }

    public void release(K key, ObjectPool.PooledObject<V> obj, boolean forceClose) {
        ObjectPool<K, V> pool = poolCache.getIfPresent(key);
        if (pool != null) {
            pool.release(obj, forceClose);
        } else {
            obj.close();
        }
    }

    ObjectPool<K, V> getObjectPool(K key) {
        return poolCache.getIfPresent(key);
    }

    public void cleanUp() {
        try {
            poolCache.cleanUp();
        } catch (Throwable t) {
            log.warn("Failed to clean up pooled objects", t);
        }
    }

    public void close() {
        poolCache.invalidateAll();
        cleanUp();
    }
}
