/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.lakehouse.compact;

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.Function;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ObjectPool<K, V> implements AutoCloseable {

    private final Function<K, V> creator;
    @Getter(value = AccessLevel.PACKAGE)
    private final ConcurrentLinkedQueue<PooledObject<V>> pool = new ConcurrentLinkedQueue<>();
    private final long idleTimeoutMillis;
    private final Consumer<V> closeAction;
    private final AtomicLong lastEvictTime = new AtomicLong(System.currentTimeMillis());
    private final AtomicBoolean closed = new AtomicBoolean(false);

    public ObjectPool(Function<K, V> creator, Consumer<V> closeAction, long idleTimeoutMillis) {
        this.creator = creator;
        this.idleTimeoutMillis = idleTimeoutMillis;
        this.closeAction = closeAction;
    }

    public PooledObject<V> borrow(K k) {
        if (closed.get()) {
            throw new IllegalStateException("ObjectPool is closed");
        }

        PooledObject<V> object;
        while ((object = pool.poll()) != null) {
            if (!object.isClosed()) {
                return object;
            }
        }

        return new PooledObject<>(creator.apply(k), closeAction);
    }

    public void release(PooledObject<V> object, boolean forceClose) {
        if (object == null) {
            return;
        }

        object.updateLastUsed();

        if (forceClose || closed.get() || object.isClosed()) {
            object.close();
        } else {
            pool.offer(object);
        }

        long now = System.currentTimeMillis();
        long lastEvict = lastEvictTime.get();
        if (now - lastEvict > idleTimeoutMillis) {
            if (lastEvictTime.compareAndSet(lastEvict, now)) {
                log.info("Try to evict idle objects from the pool, last evict time: {}, current time: {}",
                    lastEvict, now);
                evictIdle();
            }
        }
    }

    public void evictIdle() {
        long now = System.currentTimeMillis();
        pool.removeIf(pooledObject -> {
            if (now - pooledObject.getLastUsed() > idleTimeoutMillis) {
                pooledObject.close();
                return true;
            }
            return false;
        });
    }

    public void close() {
        if (closed.compareAndSet(false, true)) {
            PooledObject<V> object;
            while ((object = pool.poll()) != null) {
                object.close();
            }
        }
    }

    public static class PooledObject<T> implements AutoCloseable {
        @Getter
        private final T instance;
        private final Consumer<T> closeAction;
        private final AtomicBoolean closed = new AtomicBoolean(false);
        private final AtomicLong lastUsed = new AtomicLong();

        public PooledObject(T instance, Consumer<T> closeAction) {
            this.instance = instance;
            this.closeAction = closeAction;
            this.lastUsed.set(System.currentTimeMillis());
        }

        public boolean isClosed() {
            return closed.get();
        }

        public long getLastUsed() {
            return lastUsed.get();
        }

        public void updateLastUsed() {
            lastUsed.set(System.currentTimeMillis());
        }

        public void close() {
            if (closed.compareAndSet(false, true)) {
                if (instance instanceof AutoCloseable closeableInstance) {
                    try {
                        closeableInstance.close();
                    } catch (Exception e) {
                        log.warn("Failed to close pooled object: {}", e.getMessage());
                    }
                }
                if (closeAction != null) {
                    try {
                        closeAction.accept(instance);
                    } catch (Exception e) {
                        log.warn("Failed to execute close action on pooled object: {}", e.getMessage());
                    }
                }
            }
        }
    }
}
