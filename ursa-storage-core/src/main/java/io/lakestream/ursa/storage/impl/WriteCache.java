/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.storage.impl;

import io.lakestream.ursa.metrics.InstrumentProvider;
import io.lakestream.ursa.storage.impl.utils.FIFOCache;
import io.netty.buffer.ByteBufAllocator;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import lombok.extern.slf4j.Slf4j;

/**
 * WriteCache is a final class that manages a cache system for write operations.
 * It provides functionality to store and retrieve PersistCache objects, with options
 * for in-memory caching and a queue-based buffer system.
 */
@Slf4j
public final class WriteCache {

    private final BlockingQueue<PersistCache> cacheBufferQueue;

    private final boolean cacheInMemoryEnabled;
    private final Map<String, PersistCache> cacheInMemory;

    private final WriteCacheMetrics metrics;

    private final ReadWriteLock cacheLock = new ReentrantReadWriteLock();

    /**
     * Constructs a new WriteCache instance.
     *
     * @param allocator The ByteBufAllocator to be used for creating PersistCache objects.
     * @param config The StorageConfig containing configuration parameters.
     */
    WriteCache(ByteBufAllocator allocator, StorageConfig config, InstrumentProvider provider) {
        var bufferSegments = config.getWriteBufferSegment();
        this.cacheInMemoryEnabled = config.isWriteCacheEnabled();
        var fifoCacheCapacity = cacheInMemoryEnabled ? bufferSegments / 2 : 0;
        var writeBufferSize = config.getWriteBufferSize();

        this.metrics = new WriteCacheMetrics(provider, bufferSegments, writeBufferSize);

        this.cacheBufferQueue = new ArrayBlockingQueue<>(bufferSegments);

        // if in-memory caching is enabled, we will make a half of the buffer segments to be in-memory cache
        this.cacheInMemory = cacheInMemoryEnabled
            ? new FIFOCache<>(Math.max(fifoCacheCapacity, 1),
                removal -> {
                    PersistCache cache = removal.getValue();
                    addCacheToBufferQueue(cache);
                    metrics.getCacheSegmentUsedCount().decrement();
                })
            : Collections.emptyMap();

        // initialize all the persist cache buffer
        for (int i = 0; i < bufferSegments; i++) {
            cacheBufferQueue.add(PersistCacheFactory.create(allocator, writeBufferSize,
                    config.getIndexSerializeFormatVersion()));
        }
    }

    /**
     * Retrieves and removes the head of the cache buffer queue.
     *
     * @return The head PersistCache object, or null if the queue is empty.
     */
    PersistCache poll() {
        try {
            return cacheBufferQueue.poll();
        } finally {
            metrics.getBufferSegmentUsedCount().increment();
        }
    }

    /**
     * Retrieves, but does not remove, the head of the cache buffer queue.
     *
     * @return The head PersistCache object, or null if the queue is empty.
     */
    PersistCache peek() {
        return cacheBufferQueue.peek();
    }

    /**
     * Returns a PersistCache object to the cache system.
     * If in-memory caching is enabled and a cache key is provided, it attempts to store the cache in memory.
     * If in-memory caching fails or is not enabled, it clears the cache and adds it to the buffer queue.
     *
     * @param cache The PersistCache object to return to the cache system.
     * @param cacheKey An Optional String representing the key for in-memory caching.
     * @return true if the cache was successfully returned (either stored in memory or added to the queue),
     *         false if it couldn't be added to the buffer queue.
     */
    boolean returnToCache(PersistCache cache, Optional<String> cacheKey) {

        metrics.getBufferUsedSize().set(cache.sizeInBytes());

        try {
            if (cacheInMemoryEnabled && cacheKey.isPresent()) {
                try {
                    cacheLock.writeLock().lock();
                    cacheInMemory.put(cacheKey.get(), cache);
                    metrics.getCacheSegmentUsedCount().increment();
                    return true;
                } catch (Exception e) {
                    log.warn("Failed to put cache into memory, cacheKey: {}", cacheKey.get(), e);
                } finally {
                    cacheLock.writeLock().unlock();
                }
            }
            return addCacheToBufferQueue(cache);
        } finally {
            metrics.getBufferSegmentUsedCount().decrement();
        }
    }

    private boolean addCacheToBufferQueue(PersistCache cache) {
        cache.clear();
        return cacheBufferQueue.add(cache);
    }

    /**
     * Retrieves a PersistCache object from the in-memory cache using the provided key.
     *
     * @param cacheKey The key of the cache to retrieve.
     * @return The PersistCache object associated with the key, or null if not found.
     */
    PersistCache get(String cacheKey) {
        cacheLock.readLock().lock();
        try {
            return cacheInMemory.get(cacheKey);
        } finally {
            cacheLock.readLock().unlock();
        }
    }

    /**
     * Closes all resources associated with this WriteCache instance.
     * This includes closing all PersistCache objects in the in-memory cache and the buffer queue.
     */
    void close() {
        try {
            cacheLock.writeLock().lock();
            for (PersistCache value : cacheInMemory.values()) {
                value.close();
            }
        } catch (Exception e) {
            log.error("Failed to close cache in memory", e);
        } finally {
            cacheLock.writeLock().unlock();
        }
        try {
            for (PersistCache cache : cacheBufferQueue) {
                cache.close();
            }
        } catch (Exception e) {
            log.error("Failed to close cache buffer queue", e);
        }
    }
}
