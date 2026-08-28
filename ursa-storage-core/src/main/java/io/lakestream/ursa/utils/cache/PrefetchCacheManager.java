/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.utils.cache;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import io.lakestream.ursa.metrics.InstrumentProvider;
import io.lakestream.ursa.storage.impl.StorageConfig;
import io.netty.util.internal.PlatformDependent;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;


@Slf4j
public class PrefetchCacheManager {

    static final long JVM_MAX_DIRECT_MEMORY = PlatformDependent.estimateMaxDirectMemory();

    @Getter
    private long maxSize = Math.round(JVM_MAX_DIRECT_MEMORY * 0.15);

    @Getter
    private final long evictionTriggerThreshold;
    private double cacheEvictionWatermark = 0.9;

    private static final double evictionTriggerThresholdPercent = 0.98;
    protected static final double MB = 1024 * 1024;
    @Getter
    private int maxInflightReadingTasks = 20 * Runtime.getRuntime().availableProcessors();

    @Getter
    private final ScheduledExecutorService scheduler;
    private final InstrumentProvider instrumentProvider;
    @Getter
    private long customExpireTimeMs = 30_000;
    @Getter
    private long defaultExpireTimeMs = 120_000;
    private AtomicLong lastEvictionTime = new AtomicLong(0);

    private final AtomicLong currentSize = new AtomicLong(0);
    @Getter
    private final Semaphore semaphore;
    @VisibleForTesting
    @Getter
    private final ConcurrentMap<String, PrefetchCache> caches = new ConcurrentHashMap<>();
    private final AtomicBoolean evictionInProgress = new AtomicBoolean(false);

    // only used for testing
    @VisibleForTesting
    public PrefetchCacheManager() {
        this(Executors.newScheduledThreadPool(1), new StorageConfig(), InstrumentProvider.NOOP);
    }

    public PrefetchCacheManager(ScheduledExecutorService scheduler,
                                StorageConfig config,
                                InstrumentProvider instrumentProvider) {
        this.scheduler = scheduler;
        this.instrumentProvider = instrumentProvider;

        if (config != null) {
            this.maxSize = config.getReadCacheMemorySize();
            this.customExpireTimeMs = config.getCustomExpireTimeMs();
            this.defaultExpireTimeMs = config.getDefaultExpireTimeMs();
            this.cacheEvictionWatermark = config.getCacheEvictionWatermark();
            this.maxInflightReadingTasks = config.getMaxInflightReadingTasks();
        }
        this.evictionTriggerThreshold = (long) (maxSize * evictionTriggerThresholdPercent);
        this.semaphore = new Semaphore(maxInflightReadingTasks);

        log.info("Initialized prefetch cache of {} MB, evictionTriggerThreshold: {} MB, "
                        + "cacheEvictionWaterMark: {}, customExpireTime: {} ms, defaultExpireTime: {} ms",
                maxSize / MB, evictionTriggerThreshold / MB,
                cacheEvictionWatermark, customExpireTimeMs, defaultExpireTimeMs);
    }


    public boolean hasSpaceInCache() {
        long currentSize = this.currentSize.get();
        // Trigger a single eviction in background. While the eviction is running we stop inserting entries in the cache
        if (currentSize > evictionTriggerThreshold
                && System.currentTimeMillis() - lastEvictionTime.get() > 30_000
                && evictionInProgress.compareAndSet(false, true)) {
            lastEvictionTime.set(System.currentTimeMillis());
            scheduler.execute(() -> {
                // Trigger a new cache eviction cycle to bring the used memory below the cacheEvictionWatermark
                // percentage limit
                long sizeToEvict = currentSize - (long) (maxSize * cacheEvictionWatermark);
                long startTime = System.nanoTime();
                log.info("Triggering cache eviction. total size: {} MB -- Need to discard: {} MB", currentSize / MB,
                        sizeToEvict / MB);

                try {
                    //evictionPolicy.doEviction(Lists.newArrayList(caches.values()), sizeToEvict);
                    doEviction(Lists.newArrayList(caches.values()), sizeToEvict);

                    long endTime = System.nanoTime();
                    double durationMs = TimeUnit.NANOSECONDS.toMicros(endTime - startTime) / 1000.0;

                    log.info("Eviction completed. Removed {} MB in {} ms", (currentSize - this.currentSize.get()) / MB,
                            durationMs);
                } finally {
                    evictionInProgress.set(false);
                }
            });
        }

        return currentSize < maxSize;
    }

    private void doEviction(List<PrefetchCache> prefetchCacheList, long sizeToEvict) {
        // Trigger expire immediately based on default expire time
        for (PrefetchCache prefetchCache : prefetchCacheList) {
            sizeToEvict -= prefetchCache.triggerExpire(defaultExpireTimeMs);
            if (sizeToEvict <= 0) {
                return;
            }
        }

        // Trigger expire immediately based on custom expire time
        for (PrefetchCache prefetchCache : prefetchCacheList) {
            sizeToEvict -= prefetchCache.triggerExpire(customExpireTimeMs);
            if (sizeToEvict <= 0) {
                return;
            }
        }
    }


    public PrefetchCache getOrCreatePrefetchCache(String name) {
        return caches.computeIfAbsent(name, n -> new PrefetchCache(this, name));
    }

    public void removePrefetchCache(String name) {
        PrefetchCache prefetchCache = caches.remove(name);
        if (prefetchCache == null) {
            return;
        }

        long size = prefetchCache.close();
        if (log.isDebugEnabled()) {
            log.debug("Removed cache for {} - Size: {} MB -- Current Size: {} MB",
                    name, size / MB, currentSize.get() / MB);
        }
    }

    public void close() {
        caches.forEach((name, cache) -> cache.close());
        caches.clear();
    }

    public void entryAdd(long size) {
        currentSize.addAndGet(size);
    }

    public void entryRemoved(long size) {
        currentSize.addAndGet(-size);
    }

    public long getCurrentSize() {
        return currentSize.get();
    }

}
