/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.storage.impl;


import com.google.common.annotations.VisibleForTesting;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.RemovalCause;
import com.google.common.cache.RemovalNotification;
import io.lakestream.ursa.metrics.SlidingWindowPercentile;
import java.util.ArrayList;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import lombok.extern.slf4j.Slf4j;


@Slf4j
public class SlidingWindowPercentileEvictionPolicy {

    private static final int WINDOW_SIZE = 100;
    private static final int TARGET_PERCENTILE = 99;
    private static final int PERCENTILE_COMPUTE_DELAY_IN_MILLIS = 1000;
    private static final int STALE_PERCENTILE_DELAY_FACTOR = 5;
    private static final float CACHE_LOAD_TO_TRY_EVICTION = 0.95f;
    private static final long SMALL_UPPER_BOUND_PERCENT = 2;
    private static final int MIN_TARGET_IDLE_DURATION_IN_MILLIS  = 1000;
    private static final int EVICTION_DELAY_IN_MILLIS  = 100;

    record Stat(int readCount, int readDuration) {
    }

    private final AtomicBoolean isEvicting = new AtomicBoolean(false);
    private final AtomicBoolean isCleaning = new AtomicBoolean(false);
    private final long removedEntriesCleanupDelayInMillis;
    private final SlidingWindowPercentile readCounts;
    private final SlidingWindowPercentile readDurations;
    private final RemovalListener removalListener = new RemovalListener();
    private final Cache<String, Stat> removed;
    private long lastCleanUpTimestamp;
    private long lastEvictionTimestamp;

    private class RemovalListener
            implements com.google.common.cache.RemovalListener<String, Stat> {
        @Override
        public void onRemoval(RemovalNotification<String, Stat> notification) {
            if (RemovalCause.EXPIRED == notification.getCause()) {
                boolean debug = log.isDebugEnabled();
                var stat = notification.getValue();
                if (stat.readDuration >= 0 && readDurations.record(stat.readDuration) && debug) {
                    log.debug("recorded readDurations:{}", readDurations);
                }

                if (stat.readCount >= 0 && readCounts.record(stat.readCount) && debug) {
                    log.debug("recorded readCounts:{}", readCounts);
                }
            }
        }
    }

    public SlidingWindowPercentileEvictionPolicy() {
        this(WINDOW_SIZE, PERCENTILE_COMPUTE_DELAY_IN_MILLIS, STALE_PERCENTILE_DELAY_FACTOR);
    }

    @VisibleForTesting
    protected SlidingWindowPercentileEvictionPolicy(int windowSize,
                                                    int percentileComputeDelayInMillis,
                                                    int stalePercentileDelayFactor) {

        this.removedEntriesCleanupDelayInMillis =
                (long) percentileComputeDelayInMillis * stalePercentileDelayFactor * 2;
        this.removed = CacheBuilder.newBuilder()
                .expireAfterWrite((long) percentileComputeDelayInMillis * stalePercentileDelayFactor,
                        TimeUnit.MILLISECONDS)
                .removalListener(removalListener)
                .build();

        this.readCounts =
                new SlidingWindowPercentile(windowSize, TARGET_PERCENTILE,
                        percentileComputeDelayInMillis, stalePercentileDelayFactor);
        this.readDurations = new SlidingWindowPercentile(windowSize, TARGET_PERCENTILE,
                percentileComputeDelayInMillis, stalePercentileDelayFactor);

    }

    public void onRemoval(String key, PersistCache cache) {
        int readCount = toInt(cache.getReadCount());
        int readDuration = toInt(cache.getReadDurationInMillis());
        removed.put(key, new Stat(readCount, readDuration));
    }

    public void onLoad(String key) {
        boolean debug = log.isDebugEnabled();
        var stat = removed.getIfPresent(key);
        if (stat != null) {
            // This key was read again before expired.
            if (debug) {
                if (stat.readCount >= 0) {
                    log.debug("cache key:{}, stat:{} was read again. skip recording", key, stat);
                } else {
                    log.debug("cache key:{} was read again during eviction.", key);
                }
            }

            removed.invalidate(key);
        }

        if (!isCleaning.get()
                && System.currentTimeMillis() - lastCleanUpTimestamp >= removedEntriesCleanupDelayInMillis
                && isCleaning.compareAndSet(false, true)) {
            try {
                removed.cleanUp();
                lastCleanUpTimestamp = System.currentTimeMillis();
                if (debug) {
                    log.debug("cleaned removed entries.");
                }
            } finally {
                isCleaning.set(false);
            }
        }
    }

    public <T> CompletableFuture<Integer> tryEvict(
            LoadingCache<T, CompletableFuture<PersistCache>> cache,
            long cacheMaxSize) {
        if (cache.size() >= cacheMaxSize * CACHE_LOAD_TO_TRY_EVICTION
                && !isEvicting.get()
                && System.currentTimeMillis() - lastEvictionTimestamp >= EVICTION_DELAY_IN_MILLIS) {
            return CompletableFuture.supplyAsync(() -> doEvict(cache));
        }
        return CompletableFuture.completedFuture(0);
    }
    private <T> int doEvict(LoadingCache<T, CompletableFuture<PersistCache>> cache) {
        if (isEvicting.compareAndSet(false, true)) {
            boolean debug = log.isDebugEnabled();

            try {
                int targetReadCount = readCounts.percentile();
                int targetReadDuration = readDurations.percentile();

                if (targetReadCount == 0 || targetReadDuration == 0) {
                    if (debug) {
                        log.debug("targetReadCount:{} or targetReadDuration:{} is 0. skip",
                                targetReadCount, targetReadDuration);
                    }
                    return 0;
                }

                int globalIdleDuration =
                        Math.max(MIN_TARGET_IDLE_DURATION_IN_MILLIS, toInt(targetReadDuration * 2L));

                var map = cache.asMap();
                var keysToEvict = new ArrayList<T>();
                var now = System.currentTimeMillis();
                for (var e : map.entrySet()) {
                    var k = e.getKey();
                    var v = e.getValue();
                    if (!v.isDone() || v.isCompletedExceptionally()) {
                        continue;
                    }
                    var c = v.join();

                    int cnt = toInt(c.getReadCount());
                    if (cnt == 0) {
                        continue;
                    }
                    long cntDiffRate = diffRate(cnt, targetReadCount);
                    int readDuration = toInt(c.getReadDurationInMillis());
                    int idleDuration = toInt(now - c.getLastReadTimestamp());
                    int targetIdleDuration = Math.max(readDuration, globalIdleDuration);

                    if (idleDuration < targetIdleDuration && !isSmall(cntDiffRate)) {
                        continue;
                    }
                    if (debug) {
                        log.debug(
                                "evicting key:{}, cnt:{}, targetCnt:{}, cntDiffRate:{}, readDuration:{}, "
                                        + "idleDuration:{}, targetIdleDuration:{}, globalIdleDuration:{}",
                                k, cnt, targetReadCount, cntDiffRate, readDuration, idleDuration,
                                targetIdleDuration, globalIdleDuration);
                    }
                    keysToEvict.add(k);
                }

                for (var k : keysToEvict) {
                    removed.put(k.toString(), new Stat(-1, -1));
                    cache.invalidate(k);
                }
                if (debug) {
                    log.debug("evicted {}/{} keys", keysToEvict.size(), map.size());
                }
                return keysToEvict.size();
            } finally {
                lastEvictionTimestamp = System.currentTimeMillis();
                isEvicting.set(false);
            }
        }

        return 0;
    }

    private static long diffRate(int num, int target) {
        return ((long) num - target) * 100 / target;
    }

    private static boolean isSmall(long num) {
        return num >= 0 && num <= SMALL_UPPER_BOUND_PERCENT;
    }

    private static int toInt(long value) {
        if (value < 0) {
            throw new IllegalArgumentException("Expected non negative value, but value:" + value);
        }
        return (value > Integer.MAX_VALUE) ? Integer.MAX_VALUE : (int) value;
    }
}
