/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.utils.cache;

import com.nimbusds.jose.util.Pair;
import io.lakestream.ursa.storage.Entry;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class PrefetchCache {

    private final Map<Long, Pair<Long, CompletableFuture<Entry>>> prefetchCache;
    private final PrefetchCacheManager manager;
    private final AtomicBoolean evictionInProgress = new AtomicBoolean(false);
    private final AtomicBoolean closed = new AtomicBoolean(false);
    private final ScheduledFuture<?> scheduledFuture;
    private final String name;

    public PrefetchCache(PrefetchCacheManager manager, String name) {
        this.prefetchCache = new ConcurrentHashMap<>();
        this.manager = manager;
        this.name = name;

        this.scheduledFuture = manager.getScheduler()
                .scheduleWithFixedDelay(this::triggerExpire, 30, 30, TimeUnit.SECONDS);
    }

    public long triggerExpire() {
        return triggerExpire(manager.getDefaultExpireTimeMs());
    }

    public long triggerExpire(long expireTime) {
        AtomicLong removed = new AtomicLong();
        if (evictionInProgress.compareAndSet(false, true)) {
            try {
                if (prefetchCache.isEmpty()) {
                    return 0;
                }

                for (Map.Entry<Long, Pair<Long, CompletableFuture<Entry>>> mapEntry
                        : prefetchCache.entrySet()) {
                    Long key = mapEntry.getKey();
                    Pair<Long, CompletableFuture<Entry>> pair = mapEntry.getValue();
                    if (System.currentTimeMillis() - pair.getLeft() >= expireTime) {
                        if (!remove(key, pair)) {
                            continue;
                        }
                        CompletableFuture<Entry> removedEntryFuture = pair.getRight();
                        trackRemoval(removedEntryFuture);

                        removed.addAndGet(completedEntrySize(removedEntryFuture));
                        discard(removedEntryFuture);
                    }
                }
            } finally {
                evictionInProgress.set(false);
            }
        }

        if (removed.get() > 0) {
            log.info("Trigger expire completed. name: {}, Removed {} MB, Prefetch Cache Size: {}",
                    name, removed.get() / 1024 / 1024, manager.getCurrentSize());
        }
        return removed.get();
    }

    public CompletableFuture<Entry> put(long key, CompletableFuture<Entry> future, long estimatedSize) {
        Pair<Long, CompletableFuture<Entry>> existing;
        synchronized (prefetchCache) {
            if (closed.get()) {
                discard(future);
                return CompletableFuture.failedFuture(
                    new IllegalStateException("Prefetch cache is closed"));
            }
            Pair<Long, CompletableFuture<Entry>> newEntry = Pair.of(System.currentTimeMillis(), future);
            existing = prefetchCache.putIfAbsent(key, newEntry);
            if (existing == null) {
                manager.entryAdd(estimatedSize);
                future.whenComplete((entry, throwable) -> {
                    if (entry != null) {
                        manager.entryAdd(entry.header().entrySize() - estimatedSize);
                    } else {
                        manager.entryRemoved(estimatedSize);
                    }
                });
                return future;
            }
        }
        if (existing.getRight() != future) {
            discard(future);
        }
        return existing.getRight();
    }

    public boolean exist(long key) {
        return prefetchCache.containsKey(key);
    }

    public CompletableFuture<Entry> get(long key) {
        Pair<Long, CompletableFuture<Entry>> pair = prefetchCache.get(key);
        if (pair == null) {
            return null;
        }
        return pair.getRight();
    }

    public CompletableFuture<Entry> remove(long key) {
        Pair<Long, CompletableFuture<Entry>> pair;
        synchronized (prefetchCache) {
            pair = prefetchCache.remove(key);
        }
        if (pair == null || pair.getRight() == null) {
            return null;
        }
        trackRemoval(pair.getRight());
        return pair.getRight();
    }

    public long clear() {
        List<Pair<Long, CompletableFuture<Entry>>> removedEntries;
        synchronized (prefetchCache) {
            removedEntries = new ArrayList<>(prefetchCache.values());
            prefetchCache.clear();
        }
        long removed = 0;
        for (Pair<Long, CompletableFuture<Entry>> pair : removedEntries) {
            CompletableFuture<Entry> future = pair.getRight();
            if (future == null) {
                continue;
            }
            trackRemoval(future);
            removed += completedEntrySize(future);
            discard(future);
        }
        return removed;
    }

    private boolean remove(Long key, Pair<Long, CompletableFuture<Entry>> expected) {
        synchronized (prefetchCache) {
            return prefetchCache.remove(key, expected);
        }
    }

    private void trackRemoval(CompletableFuture<Entry> future) {
        future.thenAccept(entry -> {
            if (entry != null) {
                manager.entryRemoved(entry.header().entrySize());
            }
        });
    }

    private static long completedEntrySize(CompletableFuture<Entry> future) {
        if (!future.isDone() || future.isCompletedExceptionally() || future.isCancelled()) {
            return 0L;
        }
        Entry entry = future.join();
        return entry == null ? 0L : entry.header().entrySize();
    }

    private static void discard(CompletableFuture<Entry> future) {
        future.thenAccept(PrefetchCache::releaseEntry);
        future.cancel(false);
    }

    private static void releaseEntry(Entry entry) {
        if (entry != null && entry.payload() != null) {
            entry.payload().release();
        }
    }

    public long close() {
        if (!closed.compareAndSet(false, true)) {
            return 0L;
        }
        if (scheduledFuture != null) {
            scheduledFuture.cancel(true);
        }
        return clear();
    }

    public boolean isEmpty() {
        return prefetchCache.isEmpty();
    }

    public long size() {
        return prefetchCache.size();
    }

}
