/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.storage.impl;

import com.github.benmanes.caffeine.cache.AsyncLoadingCache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.google.common.annotations.VisibleForTesting;
import io.lakestream.api.EntryHeader;
import io.lakestream.api.EntryIndex;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.experimental.Accessors;
import lombok.extern.slf4j.Slf4j;


@Slf4j
public class EntryIndexCache {

    @Data
    @AllArgsConstructor
    @Accessors(fluent = true)
    public class Key {
        long streamId;
        long offset;
    }

    private final AsyncLoadingCache<Key, EntryIndex> compactedEntryIndexCache;

    private final Map<Long, ConcurrentSkipListMap<Long, EntryIndex>> compactedEntryIndexShadowCache =
            new ConcurrentHashMap<>();


    public void invalidateAll() {
        compactedEntryIndexCache.synchronous().invalidateAll();
        compactedEntryIndexShadowCache.clear();
    }

    public void invalidate(long streamId) {
        var val = compactedEntryIndexShadowCache.remove(streamId);
        if (val != null) {
            var map = compactedEntryIndexCache.synchronous();
            Key key = new Key(streamId, -1);
            for (var offset : val.keySet()) {
                key.offset(offset);
                map.invalidate(key);
            }
        }
    }

    public void invalidate(long streamId, long offset) {
        compactedEntryIndexCache.synchronous().invalidate(new Key(streamId, offset));
        compactedEntryIndexShadowCache.computeIfPresent(streamId, (ignored, indices) -> {
            indices.remove(offset);
            return indices.isEmpty() ? null : indices;
        });
    }


    public CompletableFuture<Integer> getMessageCount(long streamId, long offset) {
        return getEntryHeader(streamId, offset).thenApply(EntryHeader::numberOfMessages);
    }

    public CompletableFuture<EntryHeader> getEntryHeader(long streamId, long offset) {
        return get(streamId, offset)
                .thenApply(index -> index.getEntryHeader(offset));
    }


    public CompletableFuture<EntryHeader> searchEntryHeader(long streamId, long offset) {
        return get(streamId, offset)
                .thenApply(index -> index.searchEntryHeader(offset));
    }

    public CompletableFuture<EntryIndex> get(long streamId, long offset) {
        var key = new Key(streamId, offset);
        return get(key).thenApply(index -> {
            if (index == null) {
                return EntryIndex.NOT_FOUND;
            }
            return index;
        });
    }

    private CompletableFuture<EntryIndex> get(Key key) {
        var indexFirstTry = compactedEntryIndexCache.getIfPresent(key);
        if (indexFirstTry != null) {
            return indexFirstTry;
        }

        var indices = compactedEntryIndexShadowCache.get(key.streamId());
        if (indices != null) {
            var indexEntry = indices.floorEntry(key.offset());
            if (indexEntry != null) {
                var index = indexEntry.getValue();
                var header = index.header();
                if (header.offset() <= key.offset()
                        && key.offset() < header.offset() + header.numberOfMessages()) {
                    return CompletableFuture.completedFuture(index);
                }
            }
        }

        return compactedEntryIndexCache.get(key);
    }

    public void put(long streamId,
                    EntryIndex index) {
        var offset = index.header().offset();
        compactedEntryIndexCache.put(new Key(streamId, offset), CompletableFuture.completedFuture(index));
        compactedEntryIndexShadowCache.computeIfAbsent(streamId, k -> new ConcurrentSkipListMap<>()).put(offset, index);
    }


    public EntryIndexCache(BiFunction<Long, Long, CompletableFuture<EntryIndex>> readEntryIndex,
                           int size, int ttlInSecs) {

        this.compactedEntryIndexCache = Caffeine.newBuilder()
                .maximumSize(size)
                .expireAfterWrite(ttlInSecs, TimeUnit.SECONDS)
                .evictionListener((x, y, cause) -> {
                    var key = (Key) x;
                    compactedEntryIndexShadowCache.compute(key.streamId(), (k, set) -> {
                        if (set != null) {
                            set.remove(key.offset());
                            return set.isEmpty() ? null : set; // Remove the set if empty
                        }
                        return null;
                    });
                })
                .buildAsync((key, executor) ->
                        readEntryIndex.apply(key.streamId, key.offset)
                                .thenApply(index -> {
                                    if (index == null || index == EntryIndex.NOT_FOUND) {
                                        return null;
                                    }
                                    compactedEntryIndexShadowCache
                                            .computeIfAbsent(key.streamId(), k -> new ConcurrentSkipListMap<>())
                                            .put(key.offset(), index);
                                    return index;
                                })
                ); // Async loading

    }

    @VisibleForTesting
    public int size() {
        return compactedEntryIndexCache.asMap().size();
    }
}
