/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.storage.impl;

import static io.lakestream.ursa.storage.impl.StorageConfig.PROTOBUF_VERSION;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.RemovalCause;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import io.lakestream.api.FileInfo;
import io.lakestream.ursa.storage.FileStorage;
import io.lakestream.ursa.storage.WalStorageMetrics;
import io.lakestream.ursa.storage.impl.exception.OperationRejectException;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicLong;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

/**
 * ReadCache is responsible for caching read operations to improve performance.
 */
@Slf4j
class ReadCache {

    @Getter
    private final LoadingCache<FileInfo, CompletableFuture<PersistCache>> readCache;
    private final SlidingWindowPercentileEvictionPolicy readCacheEvictionPolicy;
    private final ReadCacheProcessor readCacheProcessor;
    private final StorageConfig storageConfig;
    // the read cache max sizeInBytes is counted as the persist cache,
    // that means how many persist cache can in the read cache
    @Getter
    private final long readCacheMaxSize;
    // the pending read is the number of the read request is waiting for the file storage response.
    // it increases when the request com and decreases when the response come.
    private final WalStorageMetrics metrics;
    private final ByteBufAllocator allocator;
    private final FileStorage fileStorage;
    @Getter
    private final AtomicLong pendingRead = new AtomicLong(0);
    private final boolean limitByRequestNumbers;
    private final AtomicLong readCacheSizeInBytes = new AtomicLong(0);

    ReadCache(StorageConfig storageConfig, ByteBufAllocator allocator, FileStorage fileStorage,
              WalStorageMetrics metrics) {
        this.metrics = metrics;
        this.allocator = allocator;
        this.fileStorage = fileStorage;
        this.limitByRequestNumbers = storageConfig.getIndexSerializeFormatVersion() < PROTOBUF_VERSION;
        this.storageConfig = storageConfig;
        this.readCacheMaxSize = Math.max(1, storageConfig.getReadCacheMemorySize()
            / storageConfig.getWriteBufferSize());
        this.readCacheProcessor = new ReadCacheProcessor();
        CacheBuilder<FileInfo, CompletableFuture<PersistCache>> cacheBuilder = CacheBuilder.newBuilder()
            .removalListener(readCacheProcessor);

        if (limitByRequestNumbers) {
            cacheBuilder.maximumSize(readCacheMaxSize);
        } else {
            cacheBuilder.maximumWeight(storageConfig.getReadCacheMemorySize())
                .weigher((k, v) -> {
                    var result = Math.toIntExact(k.size());
                    return result;
                });
        }
        this.readCache = cacheBuilder.build(readCacheProcessor);
        this.readCacheEvictionPolicy = new SlidingWindowPercentileEvictionPolicy();

        // record the read cache size in memory
        this.metrics.buildReadCacheSizeInBytesGauge((avoid) -> {
            return readCacheSizeInBytes.get();
        });
    }

    /**
     * Attempts to evict entries from the read cache based on the eviction policy.
     */
    void tryEvict() {
        readCacheEvictionPolicy.tryEvict(readCache, readCacheMaxSize);
    }

    /**
     * Loads entries into the read cache.
     *
     * @param streamId The stream ID.
     * @param locations The set of locations to load.
     */
    void load(long streamId, Set<FileInfo> locations) {
        for (var location : locations) {
            if (readCache.size() >= readCacheMaxSize) {
                break;
            }
            try {
                CompletableFuture<PersistCache> res = readCache.get(location);
                res.exceptionally(__ -> {
                    readCache.invalidate(location);
                    return null;
                });
            } catch (ExecutionException e) {
                log.warn("Failed to prefetch entry for id: {}, location: {}", streamId, location, e);
            }
        }
    }


    /**
     * Gets a persistCache from the read cache.
     *
     * @param fileInfo The location of the persistCache.
     * @param readEntryRequestsInLocation The number of read entry requests in the location.
     * @return A CompletableFuture representing the result of the read operation.
     */
    CompletableFuture<PersistCache> get(FileInfo fileInfo, int readEntryRequestsInLocation) {
        var request = readCache.getIfPresent(fileInfo);
        if (request == null) {
            metrics.getGetEntriesCacheMiss()
                .add(readEntryRequestsInLocation, Attributes.of(AttributeKey.stringKey("type"), "read_cache"));
            if (limitByRequestNumbers) {
                var currentRequestNum = pendingRead.get();
                if (currentRequestNum >= readCacheMaxSize) {
                    return CompletableFuture.failedFuture(new OperationRejectException("Read reached max limitation. "
                        + "Try increase the reach cache size (" + currentRequestNum + ")"));
                }
                pendingRead.incrementAndGet();
            }
        } else {
            return request;
        }
        try {
            CompletableFuture<PersistCache> res = readCache.get(fileInfo);
            res.whenComplete((__, e) -> {
                if (e != null) {
                    readCache.invalidate(fileInfo);
                }
                if (limitByRequestNumbers) {
                    pendingRead.decrementAndGet();
                }
            });
            return res;
        } catch (ExecutionException e) {
            if (e.getCause() != null) {
                return CompletableFuture.failedFuture(e.getCause());
            } else {
                return CompletableFuture.failedFuture(e);
            }
        }
    }

    /**
     * Leased variant of {@link #get(FileInfo, int)}.
     *
     * <p>The returned future completes with a segment on which a read lease is already held — the
     * caller owns that lease and must {@link PersistCache#release()} it — or with {@code null} when
     * the segment was retired between the lookup and the lease. {@code null} is an ordinary cache
     * miss that the caller satisfies from storage, not an error: it replaces the
     * {@code EntryCacheClosedException} a reader used to hit when eviction closed a segment it was
     * still holding.
     *
     * <p>No lock is taken or needed here: {@code tryRetain()} is itself the synchronization point
     * against the removal listener's close.
     *
     * @param fileInfo The location of the persistCache.
     * @param readEntryRequestsInLocation The number of read entry requests in the location.
     * @return A future completing with a leased persistCache, or with null on a retirement race.
     */
    CompletableFuture<PersistCache> acquire(FileInfo fileInfo, int readEntryRequestsInLocation) {
        return get(fileInfo, readEntryRequestsInLocation)
            .thenApply(cache -> cache != null && cache.tryRetain() ? cache : null);
    }

    /**
     * Processor for handling read cache operations.
     */
    private class ReadCacheProcessor extends
            CacheLoader<FileInfo, CompletableFuture<PersistCache>>
            implements RemovalListener<FileInfo, CompletableFuture<PersistCache>> {

        @Override
        public CompletableFuture<PersistCache> load(FileInfo file) throws Exception {
            readCacheEvictionPolicy.onLoad(file.toString());
            long start = System.nanoTime();
            CompletableFuture<ByteBuf> promise = fileStorage.getAsync(file.location());
            promise.whenComplete((__, e) -> {
                if (e == null) {
                    metrics.getReadCacheLoadingDuration().recordSuccess(System.nanoTime() - start);
                    metrics.getReadCacheLoadingCount().increment();
                } else {
                    metrics.getReadCacheLoadingDuration().recordFailure(System.nanoTime() - start);
                    log.info("Read cache loaded failed, location: {}", file.location(), e);
                }

            });
            return promise.thenApplyAsync(c -> {
                var pc = PersistCacheFactory.deserialize(allocator, c,
                    storageConfig.getIndexSerializeFormatVersion());
                readCacheSizeInBytes.addAndGet(pc.sizeInBytes());
                return pc;
            });
        }

        @Override
        public void onRemoval(RemovalNotification<FileInfo, CompletableFuture<PersistCache>> notification) {
            CompletableFuture<PersistCache> cache = notification.getValue();
            if (cache != null) {
                cache.thenAccept(c -> {
                    readCacheSizeInBytes.addAndGet(-c.sizeInBytes());
                    if (RemovalCause.SIZE == notification.getCause()) {
                        readCacheEvictionPolicy.onRemoval(notification.getKey().toString(), c);
                    }
                    c.close();
                });
            }
            metrics.getReadCacheEvictionCount().increment();
        }
    }

    /**
     * Closes the read cache and invalidates all entries.
     */
    void close() {
        readCache.invalidateAll();
    }
}
