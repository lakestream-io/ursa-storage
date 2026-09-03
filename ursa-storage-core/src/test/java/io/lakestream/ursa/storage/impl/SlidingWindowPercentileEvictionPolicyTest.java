/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.storage.impl;

import static io.lakestream.ursa.storage.impl.StorageConfig.PROTOBUF_VERSION;
import static org.assertj.core.api.Assertions.assertThat;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.RemovalCause;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.PooledByteBufAllocator;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ThreadLocalRandom;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockitoAnnotations;

public class SlidingWindowPercentileEvictionPolicyTest {

    ByteBufAllocator allocator = PooledByteBufAllocator.DEFAULT;
    int windowSize = 10;
    int cacheSize = 20;
    int percentileComputeDelayInMillis = 100;
    int stalePercentileDelayFactor = 10;
    int entryCount = 3;
    LoadingCache<String, CompletableFuture<PersistCache>> cache;
    SlidingWindowPercentileEvictionPolicy target;

    StorageFormat format = new StorageFormat(new StorageConfig());

    private class Processor extends CacheLoader<String, CompletableFuture<PersistCache>>
            implements RemovalListener<String, CompletableFuture<PersistCache>> {

        @Override
        public CompletableFuture<PersistCache> load(String key) throws Exception {
            target.onLoad(key);
            var w = PersistCacheFactory.create(allocator, cacheSize, PROTOBUF_VERSION);
            var payload = allocator.buffer(1);
            for (int i = 0; i < entryCount; i++) {
                var pendingAdd = new PendingAdd(0, 1, payload, new CompletableFuture<>(), null);
                w.put(pendingAdd);
            }
            var serialized = w.serialize("location", format);
            var r = PersistCacheFactory.deserialize(allocator, serialized, PROTOBUF_VERSION);
            serialized.release();
            payload.release();
            w.close();
            return CompletableFuture.completedFuture(r);
        }

        @Override
        public void onRemoval(RemovalNotification<String, CompletableFuture<PersistCache>> notification) {
            CompletableFuture<PersistCache> cache = notification.getValue();
            if (cache != null) {
                cache.thenAccept(c -> {
                    if (RemovalCause.SIZE == notification.getCause()) {
                        target.onRemoval(notification.getKey(), c);
                    }
                    c.close();
                });
            }
        }
    }


    @BeforeEach
    public void setUp() {
        MockitoAnnotations.openMocks(this);
        target = new SlidingWindowPercentileEvictionPolicy(windowSize, percentileComputeDelayInMillis,
                stalePercentileDelayFactor);
        Processor processor = new Processor();
        cache = CacheBuilder.newBuilder()
                .maximumSize(cacheSize)
                .removalListener(processor)
                .build(processor);


    }

    @AfterEach
    public void clean() {
        cache.invalidateAll();
    }

    void fill(int i) throws ExecutionException, InterruptedException {
        String key = "key" + i;
        var c = cache.get(key).join();
        int readCount = ThreadLocalRandom.current().nextInt(5);
        for (int j = 0; j <= readCount; j++) {
            c.get(0, j % entryCount);
            Thread.sleep(1);
        }
    }

    void calculatePercentiles() throws InterruptedException {
        Thread.sleep((long) percentileComputeDelayInMillis * stalePercentileDelayFactor * 2 + 50);
        target.onLoad("cleanup-trigger");
    }

    @Test
    public void test_eviction() throws Exception {

        int size = cacheSize * 10;
        for (int i = 0; i < size; i++) {
            fill(i);
        }

        calculatePercentiles();
        int evicted = target.tryEvict(cache, cacheSize).join();
        assertThat(evicted > 0).isTrue();
    }

    @Test
    public void test_evictionSurvivesConcurrentClose() throws Exception {
        int size = cacheSize * 10;
        for (int i = 0; i < size; i++) {
            fill(i);
        }
        calculatePercentiles();

        // A concurrent Guava eviction closes an entry while the pass is walking the map. Reading its
        // read statistics used to throw EntryCacheClosedException out of doEvict, silently aborting
        // the whole pass -- exactly under the load where eviction is needed.
        var closedSegment = PersistCacheFactory.create(allocator, cacheSize, PROTOBUF_VERSION);
        closedSegment.close();
        cache.asMap().put("closed-key", CompletableFuture.completedFuture(closedSegment));

        int evicted = target.tryEvict(cache, cacheSize).join();
        assertThat(evicted > 0).isTrue();
    }

    @Test
    public void test_evictionSurvivesNegativeReadDuration() throws Exception {
        int size = cacheSize * 10;
        for (int i = 0; i < size; i++) {
            fill(i);
        }
        calculatePercentiles();

        // A segment that has been read, so the pass does not short-circuit on readCount == 0 and
        // actually reaches toInt(c.getReadDurationInMillis()). doClear() writes createdTimestamp (a
        // plain field) before lastReadTimestamp (volatile), so a walker can pair a new
        // createdTimestamp with a stale lastReadTimestamp and compute a negative duration -- which
        // toInt() rejects, aborting the whole pass.
        var torn = cache.get("torn-key").join();
        torn.get(0, 0).release();
        ((EntryCache) torn).setCreatedTimestampForTest(System.currentTimeMillis() + 60_000);

        int evicted = target.tryEvict(cache, cacheSize).join();
        assertThat(evicted > 0).isTrue();
        assertThat(torn.getReadDurationInMillis()).isNotNegative();
    }

    @Test
    public void test_whenReadAgain() throws Exception {
        int size = cacheSize * 3;
        for (int i = 0; i < size; i++) {
            fill(i % (cacheSize + 1));
        }

        int evicted = target.tryEvict(cache, cacheSize).join();
        assertThat(evicted).isZero();
    }

    @Test
    public void test_beforeFull() throws Exception {
        int size = cacheSize;
        for (int i = 0; i < size; i++) {
            fill(i);
        }

        int evicted = target.tryEvict(cache, cacheSize).join();
        assertThat(evicted).isZero();
    }

    @Test
    public void test_stalePercentile() throws Exception {
        int size = cacheSize * 10;
        for (int i = 0; i < size; i++) {
            fill(i);
        }

        calculatePercentiles();
        Thread.sleep((long) percentileComputeDelayInMillis * stalePercentileDelayFactor + 50);
        int evicted = target.tryEvict(cache, cacheSize).join();
        assertThat(evicted).isZero();
    }

}
