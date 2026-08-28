/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.utils.cache;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.lakestream.api.EntryHeader;
import io.lakestream.ursa.metrics.InstrumentProvider;
import io.lakestream.ursa.storage.Entry;
import io.lakestream.ursa.storage.OwnedResultFutures;
import io.lakestream.ursa.storage.impl.StorageConfig;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;



public class PrefetchCacheTest {

    private PrefetchCacheManager cacheManager;
    private PrefetchCache cache;
    private ScheduledExecutorService scheduler;

    @BeforeEach
    void setup() throws IOException {
        // Create a mock PrefetchCacheManager with necessary configurations
        scheduler = Executors.newScheduledThreadPool(1);
        Properties properties = new Properties();
        properties.put("readCacheMemorySize", String.valueOf(10 * 1024 * 1024L)); // 10 MB for testing
        properties.put("cacheEvictionWatermark", "0.8"); // Evict at 80%
        properties.put("defaultExpireTimeMs", "120"); // 120 ms (default expiration time)
        properties.put("customExpireTimeMs", "60"); // 60 ms (custom expiration time)
        StorageConfig config = StorageConfig.fromProperties(properties);
        cacheManager = new PrefetchCacheManager(scheduler, config, InstrumentProvider.NOOP);

        cache = cacheManager.getOrCreatePrefetchCache("testCache");
    }

    @AfterEach
    void teardown() {
        cacheManager.close();
        scheduler.shutdown();
    }

    @Test
    void testPutAndGet() {
        Entry mockEntry = createMockEntry(1 * 1024 * 1024); // 1 MB Entry
        CompletableFuture<Entry> future = CompletableFuture.completedFuture(mockEntry);

        cache.put(1L, future, 1 * 1024 * 1024); // 1 MB Entry
        CompletableFuture<Entry> retrievedFuture = cache.get(1L);

        assertNotNull(retrievedFuture);
        assertEquals(future, retrievedFuture);
    }

    @Test
    void testRemove() {
        Entry mockEntry = createMockEntry(1 * 1024 * 1024); // 1 MB Entry
        CompletableFuture<Entry> future = CompletableFuture.completedFuture(mockEntry);

        cache.put(1L, future, 1 * 1024 * 1024); // 1 MB Entry
        CompletableFuture<Entry> removedFuture = cache.remove(1L);

        assertNotNull(removedFuture);
        assertEquals(future, removedFuture);
        assertNull(cache.get(1L));
    }

    @Test
    void testTriggerExpireDefault() {
        Entry mockEntry = createMockEntry(1 * 1024 * 1024); // 1 MB Entry
        CompletableFuture<Entry> future = CompletableFuture.completedFuture(mockEntry);

        cache.put(1L, future, 1 * 1024 * 1024);

        // Simulate time passing for expiration
        sleep(200); // Assuming the default expiration is 120 ms
        long removedSize = cache.triggerExpire();

        assertEquals(1 * 1024 * 1024L, removedSize); // 1 MB removed
        assertNull(cache.get(1L)); // Entry should no longer exist
    }

    @Test
    void testTriggerExpireCustom() {
        Entry mockEntry = createMockEntry(1 * 1024 * 1024); // 1 MB Entry
        CompletableFuture<Entry> future = CompletableFuture.completedFuture(mockEntry);

        cache.put(1L, future, 1 * 1024 * 1024);

        // Simulate time passing for expiration
        sleep(70); // Assuming a custom expiration of 150 ms
        long removedSize = cache.triggerExpire(60);

        assertEquals(1 * 1024 * 1024L, removedSize); // 1 MB removed
        assertNull(cache.get(1L)); // Entry should no longer exist
    }

    @Test
    void testClear() {
        Entry mockEntry1 = createMockEntry(1 * 1024 * 1024); // 1 MB Entry
        Entry mockEntry2 = createMockEntry(2 * 1024 * 1024); // 2 MB Entry
        CompletableFuture<Entry> future1 = CompletableFuture.completedFuture(mockEntry1);
        CompletableFuture<Entry> future2 = CompletableFuture.completedFuture(mockEntry2);

        cache.put(1L, future1, 1 * 1024 * 1024);
        cache.put(2L, future2, 2 * 1024 * 1024);

        long removedSize = cache.clear();

        assertEquals(3 * 1024 * 1024L, removedSize); // 3 MB removed
        assertTrue(cache.isEmpty());
    }

    @Test
    void testSizeAndIsEmpty() {
        assertTrue(cache.isEmpty());
        assertEquals(0, cache.size());

        Entry mockEntry = createMockEntry(1 * 1024 * 1024); // 1 MB Entry
        CompletableFuture<Entry> future = CompletableFuture.completedFuture(mockEntry);

        cache.put(1L, future, 1 * 1024 * 1024);

        assertFalse(cache.isEmpty());
        assertEquals(1, cache.size());
    }

    @Test
    void duplicatePutReturnsExistingAndReleasesTheLosingResult() {
        CompletableFuture<Entry> existing = new CompletableFuture<>();
        assertSame(existing, cache.put(1L, existing, 1));

        CompletableFuture<Entry> losingSource = new CompletableFuture<>();
        CompletableFuture<Entry> losingFuture = OwnedResultFutures.transfer(
            losingSource, entry -> entry.payload().release());

        assertSame(existing, cache.put(1L, losingFuture, 1));
        assertTrue(losingFuture.isCancelled());

        ByteBuf payload = Unpooled.buffer(1).writeByte(1);
        losingSource.complete(new Entry(new EntryHeader(0L, 1, 0L, 1, 1), payload));

        assertEquals(0, payload.refCnt());
    }

    @Test
    void expirationReleasesAResultThatWinsTheCancellationRace() {
        ByteBuf payload = Unpooled.buffer(1).writeByte(1);
        Entry entry = new Entry(new EntryHeader(0L, 1, 0L, 1, 1), payload);
        CompletableFuture<Entry> racingFuture = new CompletableFuture<>() {
            @Override
            public boolean cancel(boolean mayInterruptIfRunning) {
                complete(entry);
                return false;
            }
        };
        cache.put(1L, racingFuture, 1);

        cache.triggerExpire(-1L);

        assertNull(cache.get(1L));
        assertEquals(0, payload.refCnt());
    }

    // Helper method to create a mock Entry
    private Entry createMockEntry(int size) {
        Entry mockEntry = mock(Entry.class);
        EntryHeader mockHeader = mock(EntryHeader.class);
        ByteBuf mockPayload = mock(ByteBuf.class);

        when(mockEntry.header()).thenReturn(mockHeader);
        when(mockHeader.entrySize()).thenReturn(size);
        when(mockEntry.payload()).thenReturn(mockPayload);

        return mockEntry;
    }

    // Helper method to simulate time passing
    private void sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
