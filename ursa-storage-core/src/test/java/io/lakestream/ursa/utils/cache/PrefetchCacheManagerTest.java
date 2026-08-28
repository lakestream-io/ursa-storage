/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.utils.cache;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.lakestream.api.EntryHeader;
import io.lakestream.ursa.metrics.InstrumentProvider;
import io.lakestream.ursa.storage.Entry;
import io.lakestream.ursa.storage.impl.StorageConfig;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class PrefetchCacheManagerTest {

    private PrefetchCacheManager cacheManager;
    private ScheduledExecutorService scheduler;

    @BeforeEach
    void setup() throws IOException {
        scheduler = Executors.newScheduledThreadPool(3);

        Properties properties = new Properties();
        properties.put("readCacheMemorySize", String.valueOf(100 * 1024 * 1024)); // 100 MB
        properties.put("cacheEvictionWatermark", "0.8"); // 80%
        properties.put("customExpireTimeMs", "5000"); // 5 milliseconds
        properties.put("defaultExpireTimeMs", "10000"); // 10 milliseconds

        StorageConfig config = StorageConfig.fromProperties(properties);
        cacheManager = new PrefetchCacheManager(scheduler, config, InstrumentProvider.NOOP);
    }

    @AfterEach
    void tearDown() {
        cacheManager.close();
        scheduler.shutdownNow();
    }

    @Test
    void testCacheInitialization() {
        assertEquals(100 * 1024 * 1024L, cacheManager.getMaxSize());
        assertEquals(98 * 1024 * 1024L, cacheManager.getEvictionTriggerThreshold());
        assertEquals(10_000L, cacheManager.getDefaultExpireTimeMs());
        assertEquals(5_000L, cacheManager.getCustomExpireTimeMs());
    }

    @Test
    void testPrefetchCacheAddAndRetrieve() throws ExecutionException, InterruptedException {
        PrefetchCache cache = cacheManager.getOrCreatePrefetchCache("test");
        CompletableFuture<Entry> future = new CompletableFuture<>();
        EntryHeader entryHeader = new EntryHeader(0, 100, 0, 100, 1024);
        ByteBuf payload = Unpooled.buffer(1024);
        Entry entry = new Entry(entryHeader, payload);
        future.complete(entry);
        cache.put(1L, future, 1024);
        assertNotNull(cache.get(1L));
        assertEquals(entry, cache.get(1L).get());
        entry.payload().release();
    }

    @Test
    void testPrefetchCacheEvictionDefaultExpire() throws Exception {
        PrefetchCache cache = cacheManager.getOrCreatePrefetchCache("test");
        CompletableFuture<Entry> future = new CompletableFuture<>();
        Entry mockEntry = mock(Entry.class);
        EntryHeader mockHeader = mock(EntryHeader.class);
        when(mockEntry.header()).thenReturn(mockHeader);

        // Mock entry with size and a mock payload
        when(mockEntry.header().entrySize()).thenReturn(512);
        when(mockEntry.payload()).thenReturn(mock(ByteBuf.class));
        future.complete(mockEntry);

        // Add an entry to the cache
        cache.put(1L, future, 512);
        assertNotNull(cache.get(1L));

        // Wait for the default expiration time
        await().atMost(120, TimeUnit.SECONDS).until(cache::isEmpty);

        // Verify the cache is empty after expiration
        assertTrue(cache.isEmpty());
        verify(mockEntry.payload(), times(1)).release();
    }

    @Test
    void testCustomExpireEviction() throws ExecutionException, InterruptedException {
        PrefetchCache cache = cacheManager.getOrCreatePrefetchCache("test");

        // Add multiple entries with different timestamps
        for (long i = 1; i <= 10; i++) {
            CompletableFuture<Entry> future = new CompletableFuture<>();
            Entry mockEntry = mock(Entry.class);
            EntryHeader mockHeader = mock(EntryHeader.class);
            when(mockEntry.header()).thenReturn(mockHeader);
            when(mockEntry.header().entrySize()).thenReturn(1024); // 1 KB per entry
            when(mockEntry.payload()).thenReturn(mock(ByteBuf.class));
            future.complete(mockEntry);

            Thread.sleep(1_000);
            cache.put(i, future, 1024);
        }

        // Trigger custom expire eviction
        long removed = cache.triggerExpire(5_000L);

        // Ensure some entries were removed
        assertTrue(removed > 0);
    }

    @Test
    void testCacheManagerEvictionPolicy() {
        // Simulate cache reaching the eviction threshold
        PrefetchCache cache = cacheManager.getOrCreatePrefetchCache("test");
        CompletableFuture<Entry> future = new CompletableFuture<>();
        Entry mockEntry = mock(Entry.class);
        EntryHeader mockHeader = mock(EntryHeader.class);
        when(mockEntry.header()).thenReturn(mockHeader);
        when(mockEntry.header().entrySize()).thenReturn(101 * 1024 * 1024); // 101 MB
        when(mockEntry.payload()).thenReturn(mock(ByteBuf.class));
        future.complete(mockEntry);
        cache.put(1L, future, 101 * 1024 * 1024);

        // hasSpaceInCache() should trigger eviction, but not remove the entry
        assertFalse(cacheManager.hasSpaceInCache());

        // Wait for eviction to trigger by the scheduler
        await().atMost(120, TimeUnit.SECONDS)
                .until(() -> cacheManager.getCurrentSize() < 98 * 1024 * 1024L);

        // Verify eviction reduced the size
        assertTrue(cacheManager.hasSpaceInCache());
    }

    @Test
    void testCacheRemoval() {
        PrefetchCache cache = cacheManager.getOrCreatePrefetchCache("test");
        CompletableFuture<Entry> future = new CompletableFuture<>();
        Entry mockEntry = mock(Entry.class);
        EntryHeader mockHeader = mock(EntryHeader.class);
        when(mockEntry.header()).thenReturn(mockHeader);
        when(mockEntry.header().entrySize()).thenReturn(101 * 1024 * 1024); // 101 MB
        when(mockEntry.payload()).thenReturn(mock(ByteBuf.class));
        future.complete(mockEntry);

        // Add and remove the cache
        cache.put(1L, future, 101 * 1024 * 1024);
        cacheManager.removePrefetchCache("test");

        // Ensure the cache is removed
        assertNull(cacheManager.getOrCreatePrefetchCache("test").get(1L));
        assertEquals(0, cacheManager.getCurrentSize());
    }

    @Test
    void testClearCache() throws ExecutionException, InterruptedException {
        PrefetchCache cache = cacheManager.getOrCreatePrefetchCache("test");
        for (long i = 1; i <= 10; i++) {
            CompletableFuture<Entry> future = new CompletableFuture<>();
            Entry mockEntry = mock(Entry.class);
            EntryHeader mockHeader = mock(EntryHeader.class);
            when(mockEntry.header()).thenReturn(mockHeader);
            when(mockEntry.header().entrySize()).thenReturn(1024 * 1024); // 101 MB
            when(mockEntry.payload()).thenReturn(mock(ByteBuf.class));
            future.complete(mockEntry);

            cache.put(i, future, 1024 * 1024);
        }

        assertEquals(10 * 1024 * 1024L, cacheManager.getCurrentSize());

        // Clear the cache
        long cleared = cache.clear();

        // Verify all entries were removed
        assertEquals(10 * 1024 * 1024L, cleared); // 10 KB total
        assertEquals(0, cacheManager.getCurrentSize());
        assertTrue(cache.isEmpty());
    }

    @Test
    void testSchedulerShutdown() {
        PrefetchCache cache = cacheManager.getOrCreatePrefetchCache("test");
        for (long i = 1; i <= 10; i++) {
            CompletableFuture<Entry> future = new CompletableFuture<>();
            Entry mockEntry = mock(Entry.class);
            EntryHeader mockHeader = mock(EntryHeader.class);
            when(mockEntry.header()).thenReturn(mockHeader);
            when(mockEntry.header().entrySize()).thenReturn(1024 * 1024); // 1 MB
            when(mockEntry.payload()).thenReturn(mock(ByteBuf.class));
            future.complete(mockEntry);

            cache.put(i, future, 1024 * 1024);
        }

        assertEquals(10 * 1024 * 1024L, cacheManager.getCurrentSize());
        cacheManager.close();

        assertTrue(cacheManager.getCaches().isEmpty());
        assertEquals(0, cacheManager.getCurrentSize());
    }

    @Test
    void testCacheEvictionAndContinuation() throws Exception {
        // Initialize a cache with a small size limit for testing
        PrefetchCache cache = cacheManager.getOrCreatePrefetchCache("test");

        // Mock an Entry that occupies 1 MB of memory
        Entry mockEntry = mock(Entry.class);
        EntryHeader mockHeader = mock(EntryHeader.class);
        when(mockEntry.header()).thenReturn(mockHeader);
        when(mockEntry.header().entrySize()).thenReturn(10 * 1024 * 1024); // 10 MB
        when(mockEntry.payload()).thenReturn(mock(ByteBuf.class));

        // Attempt to put 12 entries into the cache (will exceed 100 MB limit)
        for (int i = 0; i < 12; i++) {
            CompletableFuture<Entry> future = CompletableFuture.completedFuture(mockEntry);
            boolean hasSpaceInCache = cacheManager.hasSpaceInCache(); // Put returns null if the entry cannot be added
            if (i < 10) {
                assertTrue(hasSpaceInCache);
                cache.put(i, future, 10 * 1024 * 1024);
                assertNotNull(cache.get(i)); // First 10 entries should be present
            } else {
                assertFalse(hasSpaceInCache);
                assertNull(cache.get(i)); // Entries beyond limit should be skipped

                // Verify the cache is full
                assertEquals(10, cache.size());
                assertEquals(100 * 1024 * 1024L, cacheManager.getCurrentSize());
            }
        }

        Thread.sleep(10_000L);
        // Trigger expiration to free up space. It may already triggered by the hasSpaceInCache() call
        // So do not check the removedSize
        long removedSize = cache.triggerExpire(cacheManager.getDefaultExpireTimeMs());
        assertTrue(cache.size() < 10); // Cache should now have fewer entries

        // Add remaining entries after expiration
        for (int i = 10; i < 12; i++) {
            CompletableFuture<Entry> future = CompletableFuture.completedFuture(mockEntry);
            cache.put(i, future, 10 * 1024 * 1024);
            assertNotNull(cache.get(i)); // New entries should be added successfully
        }

        // Ensure cache usage is below the limit
        assertTrue(cacheManager.getCurrentSize() <= cacheManager.getMaxSize());

        // Clean up
        cacheManager.close();
        scheduler.shutdown();
    }

}
