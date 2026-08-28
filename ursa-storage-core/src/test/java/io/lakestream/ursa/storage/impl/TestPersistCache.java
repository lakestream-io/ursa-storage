/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.storage.impl;

import static io.lakestream.ursa.storage.impl.StorageConfig.DEFAULT_INDEX_SERIALIZE_FORMAT_VERSION;
import static io.lakestream.ursa.storage.impl.StorageConfig.STRING_VERSION;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

import io.lakestream.ursa.storage.FileStorage;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import lombok.Cleanup;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

public class TestPersistCache {

    private PersistCache persistCache;
    private ByteBufAllocator allocator;

    @Mock
    private FileStorage mockFileStorage;

    private PendingAdd pendingAdd;

    StorageFormat format = new StorageFormat(StorageConfig.builder().build());
    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
        allocator = ByteBufAllocator.DEFAULT;
        persistCache =  // 1MB cache
                PersistCacheFactory.create(allocator, 1024 * 1024, DEFAULT_INDEX_SERIALIZE_FORMAT_VERSION);

    }

    @AfterEach
    void tearDown() {
        persistCache.close();
    }

    @Test
    void testPutAndGet() {
        long streamId = 1;
        long entryId = 0;
        @Cleanup("release")
        ByteBuf entry = Unpooled.wrappedBuffer("test entry".getBytes());
        pendingAdd = new PendingAdd(streamId, 1, entry, new CompletableFuture<>(), null);
        long result = persistCache.put(pendingAdd);
        assertEquals(0, result);

        ByteBuf retrievedEntry = persistCache.get(streamId, entryId);
        assertNotNull(retrievedEntry);
        assertEquals(entry, retrievedEntry);
    }

    @Test
    void testConcurrentPut() throws InterruptedException, ExecutionException {
        final var pool1 = Executors.newSingleThreadExecutor();
        final var pool2 = Executors.newSingleThreadExecutor();

        when(mockFileStorage.putAsync(any(ByteBuf.class), eq("")))
                .thenReturn(CompletableFuture.completedFuture(null));
        pendingAdd = new PendingAdd(1, 1, Unpooled.wrappedBuffer("data".getBytes()), new CompletableFuture<>(), null);
        pool1.submit(() -> {
            persistCache.put(pendingAdd);
        }).get();

        try {
            pool2.submit(() -> {
                persistCache.put(pendingAdd);
            }).get();
            fail();
        } catch (ExecutionException e) {
            assertInstanceOf(RuntimeException.class, e.getCause());
            assertTrue(e.getMessage().contains("put is called in another thread: "));
        }
        try {
            pool2.submit(() -> {
                persistCache.persist(mockFileStorage, "", format);
            }).get();
            fail();
        } catch (ExecutionException e) {
            assertInstanceOf(RuntimeException.class, e.getCause());
            assertTrue(e.getMessage().contains("persist is called in another thread: "));
        }

        pool1.submit(() -> {
            persistCache.put(pendingAdd);
            persistCache.persist(mockFileStorage, "", format).join();
        }).get();

        pool1.shutdown();
        pool2.shutdown();
    }

    @Test
    void testPutWithFullCache() {
        long streamId = 1;
        @Cleanup("release")
        ByteBuf largeEntry = Unpooled.wrappedBuffer(new byte[2 * 1024 * 1024]); // 2MB entry
        pendingAdd = new PendingAdd(streamId, 1, largeEntry, new CompletableFuture<>(), null);
        long result = persistCache.put(pendingAdd);
        assertEquals(-1, result);
    }

    @Test
    void testPersistWithoutCallbackAsync() {
        long streamId = 1;
        @Cleanup("release")
        ByteBuf entry = Unpooled.wrappedBuffer("test entry".getBytes());
        pendingAdd = new PendingAdd(streamId, 1, entry, new CompletableFuture<>(), null);
        persistCache.put(pendingAdd);

        String location = "test-location";
        when(mockFileStorage.putAsync(any(ByteBuf.class), eq(location)))
                .thenReturn(CompletableFuture.completedFuture(null));

        CompletableFuture<String> future = persistCache.persist(mockFileStorage, location, format);

        assertNotNull(future);
        assertEquals(location, future.join());
    }

    @Test
    void testSerializeAndDeserialize() {
        long streamId = 1;
        @Cleanup("release")
        ByteBuf entry = Unpooled.wrappedBuffer("test entry".getBytes());

        pendingAdd = new PendingAdd(streamId, 1, entry, new CompletableFuture<>(), null);

        persistCache.put(pendingAdd);
        String location = "test-location";
        @Cleanup("release")
        ByteBuf serialized = persistCache.serialize(location, format);
        PersistCache deserialized =
                PersistCacheFactory.deserialize(allocator, serialized, DEFAULT_INDEX_SERIALIZE_FORMAT_VERSION);

        ByteBuf retrievedEntry = deserialized.get(streamId, 0);
        assertNotNull(retrievedEntry);
        assertEquals(entry, retrievedEntry);
    }

    @Test
    void testClear() {
        long streamId = 1;
        long entryId = 0;
        @Cleanup("release")
        ByteBuf entry = Unpooled.wrappedBuffer("test entry".getBytes());

        pendingAdd = new PendingAdd(streamId, 1, entry, new CompletableFuture<>(), null);

        persistCache.put(pendingAdd);

        persistCache.clear();

        // Verify size, count, and isEmpty
        assertEquals(0, persistCache.sizeInBytes());

        // Verify getEntry
        assertNull(persistCache.get(streamId, entryId));

        // Verify forEach
        assertEquals(0, persistCache.entryCount());
    }

    @Test
    public void testReadonlyCache() {
        ByteBufAllocator allocator = ByteBufAllocator.DEFAULT;
        int maxCacheSize = 1024 * 1024; // 1MB
        @Cleanup
        PersistCache cache = PersistCacheFactory.create(allocator, maxCacheSize, DEFAULT_INDEX_SERIALIZE_FORMAT_VERSION);

        // Test that the cache is not readonly by default
        assertFalse(cache.isReadonly());

        // Add an entry to the non-readonly cache
        long streamId = 1L;
        long entryId = 0L;
        @Cleanup("release")
        ByteBuf entry = Unpooled.wrappedBuffer("test entry".getBytes());

        pendingAdd = new PendingAdd(streamId, 1, entry, new CompletableFuture<>(), null);
        assertTrue(cache.put(pendingAdd) != -1);

        String location = "test-location";

        // Serialize the cache
        ByteBuf serialized = cache.serialize(location, format);

        // Deserialize to create a readonly cache
        @Cleanup
        PersistCache readonlyCache = PersistCacheFactory.deserialize(allocator, serialized, DEFAULT_INDEX_SERIALIZE_FORMAT_VERSION);

        // Verify that the deserialized cache is readonly
        assertTrue(readonlyCache.isReadonly());

        // Attempt to add an entry to the readonly cache
        long newStreamId = 2L;
        @Cleanup("release")
        ByteBuf newEntry = Unpooled.wrappedBuffer("new entry".getBytes());
        var newPendingAdd = new PendingAdd(newStreamId, 1, newEntry, new CompletableFuture<>(), null);
        assertTrue(readonlyCache.put(newPendingAdd) == -1);

        // Verify that the entry was not added
        assertNull(readonlyCache.get(newStreamId, entryId));

        // Verify that the original entry is still accessible
        ByteBuf retrievedEntry = readonlyCache.get(streamId, entryId);
        assertNotNull(retrievedEntry);
        assertEquals(entry, retrievedEntry);

        // Test clear() on readonly cache (should have no effect)
        readonlyCache.clear();
        retrievedEntry = readonlyCache.get(streamId, entryId);
        assertNull(retrievedEntry);
        assertTrue(readonlyCache.isEmpty());
    }

    @Test
    void testPartialSegmentPersistence() {
        // Create a cache with 4MB total size
        int maxCacheSize = 4 * 1024 * 1024; // 4MB
        @Cleanup
        PersistCache cache = PersistCacheFactory.create(allocator, maxCacheSize, DEFAULT_INDEX_SERIALIZE_FORMAT_VERSION);

        // Write only 100KB of data
        int entrySize = 1024; // 1KB per entry
        int numEntries = 100; // 100KB total

        for (int i = 0; i < numEntries; i++) {
            @Cleanup("release")
            ByteBuf entry = Unpooled.buffer(entrySize);
            entry.writeBytes(new byte[entrySize]); // Fill with zeros
            var pendingAdd = new PendingAdd(1L, 1, entry, new CompletableFuture<>(), null);
            cache.put(pendingAdd);
        }

        String location = "test-location";
        ByteBuf serialized = cache.serialize(location, format);

        // - Index size (numEntries * 32 bytes for streamId, entryId, offset, length)
        // - Index length (8 bytes)
        // - Actual data (100KB)
        long expectedMaxSize = (numEntries * 32) + 8 + (numEntries * entrySize);
        assertTrue(
                serialized.readableBytes() < maxCacheSize,
                "Serialized size should be much less than maxCacheSize");
        if (DEFAULT_INDEX_SERIALIZE_FORMAT_VERSION == STRING_VERSION) {
            assertEquals(expectedMaxSize, serialized.readableBytes(),
                    "Serialized size should be close to expected sizeInBytes");

        }

        // Verify we can deserialize and read the data back
        @Cleanup
        PersistCache deserialized = PersistCacheFactory.deserialize(allocator, serialized, DEFAULT_INDEX_SERIALIZE_FORMAT_VERSION);

        // Verify we can read all entries back
        for (int i = 0; i < numEntries; i++) {
            ByteBuf entry = deserialized.get(1L, i);
            assertNotNull(entry, "Entry should exist");
            assertEquals(entrySize, entry.readableBytes(), "Entry size should match");
            entry.release();
        }
    }
}
