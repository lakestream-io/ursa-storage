/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.storage.impl;

import static io.lakestream.ursa.storage.impl.StorageConfig.PROTOBUF_VERSION;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.lakestream.api.FileInfo;
import io.lakestream.ursa.storage.FileStorage;
import io.lakestream.ursa.storage.WalStorageMetrics;
import io.lakestream.ursa.storage.impl.exception.EntryCacheClosedException;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

/**
 * Covers the read lease that keeps a {@link ReadCache} segment alive while a reader is still using
 * it. Before segment leases existed, a Guava eviction closed the segment out from under the holder of
 * the completed future and the read failed with {@link EntryCacheClosedException}.
 */
public class ReadCacheLeaseTest {

    private static final ByteBufAllocator ALLOCATOR = ByteBufAllocator.DEFAULT;
    private static final long STREAM_ID = 7L;
    private static final int FILE_WEIGHT = 256;
    private static final StorageFormat FORMAT = new StorageFormat(
        StorageConfig.builder().indexSerializeFormatVersion(PROTOBUF_VERSION).build());

    /** Serves one deterministic single-entry WAL segment per location and counts the reads. */
    private static final class SegmentFileStorage implements FileStorage {

        private final AtomicInteger reads = new AtomicInteger();

        @Override
        public CompletableFuture<ByteBuf> getAsync(String location) {
            reads.incrementAndGet();
            byte[] bytes = payloadFor(location);
            PersistCache writer = PersistCacheFactory.create(ALLOCATOR, bytes.length, PROTOBUF_VERSION);
            ByteBuf payload = Unpooled.wrappedBuffer(bytes);
            try {
                writer.put(new PendingAdd(STREAM_ID, 1, payload, new CompletableFuture<>(), null));
                return CompletableFuture.completedFuture(writer.serialize(location, FORMAT));
            } finally {
                writer.close();
                payload.release();
            }
        }

        @Override
        public CompletableFuture<Void> putAsync(ByteBuf data, String location) {
            return CompletableFuture.failedFuture(new UnsupportedOperationException("read-only stub"));
        }

        @Override
        public void delete(String location) {
        }

        @Override
        public CompletableFuture<Void> deleteAsync(List<String> locations) {
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public void close() {
        }
    }

    private static byte[] payloadFor(String location) {
        return ("payload-of-" + location).getBytes(UTF_8);
    }

    private static ReadCache newReadCache(FileStorage fileStorage) {
        StorageConfig config = new StorageConfig();
        config.setIndexSerializeFormatVersion(PROTOBUF_VERSION);
        config.setReadCacheMemorySize(1024);
        return new ReadCache(config, ALLOCATOR, fileStorage, WalStorageMetrics.NULL);
    }

    private static byte[] toBytes(ByteBuf buf) {
        byte[] bytes = new byte[buf.readableBytes()];
        buf.getBytes(buf.readerIndex(), bytes);
        return bytes;
    }

    @Test
    void testEvictionDefersSegmentCloseWhileLeased() throws Exception {
        SegmentFileStorage fileStorage = new SegmentFileStorage();
        ReadCache readCache = newReadCache(fileStorage);
        FileInfo file = new FileInfo("wal-leased", FILE_WEIGHT);

        PersistCache leased = readCache.acquire(file, 1).get();
        assertNotNull(leased);
        assertEquals(1, ((EntryCache) leased).leaseCount());

        // Evict while the reader still holds the segment: the removal listener may only retire it.
        readCache.getReadCache().invalidate(file);
        assertNull(readCache.getReadCache().getIfPresent(file));

        // Without the lease this read threw EntryCacheClosedException("already closed").
        ByteBuf payload = leased.get(STREAM_ID, 0);
        assertNotNull(payload);
        assertArrayEquals(payloadFor("wal-leased"), toBytes(payload));
        payload.release();

        // The last release performs the deferred close.
        leased.release();
        assertEquals(0, ((EntryCache) leased).leaseCount());
        assertThrows(EntryCacheClosedException.class, () -> leased.get(STREAM_ID, 0));

        readCache.close();
    }

    @Test
    void testEvictionClosesSegmentHeldWithoutLease() throws Exception {
        SegmentFileStorage fileStorage = new SegmentFileStorage();
        ReadCache readCache = newReadCache(fileStorage);
        FileInfo file = new FileInfo("wal-unleased", FILE_WEIGHT);

        // get() hands out the bare future with no lifetime contract; this is the shape that produced
        // the production EntryCacheClosedException storm and is why acquire() exists.
        PersistCache unleased = readCache.get(file, 1).get();
        assertNotNull(unleased);

        readCache.getReadCache().invalidate(file);

        assertThrows(EntryCacheClosedException.class, () -> unleased.get(STREAM_ID, 0));

        readCache.close();
    }

    @Test
    void testAcquireReturnsNullWhenSegmentAlreadyRetired() throws Exception {
        SegmentFileStorage fileStorage = new SegmentFileStorage();
        ReadCache readCache = newReadCache(fileStorage);
        FileInfo file = new FileInfo("wal-retired", FILE_WEIGHT);

        PersistCache segment = readCache.acquire(file, 1).get();
        assertNotNull(segment);
        segment.release();
        // Retire the segment but leave the (now stale) future reachable, reproducing the window where
        // a reader resolves the cache entry just as the removal listener retires it.
        segment.close();
        readCache.getReadCache().asMap().put(file, CompletableFuture.completedFuture(segment));

        assertNull(readCache.acquire(file, 1).get());

        readCache.close();
    }

    @Test
    void testEvictionBySizeStillClosesUnleasedSegments() throws Exception {
        SegmentFileStorage fileStorage = new SegmentFileStorage();
        ReadCache readCache = newReadCache(fileStorage);

        // maximumWeight is 1024 and every FileInfo weighs 256, so the cache holds four segments.
        for (int i = 0; i < 20; i++) {
            PersistCache segment = readCache.acquire(new FileInfo("wal-evict-" + i, FILE_WEIGHT), 1).get();
            assertNotNull(segment);
            segment.release();
        }
        assertEquals(20, fileStorage.reads.get());
        assertEquals(4, readCache.getReadCache().size());

        readCache.close();
    }
}
