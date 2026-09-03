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
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.lakestream.api.FileInfo;
import io.lakestream.ursa.metrics.InstrumentProvider;
import io.lakestream.ursa.storage.FileStorage;
import io.lakestream.ursa.storage.WalStorageMetrics;
import io.lakestream.ursa.storage.impl.exception.EntryCacheClosedException;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.metrics.SdkMeterProvider;
import io.opentelemetry.sdk.metrics.data.MetricData;
import io.opentelemetry.sdk.testing.exporter.InMemoryMetricReader;
import java.util.Collection;
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
        return newReadCache(fileStorage, WalStorageMetrics.NULL);
    }

    private static ReadCache newReadCache(FileStorage fileStorage, WalStorageMetrics metrics) {
        return new ReadCache(readCacheConfig(), ALLOCATOR, fileStorage, metrics);
    }

    private static StorageConfig readCacheConfig() {
        StorageConfig config = new StorageConfig();
        config.setIndexSerializeFormatVersion(PROTOBUF_VERSION);
        config.setReadCacheMemorySize(1024);
        return config;
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
    void testRemovalClosesSegmentEvenWhenItsSizeIsUnreadable() {
        SegmentFileStorage fileStorage = new SegmentFileStorage();
        ReadCache readCache = newReadCache(fileStorage);
        FileInfo file = new FileInfo("wal-already-closed", FILE_WEIGHT);

        // Stands in for a segment whose size can no longer be read when the removal listener runs.
        // The listener used to call sizeInBytes() first and let the throw escape, skipping both the
        // size-gauge decrement and, worse, the close() -- leaking the segment's retained slice of the
        // storage read buffer until GC.
        PersistCache unreadable = mock(PersistCache.class);
        when(unreadable.sizeInBytes()).thenThrow(new EntryCacheClosedException("already closed"));
        readCache.getReadCache().asMap().put(file, CompletableFuture.completedFuture(unreadable));

        readCache.getReadCache().invalidate(file);

        verify(unreadable).close();

        readCache.close();
    }

    @Test
    void testSizeGaugeDecrementsOnDoubleRemoval() throws Exception {
        InMemoryMetricReader metricReader = InMemoryMetricReader.create();
        SdkMeterProvider meterProvider = SdkMeterProvider.builder().registerMetricReader(metricReader).build();
        OpenTelemetry otel = OpenTelemetrySdk.builder().setMeterProvider(meterProvider).build();
        WalStorageMetrics metrics = new WalStorageMetrics("test", new InstrumentProvider(otel), readCacheConfig());

        SegmentFileStorage fileStorage = new SegmentFileStorage();
        ReadCache readCache = newReadCache(fileStorage, metrics);
        FileInfo file = new FileInfo("wal-gauge", FILE_WEIGHT);

        PersistCache segment = readCache.acquire(file, 1).get();
        assertNotNull(segment);
        segment.release();
        long loadedSize = segment.sizeInBytes();
        assertTrue(loadedSize > 0);
        assertEquals(loadedSize, readCacheSize(metricReader));

        readCache.getReadCache().invalidate(file);
        assertEquals(0, readCacheSize(metricReader));

        // Double removal: the stale future is reachable again and removed a second time. The first
        // removal already accounted for the segment's size, so the gauge must stay at 0 rather than
        // going negative or being skipped by a throw out of sizeInBytes() on the closed segment.
        readCache.getReadCache().asMap().put(file, CompletableFuture.completedFuture(segment));
        readCache.getReadCache().invalidate(file);
        assertEquals(0, readCacheSize(metricReader));

        readCache.close();
        assertEquals(0, readCacheSize(metricReader));
    }

    private static long readCacheSize(InMemoryMetricReader metricReader) {
        Collection<MetricData> metrics = metricReader.collectAllMetrics();
        return metrics.stream()
            .filter(metric -> "ursa.storage.wal.readCache.size".equals(metric.getName()))
            .flatMap(metric -> metric.getLongGaugeData().getPoints().stream())
            .filter(point -> point.getAttributes().equals(Attributes.empty()))
            .mapToLong(point -> point.getValue())
            .findFirst()
            .orElseThrow(() -> new AssertionError("read cache size gauge not reported"));
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
