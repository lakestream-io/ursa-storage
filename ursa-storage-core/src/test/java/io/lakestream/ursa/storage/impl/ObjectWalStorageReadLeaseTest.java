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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

import io.lakestream.api.EntryHeader;
import io.lakestream.api.EntryIndex;
import io.lakestream.api.FileInfo;
import io.lakestream.api.Position;
import io.lakestream.ursa.metrics.InstrumentProvider;
import io.lakestream.ursa.storage.EntryList;
import io.lakestream.ursa.storage.FileStorage;
import io.lakestream.ursa.storage.IDGenerator;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.metrics.SdkMeterProvider;
import io.opentelemetry.sdk.metrics.data.MetricData;
import io.opentelemetry.sdk.testing.exporter.InMemoryMetricReader;
import io.oxia.client.api.AsyncOxiaClient;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * End-to-end cover for the read-site lease adoption in {@link ObjectWalStorageImpl}: a cache segment
 * that has been retired must read as an ordinary miss that falls through to object storage, instead
 * of failing the whole fetch with {@code EntryCacheClosedException}.
 */
public class ObjectWalStorageReadLeaseTest {

    private static final ByteBufAllocator ALLOCATOR = PooledByteBufAllocator.DEFAULT;
    private static final long STREAM_ID = 11L;
    private static final String LOCATION = "wal-segment-0";
    private static final int MESSAGES_PER_STORED_ENTRY = 10;

    private StorageConfig config;
    private StorageFormat format;
    private CountingSegmentFileStorage fileStorage;
    private ObjectWalStorageImpl storage;
    private InMemoryMetricReader metricReader;

    /** Serves one deterministic single-entry WAL segment per location and counts the reads. */
    private static final class CountingSegmentFileStorage implements FileStorage {

        private final AtomicInteger reads = new AtomicInteger();
        private final StorageFormat format;
        private volatile boolean failReads;

        CountingSegmentFileStorage(StorageFormat format) {
            this.format = format;
        }

        @Override
        public CompletableFuture<ByteBuf> getAsync(String location) {
            reads.incrementAndGet();
            if (failReads) {
                return CompletableFuture.failedFuture(new IllegalStateException("storage unavailable"));
            }
            byte[] bytes = payloadFor(location);
            PersistCache writer = PersistCacheFactory.create(ALLOCATOR, bytes.length, PROTOBUF_VERSION);
            ByteBuf payload = Unpooled.wrappedBuffer(bytes);
            try {
                writer.put(new PendingAdd(STREAM_ID, MESSAGES_PER_STORED_ENTRY, payload,
                    new CompletableFuture<>(), null));
                return CompletableFuture.completedFuture(writer.serialize(location, format));
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

    @BeforeEach
    void setUp() throws Exception {
        config = new StorageConfig();
        config.setIndexSerializeFormatVersion(PROTOBUF_VERSION);
        config.setWriteCacheEnabled(true);
        config.setWriteBufferSegment(4);
        config.setWriteBufferSize(64 * 1024);
        format = new StorageFormat(config);
        fileStorage = new CountingSegmentFileStorage(format);
        metricReader = InMemoryMetricReader.create();
        OpenTelemetry otel = OpenTelemetrySdk.builder()
            .setMeterProvider(SdkMeterProvider.builder().registerMetricReader(metricReader).build())
            .build();
        storage = new ObjectWalStorageImpl(ALLOCATOR, fileStorage,
            IDGenerator.create("memory", null, null), config, new InstrumentProvider(otel),
            mock(AsyncOxiaClient.class), format, new StreamStateManagerImpl());
        storage.initialize();
    }

    @AfterEach
    void tearDown() {
        storage.close().join();
    }

    private EntryIndex indexFor(String location) {
        return indexFor(location, 0);
    }

    private EntryIndex indexFor(String location, long offset) {
        byte[] bytes = payloadFor(location);
        EntryHeader header = new EntryHeader(offset, MESSAGES_PER_STORED_ENTRY, System.currentTimeMillis(),
            bytes.length, bytes.length);
        Position position = new Position(new FileInfo(location, bytes.length), 0, Position.FileType.RAW);
        return new EntryIndex(header, position, 1, EntryIndex.IndexType.COMPACT, Optional.of(new int[]{1}));
    }

    /**
     * Seeds the write cache with a segment for {@code location} that holds {@code payload}, then
     * optionally retires it the way a shutdown or an error-path close would.
     */
    private PersistCache seedWriteCacheSegment(String location, byte[] payload) {
        return seedWriteCacheSegment(location, payload, MESSAGES_PER_STORED_ENTRY);
    }

    private PersistCache seedWriteCacheSegment(String location, byte[] payload, int messageCount) {
        PersistCache segment = storage.getWriteCacheForTest().poll();
        assertNotNull(segment);
        ByteBuf buf = Unpooled.wrappedBuffer(payload);
        try {
            segment.put(new PendingAdd(STREAM_ID, messageCount, buf, new CompletableFuture<>(), null));
        } finally {
            buf.release();
        }
        ((EntryCache) segment).setStartOffsets(STREAM_ID, 0);
        storage.getWriteCacheForTest().returnToCache(segment, Optional.of(location));
        return segment;
    }

    private static byte[] toBytes(ByteBuf buf) {
        byte[] bytes = new byte[buf.readableBytes()];
        buf.getBytes(buf.readerIndex(), bytes);
        return bytes;
    }

    @Test
    void testRetiredWriteCacheSegmentFallsBackToStorage() throws Exception {
        byte[] stale = "stale-write-cache-bytes".getBytes(UTF_8);
        PersistCache segment = seedWriteCacheSegment(LOCATION, stale);
        // Retire the cached segment while it is still reachable through the write cache, which is what
        // a shutdown close or an error-path close does. The read must treat it as a miss.
        segment.close();

        EntryList entryList = new EntryList(STREAM_ID, 0, Long.MAX_VALUE, 10, 10_000, null, null);
        storage.get(List.of(indexFor(LOCATION)), entryList).get();

        assertEquals(1, entryList.size());
        assertArrayEquals(payloadFor(LOCATION), toBytes(entryList.get(0).payload()));
        assertEquals(1, fileStorage.reads.get());
        entryList.clear();
    }

    @Test
    void testLiveWriteCacheSegmentIsStillServedFromCache() throws Exception {
        byte[] cached = "live-write-cache-bytes".getBytes(UTF_8);
        seedWriteCacheSegment(LOCATION, cached);

        EntryList entryList = new EntryList(STREAM_ID, 0, Long.MAX_VALUE, 10, 10_000, null, null);
        storage.get(List.of(indexFor(LOCATION)), entryList).get();

        assertEquals(1, entryList.size());
        assertArrayEquals(cached, toBytes(entryList.get(0).payload()));
        assertEquals(0, fileStorage.reads.get());
        entryList.clear();
    }

    @Test
    void testRepeatedReadsBalanceLeases() throws Exception {
        byte[] cached = "leases-must-balance".getBytes(UTF_8);
        PersistCache segment = seedWriteCacheSegment(LOCATION, cached);

        for (int i = 0; i < 32; i++) {
            EntryList entryList = new EntryList(STREAM_ID, 0, Long.MAX_VALUE, 10, 10_000, null, null);
            storage.get(List.of(indexFor(LOCATION)), entryList).get();
            assertEquals(1, entryList.size());
            entryList.clear();
        }

        // An unbalanced acquire would either pin the segment forever or drain it early.
        assertEquals(0, ((EntryCache) segment).leaseCount());
        // ... and the production tripwire for that must agree.
        assertEquals(0, storage.leasedSegmentsForTest());
        assertEquals(0, leasedSegmentsGauge());
    }

    /** Leaves a completed future for an already-retired segment reachable in the read cache. */
    private void seedRetiredReadCacheSegment(EntryIndex index) {
        PersistCache retired = PersistCacheFactory.create(ALLOCATOR, 64, PROTOBUF_VERSION);
        retired.close();
        storage.getReadCacheForTest().getReadCache().asMap()
            .put(index.position().file(), CompletableFuture.completedFuture(retired));
    }

    @Test
    void testRetiredReadCacheSegmentIsReadFromStorage() throws Exception {
        // Reproduces the production log line "Cache segment retired while reading location ...":
        // the location resolves to a retired segment, so no lease can be granted for it. That is an
        // ordinary cache miss and the batch must still complete, from storage.
        EntryIndex index = indexFor(LOCATION);
        seedRetiredReadCacheSegment(index);

        EntryList entryList = new EntryList(STREAM_ID, 0, Long.MAX_VALUE, 10, 10_000, null, null);
        storage.get(List.of(index), entryList).get();

        assertEquals(1, entryList.size());
        assertArrayEquals(payloadFor(LOCATION), toBytes(entryList.get(0).payload()));
        assertEquals(1, fileStorage.reads.get());
        assertEquals(1, staleSegmentMissCount());
        assertEquals(0, storage.leasedSegmentsForTest());
        entryList.clear();
    }

    @Test
    void testRetiredReadCacheSegmentAcrossMultipleLocationsIsReadFromStorage() throws Exception {
        // One live location and one retired location in the same batch: the live one is served from
        // cache, the retired one from storage, and the batch completes with both payloads in order.
        EntryIndex live = indexFor("wal-segment-live", 0);
        EntryIndex retired = indexFor("wal-segment-retired", MESSAGES_PER_STORED_ENTRY);
        seedRetiredReadCacheSegment(retired);

        EntryList entryList = new EntryList(STREAM_ID, 0, Long.MAX_VALUE, 100, 100_000, null, null);
        storage.get(List.of(live, retired), entryList).get();

        assertEquals(2, entryList.size());
        assertArrayEquals(payloadFor("wal-segment-live"), toBytes(entryList.get(0).payload()));
        assertArrayEquals(payloadFor("wal-segment-retired"), toBytes(entryList.get(1).payload()));
        assertEquals(0, storage.leasedSegmentsForTest());
        entryList.clear();
    }

    @Test
    void testRetiredSegmentReadFailsWithCacheLifecycleCauseWhenStorageFails() {
        EntryIndex index = indexFor(LOCATION);
        seedRetiredReadCacheSegment(index);
        fileStorage.failReads = true;

        EntryList entryList = new EntryList(STREAM_ID, 0, Long.MAX_VALUE, 10, 10_000, null, null);
        ExecutionException failure = assertThrows(ExecutionException.class,
            () -> storage.get(List.of(index), entryList).get());

        // The recovery read is what failed, so the storage error is what surfaces -- the read is not
        // silently swallowed -- and no lease is left behind.
        assertTrue(hasCause(failure, IllegalStateException.class), "unexpected failure: " + failure);
        assertEquals(0, storage.leasedSegmentsForTest());
        entryList.clear();
    }

    @Test
    void testStaleSegmentReadIsCountedAsACacheMiss() throws Exception {
        // A recycled segment: cleared after its flush, so the entry-id guard refuses the read.
        PersistCache segment = seedWriteCacheSegment(LOCATION, "recycled".getBytes(UTF_8), 1);
        segment.clear();

        var entry = storage.get(STREAM_ID, indexFor(LOCATION)).get();

        assertNotNull(entry);
        assertArrayEquals(payloadFor(LOCATION), toBytes(entry.payload()));
        entry.payload().release();
        assertEquals(1, staleSegmentMissCount());
        assertEquals(0, storage.leasedSegmentsForTest());
    }

    private static boolean hasCause(Throwable failure, Class<? extends Throwable> type) {
        for (Throwable cause = failure; cause != null; cause = cause.getCause()) {
            if (type.isInstance(cause)) {
                return true;
            }
            if (cause.getCause() == cause) {
                break;
            }
        }
        return false;
    }

    private long staleSegmentMissCount() {
        Collection<MetricData> metrics = metricReader.collectAllMetrics();
        return metrics.stream()
            .filter(metric -> "ursa.storage.wal.read.cache.missed".equals(metric.getName()))
            .flatMap(metric -> metric.getLongSumData().getPoints().stream())
            .filter(point -> "stale_segment".equals(
                point.getAttributes().get(io.opentelemetry.api.common.AttributeKey.stringKey("type"))))
            .mapToLong(point -> point.getValue())
            .sum();
    }

    private long leasedSegmentsGauge() {
        Collection<MetricData> metrics = metricReader.collectAllMetrics();
        return metrics.stream()
            .filter(metric -> "ursa.storage.wal.cache.leasedSegments".equals(metric.getName()))
            .flatMap(metric -> metric.getLongGaugeData().getPoints().stream())
            .filter(point -> point.getAttributes().equals(Attributes.empty()))
            .mapToLong(point -> point.getValue())
            .findFirst()
            .orElseThrow(() -> new AssertionError("leased segments gauge not reported"));
    }

    @Test
    void testStorageFallbackReadsFromTheDeserializedSegment() throws Exception {
        // The cached segment covers offset 0 only, so the read at offset 5 misses it and falls through
        // to storage. The fallback used to re-read the very same cached segment, which had just
        // returned null, so the read resolved to a null entry while the object it deserialized from
        // storage was discarded unused.
        seedWriteCacheSegment(LOCATION, "covers-offset-0-only".getBytes(UTF_8), 1);

        var entry = storage.get(STREAM_ID, 5, indexFor(LOCATION)).get();

        assertNotNull(entry);
        assertArrayEquals(payloadFor(LOCATION), toBytes(entry.payload()));
        assertEquals(1, fileStorage.reads.get());
        entry.payload().release();
    }

    @Test
    void testSingleEntryReadFallsBackToStorageForRetiredSegment() throws Exception {
        byte[] stale = "stale-write-cache-bytes".getBytes(UTF_8);
        PersistCache segment = seedWriteCacheSegment(LOCATION, stale);
        segment.close();

        EntryIndex index = indexFor(LOCATION);
        var entry = storage.get(STREAM_ID, 0, index).get();

        assertNotNull(entry);
        assertArrayEquals(payloadFor(LOCATION), toBytes(entry.payload()));
        entry.payload().release();
        assertEquals(1, fileStorage.reads.get());
    }
}
