/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.storage.impl;

import static io.lakestream.ursa.storage.impl.StorageConfig.PROTOBUF_VERSION;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.lakestream.api.EntryHeader;
import io.lakestream.api.EntryIndex;
import io.lakestream.api.FileInfo;
import io.lakestream.api.Position;
import io.lakestream.ursa.metrics.InstrumentProvider;
import io.lakestream.ursa.metrics.LatencyHistogram;
import io.lakestream.ursa.storage.AddResult;
import io.lakestream.ursa.storage.Entry;
import io.lakestream.ursa.storage.EntryList;
import io.lakestream.ursa.storage.FileBasedTestClass;
import io.lakestream.ursa.storage.FileStorage;
import io.lakestream.ursa.storage.Key;
import io.lakestream.ursa.storage.Value;
import io.lakestream.ursa.storage.impl.exception.EntryCacheClosedException;
import io.lakestream.ursa.storage.impl.exception.RetryableException;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.oxia.client.api.AsyncOxiaClient;
import io.oxia.client.api.PutResult;
import io.oxia.client.api.Version;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import lombok.Cleanup;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

public class TestEntryCache extends FileBasedTestClass {

    private StorageConfig config;

    private PersistCache persistCache;
    private ByteBufAllocator allocator;

    @Mock
    private FileStorage mockFileStorage;

    @Mock
    private AsyncOxiaClient mockOxiaClient;
    private long streamId = 1;
    private Optional<int[]> entryOffsets = Optional.of(new int[]{10, 20});
    private StorageFormat format;
    private Version version;
    private String key1;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
        config =
                StorageConfig.builder().indexSerializeFormatVersion(PROTOBUF_VERSION)
                        .storagePath(path.toAbsolutePath().toString()).build();
        format = new StorageFormat(config);
        allocator = ByteBufAllocator.DEFAULT;
        persistCache =  // 1MB cache
                PersistCacheFactory.create(allocator, 1024 * 1024, PROTOBUF_VERSION);
        version = new Version(1, 1, 1, 1, Optional.empty(), Optional.empty());
        key1 = format.getStreamIdKey(streamId);
        when(mockFileStorage.putAsync(any(), any())).thenReturn(CompletableFuture.completedFuture(null));
    }

    @AfterEach
    void tearDown() {
        persistCache.close();
    }

    @RepeatedTest(100)
    void testConcurrentCloseAndCopy() throws Exception {

        String location = UUID.randomUUID().toString();
        @Cleanup("release")
        ByteBuf payload1 = Unpooled.wrappedBuffer("test entry1".getBytes());
        @Cleanup("release")
        ByteBuf payload2 = Unpooled.wrappedBuffer("test entry2".getBytes());
        int entrySize = payload1.readableBytes() + payload2.readableBytes();
        EntryHeader compactedHeader =
                new EntryHeader(0, 20, 1, entrySize, 100 + entrySize);

        EntryHeader header1 =
                new EntryHeader(0, 10, 1, payload1.readableBytes(), 100 + payload1.readableBytes());

        EntryHeader header2 =
                new EntryHeader(10, 10, 1, payload2.readableBytes(), 100 + entrySize);

        io.lakestream.api.Position
                position =
                new io.lakestream.api.Position(new FileInfo(location, entrySize), -1, Position.FileType.RAW);
        EntryIndex index = new EntryIndex(compactedHeader, position, 2, EntryIndex.IndexType.COMPACT, entryOffsets);

        Entry entry1 = Entry.of(header1, payload1);
        Entry entry2 = Entry.of(header2, payload2);


        var pendingAdd1 = new PendingAdd(streamId, 10, payload1, new CompletableFuture<>(), null);
        var pendingAdd2 = new PendingAdd(streamId, 10, payload2, new CompletableFuture<>(), null);
        long result1 = persistCache.put(pendingAdd1);
        long result2 = persistCache.put(pendingAdd2);
        assertEquals(0, result1);
        assertEquals(1, result2);

        ((EntryCache) persistCache).setStartOffsets(streamId, 0);

        EntryList entryList = new EntryList(streamId, 0, Long.MAX_VALUE, 1000, 10000, null, null);

        // The reader holds a lease for the whole copy, so a concurrent close may only retire the
        // segment: the copy must succeed and the teardown must wait for release().
        assertTrue(persistCache.tryRetain());

        ExecutorService executorA = Executors.newSingleThreadExecutor();
        ExecutorService executorB = Executors.newSingleThreadExecutor();
        CompletableFuture<Void> copyFuture = CompletableFuture.runAsync(() -> {
            try {
                persistCache.copy(index, entryList);
            } catch (RetryableException e) {
                throw new RuntimeException(e);
            }
            assertEquals(2, entryList.size());
            assertEquals(entry1, entryList.getEntries().get(0));
            assertEquals(entry2, entryList.getEntries().get(1));
            assertEquals(entrySize, entryList.getSizeInBytes());
        }, executorA);
        CompletableFuture<Void> closeFuture = CompletableFuture.runAsync(() -> {
            persistCache.close();
        }, executorB);

        closeFuture.join();
        // No EntryCacheClosedException is reachable here any more: the lease outlives the close.
        copyFuture.join();
        assertEquals(2, entryList.size());
        entryList.clear();

        persistCache.release();
        // The last release runs the deferred teardown.
        assertThrows(EntryCacheClosedException.class, () -> persistCache.copy(index, entryList));
        executorA.shutdownNow();
        executorB.shutdownNow();
    }

    @Test
    void testRetainAfterCloseIsRefused() {
        assertTrue(persistCache.tryRetain());
        persistCache.release();

        persistCache.close();

        assertFalse(persistCache.tryRetain());
        assertEquals(0, ((EntryCache) persistCache).leaseCount());
    }

    @Test
    void testCloseIsDeferredUntilLastReleaseAndRunsOnce() {
        assertTrue(persistCache.tryRetain());
        assertTrue(persistCache.tryRetain());
        assertEquals(2, ((EntryCache) persistCache).leaseCount());

        persistCache.close();
        // Retired but not torn down: an in-flight reader can still read.
        assertEquals(0, persistCache.sizeInBytes());

        persistCache.release();
        assertEquals(0, persistCache.sizeInBytes());

        persistCache.release();
        assertThrows(EntryCacheClosedException.class, persistCache::sizeInBytes);

        // Idempotent: a second close (and the tearDown close) must not release the buffer twice.
        persistCache.close();
        persistCache.close();
    }

    @Test
    void testGetByEntryIdIsGuardedAfterRecycle() {
        @Cleanup("release")
        ByteBuf original = Unpooled.wrappedBuffer("original-entry".getBytes());
        assertEquals(0, persistCache.put(
                new PendingAdd(streamId, 10, original, new CompletableFuture<>(), null)));
        // A flush publishes the segment's first offset, which is what makes it readable by location.
        ((EntryCache) persistCache).setStartOffsets(streamId, 0);

        ByteBuf payload = persistCache.get(streamId, 0);
        assertNotNull(payload);
        assertEquals(original, payload);
        payload.release();

        // FIFO recycle: clear() drops the start offsets, and the segment is refilled with entirely
        // different data for the same stream before the next flush publishes a new start offset.
        persistCache.clear();
        @Cleanup("release")
        ByteBuf recycled = Unpooled.wrappedBuffer("a-completely-different-entry".getBytes());
        assertEquals(0, persistCache.put(
                new PendingAdd(streamId, 10, recycled, new CompletableFuture<>(), null)));

        // Without the guard this returned the new payload for the old entry id: well formed, wrong
        // data, no error and no metric. A miss sends the caller to storage instead.
        assertNull(persistCache.get(streamId, 0));

        // Once the refilled segment is flushed it is readable again, with its own data.
        ((EntryCache) persistCache).setStartOffsets(streamId, 10);
        ByteBuf afterFlush = persistCache.get(streamId, 0);
        assertNotNull(afterFlush);
        assertEquals(recycled, afterFlush);
        afterFlush.release();
    }

    @Test
    void testReleaseWithoutRetainIsRejected() {
        assertThrows(IllegalStateException.class, persistCache::release);
        assertEquals(0, ((EntryCache) persistCache).leaseCount());
    }
    @Test
    void testApisAfterClose() throws Exception {
        persistCache.close();

        String location = UUID.randomUUID().toString();
        @Cleanup("release")
        ByteBuf payload1 = Unpooled.wrappedBuffer("test entry1".getBytes());
        @Cleanup("release")
        ByteBuf payload2 = Unpooled.wrappedBuffer("test entry2".getBytes());
        int entrySize = payload1.readableBytes() + payload2.readableBytes();
        EntryHeader compactedHeader =
                new EntryHeader(100, 20, 1, entrySize, 100 + entrySize);

        io.lakestream.api.Position
                position =
                new io.lakestream.api.Position(new FileInfo(location, entrySize), -1, Position.FileType.RAW);
        EntryIndex index = new EntryIndex(compactedHeader, position, 2, EntryIndex.IndexType.COMPACT, entryOffsets);

        String key = format.getStreamIdKey(streamId);
        Value value = Value.of(index);
        PutResult indexResult = new PutResult(Key.of(streamId, compactedHeader).toString(), version);

        when(mockOxiaClient.put(eq(key),
                argThat(actual -> Arrays.equals(actual, value.toBytes(config.getIndexSerializeFormatVersion()))),
                any()
        ))
                .thenReturn(CompletableFuture.completedFuture(indexResult));

        var pendingAdd1 = new PendingAdd(streamId, 10, payload1, new CompletableFuture<>(), null);
        Exception ex = assertThrows(EntryCacheClosedException.class, () -> persistCache.put(pendingAdd1));
        assertEquals("already closed", ex.getMessage());

        EntryList entryList = new EntryList(streamId, 0, Long.MAX_VALUE, 1000, 10000, null, null);
        ex = assertThrows(EntryCacheClosedException.class, () -> persistCache.copy(index, entryList));
        assertEquals("already closed", ex.getMessage());

        ex = assertThrows(EntryCacheClosedException.class, () -> persistCache.get(streamId, 100, index));
        assertEquals("already closed", ex.getMessage());

        ex = assertThrows(EntryCacheClosedException.class, () -> persistCache.get(streamId, 0));
        assertEquals("already closed", ex.getMessage());

        ex = assertThrows(EntryCacheClosedException.class, () -> persistCache.serialize("1", format));
        assertEquals("already closed", ex.getMessage());

        @Cleanup
        LocalFileStorage storage = new LocalFileStorage(config, InstrumentProvider.NOOP);
        ex = assertThrows(CompletionException.class, () -> persistCache.persist(storage, location, format).join());
        assertEquals("io.lakestream.ursa.storage.impl.exception.EntryCacheClosedException: already closed",
                ex.getMessage());

        ex = assertThrows(EntryCacheClosedException.class, () -> persistCache.index(mockOxiaClient,
                new StreamStateManagerImpl()));
        assertEquals("already closed", ex.getMessage());

        // flushSucceed and flushFailed must work on closed caches to avoid orphaning callbacks
        persistCache.flushSucceed(location, new HashMap<>());
        persistCache.flushFailed(new RuntimeException());

        ex = assertThrows(EntryCacheClosedException.class, () -> persistCache.entryCount());
        assertEquals("already closed", ex.getMessage());

        ex = assertThrows(EntryCacheClosedException.class, () -> persistCache.sizeInBytes());
        assertEquals("already closed", ex.getMessage());

        ex = assertThrows(EntryCacheClosedException.class, () -> persistCache.isEmpty());
        assertEquals("already closed", ex.getMessage());

        // The read statistics are deliberately unguarded: the eviction pass reads them while walking
        // a cache other threads are evicting from, and a throw there aborted the whole pass. After a
        // close they report their last known, harmless values instead.
        assertEquals(0, persistCache.getReadCount());
        assertTrue(persistCache.getLastReadTimestamp() > 0);
        assertEquals(0, persistCache.getReadDurationInMillis());

        ex = assertThrows(EntryCacheClosedException.class, () -> persistCache.clear());
        assertEquals("already closed", ex.getMessage());

        // close() is idempotent — second call should be a no-op
        persistCache.close();
    }

    @Test
    void testPutAndGetAndPersistAndIndex() throws Exception {
        String location = UUID.randomUUID().toString();
        @Cleanup("release")
        ByteBuf payload1 = Unpooled.wrappedBuffer("test entry1".getBytes());
        @Cleanup("release")
        ByteBuf payload2 = Unpooled.wrappedBuffer("test entry2".getBytes());
        int entrySize = payload1.readableBytes() + payload2.readableBytes();
        EntryHeader compactedHeader =
                new EntryHeader(0, 20, 1, entrySize, 100 + entrySize);

        EntryHeader header1 =
                new EntryHeader(0, 10, 1, payload1.readableBytes(), 100 + payload1.readableBytes());

        EntryHeader header2 =
                new EntryHeader(10, 10, 1, payload2.readableBytes(), 100 + entrySize);

        io.lakestream.api.Position
                position =
                new io.lakestream.api.Position(new FileInfo(location, entrySize), -1, Position.FileType.RAW);
        EntryIndex index = new EntryIndex(compactedHeader, position, 2, EntryIndex.IndexType.COMPACT, entryOffsets);

        Entry entry1 = Entry.of(header1, payload1);
        Entry entry2 = Entry.of(header2, payload2);

        String key = format.getStreamIdKey(streamId);
        Value value = Value.of(index);
        PutResult indexResult = new PutResult(Key.of(streamId, compactedHeader).toString(), version);

        when(mockOxiaClient.put(eq(key),
                argThat(actual -> Arrays.equals(actual, value.toBytes(config.getIndexSerializeFormatVersion()))),
                any()
        ))
                .thenReturn(CompletableFuture.completedFuture(indexResult));

        var pendingAdd1 = new PendingAdd(streamId, 10, payload1, new CompletableFuture<>(), null);
        var pendingAdd2 = new PendingAdd(streamId, 10, payload2, new CompletableFuture<>(), null);
        long result1 = persistCache.put(pendingAdd1);
        long result2 = persistCache.put(pendingAdd2);
        assertEquals(0, result1);
        assertEquals(1, result2);

        ((EntryCache) persistCache).setStartOffsets(streamId, 0);

        EntryList entryList = new EntryList(streamId, 0, Long.MAX_VALUE, 1000, 10000, null, null);
        persistCache.copy(index, entryList);
        assertEquals(2, entryList.size());
        assertEquals(entry1, entryList.getEntries().get(0));
        assertEquals(entry2, entryList.getEntries().get(1));
        assertEquals(entrySize, entryList.getSizeInBytes());

        EntryList entryList2 = new EntryList(streamId, 1000, Long.MAX_VALUE, 1000, 10000, null, null);
        persistCache.copy(index, entryList2);
        assertEquals(0, entryList2.size());
        assertEquals(0, entryList2.getSizeInBytes());


        var readEntry1 = persistCache.get(streamId, 0, index);
        var readEntry2 = persistCache.get(streamId, 10, index);
        var readEntry3 = persistCache.get(streamId, 20, index);
        assertEquals(entry1, readEntry1);
        assertEquals(entry2, readEntry2);
        assertNull(readEntry3);

        @Cleanup
        LocalFileStorage storage = new LocalFileStorage(config, InstrumentProvider.NOOP);
        persistCache.persist(storage, location, format).join();

        EntryCache readEntryCache = EntryCache.deserialize(allocator, storage.get(location));
        EntryList entryList3 = new EntryList(streamId);
        readEntryCache.copy(index, entryList3);

        assertEquals(2, entryList3.size());
        assertEquals(entry1, entryList3.getEntries().get(0));
        assertEquals(entry2, entryList3.getEntries().get(1));
        assertEquals(entrySize, entryList3.getSizeInBytes());

        var indexResults = persistCache.index(mockOxiaClient, new StreamStateManagerImpl()).join();
        verify(mockOxiaClient, times(1)).put(anyString(), any(), any());
        assertEquals(indexResult, ((StreamIndexResult.Success) indexResults.get(streamId)).putResult());
        persistCache.flushSucceed(location, indexResults);
    }

    @Test
    void testSingleEntryIndex() throws Exception {
        @Cleanup("release")
        ByteBuf entry = Unpooled.wrappedBuffer("test entry1".getBytes());
        int entrySize = entry.readableBytes();
        String location = UUID.randomUUID().toString();
        EntryHeader header =
                new EntryHeader(100, 10, 1, entrySize, 100 + entrySize);
        io.lakestream.api.Position
                position =
                new io.lakestream.api.Position(new FileInfo(location, entrySize), -1, Position.FileType.RAW);
        EntryIndex index = new EntryIndex(header, position, 1, EntryIndex.IndexType.COMPACT, Optional.empty());
        Value value = Value.of(index);
        PutResult indexResult = new PutResult(Key.of(streamId, header).toString(), version);

        when(mockOxiaClient.put(eq(key1),
                argThat(actual -> Arrays.equals(actual, value.toBytes(config.getIndexSerializeFormatVersion()))),
                any()
        ))
                .thenReturn(CompletableFuture.completedFuture(indexResult));

        var pendingAdd = new PendingAdd(streamId, 10, entry, new CompletableFuture<>(), null);
        long result1 = persistCache.put(pendingAdd);
        assertEquals(0, result1);

        @Cleanup
        LocalFileStorage storage = new LocalFileStorage(config, InstrumentProvider.NOOP);
        persistCache.persist(storage, location, format).join();

        EntryCache readEntryCache = EntryCache.deserialize(allocator, storage.get(location));
        EntryList entryList = new EntryList(streamId);
        readEntryCache.copy(index, entryList);

        assertEquals(1, entryList.size());
        assertEquals(header, entryList.get(0).header());
        assertEquals(entry, entryList.get(0).payload());

        var indexResults = persistCache.index(mockOxiaClient, new StreamStateManagerImpl()).join();
        verify(mockOxiaClient, times(1)).put(any(), any(), any());
        assertEquals(indexResult, ((StreamIndexResult.Success) indexResults.get(streamId)).putResult());
        persistCache.flushSucceed(location, indexResults);
    }

    @Test
    void testPutRejectedDoesNotCreateEmptyStreamIndexEntry() {
        @Cleanup
        PersistCache tinyCache = PersistCacheFactory.create(allocator, 10, PROTOBUF_VERSION);

        @Cleanup("release")
        ByteBuf payload1 = Unpooled.wrappedBuffer(new byte[10]);
        @Cleanup("release")
        ByteBuf payload2 = Unpooled.wrappedBuffer(new byte[1]);

        long stream1 = 1;
        long stream2 = 2;

        var pendingAdd1 = new PendingAdd(stream1, 1, payload1, new CompletableFuture<>(), null);
        var pendingAdd2 = new PendingAdd(stream2, 1, payload2, new CompletableFuture<>(), null);

        assertEquals(0, tinyCache.put(pendingAdd1));
        assertEquals(-1, tinyCache.put(pendingAdd2));

        assertEquals(1, ((EntryCache) tinyCache).getIndex().size());

        @Cleanup("release")
        ByteBuf serialized = tinyCache.serialize(UUID.randomUUID().toString(), format);
    }

    @Test
    void testIndexCapturesPerStreamFailures() {
        long stream1 = 1;
        long stream2 = 2;
        String key1 = format.getStreamIdKey(stream1);
        String key2 = format.getStreamIdKey(stream2);

        @Cleanup("release")
        ByteBuf payload1 = Unpooled.wrappedBuffer("entry-for-stream1".getBytes());
        @Cleanup("release")
        ByteBuf payload2 = Unpooled.wrappedBuffer("entry-for-stream2".getBytes());

        PutResult result1 = new PutResult(
                Key.of(stream1, new EntryHeader(0, 1, 1, payload1.readableBytes(),
                        payload1.readableBytes())).toString(), version);

        // stream1 put succeeds, stream2 put fails
        when(mockOxiaClient.put(eq(key1), any(), any()))
                .thenReturn(CompletableFuture.completedFuture(result1));
        when(mockOxiaClient.put(eq(key2), any(), any()))
                .thenReturn(CompletableFuture.failedFuture(new Exception("index write failed")));

        var pendingAdd1 = new PendingAdd(stream1, 1, payload1, new CompletableFuture<>(), null);
        var pendingAdd2 = new PendingAdd(stream2, 1, payload2, new CompletableFuture<>(), null);
        persistCache.put(pendingAdd1);
        persistCache.put(pendingAdd2);

        // serialize() populates indexRequests which index() iterates
        String location = UUID.randomUUID().toString();
        @Cleanup("release")
        ByteBuf serialized = persistCache.serialize(location, format);

        // index() should complete normally even though stream2 failed
        var indexResults = persistCache.index(mockOxiaClient, new StreamStateManagerImpl()).join();

        assertNotNull(indexResults);
        assertInstanceOf(StreamIndexResult.Success.class, indexResults.get(stream1));
        assertInstanceOf(StreamIndexResult.Failed.class, indexResults.get(stream2));

        assertEquals(result1,
                ((StreamIndexResult.Success) indexResults.get(stream1)).putResult());
        assertEquals("index write failed",
                ((StreamIndexResult.Failed) indexResults.get(stream2)).cause().getMessage());
    }

    @Test
    void testFlushSucceedWithMixedResults() {
        long stream1 = 1;
        long stream2 = 2;
        String location = UUID.randomUUID().toString();

        @Cleanup("release")
        ByteBuf payload1 = Unpooled.wrappedBuffer("entry-for-stream1".getBytes());
        @Cleanup("release")
        ByteBuf payload2 = Unpooled.wrappedBuffer("entry-for-stream2".getBytes());

        CompletableFuture<AddResult> future1 = new CompletableFuture<>();
        CompletableFuture<AddResult> future2 = new CompletableFuture<>();

        // Use NOOP latency histogram so onFailure() doesn't NPE
        var pendingAdd1 = new PendingAdd(stream1, 1, payload1, future1, LatencyHistogram.NOOP);
        var pendingAdd2 = new PendingAdd(stream2, 1, payload2, future2, LatencyHistogram.NOOP);
        persistCache.put(pendingAdd1);
        persistCache.put(pendingAdd2);

        // Build mixed result map: stream1 succeeds, stream2 fails
        int entrySize = payload1.readableBytes();
        EntryHeader header1 = new EntryHeader(0, 1, 1, entrySize, entrySize);
        PutResult putResult1 = new PutResult(Key.of(stream1, header1).toString(), version);
        var indexResults = new HashMap<Long, StreamIndexResult>();
        indexResults.put(stream1, new StreamIndexResult.Success(putResult1));
        indexResults.put(stream2, new StreamIndexResult.Failed(new Exception("index write failed")));

        persistCache.flushSucceed(location, indexResults);

        // stream1's callback should have received success with correct header values
        assertTrue(future1.isDone());
        assertFalse(future1.isCompletedExceptionally());
        AddResult result1 = future1.join();
        assertNotNull(result1.header());
        assertEquals(1, result1.header().numberOfMessages());

        // stream2's callback should have received failure with the original cause
        assertTrue(future2.isDone());
        assertTrue(future2.isCompletedExceptionally());
        var ex2 = assertThrows(java.util.concurrent.ExecutionException.class, future2::get);
        assertEquals("index write failed", ex2.getCause().getMessage());
    }

    @Test
    void testFlushSucceedWithFencedAndSuccess() {
        long stream1 = 1;
        long stream2 = 2;
        String location = UUID.randomUUID().toString();

        @Cleanup("release")
        ByteBuf payload1 = Unpooled.wrappedBuffer("entry-for-stream1".getBytes());
        @Cleanup("release")
        ByteBuf payload2 = Unpooled.wrappedBuffer("entry-for-stream2".getBytes());

        CompletableFuture<AddResult> future1 = new CompletableFuture<>();
        CompletableFuture<AddResult> future2 = new CompletableFuture<>();

        var pendingAdd1 = new PendingAdd(stream1, 1, payload1, future1, LatencyHistogram.NOOP);
        var pendingAdd2 = new PendingAdd(stream2, 1, payload2, future2, LatencyHistogram.NOOP);
        persistCache.put(pendingAdd1);
        persistCache.put(pendingAdd2);

        int entrySize = payload1.readableBytes();
        EntryHeader header1 = new EntryHeader(0, 1, 1, entrySize, entrySize);
        PutResult putResult1 = new PutResult(Key.of(stream1, header1).toString(), version);
        var indexResults = new HashMap<Long, StreamIndexResult>();
        indexResults.put(stream1, new StreamIndexResult.Success(putResult1));
        indexResults.put(stream2, new StreamIndexResult.Fenced());

        persistCache.flushSucceed(location, indexResults);

        // stream1 succeeds
        assertTrue(future1.isDone());
        assertFalse(future1.isCompletedExceptionally());

        // stream2 is fenced — should fail with the storage-level exception
        assertTrue(future2.isDone());
        assertTrue(future2.isCompletedExceptionally());
        var ex2 = assertThrows(java.util.concurrent.ExecutionException.class, future2::get);
        assertInstanceOf(io.lakestream.api.exception.LogFencedException.class, ex2.getCause());
    }

    @Test
    void testFlushSucceedWithMissingIndexResult() {
        long stream1 = 1;
        String location = UUID.randomUUID().toString();

        @Cleanup("release")
        ByteBuf payload1 = Unpooled.wrappedBuffer("entry-for-stream1".getBytes());

        CompletableFuture<AddResult> future1 = new CompletableFuture<>();
        var pendingAdd1 = new PendingAdd(stream1, 1, payload1, future1, LatencyHistogram.NOOP);
        persistCache.put(pendingAdd1);

        // Empty index results — stream1 is missing
        var indexResults = new HashMap<Long, StreamIndexResult>();

        persistCache.flushSucceed(location, indexResults);

        // stream1's callback should have received failure with IllegalStateException
        assertTrue(future1.isDone());
        assertTrue(future1.isCompletedExceptionally());
        var ex1 = assertThrows(java.util.concurrent.ExecutionException.class, future1::get);
        assertInstanceOf(IllegalStateException.class, ex1.getCause());
        assertTrue(ex1.getCause().getMessage().contains("Index result missing for stream"));
    }

    @Test
    void testIndexWithFencedStream() {
        long stream1 = 1;
        long stream2 = 2;

        @Cleanup("release")
        ByteBuf payload1 = Unpooled.wrappedBuffer("entry-for-stream1".getBytes());
        @Cleanup("release")
        ByteBuf payload2 = Unpooled.wrappedBuffer("entry-for-stream2".getBytes());

        String key1 = format.getStreamIdKey(stream1);
        PutResult result1 = new PutResult(
                Key.of(stream1, new EntryHeader(0, 1, 1, payload1.readableBytes(),
                        payload1.readableBytes())).toString(), version);

        when(mockOxiaClient.put(eq(key1), any(), any()))
                .thenReturn(CompletableFuture.completedFuture(result1));

        var pendingAdd1 = new PendingAdd(stream1, 1, payload1, new CompletableFuture<>(), null);
        var pendingAdd2 = new PendingAdd(stream2, 1, payload2, new CompletableFuture<>(), null);
        persistCache.put(pendingAdd1);
        persistCache.put(pendingAdd2);

        // serialize() populates indexRequests which index() iterates
        String location = UUID.randomUUID().toString();
        @Cleanup("release")
        ByteBuf serialized = persistCache.serialize(location, format);

        // Fence stream2
        var stateManager = new StreamStateManagerImpl();
        stateManager.setState(stream2, io.lakestream.api.LogState.FENCED);

        var indexResults = persistCache.index(mockOxiaClient, stateManager).join();

        assertNotNull(indexResults);
        assertInstanceOf(StreamIndexResult.Success.class, indexResults.get(stream1));
        assertInstanceOf(StreamIndexResult.Fenced.class, indexResults.get(stream2));

        // Oxia put() should only be called for stream1, not for fenced stream2
        String key2 = format.getStreamIdKey(stream2);
        verify(mockOxiaClient, never()).put(eq(key2), any(), any());
        verify(mockOxiaClient, times(1)).put(eq(key1), any(), any());
    }

    @Test
    void testFlushSucceedWithMultipleEntriesPerStream() {
        long stream1 = 1;
        long stream2 = 2;
        String location = UUID.randomUUID().toString();

        @Cleanup("release")
        ByteBuf payload1a = Unpooled.wrappedBuffer("stream1-entry-a".getBytes());
        @Cleanup("release")
        ByteBuf payload1b = Unpooled.wrappedBuffer("stream1-entry-b".getBytes());
        @Cleanup("release")
        ByteBuf payload2a = Unpooled.wrappedBuffer("stream2-entry-a".getBytes());
        @Cleanup("release")
        ByteBuf payload2b = Unpooled.wrappedBuffer("stream2-entry-b".getBytes());

        CompletableFuture<AddResult> future1a = new CompletableFuture<>();
        CompletableFuture<AddResult> future1b = new CompletableFuture<>();
        CompletableFuture<AddResult> future2a = new CompletableFuture<>();
        CompletableFuture<AddResult> future2b = new CompletableFuture<>();

        var pending1a = new PendingAdd(stream1, 1, payload1a, future1a, LatencyHistogram.NOOP);
        var pending1b = new PendingAdd(stream1, 1, payload1b, future1b, LatencyHistogram.NOOP);
        var pending2a = new PendingAdd(stream2, 1, payload2a, future2a, LatencyHistogram.NOOP);
        var pending2b = new PendingAdd(stream2, 1, payload2b, future2b, LatencyHistogram.NOOP);
        persistCache.put(pending1a);
        persistCache.put(pending1b);
        persistCache.put(pending2a);
        persistCache.put(pending2b);

        // Build results: stream1 succeeds (2 entries), stream2 fails
        int totalSize1 = payload1a.readableBytes() + payload1b.readableBytes();
        EntryHeader header1 = new EntryHeader(0, 2, 1, totalSize1, totalSize1);
        PutResult putResult1 = new PutResult(Key.of(stream1, header1).toString(), version);
        var indexResults = new HashMap<Long, StreamIndexResult>();
        indexResults.put(stream1, new StreamIndexResult.Success(putResult1));
        indexResults.put(stream2, new StreamIndexResult.Failed(new Exception("index write failed")));

        persistCache.flushSucceed(location, indexResults);

        // Both stream1 entries should succeed with correct headers
        assertTrue(future1a.isDone());
        assertFalse(future1a.isCompletedExceptionally());
        assertTrue(future1b.isDone());
        assertFalse(future1b.isCompletedExceptionally());

        AddResult result1a = future1a.join();
        AddResult result1b = future1b.join();
        assertNotNull(result1a.header());
        assertNotNull(result1b.header());
        assertEquals(1, result1a.header().numberOfMessages());
        assertEquals(1, result1b.header().numberOfMessages());
        // notifyCursors is true only for the last entry in the stream
        assertFalse(result1a.notifyCursors());
        assertTrue(result1b.notifyCursors());

        // Both stream2 entries should fail
        assertTrue(future2a.isDone());
        assertTrue(future2a.isCompletedExceptionally());
        assertTrue(future2b.isDone());
        assertTrue(future2b.isCompletedExceptionally());
        var ex2a = assertThrows(java.util.concurrent.ExecutionException.class, future2a::get);
        assertEquals("index write failed", ex2a.getCause().getMessage());
        var ex2b = assertThrows(java.util.concurrent.ExecutionException.class, future2b::get);
        assertEquals("index write failed", ex2b.getCause().getMessage());
    }

    @Test
    void testFailNotifiesAllCallbacksEvenWhenOneThrows() {
        long stream1 = 1;

        @Cleanup("release")
        ByteBuf payload1 = Unpooled.wrappedBuffer("entry1".getBytes());
        @Cleanup("release")
        ByteBuf payload2 = Unpooled.wrappedBuffer("entry2".getBytes());

        CompletableFuture<AddResult> future1 = new CompletableFuture<>();
        CompletableFuture<AddResult> future2 = new CompletableFuture<>();

        // First PendingAdd uses null putLatencyRef — onFailure will NPE before completing future
        var pendingAdd1 = new PendingAdd(stream1, 5, payload1, future1, null);
        // Second PendingAdd uses NOOP — onFailure will work correctly
        var pendingAdd2 = new PendingAdd(stream1, 5, payload2, future2, LatencyHistogram.NOOP);

        persistCache.put(pendingAdd1);
        persistCache.put(pendingAdd2);

        // flushFailed should attempt to fail all callbacks
        persistCache.flushFailed(new RuntimeException("test failure"));

        // pendingAdd1.onFailure() NPEs on null putLatencyRef before reaching release(),
        // so the extra reference from retain() in PendingAdd's constructor is still held.
        // Release it here to avoid leaking direct memory.
        pendingAdd1.release();

        // With the fix, the second callback should still be notified even though the first throws NPE
        assertTrue(future2.isDone(),
                "Second callback's future must be completed even when first callback's onFailure throws");
        assertTrue(future2.isCompletedExceptionally());
    }
}
