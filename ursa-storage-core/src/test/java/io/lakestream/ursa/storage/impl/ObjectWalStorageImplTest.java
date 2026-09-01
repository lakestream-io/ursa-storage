/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.storage.impl;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.atMost;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

import io.lakestream.api.EntryHeader;
import io.lakestream.api.EntryIndex;
import io.lakestream.api.Position;
import io.lakestream.ursa.metrics.InstrumentProvider;
import io.lakestream.ursa.storage.AddResult;
import io.lakestream.ursa.storage.Entry;
import io.lakestream.ursa.storage.EntryList;
import io.lakestream.ursa.storage.FailureInjectedOxiaClient;
import io.lakestream.ursa.storage.IDGenerator;
import io.lakestream.ursa.storage.Key;
import io.lakestream.ursa.storage.UrsaStorageTestBase;
import io.lakestream.ursa.storage.impl.exception.OperationRejectException;
import io.lakestream.ursa.utils.FutureUtils;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.buffer.Unpooled;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.Pair;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.provider.CsvSource;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;

@Slf4j
public class ObjectWalStorageImplTest {

    private UrsaStorageTestBase ursaStorageTestBase;
    private FailureInjectedOxiaClient client;
    private FailureInjectedFileStorage s3FileStorage;
    private FailureInjectedObjectWalStorageImpl simpleStorage;
    private StorageConfig config;
    private  PersistStorageApi persistStorageApi;
    private  StorageFormat format;

    @BeforeEach
    public void setup() throws Exception {
        config = StorageConfig.builder().backendStorageType("local").build();
        config.setWriteBufferSegment(2);
        config.setWriteBufferSize(512 * 1024);
        config.setWriteCacheEnabled(true);
        config.setMaxPendingAddRequestsUsedBytes(10 * 1024 * 1024);
        config.setWriteBufferFlushIntervalMs(10);

        ursaStorageTestBase = new UrsaStorageTestBase();
        ursaStorageTestBase.setup(
                UrsaStorageTestBase.UrsaStorageTestConfig.builder()
                        .ursaConfig(config)
                        .build()
        );
        this.client = ursaStorageTestBase.getFailureInjectedOxiaClient();
        this.format = new StorageFormat(config);
        IDGenerator generator = IDGenerator.create("memory", null, null);
        this.persistStorageApi = ursaStorageTestBase.createStorageApi(InstrumentProvider.NOOP);
        this.s3FileStorage = ursaStorageTestBase.getFailureInjectedFileStorage();
        this.s3FileStorage.setFailureMode(false);
        this.simpleStorage = new FailureInjectedObjectWalStorageImpl(PooledByteBufAllocator.DEFAULT, s3FileStorage,
                generator, config, InstrumentProvider.NOOP, client, format,
                ursaStorageTestBase.getStreamStateManager());
    }

    @AfterEach
    public void cleanup() {
        ursaStorageTestBase.cleanup();
    }

/*
    @BeforeEach
    @Override
    public void setup() throws Exception {
        super.setup();
        simpleStorage = setupStorage(c -> {});
    }

    private FailureInjectedObjectWalStorageImpl setupStorage(Consumer<StorageConfig> configModifier)
        throws Exception {

        config = new StorageConfig();
        config.setS3Region(localStack.getRegion());
        config.setS3Bucket(bucket);
        config.setCloudStorageEndpoint(localStack.getEndpoint().toString());
        config.setS3AccessKeyId(localStack.getAccessKey());
        config.setS3SecretAccessKey(localStack.getSecretKey());

        config.setWriteBufferSegment(2);
        config.setWriteBufferSize(512 * 1024);
        config.setWriteCacheEnabled(false);
        config.setMaxPendingAddRequestsUsedBytes(10 * 1024 * 1024);
        config.setWriteBufferFlushIntervalMs(10);
        configModifier.accept(config);


        IDGenerator generator = IDGenerator.create("memory", null, null);

    }

    @Override
    public void cleanup() throws Exception {
        try {
            if (simpleStorage != null) {
                simpleStorage.close();
            }
        } catch (Exception e) {
            log.error("Failed to close simple storage", e);
        }
        try {
            if (s3FileStorage != null) {
                s3FileStorage.close();
            }
        } catch (Exception e) {
            log.error("Failed to close s3 file storage", e);
        }
        super.cleanup();
    }*/

    private void initializeStorage() throws Exception {
        simpleStorage.initialize();
    }

    @Test
    void testAsyncWriteAndRead() throws Exception {
        initializeStorage();

        final long id = 1;
        List<CompletableFuture<AddResult>> results = new ArrayList<>();
        List<ByteBuf> dataSets = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            ByteBuf data = Unpooled.wrappedBuffer(("test-" + i).getBytes());
            dataSets.add(data);
            var putResult = simpleStorage.put(id, data);
            results.add(putResult);
        }

        CompletableFuture.allOf(results.toArray(new CompletableFuture[0])).get();

        // verify the data ref cnt
        for (ByteBuf data : dataSets) {
            Awaitility.await().untilAsserted(() -> assertEquals(1, data.refCnt()));
            data.release();
        }

        for (int i = 0; i < results.size(); i++) {
            var putResult = results.get(i).get();
            var header = putResult.header();
            if (header != null) {
                assertEquals(i, header.offset());
            } else {
                assertEquals(i, putResult.position().indexId());
            }
        }

        List<CompletableFuture<Entry>> readResults = new ArrayList<>();
        for (int i = 0; i < results.size(); i++) {
            var putResult = results.get(i).get();
            var index = prepareReadIndex(id, 1, dataSets.get(i).readableBytes(), putResult);
            if (putResult.position() == null) {
                readResults.add(simpleStorage.get(id, putResult.header().offset(), index));
            } else {
                readResults.add(simpleStorage.get(id, index));
            }
        }

        CompletableFuture.allOf(readResults.toArray(new CompletableFuture[0])).get();
        for (int i = 0; i < readResults.size(); i++) {
            ByteBuf data = readResults.get(i).get().payload();
            byte[] bytes = new byte[data.readableBytes()];
            data.readBytes(bytes);
            assertEquals("test-" + i, new String(bytes));
            data.release();
        }
    }

    @Test
    public void testSyncReadAndWrite() throws Exception {
        initializeStorage();

        final long id = 1;
        List<Pair<AddResult, ByteBuf>> dataSets = new ArrayList<>();
        LinkedHashMap<Long, EntryIndex> readIndices = new LinkedHashMap<>();
        for (int i = 0; i < 10; i++) {
            ByteBuf data = Unpooled.wrappedBuffer(("test-" + i).getBytes());
            var putResult = simpleStorage.put(id, data).get();
            prepareReadIndex(id, 1, data.readableBytes(), putResult, readIndices);
            dataSets.add(Pair.of(putResult, data));
        }

        for (var dataSet : dataSets) {
            Awaitility.await().untilAsserted(() -> assertEquals(1, dataSet.getRight().refCnt()));
            dataSet.getRight().release();
        }

        for (int i = 0; i < dataSets.size(); i++) {
            var index = dataSets.get(i).getLeft();
            if (index.position() != null) {
                assertEquals(0, index.position().indexId());
            } else {
                assertEquals(i, index.header().offset());
            }
        }

        for (int i = 0; i < dataSets.size(); i++) {
            var index = findIndex(i, readIndices.values());
            Entry entry;
            if (index.position().indexId() >= 0) {
                entry = simpleStorage.get(id, index).get();
            } else {
                entry = simpleStorage.get(id, i, index).get();
            }

            var data = entry.payload();
            byte[] bytes = new byte[data.readableBytes()];
            data.readBytes(bytes);
            assertEquals("test-" + i, new String(bytes));
            data.release();
        }
    }

    @Test
    public void testReadBatch() throws Exception {
        initializeStorage();

        final long id = 1;

        LinkedHashMap<Long, EntryIndex> readIndices = new LinkedHashMap<>();
        for (int i = 0; i < 10; i++) {
            ByteBuf data = Unpooled.wrappedBuffer(("test-" + i).getBytes());
            var index = simpleStorage.put(id, data).get();
            prepareReadIndex(id, 1, data.readableBytes(), index, readIndices);
            data.release();
        }

        var indices = new ArrayList<>(readIndices.values());

        var entryList = new EntryList(id);
        simpleStorage.get(indices, entryList).join();
        var readData = entryList.getEntries();
        for (int i = 0; i < readData.size(); i++) {
            var data = readData.get(i).payload();
            byte[] bytes = new byte[data.readableBytes()];
            data.readBytes(bytes);
            assertEquals("test-" + i, new String(bytes));
            data.release();
        }
    }

    @Test
    public void testPutWithStorageFailure() throws Exception {
        initializeStorage();

        final long id = 1;
        ByteBuf data = Unpooled.wrappedBuffer("test".getBytes());
        s3FileStorage.setFailureMode(true);

        try {
            simpleStorage.put(id, data).get();
            fail("Should fail");
        } catch (Exception e) {
            // expected an exception
        } finally {
            data.release();
        }
    }

    //@Test
    public void testGetWithStorageFailure() throws Exception {
        initializeStorage();

        final long id = 1;
        ByteBuf data = Unpooled.wrappedBuffer("test".getBytes());
        var putResult = simpleStorage.put(id, data).get();
        data.release();
        var index = prepareReadIndex(id, 1, data.readableBytes(), putResult);

        // wait for the cache is dropped by the cache
        TimeUnit.SECONDS.sleep(1);

        s3FileStorage.setFailureMode(true);
        try {
            simpleStorage.get(id, 0, index).get();
            fail("Should fail");
        } catch (Exception e) {
            assertEquals("Failed to get data from the location 0", e.getCause().getMessage());
        }
    }

    @Test
    public void testReadBatchWithStorageFailure() throws Exception {
        initializeStorage();

        final long id = 1;
        LinkedHashMap<Long, EntryIndex> readIndices = new LinkedHashMap<>();
        for (int i = 0; i < 10; i++) {
            ByteBuf data = Unpooled.wrappedBuffer(("test-" + i).getBytes());
            var index = simpleStorage.put(id, data).join();
            prepareReadIndex(id, 1, data.readableBytes(), index, readIndices);
            data.release();
        }

        s3FileStorage.setFailureMode(true);
        try {
            var entryList = new EntryList(id);
            simpleStorage.get(new ArrayList<>(readIndices.values()), entryList).get();
            fail("Should fail");
        } catch (Exception e) {
            assertEquals("Failed to get data from the location 0", e.getCause().getMessage());
        }
    }

    @Test
    public void testReadWithConvertPersistCacheToEntryListPaused() throws Exception {
        initializeStorage();
        final long id = 1;
        var entryList = new EntryList(id);
        AtomicReference<Throwable> exception = new AtomicReference<>();
        for (int i = 0; i < 9; i++) {
            ByteBuf data = Unpooled.wrappedBuffer(("test-" + i).getBytes());
            var index = simpleStorage.put(id, data).join();
            if (i == 0) {
                EntryIndex ei = prepareReadIndex(id, 1, data.readableBytes(), index);
                simpleStorage.pauseConvertPersistCacheToEntryList();
                new Thread(() -> simpleStorage.get(List.of(ei), entryList).exceptionally(ex -> {
                    exception.set(ex);
                    return null;
                })).start();
            }
            data.release();
        }
        simpleStorage.resumeConvertPersistCacheToEntryList();
        Awaitility.await().untilAsserted(() -> {
            assertNull(exception.get());
            Entry entry = entryList.get(0);
            assertEquals(0, entry.header().offset());
            var data = entryList.get(0).payload();
            byte[] bytes = new byte[data.readableBytes()];
            data.readBytes(bytes);
            assertEquals("test-0", new String(bytes));
        });
    }

    @Test
    public void testPartialIndexFailureOnlyFailsAffectedStreams() throws Exception {
        initializeStorage();

        long successStreamId = 1;
        long failStreamId = 2;

        // Configure per-stream failure: only failStreamId's index writes will fail
        client.addFailingStreamId(failStreamId);

        // Submit puts for both streams quickly so they end up in the same cache batch
        ByteBuf data1 = Unpooled.wrappedBuffer("success-data".getBytes());
        ByteBuf data2 = Unpooled.wrappedBuffer("fail-data".getBytes());
        CompletableFuture<AddResult> successFuture = simpleStorage.put(successStreamId, data1);
        CompletableFuture<AddResult> failFuture = simpleStorage.put(failStreamId, data2);
        data1.release();
        data2.release();

        // The successful stream's put should succeed
        AddResult successResult = successFuture.get(10, TimeUnit.SECONDS);
        assertTrue(successResult.header() != null || successResult.position() != null,
                "Successful stream should have a valid result");

        // The failed stream's put should fail
        ExecutionException ex = org.junit.jupiter.api.Assertions.assertThrows(
                ExecutionException.class, () -> failFuture.get(10, TimeUnit.SECONDS));
        assertTrue(ex.getCause().getMessage().contains("operation failed"),
                "Expected index failure cause, got: " + ex.getCause().getMessage());

        client.clearFailingStreamIds();
    }

    @Test
    public void testWriteCacheNotCachedWhenIndexFails() throws Exception {
        initializeStorage();
        client.setFailureMode(true);
        long streamId = 1;
        ByteBuf data = Unpooled.wrappedBuffer("test".getBytes());
        int entrySize = data.readableBytes();
        CompletableFuture<AddResult> future = simpleStorage.put(streamId, data);
        data.release();
        try {
            future.get();
            fail("Expected index failure");
        } catch (Exception e) {
            // expected
        }

        WriteCache writeCache = simpleStorage.getWriteCacheForTest();
        Awaitility.await().untilAsserted(() -> assertNull(writeCache.get("0")));

        EntryHeader header = new EntryHeader(0, 1, System.currentTimeMillis(), entrySize, entrySize);
        Position position = new Position("0", 0, Position.FileType.RAW);
        EntryIndex index = new EntryIndex(header, position, 1, EntryIndex.IndexType.COMPACT, Optional.empty());
        assertDoesNotThrow(() -> simpleStorage.get(streamId, 0, index).get());
    }

    @Test
    public void testReadFromBlobStorageAfterPartialIndexFailure() throws Exception {
        initializeStorage();

        long successStreamId = 1;
        long failStreamId = 2;

        // Configure per-stream failure: only failStreamId's index writes will fail
        client.addFailingStreamId(failStreamId);

        // Submit puts for both streams so they end up in the same cache batch
        ByteBuf data1 = Unpooled.wrappedBuffer("success-data".getBytes());
        ByteBuf data2 = Unpooled.wrappedBuffer("fail-data".getBytes());
        CompletableFuture<AddResult> successFuture = simpleStorage.put(successStreamId, data1);
        CompletableFuture<AddResult> failFuture = simpleStorage.put(failStreamId, data2);
        data1.release();
        data2.release();

        // The successful stream's put should succeed
        AddResult successResult = successFuture.get(10, TimeUnit.SECONDS);

        // The failed stream's put should fail
        org.junit.jupiter.api.Assertions.assertThrows(
                ExecutionException.class, () -> failFuture.get(10, TimeUnit.SECONDS));

        // Write cache should NOT be populated because one stream's index failed
        WriteCache writeCache = simpleStorage.getWriteCacheForTest();
        Awaitility.await().atMost(5, TimeUnit.SECONDS).untilAsserted(() -> {
            // The cache entry for this location should not be in the write cache
            String location = successResult.position() != null
                    ? successResult.position().location()
                    : "0";
            assertNull(writeCache.get(location),
                    "Write cache should not hold entries when any index write failed");
        });

        // Clear the failure so reads through the index path work normally
        client.clearFailingStreamIds();

        // Read the successful stream's data — it must come from blob storage (not write cache)
        var readIndex = prepareReadIndex(successStreamId, 1, "success-data".length(), successResult);
        Entry entry;
        if (readIndex.position().indexId() >= 0) {
            entry = simpleStorage.get(successStreamId, readIndex).get(10, TimeUnit.SECONDS);
        } else {
            entry = simpleStorage.get(successStreamId, 0, readIndex).get(10, TimeUnit.SECONDS);
        }
        ByteBuf payload = entry.payload();
        byte[] bytes = new byte[payload.readableBytes()];
        payload.readBytes(bytes);
        assertEquals("success-data", new String(bytes),
                "Data should be readable from blob storage after partial index failure");
        payload.release();
    }

    private void prepareReadIndex(long streamId, int numberOfMessages, int entrySize,
                                  AddResult addResult, LinkedHashMap<Long, EntryIndex> readIndices) {

        var index = prepareReadIndex(streamId, numberOfMessages, entrySize, addResult);
        readIndices.put(index.header().offset(), index);
    }

    private EntryIndex prepareReadIndex(long streamId, int numberOfMessages, int entrySize,
                                        AddResult addResult) {

        if (addResult.position() == null) {
            return persistStorageApi.readEntryIndex(streamId, addResult.header().offset()).join();
        } else {
            var indexResult = persistStorageApi.withStreamWriteLease(streamId, ignoredLease ->
                persistStorageApi.writeNonCompactedIndex(
                    streamId, numberOfMessages, entrySize, addResult.position())).join();
            // single entry index
            Key k = Key.parse(indexResult.key());
            return persistStorageApi.readEntryIndex(streamId, k.offset() - numberOfMessages).join();
        }
    }

    private EntryIndex findIndex(long offset, Collection<EntryIndex> indices) {
        for (var e : indices) {
            var start = e.header().offset();
            var end = e.header().offset() + e.header().numberOfMessages();
            if (start <= offset && offset < end) {
                return e;
            }
        }
        throw new IllegalStateException("index not found");
    }

    @Test
    public void testReadUnknownPosition() throws Exception {
        initializeStorage();

        final long id = 1;
        var index = new EntryIndex(null, new Position("test", 0, Position.FileType.RAW), 1, EntryIndex.IndexType.NORMAL,
                Optional.empty());
        try {
            simpleStorage.get(id, 0, index).get();
            fail("Should fail");
        } catch (Exception e) {
            assertTrue(e.getCause() instanceof NoSuchKeyException | e.getCause() instanceof FileNotFoundException);
        }
    }

    @Test
    public void callApiWithoutInitialize() {
        try {
            simpleStorage.put(1, 1, Unpooled.wrappedBuffer("test".getBytes())).get();
            fail("Should fail");
        } catch (Exception e) {
            assertEquals("WalStorage is not initialized", e.getCause().getMessage());
        }
        var index = new EntryIndex(null, new Position("test", 0, Position.FileType.RAW), 1, EntryIndex.IndexType.NORMAL,
                Optional.empty());
        try {
            simpleStorage.get(1, 0, index).get();
            fail("Should fail");
        } catch (Exception e) {
            assertEquals("WalStorage is not initialized", e.getCause().getMessage());
        }

        try {
            var entryList = new EntryList(1);
            simpleStorage.get(new ArrayList<>(), entryList).get();
            fail("Should fail");
        } catch (Exception e) {
            assertEquals("WalStorage is not initialized", e.getCause().getMessage());
        }

        try {
            simpleStorage.delete(1, new ArrayList<>()).get();
            fail("Should fail");
        } catch (Exception e) {
            assertEquals("WalStorage is not initialized", e.getCause().getMessage());
        }
    }

    @Test
    public void testMessageSizeExceedsMaxSize() throws Exception {
        initializeStorage();

        final long id = 1;
        ByteBuf data = Unpooled.wrappedBuffer(new byte[config.getWriteBufferSize() + 1]);
        try {
            simpleStorage.put(id, data).get();
        } catch (Exception e) {
            // expected exception
        } finally {
            data.release();
        }
        Awaitility.await().untilAsserted(() -> assertEquals(0, data.refCnt()));
    }

    @Test
    public void testOperationRejection() throws Exception {
        simpleStorage.stopProcessing();
        initializeStorage();

        final long id = 1;
        List<CompletableFuture<AddResult>> futures = new ArrayList<>();
        List<ByteBuf> dataSets = new ArrayList<>();

        // Try to put 15 messages, which is more than the queue size of 10
        byte[] bytes = new byte[1024 * 1024];
        new Random().nextBytes(bytes);
        ByteBuf content = Unpooled.wrappedBuffer(bytes);
        for (int i = 0; i < 15; i++) {
            dataSets.add(content.retain());
            futures.add(simpleStorage.put(id, content));
        }

        int progressingCount = 0;
        int rejectCount = 0;
        for (CompletableFuture<AddResult> future : futures) {
            if (!future.isDone()) {
                progressingCount++;
            } else if (future.isCompletedExceptionally()) {
                try {
                    future.get();
                } catch (Exception e) {
                    if (e.getCause() instanceof OperationRejectException) {
                        rejectCount++;
                    } else {
                        fail("Unexpected exception: " + e.getCause());
                    }
                }
            }
        }

        // We expect 10 successful operations and 5 rejections
        assertEquals(10, progressingCount, "Expected 10 successful operations");
        assertEquals(5, rejectCount, "Expected 5 rejected operations");

        simpleStorage.continueProcessing();

        CompletableFuture.allOf(futures.stream().filter(f -> !f.isDone()).toArray(CompletableFuture[]::new)).get();

        // Clean up
        for (ByteBuf data : dataSets) {
            data.release();
        }
    }

    @Test
    public void testPreFetch() throws Exception {
        initializeStorage();

        final long id = 1;
        LinkedHashMap<Long, EntryIndex> readIndices = new LinkedHashMap<>();

        // Create and store some entries first
        for (int i = 0; i < 5; i++) {
            ByteBuf data = Unpooled.wrappedBuffer(("test-" + i).getBytes());
            var index = simpleStorage.put(id, data).get();
            prepareReadIndex(id, 1, data.readableBytes(), index, readIndices);
            data.release();
        }

        var indices = new ArrayList<>(readIndices.values());

        // Verify preFetch behavior
        simpleStorage.preFetch(id,
                indices.stream().map(EntryIndex::position).collect(Collectors.toList()));

        // Wait a short time to ensure cache is populated
        TimeUnit.MILLISECONDS.sleep(1000);

        // Force S3 to fail to verify we're reading from cache
        s3FileStorage.setFailureMode(true);

        // Verify that entries are now in read cache by reading them
        for (int i = 0; i < 5; i++) {
            var index = findIndex(i, indices);
            Entry entry;
            if (index.position().indexId() >= 0) {
                entry = simpleStorage.get(id, index).get();
            } else {
                entry = simpleStorage.get(id, i, index).get();
            }

            ByteBuf data = entry.payload();
            byte[] bytes = new byte[data.readableBytes()];
            data.readBytes(bytes);
            assertTrue(new String(bytes).startsWith("test-"));
            data.release();
        }

        // Test preFetch with non-existent positions
        List<Position> nonExistentPositions = new ArrayList<>();
        nonExistentPositions.add(new Position("non-existent", 0, Position.FileType.RAW));
        simpleStorage.preFetch(id, nonExistentPositions);
        // No exception should be thrown, failures should be logged
    }

    @Test
    public void testLargeEntryWriteAndRead() throws Exception {
        initializeStorage();

        final long id = 1;
        // Create data larger than write buffer size but smaller than max size
        int dataSize = config.getWriteBufferSize() * 2;
        final int numEntries = 3;
        LinkedHashMap<Long, EntryIndex> readIndices = new LinkedHashMap<>();

        // Write 3 large entries that together exceed the write buffer sizeInBytes
        for (int i = 0; i < numEntries; i++) {
            byte[] largeData = new byte[dataSize];
            // Fill with unique pattern for verification
            for (int j = 0; j < dataSize; j++) {
                largeData[j] = (byte) ((i + j) % 256);
            }
            ByteBuf data = Unpooled.wrappedBuffer(largeData);
            var index = simpleStorage.put(id, data).get();
            prepareReadIndex(id, 1, data.readableBytes(), index, readIndices);
            data.release();
        }

        // Read and verify each entry
        for (int i = 0; i < numEntries; i++) {
            var index = findIndex(i, readIndices.values());
            Entry entry;
            if (index.position().indexId() >= 0) {
                entry = simpleStorage.get(id, index).get();
            } else {
                entry = simpleStorage.get(id, i, index).get();
            }

            ByteBuf readData = entry.payload();
            assertEquals(dataSize, readData.readableBytes());
            byte[] readBytes = new byte[dataSize];
            readData.readBytes(readBytes);
            // Verify the pattern
            for (int j = 0; j < dataSize; j++) {
                assertEquals((byte) ((i + j) % 256), readBytes[j],
                    "Mismatch at entry " + i + ", position " + j);
            }
            readData.release();
        }
    }

    @Test
    public void testReleaseBufferBeforeResponse() throws Exception {
        initializeStorage();

        // Test successful case
        final long id = 1;
        ByteBuf data = Unpooled.wrappedBuffer("test-data".getBytes());
        CompletableFuture<AddResult> future = simpleStorage.put(id, data);

        // Release buffer before waiting for response
        data.release();
        future.get();
        Awaitility.await().untilAsserted(() -> assertEquals(0, data.refCnt()));

        // Test S3 failure case
        ByteBuf rejectedData = Unpooled.wrappedBuffer("rejected-data".getBytes());
        s3FileStorage.setFailureMode(true);
        CompletableFuture<AddResult> rejectedFuture = simpleStorage.put(id, rejectedData);

        // Release buffer before waiting for response
        rejectedData.release();
        try {
            rejectedFuture.get();
            fail("Should fail due to storage failure");
        } catch (Exception e) {
            // Expected failure
            Awaitility.await().untilAsserted(() -> assertEquals(0, rejectedData.refCnt()));
        }

        ObjectWalStorageImpl spyStorage = spy(simpleStorage);
        doReturn(false).when(spyStorage).addPendingRequest(any());

        // Test reject by queue case using spy
        ByteBuf queueRejectedData = Unpooled.wrappedBuffer("queue-rejected-data".getBytes());
        CompletableFuture<AddResult> queueRejectedFuture = spyStorage.put(id, queueRejectedData);

        // Release buffer before waiting for response
        queueRejectedData.release();
        try {
            queueRejectedFuture.get();
            fail("Should fail due to queue rejection");
        } catch (Exception e) {
            assertTrue(e.getCause() instanceof OperationRejectException);
            Awaitility.await().untilAsserted(() -> assertEquals(0, queueRejectedData.refCnt()));
            verify(spyStorage).addPendingRequest(any());
        }
    }

    @Test
    @Disabled
    @CsvSource({"2", "3"})
    public void testReadRequestLimit(String version) throws Exception {
        config.setReadCacheMemorySize(config.getWriteBufferSize() * 2L);

        IDGenerator generator = IDGenerator.create("memory", null, null);
        @Cleanup
        ObjectWalStorageImpl storage =
                new FailureInjectedObjectWalStorageImpl(PooledByteBufAllocator.DEFAULT, s3FileStorage,
                        generator, config, InstrumentProvider.NOOP, client, format,
                        ursaStorageTestBase.getStreamStateManager());
        storage.initialize();

        long id = 1;
        LinkedHashMap<Long, EntryIndex> readIndices = new LinkedHashMap<>();
        for (int i = 0; i < 10; i++) {
            ByteBuf data = Unpooled.wrappedBuffer(("test-" + i).getBytes());
            var index = storage.put(id, data).get();
            prepareReadIndex(id, 1, data.readableBytes(), index, readIndices);
            data.release();
        }

        List<EntryIndex> indices = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            indices.add(findIndex(i, readIndices.values()));
        }

        List<CompletableFuture<Entry>> getFutures = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            var index = indices.get(i);
            CompletableFuture<Entry> entry;
            if (index.position().indexId() >= 0) {
                entry = storage.get(id, index);
            } else {
                entry = storage.get(id, i, index);
            }
            getFutures.add(entry);
        }

        Awaitility.await().untilAsserted(() -> {
            List<CompletableFuture<Entry>> failedFuture = getFutures.stream()
                    .filter(f -> f.isDone() && f.isCompletedExceptionally()).toList();
            if (!version.equals("3")) {
                assertNotEquals(0, failedFuture.size());
            } else {
                // v3 limitation will depend on the size, instead of the numbers
                assertEquals(0, failedFuture.size());
            }
        });
        getFutures.stream().filter(f -> f.isDone() && !f.isCompletedExceptionally())
                .map(CompletableFuture::join).map(Entry::payload).forEach(ByteBuf::release);

        assertEquals(0, storage.getPendingReadRequests());

        TimeUnit.SECONDS.sleep(1);

        // get one by one will succeed all
        for (int i = 0; i < 10; i++) {
            var index = findIndex(i, readIndices.values());
            if (index.position().indexId() >= 0) {
                storage.get(id, index).get();
            } else {
                storage.get(id, i, index).get();
            }
            TimeUnit.MILLISECONDS.sleep(300);
        }
        assertEquals(0, storage.getPendingReadRequests());
    }

    @Test
    public void testWriteAndReadOnMultipleStreams() throws Exception {
        config.setMaxPendingAddRequestsUsedBytes(10 * 1024 * 1024);
        config.setWriteBufferFlushIntervalMs(250);
        config.setWriteBufferMaxStreamIds(2);
        @Cleanup
        io.lakestream.ursa.storage.FileStorage s3FileStorageSpy = spy(s3FileStorage);
        IDGenerator generator = IDGenerator.create("memory", null, null);
        @Cleanup
        ObjectWalStorageImpl simpleStorage =
                new FailureInjectedObjectWalStorageImpl(PooledByteBufAllocator.DEFAULT, s3FileStorageSpy,
                        generator, config, InstrumentProvider.NOOP, client, format,
                        ursaStorageTestBase.getStreamStateManager());
        simpleStorage.initialize();
        int numStreams = 10;
        int numMessagesForEachStream = 5;
        List<LinkedHashMap<Long, EntryIndex>> readIndicesList = new ArrayList<>();
        List<CompletableFuture<Void>> futures = new ArrayList<>();
        for (int i = 0; i < numStreams; i++) {
            readIndicesList.add(new LinkedHashMap<>());
        }

        for (int j = 0; j < numMessagesForEachStream; j++) {
            for (int i = 0; i < numStreams; i++) {
                var readIndices = readIndicesList.get(i);
                ByteBuf data = Unpooled.wrappedBuffer(("test-" + j).getBytes());
                int finalI = i;
                CompletableFuture<Void> future = simpleStorage.put(i, data).thenAccept(index ->
                        prepareReadIndex(finalI, 1, data.readableBytes(), index, readIndices));
                futures.add(future);
            }
        }

        FutureUtils.waitForAll(futures).join();

        verify(s3FileStorageSpy, atLeast(5)).putAsync(any(ByteBuf.class), anyString());
        verify(s3FileStorageSpy, atMost(10)).putAsync(any(ByteBuf.class), anyString());
        for (int j = 0; j < numMessagesForEachStream; j++) {
            for (int i = 0; i < numStreams; i++) {
                var readIndices = readIndicesList.get(i);
                var index = findIndex(j, readIndices.values());
                Entry entry;
                if (index.position().indexId() >= 0) {
                    entry = simpleStorage.get(i, index).get();
                } else {
                    entry = simpleStorage.get(i, j, index).get();
                }
                ByteBuf data = entry.payload();
                byte[] bytes = new byte[data.readableBytes()];
                data.readBytes(bytes);
                assertEquals("test-" + j, new String(bytes));
                data.release();
            }
        }

        simpleStorage.close();
    }

    @Test
    public void testAddPendingRequestToctouRaceCondition() throws Exception {
        // Create a storage with a small limit to make the race easy to trigger
        long maxPendingBytes = 100;
        config.setMaxPendingAddRequestsUsedBytes(maxPendingBytes);
        IDGenerator generator = IDGenerator.create("memory", null, null);
        @Cleanup
        ObjectWalStorageImpl storage =
                new FailureInjectedObjectWalStorageImpl(PooledByteBufAllocator.DEFAULT, s3FileStorage,
                        generator, config, InstrumentProvider.NOOP, client, format,
                        ursaStorageTestBase.getStreamStateManager());

        AtomicLong pendingSize = storage.getPendingAddRequestsDataSizeForTest();

        int threadCount = 50;
        int bufferSize = 60; // Each buffer is 60 bytes; limit is 100, so at most 1 should be accepted
        CyclicBarrier barrier = new CyclicBarrier(threadCount);
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        AtomicInteger acceptedCount = new AtomicInteger(0);
        AtomicReference<Throwable> firstError = new AtomicReference<>();

        List<ByteBuf> buffers = new ArrayList<>();
        for (int i = 0; i < threadCount; i++) {
            buffers.add(Unpooled.wrappedBuffer(new byte[bufferSize]));
        }

        java.util.concurrent.BlockingQueue<PendingAdd> queue =
                storage.getPendingAddRequestsForTest();

        try {
            for (int i = 0; i < threadCount; i++) {
                final int idx = i;
                executor.submit(() -> {
                    try {
                        barrier.await(); // Maximize contention
                        PendingAdd pendingAdd = new PendingAdd(1, 1, buffers.get(idx),
                                new CompletableFuture<>(), null);
                        boolean added = storage.addPendingRequest(pendingAdd);
                        if (added) {
                            acceptedCount.incrementAndGet();
                        } else {
                            pendingAdd.release();
                        }
                    } catch (InterruptedException | java.util.concurrent.BrokenBarrierException e) {
                        // Expected from barrier
                    } catch (Exception e) {
                        firstError.compareAndSet(null, e);
                    }
                });
            }

            executor.shutdown();
            assertTrue(executor.awaitTermination(10, TimeUnit.SECONDS));

            // Fail if any thread hit an unexpected exception
            assertNull(firstError.get(),
                    "Unexpected exception in worker thread: " + firstError.get());

            // At least one request must be accepted (guards against always-reject bugs)
            assertTrue(acceptedCount.get() >= 1,
                    "At least one request should have been accepted");

            // The invariant: total accepted data size must not exceed the limit.
            // Each accepted request is 60 bytes and the limit is 100, so at most 1 can be accepted.
            long totalAcceptedSize = (long) acceptedCount.get() * bufferSize;
            assertTrue(totalAcceptedSize <= maxPendingBytes,
                    "Total accepted size (" + totalAcceptedSize + ") exceeded limit (" + maxPendingBytes
                            + "). " + acceptedCount.get() + " requests were accepted but at most "
                            + (maxPendingBytes / bufferSize) + " should have been.");

            // Counter must exactly match what was accepted
            assertEquals(pendingSize.get(), totalAcceptedSize,
                    "pendingAddRequestsDataSize should exactly match accepted requests");
        } finally {
            // Release accepted PendingAdd items still in the queue
            List<PendingAdd> remaining = new ArrayList<>();
            queue.drainTo(remaining);
            for (PendingAdd pa : remaining) {
                pa.release();
            }
            // Release the original buffer references
            for (ByteBuf buf : buffers) {
                if (buf.refCnt() > 0) {
                    buf.release();
                }
            }
        }
    }

    @Test
    public void testPutPathCounterConsistency() throws Exception {
        // Use a small limit so that concurrent puts cause rejections
        long maxPendingBytes = 100;
        config.setMaxPendingAddRequestsUsedBytes(maxPendingBytes);
        initializeStorage();

        AtomicLong pendingSize = simpleStorage.getPendingAddRequestsDataSizeForTest();

        int threadCount = 50;
        int bufferSize = 60;
        CyclicBarrier barrier = new CyclicBarrier(threadCount);
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        AtomicInteger acceptedCount = new AtomicInteger(0);
        AtomicInteger rejectedCount = new AtomicInteger(0);

        for (int i = 0; i < threadCount; i++) {
            executor.submit(() -> {
                try {
                    barrier.await();
                    ByteBuf data = Unpooled.wrappedBuffer(new byte[bufferSize]);
                    CompletableFuture<AddResult> future = simpleStorage.put(1, data);
                    data.release();
                    future.get();
                    acceptedCount.incrementAndGet();
                } catch (InterruptedException | java.util.concurrent.BrokenBarrierException e) {
                    // Expected from barrier
                } catch (ExecutionException e) {
                    if (e.getCause() instanceof OperationRejectException) {
                        rejectedCount.incrementAndGet();
                    }
                }
            });
        }

        executor.shutdown();
        assertTrue(executor.awaitTermination(30, TimeUnit.SECONDS));

        // Wait for the processor to flush all accepted requests
        Awaitility.await().atMost(30, TimeUnit.SECONDS).untilAsserted(
                () -> assertEquals(0, pendingSize.get(),
                        "All accepted requests should be flushed"));

        assertTrue(acceptedCount.get() >= 1,
                "At least one request should have been accepted through put()");
        assertEquals(threadCount, acceptedCount.get() + rejectedCount.get(),
                "Every thread should either succeed or be rejected");
    }
}
