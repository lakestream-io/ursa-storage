/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.storage.impl;

import static io.lakestream.ursa.storage.proto.IndexType.NORMAL;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import io.lakestream.api.EntryHeader;
import io.lakestream.api.EntryIndex;
import io.lakestream.api.LogId;
import io.lakestream.api.LogState;
import io.lakestream.api.Position;
import io.lakestream.api.exception.LogFencedException;
import io.lakestream.ursa.metrics.InstrumentProvider;
import io.lakestream.ursa.storage.AddResult;
import io.lakestream.ursa.storage.Entry;
import io.lakestream.ursa.storage.EntryList;
import io.lakestream.ursa.storage.FailureInjectedOxiaClient;
import io.lakestream.ursa.storage.FailureInjectedStorage;
import io.lakestream.ursa.storage.Key;
import io.lakestream.ursa.storage.StorageApi.StreamWriteLease;
import io.lakestream.ursa.storage.StreamProperties;
import io.lakestream.ursa.storage.UrsaStorageTestBase;
import io.lakestream.ursa.storage.Value;
import io.lakestream.ursa.storage.impl.exception.NoSuchOffsetException;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.oxia.client.api.options.GetOption;
import io.oxia.client.api.options.PutOption;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

@Slf4j
public class TestPersistStorageApi {

    private static final long WRITE_BUFFER_FLUSH_MS = 250;
    protected UrsaStorageTestBase ursaStorageTestBase;
    protected PersistStorageApi storage;
    protected FailureInjectedStorage failureInjectedStorage;
    protected FailureInjectedOxiaClient client;
    private final Map<Long, StreamWriteLease> writeLeases = new ConcurrentHashMap<>();

    @BeforeEach
    public void setup() throws Exception {
        var config = StorageConfig.builder().backendStorageType("local")
                .writeBufferFlushIntervalMs(WRITE_BUFFER_FLUSH_MS)
                .build();

        ursaStorageTestBase = new UrsaStorageTestBase();
        ursaStorageTestBase.setup(
                UrsaStorageTestBase.UrsaStorageTestConfig.builder()
                        .ursaConfig(config)
                        .build()
        );
        this.client = ursaStorageTestBase.getFailureInjectedOxiaClient();
        this.failureInjectedStorage = ursaStorageTestBase.getFailureInjectedStorage();
        this.storage = ursaStorageTestBase.createStorageApi(InstrumentProvider.NOOP);
    }

    @AfterEach
    public void cleanup() {
        try {
            writeLeases.values().forEach(StreamWriteLease::close);
            writeLeases.clear();
        } finally {
            ursaStorageTestBase.cleanup();
        }
    }

    private StreamWriteLease writeLease(long streamId) {
        return writeLeases.computeIfAbsent(
            streamId, id -> storage.acquireStreamWriteLease(id).join());
    }

    private CompletableFuture<AddResult> appendWithLease(
            long streamId, int numberOfMessages, ByteBuf data) {
        writeLease(streamId);
        return storage.append(streamId, numberOfMessages, data);
    }

    private CompletableFuture<AddResult> writeWithLease(
            long streamId, int numberOfMessages, long initialOffset,
            long cumulativeSize, ByteBuf data) {
        writeLease(streamId);
        return storage.write(
            streamId, numberOfMessages, initialOffset, cumulativeSize, data);
    }

    private CompletableFuture<Long> softTrimWithLease(
            long streamId, long offsetIncluded) {
        writeLease(streamId);
        return storage.softTrimStream(streamId, offsetIncluded);
    }

    private CompletableFuture<Void> compactIndexWithLease(
            long streamId, long startOffset, long endOffset,
            long endCumulativeSize, Value value) {
        writeLease(streamId);
        return storage.compactEntryIndex(
            streamId, startOffset, endOffset, endCumulativeSize, value);
    }

    private String randomString() {
        return "persist-storage-" + RandomStringUtils.random(4, true, false);
    }

    @Test
    public void testAsyncApi() throws Exception {
        final long streamId = storage.generateStreamId().get();
        ArrayList<CompletableFuture<AddResult>> ehs = new ArrayList<>();
        List<ByteBuf> list = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            ByteBuf byteBuf = Unpooled.wrappedBuffer(("entry-" + i).getBytes());
            list.add(byteBuf);
            ehs.add(appendWithLease(streamId, 1, byteBuf));
        }
        CompletableFuture.allOf(ehs.toArray(new CompletableFuture[0])).join();
        for (ByteBuf byteBuf : list) {
            byteBuf.release();
            Awaitility.await().untilAsserted(() -> {
                assertEquals(byteBuf.refCnt(), 0);
            });
        }

        for (int i = 0; i < ehs.size(); i++) {
            EntryHeader eh = ehs.get(i).join().header();
            assertEquals(i, eh.offset());
            assertEquals(1, eh.numberOfMessages());
        }

        // read all entries
        List<Entry> entries = storage.readEntries(streamId, 0, 100, Integer.MAX_VALUE).join();
        assertEquals(100, entries.size());
        for (int i = 0; i < entries.size(); i++) {
            Entry e = entries.get(i);
            assertEquals(i, e.header().offset());
            assertEquals(1, e.header().numberOfMessages());
            assertEquals("entry-" + i, e.payload().toString(StandardCharsets.UTF_8));
            e.payload().release();
        }
    }

    @Test
    public void testMixedStreamsWithAsyncApi() {
        Random random = new Random();
        Map<Long, AtomicLong> counter = new HashMap<>();
        Map<Long, List<CompletableFuture<AddResult>>> futures = new HashMap<>();
        for (int i = 0; i < 100; i++) {
            long streamId = random.nextInt(2);
            AtomicLong entryCounter = counter.computeIfAbsent(streamId, k -> new AtomicLong(0));
            List<CompletableFuture<AddResult>> result =
                    futures.computeIfAbsent(streamId, k -> new ArrayList<>());
            result.add(appendWithLease(streamId, 1,
                    Unpooled.wrappedBuffer(("entry-" + entryCounter.getAndIncrement()).getBytes())));
        }
        List<CompletableFuture<AddResult>> list = new ArrayList<>();
        for (List<CompletableFuture<AddResult>> value : futures.values()) {
            list.addAll(value);
        }
        CompletableFuture.allOf(list.toArray(new CompletableFuture[0])).join();

        Map<Long, AtomicLong> verifyCounter = new HashMap<>();
        Set<Long> keys = futures.keySet();
        for (Long key : keys) {
            for (CompletableFuture<AddResult> entryHeaderCompletableFuture : futures.get(key)) {
                EntryHeader eh = entryHeaderCompletableFuture.join().header();
                assertEquals(verifyCounter.computeIfAbsent(key, k -> new AtomicLong(0)).getAndIncrement(), eh.offset());
                assertEquals(1, eh.numberOfMessages());
            }
        }

        for (Long key : keys) {
            List<Entry> entries = storage.readEntries(key, 0, 100, Integer.MAX_VALUE).join();
            assertEquals(verifyCounter.get(key).get(), entries.size());
            for (int i = 0; i < entries.size(); i++) {
                Entry e = entries.get(i);
                assertEquals(i, e.header().offset());
                assertEquals(1, e.header().numberOfMessages());
                assertEquals("entry-" + i, e.payload().toString(StandardCharsets.UTF_8));
                e.payload().release();
            }
        }
    }

    @Test
    public void test() throws Exception {
        EntryHeader eh = storage.getFirstEntry(1).join().header();
        assertEquals(EntryHeader.NOT_FOUND, eh);

        eh = storage.getLastEntry(1).join().header();
        assertEquals(EntryHeader.NOT_FOUND, eh);

        EntryHeader eh1 = appendWithLease(1, 1, Unpooled.wrappedBuffer("entry-1".getBytes())).join().header();
        assertEquals(0, eh1.offset());
        assertEquals(7, eh1.cumulativeSize());
        assertEquals(1, eh1.numberOfMessages());

        EntryHeader eh2 = appendWithLease(1, 3, Unpooled.wrappedBuffer("entry-2".getBytes())).join().header();
        assertEquals(1, eh2.offset());
        assertEquals(14, eh2.cumulativeSize());
        assertEquals(3, eh2.numberOfMessages());

        eh = storage.getFirstEntry(1).join().header();
        assertEquals(eh1, eh);

        eh = storage.getLastEntry(1).join().header();
        assertEquals(eh2, eh);

        List<Entry> entries = storage.readEntries(1, 0, 100, 1024).join();
        assertEquals(2, entries.size());

        assertEquals(eh1, entries.get(0).header());
        assertEquals("entry-1", entries.get(0).payload().toString(StandardCharsets.UTF_8));
        assertEquals(eh2, entries.get(1).header());
        assertEquals("entry-2", entries.get(1).payload().toString(StandardCharsets.UTF_8));
        entries.forEach(e -> e.payload().release());

        Entry e = null;
        try {
            e = storage.read(1, 0).join();
            assertEquals(eh1, e.header());
            assertEquals("entry-1", e.payload().toString(StandardCharsets.UTF_8));
        } finally {
            if (e != null) {
                e.payload().release();
            }
        }

        eh = storage.readEntryHeader(1, 0).join();
        assertEquals(eh, eh1);

        eh = storage.readEntryHeader(5, 0).join();
        assertEquals(eh, EntryHeader.NOT_FOUND);

        eh = storage.getFirstEntry(5).join().header();
        assertEquals(EntryHeader.NOT_FOUND, eh);

        eh = storage.getLastEntry(6).join().header();
        assertEquals(EntryHeader.NOT_FOUND, eh);

        softTrimWithLease(1, 0).join();
        eh = storage.getFirstEntry(1).join().header();
        assertEquals(eh2, eh);

        storage.deleteStream(1).join();
        eh = storage.getFirstEntry(1).join().header();
        assertEquals(EntryHeader.NOT_FOUND, eh);

        eh = storage.getLastEntry(1).join().header();
        assertEquals(EntryHeader.NOT_FOUND, eh);
    }

    @Test
    public void testReadWithMaxLimitation() throws Exception {
        final long streamId = 1;

        // generate data
        int dataSet = 1000;
        generateData(streamId, dataSet);

        // read data by entries
        List<Entry> entries = storage.readEntries(streamId, 0, 10, 1000).get();
        verifyResult(entries, 0, 10);

        // read data by size
        entries = storage.readEntries(streamId, 0, 10, 10).get();
        verifyResult(entries, 0, 1);

        // read by entries with middle offset
        entries = storage.readEntries(streamId, 321, 10, 1000).get();
        verifyResult(entries, 321, 10);

        // read by size with middle offset
        entries = storage.readEntries(streamId, 321, 10, 15).get();
        verifyResult(entries, 321, 1);
    }

    private Pair<Long, Integer> generateData(long streamId, int numEntries) throws Exception {
        long differentStreamId = streamId + 1;
        int differentEntryIndex = 0;
        int numPartitions = 5;
        int partitionSize = numEntries / numPartitions;

        for (int partitionIndex = 0; partitionIndex < numPartitions; partitionIndex++) {
            List<CompletableFuture<AddResult>> appendResults = new ArrayList<>(numEntries);
            for (int i = 0; i < partitionSize; i++) {
                int numMessages = 1;
                ByteBuf payload =
                        Unpooled.wrappedBuffer(("message-" + (partitionIndex * partitionSize + i)).getBytes());
                appendResults.add(appendWithLease(streamId, numMessages, payload));
                if (partitionIndex % 2 == 0) {
                    ByteBuf payload2 = Unpooled.wrappedBuffer(("message-" + differentEntryIndex++).getBytes());
                    appendResults.add(appendWithLease(differentStreamId, numMessages, payload2));
                }
            }
            CompletableFuture.allOf(appendResults.toArray(new CompletableFuture[0])).get();
        }
        return Pair.of(differentStreamId, differentEntryIndex);
    }

    private void verifyResult(List<Entry> entries, int start, int expectedSize) {
        assertEquals(expectedSize, entries.size());
        for (int i = 0; i < entries.size(); i++) {
            Entry entry = entries.get(i);
            byte[] payload = new byte[entry.header().entrySize()];
            entry.payload().readBytes(payload);
            entry.payload().release();
            String message = new String(payload);
            String expectedMessage = "message-" + (start + i);
            assertEquals(expectedMessage, message);
        }
    }

    @Test
    public void testReadEntriesWithOxiaException() throws Exception {
        final long streamId = 1;

        // generate data
        int dataSet = 1000;
        generateData(streamId, dataSet);

        client.setFailureMode(true);
        try {
            storage.readEntries(streamId, 0, 10, 1000).get();
            fail();
        } catch (Exception e) {
            // expected
        }
    }

    @Test
    public void testReadEntriesWithStorageException() throws Exception {
        final long streamId = 1;

        // generate data
        int dataSet = 1000;
        generateData(streamId, dataSet);

        failureInjectedStorage.setFailureMode(true);
        try {
            storage.readEntries(streamId, 0, 10, 1000).get();
            fail();
        } catch (Exception e) {
            // expected
        }
    }

    @Test
    public void testReadEntriesWithPartReadFailure() throws Exception {
        final long streamId = 1;

        // generate data
        int dataSet = 1000;
        generateData(streamId, dataSet);

        // part read failure, it will return the success part
        failureInjectedStorage.setPartReadFailureMode(true);
        try {
            List<Entry> entries = storage.readEntries(streamId, 0, 10, 1000).get();
            verifyResult(entries, 0, entries.size());
        } catch (Exception e) {
            fail(e);
        }

        try {
            List<Entry> entries = storage.readEntries(streamId, 20, 10, 1000).get();
            verifyResult(entries, 20, entries.size());
        } catch (Exception e) {
            fail(e);
        }
    }

    @Test
    public void testReadEntry() {
        final long streamId = 1;

        ByteBuf data = Unpooled.wrappedBuffer("entry-1".getBytes());
        EntryHeader firstAppendedEntry = appendWithLease(streamId, 1, data).join().header();
        assertEquals(0, firstAppendedEntry.offset());

        ByteBuf data2 = Unpooled.wrappedBuffer("entry-2".getBytes());
        EntryHeader secondAppendedEntry = appendWithLease(streamId, 1, data2).join().header();
        assertEquals(1, secondAppendedEntry.offset());

        Entry firstEntry = storage.read(streamId, firstAppendedEntry.offset()).join();
        assertEquals(firstAppendedEntry, firstEntry.header());
        firstEntry.payload().release();

        Entry secondEntry = storage.read(streamId, secondAppendedEntry.offset()).join();
        assertEquals(secondAppendedEntry, secondEntry.header());
        secondEntry.payload().release();

        EntryHeader firstEntryHeader = storage.readEntryHeader(streamId, firstAppendedEntry.offset()).join();
        assertEquals(firstAppendedEntry, firstEntryHeader);

        EntryHeader secondEntryHeader = storage.readEntryHeader(streamId, secondAppendedEntry.offset()).join();
        assertEquals(secondAppendedEntry, secondEntryHeader);
    }

    @Test
    public void testReadEntriesByPositions() throws Exception {
        final long streamId = 1;
        final int numEntries = 5;
        List<ByteBuf> dataList = new ArrayList<>();
        List<EntryHeader> entryHeaders = new ArrayList<>();

        // Prepare and append multiple entries
        for (int i = 0; i < numEntries; i++) {
            ByteBuf data = Unpooled.wrappedBuffer(("entry-" + i).getBytes(StandardCharsets.UTF_8));
            dataList.add(data);
            EntryHeader header = appendWithLease(streamId, 1, data).join().header();
            entryHeaders.add(header);
            assertEquals(i, header.offset());
        }

        // Test reading entries one by one
        for (int i = 0; i < numEntries; i++) {
            List<EntryIndex> indices = storage.readIndexes(streamId, i, i + 1).get();
            assertEquals(1, indices.size());
            EntryList entryList = new EntryList(streamId);
            List<Entry> entries = storage.readEntries(indices, entryList).get();
            assertEquals(1, entries.size());

            Entry entry = entries.get(0);
            assertEquals(entryHeaders.get(i), entry.header());
            assertEquals(dataList.get(i), entry.payload());

            entry.payload().release();
        }

        // Test reading all entries at once
        List<EntryIndex> allPositions = storage.readIndexes(streamId, 0, numEntries).get();
        assertEquals(numEntries, allPositions.size());

        EntryList entryList = new EntryList(streamId);
        List<Entry> allEntries = storage.readEntries(allPositions, entryList).get();
        assertEquals(numEntries, allEntries.size());

        for (int i = 0; i < numEntries; i++) {
            Entry entry = allEntries.get(i);
            assertEquals(entryHeaders.get(i), entry.header());
            assertEquals(dataList.get(i), entry.payload());
            entry.payload().release();
        }

        // Clean up
        dataList.forEach(ByteBuf::release);
    }

    @Test
    public void generateAndDeleteEmptyStreamId() throws Exception {
        long streamId = storage.generateStreamId().get();
        storage.deleteStream(streamId).get();
    }

    @Test
    public void deleteLogIsIdempotentAcrossCrashReplay() throws Exception {
        long streamId = storage.generateStreamId().get();
        StorageApiLogStorage logStorage = new StorageApiLogStorage(storage);

        logStorage.deleteLog(LogId.of(streamId)).get();
        logStorage.deleteLog(LogId.of(streamId)).get();

        assertTrue(storage.listStreams().get().stream()
            .noneMatch(id -> id == streamId));
    }

    @Test
    public void generateAndDeleteNonEmptyStreamId() throws Exception {
        long streamId = storage.generateStreamId().get();
        generateData(1, 1);

        storage.deleteStream(streamId).get();
        Entry entry = storage.read(streamId, 0).get();
        assertNull(entry);
    }

    @Test
    public void testListAllStreams() throws Exception {
        Set<Long> generatedStreams = new HashSet<>();
        for (int j = 0; j < 2; j++) {
            boolean hasKey = j == 0;
            Set<Long> emptyStreams = storage.listStreams().get();
            assertEquals(generatedStreams.size(), emptyStreams.size());

            for (int i = 0; i < 10; i++) {
                if (hasKey) {
                    generatedStreams.add(storage.generateStreamId(Optional.of(randomString())).get());
                } else {
                    generatedStreams.add(storage.generateStreamId().get());
                }
            }

            assertEquals(10 * (j + 1), generatedStreams.size());

            Set<Long> listedStreams = new HashSet<>(storage.listStreams().get());
            assertEquals(generatedStreams, listedStreams);
        }
    }

    @Test
    public void testGetFirstCompactedPosition() throws Exception {
        long streamId = storage.generateStreamId().get();

        for (int i = 0; i < 10; i++) {
            appendWithLease(streamId, 1, Unpooled.wrappedBuffer(("entry-" + i).getBytes())).get();
        }

        Position position = storage.getFirstUnCompactedPosition(streamId).get();

        compactIndexWithLease(streamId, 0, 5, 10,
                new Value(1, 1, 1, NORMAL, new Position("test"))).get();
        Position p1 = storage.getFirstUnCompactedPosition(streamId).get();
        assertNotEquals(position, p1);
        Position p2 = storage.readEntryIndex(streamId, 6).get().position();
        assertEquals(p1, p2);

        compactIndexWithLease(streamId, 5, 10, 10,
                new Value(1, 1, 1, NORMAL, new Position("test"))).get();
        Position p3 = storage.getFirstUnCompactedPosition(streamId).get();
        assertEquals(Position.NOT_FOUND, p3);
    }

    @Test
    public void testGetFirstCompactedPositionWithUnknownStream() throws Exception {
        long streamId = -1;
        Position position = storage.getFirstUnCompactedPosition(streamId).get();
        assertEquals(Position.NOT_FOUND, position);
    }

    @Test
    public void testStreamIdGenerateDifferentStreamId() throws Exception {
        List<CompletableFuture<Long>> idsFuture = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            idsFuture.add(storage.generateStreamId(Optional.of(String.valueOf(i))));
        }
        CompletableFuture.allOf(idsFuture.toArray(new CompletableFuture[0])).get();
        Set<Long> ids = idsFuture.stream().map(CompletableFuture::join).collect(Collectors.toSet());
        assertEquals(100, ids.size());

        // generate with the same key again, the result should same
        List<CompletableFuture<Long>> sameIdsFuture = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            sameIdsFuture.add(storage.generateStreamId(Optional.of(String.valueOf(i))));
        }
        CompletableFuture.allOf(sameIdsFuture.toArray(new CompletableFuture[0])).get();
        Set<Long> sameIds = sameIdsFuture.stream().map(CompletableFuture::join).collect(Collectors.toSet());
        assertEquals(100, sameIds.size());
        assertEquals(
                ids.stream().sorted().collect(Collectors.toSet()),
                sameIds.stream().sorted().collect(Collectors.toSet()));

        // generate without key should also different with the generated key one
        List<CompletableFuture<Long>> idsFuture2 = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            idsFuture2.add(storage.generateStreamId());
            idsFuture2.add(storage.generateStreamId(Optional.of("key-" + i)));
        }
        CompletableFuture.allOf(idsFuture2.toArray(new CompletableFuture[0])).get();
        Set<Long> ids2 = idsFuture2.stream().map(CompletableFuture::join).collect(Collectors.toSet());
        assertEquals(200, ids2.size());

        ids.addAll(ids2);
        assertEquals(300, ids.size());
    }

    @Test
    public void testGenerateSameKeyMultipleTimesAtOneTime() throws Exception {
        List<CompletableFuture<Long>> idsFuture = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            idsFuture.add(storage.generateStreamId(Optional.of("key")));
        }
        CompletableFuture.allOf(idsFuture.toArray(new CompletableFuture[0])).get();
        Set<Long> ids = idsFuture.stream().map(CompletableFuture::join).collect(Collectors.toSet());
        assertEquals(1, ids.size());
    }

    @Test
    public void testReadEntries() throws Exception {
        EntryHeader eh = storage.getFirstEntry(1).join().header();
        assertEquals(EntryHeader.NOT_FOUND, eh);

        eh = storage.getLastEntry(1).join().header();
        assertEquals(EntryHeader.NOT_FOUND, eh);

        EntryHeader eh1 = appendWithLease(1, 1, Unpooled.wrappedBuffer("entry-1".getBytes())).join().header();
        assertEquals(0, eh1.offset());
        assertEquals(7, eh1.cumulativeSize());
        assertEquals(1, eh1.numberOfMessages());

        List<Entry> entries = storage.readEntries(1, 0, 100, 1024).join();
        assertEquals(1, entries.size());
        assertEquals(eh1, entries.get(0).header());
        assertEquals("entry-1", entries.get(0).payload().toString(StandardCharsets.UTF_8));
        entries.forEach(e -> e.payload().release());

        EntryHeader eh2 = appendWithLease(1, 100, Unpooled.wrappedBuffer("entry-2-100".getBytes())).join().header();
        assertEquals(1, eh2.offset());
        assertEquals(18, eh2.cumulativeSize());
        assertEquals(100, eh2.numberOfMessages());

        entries = storage.readEntries(1, 1, 30, 1024).join();
        assertEquals(1, entries.size());
        assertEquals(eh2, entries.get(0).header());
        assertEquals("entry-2-100", entries.get(0).payload().toString(StandardCharsets.UTF_8));
        entries.forEach(e -> e.payload().release());

        entries = storage.readEntries(1, 31, 50, 1024).join();
        assertEquals(1, entries.size());
        assertEquals(eh2, entries.get(0).header());
        assertEquals("entry-2-100", entries.get(0).payload().toString(StandardCharsets.UTF_8));
        entries.forEach(e -> e.payload().release());

        entries = storage.readEntries(1, 0, 50, 1024).join();
        assertEquals(1, entries.size());
        assertEquals(eh1, entries.get(0).header());
        assertEquals("entry-1", entries.get(0).payload().toString(StandardCharsets.UTF_8));
        entries.forEach(e -> e.payload().release());

        entries = storage.readEntries(1, 0, 100, 1024).join();
        assertEquals(1, entries.size());
        assertEquals(eh1, entries.get(0).header());
        assertEquals("entry-1", entries.get(0).payload().toString(StandardCharsets.UTF_8));
        entries.forEach(e -> e.payload().release());

        entries = storage.readEntries(1, 0, 101, 1024).join();
        assertEquals(2, entries.size());
        assertEquals(eh1, entries.get(0).header());
        assertEquals(eh2, entries.get(1).header());
        assertEquals("entry-1", entries.get(0).payload().toString(StandardCharsets.UTF_8));
        assertEquals("entry-2-100", entries.get(1).payload().toString(StandardCharsets.UTF_8));
        entries.forEach(e -> e.payload().release());

        entries = storage.readEntries(1, 0, 1, 1024).join();
        assertEquals(1, entries.size());
        assertEquals(eh1, entries.get(0).header());
        assertEquals("entry-1", entries.get(0).payload().toString(StandardCharsets.UTF_8));
        entries.forEach(e -> e.payload().release());

        entries = storage.readEntries(1, 1, 100, 1024).join();
        assertEquals(1, entries.size());
        assertEquals(eh2, entries.get(0).header());
        assertEquals("entry-2-100", entries.get(0).payload().toString(StandardCharsets.UTF_8));
        entries.forEach(e -> e.payload().release());
    }

    @Test
    public void testReadEntriesWithCompactedWAL() throws Exception {
        int numEntries = 10;
        long streamId = 1;
        var differentStreamIdAndEntryCount = generateData(streamId, numEntries);

        for (int maxEntries = 1; maxEntries <= numEntries; maxEntries++) {
            for (int start = 0; start <= numEntries; start++) {
                if (start == numEntries) {
                    try {
                        storage.readEntries(streamId, start, maxEntries, 1000).get();
                        fail();
                    } catch (Exception e) {
                        assertTrue(e.getCause() instanceof NoSuchOffsetException);
                        assertEquals("No such offset " + streamId + ":" + start, e.getCause().getMessage());
                    }
                } else {
                    var entries = storage.readEntries(streamId, start, maxEntries, 1000).get();
                    verifyResult(entries, start, Math.min(maxEntries, numEntries - start));
                }

            }
        }

        numEntries = differentStreamIdAndEntryCount.getRight();
        streamId = differentStreamIdAndEntryCount.getLeft();
        for (int maxEntries = 1; maxEntries <= numEntries; maxEntries++) {
            for (int start = 0; start <= numEntries; start++) {
                if (start == numEntries) {
                    try {
                        storage.readEntries(streamId, start, maxEntries, 1000).get();
                        fail();
                    } catch (Exception e) {
                        assertTrue(e.getCause() instanceof NoSuchOffsetException);
                        assertEquals("No such offset " + streamId + ":" + start, e.getCause().getMessage());
                    }
                } else {
                    var entries = storage.readEntries(streamId, start, maxEntries, 1000).get();
                    verifyResult(entries, start, Math.min(maxEntries, numEntries - start));
                }

            }
        }
    }


    @Test
    public void testAppendWithInitialOffset() throws Exception {
        long streamId = storage.generateStreamId().get();

        var testMsg = "test-entry-1";
        var data = Unpooled.wrappedBuffer(testMsg.getBytes());
        int numberOfMessages = 10;

        long lastEntryOffset = 10L; // lastEntryOffset represents the first offset of the last entry in the stream
        long cumulativeSize = data.readableBytes();
        writeWithLease(streamId, numberOfMessages, lastEntryOffset, cumulativeSize, data).join();
        // After this write: {streamId}-00000000000000000020-00000000000000000012
        assertEquals(10, storage.getFirstEntry(streamId).join().header().offset());
        assertEquals(10, storage.getLastEntry(streamId).join().header().offset());
        assertEquals(12, storage.getLastEntry(streamId).join().header().cumulativeSize());

        var entry = storage.read(streamId, 10).join();
        assertEquals(10, entry.header().offset());
        assertEquals(numberOfMessages, entry.header().numberOfMessages());
        assertEquals(testMsg, entry.payload().toString(StandardCharsets.UTF_8));

        // Write another message to make sure the cumulativeSize works correctly
        lastEntryOffset += numberOfMessages;
        cumulativeSize += data.readableBytes();
        writeWithLease(streamId, numberOfMessages, lastEntryOffset, cumulativeSize, data).join();
        // After this write: {streamId}-00000000000000000030-00000000000000000024
        assertEquals(20, storage.getLastEntry(streamId).join().header().offset());
        assertEquals(numberOfMessages, storage.getLastEntry(streamId).join().header().numberOfMessages());
        assertEquals(24, storage.getLastEntry(streamId).join().header().cumulativeSize());

        entry = storage.read(streamId, 20).join();
        assertEquals(20, entry.header().offset());
        assertEquals(numberOfMessages, entry.header().numberOfMessages());
        assertEquals(testMsg, entry.payload().toString(StandardCharsets.UTF_8));

        // Make sure that we can continue to append entries after the initial offset
        testMsg = "test-entry-2";
        lastEntryOffset += numberOfMessages;
        appendWithLease(streamId, 1, Unpooled.wrappedBuffer(testMsg.getBytes())).get();
        // After this append: {streamId}-00000000000000000031-00000000000000000036
        cumulativeSize += testMsg.getBytes().length;
        assertEquals(30, storage.getLastEntry(streamId).join().header().offset());
        assertEquals(1, storage.getLastEntry(streamId).join().header().numberOfMessages());
        assertEquals(36, storage.getLastEntry(streamId).join().header().cumulativeSize());

        entry = storage.read(streamId, 30).join();
        assertEquals(30, entry.header().offset());
        assertEquals(1, entry.header().numberOfMessages());
        assertEquals(testMsg, entry.payload().toString(StandardCharsets.UTF_8));

        data.release();
        data = Unpooled.wrappedBuffer(testMsg.getBytes());
        cumulativeSize += data.readableBytes();
        lastEntryOffset += 1;
        writeWithLease(streamId, numberOfMessages, lastEntryOffset, cumulativeSize, data);
        // After this write: {streamId}-00000000000000000041-00000000000000000048 (inflight)
        final var finalData = Unpooled.wrappedBuffer("test".getBytes());

        if (ursaStorageTestBase.getConfig().getUrsaConfig().getIndexSerializeFormatVersion() > 2) {
            var future = writeWithLease(streamId, numberOfMessages, 100, finalData.readableBytes(), finalData);
            var exception = assertThrows(CompletionException.class, () -> future.join());
            assertInstanceOf(IllegalArgumentException.class, exception.getCause());
            assertEquals("Invalid initial offset 100, expected 41", exception.getCause().getMessage());
        } else {
            var future = writeWithLease(streamId, numberOfMessages, 20, 24, finalData);
            var exception = assertThrows(CompletionException.class, () -> future.join());
            assertInstanceOf(io.oxia.client.api.exceptions.KeyAlreadyExistsException.class, exception.getCause());
        }

        var exception = assertThrows(
                CompletionException.class,
                () -> writeWithLease(streamId, numberOfMessages, 100, -1, finalData)
                        .join());
        assertInstanceOf(IllegalArgumentException.class, exception.getCause());
        assertEquals("cumulativeSize must be set if initialOffset is set", exception.getCause().getMessage());
        data.release();
    }

    @Test
    public void testFence() throws Exception {
        final var key0 = Optional.of("streams/testFence0/partition-0");
        final var streamId0 = storage.generateStreamId(key0).get();
        final var key1 = Optional.of("streams/testFence1/partition-0");
        final var streamId1 = storage.generateStreamId(key1).get();

        final Function<Long, CompletableFuture<AddResult>> append = streamId -> appendWithLease(streamId, 1,
                Unpooled.wrappedBuffer("msg".getBytes()));

        // the next flush will happen after WRITE_BUFFER_FLUSH_MS ms
        assertEquals(append.apply(streamId0).get(3, TimeUnit.SECONDS).header().offset(), 0L);
        final var appendFutures = new ArrayList<CompletableFuture<AddResult>>();
        for (int i = 0; i < 5; i++) {
            appendFutures.add(append.apply(streamId0));
            appendFutures.add(append.apply(streamId1));
        }
        Thread.sleep(WRITE_BUFFER_FLUSH_MS / 2);
        storage.getStreamStateManager().setState(streamId0, LogState.FENCED);
        // Pending append operations will eventually fail
        for (int i = 0; i < appendFutures.size(); i++) {
            try {
                final var result = appendFutures.get(i).get();
                if (i % 2 == 0) {
                    fail("future " + i + " succeeded");
                } else {
                    assertEquals(result.header().offset(), i / 2);
                }
            } catch (ExecutionException e) {
                assertInstanceOf(LogFencedException.class, e.getCause());
            }
        }
        // The new append operation will fail immediately
        try {
            append.apply(streamId0).getNow(null);
            fail();
        } catch (CompletionException e) {
            assertInstanceOf(LogFencedException.class, e.getCause());
        }

        assertEquals(streamId0, storage.generateStreamId(key0).get());
        assertEquals(LogState.FENCED, storage.getStreamStateManager().getState(streamId0));
        // A numeric stream ID is terminal once fenced, even if its keyed mapping is resolved again.
        ExecutionException fenced = assertThrows(
            ExecutionException.class,
            () -> append.apply(streamId0).get(3, TimeUnit.SECONDS));
        assertInstanceOf(LogFencedException.class, fenced.getCause());
    }

    @Test
    public void testMarkDeletedOffsetBasicUpdate() throws Exception {
        long streamId = storage.generateStreamId().get();
        // Should be -1 initially
        assertEquals(-1L, storage.getMarkDeletedOffsetWithVersion(streamId).get().getLeft());
        ByteBuf byteBuf = Unpooled.wrappedBuffer(("entry").getBytes());
        for (int i = 0; i < 3; i++) {
            appendWithLease(streamId, 100, byteBuf).get();
        }
        // Update to 100
        assertEquals(100L, softTrimWithLease(streamId, 99L).get());
        assertEquals(100L, storage.getMarkDeletedOffsetWithVersion(streamId).get().getLeft());
        // Should only move forward, not backward
        softTrimWithLease(streamId, 199L).get();
        assertEquals(200L, storage.getMarkDeletedOffsetWithVersion(streamId).get().getLeft());
        softTrimWithLease(streamId, 150L).get();
        assertEquals(200L, storage.getMarkDeletedOffsetWithVersion(streamId).get().getLeft());

        // Currently there are 3 entries in the stream: [0,99], [100,199], [200,299]
        // Ensure that truncateStreamHead will leave at least one entry in the stream. So truncate to the [299], the
        // markDeletedOffset will still be pointed to 200L to avoid cleaning the last entry [200,299].
        softTrimWithLease(streamId, 299L).get();
        assertEquals(200L, storage.getMarkDeletedOffsetWithVersion(streamId).get().getLeft());
    }

    @Test
    public void testMarkDeletedOffsetConcurrentUpdate() throws Exception {
        long streamId = storage.generateStreamId().get();
        List<CompletableFuture<Long>> updates = new ArrayList<>();
        ByteBuf byteBuf = Unpooled.wrappedBuffer(("entry").getBytes());
        for (int i = 0; i < 5; i++) {// [0,99] [100,199] [200, 299] [300,399] [400,499]
            appendWithLease(streamId, 100, byteBuf).get();
        }
        for (int i = 0; i < 3; i++) {
            updates.add(softTrimWithLease(streamId, i * 100L));
        }
        CompletableFuture.allOf(updates.toArray(new CompletableFuture[0])).get();
        // The final offset should be the highest one
        assertEquals(201L, storage.getMarkDeletedOffsetWithVersion(streamId).get().getLeft());
    }

    @Test
    public void testMarkDeletedOffsetNonExistentStream() throws Exception {
        // Should return -1 for non-existent stream
        assertEquals(-1L, storage.getMarkDeletedOffsetWithVersion(99999999L).get().getLeft());
    }

    @Test
    public void testTruncateStreamHeadConflicts() throws Exception {
        long streamId = storage.generateStreamId().get();
        ByteBuf byteBuf = Unpooled.wrappedBuffer(("entry").getBytes());
        for (int i = 0; i < 4; i++) {
            appendWithLease(streamId, 100, byteBuf).get();
        }
        // Set markDeletedOffset to 100
        assertEquals(100L, softTrimWithLease(streamId, 99L).get());
        // Manually update the markDeletedOffset to simulate a concurrent update (simulate version conflict)
        storage.getStorageOxiaClient().put(
                io.lakestream.ursa.storage.impl.StorageFormat.MARK_DELETED_OFFSET_PATH + "/" + streamId,
                "150".getBytes(StandardCharsets.UTF_8),
                Set.of(PutOption.PartitionKey(String.valueOf(streamId)))
        ).get();
        // Now try to set it back to 100, should not move backward
        assertEquals(150L, softTrimWithLease(streamId, 99L).get());
        assertEquals(150L, storage.getMarkDeletedOffsetWithVersion(streamId).get().getLeft());
        // Try to set it to 200, should succeed
        assertEquals(200L, softTrimWithLease(streamId, 199L).get());
        assertEquals(200L, storage.getMarkDeletedOffsetWithVersion(streamId).get().getLeft());
        // Idempotency: set to 200 again
        assertEquals(200L, softTrimWithLease(streamId, 199L).get());
        assertEquals(200L, storage.getMarkDeletedOffsetWithVersion(streamId).get().getLeft());
    }

    @Test
    public void testGetFirstEntry() throws Exception {
        long streamId = storage.generateStreamId().get();
        ByteBuf byteBuf = Unpooled.wrappedBuffer(("entry").getBytes());
        for (int i = 0; i < 5; i++) {
            appendWithLease(streamId, 100, byteBuf).get();
        }
        // Set markDeletedOffset to 200
        softTrimWithLease(streamId, 199L).get();

        // Get first entry including trimmed offset
        var firstEntry = storage.getFirstEntry(streamId, true).get();
        assertNotNull(firstEntry);
        assertEquals(0L, firstEntry.header().offset());
        assertEquals(100L, firstEntry.header().numberOfMessages());

        // Get first entry excluding trimmed offset
        firstEntry = storage.getFirstEntry(streamId, false).get();
        assertNotNull(firstEntry);
        assertEquals(200, firstEntry.header().offset());
        assertEquals(100L, firstEntry.header().numberOfMessages());
    }

    private void assertEntry(Entry entry, long expectedOffset, String expectedContent) {
        byte[] payload = new byte[entry.header().entrySize()];
        entry.payload().readBytes(payload);
        String message = new String(payload);
        assertEquals(expectedOffset, entry.header().offset());
        assertEquals(expectedContent, message);
    }

    @Test
    public void testReadEntriesAfterTrimmed() throws Exception {
        long streamId = storage.generateStreamId().get();
        final var entry1 = "entry-1";
        final var entry2 = "entry-2";
        final var entry3 = "entry-3";
        appendWithLease(streamId, 100, Unpooled.wrappedBuffer(entry1.getBytes())).get();
        appendWithLease(streamId, 100, Unpooled.wrappedBuffer(entry2.getBytes())).get();
        appendWithLease(streamId, 100, Unpooled.wrappedBuffer(entry3.getBytes())).get();
        softTrimWithLease(streamId, 99L).get();

        var entry = storage.read(streamId, 0).join();
        assertEntry(entry, 100L, entry2);

        var entryIndex = storage.readEntryIndex(streamId, 0).join();
        assertEquals(100L, entryIndex.header().offset());

        entry = storage.read(streamId, 0, entryIndex).join();
        assertEntry(entry, 100L, entry2);

        var firstEntryIndex = storage.getStorageFormat().getEntryIndex(
                storage.getStorageOxiaClient().get(Key.largestKey(streamId, 0L).toString(),
                        Set.of(GetOption.PartitionKey(String.valueOf(streamId)), GetOption.ComparisonHigher)).join());

        entry = storage.read(streamId, 0, firstEntryIndex, true).join();
        assertEntry(entry, 0L, entry1);

        var entries = storage.readEntries(streamId, 0L, 50, 10000).join();
        assertEquals(1, entries.size());
        assertEntry(entries.get(0), 100L, entry2);

        // Ensure readEntries can keep reading to meet maxMessageCount
        entries = storage.readEntries(streamId, 0L, 200, -1).join();
        assertEquals(2, entries.size());
        assertEntry(entries.get(0), 100L, entry2);
        assertEntry(entries.get(1), 200L, entry3);

        var entryHeader = storage.readEntryHeader(streamId, 0).join();
        assertEquals(100L, entryHeader.offset());

        var entryIndexes = storage.readIndexes(streamId, 0L, 100L).join();
        assertEquals(0, entryIndexes.size());

        entryIndexes = storage.readIndexes(streamId, 0L, 200L).join();
        assertEquals(1, entryIndexes.size());
        assertEquals(100L, entryIndexes.get(0).header().offset());

        entryIndexes = storage.readIndexes(streamId, 0L, 100L, true).join();
        assertEquals(1, entryIndexes.size());
        assertEquals(0, entryIndexes.get(0).header().offset());
    }

    @Test
    public void testListStreamsWithProperties() throws Exception {
        assertEquals(Collections.emptyMap(), storage.listStreamsWithProperties().get());
        Map<Long, StreamProperties> streamIdToKeyMap = Map.of(
                storage.generateStreamId().get(), new StreamProperties(null),
                storage.generateStreamId(Optional.of("topic1")).get(), new StreamProperties("topic1"),
                storage.generateStreamId(Optional.of("streams/test")).get(),
                new StreamProperties("streams/test")
        );

        var realStreamIdToKeyMap = storage.listStreamsWithProperties().get();
        assertEquals(streamIdToKeyMap, realStreamIdToKeyMap);
    }

}
