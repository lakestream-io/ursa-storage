/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.storage;

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.lakestream.api.EntryHeader;
import io.lakestream.ursa.metrics.InstrumentProvider;
import io.lakestream.ursa.storage.StorageApi.StreamWriteLease;
import io.lakestream.ursa.storage.impl.PersistStorageApi;
import io.netty.buffer.Unpooled;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

@Slf4j
public class S3BackendPersistStorageApiTest {

    private UrsaStorageTestBase ursaStorageTestBase;
    private PersistStorageApi storage;
    private final Map<Long, StreamWriteLease> writeLeases = new ConcurrentHashMap<>();

    @BeforeEach
    public void setup() throws Exception {
        ursaStorageTestBase = new UrsaStorageTestBase();
        ursaStorageTestBase.setup();

        this.storage = ursaStorageTestBase.createStorageApi(InstrumentProvider.NOOP);
    }

    @AfterEach
    public void cleanup() throws Exception {
        try {
            writeLeases.values().forEach(StreamWriteLease::close);
            writeLeases.clear();
        } finally {
            ursaStorageTestBase.cleanup();
        }
    }

    private CompletableFuture<AddResult> appendWithLease(
            long streamId, int numberOfMessages, io.netty.buffer.ByteBuf data) {
        writeLeases.computeIfAbsent(
            streamId, id -> storage.acquireStreamWriteLease(id).join());
        return storage.append(streamId, numberOfMessages, data);
    }

    private void closeWriteLease(long streamId) {
        StreamWriteLease lease = writeLeases.get(streamId);
        if (lease != null) {
            lease.close();
            writeLeases.remove(streamId, lease);
        }
    }

    @Test
    public void testAsyncApi() throws Exception {
        final long streamId = 100;
        ArrayList<CompletableFuture<AddResult>> ehs = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            ehs.add(appendWithLease(streamId, 1, Unpooled.wrappedBuffer(("entry-" + i).getBytes())));
        }
        CompletableFuture.allOf(ehs.toArray(new CompletableFuture[0])).join();

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
            List<CompletableFuture<AddResult>> result = futures.computeIfAbsent(streamId, k -> new ArrayList<>());
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

        Entry e = storage.read(1, 0).join();
        assertEquals(eh1, e.header());
        assertEquals("entry-1", e.payload().toString(StandardCharsets.UTF_8));
        e.payload().release();

        eh = storage.readEntryHeader(1, 0).join();
        assertEquals(eh, eh1);

        eh = storage.readEntryHeader(5, 0).join();
        assertEquals(eh, EntryHeader.NOT_FOUND);

        eh = storage.getFirstEntry(5).join().header();
        assertEquals(EntryHeader.NOT_FOUND, eh);

        eh = storage.getLastEntry(6).join().header();
        assertEquals(EntryHeader.NOT_FOUND, eh);

        closeWriteLease(1);
        storage.deleteStream(1).join();
        eh = storage.getFirstEntry(1).join().header();
        assertEquals(EntryHeader.NOT_FOUND, eh);

        eh = storage.getLastEntry(1).join().header();
        assertEquals(EntryHeader.NOT_FOUND, eh);
    }

}
