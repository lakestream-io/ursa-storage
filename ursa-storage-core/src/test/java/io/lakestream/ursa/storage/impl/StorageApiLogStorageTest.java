/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.storage.impl;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.lakestream.api.EntryHeader;
import io.lakestream.api.EntryIndex;
import io.lakestream.api.LogEntry;
import io.lakestream.api.LogId;
import io.lakestream.api.LogOffset;
import io.lakestream.api.Position;
import io.lakestream.ursa.storage.AddResult;
import io.lakestream.ursa.storage.Entry;
import io.lakestream.ursa.storage.StorageApi;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class StorageApiLogStorageTest {

    private StorageApi storageApi;
    private StorageApiLogStorage logStorage;

    @BeforeEach
    void setUp() {
        storageApi = mock(StorageApi.class);
        logStorage = new StorageApiLogStorage(storageApi);
    }

    @Test
    void testAppendDelegatesToStorageApi() throws Exception {
        LogId logId = LogId.of(123);
        ByteBuf data = Unpooled.wrappedBuffer(new byte[]{1, 2, 3});
        EntryHeader header = new EntryHeader(0, 5, 1000L, 3, 3);
        AddResult addResult = new AddResult(header, new Position("loc"), false);
        when(storageApi.append(eq(123L), eq(5), any(ByteBuf.class)))
            .thenReturn(CompletableFuture.completedFuture(addResult));

        logStorage.append(logId, 5, data).get();

        verify(storageApi).append(eq(123L), eq(5), any(ByteBuf.class));
    }

    @Test
    void testReadEntriesDelegatesToStorageApi() throws Exception {
        LogId logId = LogId.of(456);
        EntryHeader header = new EntryHeader(10, 3, 2000L, 100, 200);
        ByteBuf payload = Unpooled.wrappedBuffer(new byte[]{4, 5, 6});
        Entry entry = Entry.of(header, payload);

        when(storageApi.readEntries(eq(456L), eq(10L), eq(5), eq(1024)))
            .thenReturn(CompletableFuture.completedFuture(List.of(entry)));

        List<LogEntry> result = logStorage.readEntries(logId, 10L, 5, 1024L).get();

        assertEquals(1, result.size());
        LogEntry logEntry = result.get(0);
        assertEquals(10, logEntry.offset());
        assertEquals(3, logEntry.numberOfRecords());
        assertEquals(2000L, logEntry.timestamp());
        assertEquals(100, logEntry.size());
        assertEquals(1, payload.refCnt());

        logEntry.close();
        assertEquals(0, payload.refCnt());
        logEntry.close();
        assertEquals(0, payload.refCnt());
    }

    @Test
    void testGetFirstOffsetDelegatesToStorageApi() throws Exception {
        LogId logId = LogId.of(789);
        EntryHeader header = new EntryHeader(0, 1, 500L, 50, 50);
        EntryIndex entryIndex = new EntryIndex(header, new Position("loc"), 1, null,
            Optional.empty(), Optional.empty(), Optional.empty());

        when(storageApi.getFirstEntry(789L))
            .thenReturn(CompletableFuture.completedFuture(entryIndex));

        LogOffset offset = logStorage.getFirstOffset(logId).get();

        assertEquals(0, offset.offset());
        assertEquals(1, offset.numberOfRecords());
        assertEquals(500L, offset.timestamp());
    }

    @Test
    void testGetLastOffsetDelegatesToStorageApi() throws Exception {
        LogId logId = LogId.of(100);
        EntryHeader header = new EntryHeader(50, 10, 3000L, 200, 1000);
        EntryIndex entryIndex = new EntryIndex(header, new Position("loc"), 1, null,
            Optional.empty(), Optional.empty(), Optional.empty());

        when(storageApi.getLastEntry(100L))
            .thenReturn(CompletableFuture.completedFuture(entryIndex));

        LogOffset offset = logStorage.getLastOffset(logId).get();

        assertEquals(50, offset.offset());
        assertEquals(10, offset.numberOfRecords());
        assertEquals(3000L, offset.timestamp());
    }

    @Test
    void testSoftTrimDelegatesToStorageApi() throws Exception {
        LogId logId = LogId.of(200);
        when(storageApi.softTrimStream(200L, 50L))
            .thenReturn(CompletableFuture.completedFuture(51L));

        Long result = logStorage.softTrim(logId, 50L).get();

        assertEquals(51L, result);
        verify(storageApi).softTrimStream(200L, 50L);
    }

    @Test
    void testHardTrimDelegatesToStorageApi() throws Exception {
        LogId logId = LogId.of(300);
        when(storageApi.hardTrimStream(300L, 100L))
            .thenReturn(CompletableFuture.completedFuture(null));

        logStorage.hardTrim(logId, 100L).get();

        verify(storageApi).hardTrimStream(300L, 100L);
    }

    @Test
    void testDeleteLogDelegatesToStorageApi() throws Exception {
        LogId logId = LogId.of(400);
        when(storageApi.deleteStream(eq(400L), eq(Optional.empty())))
            .thenReturn(CompletableFuture.completedFuture(null));

        logStorage.deleteLog(logId).get();

        verify(storageApi).deleteStream(eq(400L), eq(Optional.empty()));
    }

    @Test
    void testReadEntriesMaxSizeClamp() throws Exception {
        LogId logId = LogId.of(500);
        when(storageApi.readEntries(eq(500L), eq(0L), eq(10), eq(Integer.MAX_VALUE)))
            .thenReturn(CompletableFuture.completedFuture(List.of()));

        // Pass a long value larger than Integer.MAX_VALUE to verify clamping
        logStorage.readEntries(logId, 0L, 10, Long.MAX_VALUE).get();

        verify(storageApi).readEntries(eq(500L), eq(0L), eq(10), eq(Integer.MAX_VALUE));
    }

    @Test
    void testEntryToLogEntry() {
        EntryHeader header = new EntryHeader(42, 7, 9999L, 256, 512);
        ByteBuf payload = Unpooled.wrappedBuffer(new byte[]{1, 2, 3, 4});
        Entry entry = Entry.of(header, payload);

        LogEntry logEntry = entry.toLogEntry();
        assertEquals(42, logEntry.offset());
        assertEquals(7, logEntry.numberOfRecords());
        assertEquals(9999L, logEntry.timestamp());
        assertEquals(256, logEntry.size());
        assertNotNull(logEntry.payload());
        assertEquals(4, logEntry.payload().readableBytes());
        assertTrue(logEntry.payload().isReadOnly());
        ByteBuf firstView = logEntry.payload();
        ByteBuf secondView = logEntry.payload();
        firstView.readByte();
        assertEquals(1, firstView.readerIndex());
        assertEquals(0, secondView.readerIndex());

        logEntry.close();
        assertEquals(0, payload.refCnt());
        logEntry.close();
        assertEquals(0, payload.refCnt());
    }

    @Test
    void testReadEntriesConversionFailureReleasesAllPayloads() {
        LogId logId = LogId.of(456);
        ByteBuf firstPayload = Unpooled.wrappedBuffer(new byte[]{1});
        ByteBuf trailingPayload = Unpooled.wrappedBuffer(new byte[]{2});
        Entry firstEntry = Entry.of(new EntryHeader(0, 1, 1000L, 1, 1), firstPayload);
        Entry trailingEntry = Entry.of(new EntryHeader(1, 1, 1001L, 1, 2), trailingPayload);
        when(storageApi.readEntries(eq(456L), eq(0L), eq(2), eq(1024)))
            .thenReturn(CompletableFuture.completedFuture(Arrays.asList(firstEntry, null, trailingEntry)));

        assertThrows(ExecutionException.class,
            () -> logStorage.readEntries(logId, 0L, 2, 1024L).get());

        assertEquals(0, firstPayload.refCnt());
        assertEquals(0, trailingPayload.refCnt());
    }

    @Test
    void testCanceledReadReleasesEntriesProducedLater() {
        LogId logId = LogId.of(789);
        CompletableFuture<List<Entry>> storageRead = new CompletableFuture<>();
        when(storageApi.readEntries(eq(789L), eq(0L), eq(1), eq(1024)))
            .thenReturn(storageRead);

        CompletableFuture<List<LogEntry>> callerRead =
            logStorage.readEntries(logId, 0L, 1, 1024L);
        assertTrue(callerRead.cancel(false));

        ByteBuf payload = Unpooled.wrappedBuffer(new byte[]{1});
        storageRead.complete(List.of(
            Entry.of(new EntryHeader(0, 1, 1000L, 1, 1), payload)));

        assertTrue(callerRead.isCancelled());
        assertEquals(0, payload.refCnt());
    }

    @Test
    void testReadIndexRangeDelegatesToStorageApi() throws Exception {
        LogId logId = LogId.of(700);
        EntryHeader header = new EntryHeader(10, 2, 1000L, 50, 100);
        EntryIndex entryIndex = new EntryIndex(header, new Position("loc"), 1, null,
            Optional.empty(), Optional.empty(), Optional.empty());

        when(storageApi.readIndexes(eq(700L), eq(10L), eq(20L)))
            .thenReturn(CompletableFuture.completedFuture(List.of(entryIndex)));

        List<EntryIndex> result = logStorage.readIndexRange(logId, 10L, 20L).get();

        assertEquals(1, result.size());
        assertEquals(10, result.get(0).header().offset());
        assertEquals(2, result.get(0).header().numberOfMessages());
        verify(storageApi).readIndexes(700L, 10L, 20L);
    }

    @Test
    void testErrorPropagation() {
        LogId logId = LogId.of(600);
        RuntimeException cause = new RuntimeException("test error");
        when(storageApi.getFirstEntry(600L))
            .thenReturn(CompletableFuture.failedFuture(cause));

        try {
            logStorage.getFirstOffset(logId).get();
        } catch (ExecutionException e) {
            assertEquals("test error", e.getCause().getMessage());
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
}
