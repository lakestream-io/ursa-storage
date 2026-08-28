/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.lakestream.impl;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.lakestream.api.EntryHeader;
import io.lakestream.api.LogEntry;
import io.lakestream.api.LogId;
import io.lakestream.api.LogStorage;
import io.lakestream.api.StreamReader;
import io.lakestream.ursa.storage.Entry;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class StreamReaderImplTest {

    private LogStorage logStorage;
    private StreamReaderImpl reader;

    @BeforeEach
    void setUp() {
        logStorage = mock(LogStorage.class);
        List<LogId> logIds = List.of(LogId.of(101), LogId.of(102));
        IndexedLayout layout = new IndexedLayout(logIds);
        reader = new StreamReaderImpl(layout, logStorage);
    }

    @Test
    void testRead() throws Exception {
        LogId logId = LogId.of(101);
        ByteBuf payload = Unpooled.wrappedBuffer(new byte[]{1, 2, 3});
        LogEntry mockEntry = Entry.of(
            new EntryHeader(10, 3, 1000L, 100, 100), payload).toLogEntry();

        when(logStorage.readEntries(eq(logId), eq(10L), eq(5), eq(1024L)))
            .thenReturn(CompletableFuture.completedFuture(List.of(mockEntry)));

        StreamReader.ReadResult result = reader.read(logId, 10L, 5, 1024L).get();

        assertEquals(1, result.entries().size());
        assertEquals(13, result.nextOffset());
        result.entries().forEach(LogEntry::close);
        assertEquals(0, payload.refCnt());
    }

    @Test
    void testReadEmpty() throws Exception {
        LogId logId = LogId.of(101);
        when(logStorage.readEntries(eq(logId), eq(0L), eq(10), eq(4096L)))
            .thenReturn(CompletableFuture.completedFuture(List.of()));

        StreamReader.ReadResult result = reader.read(logId, 0L, 10, 4096L).get();

        assertEquals(0, result.entries().size());
        assertEquals(0, result.nextOffset());
    }

    @Test
    void testMetadataFailureClosesEntries() {
        LogId logId = LogId.of(101);
        LogEntry brokenEntry = mock(LogEntry.class);
        when(brokenEntry.offset()).thenThrow(new IllegalStateException("invalid metadata"));
        when(logStorage.readEntries(logId, 10L, 1, 1024L))
            .thenReturn(CompletableFuture.completedFuture(List.of(brokenEntry)));

        assertThrows(ExecutionException.class,
            () -> reader.read(logId, 10L, 1, 1024L).get());

        verify(brokenEntry).close();
    }

    @Test
    void testLogIds() throws Exception {
        List<LogId> logIds = reader.logIds().get();
        assertEquals(2, logIds.size());
        assertEquals(LogId.of(101), logIds.get(0));
        assertEquals(LogId.of(102), logIds.get(1));
    }

    @Test
    void testLayout() {
        assertNotNull(reader.layout());
        assertEquals(2, reader.layout().logCount());
    }

    @Test
    void testReadViaUnifiedReader() throws Exception {
        List<LogId> logIds = List.of(LogId.of(201), LogId.of(202));
        IndexedLayout unifiedLayout = new IndexedLayout(logIds);
        UnifiedStreamReader unifiedReader = mock(UnifiedStreamReader.class);
        StreamReaderImpl unifiedModeReader = new StreamReaderImpl(unifiedLayout, unifiedReader);

        LogId logId = LogId.of(201);
        Entry entry1 = Entry.of(
            new EntryHeader(5, 2, 1000L, 50, 50),
            Unpooled.wrappedBuffer(new byte[]{10, 20}));
        Entry entry2 = Entry.of(
            new EntryHeader(7, 3, 1001L, 60, 110),
            Unpooled.wrappedBuffer(new byte[]{30, 40}));

        UnifiedStreamReader.ReadResult unifiedResult =
            new UnifiedStreamReader.ReadResult(
                List.of(entry1.toLogEntry(), entry2.toLogEntry()), 10L);

        when(unifiedReader.readEntries(eq(logId), eq(5L), eq(100), eq(8192L)))
            .thenReturn(CompletableFuture.completedFuture(unifiedResult));

        StreamReader.ReadResult result = unifiedModeReader.read(logId, 5L, 100, 8192L).get();

        assertEquals(2, result.entries().size());
        assertEquals(5, result.entries().get(0).offset());
        assertEquals(2, result.entries().get(0).numberOfRecords());
        assertEquals(7, result.entries().get(1).offset());
        assertEquals(3, result.entries().get(1).numberOfRecords());
        assertEquals(10L, result.nextOffset());
        result.entries().forEach(LogEntry::close);
    }
}
