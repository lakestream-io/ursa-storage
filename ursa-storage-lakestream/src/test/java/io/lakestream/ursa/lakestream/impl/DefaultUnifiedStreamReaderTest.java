/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.lakestream.impl;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import io.lakestream.api.EntryHeader;
import io.lakestream.api.EntryIndex;
import io.lakestream.api.LogEntry;
import io.lakestream.api.LogId;
import io.lakestream.api.Position;
import io.lakestream.ursa.lakestream.reader.CompactedObjectReader;
import io.lakestream.ursa.storage.Entry;
import io.lakestream.ursa.storage.StorageApi;
import io.lakestream.ursa.storage.impl.EntryIndexCache;
import io.netty.buffer.Unpooled;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class DefaultUnifiedStreamReaderTest {

    @Mock
    private StorageApi storageApi;

    @Mock
    private CompactedObjectReader compactedReader;

    @Mock
    private EntryIndexCache entryIndexCache;

    private DefaultUnifiedStreamReader reader;

    @BeforeEach
    void setUp() {
        reader = new DefaultUnifiedStreamReader(storageApi, compactedReader, entryIndexCache);
    }

    @Test
    void testReadFromRaw() throws Exception {
        LogId logId = LogId.of(101);
        long startOffset = 10;
        EntryHeader header = new EntryHeader(10, 3, 1000L, 100, 500);
        Position position = new Position("wal-file-1", -1, Position.FileType.RAW);
        EntryIndex entryIndex = new EntryIndex(header, position, 1, EntryIndex.IndexType.NORMAL,
            Optional.empty(), Optional.empty(), Optional.empty());

        when(entryIndexCache.get(101L, 10L))
            .thenReturn(CompletableFuture.completedFuture(entryIndex));

        Entry entry = Entry.of(header, Unpooled.wrappedBuffer(new byte[]{1, 2, 3}));
        when(storageApi.readEntries(eq(101L), eq(10L), eq(5), eq(1024)))
            .thenReturn(CompletableFuture.completedFuture(List.of(entry)));

        UnifiedStreamReader.ReadResult result = reader.readEntries(logId, startOffset, 5, 1024L).get();

        assertEquals(1, result.entries().size());
        assertEquals(13, result.nextOffset()); // 10 + 3
        verifyNoInteractions(compactedReader);
        result.entries().forEach(LogEntry::close);
    }

    @Test
    void testReadFromParquet() throws Exception {
        LogId logId = LogId.of(202);
        long startOffset = 50;
        EntryHeader header = new EntryHeader(50, 10, 2000L, 500, 1000);
        Position position = new Position("parquet-file-1", 0, Position.FileType.PARQUET);
        EntryIndex entryIndex = new EntryIndex(header, position, 1, EntryIndex.IndexType.COMPACT,
            Optional.empty(), Optional.empty(), Optional.empty());

        when(entryIndexCache.get(202L, 50L))
            .thenReturn(CompletableFuture.completedFuture(entryIndex));

        Entry parquetEntry = Entry.of(
            new EntryHeader(50, 10, 2000L, 500, 1000),
            Unpooled.wrappedBuffer(new byte[]{4, 5, 6}));
        CompactedObjectReader.ReadResult readResult =
            new CompactedObjectReader.ReadResult(false, List.of(parquetEntry.toLogEntry()));

        when(compactedReader.readMessagesWithEntryIndexAsync(
            eq(entryIndex), eq(50L), eq(50L), eq(5L), eq(1024L)))
            .thenReturn(CompletableFuture.completedFuture(readResult));

        UnifiedStreamReader.ReadResult result = reader.readEntries(logId, startOffset, 5, 1024L).get();

        assertEquals(1, result.entries().size());
        assertEquals(60, result.nextOffset()); // 50 + 10
        verify(compactedReader).readMessagesWithEntryIndexAsync(
            eq(entryIndex), eq(50L), eq(50L), eq(5L), eq(1024L));
        result.entries().forEach(LogEntry::close);
    }

    @Test
    void testReadFromParquetV2MultipleEntries() throws Exception {
        LogId logId = LogId.of(303);
        long startOffset = 100;
        EntryHeader header = new EntryHeader(100, 20, 3000L, 800, 2000);
        Position position = new Position("parquet-file-2", 0, Position.FileType.PARQUET);
        EntryIndex entryIndex = new EntryIndex(header, position, 1, EntryIndex.IndexType.COMPACT,
            Optional.empty(), Optional.empty(), Optional.empty());

        when(entryIndexCache.get(303L, 100L))
            .thenReturn(CompletableFuture.completedFuture(entryIndex));

        Entry entry1 = Entry.of(
            new EntryHeader(100, 5, 3000L, 200, 200),
            Unpooled.wrappedBuffer(new byte[]{1}));
        Entry entry2 = Entry.of(
            new EntryHeader(105, 5, 3001L, 200, 400),
            Unpooled.wrappedBuffer(new byte[]{2}));
        CompactedObjectReader.ReadResult readResult =
            new CompactedObjectReader.ReadResult(true,
                List.of(entry1.toLogEntry(), entry2.toLogEntry()));

        when(compactedReader.readMessagesWithEntryIndexAsync(
            eq(entryIndex), eq(100L), eq(100L), eq(10L), eq(2048L)))
            .thenReturn(CompletableFuture.completedFuture(readResult));

        UnifiedStreamReader.ReadResult result = reader.readEntries(logId, startOffset, 10, 2048L).get();

        assertEquals(2, result.entries().size());
        assertEquals(110, result.nextOffset()); // 105 + 5
        result.entries().forEach(LogEntry::close);
    }

    @Test
    void testMetadataFailureClosesCompactedEntries() {
        LogId logId = LogId.of(304);
        long startOffset = 100;
        EntryHeader header = new EntryHeader(100, 1, 3000L, 10, 10);
        EntryIndex entryIndex = new EntryIndex(
            header,
            new Position("parquet-file-3", 0, Position.FileType.PARQUET),
            1,
            EntryIndex.IndexType.COMPACT,
            Optional.empty(),
            Optional.empty(),
            Optional.empty());
        LogEntry brokenEntry = mock(LogEntry.class);
        when(brokenEntry.offset()).thenThrow(new IllegalStateException("invalid metadata"));
        when(entryIndexCache.get(logId.id(), startOffset))
            .thenReturn(CompletableFuture.completedFuture(entryIndex));
        when(compactedReader.readMessagesWithEntryIndexAsync(
            entryIndex, startOffset, startOffset, 1L, 1024L))
            .thenReturn(CompletableFuture.completedFuture(
                new CompactedObjectReader.ReadResult(true, List.of(brokenEntry))));

        assertThrows(ExecutionException.class,
            () -> reader.readEntries(logId, startOffset, 1, 1024L).get());

        verify(brokenEntry).close();
    }

    @Test
    void testReadNotFoundIndex() throws Exception {
        LogId logId = LogId.of(404);
        when(entryIndexCache.get(404L, 0L))
            .thenReturn(CompletableFuture.completedFuture(EntryIndex.NOT_FOUND));

        UnifiedStreamReader.ReadResult result = reader.readEntries(logId, 0L, 10, 4096L).get();

        assertTrue(result.entries().isEmpty());
        assertEquals(0, result.nextOffset());
        verifyNoInteractions(storageApi);
        verifyNoInteractions(compactedReader);
    }

    @Test
    void testReadNullIndex() throws Exception {
        LogId logId = LogId.of(500);
        when(entryIndexCache.get(500L, 0L))
            .thenReturn(CompletableFuture.completedFuture(null));

        UnifiedStreamReader.ReadResult result = reader.readEntries(logId, 0L, 10, 4096L).get();

        assertTrue(result.entries().isEmpty());
        assertEquals(0, result.nextOffset());
    }

    @Test
    void testReadRawEmpty() throws Exception {
        LogId logId = LogId.of(600);
        EntryHeader header = new EntryHeader(0, 1, 1000L, 10, 10);
        Position position = new Position("wal-file", -1, Position.FileType.RAW);
        EntryIndex entryIndex = new EntryIndex(header, position, 1, EntryIndex.IndexType.NORMAL,
            Optional.empty(), Optional.empty(), Optional.empty());

        when(entryIndexCache.get(600L, 0L))
            .thenReturn(CompletableFuture.completedFuture(entryIndex));
        when(storageApi.readEntries(eq(600L), eq(0L), eq(10), eq(4096)))
            .thenReturn(CompletableFuture.completedFuture(List.of()));

        UnifiedStreamReader.ReadResult result = reader.readEntries(logId, 0L, 10, 4096L).get();

        assertTrue(result.entries().isEmpty());
        assertEquals(0, result.nextOffset());
    }

    @Test
    void testMaxSizeBytesClamping() throws Exception {
        LogId logId = LogId.of(700);
        EntryHeader header = new EntryHeader(0, 1, 1000L, 10, 10);
        Position position = new Position("wal-file", -1, Position.FileType.RAW);
        EntryIndex entryIndex = new EntryIndex(header, position, 1, EntryIndex.IndexType.NORMAL,
            Optional.empty(), Optional.empty(), Optional.empty());

        when(entryIndexCache.get(700L, 0L))
            .thenReturn(CompletableFuture.completedFuture(entryIndex));
        // maxSizeBytes exceeds Integer.MAX_VALUE — should clamp to Integer.MAX_VALUE
        when(storageApi.readEntries(eq(700L), eq(0L), eq(10), eq(Integer.MAX_VALUE)))
            .thenReturn(CompletableFuture.completedFuture(List.of()));

        reader.readEntries(logId, 0L, 10, Long.MAX_VALUE).get();

        verify(storageApi).readEntries(700L, 0L, 10, Integer.MAX_VALUE);
    }

    @Test
    void testClose() throws Exception {
        reader.close();
        verify(compactedReader).close();
    }
}
