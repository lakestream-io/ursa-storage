/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.lakestream.impl;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.lakestream.api.EntryHeader;
import io.lakestream.api.EntryIndex;
import io.lakestream.api.FileInfo;
import io.lakestream.api.Log;
import io.lakestream.api.LogEntry;
import io.lakestream.api.LogId;
import io.lakestream.api.LogOffset;
import io.lakestream.api.LogStorage;
import io.lakestream.api.Position;
import io.lakestream.ursa.lakestream.reader.CompactedObjectReader;
import io.lakestream.ursa.storage.Entry;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.function.Predicate;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class LogCursorImplTest {

    @Mock
    private Log log;
    @Mock
    private LogStorage logStorage;

    private LogCursorImpl cursor;
    private final LogId logId = LogId.of(100);

    @BeforeEach
    void setUp() {
        lenient().when(log.id()).thenReturn(logId);
        lenient().when(log.logStorage()).thenReturn(logStorage);
        lenient().when(log.getLastOffset()).thenReturn(CompletableFuture.completedFuture(
            new LogOffset(1000, 1, System.currentTimeMillis(), 100, 10000)));
        lenient().when(log.getEntryIndex(anyLong())).thenReturn(
            CompletableFuture.completedFuture(makeRawEntryIndex(0, 1)));
        lenient().when(log.readIndexRange(anyLong(), anyLong())).thenReturn(
            CompletableFuture.completedFuture(List.of()));
        // Default: readEntries returns empty
        lenient().when(log.readEntries(anyLong(), anyInt(), anyLong())).thenReturn(
            CompletableFuture.completedFuture(List.of()));
        // Default: preFetchEntries is no-op (void method, no stub needed)

        cursor = new LogCursorImpl("test-cursor", log, 0, -1L);
    }

    // --- Basic read ---

    @Test
    void testReadEntriesFallbackToLogOnEmptyCache() throws Exception {
        LogEntry entry = mockLogEntry(0, 1, 50);
        when(log.readEntries(eq(0L), anyInt(), anyLong()))
            .thenReturn(CompletableFuture.completedFuture(List.of(entry)));

        List<LogEntry> result = cursor.readEntries(10, Long.MAX_VALUE).get();

        assertEquals(1, result.size());
        assertEquals(0, result.get(0).offset());
        assertEquals(1, cursor.readOffset());
    }

    @Test
    void testReadEntriesWithPrefetchCache() throws Exception {
        EntryIndex idx0 = makeRawEntryIndex(0, 1);
        EntryIndex idx1 = makeRawEntryIndex(1, 1);
        cursor.getPrefetchedIndexes().add(idx0);
        cursor.getPrefetchedIndexes().add(idx1);
        cursor.setPrefetchedMessageCount(2);

        LogEntry entry0 = mockLogEntry(0, 1, 50);
        LogEntry entry1 = mockLogEntry(1, 1, 50);
        when(logStorage.readEntriesByIndex(eq(logId), any(), anyLong(), anyLong(),
            anyInt(), anyLong(), any(), any()))
            .thenReturn(CompletableFuture.completedFuture(List.of(entry0, entry1)));

        List<LogEntry> result = cursor.readEntries(10, Long.MAX_VALUE).get();

        assertEquals(2, result.size());
        assertEquals(2, cursor.readOffset());
    }

    @Test
    void testRawPrefetchPassesDerivedMessageCount() throws Exception {
        EntryIndex idx0 = makeRawEntryIndex(0, 3);
        EntryIndex idx1 = makeRawEntryIndex(3, 7);
        cursor.getPrefetchedIndexes().add(idx0);
        cursor.getPrefetchedIndexes().add(idx1);
        cursor.setPrefetchedMessageCount(10);

        LogEntry entry0 = mockLogEntry(0, 3, 50);
        LogEntry entry1 = mockLogEntry(3, 7, 50);
        when(logStorage.readEntriesByIndex(eq(logId), any(), anyLong(), anyLong(),
            eq(10), anyLong(), any(), any()))
            .thenReturn(CompletableFuture.completedFuture(List.of(entry0, entry1)));

        List<LogEntry> result = cursor.readEntries(2, Long.MAX_VALUE).get();

        assertEquals(2, result.size());
        assertEquals(10, cursor.readOffset());
        verify(logStorage).readEntriesByIndex(eq(logId), any(), eq(0L), eq(Long.MAX_VALUE),
            eq(10), eq(Long.MAX_VALUE), any(), any());
    }

    // --- Non-binary detection ---

    @Test
    void testPollSkipsNonBinaryEntries() throws Exception {
        EntryIndex parquetIdx = makeParquetEntryIndex(0, 10);
        cursor.getPrefetchedIndexes().add(parquetIdx);
        cursor.setPrefetchedMessageCount(10);

        LogEntry entry = mockLogEntry(0, 1, 50);
        when(log.readEntries(eq(0L), anyInt(), anyLong()))
            .thenReturn(CompletableFuture.completedFuture(List.of(entry)));

        List<LogEntry> result = cursor.readEntries(10, Long.MAX_VALUE).get();

        assertEquals(1, result.size());
        assertTrue(cursor.getPrefetchedIndexes().isEmpty());
    }

    // --- Deleted entry skip ---

    @Test
    void testPollSkipsDeletedEntries() throws Exception {
        cursor.individualDelete(0, 1).get();

        EntryIndex idx0 = makeRawEntryIndex(0, 1);
        EntryIndex idx1 = makeRawEntryIndex(1, 1);
        cursor.getPrefetchedIndexes().add(idx0);
        cursor.getPrefetchedIndexes().add(idx1);
        cursor.setPrefetchedMessageCount(2);

        LogEntry entry1 = mockLogEntry(1, 1, 50);
        when(logStorage.readEntriesByIndex(eq(logId), any(), anyLong(), anyLong(),
            anyInt(), anyLong(), any(), any()))
            .thenReturn(CompletableFuture.completedFuture(List.of(entry1)));

        List<LogEntry> result = cursor.readEntries(10, Long.MAX_VALUE).get();

        assertEquals(1, result.size());
        assertEquals(1, result.get(0).offset());
    }

    // --- Size limit ---

    @Test
    void testPollRespectsMaxSize() throws Exception {
        // Two entries of 100 bytes each. MaxSize = 100 → poll should only take first entry
        // (push back second), then top-up reads zero more since size exhausted
        EntryIndex idx0 = makeRawEntryIndex(0, 1, 100);
        EntryIndex idx1 = makeRawEntryIndex(1, 1, 100);
        cursor.getPrefetchedIndexes().add(idx0);
        cursor.getPrefetchedIndexes().add(idx1);
        cursor.setPrefetchedMessageCount(2);

        LogEntry entry0 = mockLogEntry(0, 1, 100);
        when(logStorage.readEntriesByIndex(eq(logId), any(), anyLong(), anyLong(),
            anyInt(), anyLong(), any(), any()))
            .thenReturn(CompletableFuture.completedFuture(List.of(entry0)));

        // MaxSize = 100: first entry fills it, second should be pushed back
        List<LogEntry> result = cursor.readEntries(10, 100).get();

        assertEquals(1, result.size());
        // Second entry should still be in the cache (pushed back)
        assertEquals(1, cursor.getPrefetchedIndexes().size());
    }

    @Test
    void testReadReturnsFirstEntryWhenMaxSizeIsSmallerThanEntry() throws Exception {
        cursor.setNextReadIndex(CompletableFuture.completedFuture(makeParquetEntryIndex(0, 50)));

        LogEntry entry = mockLogEntry(0, 50, 500);
        when(log.readEntries(eq(0L), eq(50), anyLong()))
            .thenReturn(CompletableFuture.completedFuture(List.of(entry)));

        List<LogEntry> result = cursor.readEntries(10, 1).get();

        assertEquals(1, result.size());
        assertEquals(50, cursor.readOffset());
        verify(log).readEntries(eq(0L), eq(50), eq(1L));
    }

    // --- preFilterEntries ---

    @Test
    void testPreFilterWithSkipCondition() throws Exception {
        Predicate<Long> skipLow = offset -> offset < 2;

        LogEntry entry2 = mockLogEntry(2, 1, 50);
        when(log.getEntryIndex(eq(2L))).thenReturn(
            CompletableFuture.completedFuture(makeRawEntryIndex(2, 1)));
        when(log.readEntries(eq(2L), anyInt(), anyLong()))
            .thenReturn(CompletableFuture.completedFuture(List.of(entry2)));

        List<LogEntry> result = cursor.readEntries(10, Long.MAX_VALUE,
            skipLow, 100).get();

        assertTrue(cursor.readOffset() >= 2);
    }

    @Test
    void testPreFilterAllDeleted() throws Exception {
        cursor.markDelete(99, Collections.emptyMap()).get();

        List<LogEntry> result = cursor.readEntries(10, Long.MAX_VALUE,
            null, 110).get();

        assertEquals(0, result.size());
        assertTrue(cursor.readOffset() >= 100);
    }

    @Test
    void testPreFilterAllSkippedAdvancesToScannedBound() throws Exception {
        Predicate<Long> skipAll = offset -> true;

        List<LogEntry> result = cursor.readEntries(2, Long.MAX_VALUE, skipAll, 50).get();

        assertEquals(0, result.size());
        assertEquals(50, cursor.readOffset());
        verify(log, never()).readEntries(anyLong(), anyInt(), anyLong());
    }

    // --- Error handling ---

    @Test
    void testErrorClearsPrefetchAndInvalidatesCache() throws Exception {
        cursor.getPrefetchedIndexes().add(makeRawEntryIndex(0, 1));
        cursor.setPrefetchedMessageCount(1);

        when(logStorage.readEntriesByIndex(eq(logId), any(), anyLong(), anyLong(),
            anyInt(), anyLong(), any(), any()))
            .thenReturn(CompletableFuture.failedFuture(new RuntimeException("storage error")));

        try {
            cursor.readEntries(10, Long.MAX_VALUE).get();
        } catch (ExecutionException e) {
            // Expected
        }

        assertTrue(cursor.getPrefetchedIndexes().isEmpty());
        assertEquals(0, cursor.getPrefetchedMessageCount());
        verify(log).invalidateCache();
    }

    @Test
    void testFallbackBudgetSelectionFailureCompletesFutureExceptionally() {
        RuntimeException failure = new RuntimeException("index failure");
        when(log.getEntryIndex(eq(0L))).thenReturn(CompletableFuture.failedFuture(failure));

        CompletableFuture<List<LogEntry>> future = cursor.readEntries(10, Long.MAX_VALUE);

        assertThrows(ExecutionException.class, future::get);
        verify(log).invalidateCache();
    }

    @Test
    void testFilteredEntryIsClosedWhileAcceptedEntriesRemainOwnedByCaller() throws Exception {
        ByteBuf firstPayload = Unpooled.wrappedBuffer(new byte[]{1});
        ByteBuf filteredPayload = Unpooled.wrappedBuffer(new byte[]{2});
        ByteBuf thirdPayload = Unpooled.wrappedBuffer(new byte[]{3});
        LogEntry first = ownedLogEntry(0, 1, firstPayload);
        LogEntry filtered = ownedLogEntry(1, 1, filteredPayload);
        LogEntry third = ownedLogEntry(2, 1, thirdPayload);
        when(log.readEntries(eq(0L), anyInt(), anyLong()))
            .thenReturn(CompletableFuture.completedFuture(List.of(first, filtered, third)));

        List<LogEntry> result = cursor.readEntries(
            10, Long.MAX_VALUE, offset -> offset == 1L, 3L).get();

        assertEquals(List.of(first, third), result);
        assertEquals(1, firstPayload.refCnt());
        assertEquals(0, filteredPayload.refCnt());
        assertEquals(1, thirdPayload.refCnt());

        result.forEach(LogEntry::close);
        assertEquals(0, firstPayload.refCnt());
        assertEquals(0, thirdPayload.refCnt());
    }

    @Test
    void testEntriesBeyondCallerLimitAreClosed() throws Exception {
        ByteBuf acceptedPayload = Unpooled.wrappedBuffer(new byte[]{1});
        ByteBuf firstRemainderPayload = Unpooled.wrappedBuffer(new byte[]{2});
        ByteBuf secondRemainderPayload = Unpooled.wrappedBuffer(new byte[]{3});
        LogEntry accepted = ownedLogEntry(0, 1, acceptedPayload);
        LogEntry firstRemainder = ownedLogEntry(1, 1, firstRemainderPayload);
        LogEntry secondRemainder = ownedLogEntry(2, 1, secondRemainderPayload);
        when(log.readEntries(eq(0L), anyInt(), anyLong()))
            .thenReturn(CompletableFuture.completedFuture(
                List.of(accepted, firstRemainder, secondRemainder)));

        List<LogEntry> result = cursor.readEntries(1, Long.MAX_VALUE).get();

        assertEquals(List.of(accepted), result);
        assertEquals(1, acceptedPayload.refCnt());
        assertEquals(0, firstRemainderPayload.refCnt());
        assertEquals(0, secondRemainderPayload.refCnt());

        result.forEach(LogEntry::close);
        assertEquals(0, acceptedPayload.refCnt());
    }

    @Test
    void testRecursiveReadFailureClosesPreviouslyAccumulatedEntries() {
        ByteBuf accumulatedPayload = Unpooled.wrappedBuffer(new byte[]{1});
        LogEntry accumulated = ownedLogEntry(0, 1, accumulatedPayload);
        RuntimeException readFailure = new RuntimeException("recursive read failed");
        when(log.getEntryIndex(eq(1L))).thenReturn(
            CompletableFuture.completedFuture(makeRawEntryIndex(1, 1)));
        when(log.readEntries(eq(0L), anyInt(), anyLong()))
            .thenReturn(CompletableFuture.completedFuture(List.of(accumulated)));
        when(log.readEntries(eq(1L), anyInt(), anyLong()))
            .thenReturn(CompletableFuture.failedFuture(readFailure));

        CompletableFuture<List<LogEntry>> readFuture = cursor.readEntries(2, Long.MAX_VALUE);

        assertThrows(ExecutionException.class, readFuture::get);
        assertEquals(0, accumulatedPayload.refCnt());
    }

    // --- PARQUET prefetch ---

    @Test
    void testParquetPrefetchSkippedOnRawRead() throws Exception {
        CompactedObjectReader reader = mock(CompactedObjectReader.class);
        cursor.setCompactedObjectReader(reader);
        cursor.setNextReadIndex(CompletableFuture.completedFuture(makeRawEntryIndex(0, 1)));

        LogEntry entry = mockLogEntry(0, 1, 50);
        when(log.readEntries(eq(0L), anyInt(), anyLong()))
            .thenReturn(CompletableFuture.completedFuture(List.of(entry)));

        cursor.readEntries(10, Long.MAX_VALUE).get();

        verify(reader, never()).preFetchMessagesAsync(any(), anyLong(), anyLong(),
            anyLong(), anyLong(), anyLong());
    }

    @Test
    void testNextReadIndexRoutesCorrectly() throws Exception {
        cursor.setNextReadIndex(CompletableFuture.completedFuture(makeParquetEntryIndex(0, 10)));

        LogEntry entry = mockLogEntry(0, 1, 50);
        when(log.readEntries(eq(0L), anyInt(), anyLong()))
            .thenReturn(CompletableFuture.completedFuture(List.of(entry)));

        List<LogEntry> result = cursor.readEntries(10, Long.MAX_VALUE).get();

        assertEquals(1, result.size());
        verify(log).readEntries(eq(0L), anyInt(), anyLong());
    }

    @Test
    void testParquetPathPassesDerivedMessageCount() throws Exception {
        cursor.setNextReadIndex(CompletableFuture.completedFuture(makeParquetEntryIndex(0, 50)));

        LogEntry entry = mockLogEntry(0, 50, 500);
        when(log.readEntries(eq(0L), eq(50), anyLong()))
            .thenReturn(CompletableFuture.completedFuture(List.of(entry)));

        List<LogEntry> result = cursor.readEntries(1, Long.MAX_VALUE, null, 10000).get();

        assertEquals(1, result.size());
        assertEquals(50, cursor.readOffset());
        verify(log, times(1)).readEntries(eq(0L), eq(50), anyLong());
    }

    @Test
    void testMessageBudgetWaitsAsynchronouslyForEntryIndex() throws Exception {
        CompletableFuture<EntryIndex> pendingIndex = new CompletableFuture<>();
        when(log.getEntryIndex(eq(0L))).thenReturn(pendingIndex);

        LogEntry entry = mockLogEntry(0, 10, 500);
        when(log.readEntries(eq(0L), eq(10), anyLong()))
            .thenReturn(CompletableFuture.completedFuture(List.of(entry)));

        CompletableFuture<List<LogEntry>> readFuture = cursor.readEntries(1, Long.MAX_VALUE);

        assertFalse(readFuture.isDone());
        verify(log, never()).readEntries(anyLong(), anyInt(), anyLong());

        pendingIndex.complete(makeParquetEntryIndex(0, 10));

        List<LogEntry> result = readFuture.get();
        assertEquals(1, result.size());
        assertEquals(10, cursor.readOffset());
        verify(log).readEntries(eq(0L), eq(10), eq(Long.MAX_VALUE));
    }

    @Test
    void testParquetReadDoesNotTopUpWithRemainingEntryCountAsMessageCount() throws Exception {
        cursor.setNextReadIndex(CompletableFuture.completedFuture(makeParquetEntryIndex(0, 50)));
        when(log.getEntryIndex(eq(50L))).thenReturn(
            CompletableFuture.completedFuture(EntryIndex.NOT_FOUND));

        LogEntry entry = mockLogEntry(0, 50, 500);
        when(log.readEntries(eq(0L), eq(50), anyLong()))
            .thenReturn(CompletableFuture.completedFuture(List.of(entry)));

        List<LogEntry> result = cursor.readEntries(50, Long.MAX_VALUE).get();

        assertEquals(1, result.size());
        assertEquals(50, cursor.readOffset());
        verify(log, times(1)).readEntries(eq(0L), eq(50), anyLong());
        verify(log, times(1)).readEntries(anyLong(), anyInt(), anyLong());
    }

    @Test
    void testParquetRecursiveTopUpStaysAlignedToEntryBoundary() throws Exception {
        cursor.setNextReadIndex(CompletableFuture.completedFuture(makeParquetEntryIndex(0, 50)));
        when(log.getEntryIndex(eq(50L))).thenReturn(
            CompletableFuture.completedFuture(makeParquetEntryIndex(50, 50)));
        when(log.getEntryIndex(eq(100L))).thenReturn(
            CompletableFuture.completedFuture(EntryIndex.NOT_FOUND));

        LogEntry entry0 = mockLogEntry(0, 50, 500);
        LogEntry entry1 = mockLogEntry(50, 50, 500);
        when(log.readEntries(eq(0L), eq(100), anyLong()))
            .thenReturn(CompletableFuture.completedFuture(List.of(entry0, entry1)));

        List<LogEntry> result = cursor.readEntries(50, Long.MAX_VALUE).get();

        assertEquals(2, result.size());
        assertEquals(100, cursor.readOffset());
        verify(log, times(1)).readEntries(eq(0L), eq(100), anyLong());
        verify(log, times(1)).readEntries(anyLong(), anyInt(), anyLong());
    }

    @Test
    void testParquetMessageBudgetSaturatesAtIntegerMaxValue() throws Exception {
        cursor.setNextReadIndex(CompletableFuture.completedFuture(
            makeParquetEntryIndex(0, Integer.MAX_VALUE)));
        when(log.readEntries(eq(0L), eq(Integer.MAX_VALUE), anyLong()))
            .thenReturn(CompletableFuture.completedFuture(List.of()));

        List<LogEntry> result = cursor.readEntries(2, Long.MAX_VALUE).get();

        assertEquals(0, result.size());
        verify(log).readEntries(eq(0L), eq(Integer.MAX_VALUE), eq(Long.MAX_VALUE));
    }

    @Test
    void testRawPrefetchMessageBudgetSaturatesAtIntegerMaxValue() throws Exception {
        EntryIndex idx0 = makeRawEntryIndex(0, Integer.MAX_VALUE);
        EntryIndex idx1 = makeRawEntryIndex(Integer.MAX_VALUE, 1);
        cursor.getPrefetchedIndexes().add(idx0);
        cursor.getPrefetchedIndexes().add(idx1);
        cursor.setPrefetchedMessageCount(Integer.MAX_VALUE);

        when(logStorage.readEntriesByIndex(eq(logId), any(), anyLong(), anyLong(),
            eq(Integer.MAX_VALUE), anyLong(), any(), any()))
            .thenReturn(CompletableFuture.completedFuture(List.of()));

        List<LogEntry> result = cursor.readEntries(2, Long.MAX_VALUE).get();

        assertEquals(0, result.size());
        verify(logStorage).readEntriesByIndex(eq(logId), any(), eq(0L), eq(Long.MAX_VALUE),
            eq(Integer.MAX_VALUE), eq(Long.MAX_VALUE), any(), any());
    }

    // --- Recursive top-up ---

    @Test
    void testRecursiveTopUp() throws Exception {
        LogEntry e0 = mockLogEntry(0, 1, 50);
        LogEntry e1 = mockLogEntry(1, 1, 50);
        LogEntry e2 = mockLogEntry(2, 1, 50);
        LogEntry e3 = mockLogEntry(3, 1, 50);
        LogEntry e4 = mockLogEntry(4, 1, 50);

        when(log.getEntryIndex(eq(0L))).thenReturn(
            CompletableFuture.completedFuture(makeRawEntryIndex(0, 1)));
        when(log.getEntryIndex(eq(1L))).thenReturn(
            CompletableFuture.completedFuture(makeRawEntryIndex(1, 1)));
        when(log.getEntryIndex(eq(2L))).thenReturn(
            CompletableFuture.completedFuture(makeRawEntryIndex(2, 1)));
        when(log.getEntryIndex(eq(3L))).thenReturn(
            CompletableFuture.completedFuture(makeRawEntryIndex(3, 1)));
        when(log.getEntryIndex(eq(4L))).thenReturn(
            CompletableFuture.completedFuture(makeRawEntryIndex(4, 1)));
        when(log.readEntries(eq(0L), anyInt(), anyLong()))
            .thenReturn(CompletableFuture.completedFuture(List.of(e0, e1)));
        when(log.readEntries(eq(2L), anyInt(), anyLong()))
            .thenReturn(CompletableFuture.completedFuture(List.of(e2, e3, e4)));

        List<LogEntry> result = cursor.readEntries(5, Long.MAX_VALUE).get();

        assertEquals(5, result.size());
        assertEquals(5, cursor.readOffset());
    }

    // --- Helpers ---

    private static EntryIndex makeRawEntryIndex(long offset, int numMessages) {
        return makeRawEntryIndex(offset, numMessages, 50);
    }

    private static EntryIndex makeRawEntryIndex(long offset, int numMessages, int entrySize) {
        EntryHeader header = new EntryHeader(offset, numMessages, System.currentTimeMillis(),
            entrySize, offset * 50L);
        Position position = new Position(new FileInfo("wal-file", 1000), 0, Position.FileType.RAW);
        return new EntryIndex(header, position, 1, null,
            Optional.empty(), Optional.empty(), Optional.empty());
    }

    private static EntryIndex makeParquetEntryIndex(long offset, int numMessages) {
        EntryHeader header = new EntryHeader(offset, numMessages, System.currentTimeMillis(),
            500, offset * 500L);
        Position position = new Position(new FileInfo("parquet-file", 5000), 0,
            Position.FileType.PARQUET);
        return new EntryIndex(header, position, 1, null,
            Optional.empty(), Optional.empty(), Optional.empty());
    }

    private static LogEntry mockLogEntry(long offset, int numRecords, int size) {
        LogEntry entry = mock(LogEntry.class);
        lenient().when(entry.offset()).thenReturn(offset);
        lenient().when(entry.numberOfRecords()).thenReturn(numRecords);
        lenient().when(entry.size()).thenReturn(size);
        return entry;
    }

    private static LogEntry ownedLogEntry(long offset, int numRecords, ByteBuf payload) {
        EntryHeader header = new EntryHeader(
            offset, numRecords, System.currentTimeMillis(), payload.readableBytes(), payload.readableBytes());
        return Entry.of(header, payload).toLogEntry();
    }
}
