/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.lakestream.impl;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.lakestream.api.EntryHeader;
import io.lakestream.api.LogEntry;
import io.lakestream.api.LogId;
import io.lakestream.api.LogStorage;
import io.lakestream.ursa.storage.Entry;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.Test;

class LogImplTest {

    @Test
    void readEntriesUsesUnifiedReaderWhenConfigured() throws Exception {
        ReadFixture fixture = new ReadFixture(LogId.of(1001L));
        LogEntry entry = entry(7L, 2, new byte[] {1, 2, 3, 4});

        List<LogEntry> entries = fixture.readWithUnifiedReader(entry, 7L, 10, 1024L, 9L,
            () -> fixture.log.readEntries(7L, 10, 1024L).get());

        assertEquals(List.of(entry), entries);
        entries.forEach(LogEntry::close);
    }

    @Test
    void readEntryUsesUnifiedReaderWhenConfigured() throws Exception {
        ReadFixture fixture = new ReadFixture(LogId.of(1002L));
        LogEntry entry = entry(12L, 1, new byte[] {5, 6, 7});

        LogEntry result = fixture.readWithUnifiedReader(entry, 12L, 1, Long.MAX_VALUE, 13L,
            () -> fixture.log.readEntry(12L).get());

        assertEquals(entry, result);
        result.close();
    }

    @Test
    void readEntryClosesUnexpectedExtraEntries() throws Exception {
        LogId logId = LogId.of(1005L);
        UnifiedStreamReader unifiedReader = mock(UnifiedStreamReader.class);
        LogImpl log = new LogImpl(logId, mock(LogStorage.class), unifiedReader, null, null);
        ByteBuf resultPayload = Unpooled.wrappedBuffer(new byte[] {1});
        ByteBuf extraPayload = Unpooled.wrappedBuffer(new byte[] {2});
        LogEntry resultEntry = Entry.of(
            new EntryHeader(20L, 1, 1000L, 1, 1), resultPayload).toLogEntry();
        LogEntry extraEntry = Entry.of(
            new EntryHeader(21L, 1, 1001L, 1, 2), extraPayload).toLogEntry();
        when(unifiedReader.readEntries(logId, 20L, 1, Long.MAX_VALUE))
            .thenReturn(CompletableFuture.completedFuture(
                new UnifiedStreamReader.ReadResult(List.of(resultEntry, extraEntry), 22L)));

        LogEntry actual = log.readEntry(20L).get();

        assertEquals(resultEntry, actual);
        assertEquals(1, resultPayload.refCnt());
        assertEquals(0, extraPayload.refCnt());
        actual.close();
        assertEquals(0, resultPayload.refCnt());
    }

    @Test
    void canceledReadClosesEntriesProducedLater() {
        LogId logId = LogId.of(1006L);
        UnifiedStreamReader unifiedReader = mock(UnifiedStreamReader.class);
        LogImpl log = new LogImpl(logId, mock(LogStorage.class), unifiedReader, null, null);
        CompletableFuture<UnifiedStreamReader.ReadResult> sourceRead = new CompletableFuture<>();
        when(unifiedReader.readEntries(logId, 30L, 1, 1024L)).thenReturn(sourceRead);

        CompletableFuture<List<LogEntry>> callerRead = log.readEntries(30L, 1, 1024L);
        assertTrue(callerRead.cancel(false));

        ByteBuf payload = Unpooled.wrappedBuffer(new byte[] {1});
        LogEntry entry = Entry.of(
            new EntryHeader(30L, 1, 1000L, 1, 1), payload).toLogEntry();
        sourceRead.complete(new UnifiedStreamReader.ReadResult(List.of(entry), 31L));

        assertTrue(callerRead.isCancelled());
        assertEquals(0, payload.refCnt());
    }

    @Test
    void closeOwnedUnifiedReaderIsIdempotent() throws Exception {
        UnifiedStreamReader unifiedReader = mock(UnifiedStreamReader.class);
        LogImpl log = new LogImpl(
            LogId.of(1003L), mock(LogStorage.class), unifiedReader, null, null, true);

        log.close();
        log.close();

        verify(unifiedReader, times(1)).close();
    }

    @Test
    void closeDoesNotCloseBorrowedUnifiedReader() throws Exception {
        UnifiedStreamReader unifiedReader = mock(UnifiedStreamReader.class);
        LogImpl log = new LogImpl(
            LogId.of(1004L), mock(LogStorage.class), unifiedReader, null, null);

        log.close();

        verify(unifiedReader, never()).close();
    }

    private static LogEntry entry(long offset, int records, byte[] data) {
        return Entry.of(
            new EntryHeader(offset, records, 1000L, data.length, data.length),
            Unpooled.wrappedBuffer(data))
            .toLogEntry();
    }

    private static class ReadFixture {
        private final LogId logId;
        private final LogStorage logStorage = mock(LogStorage.class);
        private final UnifiedStreamReader unifiedReader = mock(UnifiedStreamReader.class);
        private final LogImpl log;

        ReadFixture(LogId logId) {
            this.logId = logId;
            this.log = new LogImpl(logId, logStorage, unifiedReader, null, null);
        }

        private <T> T readWithUnifiedReader(LogEntry entry, long startOffset, int maxEntries,
                                            long maxSizeBytes, long nextOffset, Callable<T> read) throws Exception {
            when(unifiedReader.readEntries(eq(logId), eq(startOffset), eq(maxEntries), eq(maxSizeBytes)))
                .thenReturn(CompletableFuture.completedFuture(
                    new UnifiedStreamReader.ReadResult(List.of(entry), nextOffset)));

            T result = read.call();

            verify(unifiedReader).readEntries(logId, startOffset, maxEntries, maxSizeBytes);
            verify(logStorage, never()).readEntries(logId, startOffset, maxEntries, maxSizeBytes);
            return result;
        }
    }
}
