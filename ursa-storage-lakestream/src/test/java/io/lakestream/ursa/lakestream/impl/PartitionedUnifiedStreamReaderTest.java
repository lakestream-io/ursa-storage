/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.lakestream.impl;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import io.lakestream.api.Log;
import io.lakestream.api.LogEntry;
import io.lakestream.api.LogId;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class PartitionedUnifiedStreamReaderTest {

    @Test
    void lazilyCreatesAndIsolatesOneLogPerIdAndComputesNextOffset() throws Exception {
        LogId firstId = LogId.of(101L);
        LogId secondId = LogId.of(202L);
        Log firstLog = mock(Log.class);
        Log secondLog = mock(Log.class);
        LogEntry firstEntry = entry(10L, 3);
        LogEntry secondEntry = entry(20L, 4);
        LogEntry otherLogEntry = entry(7L, 2);
        when(firstLog.readEntries(10L, 10, 1024L))
            .thenReturn(CompletableFuture.completedFuture(List.of(firstEntry, secondEntry)));
        when(firstLog.readEntries(24L, 10, 1024L))
            .thenReturn(CompletableFuture.completedFuture(List.of()));
        when(secondLog.readEntries(7L, 5, 512L))
            .thenReturn(CompletableFuture.completedFuture(List.of(otherLogEntry)));
        Map<LogId, Log> availableLogs = Map.of(firstId, firstLog, secondId, secondLog);
        AtomicInteger factoryCalls = new AtomicInteger();
        PartitionedUnifiedStreamReader reader = new PartitionedUnifiedStreamReader(logId -> {
            factoryCalls.incrementAndGet();
            return availableLogs.get(logId);
        });

        verifyNoInteractions(firstLog, secondLog);

        UnifiedStreamReader.ReadResult first = reader.readEntries(firstId, 10L, 10, 1024L).get();
        UnifiedStreamReader.ReadResult sameLog = reader.readEntries(firstId, 24L, 10, 1024L).get();
        UnifiedStreamReader.ReadResult second = reader.readEntries(secondId, 7L, 5, 512L).get();

        assertEquals(List.of(firstEntry, secondEntry), first.entries());
        assertEquals(24L, first.nextOffset());
        assertTrue(sameLog.entries().isEmpty());
        assertEquals(24L, sameLog.nextOffset());
        assertEquals(List.of(otherLogEntry), second.entries());
        assertEquals(9L, second.nextOffset());
        assertEquals(2, factoryCalls.get());
        verify(firstLog).readEntries(10L, 10, 1024L);
        verify(firstLog).readEntries(24L, 10, 1024L);
        verify(secondLog).readEntries(7L, 5, 512L);

        first.entries().forEach(LogEntry::close);
        second.entries().forEach(LogEntry::close);
        reader.close();
    }

    @Test
    void canceledReadClosesEntriesThatArriveAfterCancellation() throws Exception {
        LogId logId = LogId.of(303L);
        Log log = mock(Log.class);
        LogEntry lateEntry = entry(30L, 5);
        CompletableFuture<List<LogEntry>> childRead = new CompletableFuture<>();
        when(log.readEntries(30L, 10, 1024L)).thenReturn(childRead);
        PartitionedUnifiedStreamReader reader = new PartitionedUnifiedStreamReader(ignored -> log);

        CompletableFuture<UnifiedStreamReader.ReadResult> exposed =
            reader.readEntries(logId, 30L, 10, 1024L);
        assertTrue(exposed.cancel(false));

        assertTrue(childRead.complete(List.of(lateEntry)));
        assertTrue(exposed.isCancelled());
        verify(lateEntry).close();
        reader.close();
    }

    @Test
    void closeIsIdempotentAndClosesEveryOpenedLogAfterFailure() throws Exception {
        LogId firstId = LogId.of(404L);
        LogId secondId = LogId.of(405L);
        Log firstLog = emptyLog();
        Log secondLog = emptyLog();
        Exception firstFailure = new Exception("first close failed");
        doThrow(firstFailure).doNothing().when(firstLog).close();
        Map<LogId, Log> availableLogs = Map.of(firstId, firstLog, secondId, secondLog);
        PartitionedUnifiedStreamReader reader =
            new PartitionedUnifiedStreamReader(availableLogs::get);

        reader.readEntries(firstId, 0L, 1, 1L).get();
        reader.readEntries(secondId, 0L, 1, 1L).get();

        Exception actual = assertThrows(Exception.class, reader::close);
        assertEquals(firstFailure, actual);
        verify(firstLog).close();
        verify(secondLog).close();

        reader.close();
        verify(firstLog, times(2)).close();
        verify(secondLog, times(1)).close();
    }

    @Test
    void unknownLogIsRejectedWithoutCreatingAChild() throws Exception {
        LogId registeredId = LogId.of(501L);
        LogId unknownId = LogId.of(-1L);
        Log registeredLog = emptyLog();
        PartitionedUnifiedStreamReader reader = new PartitionedUnifiedStreamReader(logId -> {
            if (!registeredId.equals(logId)) {
                throw new IllegalArgumentException("Unknown log " + logId);
            }
            return registeredLog;
        });

        ExecutionException failure = assertThrows(ExecutionException.class,
            () -> reader.readEntries(unknownId, 0L, 1, 1L).get());

        assertInstanceOf(IllegalArgumentException.class, failure.getCause());
        verifyNoInteractions(registeredLog);
        reader.close();
    }

    @Test
    void readAfterCloseFailsWithoutCreatingLog() throws Exception {
        AtomicInteger factoryCalls = new AtomicInteger();
        PartitionedUnifiedStreamReader reader = new PartitionedUnifiedStreamReader(logId -> {
            factoryCalls.incrementAndGet();
            return emptyLog();
        });
        reader.close();

        ExecutionException failure = assertThrows(ExecutionException.class,
            () -> reader.readEntries(LogId.of(601L), 0L, 1, 1L).get());

        assertInstanceOf(IllegalStateException.class, failure.getCause());
        assertEquals(0, factoryCalls.get());
    }

    private static Log emptyLog() {
        Log log = mock(Log.class);
        when(log.readEntries(0L, 1, 1L))
            .thenReturn(CompletableFuture.completedFuture(List.of()));
        return log;
    }

    private static LogEntry entry(long offset, int numberOfRecords) {
        LogEntry entry = mock(LogEntry.class);
        when(entry.offset()).thenReturn(offset);
        when(entry.numberOfRecords()).thenReturn(numberOfRecords);
        return entry;
    }
}
