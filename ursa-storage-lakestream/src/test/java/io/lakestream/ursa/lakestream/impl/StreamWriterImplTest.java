/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.lakestream.impl;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.lakestream.api.Log;
import io.lakestream.api.LogEntryHeader;
import io.lakestream.api.LogId;
import io.lakestream.api.RoutingKey;
import io.lakestream.api.StreamLayout;
import io.lakestream.api.StreamWriter;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class StreamWriterImplTest {

    private List<Log> logs;
    private StreamWriterImpl writer;

    @BeforeEach
    void setUp() {
        List<LogId> logIds = List.of(LogId.of(101), LogId.of(102), LogId.of(103));
        logs = logIds.stream().map(logId -> {
            Log log = mock(Log.class);
            when(log.id()).thenReturn(logId);
            return log;
        }).toList();
        IndexedLayout layout = new IndexedLayout(logIds);
        writer = new StreamWriterImpl(layout, logs);
    }

    @Test
    void testWriteWithExplicitIndex() throws Exception {
        LogId expectedLogId = LogId.of(101);
        ByteBuf data = Unpooled.wrappedBuffer(new byte[]{1, 2, 3});
        LogEntryHeader mockHeader = mock(LogEntryHeader.class);
        when(mockHeader.offset()).thenReturn(42L);
        when(logs.get(0).append(eq(5), any(ByteBuf.class)))
            .thenReturn(CompletableFuture.completedFuture(mockHeader));

        StreamWriter.WriteResult result = writer.write(RoutingKey.ofIndex(0), 5, data).get();

        assertEquals(expectedLogId, result.logId());
        verify(logs.get(0)).append(eq(5), any(ByteBuf.class));
    }

    @Test
    void testWriteWithRoundRobin() throws Exception {
        LogEntryHeader mockHeader = mock(LogEntryHeader.class);
        when(mockHeader.offset()).thenReturn(0L);
        for (Log log : logs) {
            when(log.append(eq(1), any(ByteBuf.class)))
                .thenReturn(CompletableFuture.completedFuture(mockHeader));
        }

        ByteBuf data = Unpooled.wrappedBuffer(new byte[]{1});

        StreamWriter.WriteResult r1 = writer.write(RoutingKey.roundRobin(), 1, data).get();
        StreamWriter.WriteResult r2 = writer.write(RoutingKey.roundRobin(), 1, data).get();
        StreamWriter.WriteResult r3 = writer.write(RoutingKey.roundRobin(), 1, data).get();

        // Should cycle through all three log IDs
        assertNotNull(r1.logId());
        assertNotNull(r2.logId());
        assertNotNull(r3.logId());
    }

    @Test
    void testLayout() {
        assertNotNull(writer.layout());
        assertEquals(3, writer.layout().logCount());
    }

    @Test
    void writeRejectsCancellationWhileRoutingIsPending() throws Exception {
        StreamLayout pendingLayout = mock(StreamLayout.class);
        LogId logId = LogId.of(101L);
        CompletableFuture<LogId> route = new CompletableFuture<>();
        Log log = mock(Log.class);
        LogEntryHeader header = mock(LogEntryHeader.class);
        when(log.id()).thenReturn(logId);
        when(header.offset()).thenReturn(12L);
        when(pendingLayout.resolveForWrite(RoutingKey.ofIndex(0))).thenReturn(route);
        when(log.append(eq(1), any(ByteBuf.class)))
            .thenReturn(CompletableFuture.completedFuture(header));
        StreamWriterImpl pendingWriter = new StreamWriterImpl(pendingLayout, List.of(log));
        ByteBuf payload = Unpooled.buffer(1).writeByte(1);

        CompletableFuture<StreamWriter.WriteResult> write =
            pendingWriter.write(RoutingKey.ofIndex(0), 1, payload);

        assertFalse(write.cancel(false));
        assertFalse(write.isDone());
        assertEquals(1, payload.refCnt());
        route.complete(logId);
        assertEquals(logId, write.get().logId());
        assertTrue(payload.release());
    }

    @Test
    void writeRejectsCancellationWhileAppendIsPending() throws Exception {
        StreamLayout resolvedLayout = mock(StreamLayout.class);
        LogId logId = LogId.of(101L);
        Log log = mock(Log.class);
        CompletableFuture<LogEntryHeader> append = new CompletableFuture<>();
        LogEntryHeader header = mock(LogEntryHeader.class);
        when(log.id()).thenReturn(logId);
        when(header.offset()).thenReturn(13L);
        when(resolvedLayout.resolveForWrite(RoutingKey.ofIndex(0)))
            .thenReturn(CompletableFuture.completedFuture(logId));
        when(log.append(eq(1), any(ByteBuf.class))).thenReturn(append);
        StreamWriterImpl pendingWriter = new StreamWriterImpl(resolvedLayout, List.of(log));
        ByteBuf payload = Unpooled.buffer(1).writeByte(2);

        CompletableFuture<StreamWriter.WriteResult> write =
            pendingWriter.write(RoutingKey.ofIndex(0), 1, payload);

        assertFalse(write.cancel(false));
        assertFalse(write.isDone());
        assertEquals(1, payload.refCnt());
        append.complete(header);
        assertEquals(header.offset(), write.get().offset());
        assertTrue(payload.release());
    }

    @Test
    void closeWaitsForAcceptedWriteWhileRoutingIsPending() throws Exception {
        StreamLayout pendingLayout = mock(StreamLayout.class);
        LogId logId = LogId.of(101L);
        CompletableFuture<LogId> route = new CompletableFuture<>();
        Log log = mock(Log.class);
        LogEntryHeader header = mock(LogEntryHeader.class);
        when(log.id()).thenReturn(logId);
        when(header.offset()).thenReturn(14L);
        when(pendingLayout.resolveForWrite(RoutingKey.ofIndex(0))).thenReturn(route);
        when(log.append(eq(1), any(ByteBuf.class)))
            .thenReturn(CompletableFuture.completedFuture(header));
        StreamWriterImpl pendingWriter = new StreamWriterImpl(pendingLayout, List.of(log), 25L);
        ByteBuf payload = Unpooled.buffer(1).writeByte(3);
        CompletableFuture<StreamWriter.WriteResult> write =
            pendingWriter.write(RoutingKey.ofIndex(0), 1, payload);

        IOException timeout = assertThrows(IOException.class, pendingWriter::close);

        assertTrue(timeout.getMessage().contains("operations to drain"));
        verify(log, never()).close();
        CompletionException rejected = assertThrows(CompletionException.class,
            () -> pendingWriter.write(RoutingKey.ofIndex(0), 1, Unpooled.EMPTY_BUFFER).join());
        assertTrue(rejected.getCause() instanceof IllegalStateException);
        verify(pendingLayout, times(1)).resolveForWrite(RoutingKey.ofIndex(0));

        route.complete(logId);
        assertEquals(logId, write.get().logId());
        pendingWriter.close();
        verify(log, times(1)).close();
        assertTrue(payload.release());
    }

    @Test
    void closeWaitsForAcceptedWriteWhileAppendIsPending() throws Exception {
        StreamLayout resolvedLayout = mock(StreamLayout.class);
        LogId logId = LogId.of(101L);
        Log log = mock(Log.class);
        CompletableFuture<LogEntryHeader> append = new CompletableFuture<>();
        LogEntryHeader header = mock(LogEntryHeader.class);
        when(log.id()).thenReturn(logId);
        when(header.offset()).thenReturn(15L);
        when(resolvedLayout.resolveForWrite(RoutingKey.ofIndex(0)))
            .thenReturn(CompletableFuture.completedFuture(logId));
        when(log.append(eq(1), any(ByteBuf.class))).thenReturn(append);
        StreamWriterImpl pendingWriter = new StreamWriterImpl(resolvedLayout, List.of(log), 25L);
        ByteBuf payload = Unpooled.buffer(1).writeByte(4);
        CompletableFuture<StreamWriter.WriteResult> write =
            pendingWriter.write(RoutingKey.ofIndex(0), 1, payload);

        IOException timeout = assertThrows(IOException.class, pendingWriter::close);

        assertTrue(timeout.getMessage().contains("operations to drain"));
        verify(log, never()).close();
        CompletionException rejected = assertThrows(CompletionException.class,
            () -> pendingWriter.write(RoutingKey.ofIndex(0), 1, Unpooled.EMPTY_BUFFER).join());
        assertTrue(rejected.getCause() instanceof IllegalStateException);
        assertEquals(1, payload.refCnt());

        append.complete(header);
        assertEquals(header.offset(), write.get().offset());
        pendingWriter.close();
        verify(log, times(1)).close();
        assertTrue(payload.release());
    }

    @Test
    void interruptedCloseKeepsLogsOpenUntilAcceptedWriteDrains() throws Exception {
        StreamLayout pendingLayout = mock(StreamLayout.class);
        LogId logId = LogId.of(101L);
        CompletableFuture<LogId> route = new CompletableFuture<>();
        Log log = mock(Log.class);
        LogEntryHeader header = mock(LogEntryHeader.class);
        when(log.id()).thenReturn(logId);
        when(header.offset()).thenReturn(16L);
        when(pendingLayout.resolveForWrite(RoutingKey.ofIndex(0))).thenReturn(route);
        when(log.append(eq(1), any(ByteBuf.class)))
            .thenReturn(CompletableFuture.completedFuture(header));
        StreamWriterImpl pendingWriter = new StreamWriterImpl(pendingLayout, List.of(log), 5000L);
        ByteBuf payload = Unpooled.buffer(1).writeByte(5);
        CompletableFuture<StreamWriter.WriteResult> write =
            pendingWriter.write(RoutingKey.ofIndex(0), 1, payload);

        IOException interrupted;
        try {
            Thread.currentThread().interrupt();
            interrupted = assertThrows(IOException.class, pendingWriter::close);
            assertTrue(Thread.currentThread().isInterrupted());
        } finally {
            Thread.interrupted();
        }

        assertTrue(interrupted.getCause() instanceof InterruptedException);
        verify(log, never()).close();
        route.complete(logId);
        assertEquals(logId, write.get().logId());
        pendingWriter.close();
        verify(log, times(1)).close();
        assertTrue(payload.release());
    }

    @Test
    void closeReleasesEveryOwnedLogOnce() throws Exception {
        writer.close();
        writer.close();

        for (Log log : logs) {
            verify(log, times(1)).close();
        }
    }

}
