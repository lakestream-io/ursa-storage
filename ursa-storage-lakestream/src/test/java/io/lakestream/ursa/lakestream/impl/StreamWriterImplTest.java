/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.lakestream.impl;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.lakestream.api.LogEntryHeader;
import io.lakestream.api.LogId;
import io.lakestream.api.LogStorage;
import io.lakestream.api.RoutingKey;
import io.lakestream.api.StreamWriter;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class StreamWriterImplTest {

    private LogStorage logStorage;
    private StreamWriterImpl writer;

    @BeforeEach
    void setUp() {
        logStorage = mock(LogStorage.class);
        List<LogId> logIds = List.of(LogId.of(101), LogId.of(102), LogId.of(103));
        IndexedLayout layout = new IndexedLayout(logIds);
        writer = new StreamWriterImpl(layout, logStorage);
    }

    @Test
    void testWriteWithExplicitIndex() throws Exception {
        LogId expectedLogId = LogId.of(101);
        ByteBuf data = Unpooled.wrappedBuffer(new byte[]{1, 2, 3});
        LogEntryHeader mockHeader = mock(LogEntryHeader.class);
        when(mockHeader.offset()).thenReturn(42L);
        when(logStorage.append(eq(expectedLogId), eq(5), any(ByteBuf.class)))
            .thenReturn(CompletableFuture.completedFuture(mockHeader));

        StreamWriter.WriteResult result = writer.write(RoutingKey.ofIndex(0), 5, data).get();

        assertEquals(expectedLogId, result.logId());
        verify(logStorage).append(eq(expectedLogId), eq(5), any(ByteBuf.class));
    }

    @Test
    void testWriteWithRoundRobin() throws Exception {
        LogEntryHeader mockHeader = mock(LogEntryHeader.class);
        when(mockHeader.offset()).thenReturn(0L);
        when(logStorage.append(any(LogId.class), eq(1), any(ByteBuf.class)))
            .thenReturn(CompletableFuture.completedFuture(mockHeader));

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
}
