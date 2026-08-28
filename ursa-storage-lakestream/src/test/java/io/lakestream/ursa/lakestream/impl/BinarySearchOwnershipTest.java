/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.lakestream.impl;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import io.lakestream.api.EntryHeader;
import io.lakestream.api.EntryIndex;
import io.lakestream.api.LogEntry;
import io.lakestream.api.Position;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class BinarySearchOwnershipTest {

    @Test
    void closesEveryEntryDuringRecursiveSearch() throws Exception {
        AtomicInteger reads = new AtomicInteger();
        AtomicInteger closes = new AtomicInteger();

        EntryHeader result = BinarySearch.binarySearchWithPayload(
                offset -> CompletableFuture.completedFuture(entry(offset, reads, closes)),
                (streamId, offset) -> CompletableFuture.completedFuture(index(offset)),
                7, 0, 4, entry -> entry.offset() <= 2).get();

        assertEquals(2, result.offset());
        assertEquals(reads.get(), closes.get());
    }

    @Test
    void closesTerminalNonMatchingEntry() throws Exception {
        AtomicInteger reads = new AtomicInteger();
        AtomicInteger closes = new AtomicInteger();

        EntryHeader result = BinarySearch.binarySearchWithPayload(
                offset -> CompletableFuture.completedFuture(entry(offset, reads, closes)),
                (streamId, offset) -> CompletableFuture.completedFuture(index(offset)),
                7, 0, 0, entry -> false).get();

        assertNull(result);
        assertEquals(1, reads.get());
        assertEquals(reads.get(), closes.get());
    }

    private static LogEntry entry(long offset, AtomicInteger reads, AtomicInteger closes) {
        reads.incrementAndGet();
        return new LogEntry() {
            private final ByteBuf payload = Unpooled.wrappedBuffer(new byte[] {1}).asReadOnly();
            private boolean closed;

            @Override
            public long offset() {
                return offset;
            }

            @Override
            public int numberOfRecords() {
                return 1;
            }

            @Override
            public long timestamp() {
                return 0;
            }

            @Override
            public int size() {
                return payload.readableBytes();
            }

            @Override
            public ByteBuf payload() {
                return payload;
            }

            @Override
            public void close() {
                if (!closed) {
                    closed = true;
                    closes.incrementAndGet();
                    payload.release();
                }
            }
        };
    }

    private static EntryIndex index(long offset) {
        return new EntryIndex(
                new EntryHeader(offset, 1, 0, 1, offset + 1),
                new Position("test"),
                1,
                EntryIndex.IndexType.NORMAL,
                Optional.empty());
    }
}
