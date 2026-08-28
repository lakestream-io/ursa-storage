/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.storage.impl;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testcontainers.shaded.org.awaitility.Awaitility.await;

import io.lakestream.api.EntryHeader;
import io.lakestream.api.EntryIndex;
import io.lakestream.api.Position;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class EntryIndexCacheTest {

    private BiFunction<Long, Long, CompletableFuture<EntryIndex>> readEntryIndex;
    private EntryIndexCache cache;

    @BeforeEach
    void setUp() {
        readEntryIndex = mock(BiFunction.class);
        when(readEntryIndex.apply(anyLong(), anyLong())).thenReturn(
                CompletableFuture.completedFuture(EntryIndex.NOT_FOUND));
        cache = new EntryIndexCache(readEntryIndex, 1, 1000);
    }

    @Test
    void testPutAndGetEntryIndex() {
        EntryIndex index = mock(EntryIndex.class);
        EntryHeader header = mock(EntryHeader.class);
        when(header.offset()).thenReturn(10L);
        when(index.header()).thenReturn(header);

        cache.put(1L, index);
        CompletableFuture<EntryIndex> result = cache.get(1L, 10L);

        Assertions.assertTrue(result.isDone());
        assertEquals(index, result.join());
    }

    @Test
    void testInvalidateAll() {
        EntryIndex index = mock(EntryIndex.class);
        EntryHeader header = mock(EntryHeader.class);
        when(header.offset()).thenReturn(10L);
        when(index.header()).thenReturn(header);

        cache.put(1L, index);
        cache.invalidateAll();
        EntryIndex result = cache.get(1L, 10L).join();
        assertEquals(EntryIndex.NOT_FOUND, result);
        assertEquals(0, cache.size());
    }

    @Test
    void testInvalidate() {
        var cache = new EntryIndexCache(readEntryIndex, 10, 1000);
        EntryIndex index = mock(EntryIndex.class);
        EntryHeader header = mock(EntryHeader.class);
        when(header.offset()).thenReturn(10L);
        when(index.header()).thenReturn(header);

        cache.put(1L, index);
        cache.put(2L, index);

        //non existent key
        cache.invalidate(0L);
        assertEquals(2, cache.size());
        EntryIndex result3 = cache.get(1L, header.offset()).join();
        EntryIndex result4 = cache.get(2L, header.offset()).join();
        assertEquals(index, result3);
        assertEquals(index, result4);

        //existent key
        cache.invalidate(1L);
        EntryIndex result5 = cache.get(1L, header.offset()).join();
        assertEquals(EntryIndex.NOT_FOUND, result5);
        EntryIndex result6 = cache.get(2L, header.offset()).join();
        assertEquals(index, result6);
        assertEquals(1, cache.size());
    }

    @Test
    void testTTL() throws InterruptedException {
        var cache = new EntryIndexCache(readEntryIndex, 10, 3);
        EntryIndex index = mock(EntryIndex.class);
        EntryHeader header = mock(EntryHeader.class);
        when(header.offset()).thenReturn(10L);
        when(index.header()).thenReturn(header);

        cache.put(1L, index);
        cache.put(2L, index);

        //non existent key
        cache.invalidate(0L);
        assertEquals(2, cache.size());
        EntryIndex result3 = cache.get(1L, header.offset()).join();
        EntryIndex result4 = cache.get(2L, header.offset()).join();
        assertEquals(index, result3);
        assertEquals(index, result4);

        Thread.sleep(3000);

        EntryIndex result5 = cache.get(1L, header.offset()).join();
        EntryIndex result6 = cache.get(2L, header.offset()).join();
        assertEquals(EntryIndex.NOT_FOUND, result5);
        assertEquals(EntryIndex.NOT_FOUND, result6);

        assertEquals(0, cache.size());
    }


    @Test
    void testPutEntriesAndGetEntryHeader() {
        EntryHeader header = new EntryHeader(100, 20, 12345, 100, 200);
        Position position = new Position("", -1, Position.FileType.RAW);
        List<Pair<EntryHeader, Integer>> headers = new ArrayList<>();
        long offset = 100;
        int msgCount = 10;
        int entrySize = 50;
        int[] offsets = new int[2];
        for (int i = 0; i < 2; i++) {
            EntryHeader h = new EntryHeader(offset, msgCount, 12345, entrySize, entrySize * (i + 1) + 100);
            offset += msgCount;
            offsets[i] = (int) (offset - 100);
            headers.add(Pair.of(h, i));
        }
        EntryIndex index = new EntryIndex(header, position, 2, EntryIndex.IndexType.NORMAL, Optional.of(offsets));
        cache.put(1L, index);
        assertEquals(1, cache.size());


        var ch = cache.getEntryHeader(1L, 100).join();
        assertEquals(headers.get(0).getLeft(), ch);
        var count = cache.getMessageCount(1L, 100).join();
        assertEquals(10, count);

        ch = cache.getEntryHeader(1L, 110).join();
        assertEquals(headers.get(1).getLeft(), ch);
        count = cache.getMessageCount(1L, 110).join();
        assertEquals(10, count);

        EntryHeader header2 = new EntryHeader(200, 20, 12346, 100, 500);
        EntryIndex index2 = new EntryIndex(header2, position, 2, EntryIndex.IndexType.NORMAL, Optional.of(offsets));
        cache.put(1L, index2);


        await().atMost(1000, TimeUnit.MILLISECONDS).untilAsserted(
                () -> {
                    cache.get(1L, 20L);
                    assertEquals(1, cache.size());
                });
    }

    @Test
    void testPutEntriesAndSearchEntryHeaders() {
        EntryHeader header = new EntryHeader(100, 20, 12345, 100, 200);
        Position position = new Position("", -1, Position.FileType.RAW);
        EntryIndex index =
                new EntryIndex(header, position, 2, EntryIndex.IndexType.NORMAL, Optional.of(new int[]{10, 20}));

        EntryHeader firstHeader = new EntryHeader(100, 10, 12345, 50, 150);
        EntryHeader lastHeader = new EntryHeader(110, 10, 12345, 50, 200);

        var childHeader = index.getLastEntryHeader();
        assertEquals(lastHeader, childHeader);

        childHeader = index.getFirstEntryHeader();
        assertEquals(firstHeader, childHeader);


        when(readEntryIndex.apply(eq(1L), argThat(val -> 100L <= val && val <= 110))).thenReturn(
                CompletableFuture.completedFuture(index));

        childHeader = cache.searchEntryHeader(1L, 95L).join();
        assertEquals(EntryHeader.NOT_FOUND, childHeader);

        childHeader = cache.searchEntryHeader(1L, 100L).join();
        assertEquals(firstHeader, childHeader);

        childHeader = cache.searchEntryHeader(1L, 105L).join();
        assertEquals(firstHeader, childHeader);

        childHeader = cache.searchEntryHeader(1L, 110L).join();
        assertEquals(lastHeader, childHeader);

        childHeader = cache.searchEntryHeader(1L, 115L).join();
        assertEquals(lastHeader, childHeader);

        childHeader = cache.searchEntryHeader(1L, 120L).join();
        assertEquals(EntryHeader.NOT_FOUND, childHeader);

        assertEquals(1, cache.size());

        cache.invalidateAll();
        assertEquals(0, cache.size());
    }

    @Test
    void testSearchEntryHeader_NotFound() {
        CompletableFuture<EntryHeader> result = cache.searchEntryHeader(1L, 20L);
        assertEquals(EntryHeader.NOT_FOUND, result.join());
        assertEquals(0, cache.size());
    }

    @Test
    void testGetEntryHeader_NotFound() {
        CompletableFuture<EntryHeader> result = cache.getEntryHeader(1L, 20L);
        assertEquals(EntryHeader.NOT_FOUND, result.join());
        assertEquals(0, cache.size());
    }

}
