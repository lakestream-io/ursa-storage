/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.storage;

import static io.lakestream.ursa.storage.impl.StorageConfig.PROTOBUF_VERSION;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.lakestream.api.EntryHeader;
import io.lakestream.api.EntryIndex;
import io.lakestream.api.Position;
import io.lakestream.ursa.storage.proto.IndexType;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;

@Slf4j
class ValueTest {

    @Test
    void testProtoBufValue() {
        var entryOffsets = Optional.of(new int[]{10, 20});
        EntryHeader header = new EntryHeader(100, 20, 12345, 100, 200);
        Position position = new Position("", -1, Position.FileType.RAW);
        EntryIndex index =
                new EntryIndex(header, position, 2, EntryIndex.IndexType.NORMAL, entryOffsets);
        var expected = new Value(20, 100, 2, IndexType.NORMAL, position, entryOffsets);
        assertEquals(expected, Value.of(index));

        var bytes = expected.toBytes(PROTOBUF_VERSION);
        var parsed = Value.parse(PROTOBUF_VERSION, bytes);
        assertEquals(expected.numberOfMessages(), parsed.numberOfMessages());
        assertEquals(expected.entrySize(), parsed.entrySize());
        assertEquals(expected.entryCount(), parsed.entryCount());
        assertEquals(expected.indexType(), parsed.indexType());
        assertEquals(expected.position(), parsed.position());
        assertArrayEquals(expected.entryOffsets().get(), parsed.entryOffsets().get());
    }

    @Test
    void testEntryOffsetCompression() {
        int entryCount = 10_000;
        int[] raw = new int[entryCount];
        for (int i = 0; i < entryCount; i++) {
            raw[i] = i + (i / (entryCount / 3)) * entryCount;
        }
        int interNum = 100;

        List<Long> durationSumZSTD = new ArrayList<>();
        List<Integer> compressionSizeZSTD = new ArrayList<>();
        for (int i = 0; i < interNum; i++) {
            long start1 = System.nanoTime();
            @Cleanup("release")
            var compressed = Value.compressEntryOffsetsByZSTD(raw);
            compressionSizeZSTD.add(compressed.readableBytes());
            long duration1 = System.nanoTime() - start1;
            // log.info("ZSTD compression duration {} ns", duration1);
            long start2 = System.nanoTime();
            var uncompressed = Value.uncompressEntryOffsetsByZSTD(compressed, raw.length * 4);
            long duration2 = System.nanoTime() - start2;
            // log.info("ZSTD uncompression duration {} ns", duration2);
            assertArrayEquals(raw, uncompressed);
            durationSumZSTD.add(duration1 + duration2);
        }


        List<Long> durationSumFastPFOR = new ArrayList<>();
        List<Integer> compressionSizeFastPFOR = new ArrayList<>();
        for (int i = 0; i < interNum; i++) {
            long start1 = System.nanoTime();
            @Cleanup("release")
            var compressed = Value.compressEntryOffsetsByFastPFOR(raw);
            compressionSizeFastPFOR.add(compressed.readableBytes());
            long duration1 = System.nanoTime() - start1;
            // log.info("FastPFOR compression duration {} ns", duration1);
            long start2 = System.nanoTime();
            int[] uncompressed = Value.uncompressEntryOffsetsByFastPFOR(compressed);
            long duration2 = System.nanoTime() - start2;
            // log.info("FastPFOR uncompression duration {} ns", duration2);
            assertArrayEquals(raw, uncompressed);
            durationSumFastPFOR.add(duration1 + duration2);
        }
        Collections.sort(durationSumZSTD);
        Collections.sort(durationSumFastPFOR);
        long durationSumZSTDP50 = durationSumZSTD.get(50);
        long durationSumFastPFORP50 = durationSumFastPFOR.get(50);
        log.info("durationSumZSTDP50:{} ns vs durationSumFastPFORP50:{} ns",
                durationSumZSTDP50,
                durationSumFastPFORP50);
        assertTrue(durationSumFastPFORP50 < durationSumZSTDP50);

        Collections.sort(compressionSizeFastPFOR);
        Collections.sort(compressionSizeZSTD);
        long compressionSizeFastPFORP50 = compressionSizeFastPFOR.get(50);
        long compressionSizeZSTDP50 = compressionSizeZSTD.get(50);
        log.info("compressionSizeFastPFORP50:{} vs compressionSizeZSTDP50:{} ",
                compressionSizeFastPFORP50,
                compressionSizeZSTDP50);
        assertTrue(compressionSizeFastPFORP50 < compressionSizeZSTDP50);
    }

}