/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.compaction.common;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

public class ManagedTableFileIndexTest {

    @Test
    void testAppendAndGet() {
        ManagedTableFileIndex idx = new ManagedTableFileIndex(64);
        idx.append(10L, "data1.parquet");
        idx.append(20L, "data2.parquet");
        idx.append(30L, "data3.parquet");

        assertEquals("data1.parquet", idx.get(5L));
        assertEquals("data1.parquet", idx.get(10L));
        assertEquals("data2.parquet", idx.get(15L));
        assertEquals("data2.parquet", idx.get(20L));
        assertEquals("data3.parquet", idx.get(30L));
        assertThrows(IllegalArgumentException.class, () -> idx.get(40L));

        assertFalse(idx.getFileBaseOffset(5).isPresent());
        assertFalse(idx.getFileBaseOffset(10).isPresent());
        assertEquals(11L, idx.getFileBaseOffset(15).get());
        assertEquals(11L, idx.getFileBaseOffset(20).get());

        assertThrows(IllegalArgumentException.class, () -> idx.getFileBaseOffset(31L));
    }

    @Test
    void testSerializeDeserialize() {
        ManagedTableFileIndex idx = new ManagedTableFileIndex(64);
        idx.append(1L, "a.parquet");
        idx.append(2L, "b.parquet");

        String s = idx.serializeToString();
        ManagedTableFileIndex idx2 = ManagedTableFileIndex.deserializeFromString(s);

        assertEquals("a.parquet", idx2.get(1L));
        assertEquals("b.parquet", idx2.get(2L));

        assertThrows(IllegalArgumentException.class, () -> idx2.get(3L));
    }

    @Test
    void testEnsureCapacity() {
        ManagedTableFileIndex idx = new ManagedTableFileIndex(8);
        String longPath1 = "x".repeat(1024);
        String longPath2 = "y".repeat(2048);
        idx.append(100L, longPath1);
        idx.append(200L, longPath2);

        assertEquals(longPath1, idx.get(100L));
        assertEquals(longPath2, idx.get(200L));
    }
}

