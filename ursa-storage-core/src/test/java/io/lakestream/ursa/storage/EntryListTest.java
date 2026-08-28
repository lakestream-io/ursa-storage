/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.storage;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.lakestream.api.EntryHeader;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Predicate;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class EntryListTest {

    private EntryList entryList;
    private Set<Long> deletedOffsets;
    private AtomicBoolean skipOffsets;
    private AtomicLong lastSkipOffset;
    private Predicate<Long> offsetDeletedCondition;
    private Predicate<Long> skipCondition;

    @BeforeEach
    void setUp() {
        deletedOffsets = new HashSet<>();
        skipOffsets = new AtomicBoolean(false);
        lastSkipOffset = new AtomicLong(-1);
        offsetDeletedCondition = deletedOffsets::contains;
        skipCondition = offset -> {
            lastSkipOffset.set(offset);
            return skipOffsets.get();
        };

        entryList =
                new EntryList(1L, 5L, 20L, 8, 120, offsetDeletedCondition, skipCondition);
    }

    @Test
    void testMaxOffset() {
        entryList =
                new EntryList(1L, 0L, 30L, 100, 120, offsetDeletedCondition, skipCondition);
        EntryHeader header = header(0L, 10, 10);

        assertTrue(entryList.isNotFull(header));

        entryList.add(entry(header));

        EntryHeader maxOffset = header(10L, 20, 10);
        assertTrue(entryList.isNotFull(maxOffset));

        EntryHeader maxOffsetPlusOne = header(10L, 21, 10);
        assertFalse(entryList.isNotFull(maxOffsetPlusOne));
    }

    @Test
    void testIsNotFull() {
        EntryHeader header = header(5L, 2, 10);

        assertTrue(entryList.isNotFull(header));

        Entry entry = entry(header);

        entryList.add(entry);
        entryList.add(entry);
        entryList.add(entry);

        assertEquals(3, entryList.size());

        EntryHeader nextHeader = header(8L, 2, 10);
        assertTrue(entryList.isNotFull(nextHeader));

        EntryHeader largeOffsetHeader = header(3000L, 2, 30);
        assertFalse(entryList.isNotFull(largeOffsetHeader));

        EntryHeader largeEntrySizeHeader = header(8L, 2, 30000);
        assertFalse(entryList.isNotFull(largeEntrySizeHeader));

        entryList.add(entry);
        assertEquals(4, entryList.size());
        assertFalse(entryList.isNotFull(nextHeader));
    }

    @Test
    void testShouldSkip() {
        // Case 1: Entry offset is before the startOffset → should skip
        assertTrue(entryList.shouldSkip(header(4L, 1, 10)), "Should skip entries before startOffset.");

        // Case 2: Entry is marked as deleted → should skip
        deletedOffsets.add(6L);
        assertTrue(entryList.shouldSkip(header(6L, 1, 10)), "Should skip deleted offsets.");

        // Case 3: Entry meets skipCondition → should skip
        deletedOffsets.remove(6L);
        skipOffsets.set(true);
        assertTrue(entryList.shouldSkip(header(6L, 1, 10)), "Should skip based on offset condition.");
        assertEquals(6L, lastSkipOffset.get());

        // Case 4: Entry should NOT be skipped (valid entry)
        skipOffsets.set(false);

        assertFalse(entryList.shouldSkip(header(6L, 2, 10)),
                "Entry should not be skipped if it's valid and after startOffset.");

        // Case 5: Entry is exactly at startOffset → should NOT skip
        assertFalse(entryList.shouldSkip(header(5L, 3, 10)),
                "Entry should not be skipped if it starts at the startOffset.");

        // Case 6: Entry offset is after startOffset and not deleted → should NOT skip
        assertFalse(entryList.shouldSkip(header(10L, 3, 10)),
                "Entry should not be skipped if it is after startOffset and not deleted.");
    }

    @Test
    void testAddEntryAndClear() {
        ByteBuf payload = Unpooled.buffer(20);
        Entry entry = Entry.of(header(5L, 1, 20), payload);

        entryList.add(entry);

        assertEquals(1, entryList.size());
        assertEquals(20, entryList.getSizeInBytes());
        assertEquals(1, entryList.getMessageCount());
        assertEquals(-1, entryList.getRepeatEntryIndex());

        entryList.clear();

        assertEquals(0, entryList.size());
        assertEquals(0, entryList.getSizeInBytes());
        assertEquals(0, entryList.getMessageCount());
        assertEquals(-1, entryList.getRepeatEntryIndex());
        assertEquals(0, payload.refCnt());
    }

    @Test
    void testGetAndSet() {
        Entry entry1 = entry(header(5L, 1, 10));
        Entry entry2 = entry(header(6L, 1, 10));

        entryList.add(entry1);
        entryList.set(0, entry2);

        assertEquals(entry2, entryList.get(0), "Entry should be updated correctly.");
    }

    @Test
    void testIsEmpty() {
        assertTrue(entryList.isEmpty(), "EntryList should be empty initially.");

        entryList.add(entry(header(5L, 1, 10)));
        assertFalse(entryList.isEmpty(), "EntryList should not be empty after adding an entry.");
    }

    @Test
    void testSize() {
        assertEquals(0, entryList.size(), "Size should be 0 initially.");

        Entry entry = entry(header(5L, 1, 10));
        entryList.add(entry);
        entryList.add(entry);

        assertEquals(2, entryList.size(), "Size should be 2 after adding two entries.");
    }

    private static EntryHeader header(long offset, int numberOfMessages, int entrySize) {
        return new EntryHeader(offset, numberOfMessages, 0L, entrySize, 0L);
    }

    private static Entry entry(EntryHeader header) {
        return Entry.of(header, null);
    }
}
