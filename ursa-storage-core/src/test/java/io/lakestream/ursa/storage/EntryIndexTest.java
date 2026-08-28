/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.storage;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.lakestream.api.EntryHeader;
import io.lakestream.api.EntryIndex;
import io.lakestream.api.Position;
import java.util.Optional;
import org.junit.jupiter.api.Test;

class EntryIndexTest {
    private Position rawPosition = new Position("", -1, Position.FileType.RAW);
    private Position parquetPosition = new Position("", -1, Position.FileType.PARQUET);
    EntryHeader twoEntriesHeader = new EntryHeader(100, 20, 10, 200, 300);
    EntryHeader threeEntriesHeader = new EntryHeader(100, 30, 10, 300, 400);
    EntryHeader fourEntriesHeader = new EntryHeader(100, 40, 10, 400, 500);
    EntryHeader firstHeader = new EntryHeader(100, 10, 10, 100, 200);
    EntryHeader secondHeader = new EntryHeader(110, 10, 10, 100, 300);
    EntryHeader thirdHeader = new EntryHeader(120, 10, 10, 100, 400);
    EntryHeader fourthHeader = new EntryHeader(130, 10, 10, 100, 500);

    EntryIndex singleEntryIndex =
            new EntryIndex(firstHeader, rawPosition, 1, EntryIndex.IndexType.NORMAL, Optional.empty());
    EntryIndex twoEntriesIndex =
            new EntryIndex(twoEntriesHeader, rawPosition, 2, EntryIndex.IndexType.NORMAL,
                    Optional.of(new int[]{10, 20}));
    EntryIndex threeEntriesIndex =
            new EntryIndex(threeEntriesHeader, rawPosition, 3, EntryIndex.IndexType.NORMAL,
                    Optional.of(new int[]{10, 20, 30}));
    EntryIndex fourEntriesIndex =
            new EntryIndex(fourEntriesHeader, rawPosition, 4, EntryIndex.IndexType.NORMAL,
                    Optional.of(new int[]{10, 20, 30, 40}));
    EntryIndex compactedEntryIndex =
            new EntryIndex(fourEntriesHeader, parquetPosition, 1, EntryIndex.IndexType.COMPACT,
                    Optional.empty());


    @Test
    void testFactoryMethodWithStorageObjectCount() {
        EntryIndex normalIndex = EntryIndex.of(twoEntriesHeader, rawPosition, 2, 2);
        assertEquals(EntryIndex.IndexType.NORMAL, normalIndex.indexType());

        EntryIndex compactIndex = EntryIndex.of(twoEntriesHeader, rawPosition, 2, 1);
        assertEquals(EntryIndex.IndexType.COMPACT, compactIndex.indexType());
    }

    @Test
    void testSearchEntryHeader() {
        // not found index
        assertEquals(EntryHeader.NOT_FOUND, EntryIndex.NOT_FOUND.searchEntryHeader(110));

        // single entry index
        assertThrows(IllegalStateException.class, () -> singleEntryIndex.searchEntryHeader(99));
        assertEquals(firstHeader, singleEntryIndex.searchEntryHeader(100));
        assertEquals(firstHeader, singleEntryIndex.searchEntryHeader(105));
        assertThrows(IllegalStateException.class, () -> singleEntryIndex.searchEntryHeader(110));
        assertThrows(IllegalStateException.class, () -> singleEntryIndex.searchEntryHeader(115));
        assertThrows(IllegalStateException.class, () -> singleEntryIndex.searchEntryHeader(119));
        assertThrows(IllegalStateException.class, () -> singleEntryIndex.searchEntryHeader(120));
        assertThrows(IllegalStateException.class, () -> singleEntryIndex.searchEntryHeader(125));
        assertThrows(IllegalStateException.class, () -> singleEntryIndex.searchEntryHeader(130));
        assertThrows(IllegalStateException.class, () -> singleEntryIndex.searchEntryHeader(139));
        assertThrows(IllegalStateException.class, () -> singleEntryIndex.searchEntryHeader(140));
        assertThrows(IllegalStateException.class, () -> singleEntryIndex.searchEntryHeader(150));

        // two entries index
        assertThrows(IllegalStateException.class, () -> twoEntriesIndex.searchEntryHeader(99));
        assertEquals(firstHeader, twoEntriesIndex.searchEntryHeader(100));
        assertEquals(firstHeader, twoEntriesIndex.searchEntryHeader(105));
        assertEquals(firstHeader, twoEntriesIndex.searchEntryHeader(105));
        assertEquals(secondHeader, twoEntriesIndex.searchEntryHeader(110));
        assertEquals(secondHeader, twoEntriesIndex.searchEntryHeader(115));
        assertEquals(secondHeader, twoEntriesIndex.searchEntryHeader(119));
        assertThrows(IllegalStateException.class, () -> twoEntriesIndex.searchEntryHeader(120));
        assertThrows(IllegalStateException.class, () -> twoEntriesIndex.searchEntryHeader(125));
        assertThrows(IllegalStateException.class, () -> twoEntriesIndex.searchEntryHeader(130));
        assertThrows(IllegalStateException.class, () -> twoEntriesIndex.searchEntryHeader(139));
        assertThrows(IllegalStateException.class, () -> twoEntriesIndex.searchEntryHeader(140));
        assertThrows(IllegalStateException.class, () -> twoEntriesIndex.searchEntryHeader(150));


        // three entries index
        assertThrows(IllegalStateException.class, () -> threeEntriesIndex.searchEntryHeader(99));
        assertEquals(firstHeader, threeEntriesIndex.searchEntryHeader(100));
        assertEquals(firstHeader, threeEntriesIndex.searchEntryHeader(105));
        assertEquals(secondHeader, threeEntriesIndex.searchEntryHeader(110));
        assertEquals(secondHeader, threeEntriesIndex.searchEntryHeader(115));
        assertEquals(secondHeader, threeEntriesIndex.searchEntryHeader(119));
        assertEquals(thirdHeader, threeEntriesIndex.searchEntryHeader(120));
        assertEquals(thirdHeader, threeEntriesIndex.searchEntryHeader(125));
        assertThrows(IllegalStateException.class, () -> threeEntriesIndex.searchEntryHeader(130));
        assertThrows(IllegalStateException.class, () -> threeEntriesIndex.searchEntryHeader(139));
        assertThrows(IllegalStateException.class, () -> threeEntriesIndex.searchEntryHeader(140));
        assertThrows(IllegalStateException.class, () -> threeEntriesIndex.searchEntryHeader(150));


        // four entries index
        assertThrows(IllegalStateException.class, () -> fourEntriesIndex.searchEntryHeader(99));
        assertEquals(firstHeader, fourEntriesIndex.searchEntryHeader(100));
        assertEquals(firstHeader, fourEntriesIndex.searchEntryHeader(105));
        assertEquals(secondHeader, fourEntriesIndex.searchEntryHeader(110));
        assertEquals(secondHeader, fourEntriesIndex.searchEntryHeader(115));
        assertEquals(secondHeader, fourEntriesIndex.searchEntryHeader(119));
        assertEquals(thirdHeader, fourEntriesIndex.searchEntryHeader(120));
        assertEquals(thirdHeader, fourEntriesIndex.searchEntryHeader(125));
        assertEquals(fourthHeader, fourEntriesIndex.searchEntryHeader(130));
        assertEquals(fourthHeader, fourEntriesIndex.searchEntryHeader(139));
        assertThrows(IllegalStateException.class, () -> fourEntriesIndex.searchEntryHeader(140));
        assertThrows(IllegalStateException.class, () -> fourEntriesIndex.searchEntryHeader(150));



        // four entries index
        assertThrows(IllegalStateException.class, () -> fourEntriesIndex.searchEntryHeader(99));
        assertEquals(firstHeader, fourEntriesIndex.searchEntryHeader(100));
        assertEquals(firstHeader, fourEntriesIndex.searchEntryHeader(105));
        assertEquals(secondHeader, fourEntriesIndex.searchEntryHeader(110));
        assertEquals(secondHeader, fourEntriesIndex.searchEntryHeader(115));
        assertEquals(secondHeader, fourEntriesIndex.searchEntryHeader(119));
        assertEquals(thirdHeader, fourEntriesIndex.searchEntryHeader(120));
        assertEquals(thirdHeader, fourEntriesIndex.searchEntryHeader(125));
        assertEquals(fourthHeader, fourEntriesIndex.searchEntryHeader(130));
        assertEquals(fourthHeader, fourEntriesIndex.searchEntryHeader(139));
        assertThrows(IllegalStateException.class, () -> fourEntriesIndex.searchEntryHeader(140));
        assertThrows(IllegalStateException.class, () -> fourEntriesIndex.searchEntryHeader(150));


        // compacted entry index
        assertThrows(IllegalStateException.class, () -> compactedEntryIndex.searchEntryHeader(99));
        assertEquals(fourEntriesHeader, compactedEntryIndex.searchEntryHeader(100));
        assertEquals(fourEntriesHeader, compactedEntryIndex.searchEntryHeader(105));
        assertEquals(fourEntriesHeader, compactedEntryIndex.searchEntryHeader(110));
        assertEquals(fourEntriesHeader, compactedEntryIndex.searchEntryHeader(115));
        assertEquals(fourEntriesHeader, compactedEntryIndex.searchEntryHeader(119));
        assertEquals(fourEntriesHeader, compactedEntryIndex.searchEntryHeader(120));
        assertEquals(fourEntriesHeader, compactedEntryIndex.searchEntryHeader(125));
        assertEquals(fourEntriesHeader, compactedEntryIndex.searchEntryHeader(130));
        assertEquals(fourEntriesHeader, compactedEntryIndex.searchEntryHeader(139));
        assertThrows(IllegalStateException.class, () -> compactedEntryIndex.searchEntryHeader(140));
        assertThrows(IllegalStateException.class, () -> compactedEntryIndex.searchEntryHeader(150));

    }

    @Test
    void testGetEntryHeader() {
        // not found index
        assertEquals(EntryHeader.NOT_FOUND, EntryIndex.NOT_FOUND.getEntryHeader(110));

        // single entry index
        assertThrows(IllegalStateException.class, () -> singleEntryIndex.getEntryHeader(99));
        assertEquals(firstHeader, singleEntryIndex.getEntryHeader(100));
        assertThrows(IllegalStateException.class, () -> singleEntryIndex.getEntryHeader(105));
        assertThrows(IllegalStateException.class, () -> singleEntryIndex.getEntryHeader(110));
        assertThrows(IllegalStateException.class, () -> singleEntryIndex.getEntryHeader(115));

        // two entries index
        assertThrows(IllegalStateException.class, () -> twoEntriesIndex.getEntryHeader(99));
        assertEquals(firstHeader, twoEntriesIndex.getEntryHeader(100));
        assertThrows(IllegalStateException.class, () -> twoEntriesIndex.getEntryHeader(105));
        assertEquals(secondHeader, twoEntriesIndex.getEntryHeader(110));
        assertThrows(IllegalStateException.class, () -> twoEntriesIndex.getEntryHeader(120));

        // compacted entry index
        assertThrows(IllegalStateException.class, () -> compactedEntryIndex.getEntryHeader(99));
        assertEquals(fourEntriesHeader, compactedEntryIndex.getEntryHeader(100));
        assertThrows(IllegalStateException.class, () -> compactedEntryIndex.getEntryHeader(105));
        assertThrows(IllegalStateException.class, () -> compactedEntryIndex.getEntryHeader(110));
        assertThrows(IllegalStateException.class, () -> compactedEntryIndex.getEntryHeader(115));
    }

    @Test
    void testFirstEntryHeader() {
        assertEquals(EntryHeader.NOT_FOUND, EntryIndex.NOT_FOUND.getFirstEntryHeader());
        assertEquals(firstHeader, singleEntryIndex.getFirstEntryHeader());
        assertEquals(firstHeader, twoEntriesIndex.getFirstEntryHeader());
        assertEquals(firstHeader, threeEntriesIndex.getFirstEntryHeader());
        assertEquals(firstHeader, fourEntriesIndex.getFirstEntryHeader());
        assertEquals(fourEntriesHeader, compactedEntryIndex.getFirstEntryHeader());
    }

    @Test
    void testLastEntryHeader() {
        assertEquals(EntryHeader.NOT_FOUND, EntryIndex.NOT_FOUND.getLastEntryHeader());
        assertEquals(firstHeader, singleEntryIndex.getLastEntryHeader());
        assertEquals(secondHeader, twoEntriesIndex.getLastEntryHeader());
        assertEquals(thirdHeader, threeEntriesIndex.getLastEntryHeader());
        assertEquals(fourthHeader, fourEntriesIndex.getLastEntryHeader());
        assertEquals(fourEntriesHeader, compactedEntryIndex.getLastEntryHeader());
    }
}
