/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.api.materialization;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

class SortColumnTest {

    @Test
    void testConstruction() {
        SortColumn s = new SortColumn("event_ts", SortDirection.DESC, true);
        assertEquals("event_ts", s.column());
        assertEquals(SortDirection.DESC, s.direction());
        assertTrue(s.nullsFirst());
    }

    @Test
    void testEqualsHashCode() {
        SortColumn a = new SortColumn("c", SortDirection.ASC, false);
        SortColumn b = new SortColumn("c", SortDirection.ASC, false);
        SortColumn c = new SortColumn("c", SortDirection.DESC, false);
        SortColumn d = new SortColumn("c", SortDirection.ASC, true);
        assertEquals(a, b);
        assertEquals(a.hashCode(), b.hashCode());
        assertNotEquals(a, c);
        assertNotEquals(a, d);
    }

    @Test
    void testRejectsNulls() {
        assertThrows(NullPointerException.class, () -> new SortColumn(null, SortDirection.ASC, false));
        assertThrows(NullPointerException.class, () -> new SortColumn("c", null, false));
    }

    @Test
    void testRejectsEmptyColumn() {
        assertThrows(IllegalArgumentException.class, () -> new SortColumn("", SortDirection.ASC, false));
    }
}
