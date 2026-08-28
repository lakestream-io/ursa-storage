/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.api;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.Test;

class LogIdTest {

    @Test
    void testConstruction() {
        LogId logId = new LogId(123);
        assertEquals(123, logId.id());
    }

    @Test
    void testOfFactory() {
        LogId logId = LogId.of(456);
        assertEquals(456, logId.id());
    }

    @Test
    void testEquality() {
        LogId a = LogId.of(100);
        LogId b = LogId.of(100);
        LogId c = LogId.of(200);
        assertEquals(a, b);
        assertNotEquals(a, c);
    }

    @Test
    void testAsMapKey() {
        Map<LogId, String> map = new HashMap<>();
        map.put(LogId.of(1), "first");
        map.put(LogId.of(2), "second");
        assertEquals("first", map.get(LogId.of(1)));
        assertEquals("second", map.get(LogId.of(2)));
    }

    @Test
    void testAsSetElement() {
        Set<LogId> set = new HashSet<>();
        set.add(LogId.of(1));
        set.add(LogId.of(1));
        set.add(LogId.of(2));
        assertEquals(2, set.size());
    }
}
