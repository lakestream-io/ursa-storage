/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.storage.utils;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.lakestream.ursa.storage.impl.utils.FIFOCache;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

public class FIFOCacheTest {

    @Test
    void testCacheCapacity() {
        int capacity = 3;
        List<Map.Entry<String, Integer>> removedEntries = new ArrayList<>();
        FIFOCache<String, Integer> cache = new FIFOCache<>(capacity, removedEntries::add);

        cache.put("A", 1);
        cache.put("B", 2);
        cache.put("C", 3);

        assertEquals(3, cache.size());
        assertTrue(cache.containsKey("A"));
        assertTrue(cache.containsKey("B"));
        assertTrue(cache.containsKey("C"));

        cache.put("D", 4);

        assertEquals(3, cache.size());
        assertFalse(cache.containsKey("A"));
        assertTrue(cache.containsKey("B"));
        assertTrue(cache.containsKey("C"));
        assertTrue(cache.containsKey("D"));

        assertEquals(1, removedEntries.size());
        assertEquals("A", removedEntries.get(0).getKey());
        assertEquals(1, removedEntries.get(0).getValue());
    }

    @Test
    void testFIFOOrder() {
        int capacity = 3;
        List<Map.Entry<String, Integer>> removedEntries = new ArrayList<>();
        FIFOCache<String, Integer> cache = new FIFOCache<>(capacity, removedEntries::add);

        cache.put("A", 1);
        cache.put("B", 2);
        cache.put("C", 3);
        cache.put("D", 4);
        cache.put("E", 5);

        assertEquals(3, cache.size());
        assertFalse(cache.containsKey("A"));
        assertFalse(cache.containsKey("B"));
        assertTrue(cache.containsKey("C"));
        assertTrue(cache.containsKey("D"));
        assertTrue(cache.containsKey("E"));

        assertEquals(2, removedEntries.size());
        assertEquals("A", removedEntries.get(0).getKey());
        assertEquals("B", removedEntries.get(1).getKey());
    }

    @Test
    void testRemovalListener() {
        int capacity = 2;
        List<Map.Entry<String, Integer>> removedEntries = new ArrayList<>();
        FIFOCache<String, Integer> cache = new FIFOCache<>(capacity, removedEntries::add);

        cache.put("A", 1);
        cache.put("B", 2);
        cache.put("C", 3);

        assertEquals(1, removedEntries.size());
        assertEquals("A", removedEntries.get(0).getKey());
        assertEquals(1, removedEntries.get(0).getValue());

        cache.put("D", 4);

        assertEquals(2, removedEntries.size());
        assertEquals("B", removedEntries.get(1).getKey());
        assertEquals(2, removedEntries.get(1).getValue());
    }
}
