/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.lakestream.impl;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.lakestream.api.LogId;
import io.lakestream.api.PartitioningStrategy;
import io.lakestream.api.RoutingKey;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import org.junit.jupiter.api.Test;

class IndexedLayoutTest {

    @Test
    void testResolveForWriteWithIndex() throws Exception {
        List<LogId> logIds = List.of(LogId.of(101), LogId.of(102), LogId.of(103));
        IndexedLayout layout = new IndexedLayout(logIds);

        assertEquals(LogId.of(101), layout.resolveForWrite(RoutingKey.ofIndex(0)).get());
        assertEquals(LogId.of(102), layout.resolveForWrite(RoutingKey.ofIndex(1)).get());
        assertEquals(LogId.of(103), layout.resolveForWrite(RoutingKey.ofIndex(2)).get());
    }

    @Test
    void testResolveForWriteWithInvalidIndex() {
        List<LogId> logIds = List.of(LogId.of(101), LogId.of(102));
        IndexedLayout layout = new IndexedLayout(logIds);

        ExecutionException ex = assertThrows(ExecutionException.class,
            () -> layout.resolveForWrite(RoutingKey.ofIndex(5)).get());
        assertTrue(ex.getCause() instanceof IllegalArgumentException);
    }

    @Test
    void testResolveForWriteRoundRobin() throws Exception {
        List<LogId> logIds = List.of(LogId.of(101), LogId.of(102), LogId.of(103));
        IndexedLayout layout = new IndexedLayout(logIds);

        // Round-robin should cycle through all log IDs
        Set<LogId> seen = new HashSet<>();
        for (int i = 0; i < 6; i++) {
            seen.add(layout.resolveForWrite(RoutingKey.roundRobin()).get());
        }
        assertEquals(3, seen.size());
    }

    @Test
    void testLogIds() throws Exception {
        List<LogId> logIds = List.of(LogId.of(101), LogId.of(102), LogId.of(103));
        IndexedLayout layout = new IndexedLayout(logIds);

        assertEquals(logIds, layout.logIds().get());
    }

    @Test
    void testLogCount() {
        List<LogId> logIds = List.of(LogId.of(101), LogId.of(102), LogId.of(103));
        IndexedLayout layout = new IndexedLayout(logIds);

        assertEquals(3, layout.logCount());
    }

    @Test
    void testPartitioning() {
        List<LogId> logIds = List.of(LogId.of(101), LogId.of(102), LogId.of(103));
        IndexedLayout layout = new IndexedLayout(logIds);

        assertEquals(PartitioningStrategy.INDEXED, layout.partitioning().strategy());
        assertEquals(3, layout.partitioning().numPartitions());
    }
}
