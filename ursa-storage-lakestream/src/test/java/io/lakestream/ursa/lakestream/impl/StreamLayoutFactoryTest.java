/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.lakestream.impl;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.lakestream.api.LogId;
import io.lakestream.api.Partitioning;
import io.lakestream.api.PartitioningStrategy;
import io.lakestream.api.StreamLayout;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class StreamLayoutFactoryTest {

    @Test
    void testCreateIndexedLayout() {
        Partitioning partitioning = new Partitioning(PartitioningStrategy.INDEXED,
            Map.of("numPartitions", "3"));
        List<LogId> logIds = List.of(LogId.of(1), LogId.of(2), LogId.of(3));

        StreamLayout layout = StreamLayoutFactory.create(partitioning, logIds);

        assertTrue(layout instanceof IndexedLayout);
        assertEquals(3, layout.logCount());
    }

    @Test
    void testCreateSingleElementList() {
        Partitioning partitioning = new Partitioning(PartitioningStrategy.INDEXED,
            Map.of("numPartitions", "1"));
        List<LogId> logIds = List.of(LogId.of(1));

        StreamLayout layout = StreamLayoutFactory.create(partitioning, logIds);

        assertTrue(layout instanceof IndexedLayout);
        assertEquals(1, layout.logCount());
    }

    @Test
    void testUnsupportedStrategy() {
        Partitioning partitioning = new Partitioning(PartitioningStrategy.RANGE, Map.of());
        List<LogId> logIds = List.of(LogId.of(1), LogId.of(2));

        assertThrows(UnsupportedOperationException.class,
            () -> StreamLayoutFactory.create(partitioning, logIds));
    }
}
