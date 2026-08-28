/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.api;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Map;
import org.junit.jupiter.api.Test;

class PartitioningTest {

    @Test
    void testNumPartitions() {
        Partitioning p = new Partitioning(PartitioningStrategy.INDEXED, Map.of("numPartitions", "3"));
        assertEquals(3, p.numPartitions());
    }

    @Test
    void testDefaultPartitionCount() {
        Partitioning p = new Partitioning(PartitioningStrategy.INDEXED, Map.of());
        assertEquals(1, p.numPartitions());
    }

    @Test
    void testStrategy() {
        Partitioning p = new Partitioning(PartitioningStrategy.INDEXED, Map.of("numPartitions", "5"));
        assertEquals(PartitioningStrategy.INDEXED, p.strategy());
    }
}
