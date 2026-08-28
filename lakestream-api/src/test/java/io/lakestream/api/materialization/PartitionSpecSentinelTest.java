/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.api.materialization;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

class PartitionSpecSentinelTest {

    @Test
    void testStreamPartitionSentinel() {
        PartitionSpec p = PartitionSpec.streamPartition();
        assertEquals("__partition", p.column());
        assertEquals(PartitionSpec.STREAM_PARTITION_COLUMN, p.column());
        assertEquals(PartitionTransform.IDENTITY, p.transform());
        assertTrue(p.parameter().isEmpty());
    }

    @Test
    void testStreamPartitionEqualsItself() {
        assertEquals(PartitionSpec.streamPartition(), PartitionSpec.streamPartition());
    }
}
