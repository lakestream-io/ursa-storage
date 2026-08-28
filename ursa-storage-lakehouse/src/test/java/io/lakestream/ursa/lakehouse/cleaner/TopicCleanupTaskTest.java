/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.lakehouse.cleaner;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

class TopicCleanupTaskTest {

    @Test
    void returnsCanonicalNonPartitionedTopic() {
        TopicCleanupTask task = new TopicCleanupTask("default/orders-partition-2", 1L, 10L);

        assertEquals("default/orders", task.getCompactionTopic());
    }
}
