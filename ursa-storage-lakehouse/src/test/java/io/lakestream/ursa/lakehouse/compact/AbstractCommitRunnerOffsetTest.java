/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.lakehouse.compact;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.lakestream.ursa.compaction.task.CompactStreamTask;
import org.junit.jupiter.api.Test;

class AbstractCommitRunnerOffsetTest {

    @Test
    void recordsLastIncludedOffsetForCompactedMetric() {
        AbstractCommitRunner runner = new AbstractCommitRunner(null, null);
        CompactStreamTask task = task(0L, 10L);

        runner.updateLastCommitOffset(task);

        assertEquals(9L, runner.lastCommitOffset.get(task.getTopic()).offset());
    }

    @Test
    void rejectsEmptyTaskRange() {
        AbstractCommitRunner runner = new AbstractCommitRunner(null, null);

        assertThrows(IllegalArgumentException.class,
                () -> runner.updateLastCommitOffset(task(10L, 10L)));
    }

    private static CompactStreamTask task(long startOffset, long endOffset) {
        CompactStreamTask task = new CompactStreamTask();
        task.setTopic("default/test-partition-0");
        task.setStreamId(1L);
        task.setStartOffset(startOffset);
        task.setEndOffset(endOffset);
        return task;
    }
}
