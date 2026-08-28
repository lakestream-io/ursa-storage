/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.compaction.task;

import java.util.Map;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/** A prepared compaction task whose logical offset range is {@code [startOffset, endOffset)}. */
@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class PreparedCompactStreamTask {

    public static final int INIT = 0;
    public static final int PUSHED_TASK = 1;

    private long streamId;
    /** Inclusive first logical offset in this task. */
    private long startOffset;
    /** Exclusive upper bound of the logical offsets in this task. */
    private long endOffset;
    private long totalSize;
    private long cumulativeSize;
    private int status;
    private String taskName;
    private String topic;
    private Map<String, String> properties;

    public CompactStreamTask toCompactStreamTask() {
        return new CompactStreamTask(streamId, startOffset, endOffset, totalSize, cumulativeSize, topic, taskName,
                CompactStreamTask.INIT, properties);
    }

    @Override
    public String toString() {
        return "PreparedCompactStreamTask{"
               + "streamId=" + streamId
               + ", startOffset=" + startOffset
               + ", endOffset=" + endOffset
               + ", taskName=" + taskName
               + ", topic=" + topic
               + '}';
    }
}
