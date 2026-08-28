/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.compaction.task;

import java.io.Serial;
import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;

/** A compaction task whose logical offset range is {@code [startOffset, endOffset)}. */
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class CompactStreamTask implements Comparable<CompactStreamTask>, Serializable {


    @Serial
    private static final long serialVersionUID = 795844312150124488L;

    public enum TaskQueueType {
        NORMAL,
        DLQ
    }

    public static final int INIT = 0;
    public static final int COMPACTED = 1;
    public static final int PREPARED_COMMIT = 2;
    public static final int COMMITTED = 3;

    protected long streamId;
    /** Inclusive first logical offset in this task. */
    protected long startOffset;
    /** Exclusive upper bound of the logical offsets in this task. */
    protected long endOffset;
    protected long totalSize;
    protected long cumulativeSize;
    protected String topic;
    protected String taskName;
    protected int status;
    protected String filePath;
    protected String fileFullPath;
    protected Long fileSize;
    protected Map<String, String> partitionValues;
    protected List<Integer> unCommittedIndex;

    @ToString.Exclude
    protected String stats;
    protected long realStartOffset;
    protected long realEndOffset;
    protected long messageWrittenToUrsaTime;

    // topic properties
    protected Map<String, String> properties;

    // Total number of logical records represented by the compacted file.
    protected int numberOfRecordsInCompactedFile;

    private TaskQueueType taskQueueType = TaskQueueType.NORMAL;
    private TreeSet<ManagedWriteResult> managedWriteResults = new TreeSet<ManagedWriteResult>();

    public CompactStreamTask(long streamId, long startOffset, long endOffset, long totalSize, long cumulativeSize,
                             String topic, String taskName, int status, Map<String, String> properties) {
        this.streamId = streamId;
        this.startOffset = startOffset;
        this.endOffset = endOffset;
        this.totalSize = totalSize;
        this.cumulativeSize = cumulativeSize;
        this.topic = topic;
        this.taskName = taskName;
        this.status = status;
        this.properties = properties;
    }

    @Override
    public int compareTo(CompactStreamTask o) {
        var streamIdCompare = Long.compare(this.streamId, o.streamId);
        if (streamIdCompare != 0) {
            return streamIdCompare;
        }
        var startOffsetCompare = Long.compare(this.startOffset, o.startOffset);
        if (startOffsetCompare != 0) {
            return startOffsetCompare;
        }
        return Long.compare(this.endOffset, o.endOffset);
    }
}
