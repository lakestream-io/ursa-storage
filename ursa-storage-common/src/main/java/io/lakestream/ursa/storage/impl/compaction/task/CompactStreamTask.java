/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.storage.impl.compaction.task;

import java.io.Serial;
import java.io.Serializable;
import java.util.List;
import java.util.Map;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.ToString;

@Data
@NoArgsConstructor
@AllArgsConstructor
@EqualsAndHashCode
public class CompactStreamTask implements Comparable<CompactStreamTask>, Serializable {

    @Serial
    private static final long serialVersionUID = -673898798706482978L;

    public static final int INIT = 0;
    public static final int COMPACTED = 1;
    public static final int PREPARED_COMMIT = 2;
    public static final int COMMITTED = 3;
    protected long streamId;
    protected long startOffset;
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
        // if stream id and start offset are the same, return 1 to indicate that this task is greater
        // we don't want to return 0 here because it will cause issues in sorting, and we don't care about
        // the order of tasks with the same stream id and start offset
        return 1;
    }

    public io.lakestream.ursa.compaction.task.CompactStreamTask toCompactStreamTask() {
        io.lakestream.ursa.compaction.task.CompactStreamTask compactStreamTask =
                new io.lakestream.ursa.compaction.task.CompactStreamTask();
        compactStreamTask.setStreamId(this.streamId);
        compactStreamTask.setStartOffset(this.startOffset);
        compactStreamTask.setEndOffset(this.endOffset);
        compactStreamTask.setTotalSize(this.totalSize);
        compactStreamTask.setCumulativeSize(this.cumulativeSize);
        compactStreamTask.setTopic(this.topic);
        compactStreamTask.setTaskName(this.taskName);
        compactStreamTask.setStatus(this.status);
        compactStreamTask.setFilePath(this.filePath);
        compactStreamTask.setFileFullPath(this.fileFullPath);
        compactStreamTask.setFileSize(this.fileSize);
        compactStreamTask.setPartitionValues(this.partitionValues);
        compactStreamTask.setUnCommittedIndex(this.unCommittedIndex);
        compactStreamTask.setStats(this.stats);
        compactStreamTask.setRealStartOffset(this.realStartOffset);
        compactStreamTask.setRealEndOffset(this.realEndOffset);
        compactStreamTask.setMessageWrittenToUrsaTime(this.messageWrittenToUrsaTime);
        compactStreamTask.setProperties(this.properties);
        return compactStreamTask;
    }
}
