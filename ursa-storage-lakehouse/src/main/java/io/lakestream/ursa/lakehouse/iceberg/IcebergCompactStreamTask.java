/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.lakehouse.iceberg;

import io.lakestream.ursa.compaction.task.CompactStreamTask;
import java.io.Serial;
import java.util.List;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.iceberg.io.WriteResult;

@Data
@NoArgsConstructor
public class IcebergCompactStreamTask extends CompactStreamTask {

    @Serial
    private static final long serialVersionUID = -7460184575118697360L;

    @Deprecated
    protected WriteResult writeResult;
    protected List<WriteResult> writeResults;
    protected List<WriteResult> dltWriteResults;

    public IcebergCompactStreamTask(CompactStreamTask task) {
        super(task.getStreamId(), task.getStartOffset(), task.getEndOffset(), task.getTotalSize(),
            task.getCumulativeSize(), task.getTopic(), task.getTaskName(), task.getStatus(), task.getFilePath(),
            task.getFileFullPath(), task.getFileSize(), task.getPartitionValues(), task.getUnCommittedIndex(),
            task.getStats(), task.getRealStartOffset(), task.getRealEndOffset(), task.getMessageWrittenToUrsaTime(),
            task.getProperties(), task.getNumberOfRecordsInCompactedFile(),
                task.getTaskQueueType(), task.getManagedWriteResults());
    }
}
