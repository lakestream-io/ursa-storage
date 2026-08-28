/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.lakehouse.delta;

import io.lakestream.ursa.compaction.task.CompactStreamTask;
import io.lakestream.ursa.lakehouse.writer.ParquetFileStat;
import java.io.Serial;
import java.util.List;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
public class DeltaCompactStreamTask extends CompactStreamTask {

    @Serial
    private static final long serialVersionUID = 1240188677593342229L;

    List<ParquetFileStat> deltaFiles;
    List<ParquetFileStat> dltDeltaFiles;

    public DeltaCompactStreamTask(CompactStreamTask task) {
        super(task.getStreamId(), task.getStartOffset(), task.getEndOffset(), task.getTotalSize(),
                task.getCumulativeSize(), task.getTopic(), task.getTaskName(), task.getStatus(), task.getFilePath(),
                task.getFileFullPath(), task.getFileSize(), task.getPartitionValues(), task.getUnCommittedIndex(),
                task.getStats(), task.getRealStartOffset(), task.getRealEndOffset(), task.getMessageWrittenToUrsaTime(),
                task.getProperties(), task.getNumberOfRecordsInCompactedFile(),
                task.getTaskQueueType(), task.getManagedWriteResults());
    }
}
