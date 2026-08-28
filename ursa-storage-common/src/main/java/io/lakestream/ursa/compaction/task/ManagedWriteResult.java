/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.compaction.task;

import java.io.Serial;
import java.io.Serializable;
import lombok.Builder;
import lombok.Data;
import org.jetbrains.annotations.NotNull;

@Builder
@Data
public class ManagedWriteResult implements Comparable<ManagedWriteResult>, Serializable {

    @Serial
    private static final long serialVersionUID = 1L;

    private String filePath;
    private String fullFilePath;
    private long fileSize;
    private int numberOfMessages;
    private long lastEntryId;
    private long lastBatchId;

    @Override
    public int compareTo(@NotNull ManagedWriteResult o) {
        int c = Long.compare(this.lastEntryId, o.lastEntryId);
        if (c != 0) {
            return c;
        }
        return Long.compare(this.lastBatchId, o.lastBatchId);
    }
}
