/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.lakehouse.compact;

import io.lakestream.ursa.compaction.task.CompactStreamTask;
import io.lakestream.ursa.exception.ExceptionWithCode;
import java.util.List;

public interface CommitRunner {

    /**
     * The tasks must be ordered by the CompactStreamTask. You need to ensure it is sorted before calling this method.
     *
     * @param tasks
     */
    void commit(List<CompactStreamTask> tasks) throws ExceptionWithCode;

    default boolean needToPublishToDLQ() {
        return false;
    }

    void close();
}
