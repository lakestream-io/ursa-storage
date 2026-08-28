/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.lakehouse.compact;


import io.lakestream.ursa.compaction.task.CompactStreamTask;

public interface CompactionTaskProcessor extends AutoCloseable {

    void doCompact(CompactStreamTask task) throws Exception;

}
