/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.lakehouse.cleaner;

import io.lakestream.ursa.lakehouse.utils.TopicName;

public record TopicCleanupTask(String topic, long streamId, long markDeletedOffset) {
    /**
     * Get the canonical, non-partitioned stream name, for example {@code default/orders}.
     *
     * @return the compaction format topic name
     */
    public String getCompactionTopic() {
        return TopicName.get(topic).getPartitionedTopicName();
    }
}
