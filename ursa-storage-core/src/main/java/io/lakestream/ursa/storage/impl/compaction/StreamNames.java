/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.storage.impl.compaction;

/** Helpers for logical stream names stored in compaction tasks. */
final class StreamNames {

    private static final String PARTITION_SUFFIX = "-partition-";
    private StreamNames() {
    }

    static String partitionedTopicName(String topic) {
        if (topic == null || topic.isBlank()) {
            throw new IllegalArgumentException("Stream name must not be blank");
        }
        int localNameStart = topic.lastIndexOf('/') + 1;
        String localName = topic.substring(localNameStart);
        int suffixStart = localName.lastIndexOf(PARTITION_SUFFIX);
        if (suffixStart < 0) {
            return topic;
        }

        String partitionText = localName.substring(suffixStart + PARTITION_SUFFIX.length());
        final int partition;
        try {
            partition = Integer.parseInt(partitionText);
        } catch (NumberFormatException ignored) {
            return topic;
        }
        if (partition < 0 || !partitionText.equals(Integer.toString(partition))) {
            return topic;
        }
        return topic.substring(0, localNameStart + suffixStart);
    }
}
