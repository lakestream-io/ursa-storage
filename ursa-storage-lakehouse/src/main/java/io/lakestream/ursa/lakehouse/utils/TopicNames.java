/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.lakehouse.utils;

import lombok.experimental.UtilityClass;

/**
 * Topic-name operations used by the Kafka lakehouse path.
 *
 * <p>The canonical form is {@code namespace/name-partition-N}; unqualified Kafka topic names use
 * the {@code default} namespace.
 */
@UtilityClass
public class TopicNames {

    public static String canonical(String name) {
        return TopicName.get(name).toString();
    }

    public static String partitionedTopicName(String name) {
        return TopicName.get(name).getPartitionedTopicName();
    }

    public static String localName(String name) {
        return TopicName.get(name).getLocalName();
    }

    public static String partitionedLocalName(String name) {
        return TopicName.get(partitionedTopicName(name)).getLocalName();
    }

    public static int partitionIndex(String name) {
        return TopicName.get(name).getPartitionIndex();
    }

    public static String storagePath(String storageRoot, String name) {
        TopicName topicName = TopicName.get(partitionedTopicName(name));
        return storageRoot + "/" + topicName.getNamespace() + "/" + topicName.getLocalName();
    }
}
