/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.materialization.serde.kafka;

import io.lakestream.api.SourceMetadataProperties;
import java.util.Map;

/** Metadata that connects a UUID-qualified storage stream to its logical Kafka topic. */
public final class KafkaSourceMetadata {

    /** Source-neutral stream property supplied by source integrations for logical-name resolution. */
    public static final String LOGICAL_NAME_PROPERTY = SourceMetadataProperties.LOGICAL_NAME_PROPERTY;

    /** Legacy Kafka-specific stream property retained for existing streams. */
    public static final String TOPIC_NAME_PROPERTY = "lakestream.kafka.topic.name";

    private KafkaSourceMetadata() {
    }

    /**
     * Returns the logical Kafka topic used for schema lookup.
     *
     * <p>The explicit stream property is authoritative because UUID-qualified storage names are not
     * a reversible representation of every valid Kafka topic name. The fallback is only for streams
     * created directly without Kafka lifecycle metadata.
     */
    public static String topicName(String fallbackStreamName, Map<String, String> streamProperties) {
        if (streamProperties != null) {
            String logicalName = streamProperties.get(LOGICAL_NAME_PROPERTY);
            if (logicalName != null && !logicalName.isBlank()) {
                return logicalName;
            }
            String topicName = streamProperties.get(TOPIC_NAME_PROPERTY);
            if (topicName != null && !topicName.isBlank()) {
                return topicName;
            }
        }
        return partitionedLocalName(fallbackStreamName);
    }

    private static String partitionedLocalName(String topic) {
        int slash = topic.lastIndexOf('/');
        String localName = slash < 0 ? topic : topic.substring(slash + 1);
        int suffix = localName.lastIndexOf("-partition-");
        if (suffix < 0) {
            return localName;
        }
        try {
            int partition = Integer.parseInt(localName.substring(suffix + "-partition-".length()));
            return partition < 0 ? localName : localName.substring(0, suffix);
        } catch (NumberFormatException ignored) {
            return localName;
        }
    }
}
