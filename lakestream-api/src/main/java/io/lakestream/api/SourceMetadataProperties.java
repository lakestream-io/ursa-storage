/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.api;

import java.util.Map;
import java.util.Objects;

/** Reserved stream properties that describe the logical source behind a storage stream. */
public final class SourceMetadataProperties {

    /**
     * Source-owned logical name used for user-facing defaults, such as an SDT table name.
     *
     * <p>This is metadata, not the storage identity. Connectors must set it themselves and prevent
     * users from overriding it.
     */
    public static final String LOGICAL_NAME_PROPERTY = "lakestream.source.logical.name";

    /** Compatibility key written by Kafka integrations before the source-neutral property existed. */
    private static final String KAFKA_TOPIC_NAME_PROPERTY = "lakestream.kafka.topic.name";

    private SourceMetadataProperties() {
    }

    /**
     * Returns the source's logical name, falling back to the storage stream name.
     *
     * <p>The Kafka-specific key remains a compatibility fallback for existing streams. New source
     * integrations should populate {@link #LOGICAL_NAME_PROPERTY}.
     */
    public static String logicalName(StreamIdentifier streamId, Map<String, String> properties) {
        Objects.requireNonNull(streamId, "streamId");
        Objects.requireNonNull(properties, "properties");
        String logicalName = properties.get(LOGICAL_NAME_PROPERTY);
        if (logicalName != null && !logicalName.isBlank()) {
            return logicalName;
        }
        String kafkaTopicName = properties.get(KAFKA_TOPIC_NAME_PROPERTY);
        if (kafkaTopicName != null && !kafkaTopicName.isBlank()) {
            return kafkaTopicName;
        }
        return streamId.name();
    }
}
