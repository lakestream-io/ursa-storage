/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.lakehouse.compact;

import io.lakestream.ursa.lakehouse.exception.TopicNotFoundException;
import io.lakestream.ursa.lakehouse.utils.TopicName;
import io.lakestream.ursa.storage.StorageApi;
import io.lakestream.ursa.storage.StreamProperties;
import io.lakestream.ursa.storage.impl.StorageConfig;
import io.lakestream.ursa.storage.impl.compaction.TopicManager;
import io.lakestream.ursa.storage.impl.compaction.TopicMetadata;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.function.Function;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;

/** Discovers compactable streams through the protocol-neutral {@link StorageApi}. */
@Slf4j
public final class StorageTopicManager implements TopicManager {

    private final StorageApi storageApi;
    private final Set<String> excludedNamespaces;
    private final Set<String> excludedTopics;
    /**
     * Reads a stream's catalog properties by log name, or null when the deployment has no catalog to
     * ask. The properties ride along on every compaction task, which is how a sink that names its
     * table from one - the Kafka topic name behind an incarnation-qualified stream, say - reaches it
     * on the commit side, where only the log name is otherwise available.
     */
    private final Function<String, Map<String, String>> streamPropertiesLookup;

    public StorageTopicManager(StorageApi storageApi, StorageConfig storageConfig) {
        this(storageApi, storageConfig, null);
    }

    public StorageTopicManager(StorageApi storageApi, StorageConfig storageConfig,
                               Function<String, Map<String, String>> streamPropertiesLookup) {
        this.storageApi = storageApi;
        this.excludedNamespaces = storageConfig.getBlackNamespaceOfCompact();
        this.excludedTopics = storageConfig.getBlackTopicOfCompact();
        this.streamPropertiesLookup = streamPropertiesLookup;
    }

    @Override
    public List<String> getAllTopics() {
        try {
            return storageApi.listStreamsWithProperties().get().values().stream()
                    .map(StreamProperties::key)
                    .filter(this::isIncluded)
                    .sorted()
                    .collect(Collectors.toList());
        } catch (Exception e) {
            log.error("Failed to list compactable streams", e);
            return Collections.emptyList();
        }
    }

    private boolean isIncluded(String topic) {
        if (topic == null || topic.isBlank()) {
            return false;
        }
        try {
            TopicName name = TopicName.get(topic);
            return !excludedNamespaces.contains(name.getNamespace())
                    && !excludedTopics.contains(topic)
                    && !excludedTopics.contains(name.getPartitionedTopicName());
        } catch (IllegalArgumentException e) {
            log.warn("Skipping stream with invalid canonical name {}", topic);
            return false;
        }
    }

    @Override
    public CompletableFuture<TopicMetadata> getTopicMetadata(String topic) {
        return storageApi.listStreamsWithProperties().thenApply(streams -> findTopic(streams, topic));
    }

    private TopicMetadata findTopic(Map<Long, StreamProperties> streams, String topic) {
        return streams.entrySet().stream()
                .filter(entry -> topic.equals(entry.getValue().key()))
                .findFirst()
                .map(entry -> new TopicMetadata(topic, entry.getKey(), streamProperties(topic)))
                .orElseThrow(() -> new CompletionException(
                        new TopicNotFoundException("The stream " + topic + " does not exist.")));
    }

    /** Never fails topic discovery over a catalog read: a task without properties still compacts. */
    private Map<String, String> streamProperties(String topic) {
        if (streamPropertiesLookup == null) {
            return Collections.emptyMap();
        }
        try {
            Map<String, String> properties = streamPropertiesLookup.apply(topic);
            return properties == null ? Collections.emptyMap() : properties;
        } catch (RuntimeException e) {
            log.warn("Failed to read catalog properties for stream {}; compacting without them", topic, e);
            return Collections.emptyMap();
        }
    }

    @Override
    public void close() {
        // StorageApi lifecycle is owned by the scheduler.
    }
}
