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
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;

/** Discovers compactable streams through the protocol-neutral {@link StorageApi}. */
@Slf4j
public final class StorageTopicManager implements TopicManager {

    private final StorageApi storageApi;
    private final Set<String> excludedNamespaces;
    private final Set<String> excludedTopics;

    public StorageTopicManager(StorageApi storageApi, StorageConfig storageConfig) {
        this.storageApi = storageApi;
        this.excludedNamespaces = storageConfig.getBlackNamespaceOfCompact();
        this.excludedTopics = storageConfig.getBlackTopicOfCompact();
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

    private static TopicMetadata findTopic(Map<Long, StreamProperties> streams, String topic) {
        return streams.entrySet().stream()
                .filter(entry -> topic.equals(entry.getValue().key()))
                .findFirst()
                .map(entry -> new TopicMetadata(topic, entry.getKey(), Collections.emptyMap()))
                .orElseThrow(() -> new CompletionException(
                        new TopicNotFoundException("The stream " + topic + " does not exist.")));
    }

    @Override
    public void close() {
        // StorageApi lifecycle is owned by the scheduler.
    }
}
