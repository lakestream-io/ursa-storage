/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.lakestream.impl;

import io.lakestream.api.CatalogPaths;
import io.lakestream.api.ExternalStreamRegistry;
import io.lakestream.api.StreamIdentifier;
import io.oxia.client.api.AsyncOxiaClient;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import lombok.extern.slf4j.Slf4j;

/** Oxia-backed metadata-only external stream registry. */
@Slf4j
final class IndexedExternalStreamRegistry implements ExternalStreamRegistry {

    private final IndexedStreamConfigStore streamConfigStore;
    private final List<AutoCloseable> ownedResources;
    private final AtomicBoolean closed = new AtomicBoolean();

    IndexedExternalStreamRegistry(
            AsyncOxiaClient oxiaClient, CatalogPaths catalogPaths,
            List<AutoCloseable> ownedResources) {
        this.streamConfigStore = new IndexedStreamConfigStore(oxiaClient, catalogPaths);
        this.ownedResources = List.copyOf(ownedResources);
    }

    @Override
    public CompletableFuture<Void> registerExternalStream(
            StreamIdentifier id, int partitionCount, Map<String, String> properties) {
        return streamConfigStore.registerExternalStream(id, partitionCount, properties);
    }

    @Override
    public CompletableFuture<Void> unregisterExternalStream(StreamIdentifier id) {
        return streamConfigStore.unregisterExternalStream(id);
    }

    @Override
    public void close() {
        if (!closed.compareAndSet(false, true)) {
            return;
        }
        for (AutoCloseable resource : ownedResources) {
            try {
                resource.close();
            } catch (Exception e) {
                log.warn("Failed to close external stream registry resource: {}",
                    resource.getClass().getSimpleName(), e);
            }
        }
    }
}
