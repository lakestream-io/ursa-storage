/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.lakehouse.compact;

import com.google.common.annotations.VisibleForTesting;
import io.lakestream.ursa.compaction.CompactTaskManager;
import io.lakestream.ursa.compaction.metrics.CompactionMetrics;
import io.lakestream.ursa.compaction.task.CompactStreamTask;
import io.lakestream.ursa.lakehouse.LakehouseConfiguration;
import io.lakestream.ursa.lakehouse.schema.KafkaSchemaRegistry;
import io.lakestream.ursa.lakehouse.schema.SchemaRegistry;
import io.lakestream.ursa.lakehouse.v2.LakehouseFactory;
import io.lakestream.ursa.materialization.serde.SchemaService;
import io.lakestream.ursa.materialization.serde.kafka.KafkaSchemaService;
import io.lakestream.ursa.storage.BaseStreamIDGenerator;
import io.lakestream.ursa.storage.FileStorage;
import io.lakestream.ursa.storage.StorageApi;
import io.lakestream.ursa.storage.impl.StorageConfig;
import io.lakestream.ursa.storage.impl.compaction.CompactionService;
import io.netty.buffer.ByteBufAllocator;
import io.oxia.client.api.AsyncOxiaClient;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

/** Lakehouse compaction backed by entries read from Ursa storage. */
@Slf4j
public class LakehouseCompactionServiceImpl implements CompactionService {

    private StorageApi storageApi;
    private CompactTaskManager compactTaskManager;
    private SchemaRegistry schemaRegistry;
    private StorageConfig config;
    private CompactionMetrics compactionMetrics;
    private volatile CompactionResources resources;

    @VisibleForTesting
    @Getter
    private final Map<String, CompactionTaskProcessor> compactWorkers = new ConcurrentHashMap<>();

    @Override
    public void compactStream(String topic, long streamId) {
        // The range-bearing CompactStreamTask overload is the supported entry point.
    }

    @Override
    public void compactStream(CompactStreamTask task) throws Exception {
        getCompactWorker(task).doCompact(task);
        compactionMetrics.getOngoingCompactionTopicCount().set(compactWorkers.size());
    }

    protected CompactionTaskProcessor getCompactWorker(CompactStreamTask task) {
        return compactWorkers.compute(task.getTopic(), (topic, existingWorker) -> {
            if (existingWorker instanceof LakehouseCompactionWorker) {
                return existingWorker;
            }
            closeWorker(existingWorker);
            CompactionResources sourceResources = resources();
            return new LakehouseCompactionWorker(
                    sourceResources.lakehouseFactory(),
                    sourceResources.entryReaderFactory(),
                    compactTaskManager,
                    compactionMetrics,
                    config);
        });
    }

    private static void closeWorker(CompactionTaskProcessor worker) {
        if (worker == null) {
            return;
        }
        try {
            worker.close();
        } catch (Exception e) {
            log.warn("Failed to close superseded lakehouse compaction worker", e);
        }
    }

    @Override
    public void initialize(ByteBufAllocator allocator, FileStorage fileStorage, BaseStreamIDGenerator idGenerator,
                           StorageApi storageApi, CompactTaskManager compactTaskManager,
                           StorageConfig config, AsyncOxiaClient oxiaClient,
                           CompactionMetrics compactionMetrics, Object ctx) {
        this.storageApi = Objects.requireNonNull(storageApi, "storageApi");
        this.compactTaskManager = Objects.requireNonNull(compactTaskManager, "compactTaskManager");
        this.config = Objects.requireNonNull(config, "config");
        this.compactionMetrics = Objects.requireNonNull(compactionMetrics, "compactionMetrics");
        this.schemaRegistry = (SchemaRegistry) Objects.requireNonNull(ctx, "schemaRegistry");
    }

    @Override
    public void maintenance() {
        try {
            CompactionResources current = resources;
            if (current != null) {
                current.cleanUp();
            }
        } catch (Throwable t) {
            log.warn("Failed to maintain lakehouse compaction resources", t);
        }
    }

    private synchronized CompactionResources resources() {
        if (resources != null) {
            return resources;
        }
        if (!(schemaRegistry instanceof KafkaSchemaRegistry registry)) {
            throw new IllegalStateException("Lakehouse compaction requires a Kafka-compatible schema registry");
        }
        LakehouseConfiguration lakehouseConfig = new LakehouseConfiguration(config.getProperties());
        SchemaService<?> schemaService = new KafkaSchemaService(registry.getSchemaRegistryClient(), false);
        EntryProcessFactory entryReaderFactory = new UrsaEntryProcessFactory(storageApi, compactionMetrics);
        LakehouseFactory lakehouseFactory = new LakehouseFactory(
                lakehouseConfig, schemaService, compactionMetrics.getProvider());
        resources = new CompactionResources(lakehouseFactory, entryReaderFactory, schemaService);
        return resources;
    }

    public void invalidateCompactWorker(String topic) {
        closeWorker(compactWorkers.remove(topic));
    }

    @Override
    public void close() {
        compactWorkers.values().forEach(LakehouseCompactionServiceImpl::closeWorker);
        compactWorkers.clear();
        closeResources();
    }

    private synchronized void closeResources() {
        CompactionResources current = resources;
        resources = null;
        if (current != null) {
            current.close();
        }
    }

    public static boolean checkTopicPropertiesUpdated(Map<String, String> oldProperties,
                                                       Map<String, String> newProperties) {
        return !Objects.equals(oldProperties, newProperties);
    }

    private record CompactionResources(LakehouseFactory lakehouseFactory,
                                       EntryProcessFactory entryReaderFactory,
                                       SchemaService<?> schemaService) {
        private void cleanUp() {
            lakehouseFactory.cleanUp();
            entryReaderFactory.cleanUp();
        }

        private void close() {
            try {
                entryReaderFactory.close();
            } catch (Exception e) {
                log.warn("Failed to close entry reader factory", e);
            }
            try {
                schemaService.close();
            } catch (Exception e) {
                log.warn("Failed to close schema service", e);
            }
            try {
                lakehouseFactory.close();
            } catch (Exception e) {
                log.warn("Failed to close lakehouse factory", e);
            }
        }
    }
}
