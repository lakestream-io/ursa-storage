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
import io.lakestream.ursa.materialization.serde.EntryFormat;
import io.lakestream.ursa.materialization.serde.SchemaService;
import io.lakestream.ursa.materialization.serde.kafka.KafkaSchemaService;
import io.lakestream.ursa.storage.BaseStreamIDGenerator;
import io.lakestream.ursa.storage.FileStorage;
import io.lakestream.ursa.storage.StorageApi;
import io.lakestream.ursa.storage.impl.StorageConfig;
import io.lakestream.ursa.storage.impl.compaction.CompactionService;
import io.netty.buffer.ByteBufAllocator;
import io.oxia.client.api.AsyncOxiaClient;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

/** Lakehouse compaction backed by either Ursa entries or a Kafka consumer. */
@Slf4j
public class LakehouseCompactionServiceImpl implements CompactionService {

    private StorageApi storageApi;
    private CompactTaskManager compactTaskManager;
    private SchemaRegistry schemaRegistry;
    private StorageConfig config;
    private Function<EntryFormat, StorageApi> storageApiProvider;
    private CompactionMetrics compactionMetrics;

    @VisibleForTesting
    @Getter
    private final Map<String, CompactionTaskProcessor> compactWorkers = new ConcurrentHashMap<>();
    private final Map<String, EntryFormat> compactWorkerFormats = new ConcurrentHashMap<>();
    private final Map<EntryFormat, CompactionResources> resources = new ConcurrentHashMap<>();

    @Override
    public void compactStream(String topic, long streamId) {
        // The range-bearing CompactStreamTask overload is the supported entry point.
    }

    @Override
    public void compactStream(CompactStreamTask task) throws Exception {
        getCompactWorker(task).doCompact(task);
    }

    protected CompactionTaskProcessor getCompactWorker(CompactStreamTask task) {
        EntryFormat format = entryFormatOf(task);
        return compactWorkers.compute(task.getTopic(), (topic, existingWorker) -> {
            if (existingWorker instanceof LakehouseCompactionWorker
                    && format == compactWorkerFormats.get(topic)) {
                return existingWorker;
            }
            closeWorker(existingWorker);
            CompactionResources sourceResources = resources.computeIfAbsent(format, this::initializeResources);
            compactWorkerFormats.put(topic, format);
            return new LakehouseCompactionWorker(
                    sourceResources.lakehouseFactory(),
                    sourceResources.entryReaderFactory(),
                    compactTaskManager,
                    compactionMetrics,
                    config,
                    format);
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

    private EntryFormat entryFormatOf(CompactStreamTask task) {
        String configured = null;
        if (task.getProperties() != null) {
            configured = task.getProperties().get("entryFormat");
        }
        if (configured == null || configured.isBlank()) {
            configured = config.getProperties().getProperty("dataSourceForCompaction", EntryFormat.URSA.name());
        }
        try {
            return EntryFormat.valueOf(configured.trim().toUpperCase(Locale.ROOT));
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException(
                    "Unsupported entry format '" + configured + "'; expected URSA or KAFKA", e);
        }
    }

    @Override
    public void initialize(ByteBufAllocator allocator, FileStorage fileStorage, BaseStreamIDGenerator idGenerator,
                           StorageApi storageApi, CompactTaskManager compactTaskManager,
                           StorageConfig config, AsyncOxiaClient oxiaClient,
                           CompactionMetrics compactionMetrics, Object ctx) {
        this.storageApi = storageApi;
        this.compactTaskManager = Objects.requireNonNull(compactTaskManager, "compactTaskManager");
        this.config = Objects.requireNonNull(config, "config");
        this.storageApiProvider = ignored -> storageApi;
        this.compactionMetrics = Objects.requireNonNull(compactionMetrics, "compactionMetrics");
        this.schemaRegistry = (SchemaRegistry) Objects.requireNonNull(ctx, "schemaRegistry");
    }

    public void setStorageApiProvider(Function<EntryFormat, StorageApi> storageApiProvider) {
        this.storageApiProvider = storageApiProvider != null ? storageApiProvider : ignored -> storageApi;
        closeResources();
    }

    @Override
    public void maintenance() {
        try {
            resources.values().forEach(CompactionResources::cleanUp);
        } catch (Throwable t) {
            log.warn("Failed to maintain lakehouse compaction resources", t);
        }
    }

    private CompactionResources initializeResources(EntryFormat format) {
        if (!(schemaRegistry instanceof KafkaSchemaRegistry registry)) {
            throw new IllegalStateException("Lakehouse compaction requires a Kafka-compatible schema registry");
        }
        LakehouseConfiguration lakehouseConfig = new LakehouseConfiguration(config.getProperties());
        SchemaService<?> schemaService;
        EntryProcessFactory entryReaderFactory;
        if (format == EntryFormat.KAFKA) {
            schemaService = new KafkaSchemaService(registry.getSchemaRegistryClient(), false);
            entryReaderFactory = new KafkaEntryProcessFactory(lakehouseConfig);
        } else {
            schemaService = new KafkaSchemaService(registry.getSchemaRegistryClient(), false);
            StorageApi sourceStorage = storageApiProvider.apply(format);
            if (sourceStorage == null) {
                throw new IllegalStateException("Ursa entry format requires a StorageApi");
            }
            entryReaderFactory = new UrsaEntryProcessFactory(sourceStorage, compactionMetrics);
        }
        LakehouseFactory lakehouseFactory = new LakehouseFactory(
                lakehouseConfig, schemaService, compactionMetrics.getProvider());
        return new CompactionResources(lakehouseFactory, entryReaderFactory, schemaService);
    }

    public void invalidateCompactWorker(String topic) {
        closeWorker(compactWorkers.remove(topic));
        compactWorkerFormats.remove(topic);
    }

    @Override
    public void close() {
        compactWorkers.values().forEach(LakehouseCompactionServiceImpl::closeWorker);
        compactWorkers.clear();
        compactWorkerFormats.clear();
        closeResources();
    }

    private void closeResources() {
        resources.values().forEach(CompactionResources::close);
        resources.clear();
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
