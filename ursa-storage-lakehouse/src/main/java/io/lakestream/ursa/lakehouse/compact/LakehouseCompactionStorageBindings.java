/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.lakehouse.compact;

import io.lakestream.api.StreamCatalog;
import io.lakestream.ursa.compaction.CompactTaskManager;
import io.lakestream.ursa.compaction.CompactionManager;
import io.lakestream.ursa.compaction.metrics.CompactionMetrics;
import io.lakestream.ursa.lakehouse.cleaner.AsyncCompactedDataCleaner;
import io.lakestream.ursa.lakehouse.cleaner.CompactedDataCleanupHandler;
import io.lakestream.ursa.lakehouse.schema.KafkaSchemaRegistry;
import io.lakestream.ursa.lakehouse.schema.SchemaRegistry;
import io.lakestream.ursa.materialization.serde.SchemaService;
import io.lakestream.ursa.materialization.serde.kafka.KafkaSchemaService;
import io.lakestream.ursa.storage.FileStorage;
import io.lakestream.ursa.storage.StorageApi;
import io.lakestream.ursa.storage.impl.StorageConfig;
import io.lakestream.ursa.storage.impl.compaction.CommitTaskProvider;
import io.lakestream.ursa.storage.impl.compaction.CompactionStorageBindings;
import io.lakestream.ursa.storage.impl.compaction.StartStopRunner;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.BooleanSupplier;
import javax.annotation.Nullable;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

/**
 * Lakehouse-backed implementation of {@link CompactionStorageBindings}.
 *
 * <p>Holds the concrete wiring for the lakehouse-specific compaction runners
 * (publish-compact-task, compacted-task, cleaner, cleanup-handler). Constructed
 * by {@code CompactionScheduler} at startup (T10); each {@code create*} method
 * returns a fresh runner instance that the orchestrator drives via
 * {@link StartStopRunner#start()} / {@link StartStopRunner#stop()}.
 *
 * <p>This class deliberately keeps the dependency surface narrow: it accepts a
 * {@link Dependencies} bag rather than ~15 constructor arguments. Each field on
 * {@code Dependencies} is the same value the existing {@code CompactionScheduler}
 * holds today.
 */
@Slf4j
public final class LakehouseCompactionStorageBindings implements CompactionStorageBindings {

    private final Dependencies deps;
    private final CompactionManager compactionManager;
    @Getter
    private final SchemaRegistry schemaRegistry;
    private volatile KafkaSchemaService schemaService;

    public LakehouseCompactionStorageBindings(Dependencies deps) {
        this.deps = Objects.requireNonNull(deps, "deps");
        this.compactionManager = new CompactionManager(
                deps.compactTaskManager, deps.compactionMetrics);
        this.schemaRegistry = deps.schemaRegistry != null
                ? deps.schemaRegistry
                : new KafkaSchemaRegistry(deps.config.getProperties());
    }

    @Override
    public StartStopRunner createPublishCompactTaskRunner() {
        StreamCatalog streamCatalog = Objects.requireNonNull(
                deps.streamCatalog,
                "StreamCatalog is required when the internal compaction task publisher is enabled");
        return new PublishCompactTaskRunner(
                streamCatalog,
                compactionManager,
                deps.scanTopicExecutor,
                deps.publishTaskExecutor,
                deps.config,
                deps.compactionMetrics);
    }

    @Override
    public StartStopRunner createCompactedTaskRunner(BooleanSupplier isLeader) {
        return new CompactedTaskRunner(
                deps.storageApi,
                deps.commitTaskProvider,
                deps.compactTaskManager,
                deps.compactedTaskExecutor,
                deps.commitParquetFileExecutor,
                deps.config,
                deps.compactionMetrics,
                isLeader);
    }

    @Override
    public StartStopRunner createAsyncCompactedDataCleaner() {
        // AsyncCompactedDataCleaner self-schedules in its constructor; we still return it as a
        // StartStopRunner so the orchestrator can call stop() at shutdown.
        return new AsyncCompactedDataCleaner(deps.config, deps.storageApi, createCompactedDataCleanupHandlerImpl());
    }

    private CompactedDataCleanupHandler createCompactedDataCleanupHandlerImpl() {
        return new CompactedDataCleanupHandler(deps.config, deps.storageApi, deps.fileStorage);
    }

    @Override
    public Object getSchemaRegistry() {
        return schemaRegistry;
    }

    @Override
    public Object schemaService() {
        KafkaSchemaService local = schemaService;
        if (local == null && schemaRegistry.client() != null) {
            synchronized (this) {
                local = schemaService;
                if (local == null) {
                    local = new KafkaSchemaService(schemaRegistry.client(), false);
                    schemaService = local;
                }
            }
        }
        return local;
    }

    /**
     * Test-visible accessor that narrows {@link #schemaService()} to its concrete materialization
     * type. The interface contract returns {@link Object} to avoid a core → materialization
     * dependency; production wiring in {@code CompactionScheduler} casts to this type.
     */
    public SchemaService<?> typedSchemaService() {
        Object svc = schemaService();
        return svc == null ? null : (SchemaService<?>) svc;
    }

    @Override
    public void close() {
        if (schemaService != null) {
            try {
                schemaService.close();
            } catch (Exception e) {
                log.warn("Failed to close schema service during bindings shutdown", e);
            }
        }
    }

    /**
     * Bag of dependencies the lakehouse runners need. Mirrors the fields the current
     * {@code CompactionScheduler} already constructs; T10 will plumb this object directly from
     * the scheduler.
     */
    public static final class Dependencies {
        private final StorageConfig config;
        @Nullable
        private final StreamCatalog streamCatalog;
        private final StorageApi storageApi;
        private final FileStorage fileStorage;
        private final CompactTaskManager compactTaskManager;
        private final CompactionMetrics compactionMetrics;
        private final CommitTaskProvider commitTaskProvider;
        private final SchemaRegistry schemaRegistry;
        private final ExecutorService scanTopicExecutor;
        private final ScheduledExecutorService publishTaskExecutor;
        private final ExecutorService compactedTaskExecutor;
        private final ExecutorService commitParquetFileExecutor;

        @SuppressWarnings("ParameterNumber")
        public Dependencies(StorageConfig config,
                            @Nullable StreamCatalog streamCatalog,
                            StorageApi storageApi,
                            FileStorage fileStorage,
                            CompactTaskManager compactTaskManager,
                            CompactionMetrics compactionMetrics,
                            CommitTaskProvider commitTaskProvider,
                            SchemaRegistry schemaRegistry,
                            ExecutorService scanTopicExecutor,
                            ScheduledExecutorService publishTaskExecutor,
                            ExecutorService compactedTaskExecutor,
                            ExecutorService commitParquetFileExecutor) {
            this.config = Objects.requireNonNull(config, "config");
            this.streamCatalog = streamCatalog;
            this.storageApi = Objects.requireNonNull(storageApi, "storageApi");
            this.fileStorage = Objects.requireNonNull(fileStorage, "fileStorage");
            this.compactTaskManager = Objects.requireNonNull(compactTaskManager, "compactTaskManager");
            this.compactionMetrics = Objects.requireNonNull(compactionMetrics, "compactionMetrics");
            this.commitTaskProvider = Objects.requireNonNull(commitTaskProvider, "commitTaskProvider");
            this.schemaRegistry = schemaRegistry;
            this.scanTopicExecutor = Objects.requireNonNull(scanTopicExecutor, "scanTopicExecutor");
            this.publishTaskExecutor = Objects.requireNonNull(publishTaskExecutor, "publishTaskExecutor");
            this.compactedTaskExecutor = Objects.requireNonNull(compactedTaskExecutor, "compactedTaskExecutor");
            this.commitParquetFileExecutor =
                    Objects.requireNonNull(commitParquetFileExecutor, "commitParquetFileExecutor");
        }
    }
}
