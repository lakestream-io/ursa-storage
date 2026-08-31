/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.materialization;

import io.lakestream.ursa.compaction.CompactTaskManager;
import io.lakestream.ursa.materialization.serde.SchemaEvolutionManager;
import io.lakestream.ursa.materialization.serde.SchemaService;
import io.lakestream.ursa.storage.StorageApi;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.Executor;
import javax.annotation.Nullable;
import org.slf4j.Logger;

/**
 * Bag of services the materialization framework injects into sinks at
 * {@link TableMaterializerFactory#create(io.lakestream.api.materialization.TableMaterializationPolicy,
 * io.lakestream.api.materialization.TableCatalog,
 * io.lakestream.api.Stream, MaterializationRuntime) create-time}.
 *
 * <p>All fields are required; the canonical constructor enforces non-null on
 * each one. Use {@link MaterializationMetrics#noop()} and
 * {@link FailureMessageHandler#noop()} for stub setups.
 *
 * @param schemaService            resolves source-side schemas
 * @param schemaEvolutionManager   drives schema evolution against the sink
 * @param materializationExecutor  executor sinks may submit work to
 * @param logger                   logger threaded through the framework
 * @param metrics                  metric sink (use {@link MaterializationMetrics#noop()} for tests)
 * @param failureMessageHandler    DLQ handler (use {@link FailureMessageHandler#noop()} for tests)
 * @param compactTaskManager       persists the compaction task (status + write results) so the
 *                                 group-commit runner can finalize it; may be {@code null} when a
 *                                 deployment does not run the Lakehouse group-commit pipeline
 * @param storageApi               the storage engine used to read source entries straight from the
 *                                 WAL. May be {@code null} in stub setups.
 * @param taskProperties           per-task compaction properties carrying the legacy {@code DynamicConfigs}
 *                                 (sdtEnabled, sdtCatalogName, identifierFields, upsertMode,
 *                                 baseSchemaVersion, …). Sinks merge these onto the resolved policy /
 *                                 writer config so deployments that drove materialization through task
 *                                 properties remain compatible with the policy-based pipeline. Never
 *                                 {@code null} (defaults to an empty map).
 */
public record MaterializationRuntime(
        SchemaService<?> schemaService,
        SchemaEvolutionManager schemaEvolutionManager,
        Executor materializationExecutor,
        Logger logger,
        MaterializationMetrics metrics,
        FailureMessageHandler failureMessageHandler,
        @Nullable CompactTaskManager compactTaskManager,
        @Nullable StorageApi storageApi,
        Map<String, String> taskProperties) {

    /** Canonical constructor: validates required dependencies ({@code compactTaskManager} optional). */
    public MaterializationRuntime {
        Objects.requireNonNull(schemaService, "schemaService");
        Objects.requireNonNull(schemaEvolutionManager, "schemaEvolutionManager");
        Objects.requireNonNull(materializationExecutor, "materializationExecutor");
        Objects.requireNonNull(logger, "logger");
        Objects.requireNonNull(metrics, "metrics");
        Objects.requireNonNull(failureMessageHandler, "failureMessageHandler");
        taskProperties = taskProperties == null ? Map.of() : Map.copyOf(taskProperties);
    }

    /** Convenience constructor without task properties. */
    public MaterializationRuntime(SchemaService<?> schemaService,
                                  SchemaEvolutionManager schemaEvolutionManager,
                                  Executor materializationExecutor,
                                  Logger logger,
                                  MaterializationMetrics metrics,
                                  FailureMessageHandler failureMessageHandler,
                                  @Nullable CompactTaskManager compactTaskManager,
                                  @Nullable StorageApi storageApi) {
        this(schemaService, schemaEvolutionManager, materializationExecutor, logger, metrics,
                failureMessageHandler, compactTaskManager, storageApi, Map.of());
    }

    /** Convenience constructor for callers that do not supply storage. */
    public MaterializationRuntime(SchemaService<?> schemaService,
                                  SchemaEvolutionManager schemaEvolutionManager,
                                  Executor materializationExecutor,
                                  Logger logger,
                                  MaterializationMetrics metrics,
                                  FailureMessageHandler failureMessageHandler,
                                  @Nullable CompactTaskManager compactTaskManager) {
        this(schemaService, schemaEvolutionManager, materializationExecutor, logger, metrics,
                failureMessageHandler, compactTaskManager, null, Map.of());
    }

    /** Back-compat 6-arg constructor for callers that do not supply a {@link CompactTaskManager}. */
    public MaterializationRuntime(SchemaService<?> schemaService,
                                  SchemaEvolutionManager schemaEvolutionManager,
                                  Executor materializationExecutor,
                                  Logger logger,
                                  MaterializationMetrics metrics,
                                  FailureMessageHandler failureMessageHandler) {
        this(schemaService, schemaEvolutionManager, materializationExecutor, logger, metrics,
                failureMessageHandler, null, null, Map.of());
    }

    /** Returns a copy of this runtime with the supplied {@link StorageApi}. */
    public MaterializationRuntime withStorageApi(@Nullable StorageApi newStorageApi) {
        return new MaterializationRuntime(schemaService, schemaEvolutionManager, materializationExecutor,
                logger, metrics, failureMessageHandler, compactTaskManager, newStorageApi,
                taskProperties);
    }

    /**
     * Returns a copy of this runtime carrying the given per-task compaction properties (legacy
     * {@code DynamicConfigs}). Used by the orchestrator to thread the task's properties to the sink.
     */
    public MaterializationRuntime withTaskProperties(@Nullable Map<String, String> newTaskProperties) {
        return new MaterializationRuntime(schemaService, schemaEvolutionManager, materializationExecutor,
                logger, metrics, failureMessageHandler, compactTaskManager, storageApi,
                newTaskProperties);
    }
}
