/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.materialization;

import io.lakestream.api.Stream;
import io.lakestream.api.StreamIdentifier;
import io.lakestream.api.materialization.ResolvedMaterialization;
import java.util.Map;
import java.util.Optional;

/**
 * Orchestrator entry point invoked by {@code CompactionWorker} (T10).
 *
 * <p>A single deployment owns one {@code MaterializationService} instance.
 * It is initialised once at scheduler startup with the framework runtime and
 * operator config, then receives one
 * {@link #materialize(MaterializationTask) materialize} call per WAL task that
 * resolves to a materialization-enabled stream.
 */
public interface MaterializationService extends AutoCloseable {

    /**
     * Lifecycle hook called once at scheduler startup. Mirrors today's
     * {@code CompactionService.initialize} signature in spirit.
     *
     * @param runtime injected framework services (schema service, evolution
     *                manager, executors, metrics, failure handler)
     * @param config  per-deployment operator config (worker sizing, perf
     *                limits) passed via a typed bag rather than raw Properties
     */
    void initialize(MaterializationRuntime runtime, MaterializationServiceConfig config);

    /**
     * Materializes the records in {@code task} to the sink described by the
     * task's effective policy. Called by {@code CompactionWorker} for each
     * WAL task that resolves to a materialization-enabled stream.
     *
     * @param task the materialization task
     * @throws MaterializationException on unrecoverable error
     */
    void materialize(MaterializationTask task);

    /**
     * Registers the {@link Stream} handle for a stream so the service can build sink materializers that
     * need it. The orchestrator ({@code CompactionWorker}) calls this once per task before
     * {@link #materialize(MaterializationTask)}. The default is a no-op for services that do not need
     * the handle.
     *
     * @param id     the stream identifier
     * @param stream the opened stream handle
     */
    default void registerActiveStream(StreamIdentifier id, Stream stream) {
    }

    /**
     * Drops any cached state for {@code id} (e.g., releases sink connections,
     * invalidates compiled schema converters). Called on failure or stream
     * delete.
     *
     * @param id the stream whose cached state should be released
     */
    void invalidate(StreamIdentifier id);

    /**
     * Resolves a {@link ResolvedMaterialization} for a stream directly from the compaction task's
     * properties, for backward compatibility with deployments that drove materialization through task
     * properties (legacy {@code DynamicConfigs} + catalog config) rather than a stream/namespace/cluster
     * policy. {@code CompactionWorker} calls this as a fallback when
     * {@code Stream.effectiveMaterialization()} resolves nothing.
     *
     * <p>The default returns {@link Optional#empty()} (no task-property-based materialization).
     *
     * @param streamId       the stream being compacted
     * @param topic          the source topic
     * @param taskProperties the task's properties (catalog config + legacy DynamicConfigs)
     * @return the resolved materialization derived from the task properties, or empty
     */
    default Optional<ResolvedMaterialization> resolveFromTaskProperties(
            StreamIdentifier streamId, String topic, Map<String, String> taskProperties) {
        return Optional.empty();
    }

    /** Releases all framework resources. */
    @Override
    void close();
}
