/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.storage.impl.compaction;

import java.util.function.BooleanSupplier;
import java.util.function.Consumer;

/**
 * Sink-neutral factory for the long-running compaction runners and storage
 * helpers the orchestrator needs.
 *
 * <p>Introduced in T9 so that {@code CompactionScheduler} (T10) no longer has
 * to import lakehouse-specific runner classes directly. Each integration module
 * (lakehouse today, clickhouse later) provides a concrete bindings
 * implementation that wires up its own concrete runners.
 *
 * <p>The runners returned here are exposed as {@link StartStopRunner} so the
 * orchestrator only sees the lifecycle contract — the concrete classes
 * ({@code PublishCompactTaskRunner}, {@code CompactedTaskRunner},
 * {@code AsyncCompactedDataCleaner}, {@code CompactedDataCleanupHandler}) remain
 * inside the integration module along with their heavy lakehouse-specific
 * transitive dependencies. This keeps the abstraction in {@code ursa-storage-core}
 * while avoiding a hard move of every runner class into core.
 */
public interface CompactionStorageBindings extends AutoCloseable {

    /**
     * Builds the runner that publishes prepared compaction tasks (WAL-side scan
     * → publish prepared task into the compaction task store).
     */
    StartStopRunner createPublishCompactTaskRunner();

    /**
     * Builds the runner that drains committed compaction tasks (writes parquet
     * files / commits to the configured table format).
     *
     * @param isLeader leadership gate: the runner only commits while this returns {@code true}, so a
     *                 demoted leader stops promptly instead of double-committing with the new leader.
     *                 Pass {@code () -> true} for manual/admin commits that are not leadership-gated.
     */
    StartStopRunner createCompactedTaskRunner(BooleanSupplier isLeader);

    /**
     * Builds the commit runner with a process-level fatal-error supervisor.
     *
     * <p>The default preserves source and binary compatibility for integrations whose commit
     * implementation has no non-interruptible external side effects. Implementations that can
     * leave an in-flight commit running after their lifecycle deadline should override this method
     * and invoke {@code fatalErrorHandler} rather than allowing an unsafe leader handoff.
     */
    default StartStopRunner createCompactedTaskRunner(
            BooleanSupplier isLeader, Consumer<Throwable> fatalErrorHandler) {
        return createCompactedTaskRunner(isLeader);
    }

    /**
     * Builds the periodic cleaner that finds compacted data eligible for
     * deletion and submits cleanup tasks to its cleanup handler.
     */
    StartStopRunner createAsyncCompactedDataCleaner();

    /**
     * Returns the sink-neutral schema service the orchestrator threads into the
     * {@code MaterializationRuntime} it hands to {@code MaterializationService}.
     *
     * <p>The return type is {@link Object} because {@code ursa-storage-core} cannot depend
     * on {@code ursa-storage-materialization} (the dependency runs the other way).
     * Callers in the orchestrator cast to
     * {@code io.lakestream.ursa.materialization.serde.SchemaService}. Bindings that have
     * no schema service may return {@code null}; the orchestrator falls back to a noop.
     */
    Object schemaService();

    /**
     * Returns the schema-registry abstraction used by the legacy compaction service.
     *
     * <p>The type stays {@link Object} so the lakehouse {@code SchemaRegistry} type
     * does not have to live in core; the lakehouse bindings impl exposes a
     * concrete getter for its callers.
     */
    Object getSchemaRegistry();

    /** Releases all resources held by the bindings. */
    @Override
    void close();
}
