/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.api;

import io.lakestream.api.materialization.ResolvedMaterialization;
import io.lakestream.api.materialization.TableMaterializationPolicy;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

/**
 * An opened stream handle — provides both metadata access and data-plane operations.
 *
 * <p>Obtained via {@link StreamCatalog#createStream} or {@link StreamCatalog#loadStream}.
 * The handle holds a layout, writer, reader, and per-log access. Closing the stream
 * releases all associated resources.
 *
 * <p>Thread safety: implementations must be safe for concurrent use.
 */
public interface Stream extends AutoCloseable {

    // --- Metadata (was the record fields) ---

    /**
     * Returns the stream's identity (namespace + name).
     *
     * @return the stream identifier
     */
    StreamIdentifier identifier();

    /**
     * Returns the stream configuration.
     *
     * @return the stream config
     */
    StreamConfig config();

    /**
     * Returns how the stream is divided into logs.
     *
     * @return the partitioning configuration
     */
    Partitioning partitioning();

    /**
     * Returns the schema configuration.
     *
     * @return the schema config
     */
    SchemaConfig schema();

    /**
     * Returns user-defined properties.
     *
     * @return the properties map
     */
    Map<String, String> properties();

    /**
     * Returns the current lifecycle state.
     *
     * @return the lifecycle state
     */
    LifecycleState state();

    /**
     * Returns the stream-level materialization policy override, if any.
     *
     * <p>This is the raw, sparse override set on the stream; it is merged
     * on top of the namespace-level baseline during resolution. Use
     * {@link #effectiveMaterialization()} to get the fully merged view.
     *
     * @return the stream's materialization override, or empty if unset
     */
    Optional<TableMaterializationPolicy> materialization();

    /**
     * Returns the fully resolved materialization view for this stream.
     *
     * <p>The result merges the namespace baseline with the stream override
     * and resolves the {@link io.lakestream.api.materialization.TableCatalog}
     * reference. Returns empty if the stream is not materialized
     * (no policy at either layer, or policy disabled).
     *
     * @return the resolved materialization, or empty if not materialized
     */
    Optional<ResolvedMaterialization> effectiveMaterialization();

    // --- Behavior (new) ---

    /**
     * Returns the layout describing how this stream is composed from logs.
     *
     * @return the stream layout
     */
    StreamLayout layout();

    /**
     * Returns the stream-level writer.
     *
     * @return the stream writer
     */
    StreamWriter writer();

    /**
     * Returns the stream-level reader.
     *
     * @return the stream reader
     */
    StreamReader reader();

    /**
     * Returns a per-log managed handle for the given log ID.
     *
     * @param logId the log to access
     * @return the log handle
     */
    Log getLog(LogId logId);

    // --- Trim (stream-level, layout-aware) ---

    /**
     * Marks entries up to and including the given position as deleted (soft trim).
     *
     * <p>In a range-based split/merge layout, a trim at a given position may
     * affect multiple logs. The stream routes based on layout.
     *
     * @param position the position up to which entries are marked deleted
     * @return a future resolving to the new first position after trimming
     */
    CompletableFuture<StreamPosition> softTrim(StreamPosition position);

    /**
     * Physically deletes entries up to the given position (hard trim).
     *
     * @param position the position up to which entries are physically removed
     * @return a future that completes when the deletion is done
     */
    CompletableFuture<Void> hardTrim(StreamPosition position);
}
