/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.api.materialization;

import io.lakestream.api.StreamIdentifier;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;

/**
 * Stream-to-table materialization policy applied at two layers:
 * <ol>
 *   <li>the <em>namespace</em> layer — the active baseline; and</li>
 *   <li>the <em>stream</em> layer — sparse overrides applied on top.</li>
 * </ol>
 *
 * <p>Every functional field is an {@link Optional}; an empty value means
 * "inherit / fall through to the next layer". Resolution into a final, fully
 * materialised policy lives in the resolution helper, not in this record.
 *
 * <p>{@code primaryKey} and {@code connectionOverrides} are defensively copied
 * via {@link List#copyOf(java.util.Collection)} and {@link Map#copyOf(Map)}
 * respectively in the canonical constructor.
 *
 * <p>Use {@link #empty()} for an all-empty policy (the natural identity at the
 * top of a resolution chain).
 *
 * @param catalogRef           name of the referenced {@link TableCatalog}
 * @param tableNaming          naming template (only meaningful at the namespace layer)
 * @param tableIdentifier      explicit table identifier (overrides naming when set)
 * @param enabled              whether materialization is enabled
 * @param framework            framework-level (engine-agnostic) configuration
 * @param evolution            schema-evolution capabilities
 * @param primaryKey           primary-key column names
 * @param baseSchemaVersion    base schema version for compatibility checks
 * @param table                table-level (engine-specific) configuration
 * @param connectionOverrides  per-stream overrides for catalog connection settings
 */
public record TableMaterializationPolicy(
        Optional<String> catalogRef,
        Optional<TableNaming> tableNaming,
        Optional<TableIdentifier> tableIdentifier,
        Optional<Boolean> enabled,
        Optional<FrameworkConf> framework,
        Optional<EvolutionPolicy> evolution,
        Optional<List<String>> primaryKey,
        Optional<Long> baseSchemaVersion,
        Optional<TableConf> table,
        Map<String, String> connectionOverrides) {

    /**
     * Canonical constructor: validates all Optional fields are non-null and
     * defensively copies the list and map fields.
     */
    public TableMaterializationPolicy {
        Objects.requireNonNull(catalogRef, "catalogRef cannot be null; use Optional.empty()");
        Objects.requireNonNull(tableNaming, "tableNaming cannot be null; use Optional.empty()");
        Objects.requireNonNull(tableIdentifier,
                "tableIdentifier cannot be null; use Optional.empty()");
        Objects.requireNonNull(enabled, "enabled cannot be null; use Optional.empty()");
        Objects.requireNonNull(framework, "framework cannot be null; use Optional.empty()");
        Objects.requireNonNull(evolution, "evolution cannot be null; use Optional.empty()");
        Objects.requireNonNull(primaryKey, "primaryKey cannot be null; use Optional.empty()");
        Objects.requireNonNull(baseSchemaVersion,
                "baseSchemaVersion cannot be null; use Optional.empty()");
        Objects.requireNonNull(table, "table cannot be null; use Optional.empty()");
        Objects.requireNonNull(connectionOverrides, "connectionOverrides");
        primaryKey = primaryKey.map(List::copyOf);
        connectionOverrides = Map.copyOf(connectionOverrides);
    }

    /**
     * Returns an all-{@link Optional#empty() empty} policy with an empty
     * connection-overrides map. This is the natural identity element for
     * policy resolution: applying it as an override changes nothing.
     */
    public static TableMaterializationPolicy empty() {
        return new TableMaterializationPolicy(
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Map.of());
    }

    /**
     * Resolves the effective materialization for {@code streamId} by deep-merging
     * the namespace and stream policies, looking up the referenced catalog via
     * {@code catalogLookup}, and deriving the table identifier from the
     * stream's explicit override (preferred) or from the namespace's
     * {@link TableNaming} template.
     *
     * <p>Merge rules (stream wins, namespace as baseline):
     * <ul>
     *   <li>{@code enabled}: if {@code streamPolicy.enabled() == Optional.of(false)},
     *       the stream short-circuits with {@link Optional#empty()}.
     *       Namespace {@code enabled} is intentionally ignored.</li>
     *   <li>{@code catalogRef}: stream-over-namespace; required for resolution.</li>
     *   <li>{@code tableIdentifier}: stream's explicit value wins; otherwise the
     *       namespace's {@link TableNaming} is applied to {@code streamId}.</li>
     *   <li>Sub-records ({@code framework}, {@code evolution}, {@code table}):
     *       merged field-by-field with stream-over-namespace semantics.</li>
     *   <li>{@code primaryKey} / {@code partitionBy} / {@code sortBy}: stream's
     *       list <em>replaces</em> namespace's (no concatenation).</li>
     *   <li>{@code connectionOverrides}: stream-only; namespace's is ignored.</li>
     *   <li>{@code tableNaming}: namespace-only; preserved on the effective
     *       policy for inspection.</li>
     * </ul>
     *
     * <p>The helper is pure: all I/O (catalog lookup) is delegated through the
     * {@code catalogLookup} callback.
     *
     * <p>Equivalent to {@link #resolve(Optional, Optional, StreamIdentifier, Function, Map)}
     * with no stream properties available; a namespace {@link TableNaming} template
     * referencing {@code ${stream.property.<key>}} is therefore rejected.
     *
     * @param namespacePolicy the namespace baseline (may be empty)
     * @param streamPolicy    the stream override (may be empty)
     * @param streamId        the stream being resolved (used for template interpolation)
     * @param catalogLookup   callback returning the named {@link TableCatalog}, or empty
     * @return the resolved materialization, or {@link Optional#empty()} if the
     *     stream is not materialized (disabled, no catalog ref, missing catalog,
     *     or no derivable table identifier)
     */
    public static Optional<ResolvedMaterialization> resolve(
            Optional<TableMaterializationPolicy> namespacePolicy,
            Optional<TableMaterializationPolicy> streamPolicy,
            StreamIdentifier streamId,
            Function<String, Optional<TableCatalog>> catalogLookup) {
        return resolve(namespacePolicy, streamPolicy, streamId, catalogLookup, Map.of());
    }

    /**
     * Resolves the effective materialization for {@code streamId} by deep-merging
     * the namespace and stream policies, looking up the referenced catalog via
     * {@code catalogLookup}, and deriving the table identifier from the
     * stream's explicit override (preferred) or from the namespace's
     * {@link TableNaming} template applied together with {@code properties}.
     *
     * <p>Merge rules (stream wins, namespace as baseline):
     * <ul>
     *   <li>{@code enabled}: if {@code streamPolicy.enabled() == Optional.of(false)},
     *       the stream short-circuits with {@link Optional#empty()}.
     *       Namespace {@code enabled} is intentionally ignored.</li>
     *   <li>{@code catalogRef}: stream-over-namespace; required for resolution.</li>
     *   <li>{@code tableIdentifier}: stream's explicit value wins; otherwise the
     *       namespace's {@link TableNaming} is applied to {@code streamId} and
     *       {@code properties}.</li>
     *   <li>Sub-records ({@code framework}, {@code evolution}, {@code table}):
     *       merged field-by-field with stream-over-namespace semantics.</li>
     *   <li>{@code primaryKey} / {@code partitionBy} / {@code sortBy}: stream's
     *       list <em>replaces</em> namespace's (no concatenation).</li>
     *   <li>{@code connectionOverrides}: stream-only; namespace's is ignored.</li>
     *   <li>{@code tableNaming}: namespace-only; preserved on the effective
     *       policy for inspection.</li>
     * </ul>
     *
     * <p>The helper is pure: all I/O (catalog lookup) is delegated through the
     * {@code catalogLookup} callback.
     *
     * @param namespacePolicy the namespace baseline (may be empty)
     * @param streamPolicy    the stream override (may be empty)
     * @param streamId        the stream being resolved (used for template interpolation)
     * @param catalogLookup   callback returning the named {@link TableCatalog}, or empty
     * @param properties      the stream's properties, consulted by the namespace's
     *                        {@link TableNaming} template for {@code ${stream.property.<key>}}
     *                        variables
     * @return the resolved materialization, or {@link Optional#empty()} if the
     *     stream is not materialized (disabled, no catalog ref, missing catalog,
     *     or no derivable table identifier)
     */
    public static Optional<ResolvedMaterialization> resolve(
            Optional<TableMaterializationPolicy> namespacePolicy,
            Optional<TableMaterializationPolicy> streamPolicy,
            StreamIdentifier streamId,
            Function<String, Optional<TableCatalog>> catalogLookup,
            Map<String, String> properties) {
        Objects.requireNonNull(namespacePolicy, "namespacePolicy");
        Objects.requireNonNull(streamPolicy, "streamPolicy");
        Objects.requireNonNull(streamId, "streamId");
        Objects.requireNonNull(catalogLookup, "catalogLookup");
        Objects.requireNonNull(properties, "properties");

        TableMaterializationPolicy namespace = namespacePolicy.orElse(empty());
        TableMaterializationPolicy stream = streamPolicy.orElse(empty());

        // Stream-side explicit opt-out short-circuits.
        if (stream.enabled().equals(Optional.of(Boolean.FALSE))) {
            return Optional.empty();
        }

        // Effective catalog reference: stream wins, else namespace, else nothing.
        Optional<String> effectiveCatalogRef = pick(stream.catalogRef(), namespace.catalogRef());
        if (effectiveCatalogRef.isEmpty()) {
            return Optional.empty();
        }

        Optional<TableCatalog> catalog = catalogLookup.apply(effectiveCatalogRef.get());
        if (catalog.isEmpty()) {
            return Optional.empty();
        }

        // Effective table identifier: stream's explicit value wins, otherwise
        // derive from namespace.tableNaming(); if neither path produces one,
        // the stream cannot be materialized.
        Optional<TableIdentifier> effectiveTableIdentifier = stream.tableIdentifier();
        if (effectiveTableIdentifier.isEmpty()) {
            effectiveTableIdentifier = namespace.tableNaming()
                    .map(naming -> naming.toTableIdentifier(streamId, properties));
        }
        if (effectiveTableIdentifier.isEmpty()) {
            return Optional.empty();
        }

        // Build the merged effective policy. tableNaming is namespace-only;
        // connectionOverrides is stream-only; everything else merges per-field.
        TableMaterializationPolicy effective = new TableMaterializationPolicy(
                effectiveCatalogRef,
                namespace.tableNaming(),
                effectiveTableIdentifier,
                pick(stream.enabled(), namespace.enabled()),
                mergeFramework(stream.framework(), namespace.framework()),
                mergeEvolution(stream.evolution(), namespace.evolution()),
                pick(stream.primaryKey(), namespace.primaryKey()),
                pick(stream.baseSchemaVersion(), namespace.baseSchemaVersion()),
                mergeTable(stream.table(), namespace.table()),
                stream.connectionOverrides());

        return Optional.of(new ResolvedMaterialization(
                catalog.get(), effectiveTableIdentifier.get(), effective));
    }

    private static <T> Optional<T> pick(Optional<T> override, Optional<T> base) {
        return override.isPresent() ? override : base;
    }

    private static Optional<FrameworkConf> mergeFramework(
            Optional<FrameworkConf> stream, Optional<FrameworkConf> namespace) {
        if (stream.isEmpty()) {
            return namespace;
        }
        if (namespace.isEmpty()) {
            return stream;
        }
        FrameworkConf s = stream.get();
        FrameworkConf n = namespace.get();
        return Optional.of(new FrameworkConf(
                pick(s.writeMode(), n.writeMode()),
                pick(s.startPosition(), n.startPosition()),
                pick(s.paused(), n.paused()),
                pick(s.errorHandling(), n.errorHandling()),
                mergeCommit(s.commit(), n.commit())));
    }

    private static Optional<CommitConfig> mergeCommit(
            Optional<CommitConfig> stream, Optional<CommitConfig> namespace) {
        if (stream.isEmpty()) {
            return namespace;
        }
        if (namespace.isEmpty()) {
            return stream;
        }
        CommitConfig s = stream.get();
        CommitConfig n = namespace.get();
        return Optional.of(new CommitConfig(
                pick(s.maxRetries(), n.maxRetries()),
                pick(s.retryDelayMs(), n.retryDelayMs()),
                pick(s.batchSize(), n.batchSize())));
    }

    private static Optional<EvolutionPolicy> mergeEvolution(
            Optional<EvolutionPolicy> stream, Optional<EvolutionPolicy> namespace) {
        if (stream.isEmpty()) {
            return namespace;
        }
        if (namespace.isEmpty()) {
            return stream;
        }
        EvolutionPolicy s = stream.get();
        EvolutionPolicy n = namespace.get();
        return Optional.of(new EvolutionPolicy(
                pick(s.addColumn(), n.addColumn()),
                pick(s.addNullableColumn(), n.addNullableColumn()),
                pick(s.dropColumn(), n.dropColumn()),
                pick(s.widenType(), n.widenType()),
                pick(s.narrowType(), n.narrowType()),
                pick(s.renameColumn(), n.renameColumn()),
                pick(s.reorderColumns(), n.reorderColumns()),
                pick(s.nullabilityRelax(), n.nullabilityRelax()),
                pick(s.nullabilityTighten(), n.nullabilityTighten())));
    }

    private static Optional<TableConf> mergeTable(
            Optional<TableConf> stream, Optional<TableConf> namespace) {
        if (stream.isEmpty()) {
            return namespace;
        }
        if (namespace.isEmpty()) {
            return stream;
        }
        TableConf s = stream.get();
        TableConf n = namespace.get();
        return Optional.of(new TableConf(
                pick(s.mode(), n.mode()),
                pick(s.partitionBy(), n.partitionBy()),
                pick(s.sortBy(), n.sortBy()),
                mergeRetention(s.retention(), n.retention()),
                pick(s.targetFileSizeBytes(), n.targetFileSizeBytes()),
                pick(s.compression(), n.compression())));
    }

    private static Optional<RetentionConfig> mergeRetention(
            Optional<RetentionConfig> stream, Optional<RetentionConfig> namespace) {
        if (stream.isEmpty()) {
            return namespace;
        }
        if (namespace.isEmpty()) {
            return stream;
        }
        RetentionConfig s = stream.get();
        RetentionConfig n = namespace.get();
        return Optional.of(new RetentionConfig(
                pick(s.snapshotRetentionMs(), n.snapshotRetentionMs()),
                pick(s.maxSnapshots(), n.maxSnapshots()),
                pick(s.rowRetentionMs(), n.rowRetentionMs())));
    }
}
