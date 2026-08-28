/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.lakestream.impl;

import io.lakestream.api.LifecycleState;
import io.lakestream.api.Log;
import io.lakestream.api.LogId;
import io.lakestream.api.LogStateManager;
import io.lakestream.api.LogStorage;
import io.lakestream.api.Partitioning;
import io.lakestream.api.SchemaConfig;
import io.lakestream.api.Stream;
import io.lakestream.api.StreamConfig;
import io.lakestream.api.StreamIdentifier;
import io.lakestream.api.StreamLayout;
import io.lakestream.api.StreamPosition;
import io.lakestream.api.StreamReader;
import io.lakestream.api.StreamWriter;
import io.lakestream.api.materialization.ResolvedMaterialization;
import io.lakestream.api.materialization.TableCatalog;
import io.lakestream.api.materialization.TableMaterializationPolicy;
import io.lakestream.ursa.storage.impl.EntryIndexCache;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * Default implementation of {@link Stream} — an opened stream handle.
 *
 * <p>Holds the stream's metadata, layout, writer, reader, and provides
 * per-log access via {@link #getLog(LogId)}. Log instances are created
 * on demand and cached for reuse.
 */
public class StreamImpl implements Stream {

    private final StreamIdentifier identifier;
    private final StreamConfig config;
    private final Partitioning partitioning;
    private final SchemaConfig schema;
    private final Map<String, String> properties;
    private final LifecycleState state;
    private final StreamLayout layout;
    private final StreamWriter writer;
    private final StreamReader reader;
    private final LogStorage logStorage;
    private final UnifiedStreamReader unifiedReader;
    private final EntryIndexCache entryIndexCache;
    private final LogStateManager streamStateManager;
    private final Optional<TableMaterializationPolicy> materialization;
    private final Function<String, Optional<TableMaterializationPolicy>> namespacePolicyLookup;
    private final Supplier<Optional<TableMaterializationPolicy>> clusterDefaultPolicyLookup;
    private final Function<String, Optional<TableCatalog>> tableCatalogLookup;
    private final Map<LogId, LogImpl> logCache = new ConcurrentHashMap<>();
    private boolean closed;
    private boolean writerClosed;
    private boolean readerClosed;

    /**
     * Legacy constructor used by callers that do not yet thread materialization
     * state. Stream-level materialization defaults to {@link Optional#empty()};
     * {@link #effectiveMaterialization()} returns {@link Optional#empty()} because
     * the namespace + catalog lookups also default to empty.
     */
    public StreamImpl(StreamIdentifier identifier,
                      StreamConfig config,
                      Partitioning partitioning,
                      SchemaConfig schema,
                      Map<String, String> properties,
                      LifecycleState state,
                      StreamLayout layout,
                      LogStorage logStorage,
                      UnifiedStreamReader unifiedReader,
                      EntryIndexCache entryIndexCache,
                      LogStateManager streamStateManager) {
        this(identifier, config, partitioning, schema, properties, state, layout,
            logStorage, unifiedReader, entryIndexCache, streamStateManager,
            Optional.empty(), ns -> Optional.empty(), Optional::empty, name -> Optional.empty());
    }

    public StreamImpl(StreamIdentifier identifier,
                      StreamConfig config,
                      Partitioning partitioning,
                      SchemaConfig schema,
                      Map<String, String> properties,
                      LifecycleState state,
                      StreamLayout layout,
                      LogStorage logStorage,
                      UnifiedStreamReader unifiedReader,
                      EntryIndexCache entryIndexCache,
                      LogStateManager streamStateManager,
                      Optional<TableMaterializationPolicy> materialization,
                      Function<String, Optional<TableMaterializationPolicy>> namespacePolicyLookup,
                      Supplier<Optional<TableMaterializationPolicy>> clusterDefaultPolicyLookup,
                      Function<String, Optional<TableCatalog>> tableCatalogLookup) {
        this.identifier = identifier;
        this.config = config;
        this.partitioning = partitioning;
        this.schema = schema;
        this.properties = properties;
        this.state = state;
        this.layout = layout;
        this.logStorage = logStorage;
        this.unifiedReader = unifiedReader;
        this.entryIndexCache = entryIndexCache;
        this.streamStateManager = streamStateManager;
        this.materialization = materialization;
        this.namespacePolicyLookup = namespacePolicyLookup;
        this.clusterDefaultPolicyLookup = clusterDefaultPolicyLookup;
        this.tableCatalogLookup = tableCatalogLookup;
        this.writer = new StreamWriterImpl(layout, logStorage);
        this.reader = unifiedReader != null
            ? new StreamReaderImpl(layout, unifiedReader)
            : new StreamReaderImpl(layout, logStorage);
    }

    // --- Metadata ---

    @Override
    public StreamIdentifier identifier() {
        return identifier;
    }

    @Override
    public StreamConfig config() {
        return config;
    }

    @Override
    public Partitioning partitioning() {
        return partitioning;
    }

    @Override
    public SchemaConfig schema() {
        return schema;
    }

    @Override
    public Map<String, String> properties() {
        return properties;
    }

    @Override
    public LifecycleState state() {
        return state;
    }

    @Override
    public Optional<TableMaterializationPolicy> materialization() {
        return materialization;
    }

    @Override
    public Optional<ResolvedMaterialization> effectiveMaterialization() {
        // Baseline resolution order: stream policy (highest) → namespace policy → cluster-wide default
        // (lowest). When the stream's own namespace has no policy, fall back to the cluster default so
        // a cluster-wide materializationEnabled materializes streams in every namespace.
        Optional<TableMaterializationPolicy> baseline =
            namespacePolicyLookup.apply(identifier.namespace());
        if (baseline.isEmpty()) {
            baseline = clusterDefaultPolicyLookup.get();
        }
        return TableMaterializationPolicy.resolve(baseline, materialization, identifier,
            tableCatalogLookup);
    }

    // --- Behavior ---

    @Override
    public StreamLayout layout() {
        return layout;
    }

    @Override
    public StreamWriter writer() {
        return writer;
    }

    @Override
    public StreamReader reader() {
        return reader;
    }

    @Override
    public synchronized Log getLog(LogId logId) {
        if (closed) {
            throw new IllegalStateException("Stream " + identifier.fullName() + " is closed");
        }
        return logCache.computeIfAbsent(logId, id ->
            new LogImpl(id, logStorage, unifiedReader, entryIndexCache, streamStateManager));
    }

    // --- Trim ---

    @Override
    public CompletableFuture<StreamPosition> softTrim(StreamPosition position) {
        if (position instanceof IndexedStreamPosition isp) {
            return logStorage.softTrim(isp.logId(), isp.offset())
                .thenApply(newFirstOffset ->
                    layout.position(isp.logId(), newFirstOffset));
        }
        return CompletableFuture.failedFuture(
            new IllegalArgumentException("Unsupported position type: " + position.getClass()));
    }

    @Override
    public CompletableFuture<Void> hardTrim(StreamPosition position) {
        if (position instanceof IndexedStreamPosition isp) {
            return logStorage.hardTrim(isp.logId(), isp.offset());
        }
        return CompletableFuture.failedFuture(
            new IllegalArgumentException("Unsupported position type: " + position.getClass()));
    }

    @Override
    public synchronized void close() throws Exception {
        // Closing is terminal even when a component close fails. Subsequent close() calls retry only
        // failed components, while getLog() can no longer create a handle that escapes this close pass.
        closed = true;
        Throwable firstFailure = null;
        for (Map.Entry<LogId, LogImpl> entry : logCache.entrySet()) {
            try {
                entry.getValue().close();
                logCache.remove(entry.getKey(), entry.getValue());
            } catch (Exception | Error closeFailure) {
                firstFailure = addCloseFailure(firstFailure, closeFailure);
            }
        }
        if (!writerClosed) {
            try {
                writer.close();
                writerClosed = true;
            } catch (Exception | Error closeFailure) {
                firstFailure = addCloseFailure(firstFailure, closeFailure);
            }
        }
        if (!readerClosed) {
            try {
                reader.close();
                readerClosed = true;
            } catch (Exception | Error closeFailure) {
                firstFailure = addCloseFailure(firstFailure, closeFailure);
            }
        }
        if (firstFailure instanceof Exception exception) {
            throw exception;
        }
        if (firstFailure instanceof Error error) {
            throw error;
        }
    }

    private static Throwable addCloseFailure(Throwable firstFailure, Throwable closeFailure) {
        if (firstFailure == null) {
            return closeFailure;
        }
        firstFailure.addSuppressed(closeFailure);
        return firstFailure;
    }
}
