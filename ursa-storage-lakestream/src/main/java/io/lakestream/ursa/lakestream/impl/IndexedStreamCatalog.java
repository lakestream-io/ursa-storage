/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.lakestream.impl;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.lakestream.api.CatalogPaths;
import io.lakestream.api.ExternalStreamRegistry;
import io.lakestream.api.LifecycleState;
import io.lakestream.api.Log;
import io.lakestream.api.LogId;
import io.lakestream.api.LogStateManager;
import io.lakestream.api.LogStorage;
import io.lakestream.api.Namespace;
import io.lakestream.api.Partitioning;
import io.lakestream.api.PartitioningStrategy;
import io.lakestream.api.SchemaConfig;
import io.lakestream.api.Stream;
import io.lakestream.api.StreamCatalog;
import io.lakestream.api.StreamConfig;
import io.lakestream.api.StreamIdentifier;
import io.lakestream.api.StreamLayout;
import io.lakestream.api.StreamReader;
import io.lakestream.api.StreamWriter;
import io.lakestream.api.exception.AlreadyExistsException;
import io.lakestream.api.exception.NamespaceNotEmptyException;
import io.lakestream.api.exception.NoSuchNamespaceException;
import io.lakestream.api.exception.NoSuchStreamException;
import io.lakestream.api.materialization.TableCatalog;
import io.lakestream.api.materialization.TableMaterializationPolicy;
import io.lakestream.ursa.catalog.metadata.LogMetadata;
import io.lakestream.ursa.catalog.metadata.LogMetadataSerde;
import io.lakestream.ursa.lakestream.impl.materialization.MaterializationJson;
import io.lakestream.ursa.lakestream.reader.CompactedObjectReader;
import io.lakestream.ursa.lakestream.reader.CompactedObjectReaderFactory;
import io.lakestream.ursa.storage.impl.EntryIndexCache;
import io.lakestream.ursa.storage.impl.exception.NoSuchKeyException;
import io.oxia.client.api.AsyncOxiaClient;
import io.oxia.client.api.exceptions.KeyAlreadyExistsException;
import io.oxia.client.api.options.PutOption;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.function.Function;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

/**
 * Oxia-backed {@link StreamCatalog} using indexed partitions.
 *
 * <p>Uses {@link AsyncOxiaClient} directly for metadata storage and
 * {@link LogMetadataSerde} for backward-compatible serialization
 * of partition metadata.
 *
 * <p>{@code createStream()} and {@code loadStream()} return fully initialized
 * {@link StreamImpl} handles with layout, writer, reader, and per-log access.
 */
@Slf4j
public class IndexedStreamCatalog implements StreamCatalog, ExternalStreamRegistry {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final LogMetadataSerde LOG_METADATA_SERDE = LogMetadataSerde.INSTANCE;
    /**
     * Placeholder {@link LogId} for a partition that is not yet registered, used by
     * {@link #getLayoutTolerant} so a stream can be loaded for materialization before every sibling
     * partition has been lazily registered. It is never opened for I/O on the materialization path;
     * resolving it for reads/writes fails fast, which is the correct outcome for a partition that does
     * not exist yet.
     */
    private static final LogId UNREGISTERED_PARTITION = LogId.of(-1L);

    @Getter
    private final AsyncOxiaClient oxiaClient;
    private final IndexedStreamConfigStore streamConfigStore;
    private final CatalogPaths catalogPaths;
    private final LogStorage logStorage;
    private final Function<LogId, Log> logFactory;
    private final LogFactory namedLogFactory;
    private final boolean supportsReaderAwareLogCreation;
    @Getter
    private final LogStateManager logStateManager;
    private final Function<Optional<String>, CompletableFuture<Long>> streamIdGenerator;
    @Nullable
    private final Function<String, CompletableFuture<Long>> streamIdLookup;
    @Nullable
    private final Function<String, CompletableFuture<Void>> streamIdMappingDeleter;
    @Nullable
    private final CompactedObjectReaderFactory readerFactory;
    @Nullable
    private final EntryIndexCache entryIndexCache;
    private final List<AutoCloseable> ownedResources;
    private volatile String catalogName;
    /**
     * Cluster-wide default materialization policy — the lowest-priority baseline used by
     * {@link StreamImpl#effectiveMaterialization()} when a stream has neither its own nor a
     * namespace-level policy. Set in-memory by the compaction bootstrap (re-derived from config on
     * every startup), so it is not persisted to Oxia.
     */
    private volatile Optional<TableMaterializationPolicy> clusterDefaultPolicy = Optional.empty();

    /**
     * Full constructor used by {@link StreamCatalogService}.
     */
    public IndexedStreamCatalog(AsyncOxiaClient oxiaClient, CatalogPaths catalogPaths,
                                LogStorage logStorage,
                                Function<LogId, Log> logFactory,
                                @Nullable LogStateManager logStateManager,
                                Function<Optional<String>, CompletableFuture<Long>> streamIdGenerator,
                                @Nullable CompactedObjectReaderFactory readerFactory,
                                @Nullable EntryIndexCache entryIndexCache,
                                List<AutoCloseable> ownedResources) {
        this(oxiaClient, catalogPaths, logStorage, logFactory,
            (name, logId, reader) -> logFactory.apply(logId), false, logStateManager,
            streamIdGenerator, readerFactory, entryIndexCache, null, null, ownedResources);
    }

    public IndexedStreamCatalog(AsyncOxiaClient oxiaClient, CatalogPaths catalogPaths,
                                LogStorage logStorage,
                                LogFactory namedLogFactory,
                                @Nullable LogStateManager logStateManager,
                                Function<Optional<String>, CompletableFuture<Long>> streamIdGenerator,
                                @Nullable CompactedObjectReaderFactory readerFactory,
                                @Nullable EntryIndexCache entryIndexCache,
                                List<AutoCloseable> ownedResources) {
        this(oxiaClient, catalogPaths, logStorage, logId -> namedLogFactory.create(null, logId, null),
            namedLogFactory, true, logStateManager, streamIdGenerator, readerFactory, entryIndexCache,
            null, null, ownedResources);
    }

    public IndexedStreamCatalog(AsyncOxiaClient oxiaClient, CatalogPaths catalogPaths,
                                LogStorage logStorage,
                                LogFactory namedLogFactory,
                                @Nullable LogStateManager logStateManager,
                                Function<Optional<String>, CompletableFuture<Long>> streamIdGenerator,
                                Function<String, CompletableFuture<Long>> streamIdLookup,
                                Function<String, CompletableFuture<Void>> streamIdMappingDeleter,
                                @Nullable CompactedObjectReaderFactory readerFactory,
                                @Nullable EntryIndexCache entryIndexCache,
                                List<AutoCloseable> ownedResources) {
        this(oxiaClient, catalogPaths, logStorage, logId -> namedLogFactory.create(null, logId, null),
            namedLogFactory, true, logStateManager, streamIdGenerator, readerFactory, entryIndexCache,
            streamIdLookup, streamIdMappingDeleter, ownedResources);
    }

    private IndexedStreamCatalog(AsyncOxiaClient oxiaClient, CatalogPaths catalogPaths,
                                LogStorage logStorage,
                                Function<LogId, Log> logFactory,
                                LogFactory namedLogFactory,
                                boolean supportsReaderAwareLogCreation,
                                @Nullable LogStateManager logStateManager,
                                Function<Optional<String>, CompletableFuture<Long>> streamIdGenerator,
                                @Nullable CompactedObjectReaderFactory readerFactory,
                                @Nullable EntryIndexCache entryIndexCache,
                                @Nullable Function<String, CompletableFuture<Long>> streamIdLookup,
                                @Nullable Function<String, CompletableFuture<Void>> streamIdMappingDeleter,
                                List<AutoCloseable> ownedResources) {
        this.oxiaClient = oxiaClient;
        this.catalogPaths = catalogPaths;
        this.streamConfigStore = new IndexedStreamConfigStore(oxiaClient, catalogPaths);
        this.logStorage = logStorage;
        this.logFactory = logFactory;
        this.namedLogFactory = namedLogFactory;
        this.supportsReaderAwareLogCreation = supportsReaderAwareLogCreation;
        this.logStateManager = logStateManager;
        this.streamIdGenerator = streamIdGenerator;
        this.streamIdLookup = streamIdLookup;
        this.streamIdMappingDeleter = streamIdMappingDeleter;
        this.readerFactory = readerFactory;
        this.entryIndexCache = entryIndexCache;
        this.ownedResources = ownedResources != null ? ownedResources : List.of();
    }

    @Override
    public String name() {
        return catalogName;
    }

    @Override
    public CompletableFuture<Void> initialize(String name, Map<String, String> properties) {
        this.catalogName = name;
        return CompletableFuture.completedFuture(null);
    }

    // --- Log factory methods ---

    /**
     * Creates a {@link Log} instance for the given log ID.
     */
    public Log createLog(LogId logId) {
        return logFactory.apply(logId);
    }

    /**
     * Creates a named log with a dedicated compacted-object reader.
     *
     * <p>Ownership of the reader transfers to the returned log. If log creation fails, this method
     * closes the reader before propagating the failure.
     */
    public Log createLog(String name, LogId logId) {
        if (!supportsReaderAwareLogCreation) {
            throw new UnsupportedOperationException(
                "Named log creation requires an IndexedStreamCatalog.LogFactory");
        }
        CompactedObjectReader reader = openCompactedObjectReader(name);
        if (reader == null) {
            throw new IllegalStateException("CompactedObjectReaderFactory returned null for " + name);
        }
        return createLog(name, logId, reader);
    }

    /**
     * Creates a named log using a caller-supplied compacted-object reader.
     *
     * <p>Ownership of {@code reader} transfers to this method when it is invoked. On success, the
     * returned log owns the reader and closes it with the log. If the catalog cannot create the log,
     * this method closes the reader before propagating the failure. A {@code null} reader is rejected
     * before the log factory is invoked.
     *
     * @param name external reader name for the log
     * @param logId log identifier
     * @param reader non-null compacted-object reader whose ownership is transferred
     * @return the created log
     * @throws NullPointerException if {@code reader} is null
     */
    public Log createLog(String name, LogId logId, CompactedObjectReader reader) {
        Objects.requireNonNull(reader, "reader");
        try {
            if (!supportsReaderAwareLogCreation) {
                throw new UnsupportedOperationException(
                    "Explicit reader creation requires an IndexedStreamCatalog.LogFactory");
            }
            Log result = namedLogFactory.create(name, logId, reader);
            if (result == null) {
                throw new IllegalStateException("LogFactory returned null");
            }
            return result;
        } catch (RuntimeException | Error error) {
            closeCompactedObjectReaderAfterFailure(reader, error);
            throw error;
        }
    }

    @FunctionalInterface
    public interface LogFactory {
        Log create(@Nullable String name, LogId logId, @Nullable CompactedObjectReader reader);
    }

    /**
     * Generates a new stream ID, optionally associated with a key.
     */
    public CompletableFuture<Long> generateStreamId(Optional<String> key) {
        return streamIdGenerator.apply(key);
    }

    /**
     * Opens a {@link CompactedObjectReader} for the given log name.
     */
    public CompactedObjectReader openCompactedObjectReader(String name) {
        if (readerFactory == null) {
            return new io.lakestream.ursa.lakestream.reader.NoopCompactedObjectReader();
        }
        return readerFactory.open(name);
    }

    private static void closeCompactedObjectReaderAfterFailure(
            CompactedObjectReader reader, Throwable creationFailure) {
        if (reader == null) {
            return;
        }
        try {
            reader.close();
        } catch (RuntimeException | Error closeFailure) {
            if (closeFailure != creationFailure) {
                creationFailure.addSuppressed(closeFailure);
            }
        }
    }

    /**
     * Returns the log storage used by this catalog.
     */
    public LogStorage getLogStorage() {
        return logStorage;
    }

    /**
     * Invalidates all cached entry indexes.
     */
    public void invalidateCache() {
        if (entryIndexCache != null) {
            entryIndexCache.invalidateAll();
        }
    }

    // --- Stream operations ---

    @Override
    public CompletableFuture<Stream> createStream(StreamIdentifier id, StreamConfig config,
                                                    Partitioning partitioning, SchemaConfig schema,
                                                    Map<String, String> properties) {
        return createStream(id, config, partitioning, schema, properties, Optional.empty());
    }

    @Override
    public CompletableFuture<Stream> createStream(StreamIdentifier id, StreamConfig config,
                                                    Partitioning partitioning, SchemaConfig schema,
                                                    Map<String, String> properties,
                                                    Optional<TableMaterializationPolicy> materialization) {
        return streamExists(id).thenCompose(exists -> {
            if (exists) {
                return CompletableFuture.failedFuture(
                    new AlreadyExistsException("Stream already exists: " + id.fullName()));
            }
            int numPartitions = partitioning.numPartitions();
            return createPartitions(id, numPartitions)
                .thenCompose(v -> streamConfigStore.write(
                    id, numPartitions, properties, materialization))
                .thenCompose(v -> buildStreamImpl(id, config, partitioning, schema, properties,
                    LifecycleState.ACTIVE, materialization));
        });
    }

    private CompletableFuture<Void> createPartitions(StreamIdentifier id, int numPartitions) {
        CompletableFuture<Void> chain = CompletableFuture.completedFuture(null);
        for (int i = 0; i < numPartitions; i++) {
            final int partIdx = i;
            chain = chain.thenCompose(v ->
                streamIdGenerator.apply(Optional.empty()).thenCompose(streamId -> {
                    LogMetadata metadata = new LogMetadata(
                        streamId, Map.of(), OptionalLong.empty());
                    return writePartitionMetadata(id, partIdx, metadata);
                })
            );
        }
        return chain;
    }

    private CompletableFuture<Void> writePartitionMetadata(StreamIdentifier id, int partitionIndex,
                                                            LogMetadata metadata) {
        String path = catalogPaths.partitionMetadataPath(id, partitionIndex);
        try {
            byte[] bytes = LOG_METADATA_SERDE.serialize(path, metadata);
            return oxiaClient.put(path, bytes).thenApply(r -> null);
        } catch (Exception e) {
            return CompletableFuture.failedFuture(e);
        }
    }

    @Override
    public CompletableFuture<Void> registerExternalStream(StreamIdentifier id, int partitionCount,
                                                          Map<String, String> properties) {
        return streamConfigStore.registerExternalStream(id, partitionCount, properties);
    }

    @Override
    public CompletableFuture<Void> unregisterExternalStream(StreamIdentifier id) {
        return streamConfigStore.unregisterExternalStream(id);
    }

    /**
     * Idempotently registers a single partition of a stream whose log was created
     * outside the catalog (for example, a topic created by the broker rather than through
     * {@link #createStream}). Writes the catalog partition metadata for {@code partitionIndex}
     * with the supplied real {@code streamId}, then grows the stream config so its partition count
     * is at least {@code partitionIndex + 1}, preserving any existing properties and
     * materialization policy.
     *
     * <p>This is what lets the materialization compaction worker resolve a broker-created stream via
     * {@link #loadStream}; without a registered stream config {@code loadStream} throws
     * {@link NoSuchStreamException}. Safe to call concurrently and repeatedly: partition metadata is
     * written only when absent, and the stream-config grow is a version-guarded compare-and-set that
     * retries on a concurrent update.
     */
    @Override
    public CompletableFuture<Void> registerExternalPartition(StreamIdentifier id, int partitionIndex,
                                                             long streamId,
                                                             @Nullable Map<String, String> properties) {
        Map<String, String> props = properties == null ? Map.of() : properties;
        return writePartitionMetadataIfAbsent(id, partitionIndex, streamId)
            .thenCompose(v -> streamConfigStore.registerExternalStream(
                id, partitionIndex + 1, props));
    }

    @Override
    public CompletableFuture<Log> openExternalPartition(StreamIdentifier id, int partitionIndex,
                                                        Map<String, String> properties) {
        Objects.requireNonNull(id, "id");
        Objects.requireNonNull(properties, "properties");
        if (partitionIndex < 0) {
            throw new IllegalArgumentException("partitionIndex must be non-negative");
        }
        String logName = catalogPaths.compactedReaderName(id, partitionIndex);
        return streamIdGenerator.apply(Optional.of(logName))
            .thenCompose(streamId -> registerExternalPartition(id, partitionIndex, streamId, properties)
                .thenApply(__ -> createLog(logName, LogId.of(streamId))));
    }

    @Override
    public CompletableFuture<Void> deleteExternalPartition(StreamIdentifier id, int partitionIndex) {
        Objects.requireNonNull(id, "id");
        if (partitionIndex < 0) {
            throw new IllegalArgumentException("partitionIndex must be non-negative");
        }
        if (streamIdLookup == null || streamIdMappingDeleter == null) {
            return CompletableFuture.failedFuture(new UnsupportedOperationException(
                "External partition deletion requires keyed stream-ID lifecycle support"));
        }

        String logName = catalogPaths.compactedReaderName(id, partitionIndex);
        String metadataPath = catalogPaths.partitionMetadataPath(id, partitionIndex);
        return findExternalPartitionStreamId(metadataPath, logName)
            .thenCompose(streamId -> {
                CompletableFuture<Void> deleteData = streamId.isPresent()
                    ? logStorage.deleteLog(LogId.of(streamId.getAsLong()))
                    : CompletableFuture.completedFuture(null);
                return deleteData
                    .thenCompose(__ -> oxiaClient.delete(metadataPath).thenApply(ignored -> null))
                    .thenCompose(__ -> streamIdMappingDeleter.apply(logName));
            });
    }

    private CompletableFuture<OptionalLong> findExternalPartitionStreamId(
            String metadataPath, String logName) {
        return oxiaClient.get(metadataPath).thenCompose(result -> {
            if (result != null) {
                try {
                    return CompletableFuture.completedFuture(OptionalLong.of(
                        LOG_METADATA_SERDE.deserialize(metadataPath, result.value()).streamId()));
                } catch (Exception e) {
                    return CompletableFuture.failedFuture(e);
                }
            }
            return streamIdLookup.apply(logName)
                .handle((streamId, failure) -> {
                    if (failure == null) {
                        return OptionalLong.of(streamId);
                    }
                    Throwable cause = rootCause(failure);
                    if (cause instanceof NoSuchKeyException) {
                        return OptionalLong.empty();
                    }
                    throw new CompletionException(cause);
                });
        });
    }

    private static Throwable rootCause(Throwable failure) {
        Throwable current = failure;
        while ((current instanceof CompletionException || current instanceof java.util.concurrent.ExecutionException)
                && current.getCause() != null) {
            current = current.getCause();
        }
        return current;
    }

    private CompletableFuture<Void> writePartitionMetadataIfAbsent(StreamIdentifier id, int partitionIndex,
                                                                   long streamId) {
        String path = catalogPaths.partitionMetadataPath(id, partitionIndex);
        LogMetadata metadata = new LogMetadata(streamId, Map.of(), OptionalLong.empty());
        byte[] bytes;
        try {
            bytes = LOG_METADATA_SERDE.serialize(path, metadata);
        } catch (Exception e) {
            return CompletableFuture.failedFuture(e);
        }
        return oxiaClient.put(path, bytes, Set.of(PutOption.IfRecordDoesNotExist))
            .handle((r, ex) -> {
                if (ex != null && !(unwrap(ex) instanceof KeyAlreadyExistsException)) {
                    throw new CompletionException(unwrap(ex));
                }
                return (Void) null;
            });
    }

    private static Throwable unwrap(Throwable ex) {
        return ex instanceof CompletionException && ex.getCause() != null ? ex.getCause() : ex;
    }

    @Override
    public CompletableFuture<Stream> loadStream(StreamIdentifier id) {
        return streamConfigStore.read(id).thenCompose(config -> {
            Partitioning partitioning = new Partitioning(
                PartitioningStrategy.INDEXED,
                Map.of("numPartitions", String.valueOf(config.partitions())));
            return buildStreamImpl(id, new StreamConfig(), partitioning, new SchemaConfig(),
                config.properties(), LifecycleState.ACTIVE, config.materialization());
        });
    }

    private CompletableFuture<Stream> buildStreamImpl(StreamIdentifier id, StreamConfig config,
                                                       Partitioning partitioning, SchemaConfig schema,
                                                       Map<String, String> properties, LifecycleState state,
                                                       Optional<TableMaterializationPolicy> materialization) {
        return getLayoutTolerant(id).thenCompose(layout ->
            createUnifiedReader(id, layout).thenApply(unifiedReader ->
                new StreamImpl(id, config, partitioning, schema, properties, state,
                    layout, logStorage, unifiedReader, entryIndexCache, logStateManager,
                    materialization, this::loadNamespaceMaterializationFromCache,
                    this::clusterDefaultMaterialization, this::lookupTableCatalog)));
    }

    /**
     * Helper for {@link StreamImpl#effectiveMaterialization()}: synchronously load
     * the namespace's materialization policy. The {@link CompletableFuture} contract
     * of {@link #loadNamespaceMetadata(String)} means we block on the join, which is
     * acceptable on a metadata-cache-hit path and avoids leaking async machinery into
     * the {@link Stream} interface.
     */
    Optional<TableMaterializationPolicy> loadNamespaceMaterializationFromCache(String namespace) {
        try {
            Namespace ns = loadNamespaceMetadata(namespace).join();
            return ns.materialization();
        } catch (Exception e) {
            log.debug("Failed to load namespace materialization for {}: {}", namespace, e.toString());
            return Optional.empty();
        }
    }

    /**
     * Helper for {@link StreamImpl#effectiveMaterialization()}: synchronously look up
     * a registered table catalog by name. Returns {@link Optional#empty()} if the
     * catalog is missing or the lookup fails.
     */
    Optional<TableCatalog> lookupTableCatalog(String name) {
        try {
            TableCatalog catalog = getTableCatalog(name).join();
            return Optional.ofNullable(catalog);
        } catch (Exception e) {
            log.debug("Failed to load table catalog {}: {}", name, e.toString());
            return Optional.empty();
        }
    }

    @Override
    public CompletableFuture<List<StreamIdentifier>> listStreams(String namespaceName) {
        String prefix = catalogPaths.streamConfigPrefix(namespaceName);
        String endKey = prefix + "\uffff";
        return oxiaClient.list(prefix, endKey).thenApply(keys ->
            keys.stream()
                .map(key -> new StreamIdentifier(namespaceName, key.substring(prefix.length())))
                .collect(Collectors.toList())
        );
    }

    @Override
    public CompletableFuture<Boolean> dropStream(StreamIdentifier id, boolean purge) {
        return streamExists(id).thenCompose(exists -> {
            if (!exists) {
                return CompletableFuture.completedFuture(false);
            }
            return readPartitionCount(id).thenCompose(numPartitions -> {
                CompletableFuture<Void> chain = CompletableFuture.completedFuture(null);
                for (int i = 0; i < numPartitions; i++) {
                    final int partIdx = i;
                    chain = chain.thenCompose(v -> {
                        String partPath = catalogPaths.partitionMetadataPath(id, partIdx);
                        if (purge) {
                            return readPartitionMetadata(id, partIdx)
                                .thenCompose(meta ->
                                    logStorage.deleteLog(LogId.of(meta.streamId()))
                                        .thenCompose(v2 -> oxiaClient.delete(partPath))
                                        .thenApply(r -> null));
                        }
                        return oxiaClient.delete(partPath).thenApply(r -> null);
                    });
                }
                return chain.thenCompose(v -> {
                    String configPath = catalogPaths.streamConfigPath(id);
                    return oxiaClient.delete(configPath).thenApply(r -> true);
                });
            });
        });
    }

    @Override
    public CompletableFuture<Boolean> streamExists(StreamIdentifier id) {
        return streamConfigStore.exists(id);
    }

    @Override
    public CompletableFuture<Void> setStreamProperties(StreamIdentifier id, Map<String, String> props) {
        return streamConfigStore.setProperties(id, props);
    }

    @Override
    public CompletableFuture<Void> removeStreamProperties(StreamIdentifier id, List<String> keys) {
        return streamConfigStore.removeProperties(id, keys);
    }

    // --- Layout / Data Plane ---

    @Override
    public CompletableFuture<StreamLayout> getLayout(StreamIdentifier id) {
        return readPartitionCount(id).thenCompose(numPartitions -> {
            List<CompletableFuture<LogId>> futures = new ArrayList<>();
            for (int i = 0; i < numPartitions; i++) {
                final int partIdx = i;
                futures.add(readPartitionMetadata(id, partIdx)
                    .thenApply(meta -> LogId.of(meta.streamId())));
            }
            return CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]))
                .thenApply(v -> {
                    List<LogId> logIds = futures.stream()
                        .map(CompletableFuture::join)
                        .toList();
                    return new IndexedLayout(logIds);
                });
        });
    }

    /**
     * Builds a layout tolerant of partitions that are not yet registered, used by {@link #loadStream}.
     *
     * <p>Partitions created by an external control plane are registered lazily, one per compaction task, via
     * {@link #registerExternalPartition}, which grows the stream's partition count to the highest index
     * seen. During that window the recorded count can exceed the
     * set of partitions whose metadata has actually been written, so the strict {@link #getLayout} (which
     * requires every partition {@code 0..count-1}) throws {@link NoSuchStreamException} until every
     * sibling partition's task has run. Materializing one partition must not depend on its siblings being
     * compacted first, so this variant substitutes {@link #UNREGISTERED_PARTITION} for a partition whose
     * metadata is absent, preserving index alignment. The materialization path never touches the layout;
     * the public {@link #getLayout} stays strict for native readers that need a complete layout.
     */
    private CompletableFuture<StreamLayout> getLayoutTolerant(StreamIdentifier id) {
        return readPartitionCount(id).thenCompose(numPartitions -> {
            List<CompletableFuture<LogId>> futures = new ArrayList<>();
            for (int i = 0; i < numPartitions; i++) {
                final int partIdx = i;
                futures.add(readPartitionMetadata(id, partIdx)
                    .handle((meta, ex) -> {
                        if (ex != null) {
                            if (unwrap(ex) instanceof NoSuchStreamException) {
                                log.debug("Partition {} of {} not yet registered; using a placeholder in "
                                    + "the materialization layout", partIdx, id.fullName());
                                return UNREGISTERED_PARTITION;
                            }
                            throw new CompletionException(unwrap(ex));
                        }
                        return LogId.of(meta.streamId());
                    }));
            }
            return CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]))
                .thenApply(v -> new IndexedLayout(futures.stream()
                    .map(CompletableFuture::join)
                    .toList()));
        });
    }

    @Override
    public CompletableFuture<StreamWriter> openWriter(StreamIdentifier id) {
        return getLayout(id).thenApply(layout -> new StreamWriterImpl(layout, logStorage));
    }

    @Override
    public CompletableFuture<StreamReader> openReader(StreamIdentifier id) {
        return getLayout(id).thenCompose(layout ->
            createUnifiedReader(id, layout).thenApply(unifiedReader -> unifiedReader == null
                ? new StreamReaderImpl(layout, logStorage)
                : new StreamReaderImpl(layout, unifiedReader)));
    }

    private CompletableFuture<UnifiedStreamReader> createUnifiedReader(
            StreamIdentifier id, StreamLayout layout) {
        if (!supportsReaderAwareLogCreation) {
            return CompletableFuture.completedFuture(null);
        }
        return layout.logIds().thenApply(logIds -> {
            Map<LogId, String> readerNames = new HashMap<>();
            for (int i = 0; i < logIds.size(); i++) {
                LogId logId = logIds.get(i);
                if (!UNREGISTERED_PARTITION.equals(logId)) {
                    readerNames.put(logId, catalogPaths.compactedReaderName(id, i));
                }
            }
            return new PartitionedUnifiedStreamReader(logId -> {
                String readerName = readerNames.get(logId);
                if (readerName == null) {
                    throw new IllegalArgumentException(
                        "Log " + logId + " is not registered in stream " + id.fullName());
                }
                return createLog(readerName, logId);
            });
        });
    }

    // --- Lifecycle ---

    @Override
    public CompletableFuture<Void> sealStream(StreamIdentifier id) {
        return readPartitionCount(id).thenCompose(numPartitions -> {
            CompletableFuture<Void> chain = CompletableFuture.completedFuture(null);
            for (int i = 0; i < numPartitions; i++) {
                final int partIdx = i;
                chain = chain.thenCompose(v -> readPartitionMetadata(id, partIdx).thenCompose(metadata -> {
                    metadata.setTerminatedOffset(OptionalLong.of(0));
                    return writePartitionMetadata(id, partIdx, metadata);
                }));
            }
            return chain;
        });
    }

    @Override
    public CompletableFuture<Void> truncateStream(StreamIdentifier id) {
        return readPartitionCount(id).thenCompose(numPartitions -> {
            CompletableFuture<Void> chain = CompletableFuture.completedFuture(null);
            for (int i = 0; i < numPartitions; i++) {
                final int partIdx = i;
                chain = chain.thenCompose(v -> readPartitionMetadata(id, partIdx)
                    .thenCompose(meta -> logStorage.deleteLog(LogId.of(meta.streamId()))));
            }
            return chain;
        });
    }

    // --- Namespace operations ---

    @Override
    public CompletableFuture<Void> createNamespace(Namespace namespace) {
        return namespaceExists(namespace.name()).thenCompose(exists -> {
            if (exists) {
                return CompletableFuture.failedFuture(
                    new AlreadyExistsException("Namespace already exists: " + namespace.name()));
            }
            return writeNamespace(namespace);
        });
    }

    @Override
    public CompletableFuture<List<Namespace>> listNamespaces() {
        String prefix = catalogPaths.namespacesPrefix();
        String endKey = prefix + "\uffff";
        return oxiaClient.list(prefix, endKey).thenCompose(keys -> {
            List<CompletableFuture<Namespace>> futures = keys.stream()
                .map(key -> {
                    String nsName = key.substring(prefix.length());
                    return loadNamespaceMetadata(nsName);
                })
                .toList();
            return CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]))
                .thenApply(v -> futures.stream()
                    .map(CompletableFuture::join)
                    .toList());
        });
    }

    @Override
    public CompletableFuture<Namespace> loadNamespaceMetadata(String namespaceName) {
        String path = catalogPaths.namespacePath(namespaceName);
        return oxiaClient.get(path).thenApply(result -> {
            if (result == null) {
                throw new NoSuchNamespaceException(namespaceName);
            }
            try {
                return parseNamespace(namespaceName, result.value());
            } catch (Exception e) {
                throw new RuntimeException("Failed to deserialize namespace metadata", e);
            }
        });
    }

    /**
     * Decode the on-disk namespace metadata, accepting both the legacy shape
     * (a top-level {@code Map<String, String>} of properties) and the current
     * shape ({@code {"properties": {...}, "materialization": {...}}}).
     */
    private Namespace parseNamespace(String name, byte[] payload) throws java.io.IOException {
        JsonNode node = MAPPER.readTree(payload);
        if (isLegacyNamespaceShape(node)) {
            Map<String, String> props = MAPPER.convertValue(node,
                new TypeReference<Map<String, String>>() {});
            return new Namespace(name, props, Optional.empty());
        }
        Map<String, String> props = node.has("properties")
            ? MAPPER.convertValue(node.get("properties"),
                new TypeReference<Map<String, String>>() {})
            : Map.of();
        Optional<TableMaterializationPolicy> materialization =
            node.has("materialization") && !node.get("materialization").isNull()
                ? Optional.of(MaterializationJson.policyFromJson(node.get("materialization")))
                : Optional.empty();
        return new Namespace(name, props, materialization);
    }

    /**
     * Detect the legacy {@code Map<String, String>} namespace metadata shape.
     * Modern records always carry at least one of {@code properties} or
     * {@code materialization}; legacy records may have neither (empty map) or
     * may have arbitrary string keys mapped to string values.
     */
    private boolean isLegacyNamespaceShape(JsonNode node) {
        if (!node.isObject()) {
            return false;
        }
        boolean hasModernKey = node.has("properties") || node.has("materialization");
        if (hasModernKey) {
            return false;
        }
        java.util.Iterator<Map.Entry<String, JsonNode>> it = node.fields();
        while (it.hasNext()) {
            Map.Entry<String, JsonNode> entry = it.next();
            if (!entry.getValue().isTextual()) {
                return false;
            }
        }
        return true;
    }

    @Override
    public CompletableFuture<Boolean> dropNamespace(String namespaceName) {
        return namespaceExists(namespaceName).thenCompose(exists -> {
            if (!exists) {
                return CompletableFuture.completedFuture(false);
            }
            return listStreams(namespaceName).thenCompose(streams -> {
                if (!streams.isEmpty()) {
                    return CompletableFuture.failedFuture(
                        new NamespaceNotEmptyException(namespaceName));
                }
                String path = catalogPaths.namespacePath(namespaceName);
                return oxiaClient.delete(path).thenApply(r -> true);
            });
        });
    }

    @Override
    public CompletableFuture<Boolean> namespaceExists(String namespaceName) {
        String path = catalogPaths.namespacePath(namespaceName);
        return oxiaClient.get(path).thenApply(result -> result != null);
    }

    @Override
    public CompletableFuture<Void> setNamespaceProperties(String name, Map<String, String> props) {
        return loadNamespaceMetadata(name).thenCompose(ns -> {
            Map<String, String> merged = new HashMap<>(ns.properties());
            merged.putAll(props);
            return writeNamespace(new Namespace(name, merged, ns.materialization()));
        });
    }

    @Override
    public CompletableFuture<Void> removeNamespaceProperties(String name, List<String> keys) {
        return loadNamespaceMetadata(name).thenCompose(ns -> {
            Map<String, String> updated = new HashMap<>(ns.properties());
            keys.forEach(updated::remove);
            return writeNamespace(new Namespace(name, updated, ns.materialization()));
        });
    }

    // --- Materialization (T6: persisted in Oxia) ---

    @Override
    public CompletableFuture<Void> registerTableCatalog(TableCatalog catalog) {
        String path = catalogPaths.tableCatalogPath(catalog.name());
        return oxiaClient.get(path).thenCompose(existing -> {
            if (existing != null) {
                log.warn("Overwriting existing TableCatalog {}", catalog.name());
            }
            try {
                byte[] bytes = MAPPER.writeValueAsBytes(
                    MaterializationJson.tableCatalogToJson(catalog));
                return oxiaClient.put(path, bytes).thenApply(r -> null);
            } catch (Exception e) {
                return CompletableFuture.<Void>failedFuture(e);
            }
        });
    }

    @Override
    public CompletableFuture<Boolean> unregisterTableCatalog(String name) {
        String path = catalogPaths.tableCatalogPath(name);
        return oxiaClient.get(path).thenCompose(existing -> {
            if (existing == null) {
                return CompletableFuture.completedFuture(false);
            }
            return oxiaClient.delete(path).thenApply(deleted -> true);
        });
    }

    @Override
    public CompletableFuture<TableCatalog> getTableCatalog(String name) {
        String path = catalogPaths.tableCatalogPath(name);
        return oxiaClient.get(path).thenApply(result -> {
            if (result == null) {
                return null;
            }
            try {
                JsonNode node = MAPPER.readTree(result.value());
                return MaterializationJson.tableCatalogFromJson(node);
            } catch (Exception e) {
                throw new RuntimeException("Failed to deserialize TableCatalog: " + name, e);
            }
        });
    }

    @Override
    public CompletableFuture<List<TableCatalog>> listTableCatalogs() {
        String prefix = catalogPaths.tableCatalogsPrefix();
        String endKey = prefix + "\uffff";
        return oxiaClient.list(prefix, endKey).thenCompose(keys -> {
            List<CompletableFuture<TableCatalog>> futures = keys.stream()
                .map(key -> getTableCatalog(key.substring(prefix.length())))
                .toList();
            return CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]))
                .thenApply(v -> futures.stream()
                    .map(CompletableFuture::join)
                    .filter(java.util.Objects::nonNull)
                    .toList());
        });
    }

    @Override
    public CompletableFuture<Void> setNamespaceMaterialization(String namespace,
                                                                TableMaterializationPolicy policy) {
        return loadNamespaceMetadata(namespace).thenCompose(ns ->
            writeNamespace(new Namespace(ns.name(), ns.properties(), Optional.of(policy))));
    }

    @Override
    public CompletableFuture<Void> clearNamespaceMaterialization(String namespace) {
        return loadNamespaceMetadata(namespace).thenCompose(ns ->
            writeNamespace(new Namespace(ns.name(), ns.properties(), Optional.empty())));
    }

    @Override
    public CompletableFuture<Void> setClusterDefaultMaterialization(TableMaterializationPolicy policy) {
        this.clusterDefaultPolicy = Optional.of(policy);
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public Optional<TableMaterializationPolicy> clusterDefaultMaterialization() {
        return clusterDefaultPolicy;
    }

    @Override
    public CompletableFuture<Void> setStreamMaterialization(StreamIdentifier id,
                                                             TableMaterializationPolicy policy) {
        return streamConfigStore.setMaterialization(id, Optional.of(policy));
    }

    @Override
    public CompletableFuture<Void> clearStreamMaterialization(StreamIdentifier id) {
        return streamConfigStore.setMaterialization(id, Optional.empty());
    }

    // --- Close ---

    @Override
    public void close() {
        if (readerFactory != null) {
            try {
                readerFactory.close();
            } catch (Exception e) {
                log.warn("Failed to close reader factory", e);
            }
        }
        invalidateCache();
        for (AutoCloseable resource : ownedResources) {
            try {
                resource.close();
            } catch (Exception e) {
                log.warn("Failed to close owned resource: {}", resource.getClass().getSimpleName(), e);
            }
        }
    }

    // --- Internal helpers ---

    private CompletableFuture<Integer> readPartitionCount(StreamIdentifier id) {
        return streamConfigStore.read(id)
            .thenApply(IndexedStreamConfigStore.StreamConfigData::partitions);
    }

    private CompletableFuture<LogMetadata> readPartitionMetadata(StreamIdentifier id, int partitionIndex) {
        String path = catalogPaths.partitionMetadataPath(id, partitionIndex);
        return oxiaClient.get(path).thenApply(result -> {
            if (result == null) {
                throw new NoSuchStreamException(id);
            }
            try {
                return LOG_METADATA_SERDE.deserialize(path, result.value());
            } catch (Exception e) {
                throw new RuntimeException("Failed to deserialize partition metadata: " + path, e);
            }
        });
    }

    private CompletableFuture<Void> writeNamespace(Namespace namespace) {
        String path = catalogPaths.namespacePath(namespace.name());
        try {
            ObjectNode node = MAPPER.createObjectNode();
            ObjectNode props = node.putObject("properties");
            namespace.properties().forEach(props::put);
            namespace.materialization().ifPresent(policy ->
                node.set("materialization", MaterializationJson.policyToJson(policy)));
            byte[] bytes = MAPPER.writeValueAsBytes(node);
            return oxiaClient.put(path, bytes).thenApply(r -> null);
        } catch (Exception e) {
            return CompletableFuture.failedFuture(e);
        }
    }
}
