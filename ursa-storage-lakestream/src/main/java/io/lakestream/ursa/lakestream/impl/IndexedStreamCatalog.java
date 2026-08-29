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
import io.lakestream.api.exception.PartitionLifecycleFencedException;
import io.lakestream.api.materialization.TableCatalog;
import io.lakestream.api.materialization.TableMaterializationPolicy;
import io.lakestream.ursa.catalog.metadata.LogMetadata;
import io.lakestream.ursa.catalog.metadata.LogMetadata.RetiredStreamMapping;
import io.lakestream.ursa.catalog.metadata.LogMetadataSerde;
import io.lakestream.ursa.lakestream.impl.materialization.MaterializationJson;
import io.lakestream.ursa.lakestream.reader.CompactedObjectReader;
import io.lakestream.ursa.lakestream.reader.CompactedObjectReaderFactory;
import io.lakestream.ursa.storage.StorageApi;
import io.lakestream.ursa.storage.StorageApi.ActiveStreamIdMapping;
import io.lakestream.ursa.storage.StorageApi.KeyedAllocationInvalidatedException;
import io.lakestream.ursa.storage.StorageApi.StreamIdAllocation;
import io.lakestream.ursa.storage.StorageApi.StreamIdMappingConflictException;
import io.lakestream.ursa.storage.StorageApi.StreamIdMappingFence;
import io.lakestream.ursa.storage.StorageApi.StreamIdMappingFenceResult;
import io.lakestream.ursa.storage.StorageApi.StreamIdMappingOwner;
import io.lakestream.ursa.storage.impl.EntryIndexCache;
import io.lakestream.ursa.storage.impl.exception.NoSuchKeyException;
import io.oxia.client.api.AsyncOxiaClient;
import io.oxia.client.api.GetResult;
import io.oxia.client.api.PutResult;
import io.oxia.client.api.exceptions.KeyAlreadyExistsException;
import io.oxia.client.api.exceptions.UnexpectedVersionIdException;
import io.oxia.client.api.options.PutOption;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;
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
    private static final int STREAM_VISIBILITY_READ_BATCH_SIZE = 32;
    private static final int MAX_EXTERNAL_DELETION_CONTEXT_RETRIES = 3;
    private static final int MAX_PARTITION_METADATA_WRITE_RETRIES = 3;
    private static final long PARTITION_METADATA_RETRY_DELAY_MILLIS = 10L;
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
    private final Function<String, CompletableFuture<StreamIdAllocation>> keyedStreamIdAllocator;
    @Nullable
    private final Function<String, CompletableFuture<Long>> streamIdLookup;
    @Nullable
    private final StorageApi fencedMappingStorage;
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
            streamIdGenerator, null, readerFactory, entryIndexCache, null, null,
            false, null, ownedResources);
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
            namedLogFactory, true, logStateManager, streamIdGenerator,
            null, readerFactory, entryIndexCache, null, null, false, null, ownedResources);
    }

    /**
     * Creates a read-only-compatible catalog using the legacy unconditional mapping deleter.
     *
     * <p>An unconditional deleter cannot satisfy the stream-ID fencing contract. The catalog can
     * therefore still be constructed for binary and source compatibility. Metadata registration
     * and initial allocation remain available, while destructive keyed-mapping cleanup fails
     * before deleting a log or mapping.
     *
     * @deprecated use the constructor that accepts a lifecycle-aware {@link StorageApi}
     */
    @Deprecated
    public IndexedStreamCatalog(AsyncOxiaClient oxiaClient, CatalogPaths catalogPaths,
                                LogStorage logStorage,
                                LogFactory namedLogFactory,
                                @Nullable LogStateManager logStateManager,
                                Function<Optional<String>, CompletableFuture<Long>> streamIdGenerator,
                                @Nullable Function<String, CompletableFuture<Long>> streamIdLookup,
                                @Nullable Function<String, CompletableFuture<Void>>
                                    streamIdMappingDeleter,
                                @Nullable CompactedObjectReaderFactory readerFactory,
                                @Nullable EntryIndexCache entryIndexCache,
                                List<AutoCloseable> ownedResources) {
        this(oxiaClient, catalogPaths, logStorage, logId -> namedLogFactory.create(null, logId, null),
            namedLogFactory, true, logStateManager, streamIdGenerator,
            key -> streamIdGenerator.apply(Optional.of(key))
                .thenApply(streamId -> new StreamIdAllocation(streamId, false)),
            readerFactory, entryIndexCache,
            streamIdLookup,
            streamIdMappingDeleter == null ? null
                : (key, ignoredStreamId) -> streamIdMappingDeleter.apply(key),
            false, null, ownedResources);
    }

    /**
     * Creates a compatibility catalog with keyed-mapping callbacks.
     *
     * <p>Callbacks cannot persist or acknowledge an owner-aware mapping fence. They remain accepted
     * for source compatibility and initial allocation, but destructive lifecycle operations and
     * replay of retired cleanup journals fail before changing metadata, mappings, or logs.
     *
     * @deprecated use the constructor that accepts a lifecycle-aware {@link StorageApi}
     */
    @Deprecated
    public static IndexedStreamCatalog withConditionalStreamIdMappingDeletion(
            AsyncOxiaClient oxiaClient, CatalogPaths catalogPaths,
            LogStorage logStorage, LogFactory namedLogFactory,
            @Nullable LogStateManager logStateManager,
            Function<Optional<String>, CompletableFuture<Long>> streamIdGenerator,
            Function<String, CompletableFuture<Long>> streamIdLookup,
            BiFunction<String, Long, CompletableFuture<Void>> streamIdMappingDeleter,
            @Nullable CompactedObjectReaderFactory readerFactory,
            @Nullable EntryIndexCache entryIndexCache,
            List<AutoCloseable> ownedResources) {
        Objects.requireNonNull(streamIdLookup, "streamIdLookup");
        Objects.requireNonNull(streamIdMappingDeleter, "streamIdMappingDeleter");
        return new IndexedStreamCatalog(
            oxiaClient, catalogPaths, logStorage, namedLogFactory, logStateManager,
            streamIdGenerator,
            key -> streamIdGenerator.apply(Optional.of(key))
                .thenApply(streamId -> new StreamIdAllocation(streamId, false)),
            streamIdLookup, streamIdMappingDeleter, readerFactory, entryIndexCache,
            ownedResources);
    }

    /**
     * Creates a compatibility catalog with explicit keyed stream-ID callbacks.
     *
     * <p>The callbacks do not provide a durable owner-aware fence or acknowledgement token. They
     * remain accepted for source compatibility and initial allocation, but destructive lifecycle
     * operations and replay of retired cleanup journals fail before changing state.
     *
     * @deprecated use the constructor that accepts a lifecycle-aware {@link StorageApi}
     */
    @Deprecated
    public IndexedStreamCatalog(AsyncOxiaClient oxiaClient, CatalogPaths catalogPaths,
                                LogStorage logStorage,
                                LogFactory namedLogFactory,
                                @Nullable LogStateManager logStateManager,
                                Function<Optional<String>, CompletableFuture<Long>> streamIdGenerator,
                                Function<String, CompletableFuture<StreamIdAllocation>> keyedStreamIdAllocator,
                                Function<String, CompletableFuture<Long>> streamIdLookup,
                                BiFunction<String, Long, CompletableFuture<Void>> streamIdMappingDeleter,
                                @Nullable CompactedObjectReaderFactory readerFactory,
                                @Nullable EntryIndexCache entryIndexCache,
                                List<AutoCloseable> ownedResources) {
        this(oxiaClient, catalogPaths, logStorage, namedLogFactory, logStateManager,
            streamIdGenerator, keyedStreamIdAllocator, streamIdLookup,
            streamIdMappingDeleter, readerFactory, entryIndexCache, ownedResources, true);
    }

    IndexedStreamCatalog(AsyncOxiaClient oxiaClient, CatalogPaths catalogPaths,
                         LogStorage logStorage,
                         LogFactory namedLogFactory,
                         @Nullable LogStateManager logStateManager,
                         Function<Optional<String>, CompletableFuture<Long>> streamIdGenerator,
                         Function<String, CompletableFuture<StreamIdAllocation>> keyedStreamIdAllocator,
                         Function<String, CompletableFuture<Long>> streamIdLookup,
                         BiFunction<String, Long, CompletableFuture<Void>> streamIdMappingDeleter,
                         @Nullable CompactedObjectReaderFactory readerFactory,
                         @Nullable EntryIndexCache entryIndexCache,
                         List<AutoCloseable> ownedResources,
                         boolean ignoredSupportsKeyedLifecycle) {
        this(oxiaClient, catalogPaths, logStorage, logId -> namedLogFactory.create(null, logId, null),
            namedLogFactory, true, logStateManager, streamIdGenerator,
            Objects.requireNonNull(keyedStreamIdAllocator, "keyedStreamIdAllocator"),
            readerFactory, entryIndexCache,
            Objects.requireNonNull(streamIdLookup, "streamIdLookup"),
            Objects.requireNonNull(streamIdMappingDeleter, "streamIdMappingDeleter"),
            ignoredSupportsKeyedLifecycle, null, ownedResources);
    }

    /**
     * Creates the production catalog backed by a lifecycle-aware {@link StorageApi}.
     *
     * <p>Unlike the callback constructors, this constructor can durably fence and acknowledge a
     * keyed allocation cleanup before a replacement owner is allowed to publish its mapping.
     */
    public IndexedStreamCatalog(AsyncOxiaClient oxiaClient, CatalogPaths catalogPaths,
                                LogStorage logStorage,
                                LogFactory namedLogFactory,
                                @Nullable LogStateManager logStateManager,
                                StorageApi storageApi,
                                @Nullable CompactedObjectReaderFactory readerFactory,
                                @Nullable EntryIndexCache entryIndexCache,
                                List<AutoCloseable> ownedResources) {
        this(oxiaClient, catalogPaths, logStorage,
            logId -> namedLogFactory.create(null, logId, null), namedLogFactory, true,
            logStateManager, storageApi::generateStreamId,
            key -> storageApi.allocateStreamId(Optional.of(key)), readerFactory,
            entryIndexCache, storageApi::getStreamIdByKey,
            storageApi::deleteStreamIdMapping,
            storageApi.supportsConditionalStreamIdMappingDeletion(),
            storageApi.supportsFencedStreamIdMappings() ? storageApi : null,
            ownedResources);
    }

    private IndexedStreamCatalog(AsyncOxiaClient oxiaClient, CatalogPaths catalogPaths,
                                LogStorage logStorage,
                                Function<LogId, Log> logFactory,
                                LogFactory namedLogFactory,
                                boolean supportsReaderAwareLogCreation,
                                @Nullable LogStateManager logStateManager,
                                Function<Optional<String>, CompletableFuture<Long>> streamIdGenerator,
                                @Nullable Function<String,
                                    CompletableFuture<StreamIdAllocation>> keyedStreamIdAllocator,
                                @Nullable CompactedObjectReaderFactory readerFactory,
                                @Nullable EntryIndexCache entryIndexCache,
                                @Nullable Function<String, CompletableFuture<Long>> streamIdLookup,
                                @Nullable BiFunction<String, Long, CompletableFuture<Void>>
                                    ignoredStreamIdMappingDeleter,
                                boolean ignoredSupportsKeyedLifecycle,
                                @Nullable StorageApi fencedMappingStorage,
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
        this.keyedStreamIdAllocator = keyedStreamIdAllocator;
        this.streamIdLookup = streamIdLookup;
        this.fencedMappingStorage = fencedMappingStorage;
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

    private CompletableFuture<Void> preflightRetiredPartitionJournals(
            StreamIdentifier id, int partitionCount, String operation) {
        if (fencedMappingStorage != null) {
            return CompletableFuture.completedFuture(null);
        }
        CompletableFuture<Void> preflight = CompletableFuture.completedFuture(null);
        for (int partitionIndex = 0; partitionIndex < partitionCount; partitionIndex++) {
            int checkedPartition = partitionIndex;
            preflight = preflight.thenCompose(ignored ->
                preflightRetiredPartitionJournal(id, checkedPartition, operation));
        }
        return preflight;
    }

    private CompletableFuture<Void> preflightRetiredPartitionJournal(
            StreamIdentifier id, int partitionIndex, String operation) {
        if (fencedMappingStorage != null) {
            return CompletableFuture.completedFuture(null);
        }
        String path = catalogPaths.partitionMetadataPath(id, partitionIndex);
        return oxiaClient.get(path).thenCompose(existing -> {
            if (existing == null) {
                return CompletableFuture.completedFuture(null);
            }
            final LogMetadata metadata;
            try {
                metadata = LOG_METADATA_SERDE.deserialize(path, existing.value());
            } catch (Exception e) {
                return CompletableFuture.failedFuture(e);
            }
            if (!metadata.deleted() && !hasRetiredJournal(metadata)) {
                return CompletableFuture.completedFuture(null);
            }
            return CompletableFuture.failedFuture(new UnsupportedOperationException(
                operation + " requires durable fenced stream-ID lifecycle support before "
                    + "claiming a partition with retired cleanup state"));
        });
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
        int numPartitions = partitioning.numPartitions();
        String ownerToken = UUID.randomUUID().toString();
        return preflightRetiredPartitionJournals(id, numPartitions, "Stream creation")
            .thenCompose(ignored -> streamConfigStore.claimCreation(
                id, numPartitions, properties, materialization,
                IndexedStreamConfigStore.CreationKind.NATIVE_CREATE, ownerToken))
            .thenCompose(claim -> createPartitions(id, claim)
                .thenCompose(ignored -> streamConfigStore.verifyProvisioningOwnership(id, claim))
                .thenCompose(ignored -> streamConfigStore.finalizeCreation(id, claim))
                .thenCompose(outcome -> outcome.active()
                    ? buildStreamImpl(
                        id, config, partitioning, schema, properties,
                        LifecycleState.ACTIVE, materialization)
                    : CompletableFuture.failedFuture(outcome.failure())));
    }

    private CompletableFuture<Void> createPartitions(
            StreamIdentifier id, IndexedStreamConfigStore.ProvisioningClaim claim) {
        CompletableFuture<Void> chain = CompletableFuture.completedFuture(null);
        for (int i = 0; i < claim.config().partitions(); i++) {
            final int partIdx = i;
            String allocationKey = nativePartitionAllocationKey(id, claim, partIdx);
            chain = chain
                .thenCompose(ignored ->
                    streamConfigStore.verifyProvisioningOwnership(id, claim))
                .thenCompose(ignored ->
                    prepareRetiredNativePartition(id, partIdx, claim))
                .thenCompose(prepared -> allocateNativeKeyedStreamId(
                    allocationKey, streamIdMappingOwner(claim),
                    prepared.acknowledgedFence())
                    .thenApply(streamId -> {
                        if (prepared.retiredStreamIds().contains(streamId)) {
                            throw new RetiredStreamIdAllocationException(
                                new StreamIdAllocation(streamId, false),
                                "Native partition " + id.fullName() + "-partition-"
                                    + partIdx
                                    + " cannot reuse a retired physical stream ID");
                        }
                        return streamId;
                    })
                    .handle((streamId, failure) -> new NativeAllocationAttempt(
                        streamId, unwrapNullable(failure))))
                .thenCompose(attempt -> {
                    if (attempt.failure() == null) {
                        return verifyAndWriteNativePartitionAfterAllocation(
                            id, partIdx, allocationKey, attempt.streamId(), claim);
                    }
                    Throwable cause = rootCause(attempt.failure());
                    if (cause instanceof KeyedAllocationInvalidatedException invalidated) {
                        return compensateRejectedNativeAllocation(
                            id, partIdx, allocationKey,
                            invalidated.allocation().streamId(), claim, cause);
                    }
                    if (cause instanceof RetiredStreamIdAllocationException retired) {
                        return compensateRetiredNativeAllocation(
                            id, partIdx, allocationKey,
                            retired.allocation().streamId(), claim, cause);
                    }
                    return CompletableFuture.failedFuture(cause);
                })
                .thenCompose(ignored ->
                    streamConfigStore.verifyProvisioningOwnership(id, claim));
        }
        return chain;
    }

    private CompletableFuture<PreparedNativePartition> prepareRetiredNativePartition(
            StreamIdentifier id, int partitionIndex,
            IndexedStreamConfigStore.ProvisioningClaim claim) {
        String path = catalogPaths.partitionMetadataPath(id, partitionIndex);
        Supplier<CompletableFuture<Void>> ownershipVerifier =
            () -> streamConfigStore.verifyProvisioningOwnership(id, claim);
        return ownershipVerifier.get()
            .thenCompose(ignored -> oxiaClient.get(path))
            .thenCompose(existing -> {
                if (existing == null) {
                    return ownershipVerifier.get().thenApply(ignored ->
                        new PreparedNativePartition(Set.of(), Optional.empty()));
                }
                final LogMetadata metadata;
                try {
                    metadata = LOG_METADATA_SERDE.deserialize(path, existing.value());
                } catch (Exception e) {
                    return CompletableFuture.failedFuture(e);
                }
                if (!hasRetiredJournal(metadata)) {
                    Set<Long> retiredStreamIds = metadata.deleted()
                        ? retiredStreamIdsWith(
                            metadata.retiredStreamIds(), metadata.streamId())
                        : metadata.retiredStreamIds();
                    return ownershipVerifier.get().thenApply(ignored ->
                        new PreparedNativePartition(
                            retiredStreamIds, acknowledgedFence(metadata)));
                }
                return sweepRetiredAllocations(
                    id, partitionIndex, ownershipVerifier,
                    ActiveMappingProtection.of(
                        streamIdMappingOwner(claim), OptionalLong.empty()))
                    .thenApply(cleaned -> new PreparedNativePartition(
                        cleaned.retiredStreamIds(), acknowledgedFence(cleaned)));
            });
    }

    private CompletableFuture<Void> compensateRetiredNativeAllocation(
            StreamIdentifier id, int partitionIndex, String allocationKey, long streamId,
            IndexedStreamConfigStore.ProvisioningClaim claim, Throwable failure) {
        return claimAndCleanupOrphanedAllocation(
                id, partitionIndex, allocationKey, streamId,
                Optional.of(claim.incarnationId()), Optional.of(claim.ownerToken()),
                claim.ownerGeneration(), AllocationCleanupProtection.missingMetadata(),
                () -> streamConfigStore.verifyProvisioningOwnership(id, claim))
            .handle((ignored, cleanupFailure) -> {
                if (cleanupFailure != null) {
                    addSuppressed(failure, cleanupFailure);
                }
                return null;
            })
            .thenCompose(ignored -> CompletableFuture.failedFuture(failure));
    }

    private CompletableFuture<Void> verifyAndWriteNativePartitionAfterAllocation(
            StreamIdentifier id, int partitionIndex, String allocationKey,
            long streamId, IndexedStreamConfigStore.ProvisioningClaim claim) {
        return streamConfigStore.verifyProvisioningOwnership(id, claim)
            .handle((ignored, failure) -> unwrapNullable(failure))
            .thenCompose(failure -> {
                if (failure == null) {
                    return writePartitionMetadataForNativeClaim(
                        id, partitionIndex, streamId, claim).thenApply(ignored -> null);
                }
                Throwable cause = rootCause(failure);
                if (!(cause
                        instanceof IndexedStreamConfigStore.ProvisioningOwnershipLostException)) {
                    return CompletableFuture.failedFuture(cause);
                }
                return compensateRejectedNativeAllocation(
                    id, partitionIndex, allocationKey, streamId, claim, cause);
            });
    }

    private CompletableFuture<Void> compensateRejectedNativeAllocation(
            StreamIdentifier id, int partitionIndex, String allocationKey, long streamId,
            IndexedStreamConfigStore.ProvisioningClaim claim,
            Throwable ownershipFailure) {
        return readLifecycleContextForCleanup(id, ownershipFailure)
            .thenCompose(read -> {
                if (read.context() == null) {
                    return CompletableFuture.failedFuture(ownershipFailure);
                }
                IndexedStreamConfigStore.LifecycleContext lifecycle = read.context();
                Optional<IndexedStreamConfigStore.StreamConfigData> current =
                    lifecycle.config();
                CompletableFuture<AllocationCleanup> disposition;
                if (current.isPresent() && sameNativeLifecycle(
                        current.orElseThrow(), claim)) {
                    IndexedStreamConfigStore.ProvisioningState state =
                        current.orElseThrow().provisioningState();
                    if (state == IndexedStreamConfigStore.ProvisioningState.PROVISIONING) {
                        return CompletableFuture.failedFuture(ownershipFailure);
                    }
                    if (state == IndexedStreamConfigStore.ProvisioningState.ACTIVE) {
                        return claimAndCleanupOrphanedAllocation(
                                id, partitionIndex, allocationKey, streamId,
                                Optional.of(claim.incarnationId()),
                                Optional.of(claim.ownerToken()), claim.ownerGeneration(),
                                AllocationCleanupProtection.active(current.orElseThrow()),
                                orphanedAllocationSweepVerifier(id, lifecycle))
                            .handle((ignored, cleanupFailure) -> {
                                if (cleanupFailure != null) {
                                    addSuppressed(ownershipFailure, cleanupFailure);
                                }
                                return null;
                            })
                            .thenCompose(ignored ->
                                CompletableFuture.failedFuture(ownershipFailure));
                    }
                    if (state != IndexedStreamConfigStore.ProvisioningState.ABORTING
                            && state != IndexedStreamConfigStore.ProvisioningState.DROPPED) {
                        return CompletableFuture.failedFuture(ownershipFailure);
                    }
                    IndexedStreamConfigStore.NativeCleanupContext cleanupContext =
                        new IndexedStreamConfigStore.NativeCleanupContext(
                            current, lifecycle.versionId());
                    disposition = claimRejectedNativeAllocation(
                        id, partitionIndex, allocationKey, streamId, cleanupContext,
                        new PartitionMetadataRetry());
                } else {
                    if (current.isPresent()
                            && current.orElseThrow().provisioningState()
                                == IndexedStreamConfigStore.ProvisioningState.PROVISIONING) {
                        return CompletableFuture.failedFuture(ownershipFailure);
                    }
                    AllocationCleanupProtection protection = current.isPresent()
                            && current.orElseThrow().provisioningState()
                                == IndexedStreamConfigStore.ProvisioningState.ACTIVE
                        ? AllocationCleanupProtection.active(current.orElseThrow())
                        : AllocationCleanupProtection.none();
                    return claimAndCleanupOrphanedAllocation(
                            id, partitionIndex, allocationKey, streamId,
                            Optional.of(claim.incarnationId()),
                            Optional.of(claim.ownerToken()), claim.ownerGeneration(), protection,
                            orphanedAllocationSweepVerifier(id, lifecycle))
                        .handle((ignored, cleanupFailure) -> {
                            if (cleanupFailure != null) {
                                addSuppressed(ownershipFailure, cleanupFailure);
                            }
                            return null;
                        })
                        .thenCompose(ignored ->
                            CompletableFuture.failedFuture(ownershipFailure));
                }
                Supplier<CompletableFuture<Void>> ownershipVerifier =
                    () -> streamConfigStore.verifyNativeCleanupContext(
                        id, new IndexedStreamConfigStore.NativeCleanupContext(
                            current, lifecycle.versionId()));
                return disposition
                    .thenCompose(cleanupDisposition -> cleanupClaimedAllocation(
                        id, partitionIndex, allocationKey, streamId,
                        cleanupDisposition, ownershipVerifier))
                    .handle((ignored, cleanupFailure) -> {
                        if (cleanupFailure != null) {
                            Throwable cause = rootCause(cleanupFailure);
                            if (cause != ownershipFailure) {
                                ownershipFailure.addSuppressed(cause);
                            }
                        }
                        return null;
                    })
                    .thenCompose(ignored ->
                        CompletableFuture.failedFuture(ownershipFailure));
            });
    }

    private static boolean sameNativeLifecycle(
            IndexedStreamConfigStore.StreamConfigData config,
            IndexedStreamConfigStore.ProvisioningClaim claim) {
        return config.creationKind().orElse(null)
                == IndexedStreamConfigStore.CreationKind.NATIVE_CREATE
            && config.incarnationId().equals(Optional.of(claim.incarnationId()));
    }

    private CompletableFuture<AllocationCleanup> claimRejectedNativeAllocation(
            StreamIdentifier id, int partitionIndex, String allocationKey, long streamId,
            IndexedStreamConfigStore.NativeCleanupContext context,
            PartitionMetadataRetry retryState) {
        IndexedStreamConfigStore.StreamConfigData config =
            context.config().orElseThrow();
        String path = catalogPaths.partitionMetadataPath(id, partitionIndex);
        Supplier<CompletableFuture<Void>> ownershipVerifier =
            () -> streamConfigStore.verifyNativeCleanupContext(id, context);
        return ownershipVerifier.get()
            .thenCompose(ignored -> oxiaClient.get(path))
            .thenCompose(existing -> {
                if (existing == null) {
                    Set<RetiredStreamMapping> retiredStreamMappings =
                        retiredStreamMappingsWith(Set.of(), allocationKey, streamId);
                    Set<String> retiredMappingKeys =
                        retiredMappingKeysWith(Set.of(), allocationKey);
                    LogMetadata desired = deletionMetadata(
                        streamId, config, Set.of(streamId), Set.of(streamId),
                        retiredStreamMappings, retiredMappingKeys);
                    return persistPartitionMetadata(
                        id, partitionIndex, desired,
                        Set.of(PutOption.IfRecordDoesNotExist), ownershipVerifier,
                        ignored -> AllocationCleanup.DELETE_LOG_AND_MAPPING,
                        () -> claimRejectedNativeAllocation(
                            id, partitionIndex, allocationKey, streamId, context,
                            retryState),
                        retryState);
                }

                final LogMetadata current;
                try {
                    current = LOG_METADATA_SERDE.deserialize(path, existing.value());
                } catch (Exception e) {
                    return CompletableFuture.failedFuture(e);
                }
                if (!metadataCanBeFencedByDeletion(current, config)) {
                    return CompletableFuture.completedFuture(AllocationCleanup.PRESERVE);
                }

                long primaryStreamId = current.streamId() >= 0
                    ? current.streamId() : streamId;
                Set<Long> retiredStreamIds = retiredStreamIdsWith(
                    current.retiredStreamIds(), primaryStreamId, streamId);
                Set<Long> purgeableRetiredStreamIds =
                    current.purgeableRetiredStreamIds();
                if (config.purgeRequested()) {
                    purgeableRetiredStreamIds = purgeAllRetiredStreamIds(
                        purgeableRetiredStreamIds, retiredStreamIds);
                } else if (!current.retiredStreamIds().contains(streamId)
                        && (current.streamId() < 0 || streamId != current.streamId())) {
                    purgeableRetiredStreamIds = retiredStreamIdsWith(
                        purgeableRetiredStreamIds, streamId);
                }
                Set<RetiredStreamMapping> retiredStreamMappings =
                    retiredStreamMappingsWith(
                        current.retiredStreamMappings(), allocationKey, streamId);
                Set<String> retiredMappingKeys = retiredMappingKeysWith(
                    current.retiredMappingKeys(), allocationKey);
                LogMetadata desired = deletionMetadata(
                    primaryStreamId, current, retiredStreamIds,
                    purgeableRetiredStreamIds, retiredStreamMappings,
                    retiredMappingKeys);
                AllocationCleanup cleanup =
                    purgeableRetiredStreamIds.contains(streamId)
                        ? AllocationCleanup.DELETE_LOG_AND_MAPPING
                        : AllocationCleanup.DELETE_MAPPING_ONLY;
                if (samePersistedRegistration(current, desired)) {
                    return ownershipVerifier.get().thenApply(ignored -> cleanup);
                }
                AllocationCleanup finalCleanup = cleanup;
                return persistPartitionMetadata(
                    id, partitionIndex, desired,
                    Set.of(PutOption.IfVersionIdEquals(existing.version().versionId())),
                    ownershipVerifier, ignored -> finalCleanup,
                    () -> claimRejectedNativeAllocation(
                        id, partitionIndex, allocationKey, streamId, context,
                        retryState),
                    retryState);
            });
    }

    private CompletableFuture<AllocationCleanup> claimOrphanedAllocation(
            StreamIdentifier id, int partitionIndex, String mappingKey, long streamId,
            Optional<String> fallbackIncarnation, Optional<String> fallbackOwner,
            long fallbackGeneration, AllocationCleanupProtection protection,
            PartitionMetadataRetry retryState) {
        String path = catalogPaths.partitionMetadataPath(id, partitionIndex);
        Supplier<CompletableFuture<Void>> noOwnershipFence =
            () -> CompletableFuture.completedFuture(null);
        return oxiaClient.get(path).thenCompose(existing -> {
            Set<RetiredStreamMapping> newMappings =
                retiredStreamMappingsWith(Set.of(), mappingKey, streamId);
            Set<String> newMappingKeys = retiredMappingKeysWith(Set.of(), mappingKey);
            if (existing == null) {
                if (protection.preserveMissingMetadata()) {
                    return CompletableFuture.completedFuture(AllocationCleanup.PRESERVE);
                }
                LogMetadata desired = new LogMetadata(
                    streamId, Map.of(), OptionalLong.empty(),
                    fallbackIncarnation.orElse(null), fallbackOwner.orElse(null),
                    fallbackGeneration >= 0 ? fallbackGeneration : null, true,
                    Set.of(streamId), Set.of(streamId), newMappings, newMappingKeys);
                return persistPartitionMetadata(
                    id, partitionIndex, desired,
                    Set.of(PutOption.IfRecordDoesNotExist), noOwnershipFence,
                    ignored -> AllocationCleanup.DELETE_LOG_AND_MAPPING,
                    () -> claimOrphanedAllocation(
                        id, partitionIndex, mappingKey, streamId, fallbackIncarnation,
                        fallbackOwner, fallbackGeneration,
                        protection, retryState), retryState);
            }

            final LogMetadata current;
            try {
                current = LOG_METADATA_SERDE.deserialize(path, existing.value());
            } catch (Exception e) {
                return CompletableFuture.failedFuture(e);
            }
            if (protection.protects(current)) {
                return CompletableFuture.completedFuture(AllocationCleanup.PRESERVE);
            }
            boolean ownedByRejectedAttempt = !current.deleted()
                && current.streamId() == streamId
                && metadataRegistrationMatches(
                    current, fallbackIncarnation, fallbackOwner, fallbackGeneration);
            if (!current.deleted() && current.streamId() == streamId
                    && (protection.activeConfig().isPresent()
                        || !ownedByRejectedAttempt)) {
                return CompletableFuture.completedFuture(AllocationCleanup.PRESERVE);
            }
            Set<Long> retiredStreamIds = retiredStreamIdsWith(
                current.retiredStreamIds(), streamId);
            Set<Long> purgeableRetiredStreamIds =
                current.purgeableRetiredStreamIds();
            if (!current.retiredStreamIds().contains(streamId)
                    && (ownedByRejectedAttempt || current.streamId() < 0
                        || current.streamId() != streamId)) {
                purgeableRetiredStreamIds = retiredStreamIdsWith(
                    purgeableRetiredStreamIds, streamId);
            }
            Set<RetiredStreamMapping> retiredStreamMappings =
                new TreeSet<>(current.retiredStreamMappings());
            retiredStreamMappings.addAll(newMappings);
            Set<String> retiredMappingKeys =
                new TreeSet<>(current.retiredMappingKeys());
            retiredMappingKeys.addAll(newMappingKeys);
            LogMetadata desired = ownedByRejectedAttempt
                ? new LogMetadata(
                    current.streamId(), current.properties(), current.terminatedOffset(),
                    current.registrationIncarnationId(),
                    current.registrationOwnerToken(),
                    current.registrationOwnerGeneration(), true, retiredStreamIds,
                    purgeableRetiredStreamIds, retiredStreamMappings,
                    retiredMappingKeys)
                : withRetiredCleanup(
                    current, retiredStreamIds, purgeableRetiredStreamIds,
                    retiredStreamMappings, retiredMappingKeys);
            AllocationCleanup disposition = purgeableRetiredStreamIds.contains(streamId)
                ? AllocationCleanup.DELETE_LOG_AND_MAPPING
                : AllocationCleanup.DELETE_MAPPING_ONLY;
            if (samePersistedRegistration(current, desired)) {
                return CompletableFuture.completedFuture(disposition);
            }
            return persistPartitionMetadata(
                id, partitionIndex, desired,
                Set.of(PutOption.IfVersionIdEquals(existing.version().versionId())),
                noOwnershipFence, ignored -> disposition,
                () -> claimOrphanedAllocation(
                    id, partitionIndex, mappingKey, streamId, fallbackIncarnation,
                    fallbackOwner, fallbackGeneration,
                    protection, retryState), retryState);
        });
    }

    private CompletableFuture<Void> claimAndCleanupOrphanedAllocation(
            StreamIdentifier id, int partitionIndex, String mappingKey, long streamId,
            Optional<String> fallbackIncarnation, Optional<String> fallbackOwner,
            long fallbackGeneration, AllocationCleanupProtection protection,
            Supplier<CompletableFuture<Void>> fixedPointVerifier) {
        return claimOrphanedAllocation(
                id, partitionIndex, mappingKey, streamId,
                fallbackIncarnation, fallbackOwner, fallbackGeneration,
                protection,
                new PartitionMetadataRetry())
            .thenCompose(disposition -> cleanupClaimedAllocation(
                id, partitionIndex, mappingKey, streamId, disposition,
                fixedPointVerifier));
    }

    private CompletableFuture<Void> cleanupClaimedAllocation(
            StreamIdentifier id, int partitionIndex, String mappingKey, long streamId,
            AllocationCleanup disposition,
            Supplier<CompletableFuture<Void>> fixedPointVerifier) {
        if (!disposition.deleteLog() && !disposition.deleteMapping()) {
            return CompletableFuture.completedFuture(null);
        }
        return sweepRetiredAllocations(
            id, partitionIndex, fixedPointVerifier).thenApply(value -> null);
    }

    private Supplier<CompletableFuture<Void>> orphanedAllocationSweepVerifier(
            StreamIdentifier id, IndexedStreamConfigStore.LifecycleContext lifecycle) {
        IndexedStreamConfigStore.NativeCleanupContext context =
            new IndexedStreamConfigStore.NativeCleanupContext(
                lifecycle.config(), lifecycle.versionId());
        return () -> streamConfigStore.verifyNativeCleanupContext(id, context);
    }

    private static String nativePartitionAllocationKey(
            StreamIdentifier id, IndexedStreamConfigStore.ProvisioningClaim claim,
            int partitionIndex) {
        return nativePartitionAllocationKey(id, partitionIndex);
    }

    private static String nativePartitionAllocationKey(
            StreamIdentifier id, int partitionIndex) {
        return "lakestream-native/" + id.fullName()
            + "/partition-" + partitionIndex;
    }

    @Override
    public CompletableFuture<Void> registerExternalStream(StreamIdentifier id, int partitionCount,
                                                          Map<String, String> properties) {
        Objects.requireNonNull(id, "id");
        Objects.requireNonNull(properties, "properties");
        if (partitionCount <= 0) {
            throw new IllegalArgumentException("partitionCount must be positive");
        }
        return preflightRetiredPartitionJournals(
                id, partitionCount, "External stream registration")
            .thenCompose(ignored ->
                streamConfigStore.registerExternalStream(id, partitionCount, properties));
    }

    @Override
    public CompletableFuture<Void> unregisterExternalStream(StreamIdentifier id) {
        return streamConfigStore.unregisterExternalStream(id);
    }

    @Override
    public CompletableFuture<Void> permanentlyDeleteExternalStream(StreamIdentifier id) {
        Objects.requireNonNull(id, "id");
        if (fencedMappingStorage != null) {
            return streamConfigStore.permanentlyDeleteExternalStream(id);
        }
        return streamConfigStore.readLifecycleContext(id).thenCompose(context -> {
            if (context.config().isPresent()) {
                IndexedStreamConfigStore.StreamConfigData config =
                    context.config().orElseThrow();
                if (config.provisioningState()
                        == IndexedStreamConfigStore.ProvisioningState
                            .PERMANENTLY_DELETED) {
                    return CompletableFuture.completedFuture(null);
                }
                return CompletableFuture.failedFuture(
                    destructiveKeyedLifecycleCapabilityFailure(
                        "External stream permanent deletion"));
            }
            String partitionPrefix = catalogPaths.partitionPrefix(id);
            return oxiaClient.list(partitionPrefix, partitionPrefix + "\uffff")
                .thenCompose(partitionKeys -> {
                    if (!partitionKeys.isEmpty()) {
                        return CompletableFuture.failedFuture(
                            destructiveKeyedLifecycleCapabilityFailure(
                                "External stream permanent deletion"));
                    }
                    return streamConfigStore.permanentlyDeleteAbsentExternalStream(id);
                });
        });
    }

    /**
     * Idempotently registers a single partition of a stream whose log was created
     * outside the catalog (for example, a topic created by the broker rather than through
     * {@link #createStream}). First grows the stream config so its partition count is at least
     * {@code partitionIndex + 1}, preserving any existing properties and materialization policy,
     * then writes the catalog partition metadata for {@code partitionIndex} with the supplied real
     * {@code streamId}. A deletion fence already present stops the operation before the partition
     * write. A deletion fence is checked again before the registration completes, so a raced
     * deletion cannot return a usable external partition.
     *
     * <p>This is what lets the materialization compaction worker resolve a broker-created stream via
     * {@link #loadStream}; without a registered stream config {@code loadStream} throws
     * {@link NoSuchStreamException}. Calls are concurrency-safe and idempotent within the same live
     * registration lifecycle: partition metadata is created or ownership-retagged with a
     * version-guarded compare-and-set, and the stream-config grow also retries on a concurrent
     * update. A partition tombstoned by {@link #deleteExternalPartition} cannot be registered again
     * in that lifecycle; the caller must first use {@link #unregisterExternalStream} so the next
     * registration advances the ownership generation. The supplied physical stream ID must be
     * globally unique and must never be reused after retirement.
     */
    @Override
    public CompletableFuture<Void> registerExternalPartition(StreamIdentifier id, int partitionIndex,
                                                             long streamId,
                                                             @Nullable Map<String, String> properties) {
        Map<String, String> props = properties == null ? Map.of() : properties;
        CompletableFuture<Void> registrationResult = preflightRetiredPartitionJournal(
                id, partitionIndex, "External partition registration")
            .thenCompose(ignored -> preflightLegacyDeletedExternalPartition(
                id, partitionIndex, props))
            .thenCompose(ignored -> streamConfigStore.beginExternalPartitionRegistration(
                id, partitionIndex + 1, props, UUID.randomUUID().toString(),
                fencedMappingStorage != null))
            .thenCompose(registration -> streamConfigStore
                .verifyExternalRegistration(id, registration)
                .handle((ignored, failure) ->
                    new ExternalWriteAttempt(null, unwrapNullable(failure)))
                .thenCompose(attempt -> attempt.failure() == null
                    ? prepareRetiredExternalPartition(
                        id, partitionIndex, registration, OptionalLong.of(streamId))
                        .thenCompose(prepared -> {
                            if (prepared.retiredStreamIds().contains(streamId)) {
                                return CompletableFuture.failedFuture(
                                    new PartitionLifecycleFencedException(
                                        id, partitionIndex,
                                        "supplied physical stream ID was already retired"));
                            }
                            return bindExternalStreamIdMapping(
                                    id, partitionIndex, streamId, registration,
                                    prepared.acknowledgedFence())
                            .thenCompose(ignored ->
                                writePartitionMetadataForExternalRegistration(
                                    id, partitionIndex, streamId, registration));
                        })
                        .handle((write, failure) ->
                            new ExternalWriteAttempt(write, unwrapNullable(failure)))
                    : CompletableFuture.completedFuture(attempt))
                .thenCompose(attempt -> attempt.failure() == null
                    ? completeExternalRegistration(id, registration)
                        .handle((ignored, failure) ->
                            new ExternalWriteAttempt(
                                attempt.write(), unwrapNullable(failure)))
                    : CompletableFuture.completedFuture(attempt))
                .thenCompose(attempt -> attempt.failure() == null
                    ? CompletableFuture.completedFuture(null)
                    : compensateRejectedRegistration(
                        id, partitionIndex, registration, attempt.write(), attempt.failure())));
        return registrationResult.handle((ignored, failure) -> unwrapNullable(failure))
            .thenCompose(failure -> {
                if (failure == null) {
                    return CompletableFuture.completedFuture(null);
                }
                Throwable cause = rootCause(failure);
                if (cause instanceof IndexedStreamConfigStore
                        .ExternalRegistrationLifecycleConflictException) {
                    return CompletableFuture.failedFuture(
                        new PartitionLifecycleFencedException(
                            id, partitionIndex,
                            "logical stream is owned by a different catalog lifecycle",
                            cause));
                }
                return CompletableFuture.failedFuture(cause);
            });
    }

    private CompletableFuture<Void> preflightLegacyDeletedExternalPartition(
            StreamIdentifier id, int partitionIndex, Map<String, String> properties) {
        if (fencedMappingStorage == null) {
            return CompletableFuture.completedFuture(null);
        }
        return streamConfigStore.readLifecycleContext(id).thenCompose(lifecycle -> {
            boolean absentOrActiveLegacy = lifecycle.config().isEmpty()
                || lifecycle.config().filter(config ->
                    config.provisioningState()
                        == IndexedStreamConfigStore.ProvisioningState.ACTIVE
                        && config.incarnationId().isEmpty()).isPresent();
            if (!absentOrActiveLegacy) {
                return CompletableFuture.completedFuture(null);
            }
            String path = catalogPaths.partitionMetadataPath(id, partitionIndex);
            return oxiaClient.get(path).thenCompose(existing -> {
                if (existing == null) {
                    return CompletableFuture.completedFuture(null);
                }
                final LogMetadata metadata;
                try {
                    metadata = LOG_METADATA_SERDE.deserialize(path, existing.value());
                } catch (Exception e) {
                    return CompletableFuture.failedFuture(e);
                }
                if (!metadata.deleted() || !streamIdMappingOwner(metadata).isLegacy()) {
                    return CompletableFuture.completedFuture(null);
                }
                CompletableFuture<Void> recoveryAnchor = lifecycle.config().isEmpty()
                    ? streamConfigStore.establishExternalRecoveryAnchor(
                        id, partitionIndex + 1, properties)
                    : CompletableFuture.completedFuture(null);
                return recoveryAnchor.handle((ignored, failure) -> unwrapNullable(failure))
                    .thenCompose(failure -> {
                        if (failure == null) {
                            return CompletableFuture.failedFuture(
                                new PartitionLifecycleFencedException(
                                    id, partitionIndex,
                                    "legacy partition deletion requires unregistering the "
                                        + "external stream before registration can advance to "
                                        + "a new lifecycle"));
                        }
                        Throwable cause = rootCause(failure);
                        if (cause instanceof IndexedStreamConfigStore
                                .ExternalRegistrationLifecycleConflictException) {
                            return CompletableFuture.failedFuture(
                                new PartitionLifecycleFencedException(
                                    id, partitionIndex,
                                    "logical stream changed while establishing a legacy "
                                        + "recovery anchor",
                                    cause));
                        }
                        return CompletableFuture.failedFuture(cause);
                    });
            });
        });
    }

    private CompletableFuture<Void> bindExternalStreamIdMapping(
            StreamIdentifier id, int partitionIndex, long streamId,
            IndexedStreamConfigStore.ExternalRegistration registration,
            Optional<StreamIdMappingFence> acknowledgedFence) {
        if (fencedMappingStorage == null) {
            return CompletableFuture.completedFuture(null);
        }
        return fencedMappingStorage.bindStreamIdMapping(
            catalogPaths.compactedReaderName(id, partitionIndex), streamId,
            streamIdMappingOwner(registration), acknowledgedFence);
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
        CompletableFuture<Log> openResult = preflightRetiredPartitionJournal(
                id, partitionIndex, "External partition creation")
            .thenCompose(ignored -> preflightLegacyDeletedExternalPartition(
                id, partitionIndex, properties))
            .thenCompose(ignored -> streamConfigStore.beginExternalPartitionRegistration(
                id, partitionIndex + 1, properties, UUID.randomUUID().toString(),
                fencedMappingStorage != null))
            .thenCompose(registration -> streamConfigStore
                .verifyExternalRegistration(id, registration)
                .handle((ignored, failure) -> new ExternalOpenAttempt(
                    null, null, unwrapNullable(failure)))
                .thenCompose(attempt -> attempt.failure() == null
                    ? allocateExternalReplacementStreamId(
                        id, partitionIndex, logName, registration)
                        .handle((allocation, failure) -> {
                            Throwable cause = unwrapNullable(failure);
                            StreamIdAllocation recoverableAllocation = allocation;
                            if (recoverableAllocation == null
                                    && cause instanceof KeyedAllocationInvalidatedException
                                        invalidated) {
                                recoverableAllocation = invalidated.allocation();
                            } else if (recoverableAllocation == null
                                    && cause instanceof RetiredStreamIdAllocationException
                                        retired) {
                                recoverableAllocation = retired.allocation();
                            }
                            return new ExternalOpenAttempt(
                                recoverableAllocation, null, cause);
                        })
                    : CompletableFuture.completedFuture(attempt))
                .thenCompose(attempt -> attempt.failure() == null
                    ? writePartitionMetadataForExternalRegistration(
                        id, partitionIndex, attempt.allocation().streamId(), registration)
                        .handle((write, failure) -> new ExternalOpenAttempt(
                            attempt.allocation(), write, unwrapNullable(failure)))
                    : CompletableFuture.completedFuture(attempt))
                .thenCompose(attempt -> attempt.failure() == null
                    ? completeExternalRegistration(id, registration)
                        .handle((ignored, failure) -> new ExternalOpenAttempt(
                            attempt.allocation(), attempt.write(), unwrapNullable(failure)))
                    : CompletableFuture.completedFuture(attempt))
                .thenCompose(attempt -> {
                    if (attempt.failure() != null) {
                        return compensateRejectedOpen(
                            id, partitionIndex, registration, attempt);
                    }
                    return CompletableFuture.completedFuture(createLog(
                        logName, LogId.of(attempt.allocation().streamId())));
                }));
        return openResult.exceptionallyCompose(failure -> {
            Throwable cause = rootCause(failure);
            if (cause instanceof IndexedStreamConfigStore
                    .ExternalRegistrationLifecycleConflictException) {
                return CompletableFuture.failedFuture(
                    new PartitionLifecycleFencedException(
                        id, partitionIndex,
                        "logical stream is owned by a different catalog lifecycle",
                        cause));
            }
            return CompletableFuture.failedFuture(cause);
        });
    }

    private CompletableFuture<Long> allocateNativeKeyedStreamId(
            String logName, StreamIdMappingOwner owner,
            Optional<StreamIdMappingFence> acknowledgedFence) {
        if (fencedMappingStorage != null) {
            return fencedMappingStorage.allocateStreamId(
                    logName, owner, acknowledgedFence)
                .thenApply(StreamIdAllocation::streamId);
        }
        return streamIdGenerator.apply(Optional.of(logName));
    }

    private CompletableFuture<StreamIdAllocation> allocateExternalKeyedStreamId(
            String logName, StreamIdMappingOwner owner,
            Optional<StreamIdMappingFence> acknowledgedFence) {
        if (fencedMappingStorage != null) {
            return fencedMappingStorage.allocateStreamId(
                logName, owner, acknowledgedFence);
        }
        return keyedStreamIdAllocator != null
            ? keyedStreamIdAllocator.apply(logName)
            : streamIdGenerator.apply(Optional.of(logName))
                .thenApply(streamId -> new StreamIdAllocation(streamId, false));
    }

    private CompletableFuture<StreamIdAllocation> allocateExternalReplacementStreamId(
            StreamIdentifier id, int partitionIndex, String logName,
            IndexedStreamConfigStore.ExternalRegistration registration) {
        return prepareRetiredExternalPartition(
                id, partitionIndex, registration, OptionalLong.empty())
            .thenCompose(prepared -> allocateExternalKeyedStreamId(
                    logName, streamIdMappingOwner(registration),
                    prepared.acknowledgedFence())
                .thenApply(allocation -> {
                    if (prepared.retiredStreamIds().contains(allocation.streamId())) {
                        throw new RetiredStreamIdAllocationException(
                            allocation,
                            "Deleted external partition " + id.fullName() + "-partition-"
                                + partitionIndex + " must use a fresh physical stream ID");
                    }
                    // The write path verifies ownership before its metadata CAS. Keeping the
                    // allocated ID in the open attempt lets a raced deletion compensate the
                    // keyed allocation instead of leaking it when that verification fails.
                    return allocation;
                }));
    }

    private CompletableFuture<PreparedExternalPartition> prepareRetiredExternalPartition(
            StreamIdentifier id, int partitionIndex,
            IndexedStreamConfigStore.ExternalRegistration registration,
            OptionalLong protectedStreamId) {
        String metadataPath = catalogPaths.partitionMetadataPath(id, partitionIndex);
        return streamConfigStore.verifyExternalRegistration(id, registration)
            .thenCompose(ignored -> oxiaClient.get(metadataPath))
            .thenCompose(existing -> {
                if (existing == null) {
                    return CompletableFuture.completedFuture(
                        new PreparedExternalPartition(Set.of(), Optional.empty()));
                }
                final LogMetadata metadata;
                try {
                    metadata = LOG_METADATA_SERDE.deserialize(
                        metadataPath, existing.value());
                } catch (Exception e) {
                    return CompletableFuture.failedFuture(e);
                }
                if (metadata.deleted()
                        && !deletedMetadataCanBeReplaced(metadata, registration)) {
                    return CompletableFuture.failedFuture(
                        new PartitionLifecycleFencedException(
                            id, partitionIndex,
                            "deleted metadata is not replaceable by the current external "
                                + "registration; unregister the stream before recreating it"));
                }
                Set<Long> retiredStreamIds = metadata.deleted()
                    ? retiredStreamIdsWith(
                        metadata.retiredStreamIds(), metadata.streamId())
                    : metadata.retiredStreamIds();
                if (!hasRetiredJournal(metadata)) {
                    return CompletableFuture.completedFuture(
                        new PreparedExternalPartition(
                            retiredStreamIds, acknowledgedFence(metadata)));
                }
                return sweepRetiredAllocations(
                        id, partitionIndex,
                        () -> streamConfigStore.verifyExternalRegistration(id, registration),
                        ActiveMappingProtection.of(
                            streamIdMappingOwner(registration), protectedStreamId))
                    .thenApply(cleaned -> new PreparedExternalPartition(
                        retiredStreamIds, acknowledgedFence(cleaned)));
            });
    }

    private static boolean deletedMetadataCanBeReplaced(
            LogMetadata metadata,
            IndexedStreamConfigStore.ExternalRegistration registration) {
        if (!metadata.deleted() || !validRegistrationIdentity(metadata)
                || registration.incarnationId().isEmpty()
                || registration.ownerGeneration() < 0) {
            return false;
        }
        long metadataGeneration = metadata.registrationOwnerGeneration() == null
            ? IndexedStreamConfigStore.LEGACY_METADATA_GENERATION
            : metadata.registrationOwnerGeneration();
        if (metadataGeneration >= registration.ownerGeneration()) {
            return false;
        }
        if (metadata.registrationIncarnationId() == null) {
            // A fresh modern owner has no predecessor and cannot bypass a legacy deletion
            // tombstone. An explicit unregister/resume records its predecessor generation; that
            // lifecycle transition authorizes replacement while the exact mapping fence still
            // prevents reuse of the retired physical allocation.
            return registration.ownerGeneration() > 0
                && registration.metadataSourceGeneration()
                    != IndexedStreamConfigStore.NO_METADATA_GENERATION;
        }
        if (Objects.equals(metadata.registrationIncarnationId(),
                registration.incarnationId().orElseThrow())) {
            return registration.metadataSourceGeneration() == metadataGeneration
                && Objects.equals(metadata.registrationOwnerToken(),
                    registration.metadataSourceOwnerToken().orElse(null));
        }
        return true;
    }

    private CompletableFuture<Void> completeExternalRegistration(
            StreamIdentifier id,
            IndexedStreamConfigStore.ExternalRegistration registration) {
        if (registration.claim().isEmpty()) {
            return streamConfigStore.verifyExternalRegistration(id, registration);
        }
        IndexedStreamConfigStore.ProvisioningClaim claim = registration.claim().orElseThrow();
        return streamConfigStore.finalizeCreation(id, claim).thenCompose(outcome -> {
            if (outcome.active()) {
                return streamConfigStore.verifyFinalizedExternalRegistration(id, claim);
            }
            return CompletableFuture.failedFuture(
                new ExternalFinalizationUnknownException(outcome.failure()));
        });
    }

    private CompletableFuture<Void> compensateRejectedRegistration(
            StreamIdentifier id, int partitionIndex,
            IndexedStreamConfigStore.ExternalRegistration registration,
            @Nullable PartitionMetadataWrite write, Throwable registrationFailure) {
        // A failed or fenced owner leaves its provisioning record and any idempotent partition
        // metadata as a recovery anchor. A compatible retry takes over the claim and reuses those
        // resources. Destructive rollback would race a new owner that can already observe them.
        Throwable cause = rootCause(registrationFailure);
        if (cause instanceof StreamIdMappingConflictException) {
            return CompletableFuture.failedFuture(
                new PartitionLifecycleFencedException(
                    id, partitionIndex,
                    "keyed stream-ID mapping belongs to a different durable owner or ID",
                    cause));
        }
        if (cause instanceof AlreadyExistsException) {
            return CompletableFuture.failedFuture(
                new PartitionLifecycleFencedException(
                    id, partitionIndex,
                    "partition metadata belongs to a different durable registration or ID",
                    cause));
        }
        return CompletableFuture.failedFuture(cause);
    }

    private <T> CompletableFuture<T> compensateRejectedOpen(
            StreamIdentifier id, int partitionIndex,
            IndexedStreamConfigStore.ExternalRegistration registration,
            ExternalOpenAttempt attempt) {
        Throwable failure = rootCause(attempt.failure());
        if (failure instanceof RetiredStreamIdAllocationException retired) {
            PartitionLifecycleFencedException terminalFailure =
                new PartitionLifecycleFencedException(
                    id, partitionIndex,
                    "allocator returned a retired physical stream ID", failure);
            String logName = catalogPaths.compactedReaderName(id, partitionIndex);
            return claimAndCleanupOrphanedAllocation(
                    id, partitionIndex, logName, retired.allocation().streamId(),
                    registration.incarnationId(), registration.ownerToken(),
                    registration.ownerGeneration(),
                    AllocationCleanupProtection.missingMetadata(),
                    () -> streamConfigStore.verifyExternalRegistration(id, registration))
                .handle((ignored, cleanupFailure) -> {
                    if (cleanupFailure != null) {
                        addSuppressed(terminalFailure, cleanupFailure);
                    }
                    return null;
                })
                .thenCompose(ignored -> CompletableFuture.failedFuture(terminalFailure));
        }
        Throwable terminalFailure = externalPartitionLifecycleFailure(
            id, partitionIndex, failure);
        if (attempt.allocation() == null || fencedMappingStorage == null) {
            return CompletableFuture.failedFuture(terminalFailure);
        }
        String mappingKey = catalogPaths.compactedReaderName(id, partitionIndex);
        return readLifecycleContextForCleanup(id, failure)
            .thenCompose(read -> {
                if (read.context() == null) {
                    return CompletableFuture.failedFuture(terminalFailure);
                }
                IndexedStreamConfigStore.LifecycleContext lifecycle = read.context();
                Optional<IndexedStreamConfigStore.StreamConfigData> current =
                    lifecycle.config();
                CompletableFuture<Void> cleanup;
                if (current.isPresent() && sameExternalLifecycle(
                        current.orElseThrow(), registration)) {
                    IndexedStreamConfigStore.ProvisioningState state =
                        current.orElseThrow().provisioningState();
                    if (state == IndexedStreamConfigStore.ProvisioningState.PROVISIONING) {
                        return CompletableFuture.failedFuture(terminalFailure);
                    }
                    if (state == IndexedStreamConfigStore.ProvisioningState.ACTIVE) {
                        cleanup = claimAndCleanupOrphanedAllocation(
                            id, partitionIndex, mappingKey,
                            attempt.allocation().streamId(), registration.incarnationId(),
                            registration.ownerToken(), registration.ownerGeneration(),
                            AllocationCleanupProtection.active(current.orElseThrow()),
                            orphanedAllocationSweepVerifier(id, lifecycle));
                    } else if (state == IndexedStreamConfigStore.ProvisioningState.ABORTING
                            || state == IndexedStreamConfigStore.ProvisioningState.DROPPED
                            || state == IndexedStreamConfigStore.ProvisioningState.UNREGISTERED
                            || state == IndexedStreamConfigStore.ProvisioningState
                                .PERMANENTLY_DELETED) {
                        cleanup = cleanupRejectedOpenAllocation(
                            id, partitionIndex, attempt.allocation().streamId(),
                            new IndexedStreamConfigStore.ExternalDeletionContext(
                                current.orElseThrow(), lifecycle.versionId()));
                    } else {
                        return CompletableFuture.failedFuture(terminalFailure);
                    }
                } else {
                    if (current.isPresent()
                            && current.orElseThrow().provisioningState()
                                == IndexedStreamConfigStore.ProvisioningState.PROVISIONING) {
                        return CompletableFuture.failedFuture(terminalFailure);
                    }
                    AllocationCleanupProtection protection = current.isPresent()
                            && current.orElseThrow().provisioningState()
                                == IndexedStreamConfigStore.ProvisioningState.ACTIVE
                        ? AllocationCleanupProtection.active(current.orElseThrow())
                        : AllocationCleanupProtection.none();
                    cleanup = claimAndCleanupOrphanedAllocation(
                        id, partitionIndex, mappingKey,
                        attempt.allocation().streamId(), registration.incarnationId(),
                        registration.ownerToken(), registration.ownerGeneration(), protection,
                        orphanedAllocationSweepVerifier(id, lifecycle));
                }
                return cleanup.handle((ignored, cleanupFailure) -> {
                    if (cleanupFailure != null) {
                        Throwable cause = rootCause(cleanupFailure);
                        if (cause != failure) {
                            terminalFailure.addSuppressed(cause);
                        }
                    }
                    return null;
                })
                    .thenCompose(ignored -> CompletableFuture.failedFuture(terminalFailure));
            });
    }

    private static Throwable externalPartitionLifecycleFailure(
            StreamIdentifier id, int partitionIndex, Throwable failure) {
        if (failure instanceof PartitionLifecycleFencedException) {
            return failure;
        }
        if (failure instanceof StreamIdMappingConflictException) {
            return new PartitionLifecycleFencedException(
                id, partitionIndex,
                "keyed stream-ID mapping belongs to a different durable owner or ID",
                failure);
        }
        if (failure instanceof AlreadyExistsException) {
            return new PartitionLifecycleFencedException(
                id, partitionIndex,
                "partition metadata belongs to a different durable registration or ID",
                failure);
        }
        return failure;
    }

    private static boolean sameExternalLifecycle(
            IndexedStreamConfigStore.StreamConfigData config,
            IndexedStreamConfigStore.ExternalRegistration registration) {
        return config.creationKind().orElse(IndexedStreamConfigStore.CreationKind.EXTERNAL)
                == IndexedStreamConfigStore.CreationKind.EXTERNAL
            && config.incarnationId().equals(registration.incarnationId());
    }

    private CompletableFuture<Void> cleanupRejectedOpenAllocation(
            StreamIdentifier id, int partitionIndex, long streamId,
            IndexedStreamConfigStore.ExternalDeletionContext context) {
        IndexedStreamConfigStore.ProvisioningState state =
            context.config().provisioningState();
        if (state != IndexedStreamConfigStore.ProvisioningState.ABORTING
                && state != IndexedStreamConfigStore.ProvisioningState.DROPPED
                && state != IndexedStreamConfigStore.ProvisioningState.PERMANENTLY_DELETED) {
            return CompletableFuture.completedFuture(null);
        }
        String logName = catalogPaths.compactedReaderName(id, partitionIndex);
        if (state == IndexedStreamConfigStore.ProvisioningState.PERMANENTLY_DELETED) {
            return deleteExternalPartitionWithResolvedMapping(
                id, partitionIndex, logName, context, OptionalLong.of(streamId), true,
                MAX_EXTERNAL_DELETION_CONTEXT_RETRIES);
        }
        return cleanupRejectedOpenAllocation(
            id, partitionIndex, logName, streamId, context,
            MAX_EXTERNAL_DELETION_CONTEXT_RETRIES);
    }

    private CompletableFuture<Void> cleanupRejectedOpenAllocation(
            StreamIdentifier id, int partitionIndex, String logName, long streamId,
            IndexedStreamConfigStore.ExternalDeletionContext context,
            int remainingContextRetries) {
        return claimUnpublishedExternalAllocation(
                id, partitionIndex, streamId, context, new PartitionMetadataRetry())
            .thenCompose(disposition -> cleanupClaimedAllocation(
                id, partitionIndex, logName, streamId, disposition,
                () -> streamConfigStore.verifyExternalDeletionContext(id, context)))
            .handle((ignored, failure) -> unwrapNullable(failure))
            .thenCompose(failure -> {
                if (failure == null) {
                    return CompletableFuture.completedFuture(null);
                }
                Throwable cause = rootCause(failure);
                if (!(cause instanceof IndexedStreamConfigStore
                        .ExternalDeletionContextInvalidatedException)
                        || remainingContextRetries == 0) {
                    return CompletableFuture.failedFuture(cause);
                }
                return streamConfigStore.readExternalDeletionContext(id)
                    .thenCompose(successor -> {
                        if (!context.canRetryWith(successor)) {
                            return CompletableFuture.failedFuture(cause);
                        }
                        if (successor.config().provisioningState()
                                == IndexedStreamConfigStore.ProvisioningState
                                    .PERMANENTLY_DELETED) {
                            return deleteExternalPartitionWithResolvedMapping(
                                id, partitionIndex, logName, successor,
                                OptionalLong.of(streamId), true,
                                remainingContextRetries - 1);
                        }
                        return cleanupRejectedOpenAllocation(
                            id, partitionIndex, logName, streamId, successor,
                            remainingContextRetries - 1);
                    });
            });
    }

    private CompletableFuture<AllocationCleanup> claimUnpublishedExternalAllocation(
            StreamIdentifier id, int partitionIndex, long streamId,
            IndexedStreamConfigStore.ExternalDeletionContext context,
            PartitionMetadataRetry retryState) {
        String path = catalogPaths.partitionMetadataPath(id, partitionIndex);
        String mappingKey = catalogPaths.compactedReaderName(id, partitionIndex);
        Supplier<CompletableFuture<Void>> ownershipVerifier =
            () -> streamConfigStore.verifyExternalDeletionContext(id, context);
        return ownershipVerifier.get()
            .thenCompose(ignored -> oxiaClient.get(path))
            .thenCompose(existing -> {
                if (existing == null) {
                    Set<RetiredStreamMapping> retiredStreamMappings =
                        retiredStreamMappingsForTombstone(
                            Set.of(), mappingKey, true, streamId);
                    Set<String> retiredMappingKeys =
                        retiredMappingKeysWith(Set.of(), mappingKey);
                    LogMetadata desired = externalDeletionMetadata(
                        -1L, context, Set.of(streamId), Set.of(streamId),
                        retiredStreamMappings, retiredMappingKeys);
                    return persistPartitionMetadata(
                        id, partitionIndex, desired,
                        Set.of(PutOption.IfRecordDoesNotExist), ownershipVerifier,
                        ignored -> AllocationCleanup.DELETE_LOG_AND_MAPPING,
                        () -> claimUnpublishedExternalAllocation(
                            id, partitionIndex, streamId, context, retryState),
                        retryState);
                }
                final LogMetadata current;
                try {
                    current = LOG_METADATA_SERDE.deserialize(path, existing.value());
                } catch (Exception e) {
                    return CompletableFuture.failedFuture(e);
                }
                if (current.streamId() == streamId && !current.deleted()) {
                    return CompletableFuture.completedFuture(
                        AllocationCleanup.PRESERVE);
                }
                if (!metadataCanBeFencedByDeletion(current, context.config())) {
                    return CompletableFuture.completedFuture(
                        AllocationCleanup.PRESERVE);
                }
                Set<Long> retiredStreamIds = retiredStreamIdsWith(
                    current.retiredStreamIds(), current.streamId(), streamId);
                Set<Long> purgeableRetiredStreamIds =
                    current.purgeableRetiredStreamIds();
                if (context.config().purgeRequested()) {
                    purgeableRetiredStreamIds = purgeAllRetiredStreamIds(
                        purgeableRetiredStreamIds, retiredStreamIds);
                } else if (!current.retiredStreamIds().contains(streamId)
                        && (current.streamId() < 0 || current.streamId() != streamId)) {
                    purgeableRetiredStreamIds = retiredStreamIdsWith(
                        purgeableRetiredStreamIds, streamId);
                }
                Set<RetiredStreamMapping> retiredStreamMappings =
                    retiredStreamMappingsWith(
                        current.retiredStreamMappings(), mappingKey, streamId);
                Set<String> retiredMappingKeys = retiredMappingKeysWith(
                    current.retiredMappingKeys(), mappingKey);
                LogMetadata desired = externalDeletionMetadata(
                    current.streamId(), current, retiredStreamIds,
                    purgeableRetiredStreamIds, retiredStreamMappings,
                    retiredMappingKeys);
                AllocationCleanup disposition = purgeableRetiredStreamIds.contains(streamId)
                    ? AllocationCleanup.DELETE_LOG_AND_MAPPING
                    : AllocationCleanup.DELETE_MAPPING_ONLY;
                if (samePersistedRegistration(current, desired)) {
                    return ownershipVerifier.get().thenApply(ignored -> disposition);
                }
                return persistPartitionMetadata(
                    id, partitionIndex, desired,
                    Set.of(PutOption.IfVersionIdEquals(existing.version().versionId())),
                    ownershipVerifier,
                    ignored -> disposition,
                    () -> claimUnpublishedExternalAllocation(
                        id, partitionIndex, streamId, context, retryState),
                    retryState);
            });
    }

    private CompletableFuture<LogMetadata> sweepRetiredAllocations(
            StreamIdentifier id, int partitionIndex,
            Supplier<CompletableFuture<Void>> ownershipVerifier) {
        return sweepRetiredAllocations(
            id, partitionIndex, ownershipVerifier, ActiveMappingProtection.none());
    }

    private CompletableFuture<LogMetadata> sweepRetiredAllocations(
            StreamIdentifier id, int partitionIndex,
            Supplier<CompletableFuture<Void>> ownershipVerifier,
            ActiveMappingProtection activeMappingProtection) {
        if (fencedMappingStorage == null) {
            return CompletableFuture.failedFuture(new UnsupportedOperationException(
                "Retired allocation cleanup requires durable fenced stream-ID lifecycle support"));
        }
        return sweepFencedRetiredAllocations(
            id, partitionIndex, ownershipVerifier, activeMappingProtection,
            MAX_PARTITION_METADATA_WRITE_RETRIES);
    }

    private CompletableFuture<LogMetadata> sweepFencedRetiredAllocations(
            StreamIdentifier id, int partitionIndex,
            Supplier<CompletableFuture<Void>> ownershipVerifier,
            ActiveMappingProtection activeMappingProtection,
            int remainingAttempts) {
        return readPersistedLogMetadata(id, partitionIndex, ownershipVerifier)
            .thenCompose(persisted -> planFencedRetiredCleanup(
                    id, partitionIndex, persisted.metadata(), ownershipVerifier,
                    activeMappingProtection)
                .thenCompose(plan -> persistCapturedRetiredMappings(
                        id, partitionIndex, persisted, plan.metadata(), ownershipVerifier)
                    .thenCompose(durable -> executeFencedRetiredCleanup(
                            id, partitionIndex, durable.metadata(), plan.outcomes(),
                            ownershipVerifier)
                        .thenCompose(ignored -> acknowledgeRetiredCleanup(
                            id, partitionIndex, durable, ownershipVerifier)))))
            .handle((metadata, failure) -> new RetiredCleanupAttempt(
                metadata, unwrapNullable(failure)))
            .thenCompose(attempt -> {
                if (attempt.failure() == null) {
                    return CompletableFuture.completedFuture(attempt.metadata());
                }
                Throwable cause = rootCause(attempt.failure());
                if (!(cause instanceof RetiredCleanupRetryException)
                        || remainingAttempts == 0) {
                    return CompletableFuture.failedFuture(cause);
                }
                return CompletableFuture.runAsync(
                        () -> { }, CompletableFuture.delayedExecutor(
                            PARTITION_METADATA_RETRY_DELAY_MILLIS,
                            TimeUnit.MILLISECONDS))
                    .thenCompose(ignored -> sweepFencedRetiredAllocations(
                        id, partitionIndex, ownershipVerifier,
                        activeMappingProtection,
                        remainingAttempts - 1));
            });
    }

    private CompletableFuture<FencedCleanupPlan> planFencedRetiredCleanup(
            StreamIdentifier id, int partitionIndex, LogMetadata metadata,
            Supplier<CompletableFuture<Void>> ownershipVerifier,
            ActiveMappingProtection activeMappingProtection) {
        if (!metadata.deleted() && metadata.streamId() >= 0
                && metadata.retiredStreamIds().contains(metadata.streamId())) {
            return CompletableFuture.failedFuture(
                new PartitionLifecycleFencedException(
                    id, partitionIndex,
                    "retired cleanup journal contains the active physical stream ID "
                        + metadata.streamId()));
        }
        StreamIdMappingOwner expectedOwner = streamIdMappingOwner(metadata);
        List<FencedMappingOutcome> outcomes = new ArrayList<>();
        Set<String> fencedKeys = new TreeSet<>();
        CompletableFuture<Void> fences = CompletableFuture.completedFuture(null);
        Map<String, List<RetiredStreamMapping>> mappingsByKey = new TreeMap<>();
        for (RetiredStreamMapping persistedMapping : metadata.retiredStreamMappings()) {
            mappingsByKey.computeIfAbsent(
                persistedMapping.mappingKey(), ignored -> new ArrayList<>())
                .add(persistedMapping);
        }
        boolean unknownLegacyMapping = expectedOwner.isLegacy()
            && (mappingsByKey.values().stream().anyMatch(mappings ->
                    mappings.size() != 1 || mappings.get(0).streamId() == -1L)
                || metadata.retiredMappingKeys().stream()
                    .anyMatch(mappingKey -> !mappingsByKey.containsKey(mappingKey)));
        if (unknownLegacyMapping) {
            return CompletableFuture.failedFuture(
                new PartitionLifecycleFencedException(
                    id, partitionIndex,
                    "legacy keyed mapping cannot be cleaned without an exact physical "
                        + "stream ID"));
        }
        for (Map.Entry<String, List<RetiredStreamMapping>> entry
                : mappingsByKey.entrySet()) {
            fencedKeys.add(entry.getKey());
            List<RetiredStreamMapping> persistedMappings = entry.getValue();
            // The current journal writes one mapping observation per key. Historical journals
            // may contain several exact IDs for one key; treat that as unknown and atomically
            // capture whichever same-owner mapping is actually present.
            long expectedStreamId = persistedMappings.size() == 1
                ? persistedMappings.get(0).streamId() : -1L;
            boolean purge = persistedMappings.stream().anyMatch(mapping ->
                mapping.purge() || mapping.streamId() >= 0
                    && metadata.purgeableRetiredStreamIds().contains(mapping.streamId()));
            RetiredStreamMapping mapping = new RetiredStreamMapping(
                expectedStreamId, entry.getKey(), purge);
            fences = fences.thenCompose(ignored -> fenceRetiredMapping(
                    id, partitionIndex, metadata, mapping, expectedOwner,
                    ownershipVerifier, activeMappingProtection)
                .thenAccept(outcomes::add));
        }
        for (String mappingKey : metadata.retiredMappingKeys()) {
            if (!fencedKeys.add(mappingKey)) {
                continue;
            }
            boolean purge = metadata.streamId() >= 0
                && metadata.purgeableRetiredStreamIds().contains(metadata.streamId());
            RetiredStreamMapping mapping = new RetiredStreamMapping(
                -1L, mappingKey, purge);
            fences = fences.thenCompose(ignored -> fenceRetiredMapping(
                    id, partitionIndex, metadata, mapping, expectedOwner,
                    ownershipVerifier, activeMappingProtection)
                .thenAccept(outcomes::add));
        }
        return fences.thenApply(ignored -> new FencedCleanupPlan(
            expandCapturedRetiredMappings(metadata, outcomes), List.copyOf(outcomes)));
    }

    private CompletableFuture<FencedMappingOutcome> fenceRetiredMapping(
            StreamIdentifier id, int partitionIndex, LogMetadata metadata,
            RetiredStreamMapping mapping, StreamIdMappingOwner expectedOwner,
            Supplier<CompletableFuture<Void>> ownershipVerifier,
            ActiveMappingProtection activeMappingProtection) {
        StorageApi storageApi = Objects.requireNonNull(
            fencedMappingStorage, "fencedMappingStorage");
        return ownershipVerifier.get()
            .thenCompose(ignored -> storageApi.fenceStreamIdMappingState(
                mapping.mappingKey(), mapping.streamId(), expectedOwner))
            .thenCompose(result -> {
                if (result instanceof StreamIdMappingFenceResult.Fenced fenced) {
                    StreamIdMappingFence observed = fenced.fence();
                    boolean expectedFence = observed.owner().equals(expectedOwner)
                        && (mapping.streamId() == -1
                            || observed.streamId() == -1
                            || observed.streamId() == mapping.streamId()
                            || metadata.deleted()
                                && observed.streamId() == metadata.streamId());
                    if (!expectedFence) {
                        return CompletableFuture.failedFuture(
                            retiredFenceConflict(id, partitionIndex, mapping.mappingKey()));
                    }
                    return ownershipVerifier.get().thenApply(ignored ->
                        FencedMappingOutcome.fenced(mapping, observed));
                }
                StreamIdMappingFenceResult.PreservedActive preserved =
                    (StreamIdMappingFenceResult.PreservedActive) result;
                ActiveStreamIdMapping active = preserved.mapping();
                boolean currentActiveMapping = (!metadata.deleted()
                    && active.streamId() == metadata.streamId()
                    && active.owner().equals(expectedOwner))
                    || activeMappingProtection.protects(active);
                if (!currentActiveMapping) {
                    return CompletableFuture.failedFuture(
                        retiredFenceConflict(id, partitionIndex, mapping.mappingKey()));
                }
                if (metadata.retiredStreamIds().contains(active.streamId())) {
                    return CompletableFuture.failedFuture(
                        new PartitionLifecycleFencedException(
                            id, partitionIndex,
                            "retired cleanup journal contains durable active stream ID "
                                + active.streamId()));
                }
                return ownershipVerifier.get().thenApply(ignored ->
                    FencedMappingOutcome.preserved(mapping, active));
            });
    }

    private static RetiredCleanupRetryException retiredFenceConflict(
            StreamIdentifier id, int partitionIndex, String mappingKey) {
        return new RetiredCleanupRetryException(
            "Keyed mapping has a different durable owner while cleaning "
                + id.fullName() + "-partition-" + partitionIndex + ": " + mappingKey);
    }

    private static LogMetadata expandCapturedRetiredMappings(
            LogMetadata metadata, List<FencedMappingOutcome> outcomes) {
        long primaryStreamId = metadata.streamId();
        TreeSet<Long> retiredStreamIds = new TreeSet<>(metadata.retiredStreamIds());
        TreeSet<Long> purgeableRetiredStreamIds =
            new TreeSet<>(metadata.purgeableRetiredStreamIds());
        TreeSet<RetiredStreamMapping> retiredMappings =
            new TreeSet<>(metadata.retiredStreamMappings());
        for (FencedMappingOutcome outcome : outcomes) {
            if (outcome.fence() == null) {
                continue;
            }
            retiredMappings.removeIf(mapping ->
                mapping.mappingKey().equals(outcome.mapping().mappingKey()));
            long actualStreamId = outcome.fence().streamId();
            retiredMappings.add(new RetiredStreamMapping(
                actualStreamId, outcome.mapping().mappingKey(), outcome.mapping().purge()));
            if (actualStreamId >= 0) {
                retiredStreamIds.add(actualStreamId);
                if (outcome.mapping().purge()) {
                    purgeableRetiredStreamIds.add(actualStreamId);
                }
                if (metadata.deleted() && primaryStreamId < 0) {
                    primaryStreamId = actualStreamId;
                }
            }
        }
        return new LogMetadata(
            primaryStreamId, metadata.properties(), metadata.terminatedOffset(),
            metadata.registrationIncarnationId(), metadata.registrationOwnerToken(),
            metadata.registrationOwnerGeneration(), metadata.deleted(), retiredStreamIds,
            purgeableRetiredStreamIds, retiredMappings, metadata.retiredMappingKeys());
    }

    private CompletableFuture<PersistedLogMetadata> persistCapturedRetiredMappings(
            StreamIdentifier id, int partitionIndex, PersistedLogMetadata persisted,
            LogMetadata expanded, Supplier<CompletableFuture<Void>> ownershipVerifier) {
        if (samePersistedRegistration(persisted.metadata(), expanded)) {
            return CompletableFuture.completedFuture(persisted);
        }
        String path = catalogPaths.partitionMetadataPath(id, partitionIndex);
        final byte[] bytes;
        try {
            bytes = LOG_METADATA_SERDE.serialize(path, expanded);
        } catch (Exception e) {
            return CompletableFuture.failedFuture(e);
        }
        return ownershipVerifier.get()
            .thenCompose(ignored -> oxiaClient.put(path, bytes, Set.of(
                PutOption.IfVersionIdEquals(persisted.versionId()))))
            .handle((write, failure) -> new PartitionWriteOutcome(
                write, unwrapNullable(failure)))
            .thenCompose(outcome -> {
                if (outcome.failure() == null) {
                    return ownershipVerifier.get().thenApply(ignored ->
                        new PersistedLogMetadata(
                            expanded, outcome.result().version().versionId()));
                }
                if (outcome.failure() instanceof UnexpectedVersionIdException
                        || outcome.failure() instanceof KeyAlreadyExistsException) {
                    return CompletableFuture.failedFuture(
                        new RetiredCleanupRetryException(
                            "Partition cleanup journal changed while recording a captured ID",
                            outcome.failure()));
                }
                return CompletableFuture.failedFuture(outcome.failure());
            });
    }

    private CompletableFuture<Void> executeFencedRetiredCleanup(
            StreamIdentifier id, int partitionIndex, LogMetadata metadata,
            List<FencedMappingOutcome> outcomes,
            Supplier<CompletableFuture<Void>> ownershipVerifier) {
        StorageApi storageApi = Objects.requireNonNull(
            fencedMappingStorage, "fencedMappingStorage");
        StreamIdMappingFence canonicalFence = new StreamIdMappingFence(
            metadata.streamId(), streamIdMappingOwner(metadata));
        Set<Long> preservedActiveStreamIds = outcomes.stream()
            .map(FencedMappingOutcome::preservedActive)
            .filter(Objects::nonNull)
            .map(ActiveStreamIdMapping::streamId)
            .collect(Collectors.toUnmodifiableSet());
        CompletableFuture<Void> cleanup = CompletableFuture.completedFuture(null);
        if (metadata.deleted()) {
            for (FencedMappingOutcome outcome : outcomes) {
                StreamIdMappingFence observed = outcome.fence();
                if (observed == null || observed.equals(canonicalFence)) {
                    continue;
                }
                cleanup = cleanup
                    .thenCompose(ignored -> ownershipVerifier.get())
                    .thenCompose(ignored -> storageApi.canonicalizeStreamIdMappingFence(
                        outcome.mapping().mappingKey(), observed, canonicalFence));
            }
        } else {
            if (metadata.streamId() < 0) {
                return CompletableFuture.failedFuture(
                    new PartitionLifecycleFencedException(
                        id, partitionIndex,
                        "active partition metadata has no physical stream ID"));
            }
            for (FencedMappingOutcome outcome : outcomes) {
                StreamIdMappingFence observed = outcome.fence();
                if (observed == null) {
                    continue;
                }
                if (!observed.equals(canonicalFence)) {
                    cleanup = cleanup
                        .thenCompose(ignored -> ownershipVerifier.get())
                        .thenCompose(ignored ->
                            storageApi.canonicalizeStreamIdMappingFence(
                                outcome.mapping().mappingKey(), observed, canonicalFence));
                }
                cleanup = cleanup
                    .thenCompose(ignored -> ownershipVerifier.get())
                    .thenCompose(ignored -> storageApi.bindStreamIdMapping(
                        outcome.mapping().mappingKey(), metadata.streamId(),
                        canonicalFence.owner(), Optional.of(canonicalFence)));
            }
        }
        for (long streamId : metadata.purgeableRetiredStreamIds()) {
            if (preservedActiveStreamIds.contains(streamId)
                    || !metadata.deleted() && metadata.streamId() == streamId) {
                continue;
            }
            cleanup = cleanup
                .thenCompose(ignored -> ownershipVerifier.get())
                .thenCompose(ignored -> logStorage.deleteLog(LogId.of(streamId)));
        }
        return cleanup.thenCompose(ignored -> ownershipVerifier.get());
    }

    private CompletableFuture<LogMetadata> acknowledgeRetiredCleanup(
            StreamIdentifier id, int partitionIndex,
            PersistedLogMetadata persisted,
            Supplier<CompletableFuture<Void>> ownershipVerifier) {
        LogMetadata current = persisted.metadata();
        if (!hasRetiredJournal(current)) {
            return ownershipVerifier.get().thenApply(ignored -> current);
        }
        LogMetadata acknowledged = new LogMetadata(
            current.streamId(), current.properties(), current.terminatedOffset(),
            current.registrationIncarnationId(), current.registrationOwnerToken(),
            current.registrationOwnerGeneration(), current.deleted(),
            Set.of(), Set.of(), Set.of(), Set.of());
        String path = catalogPaths.partitionMetadataPath(id, partitionIndex);
        final byte[] bytes;
        try {
            bytes = LOG_METADATA_SERDE.serialize(path, acknowledged);
        } catch (Exception e) {
            return CompletableFuture.failedFuture(e);
        }
        return ownershipVerifier.get()
            .thenCompose(ignored -> oxiaClient.put(path, bytes, Set.of(
                PutOption.IfVersionIdEquals(persisted.versionId()))))
            .handle((write, failure) -> new PartitionWriteOutcome(
                write, unwrapNullable(failure)))
            .thenCompose(outcome -> {
                if (outcome.failure() == null) {
                    return ownershipVerifier.get().thenApply(ignored -> acknowledged);
                }
                if (outcome.failure() instanceof UnexpectedVersionIdException
                        || outcome.failure() instanceof KeyAlreadyExistsException) {
                    return CompletableFuture.failedFuture(
                        new RetiredCleanupRetryException(
                            "Partition cleanup journal changed before acknowledgement",
                            outcome.failure()));
                }
                return oxiaClient.get(path).thenCompose(readback -> {
                    if (readback != null) {
                        try {
                            LogMetadata observed = LOG_METADATA_SERDE.deserialize(
                                path, readback.value());
                            if (samePersistedRegistration(observed, acknowledged)) {
                                return ownershipVerifier.get().thenApply(
                                    ignored -> observed);
                            }
                        } catch (Exception e) {
                            outcome.failure().addSuppressed(e);
                        }
                    }
                    return CompletableFuture.failedFuture(outcome.failure());
                });
            });
    }

    private CompletableFuture<PersistedLogMetadata> readPersistedLogMetadata(
            StreamIdentifier id, int partitionIndex,
            Supplier<CompletableFuture<Void>> ownershipVerifier) {
        String path = catalogPaths.partitionMetadataPath(id, partitionIndex);
        return ownershipVerifier.get()
            .thenCompose(ignored -> oxiaClient.get(path))
            .thenCompose(result -> {
                if (result == null) {
                    return CompletableFuture.failedFuture(new NoSuchStreamException(id));
                }
                final LogMetadata metadata;
                try {
                    metadata = LOG_METADATA_SERDE.deserialize(path, result.value());
                } catch (Exception e) {
                    return CompletableFuture.failedFuture(e);
                }
                return ownershipVerifier.get().thenApply(ignored ->
                    new PersistedLogMetadata(metadata, result.version().versionId()));
            });
    }

    private static boolean hasRetiredSideEffects(LogMetadata metadata) {
        return !metadata.purgeableRetiredStreamIds().isEmpty()
            || !metadata.retiredStreamMappings().isEmpty()
            || !metadata.retiredMappingKeys().isEmpty();
    }

    private static boolean hasRetiredJournal(LogMetadata metadata) {
        return !metadata.retiredStreamIds().isEmpty()
            || hasRetiredSideEffects(metadata);
    }

    private record LifecycleContextRead(
            @Nullable IndexedStreamConfigStore.LifecycleContext context,
            @Nullable Throwable failure) {
    }

    private record PersistedLogMetadata(LogMetadata metadata, long versionId) {
    }

    private record RetiredCleanupAttempt(
            @Nullable LogMetadata metadata, @Nullable Throwable failure) {
    }

    private record FencedCleanupPlan(
            LogMetadata metadata, List<FencedMappingOutcome> outcomes) {
    }

    private record FencedMappingOutcome(
            RetiredStreamMapping mapping,
            @Nullable StreamIdMappingFence fence,
            @Nullable ActiveStreamIdMapping preservedActive) {

        private static FencedMappingOutcome fenced(
                RetiredStreamMapping mapping, StreamIdMappingFence fence) {
            return new FencedMappingOutcome(mapping, fence, null);
        }

        private static FencedMappingOutcome preserved(
                RetiredStreamMapping mapping, ActiveStreamIdMapping active) {
            return new FencedMappingOutcome(mapping, null, active);
        }
    }

    private record ActiveMappingProtection(
            @Nullable StreamIdMappingOwner owner, OptionalLong streamId) {

        private static ActiveMappingProtection none() {
            return new ActiveMappingProtection(null, OptionalLong.empty());
        }

        private static ActiveMappingProtection of(
                StreamIdMappingOwner owner, OptionalLong streamId) {
            return new ActiveMappingProtection(
                Objects.requireNonNull(owner, "owner"),
                Objects.requireNonNull(streamId, "streamId"));
        }

        private boolean protects(ActiveStreamIdMapping active) {
            return owner != null && owner.equals(active.owner())
                && (streamId.isEmpty() || streamId.getAsLong() == active.streamId());
        }
    }

    private static final class RetiredCleanupRetryException extends RuntimeException {

        private RetiredCleanupRetryException(String message) {
            super(message);
        }

        private RetiredCleanupRetryException(String message, Throwable cause) {
            super(message, cause);
        }
    }

    private record ExternalWriteAttempt(
            @Nullable PartitionMetadataWrite write, @Nullable Throwable failure) {
    }

    private record NativeAllocationAttempt(
            @Nullable Long streamId, @Nullable Throwable failure) {
    }

    private record ExternalOpenAttempt(
            @Nullable StreamIdAllocation allocation, @Nullable PartitionMetadataWrite write,
            @Nullable Throwable failure) {
    }

    private record PreparedNativePartition(
            Set<Long> retiredStreamIds,
            Optional<StreamIdMappingFence> acknowledgedFence) {
    }

    private record PreparedExternalPartition(
            Set<Long> retiredStreamIds,
            Optional<StreamIdMappingFence> acknowledgedFence) {
    }

    private record DeletionRegistrationIdentity(
            @Nullable String incarnationId, @Nullable String ownerToken,
            @Nullable Long ownerGeneration) {

        private static DeletionRegistrationIdentity legacy() {
            return new DeletionRegistrationIdentity(null, null, null);
        }
    }

    private record AllocationCleanup(boolean deleteLog, boolean deleteMapping) {

        private static final AllocationCleanup PRESERVE =
            new AllocationCleanup(false, false);
        private static final AllocationCleanup DELETE_MAPPING_ONLY =
            new AllocationCleanup(false, true);
        private static final AllocationCleanup DELETE_LOG_AND_MAPPING =
            new AllocationCleanup(true, true);
    }

    private record AllocationCleanupProtection(
            boolean preserveMissingMetadata,
            Optional<IndexedStreamConfigStore.StreamConfigData> activeConfig) {

        private AllocationCleanupProtection {
            Objects.requireNonNull(activeConfig, "activeConfig");
        }

        private static AllocationCleanupProtection none() {
            return new AllocationCleanupProtection(false, Optional.empty());
        }

        private static AllocationCleanupProtection missingMetadata() {
            return new AllocationCleanupProtection(true, Optional.empty());
        }

        private static AllocationCleanupProtection active(
                IndexedStreamConfigStore.StreamConfigData config) {
            if (config.provisioningState()
                    != IndexedStreamConfigStore.ProvisioningState.ACTIVE) {
                throw new IllegalArgumentException("Cleanup protection requires ACTIVE config");
            }
            return new AllocationCleanupProtection(true, Optional.of(config));
        }

        private boolean protects(LogMetadata metadata) {
            if (activeConfig.isEmpty()) {
                return false;
            }
            IndexedStreamConfigStore.StreamConfigData config = activeConfig.orElseThrow();
            return metadata.deleted() || !metadataRegistrationMatches(
                metadata, config.incarnationId(), config.ownerToken(), config.ownerGeneration());
        }
    }

    private static final class RetiredStreamIdAllocationException
            extends AlreadyExistsException {

        private final StreamIdAllocation allocation;

        private RetiredStreamIdAllocationException(
                StreamIdAllocation allocation, String message) {
            super(message);
            this.allocation = allocation;
        }

        private StreamIdAllocation allocation() {
            return allocation;
        }

    }

    private static final class ExternalFinalizationUnknownException extends RuntimeException {

        private ExternalFinalizationUnknownException(Throwable cause) {
            super("External stream finalization outcome is unknown", cause);
        }
    }

    @Override
    public CompletableFuture<Void> deleteExternalPartition(StreamIdentifier id, int partitionIndex) {
        Objects.requireNonNull(id, "id");
        if (partitionIndex < 0) {
            throw new IllegalArgumentException("partitionIndex must be non-negative");
        }
        UnsupportedOperationException capabilityFailure = destructiveKeyedLifecycleCapabilityFailure(
            "External partition deletion");
        if (capabilityFailure != null) {
            return CompletableFuture.failedFuture(capabilityFailure);
        }

        String logName = catalogPaths.compactedReaderName(id, partitionIndex);
        return streamConfigStore.readExternalDeletionContext(id)
            .thenCompose(context -> deleteExternalPartitionWithContext(
                id, partitionIndex, logName, context,
                MAX_EXTERNAL_DELETION_CONTEXT_RETRIES));
    }

    private CompletableFuture<Void> deleteExternalPartitionWithContext(
            StreamIdentifier id, int partitionIndex, String logName,
            IndexedStreamConfigStore.ExternalDeletionContext context,
            int remainingContextRetries) {
        return deleteExternalPartitionWithResolvedMapping(
            id, partitionIndex, logName, context, OptionalLong.empty(), false,
            remainingContextRetries);
    }

    private CompletableFuture<Void> deleteExternalPartitionWithResolvedMapping(
            StreamIdentifier id, int partitionIndex, String logName,
            IndexedStreamConfigStore.ExternalDeletionContext context,
            OptionalLong mappedStreamId, boolean mappingPinned,
            int remainingContextRetries) {
        return tombstoneExternalPartition(id, partitionIndex, context, mappedStreamId)
            .thenCompose(tombstone -> sweepRetiredAllocations(
                id, partitionIndex,
                () -> streamConfigStore.verifyExternalDeletionContext(id, context))
                .thenApply(ignored -> null))
            .handle((ignored, failure) -> unwrapNullable(failure))
            .thenCompose(failure -> {
                if (failure == null) {
                    return CompletableFuture.completedFuture(null);
                }
                Throwable cause = rootCause(failure);
                if (!(cause instanceof IndexedStreamConfigStore
                        .ExternalDeletionContextInvalidatedException)
                        || remainingContextRetries == 0) {
                    return CompletableFuture.failedFuture(cause);
                }
                return streamConfigStore.readExternalDeletionContext(id)
                    .thenCompose(successor -> {
                        if (!context.canRetryWith(successor)) {
                            return CompletableFuture.failedFuture(cause);
                        }
                        if (mappingPinned) {
                            return deleteExternalPartitionWithResolvedMapping(
                                id, partitionIndex, logName, successor,
                                mappedStreamId, true, remainingContextRetries - 1);
                        }
                        return deleteExternalPartitionWithContext(
                            id, partitionIndex, logName, successor,
                            remainingContextRetries - 1);
                    });
            });
    }

    private CompletableFuture<PartitionTombstone> tombstoneExternalPartition(
            StreamIdentifier id, int partitionIndex,
            IndexedStreamConfigStore.ExternalDeletionContext context,
            OptionalLong mappedStreamId) {
        return tombstoneExternalPartition(
            id, partitionIndex, context, mappedStreamId,
            new PartitionMetadataRetry());
    }

    private CompletableFuture<PartitionTombstone> tombstoneExternalPartition(
            StreamIdentifier id, int partitionIndex,
            IndexedStreamConfigStore.ExternalDeletionContext context,
            OptionalLong mappedStreamId,
            PartitionMetadataRetry retryState) {
        IndexedStreamConfigStore.StreamConfigData config = context.config();
        String path = catalogPaths.partitionMetadataPath(id, partitionIndex);
        String mappingKey = catalogPaths.compactedReaderName(id, partitionIndex);
        return streamConfigStore.verifyExternalDeletionContext(id, context)
            .thenCompose(ignored -> oxiaClient.get(path))
            .thenCompose(existing -> {
                if (existing == null) {
                    long streamId = mappedStreamId.orElse(-1L);
                    Set<Long> retiredStreamIds = retiredStreamIdsWith(Set.of(), streamId);
                    Set<Long> purgeableRetiredStreamIds = retiredStreamIdsWith(
                        Set.of(), streamId);
                    Set<RetiredStreamMapping> retiredStreamMappings =
                        retiredStreamMappingsForTombstone(
                            Set.of(), mappingKey, true,
                            mappedStreamId.orElse(-1L));
                    Set<String> retiredMappingKeys =
                        retiredMappingKeysWith(Set.of(), mappingKey);
                    LogMetadata tombstone = externalDeletionMetadata(
                        streamId, context, retiredStreamIds, purgeableRetiredStreamIds,
                        retiredStreamMappings, retiredMappingKeys);
                    return persistPartitionMetadata(
                        id, partitionIndex, tombstone,
                        Set.of(PutOption.IfRecordDoesNotExist),
                        () -> streamConfigStore.verifyExternalDeletionContext(id, context),
                        write -> new PartitionTombstone(
                            streamId, retiredStreamIds,
                            purgeableRetiredStreamIds, retiredStreamMappings,
                            retiredMappingKeys, write),
                        () -> tombstoneExternalPartition(
                            id, partitionIndex, context, mappedStreamId, retryState),
                        retryState);
                }
                final LogMetadata current;
                try {
                    current = LOG_METADATA_SERDE.deserialize(path, existing.value());
                } catch (Exception e) {
                    return CompletableFuture.failedFuture(e);
                }
                if (context.config().provisioningState()
                            == IndexedStreamConfigStore.ProvisioningState.ACTIVE
                        && mappedStreamId.isPresent()
                        && current.streamId() >= 0
                        && current.streamId() != mappedStreamId.getAsLong()) {
                    return CompletableFuture.failedFuture(new AlreadyExistsException(
                        "External partition mapping and metadata disagree for "
                            + id.fullName() + "-partition-" + partitionIndex));
                }
                long streamId = current.streamId() >= 0
                    ? current.streamId() : mappedStreamId.orElse(-1L);
                long expectedMappingStreamId = mappingStreamIdForTombstone(
                    current, mappingKey, mappedStreamId);
                Set<Long> retiredStreamIds = retiredStreamIdsWith(
                    current.retiredStreamIds(), streamId,
                    mappedStreamId.orElse(-1L));
                Set<Long> purgeableRetiredStreamIds = purgeAllRetiredStreamIds(
                    current.purgeableRetiredStreamIds(), retiredStreamIds);
                Set<RetiredStreamMapping> retiredStreamMappings =
                    retiredStreamMappingsForTombstone(
                        current.retiredStreamMappings(), mappingKey, true,
                        expectedMappingStreamId);
                Set<String> retiredMappingKeys = retiredMappingKeysWith(
                    current.retiredMappingKeys(), mappingKey);
                LogMetadata tombstone = externalDeletionMetadata(
                    streamId, current, retiredStreamIds, purgeableRetiredStreamIds,
                    retiredStreamMappings, retiredMappingKeys);
                if (samePersistedRegistration(current, tombstone)) {
                    return streamConfigStore.verifyExternalDeletionContext(id, context)
                        .thenApply(ignored -> new PartitionTombstone(
                            streamId, retiredStreamIds,
                            purgeableRetiredStreamIds, retiredStreamMappings,
                            retiredMappingKeys,
                            new PartitionMetadataWrite(OptionalLong.of(
                                existing.version().versionId()))));
                }
                if (!metadataCanBeFencedByDeletion(current, config)) {
                    return CompletableFuture.failedFuture(new NoSuchStreamException(id));
                }
                return persistPartitionMetadata(
                    id, partitionIndex, tombstone,
                    Set.of(PutOption.IfVersionIdEquals(existing.version().versionId())),
                    () -> streamConfigStore.verifyExternalDeletionContext(id, context),
                    write -> new PartitionTombstone(
                        streamId, retiredStreamIds,
                        purgeableRetiredStreamIds, retiredStreamMappings,
                        retiredMappingKeys, write),
                    () -> tombstoneExternalPartition(
                        id, partitionIndex, context, mappedStreamId, retryState),
                    retryState);
            });
    }

    private static LogMetadata externalDeletionMetadata(
            long streamId, IndexedStreamConfigStore.ExternalDeletionContext context,
            Set<Long> retiredStreamIds, Set<Long> purgeableRetiredStreamIds) {
        return externalDeletionMetadata(
            streamId, context, retiredStreamIds, purgeableRetiredStreamIds,
            Set.of(), Set.of());
    }

    private static LogMetadata externalDeletionMetadata(
            long streamId, IndexedStreamConfigStore.ExternalDeletionContext context,
            Set<Long> retiredStreamIds, Set<Long> purgeableRetiredStreamIds,
            Set<RetiredStreamMapping> retiredStreamMappings,
            Set<String> retiredMappingKeys) {
        return deletionMetadata(
            streamId, deletionRegistrationIdentity(context.config()), retiredStreamIds,
            purgeableRetiredStreamIds, retiredStreamMappings, retiredMappingKeys);
    }

    private static LogMetadata externalDeletionMetadata(
            long streamId, LogMetadata registrationSource,
            Set<Long> retiredStreamIds, Set<Long> purgeableRetiredStreamIds,
            Set<RetiredStreamMapping> retiredStreamMappings,
            Set<String> retiredMappingKeys) {
        return deletionMetadata(
            streamId, deletionRegistrationIdentity(registrationSource), retiredStreamIds,
            purgeableRetiredStreamIds,
            retiredStreamMappings, retiredMappingKeys);
    }

    private static Throwable rootCause(Throwable failure) {
        Throwable current = failure;
        while ((current instanceof CompletionException || current instanceof java.util.concurrent.ExecutionException)
                && current.getCause() != null) {
            current = current.getCause();
        }
        return current;
    }

    private CompletableFuture<LifecycleContextRead> readLifecycleContextForCleanup(
            StreamIdentifier id, Throwable originalFailure) {
        return streamConfigStore.readLifecycleContext(id)
            .handle((context, failure) -> new LifecycleContextRead(
                context, unwrapNullable(failure)))
            .thenCompose(first -> {
                if (first.failure() == null) {
                    return CompletableFuture.completedFuture(first);
                }
                addSuppressed(originalFailure, first.failure());
                return streamConfigStore.readLifecycleContext(id)
                    .handle((context, retryFailure) -> {
                        if (retryFailure != null) {
                            addSuppressed(originalFailure, retryFailure);
                            return new LifecycleContextRead(null, retryFailure);
                        }
                        return new LifecycleContextRead(context, null);
                    });
            });
    }

    private static void addSuppressed(Throwable target, Throwable failure) {
        Throwable cause = rootCause(failure);
        if (cause == target) {
            return;
        }
        for (Throwable suppressed : target.getSuppressed()) {
            if (suppressed == cause) {
                return;
            }
        }
        target.addSuppressed(cause);
    }

    private CompletableFuture<PartitionMetadataWrite> writePartitionMetadataForNativeClaim(
            StreamIdentifier id, int partitionIndex, long streamId,
            IndexedStreamConfigStore.ProvisioningClaim claim) {
        return writePartitionMetadataForRegistration(
            id, partitionIndex, streamId,
            Optional.of(claim.incarnationId()), Optional.of(claim.ownerToken()),
            claim.ownerGeneration(), claim.config().metadataSourceGeneration(),
            IndexedStreamConfigStore.CreationKind.NATIVE_CREATE,
            () -> streamConfigStore.verifyProvisioningOwnership(id, claim));
    }

    private CompletableFuture<PartitionMetadataWrite> writePartitionMetadataForExternalRegistration(
            StreamIdentifier id, int partitionIndex, long streamId,
            IndexedStreamConfigStore.ExternalRegistration registration) {
        return writePartitionMetadataForRegistration(
            id, partitionIndex, streamId,
            registration.incarnationId(), registration.ownerToken(),
            registration.ownerGeneration(), registration.metadataSourceGeneration(),
            IndexedStreamConfigStore.CreationKind.EXTERNAL,
            () -> streamConfigStore.verifyExternalRegistration(id, registration));
    }

    private CompletableFuture<PartitionMetadataWrite> writePartitionMetadataForRegistration(
            StreamIdentifier id, int partitionIndex, long streamId,
            Optional<String> incarnationId, Optional<String> ownerToken,
            long ownerGeneration, long metadataSourceGeneration,
            IndexedStreamConfigStore.CreationKind creationKind,
            Supplier<CompletableFuture<Void>> ownershipVerifier) {
        return writePartitionMetadataForRegistration(
            id, partitionIndex, streamId, incarnationId, ownerToken,
            ownerGeneration, metadataSourceGeneration, creationKind,
            ownershipVerifier, new PartitionMetadataRetry());
    }

    private CompletableFuture<PartitionMetadataWrite> writePartitionMetadataForRegistration(
            StreamIdentifier id, int partitionIndex, long streamId,
            Optional<String> incarnationId, Optional<String> ownerToken,
            long ownerGeneration, long metadataSourceGeneration,
            IndexedStreamConfigStore.CreationKind creationKind,
            Supplier<CompletableFuture<Void>> ownershipVerifier,
            PartitionMetadataRetry retryState) {
        if (incarnationId.isPresent() != ownerToken.isPresent()) {
            return CompletableFuture.failedFuture(new IllegalStateException(
                "Registration identity must contain both incarnation and owner"));
        }
        if (incarnationId.isPresent() != (ownerGeneration >= 0)) {
            return CompletableFuture.failedFuture(new IllegalStateException(
                "Registration identity must contain an owner generation"));
        }
        String path = catalogPaths.partitionMetadataPath(id, partitionIndex);
        return ownershipVerifier.get()
            .thenCompose(ignored -> oxiaClient.get(path))
            .thenCompose(existing -> {
                if (existing == null) {
                    LogMetadata desired = registrationMetadata(
                        streamId, incarnationId, ownerToken, ownerGeneration);
                    return persistPartitionMetadata(
                        id, partitionIndex, desired,
                        Set.of(PutOption.IfRecordDoesNotExist), ownershipVerifier,
                        () -> writePartitionMetadataForRegistration(
                            id, partitionIndex, streamId, incarnationId, ownerToken,
                            ownerGeneration, metadataSourceGeneration,
                            creationKind,
                            ownershipVerifier, retryState), retryState);
                }
                final LogMetadata current;
                try {
                    current = LOG_METADATA_SERDE.deserialize(path, existing.value());
                } catch (Exception e) {
                    return CompletableFuture.failedFuture(e);
                }
                if (!validRegistrationIdentity(current)) {
                    return partitionMetadataConflict(
                        id, partitionIndex, streamId, current.streamId(),
                        "has a partial registration identity");
                }
                // A successful durable sweep has already fenced the deleted primary ID and
                // acknowledged its cleanup journal. Keep that ID only in the prepare result for
                // this allocation attempt; carrying it into ACTIVE metadata would recreate an
                // unbounded retired-ID history on every delete/recreate cycle.
                Set<Long> currentRetiredStreamIds = current.retiredStreamIds();
                Set<Long> currentPurgeableRetiredStreamIds =
                    current.purgeableRetiredStreamIds();
                if (currentRetiredStreamIds.contains(streamId)) {
                    if (current.deleted()) {
                        return partitionLifecycleFenced(
                            id, partitionIndex,
                            "cannot reuse physical stream ID " + streamId
                                + " retired by a deleted partition lifecycle");
                    }
                    return partitionMetadataConflict(
                        id, partitionIndex, streamId, current.streamId(),
                        "cannot reuse retired physical stream ID " + streamId);
                }
                boolean desiredLegacy = incarnationId.isEmpty();
                boolean currentLegacy = current.registrationIncarnationId() == null;
                boolean sameIdentity = metadataRegistrationMatches(
                    current, incarnationId, ownerToken, ownerGeneration);
                if (!current.deleted() && sameIdentity && current.streamId() == streamId) {
                    return ownershipVerifier.get().thenApply(ignored ->
                        new PartitionMetadataWrite(OptionalLong.of(
                            existing.version().versionId())));
                }
                if (desiredLegacy) {
                    if (desiredLegacy && currentLegacy && current.streamId() == streamId) {
                        return ownershipVerifier.get().thenApply(ignored ->
                            new PartitionMetadataWrite(OptionalLong.of(
                                existing.version().versionId())));
                    }
                    return partitionMetadataConflict(
                        id, partitionIndex, streamId, current.streamId(),
                        "has legacy metadata from a different registration lifecycle");
                }
                if (currentLegacy && !current.deleted()
                        && metadataSourceGeneration
                            != IndexedStreamConfigStore.LEGACY_METADATA_GENERATION) {
                    return partitionMetadataConflict(
                        id, partitionIndex, streamId, current.streamId(),
                        "has unclaimed legacy metadata");
                }
                if (!currentLegacy && !incarnationId.orElseThrow()
                        .equals(current.registrationIncarnationId())) {
                    if (!current.deleted()) {
                        return partitionMetadataConflict(
                            id, partitionIndex, streamId, current.streamId(),
                            "belongs to a different stream incarnation");
                    }
                    if (current.registrationOwnerGeneration() >= ownerGeneration) {
                        return partitionLifecycleFenced(
                            id, partitionIndex,
                            "is fenced by a newer deleted stream lifecycle");
                    }
                    if (creationKind == IndexedStreamConfigStore.CreationKind.EXTERNAL
                            && current.streamId() >= 0
                            && current.streamId() == streamId) {
                        return partitionMetadataConflict(
                            id, partitionIndex, streamId, current.streamId(),
                            "deleted external partition must use a fresh physical stream ID");
                    }
                    return replacePartitionMetadata(
                        id, partitionIndex,
                        registrationMetadata(
                            streamId, incarnationId, ownerToken, ownerGeneration,
                            currentRetiredStreamIds, currentPurgeableRetiredStreamIds,
                            current.retiredStreamMappings(), current.retiredMappingKeys()),
                        existing.version().versionId(), ownershipVerifier,
                        () -> writePartitionMetadataForRegistration(
                            id, partitionIndex, streamId, incarnationId, ownerToken,
                            ownerGeneration, metadataSourceGeneration, creationKind,
                            ownershipVerifier, retryState), retryState);
                }
                long currentGeneration = current.registrationOwnerGeneration() == null
                    ? IndexedStreamConfigStore.LEGACY_METADATA_GENERATION
                    : current.registrationOwnerGeneration();
                if (currentGeneration > ownerGeneration) {
                    return partitionMetadataConflict(
                        id, partitionIndex, streamId, current.streamId(),
                        "is owned by a newer generation");
                }
                if (currentGeneration == ownerGeneration) {
                    if (current.deleted()) {
                        return partitionLifecycleFenced(
                            id, partitionIndex,
                            "is fenced by deleted metadata from the current generation");
                    }
                    return partitionMetadataConflict(
                        id, partitionIndex, streamId, current.streamId(),
                        "already belongs to the current generation with different metadata");
                }
                boolean sameStreamId = current.streamId() == streamId;
                if (current.deleted()
                        && creationKind == IndexedStreamConfigStore.CreationKind.EXTERNAL
                        && current.streamId() >= 0
                        && sameStreamId) {
                    return partitionMetadataConflict(
                        id, partitionIndex, streamId, current.streamId(),
                        "deleted external partition must use a fresh physical stream ID");
                }
                if (!current.deleted()
                        && creationKind == IndexedStreamConfigStore.CreationKind.NATIVE_CREATE
                        && !sameStreamId) {
                    return partitionMetadataConflict(
                        id, partitionIndex, streamId, current.streamId(),
                        "native takeover must reuse its stable stream ID");
                }
                if (!current.deleted()
                        && metadataSourceGeneration
                            != IndexedStreamConfigStore.NO_METADATA_GENERATION
                        && !sameStreamId) {
                    return partitionMetadataConflict(
                        id, partitionIndex, streamId, current.streamId(),
                        "was reversibly unregistered with a different stream ID");
                }
                LogMetadata replacement = sameStreamId
                    ? new LogMetadata(
                        streamId, current.properties(), current.terminatedOffset(),
                        incarnationId.orElseThrow(), ownerToken.orElseThrow(),
                        ownerGeneration, false, currentRetiredStreamIds,
                        currentPurgeableRetiredStreamIds,
                        current.retiredStreamMappings(), current.retiredMappingKeys())
                    : registrationMetadata(
                        streamId, incarnationId, ownerToken, ownerGeneration,
                        currentRetiredStreamIds, currentPurgeableRetiredStreamIds,
                        current.retiredStreamMappings(), current.retiredMappingKeys());
                return replacePartitionMetadata(
                    id, partitionIndex, replacement,
                    existing.version().versionId(), ownershipVerifier,
                    () -> writePartitionMetadataForRegistration(
                        id, partitionIndex, streamId, incarnationId, ownerToken,
                        ownerGeneration, metadataSourceGeneration, creationKind,
                        ownershipVerifier, retryState), retryState);
            });
    }

    private static LogMetadata registrationMetadata(
            long streamId, Optional<String> incarnationId, Optional<String> ownerToken,
            long ownerGeneration) {
        return registrationMetadata(
            streamId, incarnationId, ownerToken, ownerGeneration, Set.of(), Set.of(),
            Set.of(), Set.of());
    }

    private static LogMetadata registrationMetadata(
            long streamId, Optional<String> incarnationId, Optional<String> ownerToken,
            long ownerGeneration, Set<Long> retiredStreamIds,
            Set<Long> purgeableRetiredStreamIds) {
        return registrationMetadata(
            streamId, incarnationId, ownerToken, ownerGeneration, retiredStreamIds,
            purgeableRetiredStreamIds, Set.of(), Set.of());
    }

    private static LogMetadata registrationMetadata(
            long streamId, Optional<String> incarnationId, Optional<String> ownerToken,
            long ownerGeneration, Set<Long> retiredStreamIds,
            Set<Long> purgeableRetiredStreamIds,
            Set<RetiredStreamMapping> retiredStreamMappings,
            Set<String> retiredMappingKeys) {
        return new LogMetadata(
            streamId, Map.of(), OptionalLong.empty(),
            incarnationId.orElse(null), ownerToken.orElse(null),
            ownerGeneration >= 0 ? ownerGeneration : null, false, retiredStreamIds,
            purgeableRetiredStreamIds, retiredStreamMappings, retiredMappingKeys);
    }

    private static Set<Long> retiredStreamIdsWith(
            Set<Long> current, long... additionalStreamIds) {
        TreeSet<Long> retiredStreamIds = new TreeSet<>(current);
        for (long streamId : additionalStreamIds) {
            if (streamId >= 0) {
                retiredStreamIds.add(streamId);
            }
        }
        return retiredStreamIds;
    }

    private static Set<Long> purgeAllRetiredStreamIds(
            Set<Long> currentPurgeable, Set<Long> retiredStreamIds) {
        TreeSet<Long> purgeableStreamIds = new TreeSet<>(currentPurgeable);
        purgeableStreamIds.addAll(retiredStreamIds);
        return purgeableStreamIds;
    }

    private static Set<RetiredStreamMapping> retiredStreamMappingsWith(
            Set<RetiredStreamMapping> current, String mappingKey,
            long... additionalStreamIds) {
        TreeSet<RetiredStreamMapping> retiredStreamMappings = new TreeSet<>(current);
        for (long streamId : additionalStreamIds) {
            if (streamId >= 0) {
                retiredStreamMappings.add(new RetiredStreamMapping(streamId, mappingKey));
            }
        }
        return retiredStreamMappings;
    }

    private static Set<RetiredStreamMapping> retiredStreamMappingsForTombstone(
            Set<RetiredStreamMapping> current, String mappingKey,
            boolean purgeUnknown, long observedMappingStreamId) {
        TreeSet<RetiredStreamMapping> updated = new TreeSet<>(current);
        boolean purge = purgeUnknown;
        for (RetiredStreamMapping mapping : current) {
            if (mapping.mappingKey().equals(mappingKey)) {
                purge |= mapping.purge();
            }
        }
        updated.removeIf(mapping -> mapping.mappingKey().equals(mappingKey));
        updated.add(new RetiredStreamMapping(
            observedMappingStreamId, mappingKey, purge));
        return updated;
    }

    private static long mappingStreamIdForTombstone(
            LogMetadata current, String mappingKey, OptionalLong observedMappingStreamId) {
        if (observedMappingStreamId.isPresent()) {
            return observedMappingStreamId.getAsLong();
        }
        List<RetiredStreamMapping> persistedMappings = current.retiredStreamMappings().stream()
            .filter(mapping -> mapping.mappingKey().equals(mappingKey))
            .toList();
        if (persistedMappings.size() == 1) {
            long persistedStreamId = persistedMappings.get(0).streamId();
            if (persistedStreamId >= 0 || !streamIdMappingOwner(current).isLegacy()) {
                return persistedStreamId;
            }
        } else if (persistedMappings.size() > 1) {
            return -1L;
        }
        return current.streamId() >= 0 ? current.streamId() : -1L;
    }

    private static Set<String> retiredMappingKeysWith(
            Set<String> current, String mappingKey) {
        TreeSet<String> retiredMappingKeys = new TreeSet<>(current);
        retiredMappingKeys.add(mappingKey);
        return retiredMappingKeys;
    }

    private static LogMetadata withRetiredCleanup(
            LogMetadata current, Set<Long> retiredStreamIds,
            Set<Long> purgeableRetiredStreamIds,
            Set<RetiredStreamMapping> retiredStreamMappings,
            Set<String> retiredMappingKeys) {
        return new LogMetadata(
            current.streamId(), current.properties(), current.terminatedOffset(),
            current.registrationIncarnationId(), current.registrationOwnerToken(),
            current.registrationOwnerGeneration(), current.deleted(), retiredStreamIds,
            purgeableRetiredStreamIds, retiredStreamMappings, retiredMappingKeys);
    }

    private CompletableFuture<PartitionMetadataWrite> replacePartitionMetadata(
            StreamIdentifier id, int partitionIndex, LogMetadata desired,
            long expectedVersion, Supplier<CompletableFuture<Void>> ownershipVerifier,
            Supplier<CompletableFuture<PartitionMetadataWrite>> retry,
            PartitionMetadataRetry retryState) {
        return persistPartitionMetadata(
            id, partitionIndex, desired,
            Set.of(PutOption.IfVersionIdEquals(expectedVersion)), ownershipVerifier,
            retry, retryState);
    }

    private CompletableFuture<PartitionMetadataWrite> persistPartitionMetadata(
            StreamIdentifier id, int partitionIndex, LogMetadata desired,
            Set<PutOption> options, Supplier<CompletableFuture<Void>> ownershipVerifier,
            Supplier<CompletableFuture<PartitionMetadataWrite>> retry,
            PartitionMetadataRetry retryState) {
        return persistPartitionMetadata(
            id, partitionIndex, desired, options, ownershipVerifier,
            Function.identity(), retry, retryState);
    }

    private <T> CompletableFuture<T> persistPartitionMetadata(
            StreamIdentifier id, int partitionIndex, LogMetadata desired,
            Set<PutOption> options, Supplier<CompletableFuture<Void>> ownershipVerifier,
            Function<PartitionMetadataWrite, T> resultFactory,
            Supplier<CompletableFuture<T>> retry,
            PartitionMetadataRetry retryState) {
        String path = catalogPaths.partitionMetadataPath(id, partitionIndex);
        final byte[] bytes;
        try {
            bytes = LOG_METADATA_SERDE.serialize(path, desired);
        } catch (Exception e) {
            return CompletableFuture.failedFuture(e);
        }
        return oxiaClient.put(path, bytes, options)
            .handle((result, failure) -> new PartitionWriteOutcome(
                result, unwrapNullable(failure)))
            .thenCompose(outcome -> {
                if (outcome.failure() == null) {
                    return ownershipVerifier.get().thenApply(ignored ->
                        resultFactory.apply(new PartitionMetadataWrite(OptionalLong.of(
                            outcome.result().version().versionId()))));
                }
                if (outcome.failure() instanceof KeyAlreadyExistsException
                        || outcome.failure() instanceof UnexpectedVersionIdException) {
                    return retryPartitionMetadataWrite(
                        id, partitionIndex, outcome.failure(), retry, retryState);
                }
                return oxiaClient.get(path)
                    .handle((current, readFailure) ->
                        new PartitionReadOutcome(current, unwrapNullable(readFailure)))
                    .thenCompose(readback -> {
                        if (readback.failure() != null) {
                            outcome.failure().addSuppressed(readback.failure());
                            return CompletableFuture.failedFuture(outcome.failure());
                        }
                        if (readback.result() == null) {
                            return CompletableFuture.failedFuture(outcome.failure());
                        }
                        final LogMetadata current;
                        try {
                            current = LOG_METADATA_SERDE.deserialize(
                                path, readback.result().value());
                        } catch (Exception e) {
                            return CompletableFuture.failedFuture(e);
                        }
                        if (!samePersistedRegistration(current, desired)) {
                            return CompletableFuture.failedFuture(outcome.failure());
                        }
                        return ownershipVerifier.get().thenApply(ignored ->
                            resultFactory.apply(new PartitionMetadataWrite(OptionalLong.of(
                                readback.result().version().versionId()))));
                    });
            });
    }

    private <T> CompletableFuture<T> retryPartitionMetadataWrite(
            StreamIdentifier id, int partitionIndex, Throwable failure,
            Supplier<CompletableFuture<T>> retry,
            PartitionMetadataRetry retryState) {
        int retryAttempt = retryState.attempts().getAndIncrement();
        if (retryAttempt >= MAX_PARTITION_METADATA_WRITE_RETRIES) {
            log.warn("Exhausted {} partition metadata retries for {}-partition-{}",
                MAX_PARTITION_METADATA_WRITE_RETRIES, id.fullName(), partitionIndex, failure);
            return CompletableFuture.failedFuture(failure);
        }
        long delayMillis = PARTITION_METADATA_RETRY_DELAY_MILLIS << retryAttempt;
        log.debug("Retrying partition metadata write for {}-partition-{} after {} ms "
                + "(retry {}/{}): {}", id.fullName(), partitionIndex, delayMillis,
            retryAttempt + 1, MAX_PARTITION_METADATA_WRITE_RETRIES, failure.toString());
        return CompletableFuture.runAsync(
                () -> { }, CompletableFuture.delayedExecutor(
                    delayMillis, TimeUnit.MILLISECONDS))
            .thenCompose(ignored -> retry.get());
    }

    private static boolean samePersistedRegistration(
            LogMetadata current, LogMetadata desired) {
        return current.streamId() == desired.streamId()
            && current.deleted() == desired.deleted()
            && current.retiredStreamIds().equals(desired.retiredStreamIds())
            && current.purgeableRetiredStreamIds().equals(
                desired.purgeableRetiredStreamIds())
            && current.retiredStreamMappings().equals(desired.retiredStreamMappings())
            && current.retiredMappingKeys().equals(desired.retiredMappingKeys())
            && Objects.equals(current.registrationIncarnationId(),
                desired.registrationIncarnationId())
            && Objects.equals(current.registrationOwnerToken(),
                desired.registrationOwnerToken())
            && Objects.equals(current.registrationOwnerGeneration(),
                desired.registrationOwnerGeneration());
    }

    private static boolean metadataRegistrationMatches(
            LogMetadata metadata, Optional<String> incarnationId,
            Optional<String> ownerToken, long ownerGeneration) {
        return Objects.equals(metadata.registrationIncarnationId(), incarnationId.orElse(null))
            && Objects.equals(metadata.registrationOwnerToken(), ownerToken.orElse(null))
            && Objects.equals(metadata.registrationOwnerGeneration(),
                ownerGeneration >= 0 ? ownerGeneration : null);
    }

    private static boolean validRegistrationIdentity(LogMetadata metadata) {
        boolean legacy = metadata.registrationIncarnationId() == null
            && metadata.registrationOwnerToken() == null
            && metadata.registrationOwnerGeneration() == null;
        boolean modern = metadata.registrationIncarnationId() != null
            && metadata.registrationOwnerToken() != null
            && metadata.registrationOwnerGeneration() != null;
        return legacy || modern;
    }

    private static StreamIdMappingOwner streamIdMappingOwner(
            IndexedStreamConfigStore.ProvisioningClaim claim) {
        return new StreamIdMappingOwner(
            claim.incarnationId(), claim.ownerToken(), claim.ownerGeneration());
    }

    private static StreamIdMappingOwner streamIdMappingOwner(
            IndexedStreamConfigStore.ExternalRegistration registration) {
        if (registration.incarnationId().isEmpty()
                || registration.ownerToken().isEmpty()
                || registration.ownerGeneration() < 0) {
            return StreamIdMappingOwner.legacy();
        }
        return new StreamIdMappingOwner(
            registration.incarnationId().orElseThrow(),
            registration.ownerToken().orElseThrow(),
            registration.ownerGeneration());
    }

    private static StreamIdMappingOwner streamIdMappingOwner(LogMetadata metadata) {
        if (metadata.registrationIncarnationId() == null
                || metadata.registrationOwnerToken() == null
                || metadata.registrationOwnerGeneration() == null) {
            return StreamIdMappingOwner.legacy();
        }
        return new StreamIdMappingOwner(
            metadata.registrationIncarnationId(), metadata.registrationOwnerToken(),
            metadata.registrationOwnerGeneration());
    }

    private static Optional<StreamIdMappingFence> acknowledgedFence(LogMetadata metadata) {
        return metadata.deleted()
            ? Optional.of(new StreamIdMappingFence(
                metadata.streamId(), streamIdMappingOwner(metadata)))
            : Optional.empty();
    }

    private static CompletableFuture<PartitionMetadataWrite> partitionMetadataConflict(
            StreamIdentifier id, int partitionIndex, long expectedStreamId,
            long existingStreamId, String detail) {
        return CompletableFuture.failedFuture(new AlreadyExistsException(
            "Partition " + id.fullName() + "-partition-" + partitionIndex + " " + detail
                + "; existing stream ID " + existingStreamId
                + ", requested stream ID " + expectedStreamId));
    }

    private static CompletableFuture<PartitionMetadataWrite> partitionLifecycleFenced(
            StreamIdentifier id, int partitionIndex, String detail) {
        return CompletableFuture.failedFuture(
            new PartitionLifecycleFencedException(id, partitionIndex, detail));
    }

    private record PartitionWriteOutcome(PutResult result, Throwable failure) {
    }

    private record PartitionMetadataWrite(OptionalLong metadataVersion) {
    }

    private record PartitionReadOutcome(
            GetResult result, Throwable failure) {
    }

    private record PartitionMetadataRetry(AtomicInteger attempts) {

        private PartitionMetadataRetry() {
            this(new AtomicInteger());
        }
    }

    private static Throwable unwrap(Throwable ex) {
        return ex instanceof CompletionException && ex.getCause() != null ? ex.getCause() : ex;
    }

    private static Throwable unwrapNullable(Throwable failure) {
        return failure == null ? null : unwrap(failure);
    }

    @Override
    public CompletableFuture<Stream> loadStream(StreamIdentifier id) {
        return streamConfigStore.readActive(id).thenCompose(active -> {
            IndexedStreamConfigStore.StreamConfigData config = active.config();
            Partitioning partitioning = new Partitioning(
                PartitioningStrategy.INDEXED,
                Map.of("numPartitions", String.valueOf(config.partitions())));
            return buildStreamImpl(id, new StreamConfig(), partitioning, new SchemaConfig(),
                config.properties(), LifecycleState.ACTIVE, config.materialization(), active);
        });
    }

    private CompletableFuture<Stream> buildStreamImpl(StreamIdentifier id, StreamConfig config,
                                                       Partitioning partitioning, SchemaConfig schema,
                                                       Map<String, String> properties, LifecycleState state,
                                                       Optional<TableMaterializationPolicy> materialization) {
        return getLayoutTolerant(id).thenCompose(layout ->
            buildStreamImpl(id, config, partitioning, schema, properties, state,
                materialization, layout));
    }

    private CompletableFuture<Stream> buildStreamImpl(StreamIdentifier id, StreamConfig config,
                                                       Partitioning partitioning, SchemaConfig schema,
                                                       Map<String, String> properties, LifecycleState state,
                                                       Optional<TableMaterializationPolicy> materialization,
                                                       IndexedStreamConfigStore.ActiveStreamConfig active) {
        return getLayoutTolerant(id, active).thenCompose(layout ->
            buildStreamImpl(id, config, partitioning, schema, properties, state,
                materialization, layout));
    }

    private CompletableFuture<Stream> buildStreamImpl(StreamIdentifier id, StreamConfig config,
                                                       Partitioning partitioning, SchemaConfig schema,
                                                       Map<String, String> properties, LifecycleState state,
                                                       Optional<TableMaterializationPolicy> materialization,
                                                       StreamLayout layout) {
        return createUnifiedReader(id, layout).thenApply(unifiedReader ->
                new StreamImpl(id, config, partitioning, schema, properties, state,
                    layout, logStorage, unifiedReader, entryIndexCache, logStateManager,
                    materialization, this::loadNamespaceMaterializationFromCache,
                    this::clusterDefaultMaterialization, this::lookupTableCatalog));
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
        return oxiaClient.list(prefix, endKey).thenCompose(keys -> {
            List<StreamIdentifier> identifiers = keys.stream()
                .map(key -> new StreamIdentifier(namespaceName, key.substring(prefix.length())))
                .collect(Collectors.toList());
            return filterVisibleStreams(identifiers);
        });
    }

    private CompletableFuture<List<StreamIdentifier>> filterVisibleStreams(
            List<StreamIdentifier> identifiers) {
        CompletableFuture<List<StreamIdentifier>> result =
            CompletableFuture.completedFuture(new ArrayList<>());
        for (int start = 0; start < identifiers.size();
                start += STREAM_VISIBILITY_READ_BATCH_SIZE) {
            int end = Math.min(start + STREAM_VISIBILITY_READ_BATCH_SIZE, identifiers.size());
            List<StreamIdentifier> batch = identifiers.subList(start, end);
            result = result.thenCompose(visible -> {
                List<CompletableFuture<Boolean>> visibility = batch.stream()
                    .map(streamConfigStore::exists)
                    .toList();
                return CompletableFuture.allOf(visibility.toArray(new CompletableFuture[0]))
                    .thenApply(ignored -> {
                        for (int index = 0; index < visibility.size(); index++) {
                            if (visibility.get(index).join()) {
                                visible.add(batch.get(index));
                            }
                        }
                        return visible;
                    });
            });
        }
        return result;
    }

    @Override
    public CompletableFuture<Boolean> dropStream(StreamIdentifier id, boolean purge) {
        UnsupportedOperationException capabilityFailure = destructiveKeyedLifecycleCapabilityFailure(
            "Stream deletion");
        if (capabilityFailure != null) {
            return CompletableFuture.failedFuture(capabilityFailure);
        }
        String dropOwnerToken = UUID.randomUUID().toString();
        return streamConfigStore.beginDrop(id, dropOwnerToken, purge).thenCompose(optionalClaim -> {
            if (optionalClaim.isEmpty()) {
                return streamConfigStore.readCompletedDrop(id)
                    .thenCompose(completed -> completed.isEmpty()
                        ? CompletableFuture.completedFuture(false)
                        : cleanupCompletedDrop(id, completed.orElseThrow())
                            .thenApply(ignored -> false));
            }
            IndexedStreamConfigStore.DropClaim claim = optionalClaim.orElseThrow();
            return cleanupDroppedStream(id, claim, claim.config().purgeRequested())
                .thenCompose(ignored -> streamConfigStore.verifyAbortingOwnership(id, claim))
                .thenCompose(ignored -> streamConfigStore.completeDrop(id, claim))
                .thenApply(ignored -> true);
        });
    }

    private CompletableFuture<Void> cleanupDroppedStream(
            StreamIdentifier id, IndexedStreamConfigStore.DropClaim claim,
            boolean purge) {
        CompletableFuture<Void> chain = CompletableFuture.completedFuture(null);
        for (int i = 0; i < claim.config().partitions(); i++) {
            int partitionIndex = i;
            chain = chain
                .thenCompose(ignored ->
                    streamConfigStore.verifyAbortingOwnership(id, claim))
                .thenCompose(ignored -> mappingForDrop(id, partitionIndex, claim))
                .thenCompose(mapping -> tombstoneDroppedPartition(
                    id, partitionIndex, claim, mapping, purge)
                    .thenCompose(tombstone -> cleanupDroppedPartition(
                        id, partitionIndex, claim, tombstone)));
        }
        return chain;
    }

    private CompletableFuture<Void> cleanupCompletedDrop(
            StreamIdentifier id, IndexedStreamConfigStore.CompletedDrop completed) {
        CompletableFuture<Void> chain = CompletableFuture.completedFuture(null);
        for (int i = 0; i < completed.config().partitions(); i++) {
            int partitionIndex = i;
            chain = chain
                .thenCompose(ignored ->
                    streamConfigStore.verifyCompletedDrop(id, completed))
                .thenCompose(ignored -> mappingForCompletedDrop(
                    id, partitionIndex, completed.config()))
                .thenCompose(mapping -> recoverCompletedDroppedPartition(
                    id, partitionIndex, completed, mapping,
                    new PartitionMetadataRetry()))
                .thenCompose(tombstone -> cleanupCompletedDroppedPartition(
                    id, partitionIndex, completed, tombstone));
        }
        return chain;
    }

    private CompletableFuture<OptionalLong> mappingForCompletedDrop(
            StreamIdentifier id, int partitionIndex,
            IndexedStreamConfigStore.StreamConfigData config) {
        String mappingKey = dropMappingKey(id, partitionIndex, config);
        return streamIdLookup.apply(mappingKey)
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
    }

    private CompletableFuture<PartitionTombstone> recoverCompletedDroppedPartition(
            StreamIdentifier id, int partitionIndex,
            IndexedStreamConfigStore.CompletedDrop completed,
            OptionalLong mappedStreamId, PartitionMetadataRetry retryState) {
        String path = catalogPaths.partitionMetadataPath(id, partitionIndex);
        String mappingKey = dropMappingKey(id, partitionIndex, completed.config());
        Supplier<CompletableFuture<Void>> ownershipVerifier =
            () -> streamConfigStore.verifyCompletedDrop(id, completed);
        return ownershipVerifier.get()
            .thenCompose(ignored -> oxiaClient.get(path))
            .thenCompose(existing -> {
                if (existing == null) {
                    long streamId = mappedStreamId.orElse(-1L);
                    Set<Long> retiredStreamIds = retiredStreamIdsWith(
                        Set.of(), streamId);
                    Set<Long> purgeableRetiredStreamIds =
                        completed.config().purgeRequested()
                            ? retiredStreamIdsWith(Set.of(), streamId)
                            : Set.of();
                    Set<RetiredStreamMapping> retiredStreamMappings =
                        retiredStreamMappingsForTombstone(
                            Set.of(), mappingKey,
                            completed.config().purgeRequested(),
                            mappedStreamId.orElse(-1L));
                    Set<String> retiredMappingKeys =
                        retiredMappingKeysWith(Set.of(), mappingKey);
                    LogMetadata tombstone = deletionMetadata(
                        streamId, completed.config(), retiredStreamIds,
                        purgeableRetiredStreamIds, retiredStreamMappings,
                        retiredMappingKeys);
                    return persistPartitionMetadata(
                        id, partitionIndex, tombstone,
                        Set.of(PutOption.IfRecordDoesNotExist), ownershipVerifier,
                        write -> new PartitionTombstone(
                            streamId, retiredStreamIds,
                            purgeableRetiredStreamIds, retiredStreamMappings,
                            retiredMappingKeys, write),
                        () -> recoverCompletedDroppedPartition(
                            id, partitionIndex, completed, mappedStreamId, retryState),
                        retryState);
                }

                final LogMetadata current;
                try {
                    current = LOG_METADATA_SERDE.deserialize(path, existing.value());
                } catch (Exception e) {
                    return CompletableFuture.failedFuture(e);
                }
                if (!metadataCanBeFencedByDeletion(current, completed.config())) {
                    return CompletableFuture.failedFuture(new AlreadyExistsException(
                        "Partition metadata belongs to a different stream lifecycle: "
                            + id.fullName() + "-partition-" + partitionIndex));
                }
                if (current.deleted() && !hasRetiredJournal(current)
                        && mappedStreamId.isEmpty()) {
                    PartitionTombstone acknowledged = new PartitionTombstone(
                        current.streamId(), current.retiredStreamIds(),
                        current.purgeableRetiredStreamIds(),
                        current.retiredStreamMappings(), current.retiredMappingKeys(),
                        new PartitionMetadataWrite(OptionalLong.of(
                            existing.version().versionId())));
                    return ownershipVerifier.get().thenApply(ignored -> acknowledged);
                }

                long streamId = current.streamId() >= 0
                    ? current.streamId() : mappedStreamId.orElse(-1L);
                long expectedMappingStreamId = mappingStreamIdForTombstone(
                    current, mappingKey, mappedStreamId);
                Set<Long> retiredStreamIds = retiredStreamIdsWith(
                    current.retiredStreamIds(), streamId,
                    mappedStreamId.orElse(-1L));
                Set<Long> purgeableRetiredStreamIds =
                    current.purgeableRetiredStreamIds();
                if (completed.config().purgeRequested()) {
                    purgeableRetiredStreamIds = purgeAllRetiredStreamIds(
                        purgeableRetiredStreamIds, retiredStreamIds);
                } else if (mappedStreamId.isPresent()
                        && !current.retiredStreamIds().contains(
                            mappedStreamId.getAsLong())
                        && (current.streamId() < 0
                            || current.streamId() != mappedStreamId.getAsLong())) {
                    purgeableRetiredStreamIds = retiredStreamIdsWith(
                        purgeableRetiredStreamIds, mappedStreamId.getAsLong());
                }
                Set<RetiredStreamMapping> retiredStreamMappings =
                    retiredStreamMappingsForTombstone(
                        current.retiredStreamMappings(), mappingKey,
                        completed.config().purgeRequested(),
                        expectedMappingStreamId);
                Set<String> retiredMappingKeys = retiredMappingKeysWith(
                    current.retiredMappingKeys(), mappingKey);
                LogMetadata tombstone = deletionMetadata(
                    streamId, current, retiredStreamIds,
                    purgeableRetiredStreamIds, retiredStreamMappings,
                    retiredMappingKeys);
                Set<Long> finalPurgeableRetiredStreamIds =
                    purgeableRetiredStreamIds;
                PartitionTombstone recovered = new PartitionTombstone(
                    streamId, retiredStreamIds, finalPurgeableRetiredStreamIds,
                    retiredStreamMappings, retiredMappingKeys,
                    new PartitionMetadataWrite(OptionalLong.of(
                        existing.version().versionId())));
                if (samePersistedRegistration(current, tombstone)) {
                    return ownershipVerifier.get().thenApply(ignored -> recovered);
                }
                return persistPartitionMetadata(
                    id, partitionIndex, tombstone,
                    Set.of(PutOption.IfVersionIdEquals(existing.version().versionId())),
                    ownershipVerifier,
                    write -> new PartitionTombstone(
                        streamId, retiredStreamIds,
                        finalPurgeableRetiredStreamIds, retiredStreamMappings,
                        retiredMappingKeys, write),
                    () -> recoverCompletedDroppedPartition(
                        id, partitionIndex, completed, mappedStreamId, retryState),
                    retryState);
            });
    }

    private CompletableFuture<Void> cleanupCompletedDroppedPartition(
            StreamIdentifier id, int partitionIndex,
            IndexedStreamConfigStore.CompletedDrop completed,
            PartitionTombstone tombstone) {
        return sweepRetiredAllocations(
            id, partitionIndex,
            () -> streamConfigStore.verifyCompletedDrop(id, completed))
            .thenApply(ignored -> null);
    }

    private CompletableFuture<OptionalLong> mappingForDrop(
            StreamIdentifier id, int partitionIndex,
            IndexedStreamConfigStore.DropClaim claim) {
        String mappingKey = dropMappingKey(id, partitionIndex, claim);
        return streamIdLookup.apply(mappingKey)
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
    }

    private String dropMappingKey(
            StreamIdentifier id, int partitionIndex,
            IndexedStreamConfigStore.DropClaim claim) {
        return dropMappingKey(id, partitionIndex, claim.config());
    }

    private String dropMappingKey(
            StreamIdentifier id, int partitionIndex,
            IndexedStreamConfigStore.StreamConfigData config) {
        if (config.creationKind().orElse(null)
                == IndexedStreamConfigStore.CreationKind.NATIVE_CREATE) {
            return nativePartitionAllocationKey(id, partitionIndex);
        }
        return catalogPaths.compactedReaderName(id, partitionIndex);
    }

    private CompletableFuture<PartitionTombstone> tombstoneDroppedPartition(
            StreamIdentifier id, int partitionIndex,
            IndexedStreamConfigStore.DropClaim claim,
            OptionalLong mappedStreamId, boolean purge) {
        return tombstoneDroppedPartition(
            id, partitionIndex, claim, mappedStreamId, purge,
            new PartitionMetadataRetry());
    }

    private CompletableFuture<PartitionTombstone> tombstoneDroppedPartition(
            StreamIdentifier id, int partitionIndex,
            IndexedStreamConfigStore.DropClaim claim,
            OptionalLong mappedStreamId,
            boolean purge,
            PartitionMetadataRetry retryState) {
        String path = catalogPaths.partitionMetadataPath(id, partitionIndex);
        String mappingKey = dropMappingKey(id, partitionIndex, claim);
        return streamConfigStore.verifyAbortingOwnership(id, claim)
            .thenCompose(ignored -> oxiaClient.get(path))
            .thenCompose(existing -> {
                if (existing == null) {
                    long streamId = mappedStreamId.orElse(-1L);
                    Set<Long> retiredStreamIds = retiredStreamIdsWith(Set.of(), streamId);
                    Set<Long> purgeableRetiredStreamIds =
                        purgeableRetiredStreamIdsForDrop(
                            Set.of(), Set.of(), retiredStreamIds, streamId,
                            mappedStreamId.orElse(-1L), purge);
                    Set<RetiredStreamMapping> retiredStreamMappings =
                        retiredStreamMappingsForTombstone(
                            Set.of(), mappingKey, purge,
                            mappedStreamId.orElse(-1L));
                    Set<String> retiredMappingKeys =
                        retiredMappingKeysWith(Set.of(), mappingKey);
                    LogMetadata tombstone = deletionMetadata(
                        streamId, claim, retiredStreamIds, purgeableRetiredStreamIds,
                        retiredStreamMappings, retiredMappingKeys);
                    return persistPartitionMetadata(
                        id, partitionIndex, tombstone,
                        Set.of(PutOption.IfRecordDoesNotExist),
                        () -> streamConfigStore.verifyAbortingOwnership(id, claim),
                        write -> new PartitionTombstone(
                            streamId, retiredStreamIds,
                            purgeableRetiredStreamIds, retiredStreamMappings,
                            retiredMappingKeys, write),
                        () -> tombstoneDroppedPartition(
                            id, partitionIndex, claim, mappedStreamId, purge, retryState),
                        retryState);
                }
                final LogMetadata current;
                try {
                    current = LOG_METADATA_SERDE.deserialize(path, existing.value());
                } catch (Exception e) {
                    return CompletableFuture.failedFuture(e);
                }
                long streamId = current.streamId() >= 0
                    ? current.streamId() : mappedStreamId.orElse(-1L);
                long expectedMappingStreamId = mappingStreamIdForTombstone(
                    current, mappingKey, mappedStreamId);
                Set<Long> retiredStreamIds = retiredStreamIdsWith(
                    current.retiredStreamIds(), streamId,
                    mappedStreamId.orElse(-1L));
                Set<Long> purgeableRetiredStreamIds =
                    purgeableRetiredStreamIdsForDrop(
                        current.purgeableRetiredStreamIds(), current.retiredStreamIds(),
                        retiredStreamIds, streamId, mappedStreamId.orElse(-1L), purge);
                Set<RetiredStreamMapping> retiredStreamMappings =
                    retiredStreamMappingsForTombstone(
                        current.retiredStreamMappings(), mappingKey, purge,
                        expectedMappingStreamId);
                Set<String> retiredMappingKeys = retiredMappingKeysWith(
                    current.retiredMappingKeys(), mappingKey);
                LogMetadata tombstone = deletionMetadata(
                    streamId, current, retiredStreamIds, purgeableRetiredStreamIds,
                    retiredStreamMappings, retiredMappingKeys);
                if (samePersistedRegistration(current, tombstone)) {
                    return streamConfigStore.verifyAbortingOwnership(id, claim)
                        .thenApply(ignored -> new PartitionTombstone(
                            streamId, retiredStreamIds,
                            purgeableRetiredStreamIds, retiredStreamMappings,
                            retiredMappingKeys,
                            new PartitionMetadataWrite(OptionalLong.of(
                                existing.version().versionId()))));
                }
                if (!metadataCanBeFencedByDeletion(current, claim.config())) {
                    return CompletableFuture.failedFuture(new AlreadyExistsException(
                        "Partition metadata belongs to a different stream lifecycle: "
                            + id.fullName() + "-partition-" + partitionIndex));
                }
                return persistPartitionMetadata(
                    id, partitionIndex, tombstone,
                    Set.of(PutOption.IfVersionIdEquals(existing.version().versionId())),
                    () -> streamConfigStore.verifyAbortingOwnership(id, claim),
                    write -> new PartitionTombstone(
                        streamId, retiredStreamIds,
                        purgeableRetiredStreamIds, retiredStreamMappings,
                        retiredMappingKeys, write),
                    () -> tombstoneDroppedPartition(
                        id, partitionIndex, claim, mappedStreamId, purge, retryState),
                    retryState);
            });
    }

    private static LogMetadata deletionMetadata(
            long streamId, IndexedStreamConfigStore.DropClaim claim,
            Set<Long> retiredStreamIds, Set<Long> purgeableRetiredStreamIds) {
        return deletionMetadata(
            streamId, claim.config(), retiredStreamIds, purgeableRetiredStreamIds);
    }

    private static LogMetadata deletionMetadata(
            long streamId, IndexedStreamConfigStore.DropClaim claim,
            Set<Long> retiredStreamIds, Set<Long> purgeableRetiredStreamIds,
            Set<RetiredStreamMapping> retiredStreamMappings,
            Set<String> retiredMappingKeys) {
        return deletionMetadata(
            streamId, claim.config(), retiredStreamIds, purgeableRetiredStreamIds,
            retiredStreamMappings, retiredMappingKeys);
    }

    private static LogMetadata deletionMetadata(
            long streamId, IndexedStreamConfigStore.StreamConfigData config,
            Set<Long> retiredStreamIds, Set<Long> purgeableRetiredStreamIds) {
        return deletionMetadata(
            streamId, config, retiredStreamIds, purgeableRetiredStreamIds,
            Set.of(), Set.of());
    }

    private static LogMetadata deletionMetadata(
            long streamId, IndexedStreamConfigStore.StreamConfigData config,
            Set<Long> retiredStreamIds, Set<Long> purgeableRetiredStreamIds,
            Set<RetiredStreamMapping> retiredStreamMappings,
            Set<String> retiredMappingKeys) {
        return deletionMetadata(
            streamId, deletionRegistrationIdentity(config), retiredStreamIds,
            purgeableRetiredStreamIds, retiredStreamMappings, retiredMappingKeys);
    }

    private static LogMetadata deletionMetadata(
            long streamId, LogMetadata registrationSource,
            Set<Long> retiredStreamIds, Set<Long> purgeableRetiredStreamIds,
            Set<RetiredStreamMapping> retiredStreamMappings,
            Set<String> retiredMappingKeys) {
        return deletionMetadata(
            streamId, deletionRegistrationIdentity(registrationSource), retiredStreamIds,
            purgeableRetiredStreamIds, retiredStreamMappings, retiredMappingKeys);
    }

    private static LogMetadata deletionMetadata(
            long streamId, DeletionRegistrationIdentity registration,
            Set<Long> retiredStreamIds, Set<Long> purgeableRetiredStreamIds,
            Set<RetiredStreamMapping> retiredStreamMappings,
            Set<String> retiredMappingKeys) {
        return new LogMetadata(
            streamId, Map.of(), OptionalLong.empty(),
            registration.incarnationId(), registration.ownerToken(),
            registration.ownerGeneration(), true,
            retiredStreamIdsWith(retiredStreamIds, streamId),
            purgeableRetiredStreamIds, retiredStreamMappings, retiredMappingKeys);
    }

    private static DeletionRegistrationIdentity deletionRegistrationIdentity(
            IndexedStreamConfigStore.StreamConfigData config) {
        if (config.incarnationId().isEmpty()) {
            return DeletionRegistrationIdentity.legacy();
        }
        boolean useMetadataSource = config.provisioningState()
            .retainsExternalDeletionSpec();
        Optional<String> ownerToken = useMetadataSource
            ? config.metadataSourceOwnerToken() : config.ownerToken();
        long ownerGeneration = useMetadataSource
            ? config.metadataSourceGeneration() : config.ownerGeneration();
        if (useMetadataSource && ownerToken.isEmpty()
                && ownerGeneration
                    == IndexedStreamConfigStore.LEGACY_METADATA_GENERATION) {
            return DeletionRegistrationIdentity.legacy();
        }
        if (ownerToken.isEmpty() || ownerGeneration < 0) {
            throw new IllegalStateException(
                "Stream config does not contain the durable metadata source owner");
        }
        return new DeletionRegistrationIdentity(
            config.incarnationId().orElseThrow(), ownerToken.orElseThrow(),
            ownerGeneration);
    }

    private static DeletionRegistrationIdentity deletionRegistrationIdentity(
            LogMetadata metadata) {
        if (!validRegistrationIdentity(metadata)) {
            throw new IllegalStateException(
                "Partition metadata contains a partial registration identity");
        }
        if (metadata.registrationIncarnationId() == null) {
            return DeletionRegistrationIdentity.legacy();
        }
        return new DeletionRegistrationIdentity(
            metadata.registrationIncarnationId(), metadata.registrationOwnerToken(),
            metadata.registrationOwnerGeneration());
    }

    private static Set<Long> purgeableRetiredStreamIdsForDrop(
            Set<Long> currentPurgeable, Set<Long> currentRetired,
            Set<Long> retiredStreamIds, long primaryStreamId,
            long mappedStreamId, boolean purge) {
        if (purge) {
            return purgeAllRetiredStreamIds(currentPurgeable, retiredStreamIds);
        }
        if (mappedStreamId >= 0 && mappedStreamId != primaryStreamId
                && !currentRetired.contains(mappedStreamId)) {
            return retiredStreamIdsWith(currentPurgeable, mappedStreamId);
        }
        return currentPurgeable;
    }

    private CompletableFuture<Void> cleanupDroppedPartition(
            StreamIdentifier id, int partitionIndex,
            IndexedStreamConfigStore.DropClaim claim,
            PartitionTombstone tombstone) {
        return sweepRetiredAllocations(
            id, partitionIndex,
            () -> streamConfigStore.verifyAbortingOwnership(id, claim))
            .thenApply(ignored -> null);
    }

    private record PartitionTombstone(
            long streamId, Set<Long> retiredStreamIds,
            Set<Long> purgeableRetiredStreamIds,
            Set<RetiredStreamMapping> retiredStreamMappings,
            Set<String> retiredMappingKeys,
            PartitionMetadataWrite write) {
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
        return streamConfigStore.readActive(id).thenCompose(active -> {
            List<CompletableFuture<LogId>> futures = new ArrayList<>();
            for (int i = 0; i < active.config().partitions(); i++) {
                final int partIdx = i;
                futures.add(readPartitionMetadata(id, partIdx, active.config())
                    .thenApply(meta -> LogId.of(meta.metadata().streamId())));
            }
            return CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]))
                .thenCompose(ignored -> streamConfigStore.verifyActiveOwnership(id, active))
                .thenApply(ignored -> new IndexedLayout(futures.stream()
                    .map(CompletableFuture::join)
                    .toList()));
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
        return streamConfigStore.readActive(id).thenCompose(active ->
            getLayoutTolerant(id, active));
    }

    private CompletableFuture<StreamLayout> getLayoutTolerant(
            StreamIdentifier id, IndexedStreamConfigStore.ActiveStreamConfig active) {
        List<CompletableFuture<LogId>> futures = new ArrayList<>();
        for (int i = 0; i < active.config().partitions(); i++) {
            final int partIdx = i;
            futures.add(readPartitionMetadata(id, partIdx, active.config())
                .handle((meta, ex) -> {
                    if (ex != null) {
                        if (unwrap(ex) instanceof NoSuchStreamException) {
                            log.debug("Partition {} of {} not yet registered; using a placeholder in "
                                + "the materialization layout", partIdx, id.fullName());
                            return UNREGISTERED_PARTITION;
                        }
                        throw new CompletionException(unwrap(ex));
                    }
                    return LogId.of(meta.metadata().streamId());
                }));
        }
        return CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]))
            .thenCompose(ignored -> streamConfigStore.verifyActiveOwnership(id, active))
            .thenApply(ignored -> new IndexedLayout(futures.stream()
                .map(CompletableFuture::join)
                .toList()));
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
        return streamConfigStore.readActive(id).thenCompose(active -> {
            CompletableFuture<Void> chain = CompletableFuture.completedFuture(null);
            for (int i = 0; i < active.config().partitions(); i++) {
                int partitionIndex = i;
                chain = chain.thenCompose(ignored ->
                    sealPartition(id, partitionIndex, active));
            }
            return chain.thenCompose(ignored ->
                streamConfigStore.verifyActiveOwnership(id, active));
        });
    }

    private CompletableFuture<Void> sealPartition(
            StreamIdentifier id, int partitionIndex,
            IndexedStreamConfigStore.ActiveStreamConfig active) {
        return sealPartition(
            id, partitionIndex, active, new PartitionMetadataRetry());
    }

    private CompletableFuture<Void> sealPartition(
            StreamIdentifier id, int partitionIndex,
            IndexedStreamConfigStore.ActiveStreamConfig active,
            PartitionMetadataRetry retryState) {
        String path = catalogPaths.partitionMetadataPath(id, partitionIndex);
        return streamConfigStore.verifyActiveOwnership(id, active)
            .thenCompose(ignored ->
                readPartitionMetadata(id, partitionIndex, active.config()))
            .thenCompose(current -> {
                LogMetadata metadata = current.metadata();
                LogMetadata sealed = new LogMetadata(
                    metadata.streamId(), metadata.properties(), OptionalLong.of(0),
                    metadata.registrationIncarnationId(),
                    metadata.registrationOwnerToken(),
                    metadata.registrationOwnerGeneration(), false,
                    metadata.retiredStreamIds(), metadata.purgeableRetiredStreamIds(),
                    metadata.retiredStreamMappings(), metadata.retiredMappingKeys());
                final byte[] bytes;
                try {
                    bytes = LOG_METADATA_SERDE.serialize(path, sealed);
                } catch (Exception e) {
                    return CompletableFuture.failedFuture(e);
                }
                return oxiaClient.put(path, bytes,
                        Set.of(PutOption.IfVersionIdEquals(current.versionId())))
                    .handle((result, failure) -> new PartitionWriteOutcome(
                        result, unwrapNullable(failure)))
                    .thenCompose(outcome -> {
                        if (outcome.failure() == null) {
                            return streamConfigStore.verifyActiveOwnership(id, active);
                        }
                        if (outcome.failure() instanceof UnexpectedVersionIdException) {
                            return retryPartitionMetadataWrite(
                                id, partitionIndex, outcome.failure(),
                                () -> sealPartition(
                                    id, partitionIndex, active, retryState),
                                retryState);
                        }
                        return readPartitionMetadata(id, partitionIndex, active.config())
                            .handle((readback, readFailure) ->
                                new VersionedMetadataRead(
                                    readback, unwrapNullable(readFailure)))
                            .thenCompose(readback -> {
                                if (readback.failure() != null) {
                                    outcome.failure().addSuppressed(readback.failure());
                                    return CompletableFuture.failedFuture(outcome.failure());
                                }
                                LogMetadata persisted = readback.metadata().metadata();
                                if (!samePersistedRegistration(persisted, sealed)
                                        || persisted.terminatedOffset().orElse(-1L) != 0L) {
                                    return CompletableFuture.failedFuture(outcome.failure());
                                }
                                return streamConfigStore.verifyActiveOwnership(id, active);
                            });
                    });
            });
    }

    private record VersionedMetadataRead(
            VersionedLogMetadata metadata, Throwable failure) {
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
            // This raw scan intentionally includes hidden PROVISIONING and ABORTING configs. It
            // does not fence a claim written after the scan; closing that cross-key race requires
            // a separate namespace reservation/epoch protocol.
            return streamConfigStore.namespaceContainsNonTombstoneStream(namespaceName)
                .thenCompose(nonEmpty -> {
                if (nonEmpty) {
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
        return streamConfigStore.readActive(id).thenCompose(active ->
            readPartitionMetadata(id, partitionIndex, active.config())
                .thenCompose(metadata ->
                    streamConfigStore.verifyActiveOwnership(id, active)
                        .thenApply(ignored -> metadata.metadata())));
    }

    private CompletableFuture<VersionedLogMetadata> readPartitionMetadata(
            StreamIdentifier id, int partitionIndex,
            IndexedStreamConfigStore.StreamConfigData config) {
        String path = catalogPaths.partitionMetadataPath(id, partitionIndex);
        return oxiaClient.get(path).thenApply(result -> {
            if (result == null) {
                throw new NoSuchStreamException(id);
            }
            final LogMetadata metadata;
            try {
                metadata = LOG_METADATA_SERDE.deserialize(path, result.value());
            } catch (Exception e) {
                throw new RuntimeException("Failed to deserialize partition metadata: " + path, e);
            }
            if (metadata.deleted()) {
                throw new PartitionMetadataFenceViolationException(
                    id, partitionIndex, "metadata is deleted");
            }
            if (!metadataMatchesActiveConfig(metadata, config)) {
                throw new PartitionMetadataFenceViolationException(
                    id, partitionIndex, "metadata belongs to a different stream lifecycle");
            }
            return new VersionedLogMetadata(
                metadata, result.version().versionId());
        });
    }

    static final class PartitionMetadataFenceViolationException
            extends PartitionLifecycleFencedException {

        private PartitionMetadataFenceViolationException(
                StreamIdentifier id, int partitionIndex, String reason) {
            super(id, partitionIndex, reason);
        }
    }

    private static boolean metadataMatchesActiveConfig(
            LogMetadata metadata, IndexedStreamConfigStore.StreamConfigData config) {
        if (!validRegistrationIdentity(metadata)) {
            return false;
        }
        if (config.incarnationId().isEmpty() || config.ownerToken().isEmpty()) {
            return config.incarnationId().isEmpty()
                && config.ownerToken().isEmpty()
                && metadata.registrationIncarnationId() == null
                && metadata.registrationOwnerToken() == null
                && metadata.registrationOwnerGeneration() == null;
        }
        return config.incarnationId().orElseThrow()
                .equals(metadata.registrationIncarnationId())
            && config.ownerToken().orElseThrow()
                .equals(metadata.registrationOwnerToken())
            && Objects.equals(config.ownerGeneration(),
                metadata.registrationOwnerGeneration());
    }

    private static boolean metadataCanBeFencedByDeletion(
            LogMetadata metadata, IndexedStreamConfigStore.StreamConfigData config) {
        if (!validRegistrationIdentity(metadata)) {
            return false;
        }
        if (metadata.deleted()) {
            // A persisted tombstone is its own durable cleanup descriptor. Preserve its
            // registration identity rather than retagging it to a later config claim.
            return true;
        }
        if (metadata.registrationOwnerGeneration() == null) {
            return config.incarnationId().isEmpty()
                || config.provisioningState().retainsExternalDeletionSpec()
                    && config.metadataSourceOwnerToken().isEmpty()
                    && config.metadataSourceGeneration()
                        == IndexedStreamConfigStore.LEGACY_METADATA_GENERATION;
        }
        if (config.incarnationId().isEmpty()) {
            return false;
        }
        if (!config.incarnationId().orElseThrow()
                .equals(metadata.registrationIncarnationId())) {
            return false;
        }
        if (config.provisioningState().retainsExternalDeletionSpec()) {
            return Objects.equals(
                    metadata.registrationOwnerToken(),
                    config.metadataSourceOwnerToken().orElse(null))
                && metadata.registrationOwnerGeneration()
                    == config.metadataSourceGeneration();
        }
        return Objects.equals(
                metadata.registrationOwnerToken(), config.ownerToken().orElse(null))
            && metadata.registrationOwnerGeneration() == config.ownerGeneration();
    }

    @Nullable
    private UnsupportedOperationException destructiveKeyedLifecycleCapabilityFailure(
            String operation) {
        return fencedMappingStorage == null
            ? new UnsupportedOperationException(
                operation + " requires durable fenced stream-ID lifecycle support")
            : null;
    }

    private record VersionedLogMetadata(LogMetadata metadata, long versionId) {
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
