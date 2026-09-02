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
import io.lakestream.api.LifecycleState;
import io.lakestream.api.Log;
import io.lakestream.api.LogId;
import io.lakestream.api.LogStateManager;
import io.lakestream.api.LogStorage;
import io.lakestream.api.Namespace;
import io.lakestream.api.Partitioning;
import io.lakestream.api.PartitioningStrategy;
import io.lakestream.api.SchemaConfig;
import io.lakestream.api.StreamCatalog;
import io.lakestream.api.StreamCatalogEntry;
import io.lakestream.api.StreamConfig;
import io.lakestream.api.StreamIdentifier;
import io.lakestream.api.StreamLayout;
import io.lakestream.api.StreamMetadata;
import io.lakestream.api.StreamReader;
import io.lakestream.api.StreamWriter;
import io.lakestream.api.exception.AlreadyExistsException;
import io.lakestream.api.exception.NamespaceNotEmptyException;
import io.lakestream.api.exception.NoSuchNamespaceException;
import io.lakestream.api.exception.NoSuchStreamException;
import io.lakestream.api.exception.PartitionLifecycleFencedException;
import io.lakestream.api.materialization.ResolvedMaterialization;
import io.lakestream.api.materialization.TableCatalog;
import io.lakestream.api.materialization.TableMaterializationPolicy;
import io.lakestream.ursa.catalog.metadata.LogMetadata;
import io.lakestream.ursa.catalog.metadata.LogMetadata.RetiredStreamMapping;
import io.lakestream.ursa.catalog.metadata.LogMetadataSerde;
import io.lakestream.ursa.lakestream.impl.materialization.MaterializationJson;
import io.lakestream.ursa.lakestream.reader.CompactedObjectReader;
import io.lakestream.ursa.lakestream.reader.CompactedObjectReaderFactory;
import io.lakestream.ursa.storage.OwnedResultFutures;
import io.lakestream.ursa.storage.StorageApi;
import io.lakestream.ursa.storage.StorageApi.ActiveStreamIdMapping;
import io.lakestream.ursa.storage.StorageApi.KeyedAllocationInvalidatedException;
import io.lakestream.ursa.storage.StorageApi.StreamIdAllocation;
import io.lakestream.ursa.storage.StorageApi.StreamIdMappingFence;
import io.lakestream.ursa.storage.StorageApi.StreamIdMappingFenceResult;
import io.lakestream.ursa.storage.StorageApi.StreamIdMappingOwner;
import io.lakestream.ursa.storage.StorageApi.StreamWriteLease;
import io.lakestream.ursa.storage.impl.EntryIndexCache;
import io.lakestream.ursa.storage.impl.exception.NoSuchKeyException;
import io.oxia.client.api.AsyncOxiaClient;
import io.oxia.client.api.GetResult;
import io.oxia.client.api.PutResult;
import io.oxia.client.api.exceptions.KeyAlreadyExistsException;
import io.oxia.client.api.exceptions.UnexpectedVersionIdException;
import io.oxia.client.api.options.PutOption;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
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
 * <p>{@code createStream()} and {@code loadStream()} return metadata snapshots. Data-plane
 * resources are opened explicitly through {@link #openLog}, {@link #openReader}, or
 * {@link #openWriter}.
 */
@Slf4j
public class IndexedStreamCatalog implements StreamCatalog {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final LogMetadataSerde LOG_METADATA_SERDE = LogMetadataSerde.INSTANCE;
    private static final int STREAM_VISIBILITY_READ_BATCH_SIZE = 32;
    private static final int MAX_PARTITION_METADATA_WRITE_RETRIES = 3;
    private static final int PROVISIONING_PARALLELISM = 8;
    private static final long PARTITION_METADATA_RETRY_DELAY_MILLIS = 10L;
    private static final long CATALOG_CLOSE_TIMEOUT_MILLIS = TimeUnit.SECONDS.toMillis(10);
    private static final long ABANDONED_HANDLE_INITIAL_RETRY_MILLIS = 100L;
    private static final long ABANDONED_HANDLE_MAX_RETRY_MILLIS =
        TimeUnit.SECONDS.toMillis(10);
    private static final int FAILED_OPEN_CLEANUP_QUEUE_CAPACITY = 64;
    private static final int DELEGATE_CLOSE_THREADS = 2;

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
    private final StorageApi writeLeaseStorage;
    @Nullable
    private final CompactedObjectReaderFactory readerFactory;
    @Nullable
    private final EntryIndexCache entryIndexCache;
    private final List<AutoCloseable> ownedResources;
    private final ExecutorService failedOpenCleanupExecutor;
    private final ExecutorService delegateCloseExecutor;
    private final Object openLifecycleMutex = new Object();
    private final Set<CompletableFuture<Void>> pendingOpenCleanups = new HashSet<>();
    private int activeOpenAttempts;
    private int activeDataPlaneHandles;
    private boolean catalogClosing;
    private boolean resourcesClosed;
    private volatile String catalogName;
    /**
     * Cluster-wide default materialization policy — the lowest-priority baseline used by
     * {@link #resolveMaterialization(StreamIdentifier)} when a stream has neither its own nor a
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
            false, null, null, ownedResources);
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
            null, readerFactory, entryIndexCache, null, null, false, null, null, ownedResources);
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
            false, null, null, ownedResources);
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
            ignoredSupportsKeyedLifecycle, null, null, ownedResources);
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
            storageApi.supportsDurableStreamWriteFencing() ? storageApi : null,
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
                                @Nullable StorageApi writeLeaseStorage,
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
        this.writeLeaseStorage = writeLeaseStorage;
        this.readerFactory = readerFactory;
        this.entryIndexCache = entryIndexCache;
        this.ownedResources = ownedResources != null ? ownedResources : List.of();
        this.failedOpenCleanupExecutor = new ThreadPoolExecutor(
            1,
            1,
            0L,
            TimeUnit.MILLISECONDS,
            new ArrayBlockingQueue<>(FAILED_OPEN_CLEANUP_QUEUE_CAPACITY),
            task -> {
                Thread thread = new Thread(task, "lakestream-failed-open-cleanup");
                thread.setDaemon(true);
                thread.setContextClassLoader(IndexedStreamCatalog.class.getClassLoader());
                return thread;
            },
            new ThreadPoolExecutor.AbortPolicy());
        AtomicInteger closeThreadId = new AtomicInteger();
        this.delegateCloseExecutor = new ThreadPoolExecutor(
            DELEGATE_CLOSE_THREADS,
            DELEGATE_CLOSE_THREADS,
            0L,
            TimeUnit.MILLISECONDS,
            // Unbounded on purpose: a bulk drop queues one small close task per partition, and a
            // bounded queue turned that into rejection + exponential backoff (minutes for a
            // thousand partitions). Rejections now only happen after shutdown.
            new LinkedBlockingQueue<>(),
            task -> {
                Thread thread = new Thread(task,
                    "lakestream-log-close-" + closeThreadId.incrementAndGet());
                thread.setDaemon(true);
                thread.setContextClassLoader(IndexedStreamCatalog.class.getClassLoader());
                return thread;
            },
            new ThreadPoolExecutor.AbortPolicy());
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
    Log createLog(LogId logId) {
        return logFactory.apply(logId);
    }

    /**
     * Creates a named log with a dedicated compacted-object reader.
     *
     * <p>Ownership of the reader transfers to the returned log. If log creation fails, this method
     * closes the reader before propagating the failure.
     */
    Log createLog(String name, LogId logId) {
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
     * @param name compacted-object reader name for the log
     * @param logId log identifier
     * @param reader non-null compacted-object reader whose ownership is transferred
     * @return the created log
     * @throws NullPointerException if {@code reader} is null
     */
    Log createLog(String name, LogId logId, CompactedObjectReader reader) {
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
    public CompletableFuture<StreamMetadata> createStream(
            StreamIdentifier id, StreamConfig config, Partitioning partitioning,
            SchemaConfig schema, Map<String, String> properties) {
        return createStream(id, config, partitioning, schema, properties, Optional.empty());
    }

    @Override
    public CompletableFuture<StreamMetadata> createStream(
            StreamIdentifier id, StreamConfig config, Partitioning partitioning,
            SchemaConfig schema, Map<String, String> properties,
            Optional<TableMaterializationPolicy> materialization) {
        Objects.requireNonNull(partitioning, "partitioning");
        if (partitioning.strategy() != PartitioningStrategy.INDEXED) {
            throw new IllegalArgumentException(
                "IndexedStreamCatalog only supports INDEXED partitioning");
        }
        int numPartitions = partitioning.numPartitions();
        String ownerToken = UUID.randomUUID().toString();
        return preflightRetiredPartitionJournals(id, numPartitions, "Stream creation")
            .thenCompose(ignored -> streamConfigStore.claimCreation(
                id, config, partitioning, schema, properties, materialization,
                IndexedStreamConfigStore.CreationKind.NATIVE_CREATE, ownerToken))
            .thenCompose(claim -> createPartitions(id, claim)
                .thenCompose(ignored -> streamConfigStore.verifyProvisioningOwnership(id, claim))
                .thenCompose(ignored -> streamConfigStore.finalizeCreation(id, claim))
                .thenCompose(outcome -> outcome.active()
                    ? loadStream(id)
                    : CompletableFuture.failedFuture(outcome.failure())));
    }

    private CompletableFuture<Void> createPartitions(
            StreamIdentifier id, IndexedStreamConfigStore.ProvisioningClaim claim) {
        return BoundedParallel.forEach(claim.config().partitions(), PROVISIONING_PARALLELISM,
            partIdx -> provisionNativePartition(id, claim, partIdx));
    }

    private CompletableFuture<Void> provisionNativePartition(
            StreamIdentifier id, IndexedStreamConfigStore.ProvisioningClaim claim,
            int partIdx) {
        String allocationKey = nativePartitionAllocationKey(id, partIdx);
        return streamConfigStore.verifyProvisioningOwnership(id, claim)
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

    private CompletableFuture<Void> createExpansionPartitions(
            StreamIdentifier id, IndexedStreamConfigStore.ExpansionClaim claim) {
        return BoundedParallel.forEach(
            claim.targetPartitions() - claim.basePartitions(), PROVISIONING_PARALLELISM,
            i -> expandPartition(id, claim, claim.basePartitions() + i));
    }

    private CompletableFuture<Void> expandPartition(
            StreamIdentifier id, IndexedStreamConfigStore.ExpansionClaim claim,
            int partitionIndex) {
        String allocationKey = nativePartitionAllocationKey(id, partitionIndex);
        return streamConfigStore.verifyExpansion(id, claim)
            .thenCompose(ignored -> prepareRetiredNativePartition(
                id, partitionIndex, streamIdMappingOwner(claim),
                () -> streamConfigStore.verifyExpansion(id, claim)))
            .thenCompose(prepared -> allocateNativeKeyedStreamId(
                allocationKey, streamIdMappingOwner(claim),
                prepared.acknowledgedFence())
                .thenApply(streamId -> {
                    if (prepared.retiredStreamIds().contains(streamId)) {
                        throw new RetiredStreamIdAllocationException(
                            new StreamIdAllocation(streamId, false),
                            "Native partition " + id.fullName() + "-partition-"
                                + partitionIndex
                                + " cannot reuse a retired physical stream ID");
                    }
                    return streamId;
                })
                .handle((streamId, failure) -> new NativeAllocationAttempt(
                    streamId, unwrapNullable(failure))))
            .thenCompose(attempt -> {
                if (attempt.failure() == null) {
                    return verifyAndWriteExpansionPartitionAfterAllocation(
                        id, partitionIndex, allocationKey,
                        attempt.streamId(), claim);
                }
                Throwable cause = rootCause(attempt.failure());
                IndexedStreamConfigStore.ProvisioningClaim cleanupClaim =
                    expansionCleanupClaim(claim);
                if (cause instanceof KeyedAllocationInvalidatedException invalidated) {
                    return compensateRejectedNativeAllocation(
                        id, partitionIndex, allocationKey,
                        invalidated.allocation().streamId(), cleanupClaim, cause);
                }
                if (cause instanceof RetiredStreamIdAllocationException retired) {
                    return compensateRetiredNativeAllocation(
                        id, partitionIndex, allocationKey,
                        retired.allocation().streamId(), cleanupClaim, cause);
                }
                return CompletableFuture.failedFuture(cause);
            })
            .thenCompose(ignored -> streamConfigStore.verifyExpansion(id, claim));
    }

    private CompletableFuture<Void> verifyAndWriteExpansionPartitionAfterAllocation(
            StreamIdentifier id, int partitionIndex, String allocationKey,
            long streamId, IndexedStreamConfigStore.ExpansionClaim claim) {
        return streamConfigStore.verifyExpansion(id, claim)
            .handle((ignored, failure) -> unwrapNullable(failure))
            .thenCompose(failure -> {
                if (failure == null) {
                    return writePartitionMetadataForExpansion(
                        id, partitionIndex, streamId, claim).thenApply(ignored -> null);
                }
                Throwable cause = rootCause(failure);
                if (!(cause instanceof IndexedStreamConfigStore
                        .ExpansionOwnershipLostException)) {
                    return CompletableFuture.failedFuture(cause);
                }
                return compensateRejectedNativeAllocation(
                    id, partitionIndex, allocationKey, streamId,
                    expansionCleanupClaim(claim), cause);
            });
    }

    private static IndexedStreamConfigStore.ProvisioningClaim expansionCleanupClaim(
            IndexedStreamConfigStore.ExpansionClaim claim) {
        IndexedStreamConfigStore.StreamConfigData config = claim.config();
        return new IndexedStreamConfigStore.ProvisioningClaim(
            config, config.incarnationId().orElseThrow(),
            config.ownerToken().orElseThrow(),
            IndexedStreamConfigStore.CreationKind.NATIVE_CREATE,
            config.ownerGeneration(), claim.versionId());
    }

    private CompletableFuture<PreparedNativePartition> prepareRetiredNativePartition(
            StreamIdentifier id, int partitionIndex,
            IndexedStreamConfigStore.ProvisioningClaim claim) {
        return prepareRetiredNativePartition(
            id, partitionIndex, streamIdMappingOwner(claim),
            () -> streamConfigStore.verifyProvisioningOwnership(id, claim));
    }

    private CompletableFuture<PreparedNativePartition> prepareRetiredNativePartition(
            StreamIdentifier id, int partitionIndex,
            StreamIdMappingOwner mappingOwner,
            Supplier<CompletableFuture<Void>> ownershipVerifier) {
        String path = catalogPaths.partitionMetadataPath(id, partitionIndex);
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
                        mappingOwner, OptionalLong.empty()))
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
            StreamIdentifier id, int partitionIndex) {
        return "lakestream-native/" + id.fullName()
            + "/partition-" + partitionIndex;
    }

    private CompletableFuture<Long> allocateNativeKeyedStreamId(
            String logName, StreamIdMappingOwner owner,
            Optional<StreamIdMappingFence> acknowledgedFence) {
        if (fencedMappingStorage != null) {
            return fencedMappingStorage.allocateStreamId(
                    logName, owner, acknowledgedFence)
                .thenApply(StreamIdAllocation::streamId);
        }
        return keyedStreamIdAllocator != null
            ? keyedStreamIdAllocator.apply(logName).thenApply(StreamIdAllocation::streamId)
            : streamIdGenerator.apply(Optional.of(logName));
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
        if (fencedMappingStorage == null || writeLeaseStorage == null) {
            return CompletableFuture.failedFuture(new UnsupportedOperationException(
                "Retired allocation cleanup requires durable stream-ID mapping and write "
                    + "fencing support"));
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
        StorageApi writeFencingStorage = Objects.requireNonNull(
            writeLeaseStorage, "writeLeaseStorage");
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
        Set<Long> retiredStreamIds = new TreeSet<>(metadata.retiredStreamIds());
        retiredStreamIds.addAll(metadata.purgeableRetiredStreamIds());
        if (metadata.deleted() && metadata.streamId() >= 0) {
            retiredStreamIds.add(metadata.streamId());
        }
        for (long streamId : retiredStreamIds) {
            if (preservedActiveStreamIds.contains(streamId)
                    || !metadata.deleted() && metadata.streamId() == streamId) {
                continue;
            }
            cleanup = cleanup
                .thenCompose(ignored -> ownershipVerifier.get())
                .thenCompose(ignored ->
                    writeFencingStorage.fenceAndDrainStreamWrites(streamId));
            if (metadata.purgeableRetiredStreamIds().contains(streamId)) {
                cleanup = cleanup
                    .thenCompose(ignored -> ownershipVerifier.get())
                    .thenCompose(ignored -> logStorage.deleteLog(LogId.of(streamId)));
            }
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
        long acknowledgedStreamId = current.deleted()
                && current.purgeableRetiredStreamIds().contains(current.streamId())
            ? -1L : current.streamId();
        LogMetadata acknowledged = new LogMetadata(
            acknowledgedStreamId, current.properties(), current.terminatedOffset(),
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

    private record NativeAllocationAttempt(
            @Nullable Long streamId, @Nullable Throwable failure) {
    }

    private record PreparedNativePartition(
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
            claim.ownerGeneration(),
            () -> streamConfigStore.verifyProvisioningOwnership(id, claim));
    }

    private CompletableFuture<PartitionMetadataWrite> writePartitionMetadataForExpansion(
            StreamIdentifier id, int partitionIndex, long streamId,
            IndexedStreamConfigStore.ExpansionClaim claim) {
        IndexedStreamConfigStore.StreamConfigData config = claim.config();
        return writePartitionMetadataForRegistration(
            id, partitionIndex, streamId,
            config.incarnationId(), config.ownerToken(), config.ownerGeneration(),
            () -> streamConfigStore.verifyExpansion(id, claim));
    }

    private CompletableFuture<PartitionMetadataWrite> writePartitionMetadataForRegistration(
            StreamIdentifier id, int partitionIndex, long streamId,
            Optional<String> incarnationId, Optional<String> ownerToken,
            long ownerGeneration,
            Supplier<CompletableFuture<Void>> ownershipVerifier) {
        return writePartitionMetadataForRegistration(
            id, partitionIndex, streamId, incarnationId, ownerToken,
            ownerGeneration,
            ownershipVerifier, new PartitionMetadataRetry());
    }

    private CompletableFuture<PartitionMetadataWrite> writePartitionMetadataForRegistration(
            StreamIdentifier id, int partitionIndex, long streamId,
            Optional<String> incarnationId, Optional<String> ownerToken,
            long ownerGeneration,
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
                            ownerGeneration,
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
                if (currentLegacy && !current.deleted()) {
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
                    return replacePartitionMetadata(
                        id, partitionIndex,
                        registrationMetadata(
                            streamId, incarnationId, ownerToken, ownerGeneration,
                            currentRetiredStreamIds, currentPurgeableRetiredStreamIds,
                            current.retiredStreamMappings(), current.retiredMappingKeys()),
                        existing.version().versionId(), ownershipVerifier,
                        () -> writePartitionMetadataForRegistration(
                            id, partitionIndex, streamId, incarnationId, ownerToken,
                            ownerGeneration,
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
                if (!current.deleted() && !sameStreamId) {
                    return partitionMetadataConflict(
                        id, partitionIndex, streamId, current.streamId(),
                        "native takeover must reuse its stable stream ID");
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
                        ownerGeneration,
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
            IndexedStreamConfigStore.ExpansionClaim claim) {
        IndexedStreamConfigStore.StreamConfigData config = claim.config();
        return new StreamIdMappingOwner(
            config.incarnationId().orElseThrow(),
            config.ownerToken().orElseThrow(), config.ownerGeneration());
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
    public CompletableFuture<StreamMetadata> loadStream(StreamIdentifier id) {
        return streamConfigStore.readActive(id).thenCompose(active ->
            buildStreamMetadata(id, active));
    }

    @Override
    public CompletableFuture<StreamMetadata> increasePartitions(
            StreamIdentifier id, int targetPartitionCount) {
        return streamConfigStore.claimExpansion(id, targetPartitionCount)
            .thenCompose(claim -> {
                if (!claim.requiresExpansion()) {
                    return buildStreamMetadata(id,
                        new IndexedStreamConfigStore.ActiveStreamConfig(
                            claim.config(), claim.versionId()));
                }
                return createExpansionPartitions(id, claim)
                    .thenCompose(ignored ->
                        streamConfigStore.verifyExpansion(id, claim))
                    .thenCompose(ignored ->
                        streamConfigStore.finalizeExpansion(id, claim))
                    .thenCompose(finalization -> finalization.complete()
                        ? loadStream(id)
                        : increasePartitions(id, finalization.targetPartitions()));
            });
    }

    @Override
    public CompletableFuture<StreamMetadata> replaceStreamProperties(
            StreamIdentifier id, Map<String, String> properties, long sourceRevision) {
        return streamConfigStore.replaceProperties(id, properties, sourceRevision)
            .thenCompose(active -> buildStreamMetadata(id, active));
    }

    @Override
    public CompletableFuture<Optional<ResolvedMaterialization>> resolveMaterialization(
            StreamIdentifier id) {
        return loadStream(id).thenCompose(metadata ->
            loadNamespaceMetadata(id.namespace())
                .handle((namespace, failure) -> {
                    if (failure == null) {
                        return namespace.materialization();
                    }
                    Throwable cause = rootCause(failure);
                    if (cause instanceof NoSuchNamespaceException) {
                        return Optional.<TableMaterializationPolicy>empty();
                    }
                    throw new CompletionException(cause);
                })
                .thenCompose(namespacePolicy -> {
                    Optional<TableMaterializationPolicy> baseline = namespacePolicy.isPresent()
                        ? namespacePolicy : clusterDefaultMaterialization();
                    Optional<String> catalogRef = metadata.materialization()
                        .flatMap(TableMaterializationPolicy::catalogRef)
                        .or(() -> baseline.flatMap(TableMaterializationPolicy::catalogRef));
                    if (catalogRef.isEmpty()) {
                        return CompletableFuture.completedFuture(Optional.empty());
                    }
                    String expectedCatalog = catalogRef.orElseThrow();
                    return getTableCatalog(expectedCatalog)
                        .thenApply(Optional::ofNullable)
                        .thenApply(tableCatalog -> TableMaterializationPolicy.resolve(
                            baseline, metadata.materialization(), id,
                            name -> expectedCatalog.equals(name)
                                ? tableCatalog : Optional.empty(),
                            metadata.properties()));
                }));
    }

    private CompletableFuture<StreamMetadata> buildStreamMetadata(
            StreamIdentifier id,
            IndexedStreamConfigStore.ActiveStreamConfig active) {
        return readCommittedLayout(id, active).thenApply(snapshot -> {
            IndexedStreamConfigStore.StreamConfigData config = snapshot.active().config();
            IndexedStreamConfigStore.ImmutableStreamDefinition definition =
                config.definition();
            return new StreamMetadata(
                id, definition.streamConfig(), definition.partitioning(), definition.schema(),
                config.properties(), config.materialization(), LifecycleState.ACTIVE,
                snapshot.layout(), snapshot.active().versionId());
        });
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

    @Override
    public CompletableFuture<List<StreamCatalogEntry>> listStreamEntries(String namespaceName) {
        return streamConfigStore.listStreamEntries(namespaceName);
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
        return BoundedParallel.forEach(claim.config().partitions(), PROVISIONING_PARALLELISM,
            partitionIndex -> cleanupDroppedPartitionChain(id, claim, purge, partitionIndex));
    }

    private CompletableFuture<Void> cleanupDroppedPartitionChain(
            StreamIdentifier id, IndexedStreamConfigStore.DropClaim claim,
            boolean purge, int partitionIndex) {
        return streamConfigStore.verifyAbortingOwnership(id, claim)
            .thenCompose(ignored -> mappingForPartitionDrop(id, partitionIndex))
            .thenCompose(mapping -> tombstoneDroppedPartition(
                id, partitionIndex, claim, mapping, purge)
                .thenCompose(tombstone -> sweepPartitionAfterTombstone(
                    id, partitionIndex,
                    () -> streamConfigStore.verifyAbortingOwnership(id, claim))));
    }

    private CompletableFuture<Void> cleanupCompletedDrop(
            StreamIdentifier id, IndexedStreamConfigStore.CompletedDrop completed) {
        return BoundedParallel.forEach(completed.config().partitions(), PROVISIONING_PARALLELISM,
            partitionIndex ->
                recoverCompletedDroppedPartitionChain(id, completed, partitionIndex));
    }

    private CompletableFuture<Void> recoverCompletedDroppedPartitionChain(
            StreamIdentifier id, IndexedStreamConfigStore.CompletedDrop completed,
            int partitionIndex) {
        return streamConfigStore.verifyCompletedDrop(id, completed)
            .thenCompose(ignored -> mappingForPartitionDrop(id, partitionIndex))
            .thenCompose(mapping -> recoverCompletedDroppedPartition(
                id, partitionIndex, completed, mapping,
                new PartitionMetadataRetry()))
            .thenCompose(tombstone -> sweepPartitionAfterTombstone(
                id, partitionIndex,
                () -> streamConfigStore.verifyCompletedDrop(id, completed)));
    }

    private CompletableFuture<PartitionTombstone> recoverCompletedDroppedPartition(
            StreamIdentifier id, int partitionIndex,
            IndexedStreamConfigStore.CompletedDrop completed,
            OptionalLong mappedStreamId, PartitionMetadataRetry retryState) {
        String path = catalogPaths.partitionMetadataPath(id, partitionIndex);
        String mappingKey = dropMappingKey(id, partitionIndex);
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
                        && mappedStreamId.isEmpty()
                        && (!completed.config().purgeRequested()
                            || current.streamId() < 0)) {
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

    /**
     * Sweeps a partition's retired allocations once its tombstone is durable.
     *
     * <p>The tombstone itself carries nothing the sweep needs; what differs between the drop and
     * the completed-drop recovery chains is only the lifecycle record the sweep re-verifies its
     * ownership against, which each caller supplies as {@code ownershipVerifier}.
     */
    private CompletableFuture<Void> sweepPartitionAfterTombstone(
            StreamIdentifier id, int partitionIndex,
            Supplier<CompletableFuture<Void>> ownershipVerifier) {
        return sweepRetiredAllocations(id, partitionIndex, ownershipVerifier)
            .thenApply(ignored -> null);
    }

    /** Reads the stream-ID mapping a partition drop has to clean up, if one is still registered. */
    private CompletableFuture<OptionalLong> mappingForPartitionDrop(
            StreamIdentifier id, int partitionIndex) {
        String mappingKey = dropMappingKey(id, partitionIndex);
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

    private String dropMappingKey(StreamIdentifier id, int partitionIndex) {
        return nativePartitionAllocationKey(id, partitionIndex);
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
        String mappingKey = dropMappingKey(id, partitionIndex);
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
            Set<Long> retiredStreamIds, Set<Long> purgeableRetiredStreamIds,
            Set<RetiredStreamMapping> retiredStreamMappings,
            Set<String> retiredMappingKeys) {
        return deletionMetadata(
            streamId, claim.config(), retiredStreamIds, purgeableRetiredStreamIds,
            retiredStreamMappings, retiredMappingKeys);
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
        boolean useMetadataSource = usesMetadataSourceForDeletion(config);
        Optional<String> ownerToken = useMetadataSource
            ? config.metadataSourceOwnerToken() : config.ownerToken();
        long ownerGeneration = useMetadataSource
            ? config.metadataSourceGeneration() : config.ownerGeneration();
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

    private static boolean usesMetadataSourceForDeletion(
            IndexedStreamConfigStore.StreamConfigData config) {
        return config.provisioningState()
                == IndexedStreamConfigStore.ProvisioningState.ABORTING
            || config.provisioningState()
                == IndexedStreamConfigStore.ProvisioningState.DROPPED;
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

    // --- Layout / Data Plane ---

    @Override
    public CompletableFuture<StreamLayout> getLayout(StreamIdentifier id) {
        return streamConfigStore.readActive(id).thenCompose(active -> getLayout(id, active));
    }

    private CompletableFuture<StreamLayout> getLayout(
            StreamIdentifier id, IndexedStreamConfigStore.ActiveStreamConfig active) {
        return readCommittedLayout(id, active).thenApply(CommittedLayoutSnapshot::layout);
    }

    private CompletableFuture<CommittedLayoutSnapshot> readCommittedLayout(
            StreamIdentifier id, IndexedStreamConfigStore.ActiveStreamConfig active) {
        List<CompletableFuture<LogId>> futures = new ArrayList<>();
        for (int i = 0; i < active.config().partitions(); i++) {
            final int partIdx = i;
            futures.add(readPartitionMetadata(id, partIdx, active.config())
                .thenApply(meta -> LogId.of(meta.metadata().streamId())));
        }
        return CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]))
            .thenCompose(ignored -> streamConfigStore.readActive(id))
            .thenCompose(current -> {
                if (!sameActiveLifecycle(current.config(), active.config())) {
                    return CompletableFuture.failedFuture(new NoSuchStreamException(id));
                }
                if (!sameCommittedLayout(current.config(), active.config())) {
                    return readCommittedLayout(id, current);
                }
                StreamLayout layout = new IndexedLayout(futures.stream()
                    .map(CompletableFuture::join)
                    .toList());
                return CompletableFuture.completedFuture(
                    new CommittedLayoutSnapshot(layout, current));
            });
    }

    private static boolean sameActiveLifecycle(
            IndexedStreamConfigStore.StreamConfigData current,
            IndexedStreamConfigStore.StreamConfigData expected) {
        return current.provisioningState()
                == IndexedStreamConfigStore.ProvisioningState.ACTIVE
            && expected.provisioningState()
                == IndexedStreamConfigStore.ProvisioningState.ACTIVE
            && current.incarnationId().equals(expected.incarnationId())
            && current.ownerToken().equals(expected.ownerToken())
            && current.ownerGeneration() == expected.ownerGeneration()
            && current.metadataSourceOwnerToken().equals(
                expected.metadataSourceOwnerToken())
            && current.metadataSourceGeneration()
                == expected.metadataSourceGeneration()
            && current.creationKind().equals(expected.creationKind());
    }

    private static boolean sameCommittedLayout(
            IndexedStreamConfigStore.StreamConfigData current,
            IndexedStreamConfigStore.StreamConfigData expected) {
        return current.partitions() == expected.partitions()
            && current.definition().equals(expected.definition());
    }

    private record CommittedLayoutSnapshot(
            StreamLayout layout, IndexedStreamConfigStore.ActiveStreamConfig active) {
    }

    @Override
    public CompletableFuture<Log> openLog(StreamIdentifier id, LogId logId) {
        Objects.requireNonNull(id, "id");
        Objects.requireNonNull(logId, "logId");
        // Resolving a LogId means finding its position, which only the whole layout can give.
        return openOwnedLog(id, streamConfigStore.readActive(id)
            .thenCompose(active -> getLayout(id, active))
            .thenCompose(layout -> layout.logIds())
            .thenCompose(logIds -> {
                int partitionIndex = logIds.indexOf(logId);
                if (partitionIndex < 0) {
                    return CompletableFuture.<ResolvedPartition>failedFuture(
                        new IllegalArgumentException(
                            "Log " + logId + " is not in the committed layout of stream "
                                + id.fullName()));
                }
                return CompletableFuture.completedFuture(
                    new ResolvedPartition(partitionIndex, logId));
            }));
    }

    @Override
    public CompletableFuture<Log> openLog(StreamIdentifier id, int partitionIndex) {
        Objects.requireNonNull(id, "id");
        return openOwnedLog(id, readPartitionMetadata(id, partitionIndex)
            .thenApply(metadata -> new ResolvedPartition(
                partitionIndex, LogId.of(metadata.streamId()))));
    }

    /** One partition of the committed layout, resolved to the log that currently backs it. */
    private record ResolvedPartition(int partitionIndex, LogId logId) {
    }

    /**
     * Opens a resolved partition and hands back a supervised handle the catalog still owns.
     *
     * <p>A caller that drops the returned future never leaks the log: the handle is closed and its
     * write lease released through {@link #startAbandonedLogCleanup}.
     */
    private CompletableFuture<Log> openOwnedLog(
            StreamIdentifier id, CompletableFuture<ResolvedPartition> resolved) {
        CompletableFuture<Log> opened = resolved.thenCompose(partition ->
            openCommittedLog(id, partition.partitionIndex(), partition.logId())
                .thenApply(Log.class::cast));
        return OwnedResultFutures.transfer(opened, this::startAbandonedLogCleanup);
    }

    private CompletableFuture<LeasedLog> openCommittedLog(
            StreamIdentifier id, int partitionIndex, LogId logId) {
        if (writeLeaseStorage == null) {
            return CompletableFuture.failedFuture(new UnsupportedOperationException(
                "Opening writable logs requires a lifecycle-aware StorageApi"));
        }
        try {
            beginOpenAttempt();
        } catch (RuntimeException failure) {
            return CompletableFuture.failedFuture(failure);
        }
        final CompletableFuture<LeasedLog> opened;
        try {
            opened = Objects.requireNonNull(
                writeLeaseStorage.acquireStreamWriteLease(logId.id()),
                "stream write lease acquisition future")
                .thenCompose(lease -> createLeasedLog(id, partitionIndex, logId, lease));
        } catch (RuntimeException | Error failure) {
            endOpenAttempt();
            return CompletableFuture.failedFuture(failure);
        }
        CompletableFuture<LeasedLog> published = new CompletableFuture<>();
        opened.whenComplete((logHandle, failure) -> {
            endOpenAttempt();
            if (failure == null) {
                published.complete(logHandle);
            } else {
                published.completeExceptionally(failure);
            }
        });
        return published;
    }

    private CompletableFuture<LeasedLog> createLeasedLog(
            StreamIdentifier id, int partitionIndex, LogId logId, StreamWriteLease lease) {
        if (lease == null) {
            return CompletableFuture.failedFuture(
                new NullPointerException("stream write lease"));
        }
        Log opened = null;
        try {
            if (lease.streamId() != logId.id()) {
                IllegalArgumentException failure = new IllegalArgumentException(
                    "Lease for stream " + lease.streamId() + " cannot protect log " + logId);
                startFailedOpenLeaseCleanup(lease, failure);
                return CompletableFuture.failedFuture(failure);
            }
            opened = supportsReaderAwareLogCreation
                ? createLog(catalogPaths.compactedReaderName(id, partitionIndex), logId)
                : createLog(logId);
            LeasedLog leasedLog = new LeasedLog(
                opened, lease, delegateCloseExecutor, this::releaseDataPlaneHandle);
            retainDataPlaneHandle();
            return CompletableFuture.completedFuture(leasedLog);
        } catch (RuntimeException | Error failure) {
            startFailedOpenCleanup(opened, lease, failure);
            return CompletableFuture.failedFuture(failure);
        }
    }

    private void startFailedOpenCleanup(
            @Nullable Log opened, StreamWriteLease lease, Throwable creationFailure) {
        if (opened == null) {
            startFailedOpenLeaseCleanup(lease, creationFailure);
            return;
        }
        try {
            superviseOpenCleanup(LeasedLog.forFailedOpen(
                    opened, lease, delegateCloseExecutor)
                    .closeEventually(failedOpenCleanupExecutor))
                .whenComplete((ignored, cleanupFailure) -> {
                    if (cleanupFailure != null) {
                        Throwable cause = unwrap(cleanupFailure);
                        addSuppressed(creationFailure, cause);
                        log.error("Failed to supervise cleanup after opening log {} failed",
                            lease.streamId(), cause);
                    }
                });
        } catch (RuntimeException | Error cleanupFailure) {
            addSuppressed(creationFailure, cleanupFailure);
            log.error("Failed to start cleanup after opening log {} failed",
                lease.streamId(), cleanupFailure);
        }
    }

    private void startFailedOpenLeaseCleanup(
            StreamWriteLease lease, Throwable creationFailure) {
        try {
            superviseOpenCleanup(LeasedLog.releaseLeaseEventually(
                    lease, failedOpenCleanupExecutor))
                .whenComplete((ignored, cleanupFailure) -> {
                    if (cleanupFailure != null) {
                        Throwable cause = unwrap(cleanupFailure);
                        addSuppressed(creationFailure, cause);
                        log.error("Failed to supervise release of write lease for log {}",
                            lease.streamId(), cause);
                    }
                });
        } catch (RuntimeException | Error cleanupFailure) {
            addSuppressed(creationFailure, cleanupFailure);
            log.error("Failed to start release of write lease for log {}",
                lease.streamId(), cleanupFailure);
        }
    }

    private void startFailedWriterCleanup(
            List<LeasedLog> logs, Throwable openFailure) {
        for (int i = logs.size() - 1; i >= 0; i--) {
            LeasedLog opened = logs.get(i);
            try {
                superviseOpenCleanup(opened.closeEventually(failedOpenCleanupExecutor))
                    .whenComplete((ignored, cleanupFailure) -> {
                    if (cleanupFailure != null) {
                        Throwable cause = unwrap(cleanupFailure);
                        addSuppressed(openFailure, cause);
                        log.error("Failed to supervise cleanup of partially opened log {}",
                            opened.id(), cause);
                    }
                    });
            } catch (RuntimeException | Error cleanupFailure) {
                addSuppressed(openFailure, cleanupFailure);
                log.error("Failed to start cleanup of partially opened log {}",
                    opened.id(), cleanupFailure);
            }
        }
    }

    private void startAbandonedLogCleanup(Log opened) {
        if (!(opened instanceof LeasedLog leasedLog)) {
            log.error("Catalog produced an unowned log handle that cannot be supervised: {}",
                opened.id());
            return;
        }
        try {
            superviseOpenCleanup(leasedLog.closeEventually(failedOpenCleanupExecutor));
        } catch (RuntimeException | Error cleanupFailure) {
            log.error("Failed to start cleanup of abandoned log {}", opened.id(), cleanupFailure);
        }
    }

    private void startAbandonedWriterCleanup(StreamWriter writer) {
        CompletableFuture<Void> completion = new CompletableFuture<>();
        CompletableFuture<Void> guarded =
            OwnedResultFutures.nonCancellableCompletion(completion);
        try {
            superviseOpenCleanup(guarded);
            attemptAbandonedWriterClose(writer, completion, 0);
        } catch (RuntimeException | Error cleanupFailure) {
            log.error("Failed to start cleanup of abandoned stream writer", cleanupFailure);
            completion.completeExceptionally(cleanupFailure);
        }
    }

    private void attemptAbandonedWriterClose(
            StreamWriter writer, CompletableFuture<Void> completion, int retryAttempt) {
        try {
            failedOpenCleanupExecutor.execute(() -> {
                try {
                    writer.close();
                    completion.complete(null);
                } catch (Throwable failure) {
                    scheduleAbandonedWriterCloseRetry(
                        writer, completion, retryAttempt, failure);
                }
            });
        } catch (Throwable failure) {
            scheduleAbandonedWriterCloseRetry(writer, completion, retryAttempt, failure);
        }
    }

    private void scheduleAbandonedWriterCloseRetry(
            StreamWriter writer,
            CompletableFuture<Void> completion,
            int retryAttempt,
            Throwable failure) {
        int nextAttempt = retryAttempt == Integer.MAX_VALUE
            ? Integer.MAX_VALUE : retryAttempt + 1;
        int shift = Math.min(retryAttempt, 6);
        long delayMillis = Math.min(
            ABANDONED_HANDLE_INITIAL_RETRY_MILLIS << shift,
            ABANDONED_HANDLE_MAX_RETRY_MILLIS);
        log.warn("Failed to close abandoned stream writer; retrying in {} ms (attempt {})",
            delayMillis, nextAttempt, failure);
        try {
            CompletableFuture.delayedExecutor(delayMillis, TimeUnit.MILLISECONDS)
                .execute(() -> attemptAbandonedWriterClose(writer, completion, nextAttempt));
        } catch (Throwable schedulingFailure) {
            failure.addSuppressed(schedulingFailure);
            completion.completeExceptionally(failure);
        }
    }

    private void startAbandonedReaderCleanup(StreamReader opened) {
        if (!(opened instanceof CatalogOwnedStreamReader reader)) {
            log.error("Catalog produced an unowned stream reader that cannot be supervised");
            return;
        }
        try {
            superviseOpenCleanup(reader.closeEventually(failedOpenCleanupExecutor));
        } catch (RuntimeException | Error cleanupFailure) {
            log.error("Failed to start cleanup of abandoned stream reader", cleanupFailure);
        }
    }

    private void beginOpenAttempt() {
        synchronized (openLifecycleMutex) {
            if (catalogClosing) {
                throw new IllegalStateException("Stream catalog is closing or closed");
            }
            activeOpenAttempts++;
        }
    }

    private void endOpenAttempt() {
        synchronized (openLifecycleMutex) {
            activeOpenAttempts--;
            openLifecycleMutex.notifyAll();
        }
    }

    private void retainDataPlaneHandle() {
        synchronized (openLifecycleMutex) {
            activeDataPlaneHandles++;
        }
    }

    private void releaseDataPlaneHandle() {
        synchronized (openLifecycleMutex) {
            activeDataPlaneHandles--;
            openLifecycleMutex.notifyAll();
        }
    }

    private CompletableFuture<Void> superviseOpenCleanup(CompletableFuture<Void> cleanup) {
        Objects.requireNonNull(cleanup, "open cleanup future");
        synchronized (openLifecycleMutex) {
            pendingOpenCleanups.add(cleanup);
        }
        cleanup.whenComplete((ignored, failure) -> {
            synchronized (openLifecycleMutex) {
                if (failure == null) {
                    pendingOpenCleanups.remove(cleanup);
                }
                openLifecycleMutex.notifyAll();
            }
        });
        return cleanup;
    }

    @Override
    public CompletableFuture<StreamWriter> openWriter(StreamIdentifier id) {
        Objects.requireNonNull(id, "id");
        CompletableFuture<StreamWriter> opened = getLayout(id).thenCompose(layout -> layout.logIds()
            .thenCompose(logIds -> openWriter(id, layout, logIds)));
        return OwnedResultFutures.transfer(opened, this::startAbandonedWriterCleanup);
    }

    private CompletableFuture<StreamWriter> openWriter(
            StreamIdentifier id, StreamLayout layout, List<LogId> logIds) {
        List<LeasedLog> opened = new ArrayList<>(logIds.size());
        CompletableFuture<Void> chain = CompletableFuture.completedFuture(null);
        for (int partitionIndex = 0; partitionIndex < logIds.size(); partitionIndex++) {
            int index = partitionIndex;
            LogId logId = logIds.get(index);
            chain = chain.thenCompose(ignored -> openCommittedLog(id, index, logId)
                .thenAccept(opened::add));
        }
        CompletableFuture<StreamWriter> result = chain.thenApply(ignored ->
            new StreamWriterImpl(layout, opened.stream().map(Log.class::cast).toList()));
        CompletableFuture<StreamWriter> completed = result.handle((writer, failure) -> {
            if (failure == null) {
                return CompletableFuture.completedFuture(writer);
            }
            Throwable cause = unwrap(failure);
            startFailedWriterCleanup(opened, cause);
            return CompletableFuture.<StreamWriter>failedFuture(cause);
        }).thenCompose(Function.identity());
        return completed;
    }

    @Override
    public CompletableFuture<StreamReader> openReader(StreamIdentifier id) {
        Objects.requireNonNull(id, "id");
        try {
            beginOpenAttempt();
        } catch (RuntimeException failure) {
            return CompletableFuture.failedFuture(failure);
        }
        final CompletableFuture<StreamReader> opened;
        try {
            opened = getLayout(id).thenCompose(layout ->
                createUnifiedReader(id, layout).thenApply(unifiedReader -> {
                    StreamReader delegate = unifiedReader == null
                        ? new StreamReaderImpl(layout, logStorage)
                        : new StreamReaderImpl(layout, unifiedReader);
                    CatalogOwnedStreamReader reader = new CatalogOwnedStreamReader(
                        delegate, delegateCloseExecutor, this::releaseDataPlaneHandle);
                    retainDataPlaneHandle();
                    return reader;
                }));
        } catch (RuntimeException | Error failure) {
            endOpenAttempt();
            return CompletableFuture.failedFuture(failure);
        }
        CompletableFuture<StreamReader> published = new CompletableFuture<>();
        opened.whenComplete((reader, failure) -> {
            endOpenAttempt();
            if (failure == null) {
                published.complete(reader);
            } else {
                published.completeExceptionally(failure);
            }
        });
        return OwnedResultFutures.transfer(published, this::startAbandonedReaderCleanup);
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
                readerNames.put(logId, catalogPaths.compactedReaderName(id, i));
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
    public synchronized void close() throws Exception {
        if (resourcesClosed) {
            return;
        }
        awaitOpenCleanupBeforeClose();
        failedOpenCleanupExecutor.shutdown();
        delegateCloseExecutor.shutdown();
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
        resourcesClosed = true;
    }

    private void awaitOpenCleanupBeforeClose() throws IOException {
        awaitOpenCleanupBeforeClose(CATALOG_CLOSE_TIMEOUT_MILLIS);
    }

    void awaitOpenCleanupBeforeClose(long timeoutMillis) throws IOException {
        if (timeoutMillis <= 0) {
            throw new IllegalArgumentException("timeoutMillis must be positive");
        }
        long deadlineNanos = System.nanoTime()
            + TimeUnit.MILLISECONDS.toNanos(timeoutMillis);
        synchronized (openLifecycleMutex) {
            catalogClosing = true;
            while (activeOpenAttempts != 0 || activeDataPlaneHandles != 0
                    || !pendingOpenCleanups.isEmpty()) {
                long remainingNanos = deadlineNanos - System.nanoTime();
                if (remainingNanos <= 0) {
                    throw new IOException("Timed out after " + timeoutMillis
                        + " ms waiting for " + activeOpenAttempts + " open attempt(s) and "
                        + activeDataPlaneHandles + " data-plane handle(s) and "
                        + pendingOpenCleanups.size() + " failed-open cleanup(s) to finish");
                }
                try {
                    TimeUnit.NANOSECONDS.timedWait(openLifecycleMutex, remainingNanos);
                } catch (InterruptedException failure) {
                    Thread.currentThread().interrupt();
                    throw new IOException(
                        "Interrupted while waiting for failed-open cleanup to finish", failure);
                }
            }
        }
    }

    // --- Internal helpers ---

    /**
     * Reads one partition's metadata under the active stream lifecycle, in a constant number of
     * catalog reads.
     *
     * <p>Opening a single partition must not cost one metadata read per partition, so this does not
     * build the layout: {@code readActive} pins the lifecycle, the partition record is read and
     * checked against it, and {@link IndexedStreamConfigStore#verifyActiveOwnership} re-reads the
     * config so a lifecycle that moved on underneath the read fails instead of yielding a stale log.
     *
     * @throws IllegalArgumentException if the index is outside the committed layout
     */
    private CompletableFuture<LogMetadata> readPartitionMetadata(
            StreamIdentifier id, int partitionIndex) {
        return streamConfigStore.readActive(id).thenCompose(active -> {
            int partitions = active.config().partitions();
            if (partitionIndex < 0 || partitionIndex >= partitions) {
                return CompletableFuture.<LogMetadata>failedFuture(new IllegalArgumentException(
                    "Partition " + partitionIndex + " is not in the committed layout of "
                        + id.fullName() + " (" + partitions + " logs)"));
            }
            return readPartitionMetadata(id, partitionIndex, active.config())
                .thenCompose(metadata ->
                    streamConfigStore.verifyActiveOwnership(id, active)
                        .thenApply(ignored -> metadata.metadata()));
        });
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
            return config.incarnationId().isEmpty();
        }
        if (config.incarnationId().isEmpty()) {
            return false;
        }
        if (!config.incarnationId().orElseThrow()
                .equals(metadata.registrationIncarnationId())) {
            return false;
        }
        if (usesMetadataSourceForDeletion(config)) {
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
        return fencedMappingStorage == null || writeLeaseStorage == null
            ? new UnsupportedOperationException(
                operation + " requires durable stream-ID mapping and write fencing support")
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
