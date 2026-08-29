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
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
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
    private final BiFunction<String, Long, CompletableFuture<Void>> streamIdMappingDeleter;
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
            namedLogFactory, true, logStateManager, streamIdGenerator,
            readerFactory, entryIndexCache, null, null, ownedResources);
    }

    public IndexedStreamCatalog(AsyncOxiaClient oxiaClient, CatalogPaths catalogPaths,
                                LogStorage logStorage,
                                LogFactory namedLogFactory,
                                @Nullable LogStateManager logStateManager,
                                Function<Optional<String>, CompletableFuture<Long>> streamIdGenerator,
                                Function<String, CompletableFuture<Long>> streamIdLookup,
                                BiFunction<String, Long, CompletableFuture<Void>> streamIdMappingDeleter,
                                @Nullable CompactedObjectReaderFactory readerFactory,
                                @Nullable EntryIndexCache entryIndexCache,
                                List<AutoCloseable> ownedResources) {
        this(oxiaClient, catalogPaths, logStorage, logId -> namedLogFactory.create(null, logId, null),
            namedLogFactory, true, logStateManager, streamIdGenerator,
            readerFactory, entryIndexCache,
            Objects.requireNonNull(streamIdLookup, "streamIdLookup"),
            Objects.requireNonNull(streamIdMappingDeleter, "streamIdMappingDeleter"),
            ownedResources);
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
                                @Nullable BiFunction<String, Long, CompletableFuture<Void>> streamIdMappingDeleter,
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
        UnsupportedOperationException capabilityFailure = keyedLifecycleCapabilityFailure(
            "Stream creation");
        if (capabilityFailure != null) {
            return CompletableFuture.failedFuture(capabilityFailure);
        }
        int numPartitions = partitioning.numPartitions();
        String ownerToken = UUID.randomUUID().toString();
        return streamConfigStore.claimCreation(
                id, numPartitions, properties, materialization,
                IndexedStreamConfigStore.CreationKind.NATIVE_CREATE, ownerToken)
            .thenCompose(claim -> createPartitions(id, claim)
                .thenCompose(ignored -> retagNativePartitions(id, claim))
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
                .thenCompose(ignored -> allocateKeyedStreamId(allocationKey))
                .thenCompose(streamId -> verifyAndWriteNativePartitionAfterAllocation(
                    id, partIdx, allocationKey, streamId, claim))
                .thenCompose(ignored ->
                    streamConfigStore.verifyProvisioningOwnership(id, claim));
        }
        return chain;
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
                    id, allocationKey, streamId, claim, cause);
            });
    }

    private CompletableFuture<Void> compensateRejectedNativeAllocation(
            StreamIdentifier id, String allocationKey, long streamId,
            IndexedStreamConfigStore.ProvisioningClaim claim,
            Throwable ownershipFailure) {
        return streamConfigStore.canCleanupRejectedNativeAllocation(id, claim)
            .thenCompose(canCleanup -> {
                if (!canCleanup) {
                    return CompletableFuture.failedFuture(ownershipFailure);
                }
                return logStorage.deleteLog(LogId.of(streamId))
                    .thenCompose(ignored -> streamIdMappingDeleter.apply(
                        allocationKey, streamId))
                    .handle((ignored, cleanupFailure) -> {
                        if (cleanupFailure != null) {
                            ownershipFailure.addSuppressed(rootCause(cleanupFailure));
                        }
                        return null;
                    })
                    .thenCompose(ignored ->
                        CompletableFuture.failedFuture(ownershipFailure));
            });
    }

    private CompletableFuture<Void> retagNativePartitions(
            StreamIdentifier id, IndexedStreamConfigStore.ProvisioningClaim claim) {
        CompletableFuture<Void> chain = CompletableFuture.completedFuture(null);
        for (int i = 0; i < claim.config().partitions(); i++) {
            int partitionIndex = i;
            String allocationKey = nativePartitionAllocationKey(id, claim, partitionIndex);
            chain = chain
                .thenCompose(ignored ->
                    streamConfigStore.verifyProvisioningOwnership(id, claim))
                .thenCompose(ignored -> readNativePartitionStreamIdForRetag(
                    id, partitionIndex, allocationKey))
                .thenCompose(streamId -> writePartitionMetadataForNativeClaim(
                    id, partitionIndex, streamId, claim))
                .thenApply(ignored -> null);
        }
        return chain;
    }

    private CompletableFuture<Long> readNativePartitionStreamIdForRetag(
            StreamIdentifier id, int partitionIndex, String allocationKey) {
        String path = catalogPaths.partitionMetadataPath(id, partitionIndex);
        return oxiaClient.get(path).thenCompose(result -> {
            if (result != null) {
                try {
                    LogMetadata metadata = LOG_METADATA_SERDE.deserialize(path, result.value());
                    if (!metadata.deleted()) {
                        return CompletableFuture.completedFuture(metadata.streamId());
                    }
                } catch (Exception e) {
                    return CompletableFuture.failedFuture(e);
                }
            }
            if (streamIdLookup != null) {
                return streamIdLookup.apply(allocationKey);
            }
            return CompletableFuture.failedFuture(new NoSuchStreamException(id));
        });
    }

    private static String nativePartitionAllocationKey(
            StreamIdentifier id, IndexedStreamConfigStore.ProvisioningClaim claim,
            int partitionIndex) {
        return nativePartitionAllocationKey(id, claim.incarnationId(), partitionIndex);
    }

    private static String nativePartitionAllocationKey(
            StreamIdentifier id, String incarnationId, int partitionIndex) {
        return "lakestream-native/" + id.fullName() + "/"
            + incarnationId + "/partition-" + partitionIndex;
    }

    @Override
    public CompletableFuture<Void> registerExternalStream(StreamIdentifier id, int partitionCount,
                                                          Map<String, String> properties) {
        UnsupportedOperationException capabilityFailure = keyedLifecycleCapabilityFailure(
            "External stream registration");
        if (capabilityFailure != null) {
            return CompletableFuture.failedFuture(capabilityFailure);
        }
        return streamConfigStore.registerExternalStream(id, partitionCount, properties);
    }

    @Override
    public CompletableFuture<Void> unregisterExternalStream(StreamIdentifier id) {
        return streamConfigStore.unregisterExternalStream(id);
    }

    @Override
    public CompletableFuture<Void> permanentlyDeleteExternalStream(StreamIdentifier id) {
        return streamConfigStore.permanentlyDeleteExternalStream(id);
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
     * {@link NoSuchStreamException}. Safe to call concurrently and repeatedly: partition metadata is
     * created or ownership-retagged with a version-guarded compare-and-set, and the stream-config
     * grow also retries on a concurrent update.
     */
    @Override
    public CompletableFuture<Void> registerExternalPartition(StreamIdentifier id, int partitionIndex,
                                                             long streamId,
                                                             @Nullable Map<String, String> properties) {
        UnsupportedOperationException capabilityFailure = keyedLifecycleCapabilityFailure(
            "External partition registration");
        if (capabilityFailure != null) {
            return CompletableFuture.failedFuture(capabilityFailure);
        }
        Map<String, String> props = properties == null ? Map.of() : properties;
        return streamConfigStore.beginExternalPartitionRegistration(
                id, partitionIndex + 1, props, UUID.randomUUID().toString())
            .thenCompose(registration -> streamConfigStore
                .verifyExternalRegistration(id, registration)
                .handle((ignored, failure) ->
                    new ExternalWriteAttempt(null, unwrapNullable(failure)))
                .thenCompose(attempt -> attempt.failure() == null
                    ? prepareRetiredExternalPartition(
                        id, partitionIndex, registration)
                        .thenCompose(ignored ->
                            writePartitionMetadataForExternalRegistration(
                                id, partitionIndex, streamId, registration))
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
    }

    @Override
    public CompletableFuture<Log> openExternalPartition(StreamIdentifier id, int partitionIndex,
                                                        Map<String, String> properties) {
        Objects.requireNonNull(id, "id");
        Objects.requireNonNull(properties, "properties");
        if (partitionIndex < 0) {
            throw new IllegalArgumentException("partitionIndex must be non-negative");
        }
        UnsupportedOperationException capabilityFailure = keyedLifecycleCapabilityFailure(
            "External partition creation");
        if (capabilityFailure != null) {
            return CompletableFuture.failedFuture(capabilityFailure);
        }
        String logName = catalogPaths.compactedReaderName(id, partitionIndex);
        return streamConfigStore.beginExternalPartitionRegistration(
                id, partitionIndex + 1, properties, UUID.randomUUID().toString())
            .thenCompose(registration -> streamConfigStore
                .verifyExternalRegistration(id, registration)
                .handle((ignored, failure) -> new ExternalOpenAttempt(
                    null, null, unwrapNullable(failure)))
                .thenCompose(attempt -> attempt.failure() == null
                    ? allocateExternalReplacementStreamId(
                        id, partitionIndex, logName, registration)
                        .handle((allocation, failure) -> new ExternalOpenAttempt(
                            allocation, null, unwrapNullable(failure)))
                    : CompletableFuture.completedFuture(attempt))
                .thenCompose(attempt -> attempt.failure() == null
                    ? writePartitionMetadataForExternalRegistration(
                        id, partitionIndex, attempt.streamId(), registration)
                        .handle((write, failure) -> new ExternalOpenAttempt(
                            attempt.streamId(), write, unwrapNullable(failure)))
                    : CompletableFuture.completedFuture(attempt))
                .thenCompose(attempt -> attempt.failure() == null
                    ? completeExternalRegistration(id, registration)
                        .handle((ignored, failure) -> new ExternalOpenAttempt(
                            attempt.streamId(), attempt.write(), unwrapNullable(failure)))
                    : CompletableFuture.completedFuture(attempt))
                .thenCompose(attempt -> {
                    if (attempt.failure() != null) {
                        return compensateRejectedOpen(
                            id, partitionIndex, attempt);
                    }
                    return CompletableFuture.completedFuture(createLog(
                        logName, LogId.of(attempt.streamId())));
                }));
    }

    private CompletableFuture<Long> allocateKeyedStreamId(String logName) {
        return streamIdGenerator.apply(Optional.of(logName));
    }

    private CompletableFuture<Long> allocateExternalReplacementStreamId(
            StreamIdentifier id, int partitionIndex, String logName,
            IndexedStreamConfigStore.ExternalRegistration registration) {
        return prepareRetiredExternalPartition(id, partitionIndex, registration)
            .thenCompose(retiredStreamId -> allocateKeyedStreamId(logName)
                .thenApply(streamId -> {
                    if (retiredStreamId.isPresent()
                            && retiredStreamId.getAsLong() == streamId) {
                        throw new AlreadyExistsException(
                            "Deleted external partition " + id.fullName() + "-partition-"
                                + partitionIndex + " must use a fresh physical stream ID");
                    }
                    // The write path verifies ownership before its metadata CAS. Keeping the
                    // allocated ID in the open attempt lets a raced deletion compensate the
                    // keyed allocation instead of leaking it when that verification fails.
                    return streamId;
                }));
    }

    private CompletableFuture<OptionalLong> prepareRetiredExternalPartition(
            StreamIdentifier id, int partitionIndex,
            IndexedStreamConfigStore.ExternalRegistration registration) {
        String metadataPath = catalogPaths.partitionMetadataPath(id, partitionIndex);
        String mappingKey = catalogPaths.compactedReaderName(id, partitionIndex);
        return streamConfigStore.verifyExternalRegistration(id, registration)
            .thenCompose(ignored -> oxiaClient.get(metadataPath))
            .thenCompose(existing -> {
                if (existing == null) {
                    return CompletableFuture.completedFuture(OptionalLong.empty());
                }
                final LogMetadata metadata;
                try {
                    metadata = LOG_METADATA_SERDE.deserialize(
                        metadataPath, existing.value());
                } catch (Exception e) {
                    return CompletableFuture.failedFuture(e);
                }
                if (!metadata.deleted()) {
                    return CompletableFuture.completedFuture(OptionalLong.empty());
                }
                if (!deletedMetadataCanBeReplaced(metadata, registration)) {
                    return CompletableFuture.failedFuture(new AlreadyExistsException(
                        "Deleted partition metadata is not replaceable by the current external "
                            + "registration: " + id.fullName() + "-partition-"
                            + partitionIndex));
                }
                long retiredStreamId = metadata.streamId();
                return streamConfigStore.verifyExternalRegistration(id, registration)
                    .thenCompose(ignored -> retiredStreamId >= 0
                        ? logStorage.deleteLog(LogId.of(retiredStreamId))
                        : CompletableFuture.completedFuture(null))
                    .thenCompose(ignored -> retiredStreamId >= 0
                        ? streamIdMappingDeleter.apply(mappingKey, retiredStreamId)
                        : CompletableFuture.completedFuture(null))
                    .thenCompose(ignored ->
                        streamConfigStore.verifyExternalRegistration(id, registration))
                    .thenApply(ignored -> retiredStreamId >= 0
                        ? OptionalLong.of(retiredStreamId) : OptionalLong.empty());
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
        if (Objects.equals(metadata.registrationIncarnationId(),
                registration.incarnationId().orElseThrow())) {
            return registration.metadataSourceGeneration() == metadataGeneration;
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
        return CompletableFuture.failedFuture(rootCause(registrationFailure));
    }

    private <T> CompletableFuture<T> compensateRejectedOpen(
            StreamIdentifier id, int partitionIndex,
            ExternalOpenAttempt attempt) {
        Throwable failure = rootCause(attempt.failure());
        if (attempt.streamId() == null
                || streamIdLookup == null || streamIdMappingDeleter == null) {
            return CompletableFuture.failedFuture(failure);
        }
        return streamConfigStore.readExternalDeletionContext(id)
            .handle((context, readFailure) -> readFailure == null ? context : null)
            .thenCompose(context -> {
                if (context == null
                        || context.config().provisioningState()
                            == IndexedStreamConfigStore.ProvisioningState.ACTIVE) {
                    return CompletableFuture.failedFuture(failure);
                }
                return deleteExternalPartition(id, partitionIndex)
                    .handle((ignored, cleanupFailure) -> {
                        if (cleanupFailure != null) {
                            failure.addSuppressed(rootCause(cleanupFailure));
                        }
                        return null;
                    })
                    .thenCompose(ignored -> CompletableFuture.failedFuture(failure));
            });
    }

    private record ExternalWriteAttempt(
            @Nullable PartitionMetadataWrite write, @Nullable Throwable failure) {
    }

    private record ExternalOpenAttempt(
            @Nullable Long streamId, @Nullable PartitionMetadataWrite write,
            @Nullable Throwable failure) {
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
        if (streamIdLookup == null || streamIdMappingDeleter == null) {
            return CompletableFuture.failedFuture(new UnsupportedOperationException(
                "External partition deletion requires keyed stream-ID lifecycle support"));
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
        return externalMappingForDelete(logName)
            .thenCompose(mappedStreamId -> tombstoneExternalPartition(
                id, partitionIndex, context, mappedStreamId))
            .thenCompose(tombstone -> {
                CompletableFuture<Void> cleanup = tombstone.streamId() >= 0
                    ? logStorage.deleteLog(LogId.of(tombstone.streamId()))
                    : CompletableFuture.completedFuture(null);
                if (tombstone.mappingStreamId() >= 0
                        && tombstone.mappingStreamId() != tombstone.streamId()) {
                    cleanup = cleanup.thenCompose(ignored ->
                        logStorage.deleteLog(LogId.of(tombstone.mappingStreamId())));
                }
                return cleanup
                    .thenCompose(ignored ->
                        streamConfigStore.verifyExternalDeletionContext(id, context))
                    .thenCompose(ignored -> tombstone.mappingStreamId() >= 0
                        ? streamIdMappingDeleter.apply(
                            logName, tombstone.mappingStreamId())
                        : CompletableFuture.completedFuture(null))
                    .thenCompose(ignored ->
                        streamConfigStore.verifyExternalDeletionContext(id, context));
            })
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
                    .thenCompose(successor -> context.canRetryWith(successor)
                        ? deleteExternalPartitionWithContext(
                            id, partitionIndex, logName, successor,
                            remainingContextRetries - 1)
                        : CompletableFuture.failedFuture(cause));
            });
    }

    private CompletableFuture<OptionalLong> externalMappingForDelete(String logName) {
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
    }

    private CompletableFuture<PartitionTombstone> tombstoneExternalPartition(
            StreamIdentifier id, int partitionIndex,
            IndexedStreamConfigStore.ExternalDeletionContext context,
            OptionalLong mappedStreamId) {
        IndexedStreamConfigStore.StreamConfigData config = context.config();
        String path = catalogPaths.partitionMetadataPath(id, partitionIndex);
        return streamConfigStore.verifyExternalDeletionContext(id, context)
            .thenCompose(ignored -> oxiaClient.get(path))
            .thenCompose(existing -> {
                if (existing == null) {
                    long streamId = mappedStreamId.orElse(-1L);
                    LogMetadata tombstone = externalDeletionMetadata(streamId, context);
                    return persistPartitionMetadata(
                        id, partitionIndex, tombstone,
                        Set.of(PutOption.IfRecordDoesNotExist),
                        () -> streamConfigStore.verifyExternalDeletionContext(id, context),
                        () -> tombstoneExternalPartition(
                            id, partitionIndex, context, mappedStreamId)
                            .thenApply(PartitionTombstone::write))
                        .thenApply(write -> new PartitionTombstone(
                            streamId, mappedStreamId.orElse(streamId), write));
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
                LogMetadata tombstone = externalDeletionMetadata(streamId, context);
                if (samePersistedRegistration(current, tombstone)) {
                    return streamConfigStore.verifyExternalDeletionContext(id, context)
                        .thenApply(ignored -> new PartitionTombstone(
                            streamId, mappedStreamId.orElse(streamId),
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
                    () -> tombstoneExternalPartition(
                        id, partitionIndex, context, mappedStreamId)
                        .thenApply(PartitionTombstone::write))
                    .thenApply(write -> new PartitionTombstone(
                        streamId, mappedStreamId.orElse(streamId), write));
            });
    }

    private static LogMetadata externalDeletionMetadata(
            long streamId, IndexedStreamConfigStore.ExternalDeletionContext context) {
        IndexedStreamConfigStore.StreamConfigData config = context.config();
        return new LogMetadata(
            streamId, Map.of(), OptionalLong.empty(),
            config.incarnationId().orElse(null),
            config.ownerToken().orElse(null),
            config.ownerGeneration() >= 0 ? config.ownerGeneration() : null, true);
    }

    private static Throwable rootCause(Throwable failure) {
        Throwable current = failure;
        while ((current instanceof CompletionException || current instanceof java.util.concurrent.ExecutionException)
                && current.getCause() != null) {
            current = current.getCause();
        }
        return current;
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
                            ownershipVerifier));
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
                        return partitionMetadataConflict(
                            id, partitionIndex, streamId, current.streamId(),
                            "is fenced by a newer deleted stream lifecycle");
                    }
                    return replacePartitionMetadata(
                        id, partitionIndex,
                        registrationMetadata(
                            streamId, incarnationId, ownerToken, ownerGeneration),
                        existing.version().versionId(), ownershipVerifier,
                        () -> writePartitionMetadataForRegistration(
                            id, partitionIndex, streamId, incarnationId, ownerToken,
                            ownerGeneration, metadataSourceGeneration, creationKind,
                            ownershipVerifier));
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
                        ownerGeneration, false)
                    : registrationMetadata(
                        streamId, incarnationId, ownerToken, ownerGeneration);
                return replacePartitionMetadata(
                    id, partitionIndex, replacement,
                    existing.version().versionId(), ownershipVerifier,
                    () -> writePartitionMetadataForRegistration(
                        id, partitionIndex, streamId, incarnationId, ownerToken,
                        ownerGeneration, metadataSourceGeneration, creationKind,
                        ownershipVerifier));
            });
    }

    private static LogMetadata registrationMetadata(
            long streamId, Optional<String> incarnationId, Optional<String> ownerToken,
            long ownerGeneration) {
        return new LogMetadata(
            streamId, Map.of(), OptionalLong.empty(),
            incarnationId.orElse(null), ownerToken.orElse(null),
            ownerGeneration >= 0 ? ownerGeneration : null, false);
    }

    private CompletableFuture<PartitionMetadataWrite> replacePartitionMetadata(
            StreamIdentifier id, int partitionIndex, LogMetadata desired,
            long expectedVersion, Supplier<CompletableFuture<Void>> ownershipVerifier,
            Supplier<CompletableFuture<PartitionMetadataWrite>> retry) {
        return persistPartitionMetadata(
            id, partitionIndex, desired,
            Set.of(PutOption.IfVersionIdEquals(expectedVersion)), ownershipVerifier, retry);
    }

    private CompletableFuture<PartitionMetadataWrite> persistPartitionMetadata(
            StreamIdentifier id, int partitionIndex, LogMetadata desired,
            Set<PutOption> options, Supplier<CompletableFuture<Void>> ownershipVerifier,
            Supplier<CompletableFuture<PartitionMetadataWrite>> retry) {
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
                        new PartitionMetadataWrite(OptionalLong.of(
                            outcome.result().version().versionId())));
                }
                if (outcome.failure() instanceof KeyAlreadyExistsException
                        || outcome.failure() instanceof UnexpectedVersionIdException) {
                    return retry.get();
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
                            new PartitionMetadataWrite(OptionalLong.of(
                                readback.result().version().versionId())));
                    });
            });
    }

    private static boolean samePersistedRegistration(
            LogMetadata current, LogMetadata desired) {
        return current.streamId() == desired.streamId()
            && current.deleted() == desired.deleted()
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

    private static CompletableFuture<PartitionMetadataWrite> partitionMetadataConflict(
            StreamIdentifier id, int partitionIndex, long expectedStreamId,
            long existingStreamId, String detail) {
        return CompletableFuture.failedFuture(new AlreadyExistsException(
            "Partition " + id.fullName() + "-partition-" + partitionIndex + " " + detail
                + "; existing stream ID " + existingStreamId
                + ", requested stream ID " + expectedStreamId));
    }

    private record PartitionWriteOutcome(PutResult result, Throwable failure) {
    }

    private record PartitionMetadataWrite(OptionalLong metadataVersion) {
    }

    private record PartitionReadOutcome(
            GetResult result, Throwable failure) {
    }

    private static Throwable unwrap(Throwable ex) {
        return ex instanceof CompletionException && ex.getCause() != null ? ex.getCause() : ex;
    }

    private static Throwable unwrapNullable(Throwable failure) {
        return failure == null ? null : unwrap(failure);
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
        if (streamIdLookup == null || streamIdMappingDeleter == null) {
            return CompletableFuture.failedFuture(new UnsupportedOperationException(
                "Stream deletion requires keyed stream-ID lifecycle support"));
        }
        String dropOwnerToken = UUID.randomUUID().toString();
        return streamConfigStore.beginDrop(id, dropOwnerToken).thenCompose(optionalClaim -> {
            if (optionalClaim.isEmpty()) {
                return CompletableFuture.completedFuture(false);
            }
            IndexedStreamConfigStore.DropClaim claim = optionalClaim.orElseThrow();
            return cleanupDroppedStream(id, claim, purge)
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
                    id, partitionIndex, claim, mapping)
                    .thenCompose(tombstone -> cleanupDroppedPartition(
                        id, partitionIndex, claim, tombstone, purge)));
        }
        return chain;
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
        if (claim.config().creationKind().orElse(null)
                == IndexedStreamConfigStore.CreationKind.NATIVE_CREATE) {
            return nativePartitionAllocationKey(
                id, claim.config().incarnationId().orElseThrow(), partitionIndex);
        }
        return catalogPaths.compactedReaderName(id, partitionIndex);
    }

    private CompletableFuture<PartitionTombstone> tombstoneDroppedPartition(
            StreamIdentifier id, int partitionIndex,
            IndexedStreamConfigStore.DropClaim claim,
            OptionalLong mappedStreamId) {
        String path = catalogPaths.partitionMetadataPath(id, partitionIndex);
        return streamConfigStore.verifyAbortingOwnership(id, claim)
            .thenCompose(ignored -> oxiaClient.get(path))
            .thenCompose(existing -> {
                if (existing == null) {
                    long streamId = mappedStreamId.orElse(-1L);
                    LogMetadata tombstone = deletionMetadata(streamId, claim);
                    return persistPartitionMetadata(
                        id, partitionIndex, tombstone,
                        Set.of(PutOption.IfRecordDoesNotExist),
                        () -> streamConfigStore.verifyAbortingOwnership(id, claim),
                        () -> tombstoneDroppedPartition(
                            id, partitionIndex, claim, mappedStreamId)
                            .thenApply(PartitionTombstone::write))
                        .thenApply(write -> new PartitionTombstone(
                            streamId, mappedStreamId.orElse(streamId), write));
                }
                final LogMetadata current;
                try {
                    current = LOG_METADATA_SERDE.deserialize(path, existing.value());
                } catch (Exception e) {
                    return CompletableFuture.failedFuture(e);
                }
                long streamId = current.streamId() >= 0
                    ? current.streamId() : mappedStreamId.orElse(-1L);
                LogMetadata tombstone = deletionMetadata(streamId, claim);
                if (samePersistedRegistration(current, tombstone)) {
                    return streamConfigStore.verifyAbortingOwnership(id, claim)
                        .thenApply(ignored -> new PartitionTombstone(
                            streamId, mappedStreamId.orElse(streamId),
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
                    () -> tombstoneDroppedPartition(
                        id, partitionIndex, claim, mappedStreamId)
                        .thenApply(PartitionTombstone::write))
                    .thenApply(write -> new PartitionTombstone(
                        streamId, mappedStreamId.orElse(streamId), write));
            });
    }

    private static LogMetadata deletionMetadata(
            long streamId, IndexedStreamConfigStore.DropClaim claim) {
        String incarnationId = claim.config().incarnationId().orElse(null);
        return new LogMetadata(
            streamId, Map.of(), OptionalLong.empty(),
            incarnationId, incarnationId == null ? null : claim.ownerToken(),
            incarnationId == null ? null : claim.config().ownerGeneration(), true);
    }

    private CompletableFuture<Void> cleanupDroppedPartition(
            StreamIdentifier id, int partitionIndex,
            IndexedStreamConfigStore.DropClaim claim,
            PartitionTombstone tombstone, boolean purge) {
        CompletableFuture<Void> cleanup = CompletableFuture.completedFuture(null);
        if (purge && tombstone.streamId() >= 0) {
            cleanup = cleanup
                .thenCompose(ignored ->
                    streamConfigStore.verifyAbortingOwnership(id, claim))
                .thenCompose(ignored ->
                    logStorage.deleteLog(LogId.of(tombstone.streamId())));
            if (tombstone.mappingStreamId() >= 0
                    && tombstone.mappingStreamId() != tombstone.streamId()) {
                cleanup = cleanup.thenCompose(ignored ->
                    logStorage.deleteLog(LogId.of(tombstone.mappingStreamId())));
            }
        }
        if (tombstone.mappingStreamId() >= 0) {
            String mappingKey = dropMappingKey(id, partitionIndex, claim);
            cleanup = cleanup
                .thenCompose(ignored ->
                    streamConfigStore.verifyAbortingOwnership(id, claim))
                .thenCompose(ignored -> streamIdMappingDeleter.apply(
                    mappingKey, tombstone.mappingStreamId()));
        }
        return cleanup.thenCompose(ignored ->
            streamConfigStore.verifyAbortingOwnership(id, claim));
    }

    private record PartitionTombstone(
            long streamId, long mappingStreamId, PartitionMetadataWrite write) {

        private PartitionTombstone(long streamId, PartitionMetadataWrite write) {
            this(streamId, streamId, write);
        }
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
                    metadata.registrationOwnerGeneration(), false);
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
                            return streamConfigStore.verifyActiveOwnership(id, active)
                                .thenCompose(ignored ->
                                    sealPartition(id, partitionIndex, active));
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
            try {
                LogMetadata metadata = LOG_METADATA_SERDE.deserialize(path, result.value());
                if (metadata.deleted() || !metadataMatchesActiveConfig(metadata, config)) {
                    throw new NoSuchStreamException(id);
                }
                return new VersionedLogMetadata(
                    metadata, result.version().versionId());
            } catch (Exception e) {
                if (e instanceof NoSuchStreamException noSuchStreamException) {
                    throw noSuchStreamException;
                }
                throw new RuntimeException("Failed to deserialize partition metadata: " + path, e);
            }
        });
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
        if (metadata.registrationOwnerGeneration() == null) {
            if (config.provisioningState()
                    == IndexedStreamConfigStore.ProvisioningState.ACTIVE) {
                return config.incarnationId().isEmpty()
                    && config.ownerToken().isEmpty();
            }
            return config.metadataSourceGeneration()
                    == IndexedStreamConfigStore.LEGACY_METADATA_GENERATION
                || metadata.deleted();
        }
        if (config.incarnationId().isEmpty()) {
            return false;
        }
        if (!config.incarnationId().orElseThrow()
                .equals(metadata.registrationIncarnationId())) {
            return metadata.deleted();
        }
        if (metadata.registrationOwnerGeneration() == config.ownerGeneration()) {
            return (config.provisioningState()
                    == IndexedStreamConfigStore.ProvisioningState.ACTIVE
                    || config.provisioningState()
                        == IndexedStreamConfigStore.ProvisioningState.UNREGISTERED)
                && Objects.equals(metadata.registrationOwnerToken(),
                    config.ownerToken().orElse(null));
        }
        return metadata.registrationOwnerGeneration() < config.ownerGeneration();
    }

    @Nullable
    private UnsupportedOperationException keyedLifecycleCapabilityFailure(String operation) {
        return streamIdLookup == null || streamIdMappingDeleter == null
            ? new UnsupportedOperationException(
                operation + " requires keyed stream-ID lifecycle support")
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
