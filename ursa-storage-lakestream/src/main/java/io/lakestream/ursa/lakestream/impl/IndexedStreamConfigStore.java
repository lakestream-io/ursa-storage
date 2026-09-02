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
import io.lakestream.api.Partitioning;
import io.lakestream.api.PartitioningStrategy;
import io.lakestream.api.SchemaConfig;
import io.lakestream.api.StreamCatalogEntry;
import io.lakestream.api.StreamConfig;
import io.lakestream.api.StreamIdentifier;
import io.lakestream.api.exception.AlreadyExistsException;
import io.lakestream.api.exception.NoSuchStreamException;
import io.lakestream.api.exception.StreamPermanentlyDeletedException;
import io.lakestream.api.materialization.TableMaterializationPolicy;
import io.lakestream.ursa.lakestream.impl.materialization.MaterializationJson;
import io.oxia.client.api.AsyncOxiaClient;
import io.oxia.client.api.GetResult;
import io.oxia.client.api.PutResult;
import io.oxia.client.api.exceptions.KeyAlreadyExistsException;
import io.oxia.client.api.exceptions.UnexpectedVersionIdException;
import io.oxia.client.api.options.DeleteOption;
import io.oxia.client.api.options.PutOption;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeUnit;
import java.util.function.LongFunction;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;
import lombok.extern.slf4j.Slf4j;

/** Owns the persisted indexed-stream config schema and its versioned update semantics. */
@Slf4j
final class IndexedStreamConfigStore {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    static final int MAX_CONFIG_WRITE_RETRIES = 3;
    private static final int LIST_BATCH_SIZE = 32;
    static final long INITIAL_RETRY_BACKOFF_MILLIS = 10L;
    static final long NO_METADATA_GENERATION = -2L;
    static final long LEGACY_METADATA_GENERATION = -1L;
    private static final String INCARNATION_ID_FIELD = "_incarnationId";
    private static final String OWNER_TOKEN_FIELD = "_ownerToken";
    private static final String OWNER_GENERATION_FIELD = "_ownerGeneration";
    private static final String METADATA_SOURCE_OWNER_TOKEN_FIELD =
        "_metadataSourceOwnerToken";
    private static final String METADATA_SOURCE_GENERATION_FIELD = "_metadataSourceGeneration";
    private static final String CREATION_KIND_FIELD = "_creationKind";
    private static final String PROVISIONING_FIELD = "_provisioning";
    private static final String PROVISIONING_STATE_FIELD = "_provisioningState";
    private static final String PURGE_REQUESTED_FIELD = "_purgeRequested";
    private static final String STREAM_CONFIG_FIELD = "streamConfig";
    private static final String PARTITIONING_FIELD = "partitioning";
    private static final String SCHEMA_FIELD = "schema";
    private static final String PENDING_EXPANSION_FIELD = "_pendingExpansion";
    private static final String EXPANSION_BASE_FIELD = "basePartitions";
    private static final String EXPANSION_TARGET_FIELD = "targetPartitions";
    private static final String PROPERTIES_SOURCE_REVISION_FIELD =
        "_propertiesSourceRevision";
    static final long NO_PROPERTIES_SOURCE_REVISION = -1L;

    private final AsyncOxiaClient oxiaClient;
    private final CatalogPaths catalogPaths;
    private final LongFunction<CompletableFuture<Void>> retryDelay;

    IndexedStreamConfigStore(AsyncOxiaClient oxiaClient, CatalogPaths catalogPaths) {
        this(oxiaClient, catalogPaths, delayMillis -> CompletableFuture.<Void>supplyAsync(
            () -> null,
            CompletableFuture.delayedExecutor(delayMillis, TimeUnit.MILLISECONDS)));
    }

    IndexedStreamConfigStore(
            AsyncOxiaClient oxiaClient, CatalogPaths catalogPaths,
            LongFunction<CompletableFuture<Void>> retryDelay) {
        this.oxiaClient = Objects.requireNonNull(oxiaClient, "oxiaClient");
        this.catalogPaths = Objects.requireNonNull(catalogPaths, "catalogPaths");
        this.retryDelay = Objects.requireNonNull(retryDelay, "retryDelay");
    }


    CompletableFuture<ActiveStreamConfig> readActive(StreamIdentifier id) {
        String path = catalogPaths.streamConfigPath(id);
        return oxiaClient.get(path).thenCompose(result -> {
            if (result == null) {
                return readTombstone(id).thenApply(tombstone -> {
                    if (tombstone.isPresent()) {
                        throw new StreamPermanentlyDeletedException(id);
                    }
                    throw new NoSuchStreamException(id);
                });
            }
            StreamConfigData config = parse(id, result.value());
            if (config.provisioningState() == ProvisioningState.DROPPED) {
                throw new StreamPermanentlyDeletedException(id);
            }
            if (config.provisioningState() != ProvisioningState.ACTIVE) {
                // A PROVISIONING or ABORTING record left behind by a drop that completed its
                // tombstone but not its config delete is still a permanently deleted identity.
                // The tombstone is only consulted once the record is known not to be ACTIVE.
                return readTombstone(id).thenApply(tombstone -> {
                    if (tombstone.isPresent()) {
                        throw new StreamPermanentlyDeletedException(id);
                    }
                    throw new NoSuchStreamException(id);
                });
            }
            return CompletableFuture.completedFuture(
                new ActiveStreamConfig(config, result.version().versionId()));
        });
    }

    /**
     * Reads the completed-deletion tombstone from its dedicated key.
     *
     * <p>Tombstones are written outside the namespace config prefix so a namespace listing never
     * reads them. Legacy deployments still carry the tombstone in the config record itself, which
     * every caller of this method also keeps honouring.
     */
    private CompletableFuture<Optional<CompletedDrop>> readTombstone(StreamIdentifier id) {
        return oxiaClient.get(catalogPaths.streamTombstonePath(id)).thenApply(result -> {
            if (result == null) {
                return Optional.empty();
            }
            StreamConfigData config = parse(id, result.value());
            return Optional.of(new CompletedDrop(config, result.version().versionId()));
        });
    }

    CompletableFuture<List<StreamCatalogEntry>> listStreamEntries(String namespaceName) {
        Objects.requireNonNull(namespaceName, "namespaceName");
        String prefix = catalogPaths.streamConfigPrefix(namespaceName);
        return oxiaClient.list(prefix, prefix + "\uffff")
            .thenCompose(keys -> listStreamEntries(
                namespaceName, prefix, keys.stream().sorted().toList(), 0,
                new ArrayList<>()));
    }

    private CompletableFuture<List<StreamCatalogEntry>> listStreamEntries(
            String namespaceName, String prefix, List<String> keys, int index,
            List<StreamCatalogEntry> entries) {
        if (index >= keys.size()) {
            return CompletableFuture.completedFuture(List.copyOf(entries));
        }
        int end = Math.min(index + LIST_BATCH_SIZE, keys.size());
        List<CompletableFuture<Optional<StreamCatalogEntry>>> reads = keys.subList(index, end)
            .stream()
            .map(key -> readStreamCatalogEntry(namespaceName, prefix, key))
            .toList();
        return CompletableFuture.allOf(reads.toArray(CompletableFuture[]::new))
            .thenCompose(ignored -> {
                reads.forEach(read -> read.join().ifPresent(entries::add));
                return listStreamEntries(namespaceName, prefix, keys, end, entries);
            });
    }

    private CompletableFuture<Optional<StreamCatalogEntry>> readStreamCatalogEntry(
            String namespaceName, String prefix, String key) {
        if (!key.startsWith(prefix) || key.length() == prefix.length()) {
            return CompletableFuture.completedFuture(Optional.empty());
        }
        StreamIdentifier id = new StreamIdentifier(namespaceName, key.substring(prefix.length()));
        return oxiaClient.get(key).thenApply(result -> {
            if (result == null) {
                return Optional.empty();
            }
            StreamConfigData config = parse(id, result.value());
            LifecycleState state = switch (config.provisioningState()) {
                case PROVISIONING -> LifecycleState.CREATING;
                case ACTIVE -> LifecycleState.ACTIVE;
                case ABORTING -> LifecycleState.DELETING;
                case DROPPED -> null;
            };
            return state == null
                ? Optional.empty()
                : Optional.of(new StreamCatalogEntry(
                    id, state, config.properties(), result.version().versionId()));
        });
    }


    CompletableFuture<Void> verifyActiveOwnership(
            StreamIdentifier id, ActiveStreamConfig expected) {
        String path = catalogPaths.streamConfigPath(id);
        return oxiaClient.get(path).thenCompose(result -> {
            if (result == null) {
                return CompletableFuture.failedFuture(new NoSuchStreamException(id));
            }
            StreamConfigData current = parse(id, result.value());
            if (current.provisioningState() == ProvisioningState.ACTIVE
                    && current.sameActiveLifecycle(expected.config(),
                    result.version().versionId(), expected.versionId())) {
                return CompletableFuture.completedFuture(null);
            }
            return CompletableFuture.failedFuture(new NoSuchStreamException(id));
        });
    }

    CompletableFuture<ExpansionClaim> claimExpansion(
            StreamIdentifier id, int targetPartitions) {
        Objects.requireNonNull(id, "id");
        if (targetPartitions <= 0) {
            throw new IllegalArgumentException("targetPartitions must be positive");
        }
        return claimExpansionAttempt(id, targetPartitions, 0);
    }

    private CompletableFuture<ExpansionClaim> claimExpansionAttempt(
            StreamIdentifier id, int targetPartitions, int retryCount) {
        String path = catalogPaths.streamConfigPath(id);
        return oxiaClient.get(path).thenCompose(result -> {
            if (result == null) {
                return CompletableFuture.failedFuture(new NoSuchStreamException(id));
            }
            StreamConfigData current = parse(id, result.value());
            if (current.provisioningState() != ProvisioningState.ACTIVE) {
                return CompletableFuture.failedFuture(new NoSuchStreamException(id));
            }
            if (current.creationKind().orElse(null) != CreationKind.NATIVE_CREATE) {
                return CompletableFuture.failedFuture(new AlreadyExistsException(
                    "Only native streams can increase partitions: " + id.fullName()));
            }
            int effectiveTarget = Math.max(targetPartitions,
                current.pendingExpansion()
                    .map(PendingExpansion::targetPartitions)
                    .orElse(current.partitions()));
            if (effectiveTarget <= current.partitions()) {
                return CompletableFuture.completedFuture(new ExpansionClaim(
                    current, current.partitions(), current.partitions(),
                    result.version().versionId()));
            }
            PendingExpansion desiredExpansion = current.pendingExpansion()
                .map(expansion -> expansion.growTo(effectiveTarget))
                .orElseGet(() -> new PendingExpansion(
                    current.partitions(), effectiveTarget));
            if (current.pendingExpansion().equals(Optional.of(desiredExpansion))) {
                return CompletableFuture.completedFuture(new ExpansionClaim(
                    current, desiredExpansion.basePartitions(),
                    desiredExpansion.targetPartitions(), result.version().versionId()));
            }
            StreamConfigData desired = current.withPendingExpansion(desiredExpansion);
            return putWithResult(path, desired,
                    Set.of(PutOption.IfVersionIdEquals(result.version().versionId())))
                .handle((write, failure) ->
                    new CreateWrite(write, unwrapNullable(failure)))
                .thenCompose(write -> {
                    if (write.failure() == null) {
                        return CompletableFuture.completedFuture(new ExpansionClaim(
                            desired, desiredExpansion.basePartitions(),
                            desiredExpansion.targetPartitions(),
                            write.result().version().versionId()));
                    }
                    if (write.failure() instanceof UnexpectedVersionIdException) {
                        return retryAfterConflict(
                            id, "stream partition expansion claim", retryCount,
                            write.failure(), () -> claimExpansionAttempt(
                                id, effectiveTarget, retryCount + 1));
                    }
                    return CompletableFuture.failedFuture(write.failure());
                });
        });
    }

    CompletableFuture<Void> verifyExpansion(
            StreamIdentifier id, ExpansionClaim claim) {
        String path = catalogPaths.streamConfigPath(id);
        return oxiaClient.get(path).thenCompose(result -> {
            if (result == null) {
                return CompletableFuture.failedFuture(
                    new ExpansionOwnershipLostException(id));
            }
            StreamConfigData current = parse(id, result.value());
            boolean alreadyCommitted = current.partitions() >= claim.targetPartitions();
            boolean stillPending = current.partitions() == claim.basePartitions()
                && current.pendingExpansion()
                    .map(PendingExpansion::targetPartitions)
                    .filter(target -> target >= claim.targetPartitions())
                    .isPresent();
            if (current.provisioningState() == ProvisioningState.ACTIVE
                    && current.sameStreamLifecycle(claim.config())
                    && (alreadyCommitted || stillPending)) {
                return CompletableFuture.completedFuture(null);
            }
            return CompletableFuture.failedFuture(
                new ExpansionOwnershipLostException(id));
        });
    }

    CompletableFuture<ExpansionFinalization> finalizeExpansion(
            StreamIdentifier id, ExpansionClaim claim) {
        return finalizeExpansionAttempt(id, claim, 0);
    }

    private CompletableFuture<ExpansionFinalization> finalizeExpansionAttempt(
            StreamIdentifier id, ExpansionClaim claim, int retryCount) {
        String path = catalogPaths.streamConfigPath(id);
        return oxiaClient.get(path).thenCompose(result -> {
            if (result == null) {
                return CompletableFuture.failedFuture(
                    new ExpansionOwnershipLostException(id));
            }
            StreamConfigData current = parse(id, result.value());
            if (current.provisioningState() != ProvisioningState.ACTIVE
                    || !current.sameStreamLifecycle(claim.config())) {
                return CompletableFuture.failedFuture(
                    new ExpansionOwnershipLostException(id));
            }
            if (current.partitions() >= claim.targetPartitions()) {
                return CompletableFuture.completedFuture(
                    ExpansionFinalization.complete(current.partitions()));
            }
            PendingExpansion expansion = current.pendingExpansion()
                .orElseThrow(() -> new ExpansionOwnershipLostException(id));
            if (expansion.basePartitions() != claim.basePartitions()) {
                return CompletableFuture.failedFuture(
                    new ExpansionOwnershipLostException(id));
            }
            if (expansion.targetPartitions() > claim.targetPartitions()) {
                return CompletableFuture.completedFuture(
                    ExpansionFinalization.continueWith(expansion.targetPartitions()));
            }
            if (expansion.targetPartitions() < claim.targetPartitions()) {
                return CompletableFuture.failedFuture(
                    new ExpansionOwnershipLostException(id));
            }
            StreamConfigData committed = current.commitExpansion(expansion);
            return put(path, committed,
                    Set.of(PutOption.IfVersionIdEquals(result.version().versionId())))
                .handle((ignored, failure) -> unwrapNullable(failure))
                .thenCompose(failure -> {
                    if (failure == null) {
                        return CompletableFuture.completedFuture(
                            ExpansionFinalization.complete(
                                expansion.targetPartitions()));
                    }
                    if (failure instanceof UnexpectedVersionIdException) {
                        return retryAfterConflict(
                            id, "stream partition expansion finalization", retryCount,
                            failure, () -> finalizeExpansionAttempt(
                                id, claim, retryCount + 1));
                    }
                    return CompletableFuture.failedFuture(failure);
                });
        });
    }

    CompletableFuture<ActiveStreamConfig> replaceProperties(
            StreamIdentifier id, Map<String, String> properties, long sourceRevision) {
        Objects.requireNonNull(id, "id");
        Objects.requireNonNull(properties, "properties");
        if (sourceRevision < 0) {
            throw new IllegalArgumentException("sourceRevision must be non-negative");
        }
        return replacePropertiesAttempt(id, Map.copyOf(properties), sourceRevision, 0);
    }

    private CompletableFuture<ActiveStreamConfig> replacePropertiesAttempt(
            StreamIdentifier id, Map<String, String> properties,
            long sourceRevision, int retryCount) {
        String path = catalogPaths.streamConfigPath(id);
        return oxiaClient.get(path).thenCompose(result -> {
            if (result == null) {
                return CompletableFuture.failedFuture(new NoSuchStreamException(id));
            }
            StreamConfigData current = parse(id, result.value());
            if (current.provisioningState() != ProvisioningState.ACTIVE) {
                return CompletableFuture.failedFuture(new NoSuchStreamException(id));
            }
            if (sourceRevision <= current.propertiesSourceRevision()) {
                return CompletableFuture.completedFuture(new ActiveStreamConfig(
                    current, result.version().versionId()));
            }
            StreamConfigData replacement = current.replaceProperties(
                properties, sourceRevision);
            return putWithResult(path, replacement,
                    Set.of(PutOption.IfVersionIdEquals(result.version().versionId())))
                .handle((write, failure) ->
                    new CreateWrite(write, unwrapNullable(failure)))
                .thenCompose(write -> {
                    if (write.failure() == null) {
                        return CompletableFuture.completedFuture(new ActiveStreamConfig(
                            replacement, write.result().version().versionId()));
                    }
                    if (write.failure() instanceof UnexpectedVersionIdException) {
                        return retryAfterConflict(
                            id, "stream properties replacement", retryCount,
                            write.failure(), () -> replacePropertiesAttempt(
                                id, properties, sourceRevision, retryCount + 1));
                    }
                    return CompletableFuture.failedFuture(write.failure());
                });
        });
    }

    CompletableFuture<Boolean> exists(StreamIdentifier id) {
        return oxiaClient.get(catalogPaths.streamConfigPath(id)).thenApply(result -> {
            if (result == null) {
                return false;
            }
            return parse(id, result.value()).provisioningState() == ProvisioningState.ACTIVE;
        });
    }

    /**
     * Returns whether the namespace contains any stream lifecycle record other than a completed
     * deletion tombstone.
     *
     * <p>This deliberately reads the raw config nodes instead of {@link #exists(StreamIdentifier)}
     * so PROVISIONING and ABORTING records keep a namespace non-empty. It is only a conservative
     * presence check: Oxia has no cross-key transaction tying this scan to deletion of the namespace
     * record, so callers must not treat it as fencing a claim written after the scan.
     */
    CompletableFuture<Boolean> namespaceContainsNonTombstoneStream(String namespaceName) {
        String prefix = catalogPaths.streamConfigPrefix(namespaceName);
        return oxiaClient.list(prefix, prefix + "\uffff")
            .thenCompose(keys -> namespaceContainsNonTombstoneStream(keys, 0));
    }

    private CompletableFuture<Boolean> namespaceContainsNonTombstoneStream(
            List<String> keys, int index) {
        if (index >= keys.size()) {
            return CompletableFuture.completedFuture(false);
        }
        return oxiaClient.get(keys.get(index)).thenCompose(result -> {
            if (result != null && !isLifecycleTombstone(result.value())) {
                return CompletableFuture.completedFuture(true);
            }
            return namespaceContainsNonTombstoneStream(keys, index + 1);
        });
    }

    CompletableFuture<Void> ensureCreatable(StreamIdentifier id) {
        return oxiaClient.get(catalogPaths.streamConfigPath(id)).thenCompose(result -> {
            if (result == null) {
                return readTombstone(id).thenCompose(tombstone -> tombstone.isPresent()
                    ? CompletableFuture.failedFuture(
                        new StreamPermanentlyDeletedException(id))
                    : CompletableFuture.completedFuture(null));
            }
            ProvisioningState state = parse(id, result.value()).provisioningState();
            if (state == ProvisioningState.DROPPED) {
                return CompletableFuture.failedFuture(
                    new StreamPermanentlyDeletedException(id));
            }
            if (state == ProvisioningState.ABORTING) {
                return failAbortingOrPermanentlyDeleted(id,
                    "Stream already exists: " + id.fullName());
            }
            return CompletableFuture.failedFuture(
                new AlreadyExistsException("Stream already exists: " + id.fullName()));
        });
    }

    /**
     * Reports an aborting stream as permanently deleted once its tombstone exists.
     *
     * <p>A drop publishes the tombstone before deleting the config record, so an {@code ABORTING}
     * record observed after that publish belongs to an identity that is already gone forever. Only
     * the aborting branch pays for the extra read; an active record never reads the tombstone.
     */
    private <T> CompletableFuture<T> failAbortingOrPermanentlyDeleted(
            StreamIdentifier id, String stillAbortingMessage) {
        return readTombstone(id).thenCompose(tombstone -> CompletableFuture.failedFuture(
            tombstone.isPresent()
                ? new StreamPermanentlyDeletedException(id)
                : new AlreadyExistsException(stillAbortingMessage)));
    }

    CompletableFuture<ProvisioningClaim> claimCreation(
            StreamIdentifier id, int partitions, Map<String, String> properties,
            Optional<TableMaterializationPolicy> materialization, String ownerToken) {
        return claimCreation(
            id, defaultDefinition(partitions), properties, materialization,
            CreationKind.NATIVE_CREATE, ownerToken);
    }

    CompletableFuture<ProvisioningClaim> claimCreation(
            StreamIdentifier id, int partitions, Map<String, String> properties,
            Optional<TableMaterializationPolicy> materialization,
            CreationKind kind, String ownerToken) {
        return claimCreation(
            id, defaultDefinition(partitions), properties, materialization, kind, ownerToken);
    }

    CompletableFuture<ProvisioningClaim> claimCreation(
            StreamIdentifier id, StreamConfig streamConfig, Partitioning partitioning,
            SchemaConfig schema, Map<String, String> properties,
            Optional<TableMaterializationPolicy> materialization,
            CreationKind kind, String ownerToken) {
        return claimCreation(
            id, new ImmutableStreamDefinition(streamConfig, partitioning, schema),
            properties, materialization, kind, ownerToken);
    }

    private CompletableFuture<ProvisioningClaim> claimCreation(
            StreamIdentifier id, ImmutableStreamDefinition definition,
            Map<String, String> properties,
            Optional<TableMaterializationPolicy> materialization,
            CreationKind kind, String ownerToken) {
        Objects.requireNonNull(id, "id");
        Objects.requireNonNull(definition, "definition");
        Objects.requireNonNull(properties, "properties");
        Objects.requireNonNull(materialization, "materialization");
        Objects.requireNonNull(kind, "kind");
        Objects.requireNonNull(ownerToken, "ownerToken");
        int partitions = definition.partitioning().numPartitions();
        if (partitions <= 0) {
            throw new IllegalArgumentException("partitions must be positive");
        }
        return claimCreationAttempt(
            id, definition, properties, materialization, kind, ownerToken,
            Optional.empty(), 0);
    }

    private CompletableFuture<ProvisioningClaim> claimCreationAttempt(
            StreamIdentifier id, ImmutableStreamDefinition definition,
            Map<String, String> properties,
            Optional<TableMaterializationPolicy> materialization,
            CreationKind kind, String ownerToken,
            Optional<String> requiredIncarnation, int retryCount) {
        String path = catalogPaths.streamConfigPath(id);
        int partitions = definition.partitioning().numPartitions();
        return oxiaClient.get(path).thenCompose(result -> {
            if (result == null) {
                if (requiredIncarnation.isPresent()) {
                    return CompletableFuture.failedFuture(new AlreadyExistsException(
                        "Stream creation lifecycle changed: " + id.fullName()));
                }
                return readTombstone(id).thenCompose(tombstone -> {
                    if (tombstone.isPresent()) {
                        return CompletableFuture.failedFuture(
                            new StreamPermanentlyDeletedException(id));
                    }
                    StreamConfigData desired = StreamConfigData.provisioning(
                        partitions, properties, materialization, definition, kind,
                        UUID.randomUUID().toString(), ownerToken);
                    return writeInitialClaim(id, desired, retryCount);
                });
            }
            StreamConfigData current = parse(id, result.value());
            if (requiredIncarnation.isPresent()
                    && !current.incarnationId().equals(requiredIncarnation)) {
                return CompletableFuture.failedFuture(new AlreadyExistsException(
                    "Stream creation lifecycle changed: " + id.fullName()));
            }
            if (current.provisioningState() == ProvisioningState.DROPPED) {
                return CompletableFuture.failedFuture(
                    new StreamPermanentlyDeletedException(id));
            }
            if (!current.provisioning()) {
                return CompletableFuture.failedFuture(new AlreadyExistsException(
                    "Stream already exists: " + id.fullName()));
            }
            if (current.provisioningState() == ProvisioningState.ABORTING) {
                return failAbortingOrPermanentlyDeleted(id,
                    "Stream creation is aborting: " + id.fullName());
            }
            if (!current.canResumeCreation(definition, kind)) {
                return CompletableFuture.failedFuture(new AlreadyExistsException(
                    "A different stream creation is already provisioning: " + id.fullName()));
            }
            return convergeProvisioningClaim(
                id, current, result.version().versionId(), definition,
                Map.copyOf(properties), materialization, kind, ownerToken, retryCount);
        });
    }

    /**
     * Resumes an existing {@code PROVISIONING} claim unless the identity is already tombstoned.
     *
     * <p>A creation that crashed between writing its claim and validating the deletion fence leaves
     * a claim that no longer has an owner. Reading the tombstone before adopting such a claim closes
     * that window: the orphan is deleted by version and the creation loses to the fence, exactly as
     * {@link #writeInitialClaim} would have done had it survived.
     */
    private CompletableFuture<ProvisioningClaim> convergeProvisioningClaim(
            StreamIdentifier id,
            StreamConfigData current,
            long currentVersion,
            ImmutableStreamDefinition requestedDefinition,
            Map<String, String> requestedProperties,
            Optional<TableMaterializationPolicy> requestedMaterialization,
            CreationKind kind,
            String requestingOwnerToken,
            int retryCount) {
        return readTombstone(id).thenCompose(tombstone -> tombstone.isPresent()
            ? rollBackFencedClaim(id, claim(current, currentVersion))
            : resumeProvisioningClaim(
                id, current, currentVersion, requestedDefinition, requestedProperties,
                requestedMaterialization, kind, requestingOwnerToken, retryCount));
    }

    private CompletableFuture<ProvisioningClaim> resumeProvisioningClaim(
            StreamIdentifier id,
            StreamConfigData current,
            long currentVersion,
            ImmutableStreamDefinition requestedDefinition,
            Map<String, String> requestedProperties,
            Optional<TableMaterializationPolicy> requestedMaterialization,
            CreationKind kind,
            String requestingOwnerToken,
            int retryCount) {
        StreamConfigData desired = current.resumeCreation(
            requestedDefinition, requestedProperties, requestedMaterialization);
        if (desired.equals(current)) {
            return CompletableFuture.completedFuture(claim(current, currentVersion));
        }

        String path = catalogPaths.streamConfigPath(id);
        return putWithResult(path, desired, Set.of(PutOption.IfVersionIdEquals(currentVersion)))
            .handle((result, failure) ->
                new CreateWrite(result, unwrapNullable(failure)))
            .thenCompose(write -> {
                if (write.failure() == null) {
                    return CompletableFuture.completedFuture(claim(
                        desired, write.result().version().versionId()));
                }
                return oxiaClient.get(path)
                    .handle((observed, readFailure) ->
                        new ConfigCheck(observed, unwrapNullable(readFailure)))
                    .thenCompose(check -> {
                        if (check.failure() != null) {
                            write.failure().addSuppressed(check.failure());
                            return CompletableFuture.failedFuture(write.failure());
                        }
                        if (check.config() != null) {
                            StreamConfigData observed = parse(id, check.config().value());
                            if (observed.equals(desired)) {
                                logResolvedWrite(id, "stream provisioning intent",
                                    check.config().version().versionId(), write.failure());
                                return CompletableFuture.completedFuture(claim(
                                    observed, check.config().version().versionId()));
                            }
                        }
                        if (write.failure() instanceof UnexpectedVersionIdException) {
                            return retryAfterConflict(
                                id, "stream provisioning intent", retryCount,
                                write.failure(), () -> claimCreationAttempt(
                                    id, requestedDefinition, requestedProperties,
                                    requestedMaterialization, kind, requestingOwnerToken,
                                    current.incarnationId(), retryCount + 1));
                        }
                        return CompletableFuture.failedFuture(write.failure());
                    });
            });
    }

    private CompletableFuture<ProvisioningClaim> writeInitialClaim(
            StreamIdentifier id, StreamConfigData desired, int retryCount) {
        String path = catalogPaths.streamConfigPath(id);
        return putWithResult(path, desired, Set.of(PutOption.IfRecordDoesNotExist))
            .handle((result, failure) ->
                new CreateWrite(result, unwrapNullable(failure)))
            .thenCompose(write -> resolveClaimWrite(
                id, desired, -1L, write, true, retryCount))
            .thenCompose(claim -> validateDeletionFence(id, claim));
    }

    /**
     * Fails a freshly written creation claim when a deletion fence appeared while it was in flight.
     *
     * <p>The config record and its tombstone are separate Oxia keys with no cross-key transaction,
     * so a drop that fences an absent identity can land between this creation's tombstone check and
     * its claim write. Re-reading the tombstone after the write turns that interleaving into a
     * losing creation instead of a silently resurrected identity.
     */
    private CompletableFuture<ProvisioningClaim> validateDeletionFence(
            StreamIdentifier id, ProvisioningClaim claim) {
        return readTombstone(id).thenCompose(tombstone -> tombstone.isEmpty()
            ? CompletableFuture.completedFuture(claim)
            : rollBackFencedClaim(id, claim));
    }

    private CompletableFuture<ProvisioningClaim> rollBackFencedClaim(
            StreamIdentifier id, ProvisioningClaim claim) {
        StreamPermanentlyDeletedException fenced = new StreamPermanentlyDeletedException(id);
        return oxiaClient.delete(catalogPaths.streamConfigPath(id),
                Set.of(DeleteOption.IfVersionIdEquals(claim.versionId())))
            .handle((deleted, failure) -> unwrapNullable(failure))
            .thenCompose(failure -> {
                // A version mismatch means someone else already moved the record on; the fence
                // still stands, so only an unexpected failure is worth reporting.
                if (failure != null && !(failure instanceof UnexpectedVersionIdException)) {
                    log.warn("Failed to roll back the creation claim for {} that lost a race "
                            + "with a permanent-deletion fence",
                        id.fullName(), failure);
                    fenced.addSuppressed(failure);
                }
                return CompletableFuture.<ProvisioningClaim>failedFuture(fenced);
            });
    }

    private CompletableFuture<ProvisioningClaim> resolveClaimWrite(
            StreamIdentifier id, StreamConfigData desired, long previousVersion,
            CreateWrite write, boolean initialWrite, int retryCount) {
        if (write.failure() == null) {
            return CompletableFuture.completedFuture(claim(
                desired, write.result().version().versionId()));
        }
        String path = catalogPaths.streamConfigPath(id);
        return oxiaClient.get(path)
            .handle((current, readFailure) ->
                new ConfigCheck(current, unwrapNullable(readFailure)))
            .thenCompose(check -> {
                if (check.failure() != null) {
                    write.failure().addSuppressed(check.failure());
                    return CompletableFuture.failedFuture(write.failure());
                }
                if (check.config() != null) {
                    StreamConfigData current = parse(id, check.config().value());
                    if (current.provisioningState() == ProvisioningState.DROPPED) {
                        return CompletableFuture.failedFuture(
                            new StreamPermanentlyDeletedException(id, write.failure()));
                    }
                    if (current.isOwnedBy(desired)) {
                        logResolvedWrite(id, "stream creation claim",
                            check.config().version().versionId(), write.failure());
                        return CompletableFuture.completedFuture(claim(
                            current, check.config().version().versionId()));
                    }
                }
                if (write.failure() instanceof KeyAlreadyExistsException
                        || write.failure() instanceof UnexpectedVersionIdException) {
                    return retryAfterConflict(
                        id, "stream creation claim", retryCount, write.failure(),
                        () -> claimCreationAttempt(
                            id, desired.definition(), desired.properties(),
                            desired.materialization(), desired.creationKind().orElseThrow(),
                            desired.ownerToken().orElseThrow(),
                            initialWrite ? Optional.empty() : desired.incarnationId(),
                            retryCount + 1));
                }
                if (!initialWrite && check.config() != null
                        && check.config().version().versionId() == previousVersion) {
                    return CompletableFuture.failedFuture(write.failure());
                }
                return CompletableFuture.failedFuture(write.failure());
            });
    }

    private static ProvisioningClaim claim(StreamConfigData config, long versionId) {
        return new ProvisioningClaim(
            config, config.incarnationId().orElseThrow(),
            config.ownerToken().orElseThrow(), config.creationKind().orElseThrow(),
            config.ownerGeneration(), versionId);
    }

    CompletableFuture<Void> verifyProvisioningOwnership(
            StreamIdentifier id, ProvisioningClaim claim) {
        String path = catalogPaths.streamConfigPath(id);
        return oxiaClient.get(path).thenCompose(current -> {
            if (current == null) {
                return CompletableFuture.failedFuture(
                    new ProvisioningOwnershipLostException(id));
            }
            StreamConfigData config = parse(id, current.value());
            if (config.provisioningState() == ProvisioningState.PROVISIONING
                    && current.version().versionId() == claim.versionId()
                    && config.isOwnedBy(claim)) {
                return CompletableFuture.completedFuture(null);
            }
            log.debug("Provisioning ownership lost for stream {}: expected version {} and "
                    + "generation {}, observed version {}, generation {}, state {}, "
                    + "owner-token-match={}",
                id.fullName(), claim.versionId(), claim.ownerGeneration(),
                current.version().versionId(), config.ownerGeneration(),
                config.provisioningState(),
                config.ownerToken().equals(Optional.of(claim.ownerToken())));
            return CompletableFuture.failedFuture(
                new ProvisioningOwnershipLostException(id));
        });
    }

    /**
     * Snapshots the lifecycle record that governs orphaned-allocation cleanup.
     *
     * <p>A completed deletion no longer keeps a config record, so the tombstone is the lifecycle
     * record once the config key is gone. {@link #verifyNativeCleanupContext} resolves the same
     * two keys in the same order so a snapshot and its later verification always agree.
     */
    CompletableFuture<LifecycleContext> readLifecycleContext(StreamIdentifier id) {
        String path = catalogPaths.streamConfigPath(id);
        return oxiaClient.get(path).thenCompose(current -> {
            if (current != null) {
                return CompletableFuture.completedFuture(new LifecycleContext(
                    Optional.of(parse(id, current.value())), current.version().versionId()));
            }
            return readTombstone(id).thenApply(tombstone -> tombstone
                .map(dropped -> new LifecycleContext(
                    Optional.of(dropped.config()), dropped.versionId()))
                .orElseGet(() -> new LifecycleContext(Optional.empty(), -1L)));
        });
    }

    CompletableFuture<Void> verifyNativeCleanupContext(
            StreamIdentifier id, NativeCleanupContext expected) {
        String path = catalogPaths.streamConfigPath(id);
        return oxiaClient.get(path).thenCompose(current -> {
            if (current != null) {
                return verifyNativeCleanupContext(id, expected, current);
            }
            return oxiaClient.get(catalogPaths.streamTombstonePath(id))
                .thenCompose(tombstone -> {
                    if (tombstone == null) {
                        return expected.config().isEmpty()
                            ? CompletableFuture.<Void>completedFuture(null)
                            : CompletableFuture.<Void>failedFuture(
                                new AbortingOwnershipLostException(id));
                    }
                    return verifyNativeCleanupContext(id, expected, tombstone);
                });
        });
    }

    private CompletableFuture<Void> verifyNativeCleanupContext(
            StreamIdentifier id, NativeCleanupContext expected, GetResult current) {
        if (expected.config().isEmpty()
                || current.version().versionId() != expected.versionId()) {
            return CompletableFuture.failedFuture(
                new AbortingOwnershipLostException(id));
        }
        StreamConfigData expectedConfig = expected.config().orElseThrow();
        StreamConfigData observed = parse(id, current.value());
        if (observed.incarnationId().equals(expectedConfig.incarnationId())
                && observed.ownerToken().equals(expectedConfig.ownerToken())
                && observed.ownerGeneration() == expectedConfig.ownerGeneration()
                && observed.metadataSourceOwnerToken().equals(
                    expectedConfig.metadataSourceOwnerToken())
                && observed.metadataSourceGeneration()
                    == expectedConfig.metadataSourceGeneration()
                && observed.creationKind().equals(expectedConfig.creationKind())
                && observed.provisioningState()
                    == expectedConfig.provisioningState()
                && observed.purgeRequested() == expectedConfig.purgeRequested()) {
            return CompletableFuture.completedFuture(null);
        }
        return CompletableFuture.failedFuture(
            new AbortingOwnershipLostException(id));
    }

    CompletableFuture<FinalizeOutcome> finalizeCreation(
            StreamIdentifier id, ProvisioningClaim claim) {
        StreamConfigData active = claim.config().activate();
        String path = catalogPaths.streamConfigPath(id);
        return put(path, active, Set.of(PutOption.IfVersionIdEquals(claim.versionId())))
            .handle((ignored, failure) -> unwrapNullable(failure))
            .thenCompose(failure -> {
                if (failure == null) {
                    return CompletableFuture.completedFuture(FinalizeOutcome.successful());
                }
                return oxiaClient.get(path)
                    .handle((current, readFailure) ->
                        new ConfigCheck(current, unwrapNullable(readFailure)))
                    .thenApply(check -> {
                        if (check.failure() != null) {
                            failure.addSuppressed(check.failure());
                            return FinalizeOutcome.indeterminate(failure);
                        }
                        if (check.config() == null) {
                            return FinalizeOutcome.indeterminate(failure);
                        }
                        StreamConfigData current = parse(id, check.config().value());
                        if (current.provisioningState() == ProvisioningState.ACTIVE
                                && current.isOwnedBy(claim)) {
                            logResolvedWrite(id, "stream creation finalization",
                                check.config().version().versionId(), failure);
                            return FinalizeOutcome.successful();
                        }
                        return FinalizeOutcome.indeterminate(failure);
                    });
            });
    }

    CompletableFuture<Optional<DropClaim>> beginDrop(
            StreamIdentifier id, String ownerToken) {
        return beginDrop(id, ownerToken, false);
    }

    CompletableFuture<Optional<DropClaim>> beginDrop(
            StreamIdentifier id, String ownerToken, boolean purge) {
        Objects.requireNonNull(id, "id");
        Objects.requireNonNull(ownerToken, "ownerToken");
        return beginDropAttempt(id, ownerToken, purge, 0);
    }

    private CompletableFuture<Optional<DropClaim>> beginDropAttempt(
            StreamIdentifier id, String ownerToken, boolean purge, int retryCount) {
        String path = catalogPaths.streamConfigPath(id);
        return oxiaClient.get(path).thenCompose(current -> {
            if (current == null) {
                return dropAbsentStream(id, ownerToken, purge, retryCount);
            }
            StreamConfigData config = parse(id, current.value());
            if (config.provisioningState() == ProvisioningState.DROPPED) {
                if (!purge || config.purgeRequested()) {
                    return CompletableFuture.completedFuture(Optional.empty());
                }
                if (config.partitions() == 0 && config.incarnationId().isEmpty()) {
                    StreamConfigData upgraded = config.requestPurge();
                    return put(path, upgraded,
                            Set.of(PutOption.IfVersionIdEquals(
                                current.version().versionId())))
                        .handle((ignored, failure) -> unwrapNullable(failure))
                        .thenCompose(failure -> {
                            if (failure == null) {
                                return CompletableFuture.completedFuture(Optional.empty());
                            }
                            if (failure instanceof UnexpectedVersionIdException) {
                                return retryAfterConflict(
                                    id, "absent stream purge upgrade", retryCount,
                                    failure, () -> beginDropAttempt(
                                        id, ownerToken, true, retryCount + 1));
                            }
                            return CompletableFuture.failedFuture(failure);
                        });
                }
                StreamConfigData aborting = config.abort(ownerToken, true);
                return putWithResult(path, aborting,
                        Set.of(PutOption.IfVersionIdEquals(
                            current.version().versionId())))
                    .handle((result, failure) ->
                        new CreateWrite(result, unwrapNullable(failure)))
                    .thenCompose(write -> resolveDropClaimWrite(
                        id, ownerToken, aborting, write, retryCount));
            }
            if (config.provisioningState() == ProvisioningState.ABORTING
                    && config.ownerToken().equals(Optional.of(ownerToken))) {
                if (purge && !config.purgeRequested()) {
                    StreamConfigData upgraded = config.requestPurge();
                    return putWithResult(path, upgraded,
                            Set.of(PutOption.IfVersionIdEquals(
                                current.version().versionId())))
                        .handle((result, failure) ->
                            new CreateWrite(result, unwrapNullable(failure)))
                        .thenCompose(write -> {
                            if (write.failure() == null) {
                                return CompletableFuture.completedFuture(Optional.of(
                                    new DropClaim(upgraded, ownerToken,
                                        write.result().version().versionId())));
                            }
                            if (write.failure() instanceof UnexpectedVersionIdException) {
                                return retryAfterConflict(
                                    id, "in-flight stream purge upgrade", retryCount,
                                    write.failure(), () -> beginDropAttempt(
                                        id, ownerToken, true, retryCount + 1));
                            }
                            return CompletableFuture.failedFuture(write.failure());
                        });
                }
                return CompletableFuture.completedFuture(Optional.of(new DropClaim(
                    config, ownerToken, current.version().versionId())));
            }
            StreamConfigData aborting = config.abort(ownerToken, purge);
            return putWithResult(path, aborting,
                    Set.of(PutOption.IfVersionIdEquals(current.version().versionId())))
                .handle((result, failure) ->
                    new CreateWrite(result, unwrapNullable(failure)))
                .thenCompose(write -> resolveDropClaimWrite(
                    id, ownerToken, aborting, write, retryCount));
        });
    }

    /**
     * Fences an identity whose config record is already gone by writing its tombstone.
     *
     * <p>An absent config record means the stream never existed or its deletion already completed,
     * so there is nothing left to claim. The tombstone keeps the identity permanently deleted, and
     * a purge requested after the fact is merged into the existing tombstone.
     *
     * <p>Every retry goes back through {@link #beginDropAttempt} rather than straight back here:
     * the config record is re-read first, so a stream created while the purge upgrade was in
     * flight is claimed and dropped for real instead of being fenced as absent.
     */
    private CompletableFuture<Optional<DropClaim>> dropAbsentStream(
            StreamIdentifier id, String ownerToken, boolean purge, int retryCount) {
        return fenceTombstone(
            id, StreamConfigData.emptyDropped(purge), !purge,
            StreamConfigData::purgeRequested,
            StreamConfigData::requestPurge,
            () -> CompletableFuture.completedFuture(Optional.<DropClaim>empty()),
            "absent stream deletion tombstone", "absent stream purge upgrade",
            retryCount, () -> beginDropAttempt(id, ownerToken, true, retryCount + 1));
    }

    /**
     * Publishes {@code payload} as the identity's permanent-deletion tombstone and converges on a
     * fence that is already there.
     *
     * <p>The tombstone key is written create-only, so whoever wins fences the identity and every
     * later writer has to decide what the standing fence still owes it: nothing at all
     * ({@code alreadySatisfied}), nothing given what it already says
     * ({@code satisfiedByExisting}), or an upgrade written by version ({@code merge}). Losing that
     * upgrade, or finding the fence gone between the create and the read, goes back through
     * {@code retry} carrying {@code retryCount}, so the whole attempt re-reads the config record
     * instead of blindly rewriting the tombstone.
     */
    private <T> CompletableFuture<T> fenceTombstone(
            StreamIdentifier id, StreamConfigData payload, boolean alreadySatisfied,
            Predicate<StreamConfigData> satisfiedByExisting,
            UnaryOperator<StreamConfigData> merge,
            Supplier<CompletableFuture<T>> onDone,
            String createOperation, String upgradeOperation,
            int retryCount, Supplier<CompletableFuture<T>> retry) {
        String tombstonePath = catalogPaths.streamTombstonePath(id);
        return putWithResult(tombstonePath, payload, Set.of(PutOption.IfRecordDoesNotExist))
            .handle((result, failure) -> unwrapNullable(failure))
            .thenCompose(failure -> {
                if (failure == null) {
                    return onDone.get();
                }
                if (!(failure instanceof KeyAlreadyExistsException)) {
                    return CompletableFuture.<T>failedFuture(failure);
                }
                if (alreadySatisfied) {
                    return onDone.get();
                }
                return upgradeTombstone(
                    id, tombstonePath, satisfiedByExisting, merge, onDone,
                    createOperation, upgradeOperation, retryCount, retry, failure);
            });
    }

    private <T> CompletableFuture<T> upgradeTombstone(
            StreamIdentifier id, String tombstonePath,
            Predicate<StreamConfigData> satisfiedByExisting,
            UnaryOperator<StreamConfigData> merge,
            Supplier<CompletableFuture<T>> onDone,
            String createOperation, String upgradeOperation,
            int retryCount, Supplier<CompletableFuture<T>> retry, Throwable conflict) {
        return oxiaClient.get(tombstonePath).thenCompose(existing -> {
            if (existing == null) {
                return retryAfterConflict(id, createOperation, retryCount, conflict, retry);
            }
            StreamConfigData tombstone = parse(id, existing.value());
            if (satisfiedByExisting.test(tombstone)) {
                return onDone.get();
            }
            return put(tombstonePath, merge.apply(tombstone),
                    Set.of(PutOption.IfVersionIdEquals(existing.version().versionId())))
                .handle((ignored, failure) -> unwrapNullable(failure))
                .thenCompose(failure -> {
                    if (failure == null) {
                        return onDone.get();
                    }
                    if (failure instanceof UnexpectedVersionIdException) {
                        return retryAfterConflict(
                            id, upgradeOperation, retryCount, failure, retry);
                    }
                    return CompletableFuture.<T>failedFuture(failure);
                });
        });
    }

    private CompletableFuture<Optional<DropClaim>> resolveDropClaimWrite(
            StreamIdentifier id, String ownerToken, StreamConfigData desired,
            CreateWrite write, int retryCount) {
        if (write.failure() == null) {
            return CompletableFuture.completedFuture(Optional.of(new DropClaim(
                desired, ownerToken, write.result().version().versionId())));
        }
        String path = catalogPaths.streamConfigPath(id);
        return oxiaClient.get(path)
            .handle((current, readFailure) ->
                new ConfigCheck(current, unwrapNullable(readFailure)))
            .thenCompose(check -> {
                if (check.failure() != null) {
                    write.failure().addSuppressed(check.failure());
                    return CompletableFuture.failedFuture(write.failure());
                }
                if (check.config() == null
                        || isLifecycleTombstone(check.config().value())) {
                    return CompletableFuture.completedFuture(Optional.empty());
                }
                StreamConfigData current = parse(id, check.config().value());
                if (current.sameAbortingOwner(desired)) {
                    logResolvedWrite(id, "stream drop claim",
                        check.config().version().versionId(), write.failure());
                    return CompletableFuture.completedFuture(Optional.of(new DropClaim(
                        current, ownerToken, check.config().version().versionId())));
                }
                if (write.failure() instanceof KeyAlreadyExistsException
                        || write.failure() instanceof UnexpectedVersionIdException) {
                    return retryAfterConflict(
                        id, "stream drop claim", retryCount, write.failure(),
                        () -> beginDropAttempt(
                            id, ownerToken, desired.purgeRequested(), retryCount + 1));
                }
                return CompletableFuture.failedFuture(write.failure());
            });
    }

    CompletableFuture<Void> verifyAbortingOwnership(
            StreamIdentifier id, DropClaim claim) {
        String path = catalogPaths.streamConfigPath(id);
        return oxiaClient.get(path).thenCompose(current -> {
            if (current == null) {
                return CompletableFuture.failedFuture(
                    new AbortingOwnershipLostException(id));
            }
            StreamConfigData config = parse(id, current.value());
            if (current.version().versionId() == claim.versionId()
                    && config.isOwnedBy(claim)) {
                return CompletableFuture.completedFuture(null);
            }
            return CompletableFuture.failedFuture(
                new AbortingOwnershipLostException(id));
        });
    }

    /**
     * Publishes the permanent-deletion tombstone and then removes the stream config record.
     *
     * <p>The tombstone is written first so a crash between the two writes can never resurrect a
     * deleted identity: the config record is still an {@code ABORTING} claim that fences creation
     * and converges on the next drop attempt. Writing the tombstone is idempotent, so an existing
     * one from a concurrent completion of the same drop counts as success.
     *
     * <p>An existing fence is replaced when it is the zero-partition one
     * {@link #dropAbsentStream} writes for an absent identity: left standing it would make the
     * completed drop describe no partitions at all and leave the recovery sweep with nothing to
     * clean up. Overwriting it by version keeps the recorded partition count and any purge intent
     * either payload carries.
     */
    CompletableFuture<Void> completeDrop(StreamIdentifier id, DropClaim claim) {
        return completeDropAttempt(id, claim, 0);
    }

    private CompletableFuture<Void> completeDropAttempt(
            StreamIdentifier id, DropClaim claim, int retryCount) {
        String path = catalogPaths.streamConfigPath(id);
        StreamConfigData dropped = claim.config().completeDrop();
        return fenceTombstone(
            id, dropped, dropped.partitions() <= 0,
            existing -> existing.partitions() > 0,
            existing -> existing.purgeRequested() ? dropped.requestPurge() : dropped,
            () -> deleteCompletedDropConfig(id, path, claim),
            "stream deletion tombstone", "stream deletion tombstone upgrade",
            retryCount, () -> completeDropAttempt(id, claim, retryCount + 1));
    }

    private CompletableFuture<Void> deleteCompletedDropConfig(
            StreamIdentifier id, String path, DropClaim claim) {
        return oxiaClient.delete(path, Set.of(
                DeleteOption.IfVersionIdEquals(claim.versionId())))
            .handle((deleted, deleteFailure) ->
                new ConfigDelete(Boolean.TRUE.equals(deleted), unwrapNullable(deleteFailure)))
            .thenCompose(delete -> {
                if (delete.failure() == null && delete.deleted()) {
                    return CompletableFuture.<Void>completedFuture(null);
                }
                // The record was already gone, its version moved on, or the call outcome is
                // unknown: read back to tell a finished drop from a lost claim.
                return oxiaClient.get(path).thenCompose(current -> {
                    if (current == null) {
                        return CompletableFuture.<Void>completedFuture(null);
                    }
                    StreamConfigData currentConfig = parse(id, current.value());
                    if (!currentConfig.isOwnedBy(claim)) {
                        return CompletableFuture.<Void>failedFuture(
                            new AbortingOwnershipLostException(id));
                    }
                    return CompletableFuture.<Void>failedFuture(delete.failure() == null
                        ? new AbortingOwnershipLostException(id)
                        : delete.failure());
                });
            });
    }

    CompletableFuture<Optional<CompletedDrop>> readCompletedPurgingDrop(
            StreamIdentifier id) {
        return readCompletedDrop(id).thenApply(completed -> completed.filter(
            value -> value.config().purgeRequested()));
    }

    CompletableFuture<Optional<CompletedDrop>> readCompletedDrop(
            StreamIdentifier id) {
        return readTombstone(id).thenCompose(tombstone -> tombstone.isPresent()
            ? CompletableFuture.completedFuture(tombstone)
            : readLegacyCompletedDrop(id));
    }

    /** Reads a tombstone that a pre-tombstone-prefix deployment left in the config record. */
    private CompletableFuture<Optional<CompletedDrop>> readLegacyCompletedDrop(
            StreamIdentifier id) {
        String path = catalogPaths.streamConfigPath(id);
        return oxiaClient.get(path).thenApply(result -> {
            if (result == null) {
                return Optional.empty();
            }
            StreamConfigData config = parse(id, result.value());
            if (config.provisioningState() != ProvisioningState.DROPPED) {
                return Optional.empty();
            }
            return Optional.of(new CompletedDrop(
                config, result.version().versionId()));
        });
    }

    CompletableFuture<Void> verifyCompletedDrop(
            StreamIdentifier id, CompletedDrop expected) {
        return oxiaClient.get(catalogPaths.streamTombstonePath(id))
            .thenCompose(result -> result == null
                ? verifyLegacyCompletedDrop(id, expected)
                : verifyCompletedDropRecord(id, result, expected));
    }

    private CompletableFuture<Void> verifyLegacyCompletedDrop(
            StreamIdentifier id, CompletedDrop expected) {
        return oxiaClient.get(catalogPaths.streamConfigPath(id)).thenCompose(result -> {
            if (result == null) {
                return CompletableFuture.failedFuture(
                    new AbortingOwnershipLostException(id));
            }
            return verifyCompletedDropRecord(id, result, expected);
        });
    }

    private CompletableFuture<Void> verifyCompletedDropRecord(
            StreamIdentifier id, GetResult result, CompletedDrop expected) {
        StreamConfigData current = parse(id, result.value());
        if (result.version().versionId() == expected.versionId()
                && current.sameCompletedDrop(expected.config())) {
            return CompletableFuture.completedFuture(null);
        }
        return CompletableFuture.failedFuture(
            new AbortingOwnershipLostException(id));
    }

    CompletableFuture<Void> setProperties(StreamIdentifier id, Map<String, String> properties) {
        Objects.requireNonNull(properties, "properties");
        return update(id, current -> {
            Map<String, String> merged = new HashMap<>(current.properties());
            merged.putAll(properties);
            return current.withProperties(merged);
        });
    }

    CompletableFuture<Void> removeProperties(StreamIdentifier id, List<String> keys) {
        Objects.requireNonNull(keys, "keys");
        return update(id, current -> {
            Map<String, String> updated = new HashMap<>(current.properties());
            keys.forEach(updated::remove);
            return current.withProperties(updated);
        });
    }

    CompletableFuture<Void> setMaterialization(
            StreamIdentifier id, Optional<TableMaterializationPolicy> materialization) {
        Objects.requireNonNull(materialization, "materialization");
        return update(id, current -> current.withMaterialization(materialization));
    }

    private CompletableFuture<Void> update(
            StreamIdentifier id, UnaryOperator<StreamConfigData> mutation) {
        return updateAttempt(id, mutation, 0);
    }

    private CompletableFuture<Void> updateAttempt(
            StreamIdentifier id, UnaryOperator<StreamConfigData> mutation,
            int retryCount) {
        String path = catalogPaths.streamConfigPath(id);
        return oxiaClient.get(path).thenCompose(result -> {
            if (result == null) {
                return CompletableFuture.failedFuture(new NoSuchStreamException(id));
            }
            StreamConfigData current = parse(id, result.value());
            if (current.provisioningState() != ProvisioningState.ACTIVE) {
                return CompletableFuture.failedFuture(new NoSuchStreamException(id));
            }
            StreamConfigData updated = mutation.apply(current);
            return put(path, updated,
                Set.of(PutOption.IfVersionIdEquals(result.version().versionId())))
                .handle((ignored, failure) -> failure)
                .thenCompose(failure -> {
                    if (failure == null) {
                        return CompletableFuture.completedFuture(null);
                    }
                    Throwable cause = unwrap(failure);
                    if (cause instanceof UnexpectedVersionIdException) {
                        return retryAfterConflict(
                            id, "stream config update", retryCount, cause,
                            () -> updateAttempt(id, mutation, retryCount + 1));
                    }
                    return CompletableFuture.failedFuture(cause);
                });
        });
    }

    private CompletableFuture<Void> put(
            String path, StreamConfigData config, Set<PutOption> options) {
        byte[] bytes;
        try {
            bytes = toBytes(config);
        } catch (Exception e) {
            return CompletableFuture.failedFuture(e);
        }
        return options == null
            ? oxiaClient.put(path, bytes).thenApply(ignored -> null)
            : oxiaClient.put(path, bytes, options).thenApply(ignored -> null);
    }

    private CompletableFuture<PutResult> putWithResult(
            String path, StreamConfigData config, Set<PutOption> options) {
        byte[] bytes;
        try {
            bytes = toBytes(config);
        } catch (Exception e) {
            return CompletableFuture.failedFuture(e);
        }
        return oxiaClient.put(path, bytes, options);
    }

    private static byte[] toBytes(StreamConfigData config) throws Exception {
        ObjectNode node = MAPPER.createObjectNode();
        node.put("partitions", config.partitions());
        ObjectNode properties = node.putObject("properties");
        config.properties().forEach(properties::put);
        ObjectNode streamConfig = node.putObject(STREAM_CONFIG_FIELD);
        ObjectNode streamConfigProperties = streamConfig.putObject("properties");
        config.definition().streamConfig().properties().forEach(streamConfigProperties::put);
        ObjectNode partitioning = node.putObject(PARTITIONING_FIELD);
        partitioning.put("strategy", config.definition().partitioning().strategy().name());
        ObjectNode partitioningConfig = partitioning.putObject("config");
        config.definition().partitioning().config().forEach(partitioningConfig::put);
        ObjectNode schema = node.putObject(SCHEMA_FIELD);
        schema.put("schemaType", config.definition().schema().schemaType());
        ObjectNode schemaProperties = schema.putObject("properties");
        config.definition().schema().properties().forEach(schemaProperties::put);
        config.pendingExpansion().ifPresent(expansion -> {
            ObjectNode pending = node.putObject(PENDING_EXPANSION_FIELD);
            pending.put(EXPANSION_BASE_FIELD, expansion.basePartitions());
            pending.put(EXPANSION_TARGET_FIELD, expansion.targetPartitions());
        });
        if (config.propertiesSourceRevision() != NO_PROPERTIES_SOURCE_REVISION) {
            node.put(PROPERTIES_SOURCE_REVISION_FIELD, config.propertiesSourceRevision());
        }
        config.materialization().ifPresent(policy ->
            node.set("materialization", MaterializationJson.policyToJson(policy)));
        config.incarnationId().ifPresent(value -> node.put(INCARNATION_ID_FIELD, value));
        config.ownerToken().ifPresent(value -> node.put(OWNER_TOKEN_FIELD, value));
        if (config.ownerGeneration() >= 0) {
            node.put(OWNER_GENERATION_FIELD, config.ownerGeneration());
        }
        config.metadataSourceOwnerToken().ifPresent(value ->
            node.put(METADATA_SOURCE_OWNER_TOKEN_FIELD, value));
        if (config.metadataSourceGeneration() != NO_METADATA_GENERATION) {
            node.put(METADATA_SOURCE_GENERATION_FIELD, config.metadataSourceGeneration());
        }
        node.put(CREATION_KIND_FIELD, config.creationKind().orElseThrow().name());
        if (config.provisioning()) {
            node.put(PROVISIONING_FIELD, true);
            node.put(PROVISIONING_STATE_FIELD, config.provisioningState().name());
        }
        if (config.purgeRequested()) {
            node.put(PURGE_REQUESTED_FIELD, true);
        }
        return MAPPER.writeValueAsBytes(node);
    }

    private static boolean isLifecycleTombstone(byte[] value) {
        try {
            JsonNode node = MAPPER.readTree(value);
            if (node == null || !node.isObject()
                    || !CreationKind.NATIVE_CREATE.name().equals(
                        node.path(CREATION_KIND_FIELD).asText())
                    || !node.path(PROVISIONING_FIELD).asBoolean(false)) {
                return false;
            }
            return ProvisioningState.DROPPED.name().equals(
                node.path(PROVISIONING_STATE_FIELD).asText());
        } catch (Exception e) {
            return false;
        }
    }

    private static StreamConfigData parse(StreamIdentifier id, byte[] value) {
        try {
            JsonNode node = MAPPER.readTree(value);
            if (node == null || !node.isObject()) {
                throw new IllegalArgumentException(
                    "Stream config must be a non-empty JSON object");
            }
            int partitions = node.path("partitions").asInt(0);
            Map<String, String> properties = node.has("properties")
                ? MAPPER.convertValue(node.get("properties"),
                    new TypeReference<Map<String, String>>() {})
                : Map.of();
            Optional<TableMaterializationPolicy> materialization =
                node.has("materialization") && !node.get("materialization").isNull()
                    ? Optional.of(MaterializationJson.policyFromJson(node.get("materialization")))
                    : Optional.empty();
            ImmutableStreamDefinition defaultDefinition = defaultDefinition(partitions);
            Map<String, String> streamConfigProperties =
                node.path(STREAM_CONFIG_FIELD).has("properties")
                    ? MAPPER.convertValue(
                        node.path(STREAM_CONFIG_FIELD).get("properties"),
                        new TypeReference<Map<String, String>>() {})
                    : defaultDefinition.streamConfig().properties();
            StreamConfig streamConfig = new StreamConfig(streamConfigProperties);
            Partitioning partitioning;
            if (node.has(PARTITIONING_FIELD)) {
                JsonNode partitioningNode = node.get(PARTITIONING_FIELD);
                PartitioningStrategy strategy = PartitioningStrategy.valueOf(
                    partitioningNode.path("strategy")
                        .asText(PartitioningStrategy.INDEXED.name()));
                Map<String, String> partitioningConfig = partitioningNode.has("config")
                    ? MAPPER.convertValue(
                        partitioningNode.get("config"),
                        new TypeReference<Map<String, String>>() {})
                    : defaultDefinition.partitioning().config();
                partitioning = new Partitioning(strategy, partitioningConfig);
            } else {
                partitioning = defaultDefinition.partitioning();
            }
            SchemaConfig schema;
            if (node.has(SCHEMA_FIELD)) {
                JsonNode schemaNode = node.get(SCHEMA_FIELD);
                Map<String, String> schemaProperties = schemaNode.has("properties")
                    ? MAPPER.convertValue(
                        schemaNode.get("properties"),
                        new TypeReference<Map<String, String>>() {})
                    : Map.of();
                schema = new SchemaConfig(
                    schemaNode.path("schemaType").asText("NONE"), schemaProperties);
            } else {
                schema = defaultDefinition.schema();
            }
            ImmutableStreamDefinition definition = new ImmutableStreamDefinition(
                streamConfig, partitioning, schema);
            Optional<PendingExpansion> pendingExpansion =
                node.has(PENDING_EXPANSION_FIELD)
                    ? Optional.of(new PendingExpansion(
                        node.path(PENDING_EXPANSION_FIELD)
                            .path(EXPANSION_BASE_FIELD).asInt(partitions),
                        node.path(PENDING_EXPANSION_FIELD)
                            .path(EXPANSION_TARGET_FIELD).asInt(partitions)))
                    : Optional.empty();
            long propertiesSourceRevision = node.has(PROPERTIES_SOURCE_REVISION_FIELD)
                ? node.get(PROPERTIES_SOURCE_REVISION_FIELD).asLong()
                : NO_PROPERTIES_SOURCE_REVISION;
            Optional<String> incarnationId = node.hasNonNull(INCARNATION_ID_FIELD)
                ? Optional.of(node.get(INCARNATION_ID_FIELD).asText())
                : Optional.empty();
            Optional<String> ownerToken = node.hasNonNull(OWNER_TOKEN_FIELD)
                ? Optional.of(node.get(OWNER_TOKEN_FIELD).asText())
                : Optional.empty();
            long ownerGeneration = node.hasNonNull(OWNER_GENERATION_FIELD)
                ? node.get(OWNER_GENERATION_FIELD).asLong() : -1L;
            Optional<String> metadataSourceOwnerToken =
                node.hasNonNull(METADATA_SOURCE_OWNER_TOKEN_FIELD)
                    ? Optional.of(node.get(METADATA_SOURCE_OWNER_TOKEN_FIELD).asText())
                    : Optional.empty();
            long metadataSourceGeneration = node.hasNonNull(METADATA_SOURCE_GENERATION_FIELD)
                ? node.get(METADATA_SOURCE_GENERATION_FIELD).asLong()
                : NO_METADATA_GENERATION;
            if (!node.hasNonNull(CREATION_KIND_FIELD)) {
                throw new IllegalArgumentException(
                    "Stream config is missing the native creation marker");
            }
            CreationKind creationKind = CreationKind.valueOf(
                node.get(CREATION_KIND_FIELD).asText());
            if (creationKind != CreationKind.NATIVE_CREATE) {
                throw new IllegalArgumentException(
                    "Stream config is not a native catalog record");
            }
            boolean provisioning = node.path(PROVISIONING_FIELD).asBoolean(false);
            boolean purgeRequested = node.path(PURGE_REQUESTED_FIELD).asBoolean(false);
            ProvisioningState provisioningState = provisioning
                ? ProvisioningState.valueOf(node.path(PROVISIONING_STATE_FIELD).asText())
                : ProvisioningState.ACTIVE;
            return new StreamConfigData(
                partitions, properties, materialization, definition, incarnationId,
                ownerToken, Optional.of(creationKind), ownerGeneration,
                metadataSourceOwnerToken, metadataSourceGeneration,
                pendingExpansion, propertiesSourceRevision,
                provisioningState, purgeRequested);
        } catch (Exception e) {
            throw new RuntimeException("Failed to parse stream config for: " + id.fullName(), e);
        }
    }

    private <T> CompletableFuture<T> retryAfterConflict(
            StreamIdentifier id, String operation, int retryCount,
            Throwable lastFailure, Supplier<CompletableFuture<T>> retry) {
        if (retryCount >= MAX_CONFIG_WRITE_RETRIES) {
            log.warn("Exhausted {} indexed stream config retries for {} while performing {}",
                MAX_CONFIG_WRITE_RETRIES, id.fullName(), operation, lastFailure);
            return CompletableFuture.failedFuture(lastFailure);
        }
        int nextRetry = retryCount + 1;
        long backoffMillis = INITIAL_RETRY_BACKOFF_MILLIS << retryCount;
        log.debug("Retrying indexed stream config write for {} while performing {} "
                + "(retry {}/{}) after {} ms: {}",
            id.fullName(), operation, nextRetry, MAX_CONFIG_WRITE_RETRIES,
            backoffMillis, lastFailure.toString());
        return retryDelay.apply(backoffMillis).thenCompose(ignored -> retry.get());
    }

    private static void logResolvedWrite(
            StreamIdentifier id, String operation, long versionId, Throwable writeFailure) {
        if (writeFailure instanceof KeyAlreadyExistsException
                || writeFailure instanceof UnexpectedVersionIdException) {
            log.debug("Observed the desired indexed stream config for {} at version {} "
                    + "after a conflicting {} write",
                id.fullName(), versionId, operation);
            return;
        }
        log.info("Confirmed an ambiguous {} write for {} by reading back version {}: {}",
            operation, id.fullName(), versionId, writeFailure.toString());
    }

    private static Throwable unwrap(Throwable failure) {
        return failure instanceof CompletionException && failure.getCause() != null
            ? failure.getCause() : failure;
    }

    private static Throwable unwrapNullable(Throwable failure) {
        return failure == null ? null : unwrap(failure);
    }

    private record ConfigCheck(GetResult config, Throwable failure) {
    }

    private record CreateWrite(PutResult result, Throwable failure) {
    }

    private record ConfigDelete(boolean deleted, Throwable failure) {
    }

    enum CreationKind {
        NATIVE_CREATE
    }

    enum ProvisioningState {
        ACTIVE,
        PROVISIONING,
        ABORTING,
        DROPPED
    }

    private static ImmutableStreamDefinition defaultDefinition(int partitions) {
        return new ImmutableStreamDefinition(
            new StreamConfig(),
            new Partitioning(
                PartitioningStrategy.INDEXED,
                Map.of("numPartitions", String.valueOf(partitions))),
            new SchemaConfig());
    }

    record ImmutableStreamDefinition(
            StreamConfig streamConfig, Partitioning partitioning, SchemaConfig schema) {

        ImmutableStreamDefinition {
            Objects.requireNonNull(streamConfig, "streamConfig");
            Objects.requireNonNull(partitioning, "partitioning");
            Objects.requireNonNull(schema, "schema");
            streamConfig = new StreamConfig(Map.copyOf(streamConfig.properties()));
            partitioning = new Partitioning(
                partitioning.strategy(), Map.copyOf(partitioning.config()));
            schema = new SchemaConfig(schema.schemaType(), Map.copyOf(schema.properties()));
        }

        ImmutableStreamDefinition withPartitions(int partitions) {
            Map<String, String> config = new HashMap<>(partitioning.config());
            config.put("numPartitions", String.valueOf(partitions));
            return new ImmutableStreamDefinition(
                streamConfig, new Partitioning(partitioning.strategy(), config), schema);
        }

        boolean sameCreationShape(ImmutableStreamDefinition requested) {
            return streamConfig.equals(requested.streamConfig())
                && schema.equals(requested.schema())
                && partitioning.strategy() == requested.partitioning().strategy()
                && partitionConfigWithoutCount(partitioning.config()).equals(
                    partitionConfigWithoutCount(requested.partitioning().config()));
        }

        private static Map<String, String> partitionConfigWithoutCount(
                Map<String, String> config) {
            Map<String, String> shape = new HashMap<>(config);
            shape.remove("numPartitions");
            return shape;
        }
    }

    record PendingExpansion(int basePartitions, int targetPartitions) {

        PendingExpansion {
            if (basePartitions < 0 || targetPartitions <= basePartitions) {
                throw new IllegalArgumentException(
                    "Pending expansion target must be greater than its base");
            }
        }

        PendingExpansion growTo(int requestedTarget) {
            return requestedTarget <= targetPartitions
                ? this : new PendingExpansion(basePartitions, requestedTarget);
        }
    }

    record StreamConfigData(
            int partitions, Map<String, String> properties,
            Optional<TableMaterializationPolicy> materialization,
            ImmutableStreamDefinition definition,
            Optional<String> incarnationId, Optional<String> ownerToken,
            Optional<CreationKind> creationKind,
            long ownerGeneration, Optional<String> metadataSourceOwnerToken,
            long metadataSourceGeneration,
            Optional<PendingExpansion> pendingExpansion,
            long propertiesSourceRevision,
            ProvisioningState provisioningState, boolean purgeRequested) {

        StreamConfigData {
            properties = Map.copyOf(properties);
            Objects.requireNonNull(materialization, "materialization");
            Objects.requireNonNull(definition, "definition");
            Objects.requireNonNull(incarnationId, "incarnationId");
            Objects.requireNonNull(ownerToken, "ownerToken");
            Objects.requireNonNull(creationKind, "creationKind");
            Objects.requireNonNull(metadataSourceOwnerToken, "metadataSourceOwnerToken");
            Objects.requireNonNull(pendingExpansion, "pendingExpansion");
            Objects.requireNonNull(provisioningState, "provisioningState");
            if (!creationKind.equals(Optional.of(CreationKind.NATIVE_CREATE))) {
                throw new IllegalArgumentException(
                    "Stream config requires the native creation marker");
            }
            if (ownerGeneration < LEGACY_METADATA_GENERATION
                    || metadataSourceGeneration < NO_METADATA_GENERATION) {
                throw new IllegalArgumentException("Invalid owner generation");
            }
            boolean modernIdentity = incarnationId.isPresent()
                || ownerToken.isPresent() || ownerGeneration >= 0;
            if (modernIdentity && (incarnationId.isEmpty()
                    || ownerToken.isEmpty() || ownerGeneration < 0)) {
                throw new IllegalArgumentException(
                    "Modern stream config requires incarnation, owner, and generation");
            }
            if (metadataSourceOwnerToken.isPresent()
                    && metadataSourceGeneration < 0) {
                throw new IllegalArgumentException(
                    "Modern metadata source requires owner token and generation");
            }
            if (partitions != definition.partitioning().numPartitions()) {
                throw new IllegalArgumentException(
                    "Partition count must match the immutable stream definition");
            }
            pendingExpansion.ifPresent(expansion -> {
                if (expansion.basePartitions() != partitions) {
                    throw new IllegalArgumentException(
                        "Pending expansion base must match committed partitions");
                }
            });
            if (propertiesSourceRevision < NO_PROPERTIES_SOURCE_REVISION) {
                throw new IllegalArgumentException("Invalid properties source revision");
            }
        }

        StreamConfigData(
                int partitions, Map<String, String> properties,
                Optional<TableMaterializationPolicy> materialization,
                ImmutableStreamDefinition definition,
                Optional<String> incarnationId, Optional<String> ownerToken,
                Optional<CreationKind> creationKind,
                long ownerGeneration, Optional<String> metadataSourceOwnerToken,
                long metadataSourceGeneration,
                ProvisioningState provisioningState, boolean purgeRequested) {
            this(partitions, properties, materialization, definition,
                incarnationId, ownerToken, creationKind, ownerGeneration,
                metadataSourceOwnerToken, metadataSourceGeneration,
                Optional.empty(), NO_PROPERTIES_SOURCE_REVISION,
                provisioningState, purgeRequested);
        }

        StreamConfigData(
                int partitions, Map<String, String> properties,
                Optional<TableMaterializationPolicy> materialization,
                Optional<String> incarnationId, Optional<String> ownerToken,
                Optional<CreationKind> creationKind,
                long ownerGeneration, Optional<String> metadataSourceOwnerToken,
                long metadataSourceGeneration,
                ProvisioningState provisioningState, boolean purgeRequested) {
            this(partitions, properties, materialization, defaultDefinition(partitions),
                incarnationId, ownerToken, creationKind, ownerGeneration,
                metadataSourceOwnerToken, metadataSourceGeneration,
                provisioningState, purgeRequested);
        }


        static StreamConfigData provisioning(
                int partitions, Map<String, String> properties,
                Optional<TableMaterializationPolicy> materialization,
                CreationKind creationKind, String incarnationId, String ownerToken) {
            return provisioning(
                partitions, properties, materialization, defaultDefinition(partitions),
                creationKind, incarnationId, ownerToken);
        }

        static StreamConfigData provisioning(
                int partitions, Map<String, String> properties,
                Optional<TableMaterializationPolicy> materialization,
                ImmutableStreamDefinition definition,
                CreationKind creationKind, String incarnationId, String ownerToken) {
            return new StreamConfigData(
                partitions, properties, materialization, definition,
                Optional.of(incarnationId),
                Optional.of(ownerToken), Optional.of(creationKind), 1L,
                Optional.empty(), NO_METADATA_GENERATION,
                ProvisioningState.PROVISIONING, false);
        }

        boolean provisioning() {
            return provisioningState != ProvisioningState.ACTIVE;
        }

        boolean sameActiveLifecycle(
                StreamConfigData expected, long currentVersion, long expectedVersion) {
            if (provisioning() || expected.provisioning()
                    || currentVersion != expectedVersion) {
                return false;
            }
            if (expected.incarnationId().isPresent()) {
                return incarnationId.equals(expected.incarnationId)
                    && ownerToken.equals(expected.ownerToken)
                    && ownerGeneration == expected.ownerGeneration
                    && metadataSourceOwnerToken.equals(
                        expected.metadataSourceOwnerToken)
                    && metadataSourceGeneration == expected.metadataSourceGeneration
                    && creationKind.equals(expected.creationKind);
            }
            return incarnationId.isEmpty()
                && ownerToken.isEmpty();
        }

        boolean canResumeCreation(
                ImmutableStreamDefinition requestedDefinition,
                CreationKind requestedKind) {
            // A retry may carry a newer desired partition count, properties, or materialization
            // policy. The latest intent is persisted while the lifecycle is still PROVISIONING so
            // ACTIVE is never externally visible with a partially converged definition.
            return creationKind.orElse(null) == requestedKind
                && requestedDefinition.partitioning().numPartitions() >= partitions
                && definition.sameCreationShape(requestedDefinition);
        }

        StreamConfigData resumeCreation(
                ImmutableStreamDefinition requestedDefinition,
                Map<String, String> requestedProperties,
                Optional<TableMaterializationPolicy> requestedMaterialization) {
            if (provisioningState != ProvisioningState.PROVISIONING) {
                throw new IllegalStateException(
                    "Only a provisioning stream can update its creation intent");
            }
            return new StreamConfigData(
                requestedDefinition.partitioning().numPartitions(), requestedProperties,
                requestedMaterialization, requestedDefinition, incarnationId, ownerToken,
                creationKind, ownerGeneration, metadataSourceOwnerToken,
                metadataSourceGeneration, Optional.empty(), propertiesSourceRevision,
                provisioningState, purgeRequested);
        }

        StreamConfigData activate() {
            return new StreamConfigData(
                partitions, properties, materialization, definition, incarnationId, ownerToken,
                creationKind, ownerGeneration, metadataSourceOwnerToken,
                metadataSourceGeneration, pendingExpansion, propertiesSourceRevision,
                ProvisioningState.ACTIVE, purgeRequested);
        }

        StreamConfigData abort(String newOwnerToken) {
            return abort(newOwnerToken, false);
        }

        StreamConfigData abort(String newOwnerToken, boolean purge) {
            int cleanupPartitions = pendingExpansion
                .map(PendingExpansion::targetPartitions)
                .orElse(partitions);
            ImmutableStreamDefinition cleanupDefinition =
                definition.withPartitions(cleanupPartitions);
            Optional<String> cleanupOwnerToken =
                provisioningState == ProvisioningState.ABORTING
                    ? metadataSourceOwnerToken : ownerToken;
            long cleanupGeneration = provisioningState == ProvisioningState.ABORTING
                ? metadataSourceGeneration : ownerGeneration;
            return new StreamConfigData(
                cleanupPartitions, properties, materialization, cleanupDefinition, incarnationId,
                Optional.of(newOwnerToken), creationKind, ownerGeneration + 1,
                cleanupOwnerToken, cleanupGeneration, Optional.empty(),
                propertiesSourceRevision, ProvisioningState.ABORTING,
                purgeRequested || purge);
        }

        StreamConfigData completeDrop() {
            if (provisioningState != ProvisioningState.ABORTING) {
                throw new IllegalStateException("Only an aborting stream can complete deletion");
            }
            return new StreamConfigData(
                partitions, properties, materialization, definition, incarnationId, ownerToken,
                creationKind, ownerGeneration, metadataSourceOwnerToken,
                metadataSourceGeneration, pendingExpansion, propertiesSourceRevision,
                ProvisioningState.DROPPED, purgeRequested);
        }

        static StreamConfigData emptyDropped(boolean purge) {
            return new StreamConfigData(
                0, Map.of(), Optional.empty(), Optional.empty(), Optional.empty(),
                Optional.of(CreationKind.NATIVE_CREATE), LEGACY_METADATA_GENERATION,
                Optional.empty(), NO_METADATA_GENERATION,
                ProvisioningState.DROPPED, purge);
        }

        StreamConfigData withPendingExpansion(PendingExpansion expansion) {
            return new StreamConfigData(
                partitions, properties, materialization, definition, incarnationId, ownerToken,
                creationKind, ownerGeneration, metadataSourceOwnerToken,
                metadataSourceGeneration, Optional.of(expansion), propertiesSourceRevision,
                provisioningState, purgeRequested);
        }

        StreamConfigData commitExpansion(PendingExpansion expansion) {
            if (!pendingExpansion.equals(Optional.of(expansion))) {
                throw new IllegalStateException("Expansion claim no longer matches");
            }
            int committedPartitions = expansion.targetPartitions();
            return new StreamConfigData(
                committedPartitions, properties, materialization,
                definition.withPartitions(committedPartitions), incarnationId, ownerToken,
                creationKind, ownerGeneration, metadataSourceOwnerToken,
                metadataSourceGeneration, Optional.empty(), propertiesSourceRevision,
                provisioningState, purgeRequested);
        }

        StreamConfigData replaceProperties(
                Map<String, String> replacement, long sourceRevision) {
            return new StreamConfigData(
                partitions, replacement, materialization, definition, incarnationId, ownerToken,
                creationKind, ownerGeneration, metadataSourceOwnerToken,
                metadataSourceGeneration, pendingExpansion, sourceRevision,
                provisioningState, purgeRequested);
        }

        StreamConfigData requestPurge() {
            if (purgeRequested) {
                return this;
            }
            return new StreamConfigData(
                partitions, properties, materialization, definition, incarnationId, ownerToken,
                creationKind, ownerGeneration, metadataSourceOwnerToken,
                metadataSourceGeneration, pendingExpansion, propertiesSourceRevision,
                provisioningState, true);
        }

        StreamConfigData withProperties(Map<String, String> newProperties) {
            return new StreamConfigData(
                partitions, newProperties, materialization, definition, incarnationId, ownerToken,
                creationKind, ownerGeneration, metadataSourceOwnerToken,
                metadataSourceGeneration, pendingExpansion, propertiesSourceRevision,
                provisioningState,
                purgeRequested);
        }

        StreamConfigData withMaterialization(
                Optional<TableMaterializationPolicy> newMaterialization) {
            return new StreamConfigData(
                partitions, properties, newMaterialization, definition, incarnationId, ownerToken,
                creationKind, ownerGeneration, metadataSourceOwnerToken,
                metadataSourceGeneration, pendingExpansion, propertiesSourceRevision,
                provisioningState,
                purgeRequested);
        }

        boolean isOwnedBy(StreamConfigData other) {
            return provisioning()
                && incarnationId.equals(other.incarnationId)
                && ownerToken.equals(other.ownerToken)
                && ownerGeneration == other.ownerGeneration
                && metadataSourceOwnerToken.equals(other.metadataSourceOwnerToken)
                && metadataSourceGeneration == other.metadataSourceGeneration
                && creationKind.equals(other.creationKind)
                && purgeRequested == other.purgeRequested;
        }

        boolean sameStreamLifecycle(StreamConfigData other) {
            return incarnationId.equals(other.incarnationId)
                && ownerToken.equals(other.ownerToken)
                && ownerGeneration == other.ownerGeneration
                && creationKind.equals(other.creationKind);
        }

        boolean isOwnedBy(ProvisioningClaim claim) {
            return incarnationId.equals(Optional.of(claim.incarnationId()))
                && ownerToken.equals(Optional.of(claim.ownerToken()))
                && ownerGeneration == claim.ownerGeneration()
                && creationKind.equals(Optional.of(claim.creationKind()));
        }

        boolean sameAbortingOwner(StreamConfigData other) {
            return provisioningState == ProvisioningState.ABORTING
                && other.provisioningState == ProvisioningState.ABORTING
                && incarnationId.equals(other.incarnationId)
                && ownerToken.equals(other.ownerToken)
                && ownerGeneration == other.ownerGeneration
                && creationKind.equals(other.creationKind)
                && purgeRequested == other.purgeRequested
                && partitions == other.partitions
                && properties.equals(other.properties)
                && materialization.equals(other.materialization);
        }

        boolean isOwnedBy(DropClaim claim) {
            return provisioningState == ProvisioningState.ABORTING
                && ownerToken.equals(Optional.of(claim.ownerToken()))
                && incarnationId.equals(claim.config().incarnationId())
                && ownerGeneration == claim.config().ownerGeneration()
                && metadataSourceOwnerToken.equals(
                    claim.config().metadataSourceOwnerToken())
                && metadataSourceGeneration
                    == claim.config().metadataSourceGeneration()
                && creationKind.equals(claim.config().creationKind());
        }

        boolean sameCompletedDrop(StreamConfigData expected) {
            return provisioningState == ProvisioningState.DROPPED
                && expected.provisioningState == ProvisioningState.DROPPED
                && incarnationId.equals(expected.incarnationId)
                && ownerToken.equals(expected.ownerToken)
                && ownerGeneration == expected.ownerGeneration
                && metadataSourceOwnerToken.equals(expected.metadataSourceOwnerToken)
                && metadataSourceGeneration == expected.metadataSourceGeneration
                && creationKind.equals(expected.creationKind)
                && purgeRequested == expected.purgeRequested;
        }
    }

    record ProvisioningClaim(
            StreamConfigData config, String incarnationId, String ownerToken,
            CreationKind creationKind, long ownerGeneration, long versionId) {
    }

    record NativeCleanupContext(
            Optional<StreamConfigData> config, long versionId) {
    }

    record LifecycleContext(
            Optional<StreamConfigData> config, long versionId) {
    }

    record DropClaim(StreamConfigData config, String ownerToken, long versionId) {
    }

    record CompletedDrop(StreamConfigData config, long versionId) {
    }

    record ActiveStreamConfig(StreamConfigData config, long versionId) {
    }

    record ExpansionClaim(
            StreamConfigData config, int basePartitions,
            int targetPartitions, long versionId) {

        boolean requiresExpansion() {
            return targetPartitions > basePartitions;
        }
    }

    record ExpansionFinalization(boolean complete, int targetPartitions) {

        static ExpansionFinalization complete(int targetPartitions) {
            return new ExpansionFinalization(true, targetPartitions);
        }

        static ExpansionFinalization continueWith(int targetPartitions) {
            return new ExpansionFinalization(false, targetPartitions);
        }
    }

    record FinalizeOutcome(boolean active, Throwable failure) {

        static FinalizeOutcome successful() {
            return new FinalizeOutcome(true, null);
        }

        static FinalizeOutcome indeterminate(Throwable failure) {
            return new FinalizeOutcome(false, failure);
        }
    }

    static final class ProvisioningOwnershipLostException extends RuntimeException {

        private ProvisioningOwnershipLostException(StreamIdentifier id) {
            super("Stream provisioning ownership was lost: " + id.fullName());
        }
    }

    static final class ExpansionOwnershipLostException extends RuntimeException {

        private ExpansionOwnershipLostException(StreamIdentifier id) {
            super("Stream expansion ownership was lost: " + id.fullName());
        }
    }

    static final class AbortingOwnershipLostException extends RuntimeException {

        private AbortingOwnershipLostException(StreamIdentifier id) {
            super("Stream deletion ownership was lost: " + id.fullName());
        }
    }

}
