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
import io.lakestream.api.StreamIdentifier;
import io.lakestream.api.exception.AlreadyExistsException;
import io.lakestream.api.exception.NoSuchStreamException;
import io.lakestream.api.materialization.TableMaterializationPolicy;
import io.lakestream.ursa.lakestream.impl.materialization.MaterializationJson;
import io.oxia.client.api.AsyncOxiaClient;
import io.oxia.client.api.GetResult;
import io.oxia.client.api.PutResult;
import io.oxia.client.api.exceptions.KeyAlreadyExistsException;
import io.oxia.client.api.exceptions.UnexpectedVersionIdException;
import io.oxia.client.api.options.PutOption;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.function.UnaryOperator;

/** Owns the persisted indexed-stream config schema and its versioned update semantics. */
final class IndexedStreamConfigStore {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    static final long NO_METADATA_GENERATION = -2L;
    static final long LEGACY_METADATA_GENERATION = -1L;
    private static final String PERMANENT_DELETION_FIELD =
        "_externalStreamPermanentlyDeleted";
    private static final String INCARNATION_ID_FIELD = "_incarnationId";
    private static final String OWNER_TOKEN_FIELD = "_ownerToken";
    private static final String OWNER_GENERATION_FIELD = "_ownerGeneration";
    private static final String METADATA_SOURCE_GENERATION_FIELD = "_metadataSourceGeneration";
    private static final String CREATION_KIND_FIELD = "_creationKind";
    private static final String PROVISIONING_FIELD = "_provisioning";
    private static final String PROVISIONING_STATE_FIELD = "_provisioningState";

    private final AsyncOxiaClient oxiaClient;
    private final CatalogPaths catalogPaths;

    IndexedStreamConfigStore(AsyncOxiaClient oxiaClient, CatalogPaths catalogPaths) {
        this.oxiaClient = Objects.requireNonNull(oxiaClient, "oxiaClient");
        this.catalogPaths = Objects.requireNonNull(catalogPaths, "catalogPaths");
    }

    CompletableFuture<Void> registerExternalStream(
            StreamIdentifier id, int partitionCount, Map<String, String> properties) {
        Objects.requireNonNull(id, "id");
        Objects.requireNonNull(properties, "properties");
        if (partitionCount <= 0) {
            throw new IllegalArgumentException("partitionCount must be positive");
        }
        return registerExternalStreamAttempt(id, partitionCount, properties);
    }

    CompletableFuture<ExternalRegistration> beginExternalPartitionRegistration(
            StreamIdentifier id, int partitionCount, Map<String, String> properties,
            String attemptToken) {
        Objects.requireNonNull(id, "id");
        Objects.requireNonNull(properties, "properties");
        Objects.requireNonNull(attemptToken, "attemptToken");
        if (partitionCount <= 0) {
            throw new IllegalArgumentException("partitionCount must be positive");
        }
        return beginExternalPartitionRegistrationAttempt(
            id, partitionCount, properties, attemptToken);
    }

    private CompletableFuture<ExternalRegistration> beginExternalPartitionRegistrationAttempt(
            StreamIdentifier id, int partitionCount, Map<String, String> properties,
            String ownerToken) {
        String path = catalogPaths.streamConfigPath(id);
        return oxiaClient.get(path).thenCompose(result -> {
            if (result == null || isProvisioning(result.value())) {
                return claimCreation(
                        id, partitionCount, properties, Optional.empty(),
                        CreationKind.EXTERNAL, ownerToken)
                    .thenApply(ExternalRegistration::provisioning);
            }
            if (isPermanentDeletionTombstone(result.value())) {
                return CompletableFuture.failedFuture(new NoSuchStreamException(id));
            }
            StreamConfigData existing = parse(id, result.value());
            if (!existing.isActiveExternalOrLegacy()) {
                return CompletableFuture.failedFuture(new AlreadyExistsException(
                    "Stream is owned by a native catalog creation: " + id.fullName()));
            }
            if (existing.partitions() >= partitionCount) {
                return CompletableFuture.completedFuture(ExternalRegistration.active(
                    existing.incarnationId(), existing.ownerToken(), existing.creationKind(),
                    existing.ownerGeneration(), existing.metadataSourceGeneration(),
                    result.version().versionId()));
            }
            StreamConfigData grown = existing.withPartitions(partitionCount);
            return putWithResult(path, grown,
                    Set.of(PutOption.IfVersionIdEquals(result.version().versionId())))
                .handle((write, failure) ->
                    new CreateWrite(write, unwrapNullable(failure)))
                .thenCompose(write -> resolveExternalPartitionGrowth(
                    id, partitionCount, properties, ownerToken, grown, write));
        });
    }

    private CompletableFuture<ExternalRegistration> resolveExternalPartitionGrowth(
            StreamIdentifier id, int partitionCount, Map<String, String> properties,
            String attemptToken, StreamConfigData grown, CreateWrite write) {
        if (write.failure() == null) {
            return CompletableFuture.completedFuture(ExternalRegistration.active(
                grown.incarnationId(), grown.ownerToken(), grown.creationKind(),
                grown.ownerGeneration(), grown.metadataSourceGeneration(),
                write.result().version().versionId()));
        }
        if (write.failure() instanceof KeyAlreadyExistsException
                || write.failure() instanceof UnexpectedVersionIdException) {
            return beginExternalPartitionRegistrationAttempt(
                id, partitionCount, properties, attemptToken);
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
                if (check.config() == null) {
                    return CompletableFuture.failedFuture(write.failure());
                }
                if (isPermanentDeletionTombstone(check.config().value())) {
                    return CompletableFuture.failedFuture(
                        new NoSuchStreamException(id, write.failure()));
                }
                if (isProvisioning(check.config().value())) {
                    return beginExternalPartitionRegistrationAttempt(
                        id, partitionCount, properties, attemptToken);
                }
                StreamConfigData current = parse(id, check.config().value());
                if (current.isActiveExternalOrLegacy()
                        && current.partitions() >= partitionCount) {
                    return CompletableFuture.completedFuture(ExternalRegistration.active(
                        current.incarnationId(), current.ownerToken(), current.creationKind(),
                        current.ownerGeneration(), current.metadataSourceGeneration(),
                        check.config().version().versionId()));
                }
                return CompletableFuture.failedFuture(write.failure());
            });
    }

    CompletableFuture<Void> verifyExternalRegistration(
            StreamIdentifier id, ExternalRegistration registration) {
        String path = catalogPaths.streamConfigPath(id);
        return oxiaClient.get(path)
            .handle((current, failure) ->
                new ConfigCheck(current, unwrapNullable(failure)))
            .thenCompose(check -> {
            if (check.failure() != null) {
                return CompletableFuture.failedFuture(
                    new VerificationUnknownException(check.failure()));
            }
            GetResult current = check.config();
            if (current == null) {
                return CompletableFuture.failedFuture(
                    new ExternalRegistrationInvalidatedException(id, false));
            }
            if (isPermanentDeletionTombstone(current.value())) {
                return CompletableFuture.failedFuture(
                    new ExternalRegistrationInvalidatedException(id, true));
            }
            StreamConfigData config = parse(id, current.value());
            if (registration.claim().isPresent()) {
                ProvisioningClaim claim = registration.claim().orElseThrow();
                if (config.provisioningState() == ProvisioningState.PROVISIONING
                        && config.isOwnedBy(claim)
                        && current.version().versionId() == claim.versionId()) {
                    return CompletableFuture.completedFuture(null);
                }
            } else if (!config.provisioning()
                    && registration.matchesActive(config, current.version().versionId())) {
                return CompletableFuture.completedFuture(null);
            }
            return CompletableFuture.failedFuture(
                new ExternalRegistrationInvalidatedException(id, false));
        });
    }

    CompletableFuture<Void> verifyFinalizedExternalRegistration(
            StreamIdentifier id, ProvisioningClaim claim) {
        String path = catalogPaths.streamConfigPath(id);
        return oxiaClient.get(path)
            .handle((current, failure) ->
                new ConfigCheck(current, unwrapNullable(failure)))
            .thenCompose(check -> {
            if (check.failure() != null) {
                return CompletableFuture.failedFuture(
                    new VerificationUnknownException(check.failure()));
            }
            GetResult current = check.config();
            if (current == null) {
                return CompletableFuture.failedFuture(
                    new ExternalRegistrationInvalidatedException(id, false));
            }
            if (isPermanentDeletionTombstone(current.value())) {
                return CompletableFuture.failedFuture(
                    new ExternalRegistrationInvalidatedException(id, true));
            }
            if (isProvisioning(current.value())) {
                return CompletableFuture.failedFuture(
                    new ExternalRegistrationInvalidatedException(id, false));
            }
            StreamConfigData config = parse(id, current.value());
            return config.isOwnedBy(claim)
                ? CompletableFuture.completedFuture(null)
                : CompletableFuture.failedFuture(
                    new ExternalRegistrationInvalidatedException(id, false));
        });
    }

    private CompletableFuture<Void> registerExternalStreamAttempt(
            StreamIdentifier id, int partitionCount, Map<String, String> creationProperties) {
        return beginExternalPartitionRegistration(
                id, partitionCount, creationProperties, UUID.randomUUID().toString())
            .thenCompose(registration -> {
                if (registration.claim().isEmpty()) {
                    return verifyExternalRegistration(id, registration);
                }
                ProvisioningClaim claim = registration.claim().orElseThrow();
                return finalizeCreation(id, claim).thenCompose(outcome -> {
                    if (!outcome.active()) {
                        return CompletableFuture.failedFuture(outcome.failure());
                    }
                    return verifyFinalizedExternalRegistration(id, claim);
                });
            });
    }

    CompletableFuture<Void> unregisterExternalStream(StreamIdentifier id) {
        Objects.requireNonNull(id, "id");
        return unregisterExternalStreamAttempt(id);
    }

    private CompletableFuture<Void> unregisterExternalStreamAttempt(StreamIdentifier id) {
        String path = catalogPaths.streamConfigPath(id);
        return oxiaClient.get(path).thenCompose(current -> {
            if (current == null) {
                return CompletableFuture.completedFuture(null);
            }
            StreamConfigData config = parse(id, current.value());
            if (config.creationKind().orElse(null) == CreationKind.NATIVE_CREATE) {
                return CompletableFuture.failedFuture(new AlreadyExistsException(
                    "Stream is owned by a native catalog creation: " + id.fullName()));
            }
            if (config.provisioningState() == ProvisioningState.PERMANENTLY_DELETED) {
                return CompletableFuture.completedFuture(null);
            }
            if (config.provisioningState() == ProvisioningState.UNREGISTERED
                    || config.provisioningState() == ProvisioningState.ABORTING
                    || config.provisioningState() == ProvisioningState.DROPPED) {
                return CompletableFuture.completedFuture(null);
            }
            StreamConfigData unregistered = config.unregister();
            return putWithResult(path, unregistered,
                    Set.of(PutOption.IfVersionIdEquals(current.version().versionId())))
                .handle((write, failure) ->
                    new CreateWrite(write, unwrapNullable(failure)))
                .thenCompose(write -> resolveUnregisterWrite(id, unregistered, write));
        });
    }

    private CompletableFuture<Void> resolveUnregisterWrite(
            StreamIdentifier id, StreamConfigData desired, CreateWrite write) {
        if (write.failure() == null) {
            return CompletableFuture.completedFuture(null);
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
                        || isPermanentDeletionTombstone(check.config().value())) {
                    return CompletableFuture.completedFuture(null);
                }
                StreamConfigData current = parse(id, check.config().value());
                if (current.provisioningState() == ProvisioningState.UNREGISTERED
                        && current.incarnationId().equals(desired.incarnationId())
                        && current.ownerGeneration() == desired.ownerGeneration()) {
                    return CompletableFuture.completedFuture(null);
                }
                if (write.failure() instanceof UnexpectedVersionIdException) {
                    return unregisterExternalStreamAttempt(id);
                }
                return CompletableFuture.failedFuture(write.failure());
            });
    }

    CompletableFuture<Void> permanentlyDeleteExternalStream(StreamIdentifier id) {
        Objects.requireNonNull(id, "id");
        return permanentlyDeleteExternalStreamAttempt(id);
    }

    private CompletableFuture<Void> permanentlyDeleteExternalStreamAttempt(StreamIdentifier id) {
        String path = catalogPaths.streamConfigPath(id);
        return oxiaClient.get(path).thenCompose(current -> {
            StreamConfigData deleted;
            if (current != null) {
                StreamConfigData config = parse(id, current.value());
                if (config.creationKind().orElse(null) == CreationKind.NATIVE_CREATE) {
                    return CompletableFuture.failedFuture(new AlreadyExistsException(
                        "Stream is owned by a native catalog creation: " + id.fullName()));
                }
                if (config.provisioningState()
                        == ProvisioningState.PERMANENTLY_DELETED) {
                    return CompletableFuture.completedFuture(null);
                }
                deleted = config.permanentlyDelete(UUID.randomUUID().toString());
            } else {
                deleted = StreamConfigData.emptyPermanentDeletion();
            }
            Set<PutOption> options = current == null
                ? Set.of(PutOption.IfRecordDoesNotExist)
                : Set.of(PutOption.IfVersionIdEquals(current.version().versionId()));
            return putWithResult(path, deleted, options)
                .handle((write, failure) ->
                    new CreateWrite(write, unwrapNullable(failure)))
                .thenCompose(write -> resolvePermanentDeletionWrite(id, deleted, write));
        });
    }

    private CompletableFuture<Void> resolvePermanentDeletionWrite(
            StreamIdentifier id, StreamConfigData desired, CreateWrite write) {
        if (write.failure() == null) {
            return CompletableFuture.completedFuture(null);
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
                if (check.config() != null
                        && isPermanentDeletionTombstone(check.config().value())) {
                    return CompletableFuture.completedFuture(null);
                }
                if (write.failure() instanceof KeyAlreadyExistsException
                        || write.failure() instanceof UnexpectedVersionIdException) {
                    return permanentlyDeleteExternalStreamAttempt(id);
                }
                return CompletableFuture.failedFuture(write.failure());
            });
    }

    CompletableFuture<StreamConfigData> read(StreamIdentifier id) {
        return readActive(id).thenApply(ActiveStreamConfig::config);
    }

    CompletableFuture<ActiveStreamConfig> readActive(StreamIdentifier id) {
        String path = catalogPaths.streamConfigPath(id);
        return oxiaClient.get(path).thenApply(result -> {
            if (result == null || isPermanentDeletionTombstone(result.value())
                    || isProvisioning(result.value())) {
                throw new NoSuchStreamException(id);
            }
            return new ActiveStreamConfig(
                parse(id, result.value()), result.version().versionId());
        });
    }

    CompletableFuture<ExternalDeletionContext> readExternalDeletionContext(
            StreamIdentifier id) {
        String path = catalogPaths.streamConfigPath(id);
        return oxiaClient.get(path).thenCompose(result -> {
            if (result == null) {
                return CompletableFuture.failedFuture(new NoSuchStreamException(id));
            }
            StreamConfigData config = parse(id, result.value());
            if (config.creationKind().orElse(CreationKind.EXTERNAL)
                    == CreationKind.NATIVE_CREATE) {
                return CompletableFuture.failedFuture(new AlreadyExistsException(
                    "Stream is owned by a native catalog creation: " + id.fullName()));
            }
            if (config.provisioningState() != ProvisioningState.ACTIVE
                    && config.provisioningState() != ProvisioningState.ABORTING
                    && config.provisioningState() != ProvisioningState.UNREGISTERED
                    && config.provisioningState()
                        != ProvisioningState.PERMANENTLY_DELETED) {
                return CompletableFuture.failedFuture(new NoSuchStreamException(id));
            }
            return CompletableFuture.completedFuture(new ExternalDeletionContext(
                config, result.version().versionId()));
        });
    }

    CompletableFuture<Void> verifyExternalDeletionContext(
            StreamIdentifier id, ExternalDeletionContext expected) {
        String path = catalogPaths.streamConfigPath(id);
        return oxiaClient.get(path).thenCompose(result -> {
            if (result == null) {
                return CompletableFuture.failedFuture(
                    new ExternalDeletionContextInvalidatedException(id));
            }
            StreamConfigData current = parse(id, result.value());
            if (current.provisioningState() != expected.config().provisioningState()
                    || !current.incarnationId().equals(expected.config().incarnationId())
                    || !current.ownerToken().equals(expected.config().ownerToken())
                    || !current.creationKind().equals(expected.config().creationKind())
                    || current.ownerGeneration() != expected.config().ownerGeneration()
                    || current.metadataSourceGeneration()
                        != expected.config().metadataSourceGeneration()) {
                return CompletableFuture.failedFuture(
                    new ExternalDeletionContextInvalidatedException(id));
            }
            if ((current.provisioningState() == ProvisioningState.ACTIVE
                    || current.provisioningState() == ProvisioningState.ABORTING)
                    && result.version().versionId() != expected.versionId()) {
                return CompletableFuture.failedFuture(
                    new ExternalDeletionContextInvalidatedException(id));
            }
            return CompletableFuture.completedFuture(null);
        });
    }

    CompletableFuture<Void> verifyActiveOwnership(
            StreamIdentifier id, ActiveStreamConfig expected) {
        String path = catalogPaths.streamConfigPath(id);
        return oxiaClient.get(path).thenCompose(result -> {
            if (result == null || isPermanentDeletionTombstone(result.value())
                    || isProvisioning(result.value())) {
                return CompletableFuture.failedFuture(new NoSuchStreamException(id));
            }
            StreamConfigData current = parse(id, result.value());
            if (current.sameActiveLifecycle(expected.config(),
                    result.version().versionId(), expected.versionId())) {
                return CompletableFuture.completedFuture(null);
            }
            return CompletableFuture.failedFuture(new NoSuchStreamException(id));
        });
    }

    CompletableFuture<Boolean> exists(StreamIdentifier id) {
        return oxiaClient.get(catalogPaths.streamConfigPath(id)).thenApply(result ->
            result != null && !isPermanentDeletionTombstone(result.value())
                && !isProvisioning(result.value()));
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
                return CompletableFuture.completedFuture(null);
            }
            String reason = isPermanentDeletionTombstone(result.value())
                ? "Stream identity was permanently deleted: "
                : "Stream already exists: ";
            return CompletableFuture.failedFuture(
                new AlreadyExistsException(reason + id.fullName()));
        });
    }

    CompletableFuture<ProvisioningClaim> claimCreation(
            StreamIdentifier id, int partitions, Map<String, String> properties,
            Optional<TableMaterializationPolicy> materialization, String ownerToken) {
        return claimCreation(
            id, partitions, properties, materialization,
            CreationKind.NATIVE_CREATE, ownerToken);
    }

    CompletableFuture<ProvisioningClaim> claimCreation(
            StreamIdentifier id, int partitions, Map<String, String> properties,
            Optional<TableMaterializationPolicy> materialization,
            CreationKind kind, String ownerToken) {
        Objects.requireNonNull(id, "id");
        Objects.requireNonNull(properties, "properties");
        Objects.requireNonNull(materialization, "materialization");
        Objects.requireNonNull(kind, "kind");
        Objects.requireNonNull(ownerToken, "ownerToken");
        if (partitions <= 0) {
            throw new IllegalArgumentException("partitions must be positive");
        }
        return claimCreationAttempt(
            id, partitions, properties, materialization, kind, ownerToken,
            Optional.empty());
    }

    private CompletableFuture<ProvisioningClaim> claimCreationAttempt(
            StreamIdentifier id, int partitions, Map<String, String> properties,
            Optional<TableMaterializationPolicy> materialization,
            CreationKind kind, String ownerToken,
            Optional<String> requiredIncarnation) {
        String path = catalogPaths.streamConfigPath(id);
        return oxiaClient.get(path).thenCompose(result -> {
            if (result == null) {
                if (requiredIncarnation.isPresent()) {
                    return CompletableFuture.failedFuture(new AlreadyExistsException(
                        "Stream creation lifecycle changed: " + id.fullName()));
                }
                StreamConfigData desired = StreamConfigData.provisioning(
                    partitions, properties, materialization, kind,
                    UUID.randomUUID().toString(), ownerToken);
                return writeInitialClaim(id, desired);
            }
            if (isPermanentDeletionTombstone(result.value())) {
                return kind == CreationKind.EXTERNAL
                    ? CompletableFuture.failedFuture(new NoSuchStreamException(id))
                    : CompletableFuture.failedFuture(new AlreadyExistsException(
                        "Stream identity was permanently deleted: " + id.fullName()));
            }
            StreamConfigData current = parse(id, result.value());
            if (requiredIncarnation.isPresent()
                    && !current.incarnationId().equals(requiredIncarnation)) {
                return CompletableFuture.failedFuture(new AlreadyExistsException(
                    "Stream creation lifecycle changed: " + id.fullName()));
            }
            if (current.provisioningState() == ProvisioningState.DROPPED) {
                if (requiredIncarnation.isPresent()) {
                    return CompletableFuture.failedFuture(new AlreadyExistsException(
                        "Stream creation lifecycle changed: " + id.fullName()));
                }
                StreamConfigData recreated = current.recreate(
                    partitions, properties, materialization, kind,
                    UUID.randomUUID().toString(), ownerToken);
                return writeTakeoverClaim(
                    id, recreated, result.version().versionId());
            }
            if (current.provisioningState() == ProvisioningState.UNREGISTERED) {
                if (kind != CreationKind.EXTERNAL
                        || current.creationKind().orElse(CreationKind.EXTERNAL)
                            != CreationKind.EXTERNAL) {
                    return CompletableFuture.failedFuture(new AlreadyExistsException(
                        "Stream lifecycle kind does not match the retained registration: "
                            + id.fullName()));
                }
                StreamConfigData resumed = current.resumeExternalRegistration(
                    ownerToken, partitions);
                return writeTakeoverClaim(id, resumed, result.version().versionId());
            }
            if (!current.provisioning()) {
                return CompletableFuture.failedFuture(new AlreadyExistsException(
                    "Stream already exists: " + id.fullName()));
            }
            if (current.provisioningState() == ProvisioningState.ABORTING) {
                return CompletableFuture.failedFuture(new AlreadyExistsException(
                    "Stream creation is aborting: " + id.fullName()));
            }
            if (!current.compatibleWith(
                    partitions, properties, materialization, kind)) {
                return CompletableFuture.failedFuture(new AlreadyExistsException(
                    "A different stream creation is already provisioning: " + id.fullName()));
            }
            if (current.ownerToken().equals(Optional.of(ownerToken))) {
                return CompletableFuture.completedFuture(new ProvisioningClaim(
                    current, current.incarnationId().orElseThrow(), ownerToken, kind,
                    current.ownerGeneration(),
                    result.version().versionId()));
            }
            StreamConfigData takeover = current.takeover(ownerToken, partitions);
            return writeTakeoverClaim(id, takeover, result.version().versionId());
        });
    }

    private CompletableFuture<ProvisioningClaim> writeInitialClaim(
            StreamIdentifier id, StreamConfigData desired) {
        String path = catalogPaths.streamConfigPath(id);
        return putWithResult(path, desired, Set.of(PutOption.IfRecordDoesNotExist))
            .handle((result, failure) ->
                new CreateWrite(result, unwrapNullable(failure)))
            .thenCompose(write -> resolveClaimWrite(
                id, desired, -1L, write, true));
    }

    private CompletableFuture<ProvisioningClaim> writeTakeoverClaim(
            StreamIdentifier id, StreamConfigData desired, long expectedVersion) {
        String path = catalogPaths.streamConfigPath(id);
        return putWithResult(path, desired,
                Set.of(PutOption.IfVersionIdEquals(expectedVersion)))
            .handle((result, failure) ->
                new CreateWrite(result, unwrapNullable(failure)))
            .thenCompose(write -> resolveClaimWrite(
                id, desired, expectedVersion, write, false));
    }

    private CompletableFuture<ProvisioningClaim> resolveClaimWrite(
            StreamIdentifier id, StreamConfigData desired, long previousVersion,
            CreateWrite write, boolean initialWrite) {
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
                if (check.config() != null
                        && !isPermanentDeletionTombstone(check.config().value())) {
                    StreamConfigData current = parse(id, check.config().value());
                    if (current.isOwnedBy(desired)) {
                        return CompletableFuture.completedFuture(claim(
                            current, check.config().version().versionId()));
                    }
                }
                if (check.config() != null
                        && isPermanentDeletionTombstone(check.config().value())) {
                    return desired.creationKind().orElseThrow() == CreationKind.EXTERNAL
                        ? CompletableFuture.failedFuture(
                            new NoSuchStreamException(id, write.failure()))
                        : CompletableFuture.failedFuture(new AlreadyExistsException(
                            "Stream identity was permanently deleted: " + id.fullName()));
                }
                if (write.failure() instanceof KeyAlreadyExistsException
                        || write.failure() instanceof UnexpectedVersionIdException) {
                    if (initialWrite) {
                        return CompletableFuture.failedFuture(new AlreadyExistsException(
                            "Stream creation raced another lifecycle: " + id.fullName()));
                    }
                    return claimCreationAttempt(
                        id, desired.partitions(), desired.properties(),
                        desired.materialization(), desired.creationKind().orElseThrow(),
                        desired.ownerToken().orElseThrow(), desired.incarnationId());
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
            if (current == null || isPermanentDeletionTombstone(current.value())) {
                return CompletableFuture.failedFuture(
                    new ProvisioningOwnershipLostException(id));
            }
            StreamConfigData config = parse(id, current.value());
            if (config.provisioningState() == ProvisioningState.PROVISIONING
                    && current.version().versionId() == claim.versionId()
                    && config.isOwnedBy(claim)) {
                return CompletableFuture.completedFuture(null);
            }
            return CompletableFuture.failedFuture(
                new ProvisioningOwnershipLostException(id));
        });
    }

    CompletableFuture<Boolean> canCleanupRejectedNativeAllocation(
            StreamIdentifier id, ProvisioningClaim claim) {
        String path = catalogPaths.streamConfigPath(id);
        return oxiaClient.get(path).thenApply(current -> {
            if (current == null) {
                // Older writers physically deleted completed drops. The allocation key embeds the
                // rejected claim's incarnation, so conditional mapping cleanup cannot affect a
                // subsequently recreated stream.
                return true;
            }
            StreamConfigData config = parse(id, current.value());
            return config.incarnationId().equals(Optional.of(claim.incarnationId()))
                && config.creationKind().orElse(null) == CreationKind.NATIVE_CREATE
                && (config.provisioningState() == ProvisioningState.ABORTING
                    || config.provisioningState() == ProvisioningState.DROPPED);
        });
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
                        if (check.config() == null
                                || isPermanentDeletionTombstone(check.config().value())) {
                            return FinalizeOutcome.indeterminate(failure);
                        }
                        StreamConfigData current = parse(id, check.config().value());
                        if (!current.provisioning() && current.isOwnedBy(claim)) {
                            return FinalizeOutcome.successful();
                        }
                        return FinalizeOutcome.indeterminate(failure);
                    });
            });
    }

    CompletableFuture<Optional<DropClaim>> beginDrop(
            StreamIdentifier id, String ownerToken) {
        Objects.requireNonNull(id, "id");
        Objects.requireNonNull(ownerToken, "ownerToken");
        return beginDropAttempt(id, ownerToken);
    }

    private CompletableFuture<Optional<DropClaim>> beginDropAttempt(
            StreamIdentifier id, String ownerToken) {
        String path = catalogPaths.streamConfigPath(id);
        return oxiaClient.get(path).thenCompose(current -> {
            if (current == null || isLifecycleTombstone(current.value())) {
                return CompletableFuture.completedFuture(Optional.empty());
            }
            StreamConfigData config = parse(id, current.value());
            if (config.provisioningState() == ProvisioningState.ABORTING
                    && config.ownerToken().equals(Optional.of(ownerToken))) {
                return CompletableFuture.completedFuture(Optional.of(new DropClaim(
                    config, ownerToken, current.version().versionId())));
            }
            StreamConfigData aborting = config.abort(ownerToken);
            return putWithResult(path, aborting,
                    Set.of(PutOption.IfVersionIdEquals(current.version().versionId())))
                .handle((result, failure) ->
                    new CreateWrite(result, unwrapNullable(failure)))
                .thenCompose(write -> resolveDropClaimWrite(id, ownerToken, aborting, write));
        });
    }

    private CompletableFuture<Optional<DropClaim>> resolveDropClaimWrite(
            StreamIdentifier id, String ownerToken, StreamConfigData desired,
            CreateWrite write) {
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
                    return CompletableFuture.completedFuture(Optional.of(new DropClaim(
                        current, ownerToken, check.config().version().versionId())));
                }
                if (write.failure() instanceof KeyAlreadyExistsException
                        || write.failure() instanceof UnexpectedVersionIdException) {
                    return beginDropAttempt(id, ownerToken);
                }
                return CompletableFuture.failedFuture(write.failure());
            });
    }

    CompletableFuture<Void> verifyAbortingOwnership(
            StreamIdentifier id, DropClaim claim) {
        String path = catalogPaths.streamConfigPath(id);
        return oxiaClient.get(path).thenCompose(current -> {
            if (current == null || isPermanentDeletionTombstone(current.value())) {
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

    CompletableFuture<Void> completeDrop(StreamIdentifier id, DropClaim claim) {
        String path = catalogPaths.streamConfigPath(id);
        StreamConfigData dropped = claim.config().completeDrop();
        return putWithResult(path, dropped,
                Set.of(PutOption.IfVersionIdEquals(claim.versionId())))
            .handle((result, failure) -> new CreateWrite(
                result, unwrapNullable(failure)))
            .thenCompose(write -> {
                if (write.failure() == null) {
                    return CompletableFuture.completedFuture(null);
                }
                return oxiaClient.get(path)
                    .handle((current, readFailure) ->
                        new ConfigCheck(current, unwrapNullable(readFailure)))
                    .thenCompose(check -> {
                        if (check.failure() != null) {
                            if (write.failure() != null) {
                                write.failure().addSuppressed(check.failure());
                                return CompletableFuture.failedFuture(write.failure());
                            }
                            return CompletableFuture.failedFuture(check.failure());
                        }
                        if (check.config() == null) {
                            return CompletableFuture.failedFuture(
                                new AbortingOwnershipLostException(id));
                        }
                        StreamConfigData current = parse(id, check.config().value());
                        if (current.sameCompletedDrop(dropped)) {
                            return CompletableFuture.completedFuture(null);
                        }
                        if (!current.isOwnedBy(claim)) {
                            return CompletableFuture.failedFuture(
                                new AbortingOwnershipLostException(id));
                        }
                        return CompletableFuture.failedFuture(write.failure());
                    });
            });
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
        String path = catalogPaths.streamConfigPath(id);
        return oxiaClient.get(path).thenCompose(result -> {
            if (result == null || isPermanentDeletionTombstone(result.value())
                    || isProvisioning(result.value())) {
                return CompletableFuture.failedFuture(new NoSuchStreamException(id));
            }
            StreamConfigData updated = mutation.apply(parse(id, result.value()));
            return put(path, updated,
                Set.of(PutOption.IfVersionIdEquals(result.version().versionId())))
                .handle((ignored, failure) -> failure)
                .thenCompose(failure -> {
                    if (failure == null) {
                        return CompletableFuture.completedFuture(null);
                    }
                    Throwable cause = unwrap(failure);
                    if (cause instanceof UnexpectedVersionIdException) {
                        return update(id, mutation);
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
        config.materialization().ifPresent(policy ->
            node.set("materialization", MaterializationJson.policyToJson(policy)));
        config.incarnationId().ifPresent(value -> node.put(INCARNATION_ID_FIELD, value));
        config.ownerToken().ifPresent(value -> node.put(OWNER_TOKEN_FIELD, value));
        if (config.ownerGeneration() >= 0) {
            node.put(OWNER_GENERATION_FIELD, config.ownerGeneration());
        }
        if (config.metadataSourceGeneration() != NO_METADATA_GENERATION) {
            node.put(METADATA_SOURCE_GENERATION_FIELD, config.metadataSourceGeneration());
        }
        config.creationKind().ifPresent(value -> node.put(CREATION_KIND_FIELD, value.name()));
        if (config.provisioning()) {
            node.put(PROVISIONING_FIELD, true);
            node.put(PROVISIONING_STATE_FIELD, config.provisioningState().name());
        }
        if (config.provisioningState() == ProvisioningState.PERMANENTLY_DELETED) {
            node.put(PERMANENT_DELETION_FIELD, true);
        }
        return MAPPER.writeValueAsBytes(node);
    }

    private static boolean isPermanentDeletionTombstone(byte[] value) {
        try {
            JsonNode node = MAPPER.readTree(value);
            return node.path(PERMANENT_DELETION_FIELD).asBoolean(false);
        } catch (Exception e) {
            return false;
        }
    }

    private static boolean isLifecycleTombstone(byte[] value) {
        if (isPermanentDeletionTombstone(value)) {
            return true;
        }
        try {
            JsonNode node = MAPPER.readTree(value);
            return node.path(PROVISIONING_FIELD).asBoolean(false)
                && ProvisioningState.DROPPED.name().equals(
                    node.path(PROVISIONING_STATE_FIELD).asText());
        } catch (Exception e) {
            return false;
        }
    }

    private static boolean isProvisioning(byte[] value) {
        try {
            return MAPPER.readTree(value).path(PROVISIONING_FIELD).asBoolean(false);
        } catch (Exception e) {
            return false;
        }
    }

    private static StreamConfigData parse(StreamIdentifier id, byte[] value) {
        try {
            JsonNode node = MAPPER.readTree(value);
            int partitions = node.path("partitions").asInt(0);
            Map<String, String> properties = node.has("properties")
                ? MAPPER.convertValue(node.get("properties"),
                    new TypeReference<Map<String, String>>() {})
                : Map.of();
            Optional<TableMaterializationPolicy> materialization =
                node.has("materialization") && !node.get("materialization").isNull()
                    ? Optional.of(MaterializationJson.policyFromJson(node.get("materialization")))
                    : Optional.empty();
            Optional<String> incarnationId = node.hasNonNull(INCARNATION_ID_FIELD)
                ? Optional.of(node.get(INCARNATION_ID_FIELD).asText())
                : Optional.empty();
            Optional<String> ownerToken = node.hasNonNull(OWNER_TOKEN_FIELD)
                ? Optional.of(node.get(OWNER_TOKEN_FIELD).asText())
                : Optional.empty();
            long ownerGeneration = node.hasNonNull(OWNER_GENERATION_FIELD)
                ? node.get(OWNER_GENERATION_FIELD).asLong() : -1L;
            long metadataSourceGeneration = node.hasNonNull(METADATA_SOURCE_GENERATION_FIELD)
                ? node.get(METADATA_SOURCE_GENERATION_FIELD).asLong()
                : NO_METADATA_GENERATION;
            Optional<CreationKind> creationKind = node.hasNonNull(CREATION_KIND_FIELD)
                ? Optional.of(CreationKind.valueOf(node.get(CREATION_KIND_FIELD).asText()))
                : Optional.empty();
            boolean permanentDeletion = node.path(PERMANENT_DELETION_FIELD).asBoolean(false);
            boolean provisioning = node.path(PROVISIONING_FIELD).asBoolean(false);
            ProvisioningState provisioningState = permanentDeletion
                ? ProvisioningState.PERMANENTLY_DELETED
                : provisioning
                    ? ProvisioningState.valueOf(node.path(PROVISIONING_STATE_FIELD)
                        .asText(ProvisioningState.PROVISIONING.name()))
                    : ProvisioningState.ACTIVE;
            if (permanentDeletion && creationKind.isEmpty()) {
                creationKind = Optional.of(CreationKind.EXTERNAL);
            }
            return new StreamConfigData(
                partitions, properties, materialization, incarnationId,
                ownerToken, creationKind, ownerGeneration,
                metadataSourceGeneration, provisioningState);
        } catch (Exception e) {
            throw new RuntimeException("Failed to parse stream config for: " + id.fullName(), e);
        }
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

    enum CreationKind {
        NATIVE_CREATE,
        EXTERNAL
    }

    enum ProvisioningState {
        ACTIVE,
        PROVISIONING,
        ABORTING,
        UNREGISTERED,
        DROPPED,
        PERMANENTLY_DELETED;

        boolean retainsExternalDeletionSpec() {
            return this == ABORTING
                || this == UNREGISTERED
                || this == PERMANENTLY_DELETED;
        }
    }

    record StreamConfigData(
            int partitions, Map<String, String> properties,
            Optional<TableMaterializationPolicy> materialization,
            Optional<String> incarnationId, Optional<String> ownerToken,
            Optional<CreationKind> creationKind,
            long ownerGeneration, long metadataSourceGeneration,
            ProvisioningState provisioningState) {

        StreamConfigData {
            properties = Map.copyOf(properties);
            Objects.requireNonNull(materialization, "materialization");
            Objects.requireNonNull(incarnationId, "incarnationId");
            Objects.requireNonNull(ownerToken, "ownerToken");
            Objects.requireNonNull(creationKind, "creationKind");
            Objects.requireNonNull(provisioningState, "provisioningState");
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
        }

        StreamConfigData(
                int partitions, Map<String, String> properties,
                Optional<TableMaterializationPolicy> materialization) {
            this(partitions, properties, materialization, Optional.empty(),
                Optional.empty(), Optional.empty(), LEGACY_METADATA_GENERATION,
                NO_METADATA_GENERATION,
                ProvisioningState.ACTIVE);
        }

        static StreamConfigData provisioning(
                int partitions, Map<String, String> properties,
                Optional<TableMaterializationPolicy> materialization,
                CreationKind creationKind, String incarnationId, String ownerToken) {
            return new StreamConfigData(
                partitions, properties, materialization, Optional.of(incarnationId),
                Optional.of(ownerToken), Optional.of(creationKind), 1L,
                NO_METADATA_GENERATION,
                ProvisioningState.PROVISIONING);
        }

        StreamConfigData recreate(
                int requestedPartitions, Map<String, String> requestedProperties,
                Optional<TableMaterializationPolicy> requestedMaterialization,
                CreationKind requestedKind, String newIncarnationId,
                String newOwnerToken) {
            if (provisioningState != ProvisioningState.DROPPED) {
                throw new IllegalStateException("Only a dropped stream can be recreated");
            }
            return new StreamConfigData(
                requestedPartitions, requestedProperties, requestedMaterialization,
                Optional.of(newIncarnationId), Optional.of(newOwnerToken),
                Optional.of(requestedKind), ownerGeneration + 1,
                NO_METADATA_GENERATION, ProvisioningState.PROVISIONING);
        }

        boolean provisioning() {
            return provisioningState != ProvisioningState.ACTIVE;
        }

        boolean isActiveExternalOrLegacy() {
            return provisioningState == ProvisioningState.ACTIVE
                && (creationKind.isEmpty() || creationKind.get() == CreationKind.EXTERNAL);
        }

        boolean sameActiveLifecycle(
                StreamConfigData expected, long currentVersion, long expectedVersion) {
            if (provisioning() || expected.provisioning()) {
                return false;
            }
            if (expected.incarnationId().isPresent()) {
                return incarnationId.equals(expected.incarnationId)
                    && ownerToken.equals(expected.ownerToken)
                    && ownerGeneration == expected.ownerGeneration
                    && creationKind.equals(expected.creationKind);
            }
            return incarnationId.isEmpty()
                && ownerToken.isEmpty()
                && currentVersion == expectedVersion;
        }

        boolean compatibleWith(
                int requestedPartitions, Map<String, String> requestedProperties,
                Optional<TableMaterializationPolicy> requestedMaterialization,
                CreationKind requestedKind) {
            if (creationKind.orElse(null) != requestedKind
                    || !properties.equals(requestedProperties)
                    || !materialization.equals(requestedMaterialization)) {
                return false;
            }
            return requestedKind == CreationKind.EXTERNAL
                || partitions == requestedPartitions;
        }

        StreamConfigData takeover(String newOwnerToken, int requestedPartitions) {
            int updatedPartitions = creationKind.orElseThrow() == CreationKind.EXTERNAL
                ? Math.max(partitions, requestedPartitions) : partitions;
            return new StreamConfigData(
                updatedPartitions, properties, materialization, incarnationId,
                Optional.of(newOwnerToken), creationKind, ownerGeneration + 1,
                metadataSourceGeneration, ProvisioningState.PROVISIONING);
        }

        StreamConfigData resumeExternalRegistration(
                String newOwnerToken, int requestedPartitions) {
            if (incarnationId.isEmpty()) {
                return new StreamConfigData(
                    Math.max(partitions, requestedPartitions), properties, materialization,
                    Optional.of(UUID.randomUUID().toString()), Optional.of(newOwnerToken),
                    Optional.of(CreationKind.EXTERNAL), 1L,
                    LEGACY_METADATA_GENERATION,
                    ProvisioningState.PROVISIONING);
            }
            return new StreamConfigData(
                Math.max(partitions, requestedPartitions), properties, materialization,
                incarnationId, Optional.of(newOwnerToken), creationKind,
                ownerGeneration + 1, ownerGeneration,
                ProvisioningState.PROVISIONING);
        }

        StreamConfigData activate() {
            return new StreamConfigData(
                partitions, properties, materialization, incarnationId, ownerToken,
                creationKind, ownerGeneration, metadataSourceGeneration,
                ProvisioningState.ACTIVE);
        }

        StreamConfigData abort(String newOwnerToken) {
            if (incarnationId.isEmpty()) {
                return new StreamConfigData(
                    partitions, properties, materialization,
                    Optional.of(UUID.randomUUID().toString()), Optional.of(newOwnerToken),
                    creationKind, 1L, LEGACY_METADATA_GENERATION,
                    ProvisioningState.ABORTING);
            }
            long cleanupGeneration = provisioningState == ProvisioningState.ABORTING
                ? metadataSourceGeneration : ownerGeneration;
            return new StreamConfigData(
                partitions, properties, materialization, incarnationId,
                Optional.of(newOwnerToken), creationKind, ownerGeneration + 1,
                cleanupGeneration, ProvisioningState.ABORTING);
        }

        StreamConfigData unregister() {
            return new StreamConfigData(
                partitions, properties, materialization, incarnationId, ownerToken,
                creationKind, ownerGeneration, ownerGeneration,
                ProvisioningState.UNREGISTERED);
        }

        StreamConfigData permanentlyDelete(String newOwnerToken) {
            long cleanupGeneration = provisioningState == ProvisioningState.ABORTING
                    || provisioningState == ProvisioningState.PERMANENTLY_DELETED
                ? metadataSourceGeneration : ownerGeneration;
            if (incarnationId.isEmpty()) {
                return new StreamConfigData(
                    partitions, properties, materialization,
                    Optional.of(UUID.randomUUID().toString()), Optional.of(newOwnerToken),
                    Optional.of(CreationKind.EXTERNAL), 1L,
                    LEGACY_METADATA_GENERATION,
                    ProvisioningState.PERMANENTLY_DELETED);
            }
            return new StreamConfigData(
                partitions, properties, materialization, incarnationId,
                Optional.of(newOwnerToken), Optional.of(CreationKind.EXTERNAL),
                ownerGeneration + 1, cleanupGeneration,
                ProvisioningState.PERMANENTLY_DELETED);
        }

        StreamConfigData completeDrop() {
            if (provisioningState != ProvisioningState.ABORTING) {
                throw new IllegalStateException("Only an aborting stream can complete deletion");
            }
            return new StreamConfigData(
                partitions, properties, materialization, incarnationId, ownerToken,
                creationKind, ownerGeneration, metadataSourceGeneration,
                ProvisioningState.DROPPED);
        }

        static StreamConfigData emptyPermanentDeletion() {
            return new StreamConfigData(
                0, Map.of(), Optional.empty(), Optional.empty(), Optional.empty(),
                Optional.of(CreationKind.EXTERNAL), LEGACY_METADATA_GENERATION,
                NO_METADATA_GENERATION,
                ProvisioningState.PERMANENTLY_DELETED);
        }

        StreamConfigData withPartitions(int newPartitions) {
            return new StreamConfigData(
                newPartitions, properties, materialization, incarnationId, ownerToken,
                creationKind, ownerGeneration, metadataSourceGeneration, provisioningState);
        }

        StreamConfigData withProperties(Map<String, String> newProperties) {
            return new StreamConfigData(
                partitions, newProperties, materialization, incarnationId, ownerToken,
                creationKind, ownerGeneration, metadataSourceGeneration, provisioningState);
        }

        StreamConfigData withMaterialization(
                Optional<TableMaterializationPolicy> newMaterialization) {
            return new StreamConfigData(
                partitions, properties, newMaterialization, incarnationId, ownerToken,
                creationKind, ownerGeneration, metadataSourceGeneration, provisioningState);
        }

        boolean isOwnedBy(StreamConfigData other) {
            return provisioning()
                && incarnationId.equals(other.incarnationId)
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
                && partitions == other.partitions
                && properties.equals(other.properties)
                && materialization.equals(other.materialization);
        }

        boolean isOwnedBy(DropClaim claim) {
            return provisioningState == ProvisioningState.ABORTING
                && ownerToken.equals(Optional.of(claim.ownerToken()))
                && incarnationId.equals(claim.config().incarnationId())
                && ownerGeneration == claim.config().ownerGeneration()
                && creationKind.equals(claim.config().creationKind());
        }

        boolean sameCompletedDrop(StreamConfigData expected) {
            return provisioningState == ProvisioningState.DROPPED
                && expected.provisioningState == ProvisioningState.DROPPED
                && incarnationId.equals(expected.incarnationId)
                && ownerToken.equals(expected.ownerToken)
                && ownerGeneration == expected.ownerGeneration
                && metadataSourceGeneration == expected.metadataSourceGeneration
                && creationKind.equals(expected.creationKind);
        }
    }

    record ProvisioningClaim(
            StreamConfigData config, String incarnationId, String ownerToken,
            CreationKind creationKind, long ownerGeneration, long versionId) {
    }

    record DropClaim(StreamConfigData config, String ownerToken, long versionId) {
    }

    record ActiveStreamConfig(StreamConfigData config, long versionId) {
    }

    record ExternalDeletionContext(StreamConfigData config, long versionId) {

        boolean permanent() {
            return config.provisioningState() == ProvisioningState.PERMANENTLY_DELETED;
        }

        long metadataGeneration() {
            return config.provisioningState() == ProvisioningState.ACTIVE
                ? config.ownerGeneration() : config.metadataSourceGeneration();
        }

        boolean canRetryWith(ExternalDeletionContext successor) {
            StreamConfigData previous = config;
            StreamConfigData next = successor.config();
            if (previous.creationKind().orElse(CreationKind.EXTERNAL)
                    != CreationKind.EXTERNAL
                    || next.creationKind().orElse(CreationKind.EXTERNAL)
                        != CreationKind.EXTERNAL) {
                return false;
            }
            if (previous.incarnationId().isEmpty()) {
                return next.provisioningState().retainsExternalDeletionSpec()
                    && next.metadataSourceGeneration() == LEGACY_METADATA_GENERATION;
            }
            if (!next.incarnationId().equals(previous.incarnationId())) {
                return false;
            }
            if (next.provisioningState() == ProvisioningState.ACTIVE) {
                return previous.provisioningState() == ProvisioningState.ACTIVE
                    && next.ownerGeneration() == previous.ownerGeneration()
                    && next.ownerToken().equals(previous.ownerToken())
                    && next.creationKind().equals(previous.creationKind());
            }
            return next.provisioningState().retainsExternalDeletionSpec()
                && next.ownerGeneration() >= previous.ownerGeneration()
                && next.metadataSourceGeneration() == metadataGeneration();
        }
    }

    static final class ExternalDeletionContextInvalidatedException
            extends NoSuchStreamException {

        private ExternalDeletionContextInvalidatedException(StreamIdentifier id) {
            super(id);
        }
    }

    record ExternalRegistration(
            Optional<ProvisioningClaim> claim,
            Optional<String> activeIncarnation,
            Optional<String> activeOwnerToken,
            Optional<CreationKind> activeKind, long activeOwnerGeneration,
            long activeMetadataSourceGeneration, long activeVersionId) {

        static ExternalRegistration provisioning(ProvisioningClaim claim) {
            return new ExternalRegistration(
                Optional.of(claim), Optional.empty(), Optional.empty(), Optional.empty(),
                LEGACY_METADATA_GENERATION, NO_METADATA_GENERATION, -1L);
        }

        static ExternalRegistration active(
                Optional<String> activeIncarnation, Optional<String> activeOwnerToken,
                Optional<CreationKind> activeKind, long activeOwnerGeneration,
                long activeMetadataSourceGeneration, long activeVersionId) {
            return new ExternalRegistration(
                Optional.empty(), activeIncarnation, activeOwnerToken,
                activeKind, activeOwnerGeneration,
                activeMetadataSourceGeneration, activeVersionId);
        }

        boolean matchesActive(StreamConfigData config, long versionId) {
            return activeIncarnation.isPresent()
                ? config.incarnationId().equals(activeIncarnation)
                    && config.ownerToken().equals(activeOwnerToken)
                    && config.ownerGeneration() == activeOwnerGeneration
                    && config.creationKind().equals(activeKind)
                : config.incarnationId().isEmpty()
                    && config.ownerToken().isEmpty()
                    && versionId == activeVersionId;
        }

        Optional<String> incarnationId() {
            return claim.map(ProvisioningClaim::incarnationId).or(() -> activeIncarnation);
        }

        Optional<String> ownerToken() {
            return claim.map(ProvisioningClaim::ownerToken).or(() -> activeOwnerToken);
        }

        long ownerGeneration() {
            return claim.map(ProvisioningClaim::ownerGeneration)
                .orElse(activeOwnerGeneration);
        }

        long metadataSourceGeneration() {
            return claim.map(value -> value.config().metadataSourceGeneration())
                .orElse(activeMetadataSourceGeneration);
        }

        boolean provisioning() {
            return claim.isPresent();
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

    static final class VerificationUnknownException extends RuntimeException {

        private VerificationUnknownException(Throwable cause) {
            super("Stream registration verification outcome is unknown", cause);
        }
    }

    static final class ProvisioningOwnershipLostException extends RuntimeException {

        private ProvisioningOwnershipLostException(StreamIdentifier id) {
            super("Stream provisioning ownership was lost: " + id.fullName());
        }
    }

    static final class AbortingOwnershipLostException extends RuntimeException {

        private AbortingOwnershipLostException(StreamIdentifier id) {
            super("Stream deletion ownership was lost: " + id.fullName());
        }
    }

    static final class ExternalRegistrationInvalidatedException
            extends NoSuchStreamException {

        private final boolean permanentDeletion;

        private ExternalRegistrationInvalidatedException(
                StreamIdentifier id, boolean permanentDeletion) {
            super(id);
            this.permanentDeletion = permanentDeletion;
        }

        boolean permanentDeletion() {
            return permanentDeletion;
        }
    }
}
