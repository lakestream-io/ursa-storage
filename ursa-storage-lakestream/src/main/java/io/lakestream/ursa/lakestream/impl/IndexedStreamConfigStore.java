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
import io.lakestream.api.exception.StreamPermanentlyDeletedException;
import io.lakestream.api.materialization.TableMaterializationPolicy;
import io.lakestream.ursa.lakestream.impl.materialization.MaterializationJson;
import io.oxia.client.api.AsyncOxiaClient;
import io.oxia.client.api.GetResult;
import io.oxia.client.api.PutResult;
import io.oxia.client.api.exceptions.KeyAlreadyExistsException;
import io.oxia.client.api.exceptions.UnexpectedVersionIdException;
import io.oxia.client.api.options.PutOption;
import java.nio.charset.StandardCharsets;
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
import java.util.function.Supplier;
import java.util.function.UnaryOperator;
import lombok.extern.slf4j.Slf4j;

/** Owns the persisted indexed-stream config schema and its versioned update semantics. */
@Slf4j
final class IndexedStreamConfigStore {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final byte[] EMPTY_NAMESPACE =
        "{\"properties\":{}}".getBytes(StandardCharsets.UTF_8);
    static final int MAX_CONFIG_WRITE_RETRIES = 3;
    static final long INITIAL_RETRY_BACKOFF_MILLIS = 10L;
    static final long NO_METADATA_GENERATION = -2L;
    static final long LEGACY_METADATA_GENERATION = -1L;
    private static final String PERMANENT_DELETION_FIELD =
        "_externalStreamPermanentlyDeleted";
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

    CompletableFuture<Void> ensureNamespaceExists(String namespace) {
        String path = catalogPaths.namespacePath(namespace);
        return oxiaClient.put(path, EMPTY_NAMESPACE, Set.of(PutOption.IfRecordDoesNotExist))
            .handle((ignored, failure) -> {
                Throwable cause = unwrapNullable(failure);
                if (cause == null || cause instanceof KeyAlreadyExistsException) {
                    return null;
                }
                throw new CompletionException(cause);
            });
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

    CompletableFuture<Void> establishExternalRecoveryAnchor(
            StreamIdentifier id, int partitionCount, Map<String, String> properties) {
        Objects.requireNonNull(id, "id");
        Objects.requireNonNull(properties, "properties");
        if (partitionCount <= 0) {
            throw new IllegalArgumentException("partitionCount must be positive");
        }
        return establishExternalRecoveryAnchorAttempt(
            id, partitionCount, properties, 0);
    }

    private CompletableFuture<Void> establishExternalRecoveryAnchorAttempt(
            StreamIdentifier id, int partitionCount, Map<String, String> properties,
            int retryCount) {
        String path = catalogPaths.streamConfigPath(id);
        return oxiaClient.get(path).thenCompose(current -> {
            if (current != null) {
                if (isPermanentDeletionTombstone(current.value())) {
                    return CompletableFuture.failedFuture(
                        new StreamPermanentlyDeletedException(id));
                }
                StreamConfigData config = parse(id, current.value());
                if (config.isActiveExternalOrLegacy()) {
                    return CompletableFuture.completedFuture(null);
                }
                return CompletableFuture.failedFuture(
                    new ExternalRegistrationLifecycleConflictException(
                        "Stream lifecycle changed while establishing a legacy recovery anchor: "
                            + id.fullName()));
            }
            StreamConfigData anchor = StreamConfigData.provisioning(
                partitionCount, properties, Optional.empty(), CreationKind.EXTERNAL,
                UUID.randomUUID().toString(), UUID.randomUUID().toString()).activate();
            return putWithResult(path, anchor, Set.of(PutOption.IfRecordDoesNotExist))
                .handle((write, failure) ->
                    new CreateWrite(write, unwrapNullable(failure)))
                .thenCompose(write -> {
                    if (write.failure() == null) {
                        return CompletableFuture.completedFuture(null);
                    }
                    if (write.failure() instanceof KeyAlreadyExistsException) {
                        return retryAfterConflict(
                            id, "legacy external recovery anchor", retryCount,
                            write.failure(),
                            () -> establishExternalRecoveryAnchorAttempt(
                                id, partitionCount, properties, retryCount + 1));
                    }
                    return oxiaClient.get(path).thenCompose(observed -> {
                        if (observed != null
                                && !isPermanentDeletionTombstone(observed.value())) {
                            StreamConfigData observedConfig = parse(id, observed.value());
                            if (observedConfig.isActiveExternalOrLegacy()) {
                                logResolvedWrite(
                                    id, "legacy external recovery anchor",
                                    observed.version().versionId(), write.failure());
                                return CompletableFuture.completedFuture(null);
                            }
                        }
                        return CompletableFuture.failedFuture(write.failure());
                    });
                });
        });
    }

    CompletableFuture<ExternalRegistration> beginExternalPartitionRegistration(
            StreamIdentifier id, int partitionCount, Map<String, String> properties,
            String attemptToken) {
        return beginExternalPartitionRegistration(
            id, partitionCount, properties, attemptToken, false);
    }

    CompletableFuture<ExternalRegistration> beginExternalPartitionRegistration(
            StreamIdentifier id, int partitionCount, Map<String, String> properties,
            String attemptToken, boolean requireModernOwner) {
        Objects.requireNonNull(id, "id");
        Objects.requireNonNull(properties, "properties");
        Objects.requireNonNull(attemptToken, "attemptToken");
        if (partitionCount <= 0) {
            throw new IllegalArgumentException("partitionCount must be positive");
        }
        return beginExternalPartitionRegistrationAttempt(
            id, partitionCount, properties, attemptToken, requireModernOwner, 0);
    }

    private CompletableFuture<ExternalRegistration> beginExternalPartitionRegistrationAttempt(
            StreamIdentifier id, int partitionCount, Map<String, String> properties,
            String ownerToken, boolean requireModernOwner, int retryCount) {
        String path = catalogPaths.streamConfigPath(id);
        return oxiaClient.get(path).thenCompose(result -> {
            if (result == null) {
                return claimCreation(
                        id, partitionCount, properties, Optional.empty(),
                        CreationKind.EXTERNAL, ownerToken)
                    .thenApply(ExternalRegistration::provisioning);
            }
            if (isProvisioning(result.value())) {
                StreamConfigData provisioning = parse(id, result.value());
                if (provisioning.creationKind().orElse(null)
                        == CreationKind.NATIVE_CREATE) {
                    return CompletableFuture.failedFuture(
                        new ExternalRegistrationLifecycleConflictException(
                            "Stream is owned by a native catalog creation: "
                                + id.fullName()));
                }
                return claimCreation(
                        id, partitionCount, properties, Optional.empty(),
                        CreationKind.EXTERNAL, ownerToken)
                    .thenApply(ExternalRegistration::provisioning);
            }
            if (isPermanentDeletionTombstone(result.value())) {
                return CompletableFuture.failedFuture(
                    new StreamPermanentlyDeletedException(id));
            }
            StreamConfigData existing = parse(id, result.value());
            if (!existing.isActiveExternalOrLegacy()) {
                return CompletableFuture.failedFuture(
                    new ExternalRegistrationLifecycleConflictException(
                        "Stream is owned by a native catalog creation: " + id.fullName()));
            }
            if (requireModernOwner && existing.incarnationId().isEmpty()) {
                StreamConfigData migrated = existing.modernizeLegacyExternal(
                    ownerToken, partitionCount);
                return putWithResult(path, migrated,
                        Set.of(PutOption.IfVersionIdEquals(result.version().versionId())))
                    .handle((write, failure) ->
                        new CreateWrite(write, unwrapNullable(failure)))
                    .thenCompose(write -> resolveLegacyExternalMigration(
                        id, partitionCount, properties, ownerToken,
                        requireModernOwner, migrated, write, retryCount));
            }
            if (existing.partitions() >= partitionCount) {
                return CompletableFuture.completedFuture(ExternalRegistration.active(
                    existing.incarnationId(), existing.ownerToken(), existing.creationKind(),
                    existing.ownerGeneration(), existing.metadataSourceOwnerToken(),
                    existing.metadataSourceGeneration(),
                    result.version().versionId()));
            }
            StreamConfigData grown = existing.withPartitions(partitionCount);
            return putWithResult(path, grown,
                    Set.of(PutOption.IfVersionIdEquals(result.version().versionId())))
                .handle((write, failure) ->
                    new CreateWrite(write, unwrapNullable(failure)))
                .thenCompose(write -> resolveExternalPartitionGrowth(
                    id, partitionCount, properties, ownerToken, requireModernOwner,
                    grown, write, retryCount));
        });
    }

    private CompletableFuture<ExternalRegistration> resolveLegacyExternalMigration(
            StreamIdentifier id, int partitionCount, Map<String, String> properties,
            String attemptToken, boolean requireModernOwner,
            StreamConfigData migrated, CreateWrite write, int retryCount) {
        if (write.failure() == null) {
            return CompletableFuture.completedFuture(
                ExternalRegistration.provisioning(
                    claim(migrated, write.result().version().versionId())));
        }
        if (write.failure() instanceof KeyAlreadyExistsException
                || write.failure() instanceof UnexpectedVersionIdException) {
            return retryAfterConflict(
                id, "legacy external owner migration", retryCount, write.failure(),
                () -> beginExternalPartitionRegistrationAttempt(
                    id, partitionCount, properties, attemptToken,
                    requireModernOwner, retryCount + 1));
        }
        return CompletableFuture.failedFuture(write.failure());
    }

    private CompletableFuture<ExternalRegistration> resolveExternalPartitionGrowth(
            StreamIdentifier id, int partitionCount, Map<String, String> properties,
            String attemptToken, boolean requireModernOwner,
            StreamConfigData grown, CreateWrite write,
            int retryCount) {
        if (write.failure() == null) {
            return CompletableFuture.completedFuture(ExternalRegistration.active(
                grown.incarnationId(), grown.ownerToken(), grown.creationKind(),
                grown.ownerGeneration(), grown.metadataSourceOwnerToken(),
                grown.metadataSourceGeneration(),
                write.result().version().versionId()));
        }
        if (write.failure() instanceof KeyAlreadyExistsException
                || write.failure() instanceof UnexpectedVersionIdException) {
            return retryAfterConflict(
                id, "external partition registration", retryCount, write.failure(),
                () -> beginExternalPartitionRegistrationAttempt(
                    id, partitionCount, properties, attemptToken,
                    requireModernOwner, retryCount + 1));
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
                        new StreamPermanentlyDeletedException(id, write.failure()));
                }
                if (isProvisioning(check.config().value())) {
                    return retryAfterConflict(
                        id, "external partition registration", retryCount, write.failure(),
                        () -> beginExternalPartitionRegistrationAttempt(
                            id, partitionCount, properties, attemptToken,
                            requireModernOwner, retryCount + 1));
                }
                StreamConfigData current = parse(id, check.config().value());
                if (current.isActiveExternalOrLegacy()
                        && current.partitions() >= partitionCount) {
                    logResolvedWrite(id, "external partition registration",
                        check.config().version().versionId(), write.failure());
                    return CompletableFuture.completedFuture(ExternalRegistration.active(
                        current.incarnationId(), current.ownerToken(), current.creationKind(),
                        current.ownerGeneration(), current.metadataSourceOwnerToken(),
                        current.metadataSourceGeneration(),
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
                boolean claimStillProvisioning =
                    config.provisioningState() == ProvisioningState.PROVISIONING
                        && current.version().versionId() == claim.versionId();
                boolean compatibleCallerFinalizedClaim =
                    config.provisioningState() == ProvisioningState.ACTIVE;
                if (config.isOwnedBy(claim)
                        && (claimStillProvisioning || compatibleCallerFinalizedClaim)) {
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
        return unregisterExternalStreamAttempt(id, 0);
    }

    private CompletableFuture<Void> unregisterExternalStreamAttempt(
            StreamIdentifier id, int retryCount) {
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
            if (config.provisioningState() == ProvisioningState.PROVISIONING) {
                return CompletableFuture.failedFuture(new AlreadyExistsException(
                    "External stream registration is still provisioning: " + id.fullName()));
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
                .thenCompose(write -> resolveUnregisterWrite(
                    id, unregistered, write, retryCount));
        });
    }

    private CompletableFuture<Void> resolveUnregisterWrite(
            StreamIdentifier id, StreamConfigData desired, CreateWrite write,
            int retryCount) {
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
                    logResolvedWrite(id, "external stream unregister",
                        check.config().version().versionId(), write.failure());
                    return CompletableFuture.completedFuture(null);
                }
                if (write.failure() instanceof UnexpectedVersionIdException) {
                    return retryAfterConflict(
                        id, "external stream unregister", retryCount, write.failure(),
                        () -> unregisterExternalStreamAttempt(id, retryCount + 1));
                }
                return CompletableFuture.failedFuture(write.failure());
            });
    }

    CompletableFuture<Void> permanentlyDeleteExternalStream(StreamIdentifier id) {
        Objects.requireNonNull(id, "id");
        return permanentlyDeleteExternalStreamAttempt(id, 0);
    }

    CompletableFuture<Void> permanentlyDeleteAbsentExternalStream(StreamIdentifier id) {
        Objects.requireNonNull(id, "id");
        String path = catalogPaths.streamConfigPath(id);
        StreamConfigData deleted = StreamConfigData.emptyPermanentDeletion();
        return putWithResult(path, deleted, Set.of(PutOption.IfRecordDoesNotExist))
            .handle((write, failure) ->
                new CreateWrite(write, unwrapNullable(failure)))
            .thenCompose(write -> {
                if (write.failure() == null) {
                    return CompletableFuture.completedFuture(null);
                }
                if (!(write.failure() instanceof KeyAlreadyExistsException)) {
                    return CompletableFuture.failedFuture(write.failure());
                }
                return oxiaClient.get(path).thenCompose(current -> {
                    if (current != null
                            && isPermanentDeletionTombstone(current.value())) {
                        return CompletableFuture.completedFuture(null);
                    }
                    return CompletableFuture.failedFuture(new AlreadyExistsException(
                        "Stream lifecycle appeared during permanent deletion: "
                            + id.fullName()));
                });
            });
    }

    private CompletableFuture<Void> permanentlyDeleteExternalStreamAttempt(
            StreamIdentifier id, int retryCount) {
        String path = catalogPaths.streamConfigPath(id);
        return oxiaClient.get(path).thenCompose(current -> {
            StreamConfigData deleted;
            if (current != null) {
                StreamConfigData config = parse(id, current.value());
                if (config.creationKind().orElse(null) == CreationKind.NATIVE_CREATE) {
                    return CompletableFuture.failedFuture(new AlreadyExistsException(
                        "Stream is owned by a native catalog creation: " + id.fullName()));
                }
                // Permanent deletion is the recovery escape hatch for a crashed external
                // provisioner. The version-guarded tombstone fences an in-flight registration
                // before it can finalize while retaining its durable owner identity so catalog
                // cleanup can fence an allocation it may already have published.
                if (config.provisioningState()
                        == ProvisioningState.PERMANENTLY_DELETED) {
                    return CompletableFuture.completedFuture(null);
                }
                deleted = config.permanentlyDelete();
            } else {
                deleted = StreamConfigData.emptyPermanentDeletion();
            }
            Set<PutOption> options = current == null
                ? Set.of(PutOption.IfRecordDoesNotExist)
                : Set.of(PutOption.IfVersionIdEquals(current.version().versionId()));
            return putWithResult(path, deleted, options)
                .handle((write, failure) ->
                    new CreateWrite(write, unwrapNullable(failure)))
                .thenCompose(write -> resolvePermanentDeletionWrite(
                    id, deleted, write, retryCount));
        });
    }

    private CompletableFuture<Void> resolvePermanentDeletionWrite(
            StreamIdentifier id, StreamConfigData desired, CreateWrite write,
            int retryCount) {
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
                    logResolvedWrite(id, "external stream permanent deletion",
                        check.config().version().versionId(), write.failure());
                    return CompletableFuture.completedFuture(null);
                }
                if (write.failure() instanceof KeyAlreadyExistsException
                        || write.failure() instanceof UnexpectedVersionIdException) {
                    return retryAfterConflict(
                        id, "external stream permanent deletion", retryCount, write.failure(),
                        () -> permanentlyDeleteExternalStreamAttempt(id, retryCount + 1));
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
                    && config.provisioningState() != ProvisioningState.DROPPED
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
                    || !current.metadataSourceOwnerToken().equals(
                        expected.config().metadataSourceOwnerToken())
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
                return CompletableFuture.completedFuture(null);
            }
            if (isPermanentDeletionTombstone(result.value())) {
                return CompletableFuture.failedFuture(
                    new StreamPermanentlyDeletedException(id));
            }
            return CompletableFuture.failedFuture(
                new AlreadyExistsException("Stream already exists: " + id.fullName()));
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
            Optional.empty(), 0);
    }

    private CompletableFuture<ProvisioningClaim> claimCreationAttempt(
            StreamIdentifier id, int partitions, Map<String, String> properties,
            Optional<TableMaterializationPolicy> materialization,
            CreationKind kind, String ownerToken,
            Optional<String> requiredIncarnation, int retryCount) {
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
                return writeInitialClaim(id, desired, retryCount);
            }
            if (isPermanentDeletionTombstone(result.value())) {
                return CompletableFuture.failedFuture(
                    new StreamPermanentlyDeletedException(id));
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
                    id, recreated, result.version().versionId(), retryCount);
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
                return writeTakeoverClaim(
                    id, resumed, result.version().versionId(), retryCount);
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
            String durableOwnerToken = current.ownerToken().orElseThrow();
            return CompletableFuture.completedFuture(new ProvisioningClaim(
                current, current.incarnationId().orElseThrow(), durableOwnerToken, kind,
                current.ownerGeneration(), result.version().versionId()));
        });
    }

    private CompletableFuture<ProvisioningClaim> writeInitialClaim(
            StreamIdentifier id, StreamConfigData desired, int retryCount) {
        String path = catalogPaths.streamConfigPath(id);
        return putWithResult(path, desired, Set.of(PutOption.IfRecordDoesNotExist))
            .handle((result, failure) ->
                new CreateWrite(result, unwrapNullable(failure)))
            .thenCompose(write -> resolveClaimWrite(
                id, desired, -1L, write, true, retryCount));
    }

    private CompletableFuture<ProvisioningClaim> writeTakeoverClaim(
            StreamIdentifier id, StreamConfigData desired, long expectedVersion,
            int retryCount) {
        String path = catalogPaths.streamConfigPath(id);
        return putWithResult(path, desired,
                Set.of(PutOption.IfVersionIdEquals(expectedVersion)))
            .handle((result, failure) ->
                new CreateWrite(result, unwrapNullable(failure)))
            .thenCompose(write -> resolveClaimWrite(
                id, desired, expectedVersion, write, false, retryCount));
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
                if (check.config() != null
                        && !isPermanentDeletionTombstone(check.config().value())) {
                    StreamConfigData current = parse(id, check.config().value());
                    if (current.isOwnedBy(desired)) {
                        logResolvedWrite(id, "stream creation claim",
                            check.config().version().versionId(), write.failure());
                        return CompletableFuture.completedFuture(claim(
                            current, check.config().version().versionId()));
                    }
                }
                if (check.config() != null
                        && isPermanentDeletionTombstone(check.config().value())) {
                    return CompletableFuture.failedFuture(
                        new StreamPermanentlyDeletedException(id, write.failure()));
                }
                if (write.failure() instanceof KeyAlreadyExistsException
                        || write.failure() instanceof UnexpectedVersionIdException) {
                    return retryAfterConflict(
                        id, "stream creation claim", retryCount, write.failure(),
                        () -> claimCreationAttempt(
                            id, desired.partitions(), desired.properties(),
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

    CompletableFuture<LifecycleContext> readLifecycleContext(StreamIdentifier id) {
        String path = catalogPaths.streamConfigPath(id);
        return oxiaClient.get(path).thenApply(current -> current == null
            ? new LifecycleContext(Optional.empty(), -1L)
            : new LifecycleContext(
                Optional.of(parse(id, current.value())), current.version().versionId()));
    }

    CompletableFuture<Void> verifyNativeCleanupContext(
            StreamIdentifier id, NativeCleanupContext expected) {
        String path = catalogPaths.streamConfigPath(id);
        return oxiaClient.get(path).thenCompose(current -> {
            if (expected.config().isEmpty()) {
                return current == null
                    ? CompletableFuture.completedFuture(null)
                    : CompletableFuture.failedFuture(
                        new AbortingOwnershipLostException(id));
            }
            if (current == null
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
            if (current == null || isLifecycleTombstone(current.value())) {
                return CompletableFuture.completedFuture(Optional.empty());
            }
            StreamConfigData config = parse(id, current.value());
            if (config.provisioningState() == ProvisioningState.ABORTING
                    && config.ownerToken().equals(Optional.of(ownerToken))) {
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
                            logResolvedWrite(id, "stream drop completion",
                                check.config().version().versionId(), write.failure());
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

    CompletableFuture<Optional<CompletedDrop>> readCompletedPurgingDrop(
            StreamIdentifier id) {
        return readCompletedDrop(id).thenApply(completed -> completed.filter(
            value -> value.config().purgeRequested()));
    }

    CompletableFuture<Optional<CompletedDrop>> readCompletedDrop(
            StreamIdentifier id) {
        String path = catalogPaths.streamConfigPath(id);
        return oxiaClient.get(path).thenApply(result -> {
            if (result == null || isPermanentDeletionTombstone(result.value())) {
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
        String path = catalogPaths.streamConfigPath(id);
        return oxiaClient.get(path).thenCompose(result -> {
            if (result == null || isPermanentDeletionTombstone(result.value())) {
                return CompletableFuture.failedFuture(
                    new AbortingOwnershipLostException(id));
            }
            StreamConfigData current = parse(id, result.value());
            if (result.version().versionId() == expected.versionId()
                    && current.sameCompletedDrop(expected.config())) {
                return CompletableFuture.completedFuture(null);
            }
            return CompletableFuture.failedFuture(
                new AbortingOwnershipLostException(id));
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
        return updateAttempt(id, mutation, 0);
    }

    private CompletableFuture<Void> updateAttempt(
            StreamIdentifier id, UnaryOperator<StreamConfigData> mutation,
            int retryCount) {
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
        config.creationKind().ifPresent(value -> node.put(CREATION_KIND_FIELD, value.name()));
        if (config.provisioning()) {
            node.put(PROVISIONING_FIELD, true);
            node.put(PROVISIONING_STATE_FIELD, config.provisioningState().name());
        }
        if (config.provisioningState() == ProvisioningState.PERMANENTLY_DELETED) {
            node.put(PERMANENT_DELETION_FIELD, true);
        }
        if (config.purgeRequested()) {
            node.put(PURGE_REQUESTED_FIELD, true);
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
            if (node == null || !node.isObject()
                    || !node.path(PROVISIONING_FIELD).asBoolean(false)) {
                return false;
            }
            String state = node.path(PROVISIONING_STATE_FIELD).asText();
            return ProvisioningState.DROPPED.name().equals(state)
                || ProvisioningState.UNREGISTERED.name().equals(state);
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
            Optional<CreationKind> creationKind = node.hasNonNull(CREATION_KIND_FIELD)
                ? Optional.of(CreationKind.valueOf(node.get(CREATION_KIND_FIELD).asText()))
                : Optional.empty();
            boolean permanentDeletion = node.path(PERMANENT_DELETION_FIELD).asBoolean(false);
            boolean provisioning = node.path(PROVISIONING_FIELD).asBoolean(false);
            boolean purgeRequested = node.path(PURGE_REQUESTED_FIELD).asBoolean(false);
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
                metadataSourceOwnerToken, metadataSourceGeneration,
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
                || this == DROPPED
                || this == PERMANENTLY_DELETED;
        }
    }

    record StreamConfigData(
            int partitions, Map<String, String> properties,
            Optional<TableMaterializationPolicy> materialization,
            Optional<String> incarnationId, Optional<String> ownerToken,
            Optional<CreationKind> creationKind,
            long ownerGeneration, Optional<String> metadataSourceOwnerToken,
            long metadataSourceGeneration,
            ProvisioningState provisioningState, boolean purgeRequested) {

        StreamConfigData {
            properties = Map.copyOf(properties);
            Objects.requireNonNull(materialization, "materialization");
            Objects.requireNonNull(incarnationId, "incarnationId");
            Objects.requireNonNull(ownerToken, "ownerToken");
            Objects.requireNonNull(creationKind, "creationKind");
            Objects.requireNonNull(metadataSourceOwnerToken, "metadataSourceOwnerToken");
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
            if (metadataSourceOwnerToken.isPresent()
                    && metadataSourceGeneration < 0) {
                throw new IllegalArgumentException(
                    "Modern metadata source requires owner token and generation");
            }
        }

        StreamConfigData(
                int partitions, Map<String, String> properties,
                Optional<TableMaterializationPolicy> materialization) {
            this(partitions, properties, materialization, Optional.empty(),
                Optional.empty(), Optional.empty(), LEGACY_METADATA_GENERATION,
                Optional.empty(), NO_METADATA_GENERATION,
                ProvisioningState.ACTIVE, false);
        }

        static StreamConfigData provisioning(
                int partitions, Map<String, String> properties,
                Optional<TableMaterializationPolicy> materialization,
                CreationKind creationKind, String incarnationId, String ownerToken) {
            return new StreamConfigData(
                partitions, properties, materialization, Optional.of(incarnationId),
                Optional.of(ownerToken), Optional.of(creationKind), 1L,
                Optional.empty(), NO_METADATA_GENERATION,
                ProvisioningState.PROVISIONING, false);
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
                Optional.empty(), NO_METADATA_GENERATION,
                ProvisioningState.PROVISIONING, false);
        }

        StreamConfigData modernizeLegacyExternal(
                String newOwnerToken, int requestedPartitions) {
            if (provisioningState != ProvisioningState.ACTIVE
                    || incarnationId.isPresent() || ownerToken.isPresent()
                    || ownerGeneration != LEGACY_METADATA_GENERATION) {
                throw new IllegalStateException(
                    "Only an active legacy external stream can migrate its owner");
            }
            return new StreamConfigData(
                Math.max(partitions, requestedPartitions), properties, materialization,
                Optional.of(UUID.randomUUID().toString()), Optional.of(newOwnerToken),
                Optional.of(CreationKind.EXTERNAL), 0L, Optional.empty(),
                LEGACY_METADATA_GENERATION, ProvisioningState.PROVISIONING, false);
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

        boolean compatibleWith(
                int requestedPartitions, Map<String, String> requestedProperties,
                Optional<TableMaterializationPolicy> requestedMaterialization,
                CreationKind requestedKind) {
            if (creationKind.orElse(null) != requestedKind) {
                return false;
            }
            if (requestedKind == CreationKind.EXTERNAL
                    && metadataSourceGeneration != NO_METADATA_GENERATION) {
                return true;
            }
            if (!properties.equals(requestedProperties)
                    || !materialization.equals(requestedMaterialization)) {
                return false;
            }
            return requestedKind == CreationKind.EXTERNAL
                || partitions == requestedPartitions;
        }

        StreamConfigData resumeExternalRegistration(
                String newOwnerToken, int requestedPartitions) {
            if (incarnationId.isEmpty()) {
                return new StreamConfigData(
                    Math.max(partitions, requestedPartitions), properties, materialization,
                    Optional.of(UUID.randomUUID().toString()), Optional.of(newOwnerToken),
                    Optional.of(CreationKind.EXTERNAL), 1L,
                    Optional.empty(), LEGACY_METADATA_GENERATION,
                    ProvisioningState.PROVISIONING, false);
            }
            return new StreamConfigData(
                Math.max(partitions, requestedPartitions), properties, materialization,
                incarnationId, Optional.of(newOwnerToken), creationKind,
                ownerGeneration + 1, ownerToken, ownerGeneration,
                ProvisioningState.PROVISIONING, false);
        }

        StreamConfigData activate() {
            return new StreamConfigData(
                partitions, properties, materialization, incarnationId, ownerToken,
                creationKind, ownerGeneration, metadataSourceOwnerToken,
                metadataSourceGeneration,
                ProvisioningState.ACTIVE, purgeRequested);
        }

        StreamConfigData abort(String newOwnerToken) {
            return abort(newOwnerToken, false);
        }

        StreamConfigData abort(String newOwnerToken, boolean purge) {
            if (incarnationId.isEmpty()) {
                return new StreamConfigData(
                    partitions, properties, materialization,
                    Optional.of(UUID.randomUUID().toString()), Optional.of(newOwnerToken),
                    creationKind, 1L, Optional.empty(), LEGACY_METADATA_GENERATION,
                    ProvisioningState.ABORTING, purgeRequested || purge);
            }
            Optional<String> cleanupOwnerToken =
                provisioningState == ProvisioningState.ABORTING
                    ? metadataSourceOwnerToken : ownerToken;
            long cleanupGeneration = provisioningState == ProvisioningState.ABORTING
                ? metadataSourceGeneration : ownerGeneration;
            return new StreamConfigData(
                partitions, properties, materialization, incarnationId,
                Optional.of(newOwnerToken), creationKind, ownerGeneration + 1,
                cleanupOwnerToken, cleanupGeneration, ProvisioningState.ABORTING,
                purgeRequested || purge);
        }

        StreamConfigData unregister() {
            return new StreamConfigData(
                partitions, properties, materialization, incarnationId, ownerToken,
                creationKind, ownerGeneration, ownerToken, ownerGeneration,
                ProvisioningState.UNREGISTERED, purgeRequested);
        }

        StreamConfigData permanentlyDelete() {
            boolean preserveMetadataSource = provisioningState.retainsExternalDeletionSpec();
            Optional<String> cleanupOwnerToken = preserveMetadataSource
                ? metadataSourceOwnerToken : ownerToken;
            long cleanupGeneration = preserveMetadataSource
                ? metadataSourceGeneration : ownerGeneration;
            return new StreamConfigData(
                partitions, properties, materialization, incarnationId,
                ownerToken, Optional.of(CreationKind.EXTERNAL),
                ownerGeneration, cleanupOwnerToken, cleanupGeneration,
                ProvisioningState.PERMANENTLY_DELETED, true);
        }

        StreamConfigData completeDrop() {
            if (provisioningState != ProvisioningState.ABORTING) {
                throw new IllegalStateException("Only an aborting stream can complete deletion");
            }
            return new StreamConfigData(
                partitions, properties, materialization, incarnationId, ownerToken,
                creationKind, ownerGeneration, metadataSourceOwnerToken,
                metadataSourceGeneration,
                ProvisioningState.DROPPED, purgeRequested);
        }

        static StreamConfigData emptyPermanentDeletion() {
            return new StreamConfigData(
                0, Map.of(), Optional.empty(), Optional.empty(), Optional.empty(),
                Optional.of(CreationKind.EXTERNAL), LEGACY_METADATA_GENERATION,
                Optional.empty(), NO_METADATA_GENERATION,
                ProvisioningState.PERMANENTLY_DELETED, true);
        }

        StreamConfigData withPartitions(int newPartitions) {
            return new StreamConfigData(
                newPartitions, properties, materialization, incarnationId, ownerToken,
                creationKind, ownerGeneration, metadataSourceOwnerToken,
                metadataSourceGeneration, provisioningState,
                purgeRequested);
        }

        StreamConfigData withProperties(Map<String, String> newProperties) {
            return new StreamConfigData(
                partitions, newProperties, materialization, incarnationId, ownerToken,
                creationKind, ownerGeneration, metadataSourceOwnerToken,
                metadataSourceGeneration, provisioningState,
                purgeRequested);
        }

        StreamConfigData withMaterialization(
                Optional<TableMaterializationPolicy> newMaterialization) {
            return new StreamConfigData(
                partitions, properties, newMaterialization, incarnationId, ownerToken,
                creationKind, ownerGeneration, metadataSourceOwnerToken,
                metadataSourceGeneration, provisioningState,
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

    record ExternalDeletionContext(StreamConfigData config, long versionId) {

        boolean permanent() {
            return config.provisioningState() == ProvisioningState.PERMANENTLY_DELETED;
        }

        long metadataGeneration() {
            return config.provisioningState() == ProvisioningState.ACTIVE
                ? config.ownerGeneration() : config.metadataSourceGeneration();
        }

        Optional<String> metadataOwnerToken() {
            return config.provisioningState() == ProvisioningState.ACTIVE
                ? config.ownerToken() : config.metadataSourceOwnerToken();
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
                    && next.metadataSourceOwnerToken().isEmpty()
                    && next.metadataSourceGeneration() == LEGACY_METADATA_GENERATION;
            }
            if (!next.incarnationId().equals(previous.incarnationId())) {
                return false;
            }
            if (next.provisioningState() == ProvisioningState.ACTIVE) {
                return previous.provisioningState() == ProvisioningState.ACTIVE
                    && next.ownerGeneration() == previous.ownerGeneration()
                    && next.ownerToken().equals(previous.ownerToken())
                    && next.metadataSourceOwnerToken().equals(
                        previous.metadataSourceOwnerToken())
                    && next.metadataSourceGeneration()
                        == previous.metadataSourceGeneration()
                    && next.creationKind().equals(previous.creationKind());
            }
            return next.provisioningState().retainsExternalDeletionSpec()
                && next.ownerGeneration() >= previous.ownerGeneration()
                && next.metadataSourceOwnerToken().equals(metadataOwnerToken())
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
            Optional<String> activeMetadataSourceOwnerToken,
            long activeMetadataSourceGeneration, long activeVersionId) {

        static ExternalRegistration provisioning(ProvisioningClaim claim) {
            return new ExternalRegistration(
                Optional.of(claim), Optional.empty(), Optional.empty(), Optional.empty(),
                LEGACY_METADATA_GENERATION, Optional.empty(),
                NO_METADATA_GENERATION, -1L);
        }

        static ExternalRegistration active(
                Optional<String> activeIncarnation, Optional<String> activeOwnerToken,
                Optional<CreationKind> activeKind, long activeOwnerGeneration,
                Optional<String> activeMetadataSourceOwnerToken,
                long activeMetadataSourceGeneration, long activeVersionId) {
            return new ExternalRegistration(
                Optional.empty(), activeIncarnation, activeOwnerToken,
                activeKind, activeOwnerGeneration,
                activeMetadataSourceOwnerToken,
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

        Optional<String> metadataSourceOwnerToken() {
            return claim.map(value -> value.config().metadataSourceOwnerToken())
                .orElse(activeMetadataSourceOwnerToken);
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

    static final class ExternalRegistrationLifecycleConflictException
            extends AlreadyExistsException {

        private ExternalRegistrationLifecycleConflictException(String message) {
            super(message);
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
