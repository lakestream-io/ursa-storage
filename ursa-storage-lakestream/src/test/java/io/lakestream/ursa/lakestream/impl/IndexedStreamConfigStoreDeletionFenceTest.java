/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.lakestream.impl;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.lakestream.api.StreamIdentifier;
import io.lakestream.api.exception.AlreadyExistsException;
import io.lakestream.api.exception.NoSuchStreamException;
import io.oxia.client.api.AsyncOxiaClient;
import io.oxia.client.api.GetResult;
import io.oxia.client.api.PutResult;
import io.oxia.client.api.Version;
import io.oxia.client.api.exceptions.KeyAlreadyExistsException;
import io.oxia.client.api.exceptions.UnexpectedVersionIdException;
import io.oxia.client.api.options.DeleteOption;
import io.oxia.client.api.options.PutOption;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class IndexedStreamConfigStoreDeletionFenceTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final Version VERSION_1 = version(1);
    private static final Version VERSION_2 = version(2);
    private static final byte[] CONFIG =
        "{\"partitions\":1,\"properties\":{}}".getBytes(StandardCharsets.UTF_8);
    private static final byte[] TOMBSTONE =
        "{\"_externalStreamPermanentlyDeleted\":true}"
            .getBytes(StandardCharsets.UTF_8);

    @Mock
    private AsyncOxiaClient oxiaClient;

    private StreamIdentifier id;
    private IndexedStreamConfigStore store;
    private DefaultCatalogPaths paths;
    private String configPath;

    @BeforeEach
    void setUp() {
        paths = new DefaultCatalogPaths();
        id = new StreamIdentifier("public/default", "orders-topic-id");
        store = new IndexedStreamConfigStore(oxiaClient, paths);
        configPath = paths.streamConfigPath(id);
    }

    @Test
    void permanentDeleteAtomicallyFencesLateCreate() {
        Set<PutOption> createOnly = Set.of(PutOption.IfRecordDoesNotExist);
        CompletableFuture<PutResult> lateRegistration = new CompletableFuture<>();
        when(oxiaClient.get(configPath))
            .thenReturn(CompletableFuture.completedFuture(null))
            .thenReturn(CompletableFuture.completedFuture(null))
            .thenReturn(CompletableFuture.completedFuture(null))
            .thenReturn(CompletableFuture.completedFuture(tombstone(VERSION_1)));
        when(oxiaClient.put(eq(configPath), any(byte[].class), eq(createOnly)))
            .thenReturn(lateRegistration)
            .thenReturn(CompletableFuture.completedFuture(
                new PutResult(configPath, VERSION_1)));

        CompletableFuture<Void> registration =
            store.registerExternalStream(id, 1, Map.of("owner", "kafka"));
        store.permanentlyDeleteExternalStream(id).join();

        lateRegistration.completeExceptionally(new KeyAlreadyExistsException(configPath));
        assertThatThrownBy(registration::join)
            .isInstanceOf(CompletionException.class)
            .hasCauseInstanceOf(NoSuchStreamException.class);

        ArgumentCaptor<byte[]> writes = ArgumentCaptor.forClass(byte[].class);
        verify(oxiaClient, times(2)).put(eq(configPath), writes.capture(), eq(createOnly));
        assertThat(json(writes.getAllValues().get(1))
            .path("_externalStreamPermanentlyDeleted").asBoolean()).isTrue();
        verify(oxiaClient, never()).delete(eq(configPath), any());
    }

    @Test
    void permanentDeleteAtomicallyFencesLateCasUpdate() {
        Set<PutOption> expectedVersion =
            Set.of(PutOption.IfVersionIdEquals(VERSION_1.versionId()));
        CompletableFuture<PutResult> lateRegistration = new CompletableFuture<>();
        when(oxiaClient.get(configPath))
            .thenReturn(CompletableFuture.completedFuture(config(VERSION_1)))
            .thenReturn(CompletableFuture.completedFuture(config(VERSION_1)))
            .thenReturn(CompletableFuture.completedFuture(tombstone(VERSION_2)));
        when(oxiaClient.put(eq(configPath), any(byte[].class), eq(expectedVersion)))
            .thenReturn(lateRegistration)
            .thenReturn(CompletableFuture.completedFuture(
                new PutResult(configPath, VERSION_2)));

        CompletableFuture<Void> registration =
            store.registerExternalStream(id, 2, Map.of());
        store.permanentlyDeleteExternalStream(id).join();

        lateRegistration.completeExceptionally(
            new UnexpectedVersionIdException(configPath, VERSION_1.versionId()));
        assertThatThrownBy(registration::join)
            .isInstanceOf(CompletionException.class)
            .hasCauseInstanceOf(NoSuchStreamException.class);

        ArgumentCaptor<byte[]> writes = ArgumentCaptor.forClass(byte[].class);
        verify(oxiaClient, times(2)).put(
            eq(configPath), writes.capture(), eq(expectedVersion));
        assertThat(json(writes.getAllValues().get(1))
            .path("_externalStreamPermanentlyDeleted").asBoolean()).isTrue();
    }

    @Test
    void ambiguousPermanentDeleteIsSuccessfulOnlyWhenTombstoneIsObserved() {
        RuntimeException ambiguous = new RuntimeException("request outcome unknown");
        when(oxiaClient.get(configPath))
            .thenReturn(CompletableFuture.completedFuture(config(VERSION_1)))
            .thenReturn(CompletableFuture.completedFuture(tombstone(VERSION_2)));
        when(oxiaClient.put(eq(configPath), any(byte[].class), any()))
            .thenReturn(CompletableFuture.failedFuture(ambiguous));

        store.permanentlyDeleteExternalStream(id).join();

        verify(oxiaClient, times(2)).get(configPath);
    }

    @Test
    void ambiguousPermanentDeleteFailsWhenTombstoneWasNotWritten() {
        RuntimeException ambiguous = new RuntimeException("request outcome unknown");
        when(oxiaClient.get(configPath))
            .thenReturn(CompletableFuture.completedFuture(config(VERSION_1)))
            .thenReturn(CompletableFuture.completedFuture(config(VERSION_1)));
        when(oxiaClient.put(eq(configPath), any(byte[].class), any()))
            .thenReturn(CompletableFuture.failedFuture(ambiguous));

        assertThatThrownBy(() -> store.permanentlyDeleteExternalStream(id).join())
            .isInstanceOf(CompletionException.class)
            .hasRootCause(ambiguous);
    }

    @Test
    void permanentDeleteFencesGenericCreateWrite() {
        Set<PutOption> createOnly = Set.of(PutOption.IfRecordDoesNotExist);
        CompletableFuture<PutResult> lateCreate = new CompletableFuture<>();
        when(oxiaClient.get(configPath))
            .thenReturn(CompletableFuture.completedFuture(null))
            .thenReturn(CompletableFuture.completedFuture(tombstone(VERSION_1)));
        when(oxiaClient.put(eq(configPath), any(byte[].class), eq(createOnly)))
            .thenReturn(lateCreate)
            .thenReturn(CompletableFuture.completedFuture(
                new PutResult(configPath, VERSION_1)));

        CompletableFuture<IndexedStreamConfigStore.ProvisioningClaim> create =
            store.claimCreation(id, 1, Map.of(), Optional.empty(), "attempt");
        store.permanentlyDeleteExternalStream(id).join();

        lateCreate.completeExceptionally(new KeyAlreadyExistsException(configPath));
        assertThatThrownBy(create::join)
            .isInstanceOf(CompletionException.class)
            .hasCauseInstanceOf(AlreadyExistsException.class);
    }

    @Test
    void ambiguousGenericCreateFailsWhenConfigWasNotWritten() {
        RuntimeException ambiguous = new RuntimeException("request outcome unknown");
        Set<PutOption> createOnly = Set.of(PutOption.IfRecordDoesNotExist);
        when(oxiaClient.put(eq(configPath), any(byte[].class), eq(createOnly)))
            .thenReturn(CompletableFuture.failedFuture(ambiguous));
        when(oxiaClient.get(configPath))
            .thenReturn(CompletableFuture.completedFuture(null));

        assertThatThrownBy(() ->
                store.claimCreation(id, 1, Map.of(), Optional.empty(), "attempt").join())
            .isInstanceOf(CompletionException.class)
            .hasRootCause(ambiguous);

        verify(oxiaClient, times(2)).get(configPath);
    }

    @Test
    void initialCreationClaimStartsAtOwnerGenerationOne() {
        Set<PutOption> createOnly = Set.of(PutOption.IfRecordDoesNotExist);
        when(oxiaClient.get(configPath)).thenReturn(CompletableFuture.completedFuture(null));
        when(oxiaClient.put(eq(configPath), any(byte[].class), eq(createOnly)))
            .thenReturn(CompletableFuture.completedFuture(
                new PutResult(configPath, VERSION_1)));

        IndexedStreamConfigStore.ProvisioningClaim claim = store.claimCreation(
            id, 1, Map.of(), Optional.empty(), "attempt-1").join();

        assertThat(claim.ownerGeneration()).isEqualTo(1L);
        ArgumentCaptor<byte[]> write = ArgumentCaptor.forClass(byte[].class);
        verify(oxiaClient).put(eq(configPath), write.capture(), eq(createOnly));
        assertThat(json(write.getValue()).path("_ownerGeneration").asLong()).isEqualTo(1L);
    }

    @Test
    void staleAbsentCreateCannotRebaseAcrossDroppedTombstoneButFreshCallCanRecreate() {
        Set<PutOption> createOnly = Set.of(PutOption.IfRecordDoesNotExist);
        Set<PutOption> replaceDropped =
            Set.of(PutOption.IfVersionIdEquals(VERSION_2.versionId()));
        byte[] dropped = streamConfigBytes(
            1, Map.of(), "completed-incarnation", "drop-owner", 2L, 1L,
            IndexedStreamConfigStore.CreationKind.NATIVE_CREATE,
            IndexedStreamConfigStore.ProvisioningState.DROPPED);
        GetResult droppedResult = new GetResult(configPath, dropped, VERSION_2);
        CompletableFuture<PutResult> staleCreateWrite = new CompletableFuture<>();
        when(oxiaClient.get(configPath))
            .thenReturn(CompletableFuture.completedFuture(null))
            .thenReturn(CompletableFuture.completedFuture(droppedResult))
            .thenReturn(CompletableFuture.completedFuture(droppedResult));
        when(oxiaClient.put(eq(configPath), any(byte[].class), eq(createOnly)))
            .thenReturn(staleCreateWrite);
        when(oxiaClient.put(eq(configPath), any(byte[].class), eq(replaceDropped)))
            .thenReturn(CompletableFuture.completedFuture(
                new PutResult(configPath, version(3))));

        CompletableFuture<IndexedStreamConfigStore.ProvisioningClaim> stale =
            store.claimCreation(id, 1, Map.of(), Optional.empty(), "stale-owner");
        staleCreateWrite.completeExceptionally(new KeyAlreadyExistsException(configPath));

        assertThatThrownBy(stale::join)
            .isInstanceOf(CompletionException.class)
            .hasCauseInstanceOf(AlreadyExistsException.class);

        IndexedStreamConfigStore.ProvisioningClaim fresh = store.claimCreation(
            id, 1, Map.of(), Optional.empty(), "fresh-owner").join();
        assertThat(fresh.ownerGeneration()).isEqualTo(3L);
        assertThat(fresh.incarnationId()).isNotEqualTo("completed-incarnation");
        verify(oxiaClient).put(eq(configPath), any(byte[].class), eq(createOnly));
        verify(oxiaClient).put(eq(configPath), any(byte[].class), eq(replaceDropped));
    }

    @Test
    void compatibleGenericCreateTakesOverMatchingProvisioningClaim() {
        when(oxiaClient.get(configPath)).thenReturn(CompletableFuture.completedFuture(
            provisioning(VERSION_1, "attempt-1")));
        when(oxiaClient.put(eq(configPath), any(byte[].class),
                eq(Set.of(PutOption.IfVersionIdEquals(VERSION_1.versionId())))))
            .thenReturn(CompletableFuture.completedFuture(
                new PutResult(configPath, VERSION_2)));

        IndexedStreamConfigStore.ProvisioningClaim claim = store.claimCreation(
            id, 1, Map.of(), Optional.empty(), "attempt-2").join();

        assertThat(claim.incarnationId()).isEqualTo("incarnation");
        assertThat(claim.ownerToken()).isEqualTo("attempt-2");
        assertThat(claim.ownerGeneration()).isEqualTo(2L);
        assertThat(claim.versionId()).isEqualTo(VERSION_2.versionId());
    }

    @Test
    void eachCompatibleTakeoverIncrementsOwnerGenerationExactlyOnce() {
        AtomicReference<VersionedValue> state = mockVersionedRecord(
            provisioningBytes("attempt-1", 1L,
                IndexedStreamConfigStore.CreationKind.NATIVE_CREATE));

        IndexedStreamConfigStore.ProvisioningClaim second = store.claimCreation(
            id, 1, Map.of(), Optional.empty(), "attempt-2").join();
        IndexedStreamConfigStore.ProvisioningClaim third = store.claimCreation(
            id, 1, Map.of(), Optional.empty(), "attempt-3").join();

        assertThat(second.ownerGeneration()).isEqualTo(2L);
        assertThat(third.ownerGeneration()).isEqualTo(3L);
        assertThat(third.incarnationId()).isEqualTo(second.incarnationId());
        assertThat(json(state.get().value()).path("_ownerGeneration").asLong())
            .isEqualTo(3L);
    }

    @Test
    void sameOwnerRetryDoesNotIncrementOwnerGeneration() {
        when(oxiaClient.get(configPath)).thenReturn(CompletableFuture.completedFuture(
            provisioning(VERSION_1, "attempt-1", 7L)));

        IndexedStreamConfigStore.ProvisioningClaim retry = store.claimCreation(
            id, 1, Map.of(), Optional.empty(), "attempt-1").join();

        assertThat(retry.ownerGeneration()).isEqualTo(7L);
        verify(oxiaClient, never()).put(eq(configPath), any(byte[].class), any());
    }

    @Test
    void ambiguousTakeoverReadbackDoesNotIncrementOwnerGenerationAgain() {
        RuntimeException ambiguous = new RuntimeException("request outcome unknown");
        when(oxiaClient.get(configPath))
            .thenReturn(CompletableFuture.completedFuture(
                provisioning(VERSION_1, "attempt-1", 4L)))
            .thenReturn(CompletableFuture.completedFuture(new GetResult(
                configPath,
                provisioningBytes("attempt-2", 5L,
                    IndexedStreamConfigStore.CreationKind.NATIVE_CREATE),
                VERSION_2)));
        when(oxiaClient.put(eq(configPath), any(byte[].class),
                eq(Set.of(PutOption.IfVersionIdEquals(VERSION_1.versionId())))))
            .thenReturn(CompletableFuture.failedFuture(ambiguous));

        IndexedStreamConfigStore.ProvisioningClaim recovered = store.claimCreation(
            id, 1, Map.of(), Optional.empty(), "attempt-2").join();

        assertThat(recovered.ownerGeneration()).isEqualTo(5L);
        assertThat(recovered.versionId()).isEqualTo(VERSION_2.versionId());
        verify(oxiaClient).put(eq(configPath), any(byte[].class),
            eq(Set.of(PutOption.IfVersionIdEquals(VERSION_1.versionId()))));
    }

    @Test
    void differentSpecAndCreationKindCannotTakeOverProvisioningClaim() {
        when(oxiaClient.get(configPath)).thenReturn(CompletableFuture.completedFuture(
            provisioning(VERSION_1, "attempt-1")));

        assertThatThrownBy(() -> store.claimCreation(
                id, 2, Map.of(), Optional.empty(), "attempt-2").join())
            .isInstanceOf(CompletionException.class)
            .hasCauseInstanceOf(AlreadyExistsException.class);
        assertThatThrownBy(() -> store.claimCreation(
                id, 1, Map.of("different", "spec"), Optional.empty(), "attempt-2").join())
            .isInstanceOf(CompletionException.class)
            .hasCauseInstanceOf(AlreadyExistsException.class);
        assertThatThrownBy(() -> store.claimCreation(
                id, 1, Map.of(), Optional.empty(),
                IndexedStreamConfigStore.CreationKind.EXTERNAL, "attempt-2").join())
            .isInstanceOf(CompletionException.class)
            .hasCauseInstanceOf(AlreadyExistsException.class);

        verify(oxiaClient, never()).put(eq(configPath), any(byte[].class), any());
    }

    @Test
    void staleOwnerCannotFinalizeAfterCompatibleTakeover() {
        IndexedStreamConfigStore.ProvisioningClaim stale = claim("attempt-1", VERSION_1);
        when(oxiaClient.get(configPath))
            .thenReturn(CompletableFuture.completedFuture(
                provisioning(VERSION_1, "attempt-1")))
            .thenReturn(CompletableFuture.completedFuture(
                provisioning(VERSION_2, "attempt-2")));
        when(oxiaClient.put(eq(configPath), any(byte[].class),
                eq(Set.of(PutOption.IfVersionIdEquals(VERSION_1.versionId())))))
            .thenReturn(CompletableFuture.completedFuture(
                new PutResult(configPath, VERSION_2)))
            .thenReturn(CompletableFuture.failedFuture(
                new UnexpectedVersionIdException(configPath, VERSION_1.versionId())));
        when(oxiaClient.put(eq(configPath), any(byte[].class),
                eq(Set.of(PutOption.IfVersionIdEquals(VERSION_2.versionId())))))
            .thenReturn(CompletableFuture.completedFuture(
                new PutResult(configPath, version(3))));

        IndexedStreamConfigStore.ProvisioningClaim current = store.claimCreation(
            id, 1, Map.of(), Optional.empty(), "attempt-2").join();
        IndexedStreamConfigStore.FinalizeOutcome staleOutcome =
            store.finalizeCreation(id, stale).join();
        IndexedStreamConfigStore.FinalizeOutcome currentOutcome =
            store.finalizeCreation(id, current).join();

        assertThat(current.ownerToken()).isEqualTo("attempt-2");
        assertThat(current.versionId()).isEqualTo(VERSION_2.versionId());
        assertThat(staleOutcome.active()).isFalse();
        assertThat(currentOutcome.active()).isTrue();
    }

    @Test
    void provisioningOwnershipRequiresExactOwnerTokenGenerationAndVersion() {
        AtomicReference<GetResult> current = new AtomicReference<>();
        when(oxiaClient.get(configPath)).thenAnswer(ignored ->
            CompletableFuture.completedFuture(current.get()));
        IndexedStreamConfigStore.ProvisioningClaim expected =
            claim("attempt-1", VERSION_1);

        current.set(provisioning(VERSION_1, "attempt-2", 1L));
        assertProvisioningOwnershipLost(expected);

        current.set(provisioning(VERSION_1, "attempt-1", 2L));
        assertProvisioningOwnershipLost(expected);

        current.set(provisioning(VERSION_2, "attempt-1", 1L));
        assertProvisioningOwnershipLost(expected);

        current.set(provisioning(VERSION_1, "attempt-1", 1L));
        store.verifyProvisioningOwnership(id, expected).join();
    }

    @Test
    void activeSnapshotVerificationRejectsSameLifecycleAtNewVersion() {
        when(oxiaClient.get(configPath))
            .thenReturn(CompletableFuture.completedFuture(
                externalActive(VERSION_1, 1, "owner", 1L, Map.of())))
            .thenReturn(CompletableFuture.completedFuture(
                externalActive(VERSION_2, 2, "owner", 1L, Map.of())));

        IndexedStreamConfigStore.ActiveStreamConfig snapshot =
            store.readActive(id).join();

        assertThatThrownBy(() -> store.verifyActiveOwnership(id, snapshot).join())
            .isInstanceOf(CompletionException.class)
            .hasCauseInstanceOf(NoSuchStreamException.class);
    }

    @Test
    void conflictingConfigUpdateWaitsForAsynchronousRetryAndThenSucceeds() {
        List<Long> backoffs = new ArrayList<>();
        CompletableFuture<Void> retryGate = new CompletableFuture<>();
        store = new IndexedStreamConfigStore(oxiaClient, paths, delayMillis -> {
            backoffs.add(delayMillis);
            return retryGate;
        });
        when(oxiaClient.get(configPath))
            .thenReturn(CompletableFuture.completedFuture(
                externalActive(VERSION_1, 1, "owner", 1L, Map.of())))
            .thenReturn(CompletableFuture.completedFuture(
                externalActive(VERSION_2, 1, "owner", 1L, Map.of())));
        when(oxiaClient.put(eq(configPath), any(byte[].class),
                eq(Set.of(PutOption.IfVersionIdEquals(VERSION_1.versionId())))))
            .thenReturn(CompletableFuture.failedFuture(
                new UnexpectedVersionIdException(configPath, VERSION_1.versionId())));
        when(oxiaClient.put(eq(configPath), any(byte[].class),
                eq(Set.of(PutOption.IfVersionIdEquals(VERSION_2.versionId())))))
            .thenReturn(CompletableFuture.completedFuture(
                new PutResult(configPath, version(3))));

        CompletableFuture<Void> update = store.setProperties(id, Map.of("tier", "hot"));

        assertThat(update.isDone()).isFalse();
        assertThat(backoffs).containsExactly(
            IndexedStreamConfigStore.INITIAL_RETRY_BACKOFF_MILLIS);
        retryGate.complete(null);
        update.join();
        verify(oxiaClient, times(2)).put(eq(configPath), any(byte[].class), any());
    }

    @Test
    void conflictingCreationClaimStopsAfterBoundedBackoffRetries() {
        List<Long> backoffs = new ArrayList<>();
        store = new IndexedStreamConfigStore(oxiaClient, paths, delayMillis -> {
            backoffs.add(delayMillis);
            return CompletableFuture.completedFuture(null);
        });
        when(oxiaClient.get(configPath)).thenReturn(CompletableFuture.completedFuture(
            provisioning(VERSION_1, "attempt-1")));
        when(oxiaClient.put(eq(configPath), any(byte[].class), any()))
            .thenReturn(CompletableFuture.failedFuture(
                new UnexpectedVersionIdException(configPath, VERSION_1.versionId())));

        assertThatThrownBy(() -> store.claimCreation(
                id, 1, Map.of(), Optional.empty(), "attempt-2").join())
            .isInstanceOf(CompletionException.class)
            .hasCauseInstanceOf(UnexpectedVersionIdException.class);

        assertThat(backoffs).containsExactly(
            IndexedStreamConfigStore.INITIAL_RETRY_BACKOFF_MILLIS,
            IndexedStreamConfigStore.INITIAL_RETRY_BACKOFF_MILLIS * 2,
            IndexedStreamConfigStore.INITIAL_RETRY_BACKOFF_MILLIS * 4);
        verify(oxiaClient, times(IndexedStreamConfigStore.MAX_CONFIG_WRITE_RETRIES + 1))
            .put(eq(configPath), any(byte[].class), any());
        verify(oxiaClient, times((IndexedStreamConfigStore.MAX_CONFIG_WRITE_RETRIES + 1) * 2))
            .get(configPath);
    }

    @Test
    void ambiguousGenericCreateClaimsOnlyItsOwnPersistedAttempt() {
        RuntimeException ambiguous = new RuntimeException("request outcome unknown");
        Set<PutOption> createOnly = Set.of(PutOption.IfRecordDoesNotExist);
        java.util.concurrent.atomic.AtomicReference<byte[]> persisted =
            new java.util.concurrent.atomic.AtomicReference<>();
        when(oxiaClient.put(eq(configPath), any(byte[].class), eq(createOnly)))
            .thenAnswer(invocation -> {
                persisted.set(invocation.getArgument(1, byte[].class));
                return CompletableFuture.failedFuture(ambiguous);
            });
        when(oxiaClient.get(configPath)).thenAnswer(ignored -> {
            byte[] current = persisted.get();
            return CompletableFuture.completedFuture(current == null ? null
                : new GetResult(configPath, current, VERSION_2));
        });

        IndexedStreamConfigStore.ProvisioningClaim claim = store.claimCreation(
            id, 1, Map.of(), Optional.empty(), "attempt").join();

        assertThat(claim.versionId()).isEqualTo(VERSION_2.versionId());
        assertThat(claim.ownerToken()).isEqualTo("attempt");
        assertThat(claim.ownerGeneration()).isEqualTo(1L);
    }

    @Test
    void permanentDeleteFencesGenericCasUpdate() {
        Set<PutOption> expectedVersion =
            Set.of(PutOption.IfVersionIdEquals(VERSION_1.versionId()));
        CompletableFuture<PutResult> lateUpdate = new CompletableFuture<>();
        when(oxiaClient.get(configPath))
            .thenReturn(CompletableFuture.completedFuture(config(VERSION_1)))
            .thenReturn(CompletableFuture.completedFuture(config(VERSION_1)))
            .thenReturn(CompletableFuture.completedFuture(tombstone(VERSION_2)));
        when(oxiaClient.put(eq(configPath), any(byte[].class), eq(expectedVersion)))
            .thenReturn(lateUpdate)
            .thenReturn(CompletableFuture.completedFuture(
                new PutResult(configPath, VERSION_2)));

        CompletableFuture<Void> update =
            store.setProperties(id, Map.of("owner", "updated"));
        store.permanentlyDeleteExternalStream(id).join();

        lateUpdate.completeExceptionally(
            new UnexpectedVersionIdException(configPath, VERSION_1.versionId()));
        assertThatThrownBy(update::join)
            .isInstanceOf(CompletionException.class)
            .hasCauseInstanceOf(NoSuchStreamException.class);
    }

    @Test
    void reversibleUnregisterCannotRemovePermanentTombstone() {
        when(oxiaClient.get(configPath))
            .thenReturn(CompletableFuture.completedFuture(tombstone(VERSION_2)));

        store.unregisterExternalStream(id).join();

        verify(oxiaClient, never()).delete(eq(configPath), any());
    }

    @Test
    void ambiguousUnregisterSucceedsOnlyWhenMatchingRetainedStateIsObserved() {
        RuntimeException ambiguous = new RuntimeException("request outcome unknown");
        Set<PutOption> expectedVersion =
            Set.of(PutOption.IfVersionIdEquals(VERSION_1.versionId()));
        byte[] active = streamConfigBytes(
            3, Map.of("tier", "hot"), "incarnation", "owner", 4L,
            IndexedStreamConfigStore.NO_METADATA_GENERATION,
            IndexedStreamConfigStore.CreationKind.EXTERNAL,
            IndexedStreamConfigStore.ProvisioningState.ACTIVE);
        byte[] unregistered = streamConfigBytes(
            3, Map.of("tier", "hot"), "incarnation", "owner", 4L, 4L,
            IndexedStreamConfigStore.CreationKind.EXTERNAL,
            IndexedStreamConfigStore.ProvisioningState.UNREGISTERED);
        when(oxiaClient.get(configPath))
            .thenReturn(CompletableFuture.completedFuture(
                new GetResult(configPath, active, VERSION_1)))
            .thenReturn(CompletableFuture.completedFuture(
                new GetResult(configPath, unregistered, VERSION_2)));
        when(oxiaClient.put(eq(configPath), any(byte[].class), eq(expectedVersion)))
            .thenReturn(CompletableFuture.failedFuture(ambiguous));

        store.unregisterExternalStream(id).join();

        verify(oxiaClient, times(2)).get(configPath);
        verify(oxiaClient).put(eq(configPath), any(byte[].class), eq(expectedVersion));
        verify(oxiaClient, never()).delete(eq(configPath), any());
    }

    @Test
    void ambiguousUnregisterFailsWhenRetainedStateWasNotWritten() {
        RuntimeException ambiguous = new RuntimeException("request outcome unknown");
        Set<PutOption> expectedVersion =
            Set.of(PutOption.IfVersionIdEquals(VERSION_1.versionId()));
        GetResult active = externalActive(VERSION_1, 3, "owner", 4L,
            Map.of("tier", "hot"));
        when(oxiaClient.get(configPath))
            .thenReturn(CompletableFuture.completedFuture(active));
        when(oxiaClient.put(eq(configPath), any(byte[].class), eq(expectedVersion)))
            .thenReturn(CompletableFuture.failedFuture(ambiguous));

        assertThatThrownBy(() -> store.unregisterExternalStream(id).join())
            .isInstanceOf(CompletionException.class)
            .hasRootCause(ambiguous);

        verify(oxiaClient, times(2)).get(configPath);
        verify(oxiaClient).put(eq(configPath), any(byte[].class), eq(expectedVersion));
    }

    @Test
    void unregisterRetainsSpecAndReregistrationKeepsIncarnationAndAdvancesGeneration() {
        AtomicReference<VersionedValue> state = mockVersionedRecord(
            externalActive(VERSION_1, 3, "owner-a", 4L,
                Map.of("tier", "hot")).value());

        store.unregisterExternalStream(id).join();

        JsonNode retained = json(state.get().value());
        assertThat(retained.path("partitions").asInt()).isEqualTo(3);
        assertThat(retained.path("properties").path("tier").asText()).isEqualTo("hot");
        assertThat(retained.path("_incarnationId").asText()).isEqualTo("incarnation");
        assertThat(retained.path("_creationKind").asText()).isEqualTo("EXTERNAL");
        assertThat(retained.path("_ownerGeneration").asLong()).isEqualTo(4L);
        assertThat(retained.path("_metadataSourceGeneration").asLong()).isEqualTo(4L);
        assertThat(retained.path("_provisioningState").asText())
            .isEqualTo("UNREGISTERED");

        IndexedStreamConfigStore.ProvisioningClaim resumed = store.claimCreation(
            id, 5, Map.of("replacement", "ignored"), Optional.empty(),
            IndexedStreamConfigStore.CreationKind.EXTERNAL, "owner-b").join();

        assertThat(resumed.incarnationId()).isEqualTo("incarnation");
        assertThat(resumed.ownerGeneration()).isEqualTo(5L);
        assertThat(resumed.config().metadataSourceGeneration()).isEqualTo(4L);
        assertThat(resumed.config().partitions()).isEqualTo(5);
        assertThat(resumed.config().properties()).containsExactlyEntriesOf(
            Map.of("tier", "hot"));
        assertThat(resumed.creationKind())
            .isEqualTo(IndexedStreamConfigStore.CreationKind.EXTERNAL);

        assertThat(store.finalizeCreation(id, resumed).join().active()).isTrue();
        JsonNode active = json(state.get().value());
        assertThat(active.path("_incarnationId").asText()).isEqualTo("incarnation");
        assertThat(active.path("_ownerGeneration").asLong()).isEqualTo(5L);
        assertThat(active.path("_metadataSourceGeneration").asLong()).isEqualTo(4L);
        assertThat(active.path("_provisioning").asBoolean(false)).isFalse();
    }

    @Test
    void unregisteredStreamAllowsNamespaceDrop() {
        String configPrefix = paths.streamConfigPrefix(id.namespace());
        byte[] unregistered = streamConfigBytes(
            1, Map.of(), "incarnation", "owner", 2L, 2L,
            IndexedStreamConfigStore.CreationKind.EXTERNAL,
            IndexedStreamConfigStore.ProvisioningState.UNREGISTERED);
        when(oxiaClient.list(configPrefix, configPrefix + "\uffff"))
            .thenReturn(CompletableFuture.completedFuture(List.of(configPath)));
        when(oxiaClient.get(configPath)).thenReturn(CompletableFuture.completedFuture(
            new GetResult(configPath, unregistered, VERSION_2)));

        assertThat(store.namespaceContainsNonTombstoneStream(id.namespace()).join())
            .isFalse();
    }

    @Test
    void readsTreatPermanentTombstoneAsAbsent() {
        when(oxiaClient.get(configPath))
            .thenReturn(CompletableFuture.completedFuture(tombstone(VERSION_2)));

        assertThatThrownBy(() -> store.read(id).join())
            .isInstanceOf(CompletionException.class)
            .hasCauseInstanceOf(NoSuchStreamException.class);
        assertThat(store.exists(id).join()).isFalse();
    }

    @Test
    void emptyWhitespaceAndNonObjectConfigsAreRejected() {
        AtomicReference<byte[]> current = new AtomicReference<>();
        when(oxiaClient.get(configPath)).thenAnswer(ignored ->
            CompletableFuture.completedFuture(
                new GetResult(configPath, current.get(), VERSION_1)));

        for (byte[] invalid : List.of(
                new byte[0], "   \n".getBytes(StandardCharsets.UTF_8),
                "[]".getBytes(StandardCharsets.UTF_8),
                "\"text\"".getBytes(StandardCharsets.UTF_8),
                "null".getBytes(StandardCharsets.UTF_8))) {
            current.set(invalid);
            assertThatThrownBy(() -> store.read(id).join())
                .isInstanceOf(CompletionException.class)
                .hasRootCauseInstanceOf(IllegalArgumentException.class);
            assertThatThrownBy(() -> store.exists(id).join())
                .isInstanceOf(CompletionException.class)
                .hasRootCauseInstanceOf(IllegalArgumentException.class);
        }
    }

    @Test
    void permanentDeletionRetainsSpecIdentityKindAndMetadataSourceGeneration() {
        AtomicReference<VersionedValue> state = mockVersionedRecord(
            externalActive(VERSION_1, 3, "registration-owner", 6L,
                Map.of("tier", "hot")).value());

        store.permanentlyDeleteExternalStream(id).join();

        JsonNode deleted = json(state.get().value());
        assertThat(deleted.path("partitions").asInt()).isEqualTo(3);
        assertThat(deleted.path("properties").path("tier").asText()).isEqualTo("hot");
        assertThat(deleted.path("_incarnationId").asText()).isEqualTo("incarnation");
        assertThat(deleted.path("_creationKind").asText()).isEqualTo("EXTERNAL");
        assertThat(deleted.path("_ownerGeneration").asLong()).isEqualTo(7L);
        assertThat(deleted.path("_metadataSourceGeneration").asLong()).isEqualTo(6L);
        assertThat(deleted.path("_provisioningState").asText())
            .isEqualTo("PERMANENTLY_DELETED");
        assertThat(deleted.path("_externalStreamPermanentlyDeleted").asBoolean()).isTrue();

        IndexedStreamConfigStore.ExternalDeletionContext deletionContext =
            store.readExternalDeletionContext(id).join();
        assertThat(deletionContext.config().partitions()).isEqualTo(3);
        assertThat(deletionContext.config().incarnationId()).contains("incarnation");
        assertThat(deletionContext.config().creationKind())
            .contains(IndexedStreamConfigStore.CreationKind.EXTERNAL);
        assertThat(deletionContext.metadataGeneration()).isEqualTo(6L);
    }

    @Test
    void unregisterDoesNotReviveACompletedExternalDrop() {
        byte[] dropped = streamConfigBytes(
            3, Map.of("tier", "hot"), "incarnation", "drop-owner", 7L, 6L,
            IndexedStreamConfigStore.CreationKind.EXTERNAL,
            IndexedStreamConfigStore.ProvisioningState.DROPPED);
        AtomicReference<VersionedValue> state = mockVersionedRecord(dropped);

        store.unregisterExternalStream(id).join();

        assertThat(state.get().value()).isEqualTo(dropped);
        verify(oxiaClient, never()).put(eq(configPath), any(byte[].class), any());
    }

    @Test
    void nativeProvisioningIsInvisibleAndCannotBeMutatedOrExternallyRegistered() {
        when(oxiaClient.get(configPath)).thenReturn(CompletableFuture.completedFuture(
            provisioning(VERSION_1, "attempt")));

        assertThatThrownBy(() -> store.read(id).join())
            .isInstanceOf(CompletionException.class)
            .hasCauseInstanceOf(NoSuchStreamException.class);
        assertThat(store.exists(id).join()).isFalse();
        assertThatThrownBy(() -> store.setProperties(id, Map.of("k", "v")).join())
            .isInstanceOf(CompletionException.class)
            .hasCauseInstanceOf(NoSuchStreamException.class);
        assertThatThrownBy(() -> store.registerExternalStream(id, 1, Map.of()).join())
            .isInstanceOf(CompletionException.class)
            .hasCauseInstanceOf(AlreadyExistsException.class);
        assertThatThrownBy(() -> store.unregisterExternalStream(id).join())
            .isInstanceOf(CompletionException.class)
            .hasCauseInstanceOf(AlreadyExistsException.class);
        assertThatThrownBy(() -> store.permanentlyDeleteExternalStream(id).join())
            .isInstanceOf(CompletionException.class)
            .hasCauseInstanceOf(AlreadyExistsException.class);

        verify(oxiaClient, never()).put(eq(configPath), any(byte[].class), any());
        verify(oxiaClient, never()).delete(eq(configPath), any());
    }

    @Test
    void externalProvisioningCannotBeUnregisteredOrPermanentlyDeleted() {
        when(oxiaClient.get(configPath)).thenReturn(CompletableFuture.completedFuture(
            new GetResult(configPath, provisioningBytes(
                "external-owner", 3L,
                IndexedStreamConfigStore.CreationKind.EXTERNAL), VERSION_1)));

        assertThatThrownBy(() -> store.unregisterExternalStream(id).join())
            .isInstanceOf(CompletionException.class)
            .hasCauseInstanceOf(AlreadyExistsException.class);
        assertThatThrownBy(() -> store.permanentlyDeleteExternalStream(id).join())
            .isInstanceOf(CompletionException.class)
            .hasCauseInstanceOf(AlreadyExistsException.class);

        verify(oxiaClient, never()).put(eq(configPath), any(byte[].class), any());
        verify(oxiaClient, never()).delete(eq(configPath), any());
    }

    @Test
    void resumedExternalProvisioningCanBeTakenOverUsingRetainedSpec() {
        byte[] unregistered = streamConfigBytes(
            2, Map.of("tier", "hot"), "incarnation", "owner-4", 4L, 4L,
            IndexedStreamConfigStore.CreationKind.EXTERNAL,
            IndexedStreamConfigStore.ProvisioningState.UNREGISTERED);
        mockVersionedRecord(unregistered);

        IndexedStreamConfigStore.ProvisioningClaim resumed = store.claimCreation(
            id, 3, Map.of("tier", "cold"), Optional.empty(),
            IndexedStreamConfigStore.CreationKind.EXTERNAL, "owner-5").join();
        IndexedStreamConfigStore.ProvisioningClaim recovered = store.claimCreation(
            id, 3, Map.of("tier", "cold"), Optional.empty(),
            IndexedStreamConfigStore.CreationKind.EXTERNAL, "owner-6").join();

        assertThat(resumed.config().properties()).containsEntry("tier", "hot");
        assertThat(recovered.config().properties()).containsEntry("tier", "hot");
        assertThat(recovered.ownerToken()).isEqualTo("owner-6");
        assertThat(recovered.ownerGeneration())
            .isEqualTo(resumed.ownerGeneration() + 1);
        assertThat(recovered.config().partitions()).isEqualTo(3);
    }

    @Test
    void externalDeletionApisRejectNativeStreamInEveryRetainedState() {
        List<GetResult> nativeStates = List.of(
            provisioning(VERSION_1, "create-owner"),
            active(VERSION_1, "create-owner", 1),
            aborting(VERSION_1, "drop-owner"),
            new GetResult(configPath, streamConfigBytes(
                1, Map.of(), "incarnation", "create-owner", 1L, 1L,
                IndexedStreamConfigStore.CreationKind.NATIVE_CREATE,
                IndexedStreamConfigStore.ProvisioningState.UNREGISTERED), VERSION_1),
            new GetResult(configPath, streamConfigBytes(
                1, Map.of(), "incarnation", "delete-owner", 2L, 1L,
                IndexedStreamConfigStore.CreationKind.NATIVE_CREATE,
                IndexedStreamConfigStore.ProvisioningState.PERMANENTLY_DELETED), VERSION_1));
        AtomicReference<GetResult> current = new AtomicReference<>();
        when(oxiaClient.get(configPath)).thenAnswer(ignored ->
            CompletableFuture.completedFuture(current.get()));

        for (GetResult nativeState : nativeStates) {
            current.set(nativeState);
            assertThatThrownBy(() -> store.unregisterExternalStream(id).join())
                .as("unregister must reject %s", json(nativeState.value())
                    .path("_provisioningState").asText("ACTIVE"))
                .isInstanceOf(CompletionException.class)
                .hasCauseInstanceOf(AlreadyExistsException.class);
            assertThatThrownBy(() -> store.permanentlyDeleteExternalStream(id).join())
                .as("permanent delete must reject %s", json(nativeState.value())
                    .path("_provisioningState").asText("ACTIVE"))
                .isInstanceOf(CompletionException.class)
                .hasCauseInstanceOf(AlreadyExistsException.class);
        }

        verify(oxiaClient, never()).put(eq(configPath), any(byte[].class), any());
        verify(oxiaClient, never()).delete(eq(configPath), any());
    }

    @Test
    void dropTakeoverFencesOldOwnerAndOnlyCurrentVersionCanComplete() {
        byte[] activeConfig = ("{\"partitions\":3,\"properties\":{\"tier\":\"hot\"},"
            + "\"_incarnationId\":\"incarnation\","
            + "\"_ownerToken\":\"create-owner\","
            + "\"_ownerGeneration\":1,"
            + "\"_creationKind\":\"NATIVE_CREATE\"}")
            .getBytes(StandardCharsets.UTF_8);
        AtomicReference<VersionedValue> state = mockVersionedRecord(activeConfig);

        IndexedStreamConfigStore.DropClaim stale =
            store.beginDrop(id, "drop-a").join().orElseThrow();
        assertThat(stale.config().provisioningState())
            .isEqualTo(IndexedStreamConfigStore.ProvisioningState.ABORTING);
        assertThat(stale.config().partitions()).isEqualTo(3);
        assertThat(stale.config().properties()).containsEntry("tier", "hot");
        assertThat(stale.config().incarnationId()).contains("incarnation");
        assertThat(stale.config().creationKind())
            .contains(IndexedStreamConfigStore.CreationKind.NATIVE_CREATE);
        assertThat(store.exists(id).join()).isFalse();

        IndexedStreamConfigStore.DropClaim current =
            store.beginDrop(id, "drop-b").join().orElseThrow();
        assertThat(current.ownerToken()).isEqualTo("drop-b");
        assertThat(current.config().incarnationId())
            .isEqualTo(stale.config().incarnationId());
        assertThat(current.config().ownerGeneration())
            .isEqualTo(stale.config().ownerGeneration() + 1);
        assertThat(current.versionId()).isGreaterThan(stale.versionId());

        assertThatThrownBy(() -> store.verifyAbortingOwnership(id, stale).join())
            .isInstanceOf(CompletionException.class)
            .hasCauseInstanceOf(IndexedStreamConfigStore.AbortingOwnershipLostException.class);
        assertThatThrownBy(() -> store.completeDrop(id, stale).join())
            .isInstanceOf(CompletionException.class)
            .hasCauseInstanceOf(IndexedStreamConfigStore.AbortingOwnershipLostException.class);

        store.verifyAbortingOwnership(id, current).join();
        store.completeDrop(id, current).join();
        assertThat(state.get()).isNotNull();
        assertThat(json(state.get().value()).path("_provisioningState").asText())
            .isEqualTo("DROPPED");
        assertThat(store.exists(id).join()).isFalse();
    }

    @Test
    void purgingDropIntentSurvivesTakeoverAndCompletion() throws Exception {
        byte[] activeConfig = ("{\"partitions\":1,\"properties\":{},"
            + "\"_incarnationId\":\"incarnation\","
            + "\"_ownerToken\":\"create-owner\","
            + "\"_ownerGeneration\":1,"
            + "\"_creationKind\":\"NATIVE_CREATE\"}")
            .getBytes(StandardCharsets.UTF_8);
        AtomicReference<VersionedValue> state = mockVersionedRecord(activeConfig);

        IndexedStreamConfigStore.DropClaim first =
            store.beginDrop(id, "drop-a", true).join().orElseThrow();
        assertThat(first.config().purgeRequested()).isTrue();

        IndexedStreamConfigStore.DropClaim takeover =
            store.beginDrop(id, "drop-b", false).join().orElseThrow();
        assertThat(takeover.config().purgeRequested()).isTrue();
        store.completeDrop(id, takeover).join();

        assertThat(json(state.get().value()).path("_purgeRequested").asBoolean())
            .isTrue();
        IndexedStreamConfigStore.CompletedDrop completed =
            store.readCompletedPurgingDrop(id).join().orElseThrow();
        assertThat(completed.config().purgeRequested()).isTrue();
        store.verifyCompletedDrop(id, completed).join();
    }

    @Test
    void dropCanAdoptCrashedProvisioningClaimWithoutChangingIncarnation() {
        AtomicReference<VersionedValue> state =
            mockVersionedRecord(provisioning(VERSION_1, "create-owner").value());

        IndexedStreamConfigStore.DropClaim drop =
            store.beginDrop(id, "drop-owner").join().orElseThrow();

        assertThat(drop.config().provisioningState())
            .isEqualTo(IndexedStreamConfigStore.ProvisioningState.ABORTING);
        assertThat(drop.config().incarnationId()).contains("incarnation");
        assertThat(drop.config().creationKind())
            .contains(IndexedStreamConfigStore.CreationKind.NATIVE_CREATE);
        assertThat(drop.ownerToken()).isEqualTo("drop-owner");
        assertThat(state.get()).isNotNull();
    }

    @Test
    void failedCompleteDropRetainsRetryableAbortingClaim() {
        IndexedStreamConfigStore.StreamConfigData aborting =
            IndexedStreamConfigStore.StreamConfigData.provisioning(
                1, Map.of(), Optional.empty(),
                IndexedStreamConfigStore.CreationKind.NATIVE_CREATE,
                "incarnation", "create-owner")
                .abort("drop-owner");
        IndexedStreamConfigStore.DropClaim claim =
            new IndexedStreamConfigStore.DropClaim(
                aborting, "drop-owner", VERSION_1.versionId());
        RuntimeException writeFailure = new RuntimeException("drop completion failed");
        Set<PutOption> expectedVersion =
            Set.of(PutOption.IfVersionIdEquals(VERSION_1.versionId()));
        when(oxiaClient.put(eq(configPath), any(byte[].class), eq(expectedVersion)))
            .thenReturn(CompletableFuture.failedFuture(writeFailure));
        when(oxiaClient.get(configPath)).thenReturn(CompletableFuture.completedFuture(
            aborting(VERSION_1, "drop-owner")));

        assertThatThrownBy(() -> store.completeDrop(id, claim).join())
            .isInstanceOf(CompletionException.class)
            .hasCause(writeFailure);

        IndexedStreamConfigStore.DropClaim retry =
            store.beginDrop(id, "drop-owner").join().orElseThrow();
        assertThat(retry.versionId()).isEqualTo(VERSION_1.versionId());
        assertThat(retry.ownerToken()).isEqualTo("drop-owner");
    }

    @Test
    void ambiguousFinalizeAcceptsActiveConfigWithSameAttemptAfterUpdate() {
        RuntimeException ambiguous = new RuntimeException("finalize response unknown");
        IndexedStreamConfigStore.ProvisioningClaim claim = claim("attempt", VERSION_1);
        when(oxiaClient.put(eq(configPath), any(byte[].class),
                eq(Set.of(PutOption.IfVersionIdEquals(VERSION_1.versionId())))))
            .thenReturn(CompletableFuture.failedFuture(ambiguous));
        when(oxiaClient.get(configPath)).thenReturn(CompletableFuture.completedFuture(
            active(VERSION_2, "attempt", 2)));

        IndexedStreamConfigStore.FinalizeOutcome outcome =
            store.finalizeCreation(id, claim).join();

        assertThat(outcome.active()).isTrue();
        assertThat(outcome.failure()).isNull();
    }

    @Test
    void ambiguousFinalizeReadbackFailureIsNotRollbackSafe() {
        RuntimeException ambiguous = new RuntimeException("finalize response unknown");
        RuntimeException readFailure = new RuntimeException("readback unavailable");
        IndexedStreamConfigStore.ProvisioningClaim claim = claim("attempt", VERSION_1);
        when(oxiaClient.put(eq(configPath), any(byte[].class),
                eq(Set.of(PutOption.IfVersionIdEquals(VERSION_1.versionId())))))
            .thenReturn(CompletableFuture.failedFuture(ambiguous));
        when(oxiaClient.get(configPath))
            .thenReturn(CompletableFuture.failedFuture(readFailure));

        IndexedStreamConfigStore.FinalizeOutcome outcome =
            store.finalizeCreation(id, claim).join();

        assertThat(outcome.active()).isFalse();
        assertThat(outcome.failure()).isSameAs(ambiguous);
        assertThat(ambiguous.getSuppressed()).contains(readFailure);
    }

    private GetResult config(Version version) {
        return new GetResult(configPath, CONFIG, version);
    }

    private GetResult tombstone(Version version) {
        return new GetResult(configPath, TOMBSTONE, version);
    }

    private GetResult provisioning(Version version, String attempt) {
        return provisioning(version, attempt, 1L);
    }

    private GetResult provisioning(Version version, String attempt, long ownerGeneration) {
        return new GetResult(configPath, provisioningBytes(
            attempt, ownerGeneration,
            IndexedStreamConfigStore.CreationKind.NATIVE_CREATE), version);
    }

    private byte[] provisioningBytes(
            String attempt, long ownerGeneration,
            IndexedStreamConfigStore.CreationKind kind) {
        return streamConfigBytes(
            1, Map.of(), "incarnation", attempt, ownerGeneration,
            IndexedStreamConfigStore.NO_METADATA_GENERATION, kind,
            IndexedStreamConfigStore.ProvisioningState.PROVISIONING);
    }

    private GetResult active(Version version, String attempt, int partitions) {
        return new GetResult(configPath, streamConfigBytes(
            partitions, Map.of(), "incarnation", attempt, 1L,
            IndexedStreamConfigStore.NO_METADATA_GENERATION,
            IndexedStreamConfigStore.CreationKind.NATIVE_CREATE,
            IndexedStreamConfigStore.ProvisioningState.ACTIVE), version);
    }

    private GetResult aborting(Version version, String attempt) {
        return new GetResult(configPath, streamConfigBytes(
            1, Map.of(), "incarnation", attempt, 2L, 1L,
            IndexedStreamConfigStore.CreationKind.NATIVE_CREATE,
            IndexedStreamConfigStore.ProvisioningState.ABORTING), version);
    }

    private GetResult externalActive(
            Version version, int partitions, String ownerToken,
            long ownerGeneration, Map<String, String> properties) {
        return new GetResult(configPath, streamConfigBytes(
            partitions, properties, "incarnation", ownerToken, ownerGeneration,
            IndexedStreamConfigStore.NO_METADATA_GENERATION,
            IndexedStreamConfigStore.CreationKind.EXTERNAL,
            IndexedStreamConfigStore.ProvisioningState.ACTIVE), version);
    }

    private static byte[] streamConfigBytes(
            int partitions, Map<String, String> properties,
            String incarnation, String ownerToken, long ownerGeneration,
            long metadataSourceGeneration,
            IndexedStreamConfigStore.CreationKind creationKind,
            IndexedStreamConfigStore.ProvisioningState state) {
        ObjectNode node = MAPPER.createObjectNode();
        node.put("partitions", partitions);
        ObjectNode propertiesNode = node.putObject("properties");
        properties.forEach(propertiesNode::put);
        node.put("_incarnationId", incarnation);
        node.put("_ownerToken", ownerToken);
        node.put("_ownerGeneration", ownerGeneration);
        if (metadataSourceGeneration != IndexedStreamConfigStore.NO_METADATA_GENERATION) {
            node.put("_metadataSourceGeneration", metadataSourceGeneration);
        }
        node.put("_creationKind", creationKind.name());
        if (state != IndexedStreamConfigStore.ProvisioningState.ACTIVE) {
            node.put("_provisioning", true);
            node.put("_provisioningState", state.name());
        }
        if (state == IndexedStreamConfigStore.ProvisioningState.PERMANENTLY_DELETED) {
            node.put("_externalStreamPermanentlyDeleted", true);
        }
        try {
            return MAPPER.writeValueAsBytes(node);
        } catch (Exception e) {
            throw new AssertionError("Failed to serialize test stream config", e);
        }
    }

    private static JsonNode json(byte[] value) {
        try {
            return MAPPER.readTree(value);
        } catch (Exception e) {
            throw new AssertionError("Failed to parse test stream config", e);
        }
    }

    private IndexedStreamConfigStore.ProvisioningClaim claim(
            String attempt, Version version) {
        return new IndexedStreamConfigStore.ProvisioningClaim(
            IndexedStreamConfigStore.StreamConfigData.provisioning(
                1, Map.of(), Optional.empty(),
                IndexedStreamConfigStore.CreationKind.NATIVE_CREATE,
                "incarnation", attempt),
            "incarnation", attempt,
            IndexedStreamConfigStore.CreationKind.NATIVE_CREATE,
            1L,
            version.versionId());
    }

    private void assertProvisioningOwnershipLost(
            IndexedStreamConfigStore.ProvisioningClaim claim) {
        assertThatThrownBy(() -> store.verifyProvisioningOwnership(id, claim).join())
            .isInstanceOf(CompletionException.class)
            .hasCauseInstanceOf(
                IndexedStreamConfigStore.ProvisioningOwnershipLostException.class);
    }

    private AtomicReference<VersionedValue> mockVersionedRecord(byte[] initialValue) {
        AtomicReference<VersionedValue> state = new AtomicReference<>(
            new VersionedValue(initialValue.clone(), VERSION_1));
        AtomicLong nextVersion = new AtomicLong(VERSION_1.versionId());
        lenient().when(oxiaClient.get(configPath)).thenAnswer(ignored -> {
            VersionedValue current = state.get();
            return CompletableFuture.completedFuture(current == null ? null
                : new GetResult(configPath, current.value(), current.version()));
        });
        lenient().when(oxiaClient.put(eq(configPath), any(byte[].class), any()))
            .thenAnswer(invocation -> {
                @SuppressWarnings("unchecked")
                Set<PutOption> options = invocation.getArgument(2, Set.class);
                VersionedValue current = state.get();
                if (current == null || !options.contains(
                        PutOption.IfVersionIdEquals(current.version().versionId()))) {
                    return CompletableFuture.failedFuture(
                        new UnexpectedVersionIdException(
                            configPath, current == null
                                ? -1L : current.version().versionId()));
                }
                Version next = version(nextVersion.incrementAndGet());
                byte[] value = invocation.getArgument(1, byte[].class);
                state.set(new VersionedValue(value.clone(), next));
                return CompletableFuture.completedFuture(new PutResult(configPath, next));
            });
        lenient().when(oxiaClient.delete(eq(configPath), any())).thenAnswer(invocation -> {
            @SuppressWarnings("unchecked")
            Set<DeleteOption> options = invocation.getArgument(1, Set.class);
            VersionedValue current = state.get();
            if (current == null || !options.contains(
                    DeleteOption.IfVersionIdEquals(current.version().versionId()))) {
                return CompletableFuture.completedFuture(false);
            }
            state.set(null);
            return CompletableFuture.completedFuture(true);
        });
        return state;
    }

    private record VersionedValue(byte[] value, Version version) {
    }

    private static Version version(long id) {
        return new Version(id, 0, 0, 0, Optional.empty(), Optional.empty());
    }
}
