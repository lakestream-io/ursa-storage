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
import java.util.concurrent.atomic.AtomicInteger;
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
    void staleAbsentCreateIsFencedAfterObservingDroppedTombstone() {
        Set<PutOption> createOnly = Set.of(PutOption.IfRecordDoesNotExist);
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

        CompletableFuture<IndexedStreamConfigStore.ProvisioningClaim> stale =
            store.claimCreation(id, 1, Map.of(), Optional.empty(), "stale-owner");
        staleCreateWrite.completeExceptionally(new KeyAlreadyExistsException(configPath));

        assertThatThrownBy(stale::join)
            .isInstanceOf(CompletionException.class)
            .hasCauseInstanceOf(StreamPermanentlyDeletedException.class);
        verify(oxiaClient).put(eq(configPath), any(byte[].class), eq(createOnly));
        verify(oxiaClient, never()).put(eq(configPath), any(byte[].class),
            eq(Set.of(PutOption.IfVersionIdEquals(VERSION_2.versionId()))));
    }

    @Test
    void compatibleGenericCreateAdoptsMatchingProvisioningClaim() {
        when(oxiaClient.get(configPath)).thenReturn(CompletableFuture.completedFuture(
            provisioning(VERSION_1, "attempt-1")));

        IndexedStreamConfigStore.ProvisioningClaim claim = store.claimCreation(
            id, 1, Map.of(), Optional.empty(), "attempt-2").join();

        assertThat(claim.incarnationId()).isEqualTo("incarnation");
        assertThat(claim.ownerToken()).isEqualTo("attempt-1");
        assertThat(claim.ownerGeneration()).isEqualTo(1L);
        assertThat(claim.versionId()).isEqualTo(VERSION_1.versionId());
        verify(oxiaClient, never()).put(eq(configPath), any(byte[].class), any());
    }

    @Test
    void eachCompatibleRetryAdoptsOwnerWithoutAdvancingGeneration() {
        AtomicReference<VersionedValue> state = mockVersionedRecord(
            provisioningBytes("attempt-1", 1L,
                IndexedStreamConfigStore.CreationKind.NATIVE_CREATE));

        IndexedStreamConfigStore.ProvisioningClaim second = store.claimCreation(
            id, 1, Map.of(), Optional.empty(), "attempt-2").join();
        IndexedStreamConfigStore.ProvisioningClaim third = store.claimCreation(
            id, 1, Map.of(), Optional.empty(), "attempt-3").join();

        assertThat(second.ownerToken()).isEqualTo("attempt-1");
        assertThat(third.ownerToken()).isEqualTo("attempt-1");
        assertThat(second.ownerGeneration()).isEqualTo(1L);
        assertThat(third.ownerGeneration()).isEqualTo(1L);
        assertThat(third.incarnationId()).isEqualTo(second.incarnationId());
        assertThat(json(state.get().value()).path("_ownerGeneration").asLong())
            .isEqualTo(1L);
        verify(oxiaClient, never()).put(eq(configPath), any(byte[].class), any());
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
    void compatibleRetryPreservesExistingOwnerGenerationAndVersion() {
        when(oxiaClient.get(configPath)).thenReturn(CompletableFuture.completedFuture(
            provisioning(VERSION_1, "attempt-1", 4L)));

        IndexedStreamConfigStore.ProvisioningClaim recovered = store.claimCreation(
            id, 1, Map.of(), Optional.empty(), "attempt-2").join();

        assertThat(recovered.ownerToken()).isEqualTo("attempt-1");
        assertThat(recovered.ownerGeneration()).isEqualTo(4L);
        assertThat(recovered.versionId()).isEqualTo(VERSION_1.versionId());
        verify(oxiaClient, never()).put(eq(configPath), any(byte[].class), any());
    }

    @Test
    void changedMutableDesiredStateConvergesDurableProvisioningClaim() {
        AtomicReference<VersionedValue> state = mockVersionedRecord(
            provisioningBytes("attempt-1", 1L,
                IndexedStreamConfigStore.CreationKind.NATIVE_CREATE));

        IndexedStreamConfigStore.ProvisioningClaim claim = store.claimCreation(
            id, new StreamConfig(), indexedPartitioning(3), new SchemaConfig(),
            Map.of("lakestream.kafka.source.revision", "7"),
            Optional.of(TableMaterializationPolicy.empty()),
            IndexedStreamConfigStore.CreationKind.NATIVE_CREATE, "attempt-2").join();

        assertThat(claim.ownerToken()).isEqualTo("attempt-1");
        assertThat(claim.ownerGeneration()).isEqualTo(1L);
        assertThat(claim.versionId()).isEqualTo(VERSION_2.versionId());
        assertThat(claim.config().partitions()).isEqualTo(3);
        assertThat(claim.config().properties())
            .containsExactlyEntriesOf(Map.of("lakestream.kafka.source.revision", "7"));
        assertThat(claim.config().materialization())
            .contains(TableMaterializationPolicy.empty());
        assertThat(json(state.get().value()).path("_ownerToken").asText())
            .isEqualTo("attempt-1");
        verify(oxiaClient).put(eq(configPath), any(byte[].class),
            eq(Set.of(PutOption.IfVersionIdEquals(VERSION_1.versionId()))));
    }

    @Test
    void immutableCreationShapeCannotTakeOverProvisioningClaim() {
        when(oxiaClient.get(configPath)).thenReturn(CompletableFuture.completedFuture(
            provisioning(VERSION_1, "attempt-1")));

        assertIncompatibleCreation(
            new StreamConfig(Map.of("retention.ms", "1000")),
            indexedPartitioning(1), new SchemaConfig());
        assertIncompatibleCreation(
            new StreamConfig(),
            new Partitioning(
                PartitioningStrategy.RANGE, Map.of("numPartitions", "1")),
            new SchemaConfig());
        assertIncompatibleCreation(
            new StreamConfig(), indexedPartitioning(1),
            new SchemaConfig("JSON", Map.of("schema", "{}")));
        assertIncompatibleCreation(
            new StreamConfig(),
            new Partitioning(
                PartitioningStrategy.INDEXED,
                Map.of("numPartitions", "1", "placement", "ordered")),
            new SchemaConfig());

        verify(oxiaClient, never()).put(eq(configPath), any(byte[].class), any());
    }

    @Test
    void provisioningResumeCannotShrinkDurablePartitionCount() {
        when(oxiaClient.get(configPath)).thenReturn(CompletableFuture.completedFuture(
            new GetResult(configPath, streamConfigBytes(
                2, Map.of(), "incarnation", "attempt-1", 1L,
                IndexedStreamConfigStore.NO_METADATA_GENERATION,
                IndexedStreamConfigStore.CreationKind.NATIVE_CREATE,
                IndexedStreamConfigStore.ProvisioningState.PROVISIONING), VERSION_1)));

        assertIncompatibleCreation(
            new StreamConfig(), indexedPartitioning(1), new SchemaConfig());

        verify(oxiaClient, never()).put(eq(configPath), any(byte[].class), any());
    }

    @Test
    void compatibleCallerAdoptsDurableOwnerAndCanFinalize() {
        AtomicReference<VersionedValue> state = mockVersionedRecord(
            provisioning(VERSION_1, "attempt-1").value());

        IndexedStreamConfigStore.ProvisioningClaim adopted = store.claimCreation(
            id, 1, Map.of(), Optional.empty(), "attempt-2").join();
        IndexedStreamConfigStore.FinalizeOutcome outcome =
            store.finalizeCreation(id, adopted).join();

        assertThat(adopted.ownerToken()).isEqualTo("attempt-1");
        assertThat(adopted.versionId()).isEqualTo(VERSION_1.versionId());
        assertThat(outcome.active()).isTrue();
        assertThat(json(state.get().value()).path("_ownerToken").asText())
            .isEqualTo("attempt-1");
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
                nativeActive(VERSION_1, 1, "owner", 1L, Map.of())))
            .thenReturn(CompletableFuture.completedFuture(
                nativeActive(VERSION_2, 2, "owner", 1L, Map.of())));

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
                nativeActive(VERSION_1, 1, "owner", 1L, Map.of())))
            .thenReturn(CompletableFuture.completedFuture(
                nativeActive(VERSION_2, 1, "owner", 1L, Map.of())));
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
    void compatibleCreationClaimReturnsDurableOwnerWithoutBackoff() {
        List<Long> backoffs = new ArrayList<>();
        store = new IndexedStreamConfigStore(oxiaClient, paths, delayMillis -> {
            backoffs.add(delayMillis);
            return CompletableFuture.completedFuture(null);
        });
        when(oxiaClient.get(configPath)).thenReturn(CompletableFuture.completedFuture(
            provisioning(VERSION_1, "attempt-1")));
        IndexedStreamConfigStore.ProvisioningClaim claim = store.claimCreation(
            id, 1, Map.of(), Optional.empty(), "attempt-2").join();

        assertThat(claim.ownerToken()).isEqualTo("attempt-1");
        assertThat(claim.ownerGeneration()).isEqualTo(1L);
        assertThat(backoffs).isEmpty();
        verify(oxiaClient, never()).put(eq(configPath), any(byte[].class), any());
        verify(oxiaClient).get(configPath);
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
    void compatibleInitialClaimRaceConvergesLatestIntentWithDurableWinnerIdentity() {
        InitialClaimRace race = mockInitiallyAbsentClaimRace();

        CompletableFuture<IndexedStreamConfigStore.ProvisioningClaim> first =
            store.claimCreation(
                id, new StreamConfig(), indexedPartitioning(1), new SchemaConfig(),
                Map.of("tier", "hot"), Optional.empty(),
                IndexedStreamConfigStore.CreationKind.NATIVE_CREATE, "owner-a");
        CompletableFuture<IndexedStreamConfigStore.ProvisioningClaim> second =
            store.claimCreation(
                id, new StreamConfig(), indexedPartitioning(2), new SchemaConfig(),
                Map.of("tier", "cold"),
                Optional.of(TableMaterializationPolicy.empty()),
                IndexedStreamConfigStore.CreationKind.NATIVE_CREATE, "owner-b");

        assertThat(first).isNotDone();
        assertThat(second).isNotDone();
        race.releaseInitialReads();

        IndexedStreamConfigStore.ProvisioningClaim firstClaim = first.join();
        IndexedStreamConfigStore.ProvisioningClaim secondClaim = second.join();
        JsonNode durable = json(race.state().get().value());
        String durableOwner = durable.path("_ownerToken").asText();

        assertThat(firstClaim.ownerToken()).isEqualTo(durableOwner);
        assertThat(secondClaim.ownerToken()).isEqualTo(durableOwner);
        assertThat(firstClaim.incarnationId()).isEqualTo(secondClaim.incarnationId());
        assertThat(firstClaim.ownerGeneration()).isEqualTo(1L);
        assertThat(secondClaim.ownerGeneration()).isEqualTo(1L);
        assertThat(firstClaim.versionId()).isEqualTo(VERSION_1.versionId());
        assertThat(secondClaim.versionId()).isEqualTo(VERSION_2.versionId());
        assertThat(secondClaim.config().partitions()).isEqualTo(2);
        assertThat(secondClaim.config().properties())
            .containsExactlyEntriesOf(Map.of("tier", "cold"));
        assertThat(secondClaim.config().materialization())
            .contains(TableMaterializationPolicy.empty());
        assertThat(durable.path("partitions").asInt()).isEqualTo(2);
        assertThat(durable.path("properties"))
            .isEqualTo(json("{\"tier\":\"cold\"}".getBytes(StandardCharsets.UTF_8)));
    }

    @Test
    void incompatibleInitialClaimRaceRejectsTheLosingClaim() {
        InitialClaimRace race = mockInitiallyAbsentClaimRace();

        CompletableFuture<IndexedStreamConfigStore.ProvisioningClaim> winner =
            store.claimCreation(
                id, new StreamConfig(), indexedPartitioning(1), new SchemaConfig(),
                Map.of("tier", "hot"), Optional.empty(),
                IndexedStreamConfigStore.CreationKind.NATIVE_CREATE, "owner-a");
        CompletableFuture<IndexedStreamConfigStore.ProvisioningClaim> incompatible =
            store.claimCreation(
                id, new StreamConfig(),
                new Partitioning(
                    PartitioningStrategy.RANGE, Map.of("numPartitions", "2")),
                new SchemaConfig(), Map.of("tier", "cold"), Optional.empty(),
                IndexedStreamConfigStore.CreationKind.NATIVE_CREATE, "owner-b");

        race.releaseInitialReads();

        assertThat(winner.join().ownerToken())
            .isEqualTo(json(race.state().get().value()).path("_ownerToken").asText());
        assertThatThrownBy(incompatible::join)
            .isInstanceOf(CompletionException.class)
            .hasCauseInstanceOf(AlreadyExistsException.class);
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
    void nativeProvisioningIsInvisibleAndCannotBeMutated() {
        when(oxiaClient.get(configPath)).thenReturn(CompletableFuture.completedFuture(
            provisioning(VERSION_1, "create-owner")));

        assertThat(store.exists(id).join()).isFalse();
        assertThatThrownBy(() -> store.readActive(id).join())
            .isInstanceOf(CompletionException.class)
            .hasCauseInstanceOf(NoSuchStreamException.class);
        assertThatThrownBy(() -> store.setProperties(id, Map.of("tier", "hot")).join())
            .isInstanceOf(CompletionException.class)
            .hasCauseInstanceOf(NoSuchStreamException.class);
    }

    @Test
    void listStreamEntriesIncludesCreatingActiveAndDeletingButNotDropped() {
        String prefix = paths.streamConfigPrefix(id.namespace());
        String activeKey = prefix + "active";
        String creatingKey = prefix + "creating";
        String deletingKey = prefix + "deleting";
        String droppedKey = prefix + "dropped";
        String disappearedKey = prefix + "gone";
        when(oxiaClient.list(prefix, prefix + "\uffff")).thenReturn(
            CompletableFuture.completedFuture(List.of(
                droppedKey, creatingKey, disappearedKey, activeKey, deletingKey)));
        when(oxiaClient.get(activeKey)).thenReturn(CompletableFuture.completedFuture(
            new GetResult(activeKey, streamConfigBytes(
                1, Map.of("state", "active"), "active-incarnation", "active-owner", 1L,
                IndexedStreamConfigStore.NO_METADATA_GENERATION,
                IndexedStreamConfigStore.CreationKind.NATIVE_CREATE,
                IndexedStreamConfigStore.ProvisioningState.ACTIVE), version(10))));
        when(oxiaClient.get(creatingKey)).thenReturn(CompletableFuture.completedFuture(
            new GetResult(creatingKey, streamConfigBytes(
                1, Map.of("state", "creating"), "creating-incarnation", "creating-owner", 1L,
                IndexedStreamConfigStore.NO_METADATA_GENERATION,
                IndexedStreamConfigStore.CreationKind.NATIVE_CREATE,
                IndexedStreamConfigStore.ProvisioningState.PROVISIONING), version(11))));
        when(oxiaClient.get(deletingKey)).thenReturn(CompletableFuture.completedFuture(
            new GetResult(deletingKey, streamConfigBytes(
                1, Map.of("state", "deleting"), "deleting-incarnation", "delete-owner", 2L,
                "create-owner", 1L, IndexedStreamConfigStore.CreationKind.NATIVE_CREATE,
                IndexedStreamConfigStore.ProvisioningState.ABORTING), version(12))));
        when(oxiaClient.get(droppedKey)).thenReturn(CompletableFuture.completedFuture(
            new GetResult(droppedKey, streamConfigBytes(
                1, Map.of("state", "dropped"), "dropped-incarnation", "delete-owner", 2L,
                "create-owner", 1L, IndexedStreamConfigStore.CreationKind.NATIVE_CREATE,
                IndexedStreamConfigStore.ProvisioningState.DROPPED), version(13))));
        when(oxiaClient.get(disappearedKey)).thenReturn(CompletableFuture.completedFuture(null));

        List<StreamCatalogEntry> entries = store.listStreamEntries(id.namespace()).join();

        assertThat(entries).extracting(entry -> entry.identifier().name())
            .containsExactly("active", "creating", "deleting");
        assertThat(entries).extracting(StreamCatalogEntry::state)
            .containsExactly(LifecycleState.ACTIVE, LifecycleState.CREATING,
                LifecycleState.DELETING);
        assertThat(entries).extracting(StreamCatalogEntry::metadataVersion)
            .containsExactly(10L, 11L, 12L);
        assertThat(entries.get(1).properties()).containsEntry("state", "creating");
    }

    @Test
    void listStreamEntriesRejectsOldNonNativeMetadata() {
        String prefix = paths.streamConfigPrefix(id.namespace());
        String oldKey = prefix + "old";
        when(oxiaClient.list(prefix, prefix + "\uffff"))
            .thenReturn(CompletableFuture.completedFuture(List.of(oldKey)));
        when(oxiaClient.get(oldKey)).thenReturn(CompletableFuture.completedFuture(
            new GetResult(oldKey,
                ("{\"partitions\":1,\"properties\":{},"
                    + "\"_creationKind\":\"EXTERNAL\"}").getBytes(StandardCharsets.UTF_8),
                VERSION_1)));

        assertThatThrownBy(() -> store.listStreamEntries(id.namespace()).join())
            .isInstanceOf(CompletionException.class)
            .hasRootCauseInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void listStreamEntriesRejectsMetadataWithoutNativeMarker() {
        String prefix = paths.streamConfigPrefix(id.namespace());
        String oldKey = prefix + "old";
        when(oxiaClient.list(prefix, prefix + "\uffff"))
            .thenReturn(CompletableFuture.completedFuture(List.of(oldKey)));
        when(oxiaClient.get(oldKey)).thenReturn(CompletableFuture.completedFuture(
            new GetResult(oldKey,
                "{\"partitions\":1,\"properties\":{}}".getBytes(StandardCharsets.UTF_8),
                VERSION_1)));

        assertThatThrownBy(() -> store.listStreamEntries(id.namespace()).join())
            .isInstanceOf(CompletionException.class)
            .hasRootCauseInstanceOf(IllegalArgumentException.class);
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
    void concurrentExpansionClaimsPersistMaximumTarget() {
        byte[] active = streamConfigBytes(
            2, Map.of(), "incarnation", "create-owner", 1L,
            IndexedStreamConfigStore.NO_METADATA_GENERATION,
            IndexedStreamConfigStore.CreationKind.NATIVE_CREATE,
            IndexedStreamConfigStore.ProvisioningState.ACTIVE);
        ConcurrentReadRace race = mockConcurrentReads(active, 2);

        CompletableFuture<IndexedStreamConfigStore.ExpansionClaim> toThree =
            store.claimExpansion(id, 3);
        CompletableFuture<IndexedStreamConfigStore.ExpansionClaim> toFive =
            store.claimExpansion(id, 5);
        race.releaseInitialReads();

        assertThat(toThree.join().targetPartitions()).isGreaterThanOrEqualTo(3);
        assertThat(toFive.join().targetPartitions()).isEqualTo(5);
        JsonNode stored = json(race.state().get().value());
        assertThat(stored.path("partitions").asInt()).isEqualTo(2);
        assertThat(stored.path("_pendingExpansion").path("basePartitions").asInt())
            .isEqualTo(2);
        assertThat(stored.path("_pendingExpansion").path("targetPartitions").asInt())
            .isEqualTo(5);
    }

    @Test
    void propertyRevisionIsExactAndPreservesPendingExpansion() {
        byte[] active = streamConfigBytes(
            2, Map.of("old", "value", "remove", "me"),
            "incarnation", "create-owner", 1L,
            IndexedStreamConfigStore.NO_METADATA_GENERATION,
            IndexedStreamConfigStore.CreationKind.NATIVE_CREATE,
            IndexedStreamConfigStore.ProvisioningState.ACTIVE);
        AtomicReference<VersionedValue> state = mockVersionedRecord(active);
        IndexedStreamConfigStore.ExpansionClaim expansion =
            store.claimExpansion(id, 4).join();

        IndexedStreamConfigStore.ActiveStreamConfig replaced =
            store.replaceProperties(id, Map.of("current", "snapshot"), 10L).join();

        assertThat(replaced.config().properties())
            .containsExactlyEntriesOf(Map.of("current", "snapshot"));
        assertThat(replaced.config().propertiesSourceRevision()).isEqualTo(10L);
        assertThat(replaced.config().pendingExpansion()).contains(
            new IndexedStreamConfigStore.PendingExpansion(2, 4));
        Version afterReplacement = state.get().version();

        IndexedStreamConfigStore.ActiveStreamConfig stale =
            store.replaceProperties(id, Map.of("stale", "ignored"), 10L).join();

        assertThat(stale.versionId()).isEqualTo(afterReplacement.versionId());
        assertThat(stale.config().properties())
            .containsExactlyEntriesOf(Map.of("current", "snapshot"));

        IndexedStreamConfigStore.ExpansionFinalization finalized =
            store.finalizeExpansion(id, expansion).join();
        assertThat(finalized.complete()).isTrue();
        JsonNode committed = json(state.get().value());
        assertThat(committed.path("partitions").asInt()).isEqualTo(4);
        assertThat(committed.has("_pendingExpansion")).isFalse();
        assertThat(committed.path("properties"))
            .isEqualTo(json("{\"current\":\"snapshot\"}"
                .getBytes(StandardCharsets.UTF_8)));
        assertThat(committed.path("_propertiesSourceRevision").asLong())
            .isEqualTo(10L);
    }

    @Test
    void dropClaimAbsorbsPendingExpansionTargetForCleanup() {
        byte[] active = streamConfigBytes(
            2, Map.of(), "incarnation", "create-owner", 1L,
            IndexedStreamConfigStore.NO_METADATA_GENERATION,
            IndexedStreamConfigStore.CreationKind.NATIVE_CREATE,
            IndexedStreamConfigStore.ProvisioningState.ACTIVE);
        mockVersionedRecord(active);
        store.claimExpansion(id, 5).join();

        IndexedStreamConfigStore.DropClaim drop =
            store.beginDrop(id, "drop-owner").join().orElseThrow();

        assertThat(drop.config().partitions()).isEqualTo(5);
        assertThat(drop.config().definition().partitioning().numPartitions())
            .isEqualTo(5);
        assertThat(drop.config().pendingExpansion()).isEmpty();
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
            1, Map.of(), "incarnation", attempt, 2L, "create-owner", 1L,
            IndexedStreamConfigStore.CreationKind.NATIVE_CREATE,
            IndexedStreamConfigStore.ProvisioningState.ABORTING), version);
    }

    private GetResult nativeActive(
            Version version, int partitions, String ownerToken,
            long ownerGeneration, Map<String, String> properties) {
        return new GetResult(configPath, streamConfigBytes(
            partitions, properties, "incarnation", ownerToken, ownerGeneration,
            IndexedStreamConfigStore.NO_METADATA_GENERATION,
            IndexedStreamConfigStore.CreationKind.NATIVE_CREATE,
            IndexedStreamConfigStore.ProvisioningState.ACTIVE), version);
    }

    private static byte[] streamConfigBytes(
            int partitions, Map<String, String> properties,
            String incarnation, String ownerToken, long ownerGeneration,
            long metadataSourceGeneration,
            IndexedStreamConfigStore.CreationKind creationKind,
            IndexedStreamConfigStore.ProvisioningState state) {
        return streamConfigBytes(
            partitions, properties, incarnation, ownerToken, ownerGeneration,
            null, metadataSourceGeneration, creationKind, state);
    }

    private static byte[] streamConfigBytes(
            int partitions, Map<String, String> properties,
            String incarnation, String ownerToken, long ownerGeneration,
            String metadataSourceOwnerToken, long metadataSourceGeneration,
            IndexedStreamConfigStore.CreationKind creationKind,
            IndexedStreamConfigStore.ProvisioningState state) {
        ObjectNode node = MAPPER.createObjectNode();
        node.put("partitions", partitions);
        ObjectNode propertiesNode = node.putObject("properties");
        properties.forEach(propertiesNode::put);
        node.put("_incarnationId", incarnation);
        node.put("_ownerToken", ownerToken);
        node.put("_ownerGeneration", ownerGeneration);
        if (metadataSourceOwnerToken != null) {
            node.put("_metadataSourceOwnerToken", metadataSourceOwnerToken);
        }
        if (metadataSourceGeneration != IndexedStreamConfigStore.NO_METADATA_GENERATION) {
            node.put("_metadataSourceGeneration", metadataSourceGeneration);
        }
        node.put("_creationKind", creationKind.name());
        if (state != IndexedStreamConfigStore.ProvisioningState.ACTIVE) {
            node.put("_provisioning", true);
            node.put("_provisioningState", state.name());
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

    private void assertIncompatibleCreation(
            StreamConfig streamConfig, Partitioning partitioning, SchemaConfig schema) {
        assertThatThrownBy(() -> store.claimCreation(
                id, streamConfig, partitioning, schema, Map.of(), Optional.empty(),
                IndexedStreamConfigStore.CreationKind.NATIVE_CREATE, "attempt-2").join())
            .isInstanceOf(CompletionException.class)
            .hasCauseInstanceOf(AlreadyExistsException.class);
    }

    private static Partitioning indexedPartitioning(int partitions) {
        return new Partitioning(
            PartitioningStrategy.INDEXED,
            Map.of("numPartitions", String.valueOf(partitions)));
    }

    private void assertProvisioningOwnershipLost(
            IndexedStreamConfigStore.ProvisioningClaim claim) {
        assertThatThrownBy(() -> store.verifyProvisioningOwnership(id, claim).join())
            .isInstanceOf(CompletionException.class)
            .hasCauseInstanceOf(
                IndexedStreamConfigStore.ProvisioningOwnershipLostException.class);
    }

    private static void assertMetadataSourceOwner(
            byte[] value, String expectedOwnerToken, long expectedGeneration) {
        JsonNode config = json(value);
        assertThat(config.path("_metadataSourceOwnerToken").asText())
            .isEqualTo(expectedOwnerToken);
        assertThat(config.path("_metadataSourceGeneration").asLong())
            .isEqualTo(expectedGeneration);
    }

    private InitialClaimRace mockInitiallyAbsentClaimRace() {
        Set<PutOption> createOnly = Set.of(PutOption.IfRecordDoesNotExist);
        List<CompletableFuture<GetResult>> initialReads = List.of(
            new CompletableFuture<>(), new CompletableFuture<>());
        AtomicLong readCount = new AtomicLong();
        AtomicReference<VersionedValue> state = new AtomicReference<>();
        when(oxiaClient.get(configPath)).thenAnswer(ignored -> {
            int index = Math.toIntExact(readCount.getAndIncrement());
            if (index < initialReads.size()) {
                return initialReads.get(index);
            }
            VersionedValue current = state.get();
            return CompletableFuture.completedFuture(current == null ? null
                : new GetResult(configPath, current.value(), current.version()));
        });
        when(oxiaClient.put(eq(configPath), any(byte[].class), any()))
            .thenAnswer(invocation -> {
                byte[] value = invocation.getArgument(1, byte[].class);
                @SuppressWarnings("unchecked")
                Set<PutOption> options = invocation.getArgument(2, Set.class);
                if (options.equals(createOnly)) {
                    VersionedValue winner = new VersionedValue(value.clone(), VERSION_1);
                    if (state.compareAndSet(null, winner)) {
                        return CompletableFuture.completedFuture(
                            new PutResult(configPath, VERSION_1));
                    }
                    return CompletableFuture.failedFuture(
                        new KeyAlreadyExistsException(configPath));
                }
                VersionedValue current = state.get();
                if (current == null || !options.contains(
                        PutOption.IfVersionIdEquals(current.version().versionId()))) {
                    return CompletableFuture.failedFuture(
                        new UnexpectedVersionIdException(
                            configPath, current == null
                                ? -1L : current.version().versionId()));
                }
                Version next = version(current.version().versionId() + 1L);
                state.set(new VersionedValue(value.clone(), next));
                return CompletableFuture.completedFuture(new PutResult(configPath, next));
            });
        return new InitialClaimRace(initialReads, state);
    }

    private ConcurrentReadRace mockConcurrentReads(
            byte[] initialValue, int delayedReadCount) {
        AtomicReference<VersionedValue> state = new AtomicReference<>(
            new VersionedValue(initialValue.clone(), VERSION_1));
        AtomicLong nextVersion = new AtomicLong(VERSION_1.versionId());
        List<CompletableFuture<GetResult>> initialReads = new ArrayList<>();
        for (int index = 0; index < delayedReadCount; index++) {
            initialReads.add(new CompletableFuture<>());
        }
        AtomicInteger readCount = new AtomicInteger();
        when(oxiaClient.get(configPath)).thenAnswer(ignored -> {
            int index = readCount.getAndIncrement();
            if (index < initialReads.size()) {
                return initialReads.get(index);
            }
            VersionedValue current = state.get();
            return CompletableFuture.completedFuture(new GetResult(
                configPath, current.value(), current.version()));
        });
        when(oxiaClient.put(eq(configPath), any(byte[].class), any()))
            .thenAnswer(invocation -> {
                @SuppressWarnings("unchecked")
                Set<PutOption> options = invocation.getArgument(2, Set.class);
                VersionedValue current = state.get();
                if (!options.contains(
                        PutOption.IfVersionIdEquals(current.version().versionId()))) {
                    return CompletableFuture.failedFuture(
                        new UnexpectedVersionIdException(
                            configPath, current.version().versionId()));
                }
                Version next = version(nextVersion.incrementAndGet());
                byte[] value = invocation.getArgument(1, byte[].class);
                state.set(new VersionedValue(value.clone(), next));
                return CompletableFuture.completedFuture(new PutResult(configPath, next));
            });
        return new ConcurrentReadRace(initialReads, state);
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

    private record InitialClaimRace(
            List<CompletableFuture<GetResult>> initialReads,
            AtomicReference<VersionedValue> state) {

        private void releaseInitialReads() {
            initialReads.forEach(read -> read.complete(null));
        }
    }

    private record ConcurrentReadRace(
            List<CompletableFuture<GetResult>> initialReads,
            AtomicReference<VersionedValue> state) {

        private void releaseInitialReads() {
            VersionedValue snapshot = state.get();
            initialReads.forEach(read -> read.complete(new GetResult(
                "ignored", snapshot.value(), snapshot.version())));
        }
    }

    private static Version version(long id) {
        return new Version(id, 0, 0, 0, Optional.empty(), Optional.empty());
    }
}
