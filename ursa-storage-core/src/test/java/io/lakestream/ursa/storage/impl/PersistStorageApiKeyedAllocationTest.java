/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.storage.impl;

import static io.lakestream.ursa.storage.impl.StorageFormat.STREAM_ID_GENERATOR_PATH;
import static io.lakestream.ursa.storage.impl.StorageFormat.STREAM_REGISTER_PATH;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anySet;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import io.lakestream.api.LogStateManager;
import io.lakestream.ursa.metrics.InstrumentProvider;
import io.lakestream.ursa.storage.StorageApi.ActiveStreamIdMapping;
import io.lakestream.ursa.storage.StorageApi.KeyedAllocationInvalidatedException;
import io.lakestream.ursa.storage.StorageApi.StreamIdAllocation;
import io.lakestream.ursa.storage.StorageApi.StreamIdMappingConflictException;
import io.lakestream.ursa.storage.StorageApi.StreamIdMappingFence;
import io.lakestream.ursa.storage.StorageApi.StreamIdMappingFenceResult;
import io.lakestream.ursa.storage.StorageApi.StreamIdMappingOwner;
import io.lakestream.ursa.storage.WalStorage;
import io.lakestream.ursa.storage.impl.exception.NoSuchKeyException;
import io.oxia.client.api.AsyncOxiaClient;
import io.oxia.client.api.GetResult;
import io.oxia.client.api.PutResult;
import io.oxia.client.api.Version;
import io.oxia.client.api.exceptions.KeyAlreadyExistsException;
import io.oxia.client.api.exceptions.UnexpectedVersionIdException;
import io.oxia.client.api.options.GetOption;
import io.oxia.client.api.options.PutOption;
import java.nio.charset.StandardCharsets;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

class PersistStorageApiKeyedAllocationTest {

    private static final Version VERSION =
        new Version(1, 0, 0, 0, Optional.empty(), Optional.empty());

    @Test
    void existingMappingDoesNotReturnUntilStreamRegistrationIsDurable() {
        String key = "topic-partition-0";
        long streamId = 41L;
        String mappingPath = STREAM_ID_GENERATOR_PATH + "/" + key;
        String registrationPath = STREAM_REGISTER_PATH + "/" + streamId;
        AsyncOxiaClient oxiaClient = mock(AsyncOxiaClient.class);
        CompletableFuture<PutResult> registration = new CompletableFuture<>();
        when(oxiaClient.get(mappingPath,
                Set.of(GetOption.PartitionKey(STREAM_ID_GENERATOR_PATH))))
            .thenReturn(CompletableFuture.completedFuture(new GetResult(
                mappingPath, Long.toString(streamId).getBytes(StandardCharsets.UTF_8), VERSION)));
        when(oxiaClient.put(eq(registrationPath), any(byte[].class),
                eq(Set.of(PutOption.IfRecordDoesNotExist))))
            .thenReturn(registration);
        PersistStorageApi storageApi = storageApi(oxiaClient);

        CompletableFuture<StreamIdAllocation> allocation =
            storageApi.allocateStreamId(Optional.of(key));

        assertThat(allocation).isNotDone();
        registration.complete(new PutResult(registrationPath, VERSION));
        assertThat(allocation.join())
            .isEqualTo(new StreamIdAllocation(streamId, false));
    }

    @Test
    void keyAlreadyExistsUsesAndRegistersTheWinningMapping() {
        String key = "topic-partition-race";
        long generatedStreamId = 101L;
        long winningStreamId = 202L;
        String mappingPath = STREAM_ID_GENERATOR_PATH + "/" + key;
        String winningRegistrationPath = STREAM_REGISTER_PATH + "/" + winningStreamId;
        AsyncOxiaClient oxiaClient = mock(AsyncOxiaClient.class);
        when(oxiaClient.get(eq(mappingPath), anySet()))
            .thenReturn(
                CompletableFuture.completedFuture(null),
                CompletableFuture.completedFuture(mapping(mappingPath, winningStreamId)),
                CompletableFuture.completedFuture(mapping(mappingPath, winningStreamId)));
        when(oxiaClient.put(eq(STREAM_ID_GENERATOR_PATH), any(byte[].class)))
            .thenReturn(CompletableFuture.completedFuture(
                new PutResult(STREAM_ID_GENERATOR_PATH, version(generatedStreamId))));
        when(oxiaClient.put(eq(mappingPath), any(byte[].class), anySet()))
            .thenReturn(CompletableFuture.failedFuture(
                new KeyAlreadyExistsException(mappingPath)));
        when(oxiaClient.put(eq(winningRegistrationPath), any(byte[].class),
                eq(Set.of(PutOption.IfRecordDoesNotExist))))
            .thenReturn(CompletableFuture.completedFuture(
                new PutResult(winningRegistrationPath, VERSION)));

        StreamIdAllocation allocation = storageApi(oxiaClient)
            .allocateStreamId(Optional.of(key))
            .join();

        assertThat(allocation)
            .isEqualTo(new StreamIdAllocation(winningStreamId, false));
        verify(oxiaClient, times(4)).get(eq(mappingPath), anySet());
        verify(oxiaClient).put(eq(winningRegistrationPath), any(byte[].class),
            eq(Set.of(PutOption.IfRecordDoesNotExist)));
    }

    @Test
    void ambiguousMappingWriteAcceptsOwnReadbackAndRegistersIt() {
        String key = "topic-partition-ambiguous-mapping";
        long generatedStreamId = 303L;
        String mappingPath = STREAM_ID_GENERATOR_PATH + "/" + key;
        String registrationPath = STREAM_REGISTER_PATH + "/" + generatedStreamId;
        AsyncOxiaClient oxiaClient = mock(AsyncOxiaClient.class);
        when(oxiaClient.get(eq(mappingPath), anySet()))
            .thenReturn(
                CompletableFuture.completedFuture(null),
                CompletableFuture.completedFuture(mapping(mappingPath, generatedStreamId)));
        when(oxiaClient.put(eq(STREAM_ID_GENERATOR_PATH), any(byte[].class)))
            .thenReturn(CompletableFuture.completedFuture(
                new PutResult(STREAM_ID_GENERATOR_PATH, version(generatedStreamId))));
        when(oxiaClient.put(eq(mappingPath), any(byte[].class), anySet()))
            .thenReturn(CompletableFuture.failedFuture(
                new RuntimeException("ambiguous mapping write")));
        when(oxiaClient.put(eq(registrationPath), any(byte[].class),
                eq(Set.of(PutOption.IfRecordDoesNotExist))))
            .thenReturn(CompletableFuture.completedFuture(
                new PutResult(registrationPath, VERSION)));

        StreamIdAllocation allocation = storageApi(oxiaClient)
            .allocateStreamId(Optional.of(key))
            .join();

        assertThat(allocation)
            .isEqualTo(new StreamIdAllocation(generatedStreamId, true));
        verify(oxiaClient, times(3)).get(eq(mappingPath), anySet());
        verify(oxiaClient).put(eq(registrationPath), any(byte[].class),
            eq(Set.of(PutOption.IfRecordDoesNotExist)));
    }

    @Test
    void keyedAllocationStopsAfterBoundedMappingConflicts() {
        String key = "topic-partition-persistent-conflict";
        String mappingPath = STREAM_ID_GENERATOR_PATH + "/" + key;
        AsyncOxiaClient oxiaClient = mock(AsyncOxiaClient.class);
        when(oxiaClient.get(eq(mappingPath), anySet()))
            .thenReturn(CompletableFuture.completedFuture(null));
        when(oxiaClient.put(eq(STREAM_ID_GENERATOR_PATH), any(byte[].class)))
            .thenReturn(CompletableFuture.completedFuture(
                new PutResult(STREAM_ID_GENERATOR_PATH, version(909L))));
        when(oxiaClient.put(eq(mappingPath), any(byte[].class), anySet()))
            .thenReturn(CompletableFuture.failedFuture(
                new KeyAlreadyExistsException(mappingPath)));

        CompletableFuture<StreamIdAllocation> allocation = storageApi(oxiaClient)
            .allocateStreamId(Optional.of(key));

        CompletionException failure = org.junit.jupiter.api.Assertions.assertThrows(
            CompletionException.class, allocation::join);
        assertThat(failure.getCause())
            .isInstanceOf(IllegalStateException.class)
            .hasMessageContaining("Keyed stream-ID allocation exhausted retries");
        verify(oxiaClient, times(4))
            .put(eq(mappingPath), any(byte[].class), anySet());
    }

    @Test
    void ambiguousRegistrationWriteAcceptsMatchingReadback() {
        String key = "topic-partition-ambiguous-registration";
        long streamId = 404L;
        String mappingPath = STREAM_ID_GENERATOR_PATH + "/" + key;
        String registrationPath = STREAM_REGISTER_PATH + "/" + streamId;
        AsyncOxiaClient oxiaClient = mock(AsyncOxiaClient.class);
        when(oxiaClient.get(eq(mappingPath), anySet()))
            .thenReturn(CompletableFuture.completedFuture(mapping(mappingPath, streamId)));
        when(oxiaClient.put(eq(registrationPath), any(byte[].class),
                eq(Set.of(PutOption.IfRecordDoesNotExist))))
            .thenReturn(CompletableFuture.failedFuture(
                new RuntimeException("ambiguous registration write")));
        when(oxiaClient.get(registrationPath))
            .thenReturn(CompletableFuture.completedFuture(new GetResult(
                registrationPath,
                ("{\"key\":\"" + key + "\"}").getBytes(StandardCharsets.UTF_8),
                VERSION)));

        long allocatedStreamId = storageApi(oxiaClient)
            .generateStreamId(Optional.of(key))
            .join();

        assertThat(allocatedStreamId).isEqualTo(streamId);
        verify(oxiaClient).get(registrationPath);
    }

    @Test
    void invalidatedMappingPreservesRegistrationCreatedByAllocation() {
        String key = "topic-partition-invalidated-registration";
        long streamId = 405L;
        String mappingPath = STREAM_ID_GENERATOR_PATH + "/" + key;
        String registrationPath = STREAM_REGISTER_PATH + "/" + streamId;
        AsyncOxiaClient oxiaClient = mock(AsyncOxiaClient.class);
        when(oxiaClient.get(eq(mappingPath), anySet()))
            .thenReturn(
                CompletableFuture.completedFuture(mapping(mappingPath, streamId)),
                CompletableFuture.completedFuture(null));
        when(oxiaClient.put(eq(registrationPath), any(byte[].class),
                eq(Set.of(PutOption.IfRecordDoesNotExist))))
            .thenReturn(CompletableFuture.completedFuture(
                new PutResult(registrationPath, version(17L))));

        CompletionException failure = org.junit.jupiter.api.Assertions.assertThrows(
            CompletionException.class,
            () -> storageApi(oxiaClient).allocateStreamId(Optional.of(key)).join());

        assertThat(failure.getCause())
            .isInstanceOf(KeyedAllocationInvalidatedException.class);
        KeyedAllocationInvalidatedException invalidated =
            (KeyedAllocationInvalidatedException) failure.getCause();
        assertThat(invalidated.allocation())
            .isEqualTo(new StreamIdAllocation(streamId, false));
        assertThat(invalidated.getCause())
            .isInstanceOf(IllegalStateException.class)
            .hasMessageContaining("mapping changed while registering");
        verify(oxiaClient, never()).delete(eq(registrationPath), anySet());
    }

    @Test
    void finalValidationFailurePreservesCreatedAllocationAndOriginalCause() {
        String key = "topic-partition-validation-failure";
        long streamId = 408L;
        String mappingPath = STREAM_ID_GENERATOR_PATH + "/" + key;
        String registrationPath = STREAM_REGISTER_PATH + "/" + streamId;
        RuntimeException validationFailure = new RuntimeException("validation unavailable");
        AsyncOxiaClient oxiaClient = mock(AsyncOxiaClient.class);
        when(oxiaClient.get(eq(mappingPath), anySet()))
            .thenReturn(
                CompletableFuture.completedFuture(null),
                CompletableFuture.failedFuture(validationFailure));
        when(oxiaClient.put(eq(STREAM_ID_GENERATOR_PATH), any(byte[].class)))
            .thenReturn(CompletableFuture.completedFuture(
                new PutResult(STREAM_ID_GENERATOR_PATH, version(streamId))));
        when(oxiaClient.put(eq(mappingPath), any(byte[].class), anySet()))
            .thenReturn(CompletableFuture.completedFuture(
                new PutResult(mappingPath, version(16L))));
        when(oxiaClient.put(eq(registrationPath), any(byte[].class),
                eq(Set.of(PutOption.IfRecordDoesNotExist))))
            .thenReturn(CompletableFuture.completedFuture(
                new PutResult(registrationPath, version(17L))));

        CompletionException failure = org.junit.jupiter.api.Assertions.assertThrows(
            CompletionException.class,
            () -> storageApi(oxiaClient).allocateStreamId(Optional.of(key)).join());

        assertThat(failure.getCause())
            .isInstanceOf(KeyedAllocationInvalidatedException.class);
        KeyedAllocationInvalidatedException invalidated =
            (KeyedAllocationInvalidatedException) failure.getCause();
        assertThat(invalidated.allocation())
            .isEqualTo(new StreamIdAllocation(streamId, true));
        assertThat(invalidated.getCause()).isSameAs(validationFailure);
        verify(oxiaClient, never()).delete(eq(registrationPath), anySet());
    }

    @Test
    void invalidatedCreatorDoesNotDeleteRegistrationReusedByConcurrentAllocator() {
        String key = "topic-partition-concurrent-registration";
        long streamId = 407L;
        String mappingPath = STREAM_ID_GENERATOR_PATH + "/" + key;
        String registrationPath = STREAM_REGISTER_PATH + "/" + streamId;
        AsyncOxiaClient oxiaClient = mock(AsyncOxiaClient.class);
        CompletableFuture<PutResult> creatorRegistration = new CompletableFuture<>();
        when(oxiaClient.get(eq(mappingPath), anySet()))
            .thenReturn(
                CompletableFuture.completedFuture(mapping(mappingPath, streamId)),
                CompletableFuture.completedFuture(mapping(mappingPath, streamId)),
                CompletableFuture.completedFuture(mapping(mappingPath, streamId)),
                CompletableFuture.completedFuture(null));
        when(oxiaClient.put(eq(registrationPath), any(byte[].class),
                eq(Set.of(PutOption.IfRecordDoesNotExist))))
            .thenReturn(
                creatorRegistration,
                CompletableFuture.failedFuture(
                    new KeyAlreadyExistsException(registrationPath)));
        when(oxiaClient.get(registrationPath))
            .thenReturn(CompletableFuture.completedFuture(new GetResult(
                registrationPath,
                ("{\"key\":\"" + key + "\"}").getBytes(StandardCharsets.UTF_8),
                VERSION)));
        PersistStorageApi storageApi = storageApi(oxiaClient);

        CompletableFuture<StreamIdAllocation> creator =
            storageApi.allocateStreamId(Optional.of(key));
        assertThat(creator).isNotDone();
        assertThat(storageApi.allocateStreamId(Optional.of(key)).join())
            .isEqualTo(new StreamIdAllocation(streamId, false));

        creatorRegistration.complete(new PutResult(registrationPath, version(18L)));

        Throwable failure = org.junit.jupiter.api.Assertions.assertThrows(
            CompletionException.class, creator::join).getCause();
        assertThat(failure).isInstanceOf(KeyedAllocationInvalidatedException.class);
        KeyedAllocationInvalidatedException invalidated =
            (KeyedAllocationInvalidatedException) failure;
        assertThat(invalidated.allocation())
            .isEqualTo(new StreamIdAllocation(streamId, false));
        assertThat(invalidated.getCause())
            .isInstanceOf(IllegalStateException.class)
            .hasMessageContaining("mapping changed while registering");
        verify(oxiaClient, never()).delete(eq(registrationPath), anySet());
    }

    @Test
    void invalidatedMappingDoesNotDeletePreexistingRegistration() {
        String key = "topic-partition-invalidated-existing-registration";
        long streamId = 406L;
        String mappingPath = STREAM_ID_GENERATOR_PATH + "/" + key;
        String registrationPath = STREAM_REGISTER_PATH + "/" + streamId;
        AsyncOxiaClient oxiaClient = mock(AsyncOxiaClient.class);
        when(oxiaClient.get(eq(mappingPath), anySet()))
            .thenReturn(
                CompletableFuture.completedFuture(mapping(mappingPath, streamId)),
                CompletableFuture.completedFuture(null));
        when(oxiaClient.put(eq(registrationPath), any(byte[].class),
                eq(Set.of(PutOption.IfRecordDoesNotExist))))
            .thenReturn(CompletableFuture.failedFuture(
                new KeyAlreadyExistsException(registrationPath)));
        when(oxiaClient.get(registrationPath))
            .thenReturn(CompletableFuture.completedFuture(new GetResult(
                registrationPath,
                ("{\"key\":\"" + key + "\"}").getBytes(StandardCharsets.UTF_8),
                VERSION)));

        Throwable failure = org.junit.jupiter.api.Assertions.assertThrows(
            CompletionException.class,
            () -> storageApi(oxiaClient).allocateStreamId(Optional.of(key)).join()).getCause();
        assertThat(failure).isInstanceOf(KeyedAllocationInvalidatedException.class);
        KeyedAllocationInvalidatedException invalidated =
            (KeyedAllocationInvalidatedException) failure;
        assertThat(invalidated.allocation())
            .isEqualTo(new StreamIdAllocation(streamId, false));
        assertThat(invalidated.getCause())
            .isInstanceOf(IllegalStateException.class)
            .hasMessageContaining("mapping changed while registering");
        verify(oxiaClient, never()).delete(eq(registrationPath), anySet());
    }

    @Test
    void conditionalDeleteWritesTombstoneInsteadOfDeletingMapping() {
        String key = "topic-partition-delete-tombstone";
        long streamId = 505L;
        String mappingPath = STREAM_ID_GENERATOR_PATH + "/" + key;
        AsyncOxiaClient oxiaClient = mock(AsyncOxiaClient.class);
        when(oxiaClient.get(eq(mappingPath), anySet()))
            .thenReturn(CompletableFuture.completedFuture(mapping(mappingPath, streamId)));
        when(oxiaClient.put(eq(mappingPath), any(byte[].class), anySet()))
            .thenReturn(CompletableFuture.completedFuture(
                new PutResult(mappingPath, version(2L))));

        storageApi(oxiaClient).deleteStreamIdMapping(key, streamId).join();

        ArgumentCaptor<byte[]> value = ArgumentCaptor.forClass(byte[].class);
        verify(oxiaClient).put(eq(mappingPath), value.capture(), anySet());
        assertThat(new String(value.getValue(), StandardCharsets.UTF_8))
            .contains("\"state\":\"TOMBSTONE\"")
            .contains("\"streamId\":" + streamId)
            .contains("\"ownerToken\":\"__legacy__\"");
        verify(oxiaClient, never()).delete(eq(mappingPath), anySet());
    }

    @Test
    @SuppressWarnings("deprecation")
    void legacyUnconditionalDeleteFailsClosedWithoutIo() {
        String key = "topic-partition-unconditional-delete";
        String mappingPath = STREAM_ID_GENERATOR_PATH + "/" + key;
        AsyncOxiaClient oxiaClient = mock(AsyncOxiaClient.class);

        CompletionException failure = org.junit.jupiter.api.Assertions.assertThrows(
            CompletionException.class,
            () -> storageApi(oxiaClient).deleteStreamIdMapping(key).join());

        assertThat(failure.getCause()).isInstanceOf(UnsupportedOperationException.class);
        verify(oxiaClient, never()).get(eq(mappingPath), anySet());
        verify(oxiaClient, never()).delete(eq(mappingPath), anySet());
    }

    @Test
    @SuppressWarnings("deprecation")
    void legacyKeyedStreamDeleteFailsClosedWithoutIo() {
        String key = "topic-partition-unsafe-keyed-delete";
        AsyncOxiaClient oxiaClient = mock(AsyncOxiaClient.class);

        CompletionException failure = org.junit.jupiter.api.Assertions.assertThrows(
            CompletionException.class,
            () -> storageApi(oxiaClient).deleteStream(707L, Optional.of(key)).join());

        assertThat(failure.getCause())
            .isInstanceOf(UnsupportedOperationException.class)
            .hasMessageContaining("durable lifecycle API");
        verifyNoInteractions(oxiaClient);
    }

    @Test
    void missingMappingCreatesStableAbsentTombstone() {
        String key = "topic-partition-delete-absent";
        String mappingPath = STREAM_ID_GENERATOR_PATH + "/" + key;
        AsyncOxiaClient oxiaClient = mock(AsyncOxiaClient.class);
        when(oxiaClient.get(eq(mappingPath), anySet()))
            .thenReturn(CompletableFuture.completedFuture(null));
        when(oxiaClient.put(eq(mappingPath), any(byte[].class), anySet()))
            .thenReturn(CompletableFuture.completedFuture(
                new PutResult(mappingPath, version(2L))));

        Optional<StreamIdMappingFence> fence = storageApi(oxiaClient)
            .fenceStreamIdMapping(key, 606L, owner("incarnation-a", "owner-a", 1L))
            .join();

        assertThat(fence).contains(new StreamIdMappingFence(
            -1L, owner("incarnation-a", "owner-a", 1L)));
        ArgumentCaptor<byte[]> value = ArgumentCaptor.forClass(byte[].class);
        verify(oxiaClient).put(eq(mappingPath), value.capture(), anySet());
        assertThat(new String(value.getValue(), StandardCharsets.UTF_8))
            .contains("\"streamId\":-1")
            .contains("\"ownerToken\":\"owner-a\"");
    }

    @Test
    void fenceAdoptsExactLegacyMappingForExpectedOwner() {
        String key = "topic-partition-fence-legacy";
        long streamId = 707L;
        String mappingPath = STREAM_ID_GENERATOR_PATH + "/" + key;
        StreamIdMappingOwner owner = owner("incarnation-a", "owner-a", 2L);
        AsyncOxiaClient oxiaClient = mock(AsyncOxiaClient.class);
        when(oxiaClient.get(eq(mappingPath), anySet()))
            .thenReturn(CompletableFuture.completedFuture(mapping(mappingPath, streamId)));
        when(oxiaClient.put(eq(mappingPath), any(byte[].class), anySet()))
            .thenReturn(CompletableFuture.completedFuture(
                new PutResult(mappingPath, version(2L))));

        Optional<StreamIdMappingFence> fence = storageApi(oxiaClient)
            .fenceStreamIdMapping(key, streamId, owner)
            .join();

        assertThat(fence).contains(new StreamIdMappingFence(streamId, owner));
    }

    @Test
    void legacyOwnerCanFenceAnExactHistoricalNumericMapping() {
        String key = "topic-partition-fence-historical-legacy";
        long streamId = 708L;
        String mappingPath = STREAM_ID_GENERATOR_PATH + "/" + key;
        StreamIdMappingOwner legacyOwner = StreamIdMappingOwner.legacy();
        AsyncOxiaClient oxiaClient = mock(AsyncOxiaClient.class);
        when(oxiaClient.get(eq(mappingPath), anySet()))
            .thenReturn(CompletableFuture.completedFuture(mapping(mappingPath, streamId)));
        when(oxiaClient.put(eq(mappingPath), any(byte[].class), anySet()))
            .thenReturn(CompletableFuture.completedFuture(
                new PutResult(mappingPath, version(2L))));

        StreamIdMappingFenceResult result = storageApi(oxiaClient)
            .fenceStreamIdMappingState(key, streamId, legacyOwner)
            .join();

        assertThat(result).isEqualTo(new StreamIdMappingFenceResult.Fenced(
            new StreamIdMappingFence(streamId, legacyOwner)));
    }

    @Test
    void conditionalFenceDoesNotOverwriteDifferentActiveOwner() {
        String key = "topic-partition-different-owner";
        long streamId = 808L;
        String mappingPath = STREAM_ID_GENERATOR_PATH + "/" + key;
        StreamIdMappingOwner currentOwner = owner("incarnation-a", "owner-a", 3L);
        StreamIdMappingOwner expectedOwner = owner("incarnation-b", "owner-b", 1L);
        AsyncOxiaClient oxiaClient = mock(AsyncOxiaClient.class);
        when(oxiaClient.get(eq(mappingPath), anySet()))
            .thenReturn(CompletableFuture.completedFuture(
                versionedMapping(mappingPath, "ACTIVE", streamId, currentOwner)));

        Optional<StreamIdMappingFence> fence = storageApi(oxiaClient)
            .fenceStreamIdMapping(key, streamId, expectedOwner)
            .join();

        assertThat(fence).isEmpty();
        verify(oxiaClient, never()).put(eq(mappingPath), any(byte[].class), anySet());
    }

    @Test
    void absentFenceCapturesLateAllocationFromTheSameOwner() {
        String key = "topic-partition-late-same-owner";
        long lateStreamId = 809L;
        String mappingPath = STREAM_ID_GENERATOR_PATH + "/" + key;
        StreamIdMappingOwner owner = owner("incarnation-a", "owner-a", 4L);
        GetResult lateActive = versionedMapping(
            mappingPath, "ACTIVE", lateStreamId, owner);
        AsyncOxiaClient oxiaClient = mock(AsyncOxiaClient.class);
        when(oxiaClient.get(eq(mappingPath), anySet()))
            .thenReturn(
                CompletableFuture.completedFuture(null),
                CompletableFuture.completedFuture(lateActive),
                CompletableFuture.completedFuture(lateActive));
        when(oxiaClient.put(eq(mappingPath), any(byte[].class), anySet()))
            .thenReturn(
                CompletableFuture.failedFuture(new KeyAlreadyExistsException(mappingPath)),
                CompletableFuture.completedFuture(
                    new PutResult(mappingPath, version(2L))));

        StreamIdMappingFenceResult result = storageApi(oxiaClient)
            .fenceStreamIdMappingState(key, -1L, owner)
            .join();

        assertThat(result).isEqualTo(new StreamIdMappingFenceResult.Fenced(
            new StreamIdMappingFence(lateStreamId, owner)));
        ArgumentCaptor<byte[]> values = ArgumentCaptor.forClass(byte[].class);
        verify(oxiaClient, times(2)).put(eq(mappingPath), values.capture(), anySet());
        assertThat(new String(values.getAllValues().get(0), StandardCharsets.UTF_8))
            .contains("\"state\":\"TOMBSTONE\"")
            .contains("\"streamId\":-1");
        assertThat(new String(values.getAllValues().get(1), StandardCharsets.UTF_8))
            .contains("\"state\":\"TOMBSTONE\"")
            .contains("\"streamId\":" + lateStreamId)
            .contains("\"ownerToken\":\"owner-a\"");
    }

    @Test
    void richerFenceResultDescribesPreservedActiveMapping() {
        String key = "topic-partition-preserved-active";
        long streamId = 810L;
        String mappingPath = STREAM_ID_GENERATOR_PATH + "/" + key;
        StreamIdMappingOwner currentOwner = owner("incarnation-a", "owner-a", 5L);
        StreamIdMappingOwner expectedOwner = owner("incarnation-b", "owner-b", 1L);
        AsyncOxiaClient oxiaClient = mock(AsyncOxiaClient.class);
        when(oxiaClient.get(eq(mappingPath), anySet()))
            .thenReturn(CompletableFuture.completedFuture(
                versionedMapping(mappingPath, "ACTIVE", streamId, currentOwner)));

        StreamIdMappingFenceResult result = storageApi(oxiaClient)
            .fenceStreamIdMappingState(key, streamId, expectedOwner)
            .join();

        assertThat(result).isEqualTo(new StreamIdMappingFenceResult.PreservedActive(
            new ActiveStreamIdMapping(streamId, currentOwner)));
        verify(oxiaClient, never()).put(eq(mappingPath), any(byte[].class), anySet());
    }

    @Test
    void absentFencePreservesUnexpectedStreamIdFromTheSameOwner() {
        String key = "topic-partition-same-owner-unexpected-id";
        long expectedStreamId = 811L;
        long activeStreamId = 812L;
        String mappingPath = STREAM_ID_GENERATOR_PATH + "/" + key;
        StreamIdMappingOwner owner = owner("incarnation-a", "owner-a", 6L);
        AsyncOxiaClient oxiaClient = mock(AsyncOxiaClient.class);
        when(oxiaClient.get(eq(mappingPath), anySet()))
            .thenReturn(CompletableFuture.completedFuture(
                versionedMapping(mappingPath, "ACTIVE", activeStreamId, owner)));

        StreamIdMappingFenceResult result = storageApi(oxiaClient)
            .fenceStreamIdMappingState(key, expectedStreamId, owner)
            .join();

        assertThat(result).isEqualTo(new StreamIdMappingFenceResult.PreservedActive(
            new ActiveStreamIdMapping(activeStreamId, owner)));
        verify(oxiaClient, never()).put(eq(mappingPath), any(byte[].class), anySet());
    }

    @Test
    void allocationConflictReportsTheActualActiveMapping() {
        String key = "topic-partition-typed-conflict";
        long streamId = 811L;
        String mappingPath = STREAM_ID_GENERATOR_PATH + "/" + key;
        StreamIdMappingOwner currentOwner = owner("incarnation-a", "owner-a", 6L);
        StreamIdMappingOwner expectedOwner = owner("incarnation-b", "owner-b", 1L);
        AsyncOxiaClient oxiaClient = mock(AsyncOxiaClient.class);
        when(oxiaClient.get(eq(mappingPath), anySet()))
            .thenReturn(CompletableFuture.completedFuture(
                versionedMapping(mappingPath, "ACTIVE", streamId, currentOwner)));

        CompletionException failure = org.junit.jupiter.api.Assertions.assertThrows(
            CompletionException.class,
            () -> storageApi(oxiaClient)
                .allocateStreamId(key, expectedOwner, Optional.empty()).join());

        assertThat(failure.getCause()).isInstanceOf(StreamIdMappingConflictException.class);
        StreamIdMappingConflictException conflict =
            (StreamIdMappingConflictException) failure.getCause();
        assertThat(conflict.key()).isEqualTo(key);
        assertThat(conflict.activeMapping())
            .isEqualTo(new ActiveStreamIdMapping(streamId, currentOwner));
    }

    @Test
    void legacyAllocatorCannotPublishAcrossTombstone() {
        String key = "topic-partition-fenced-legacy-allocator";
        long streamId = 909L;
        String mappingPath = STREAM_ID_GENERATOR_PATH + "/" + key;
        StreamIdMappingFence fence = new StreamIdMappingFence(
            streamId, owner("incarnation-a", "owner-a", 4L));
        AsyncOxiaClient oxiaClient = mock(AsyncOxiaClient.class);
        when(oxiaClient.get(eq(mappingPath), anySet()))
            .thenReturn(CompletableFuture.completedFuture(
                versionedMapping(mappingPath, "TOMBSTONE", fence.streamId(), fence.owner())));

        CompletionException failure = org.junit.jupiter.api.Assertions.assertThrows(
            CompletionException.class,
            () -> storageApi(oxiaClient).allocateStreamId(Optional.of(key)).join());

        assertThat(failure.getCause())
            .isInstanceOf(IllegalStateException.class)
            .hasMessageContaining("mapping is fenced");
        verify(oxiaClient, never()).put(eq(mappingPath), any(byte[].class), anySet());
    }

    @Test
    void tombstoneIsAbsentFromThePublicKeyLookup() {
        String key = "topic-partition-tombstone-lookup";
        String mappingPath = STREAM_ID_GENERATOR_PATH + "/" + key;
        StreamIdMappingFence fence = new StreamIdMappingFence(
            909L, owner("incarnation-a", "owner-a", 5L));
        AsyncOxiaClient oxiaClient = mock(AsyncOxiaClient.class);
        when(oxiaClient.get(eq(mappingPath), anySet()))
            .thenReturn(CompletableFuture.completedFuture(versionedMapping(
                mappingPath, "TOMBSTONE", fence.streamId(), fence.owner())));

        CompletionException failure = org.junit.jupiter.api.Assertions.assertThrows(
            CompletionException.class,
            () -> storageApi(oxiaClient).getStreamIdByKey(key).join());

        assertThat(failure.getCause()).isInstanceOf(NoSuchKeyException.class);
    }

    @Test
    void lifecycleAllocatorCannotPublishAfterConcurrentAbsentFenceWins() {
        String key = "topic-partition-fence-wins-allocation-race";
        long generatedStreamId = 910L;
        String mappingPath = STREAM_ID_GENERATOR_PATH + "/" + key;
        String registrationPath = STREAM_REGISTER_PATH + "/" + generatedStreamId;
        StreamIdMappingOwner owner = owner("incarnation-a", "owner-a", 5L);
        StreamIdMappingFence fence = new StreamIdMappingFence(-1L, owner);
        GetResult tombstone = versionedMapping(
            mappingPath, "TOMBSTONE", fence.streamId(), fence.owner());
        AsyncOxiaClient oxiaClient = mock(AsyncOxiaClient.class);
        when(oxiaClient.get(eq(mappingPath), anySet()))
            .thenReturn(
                CompletableFuture.completedFuture(null),
                CompletableFuture.completedFuture(tombstone),
                CompletableFuture.completedFuture(tombstone));
        when(oxiaClient.put(eq(STREAM_ID_GENERATOR_PATH), any(byte[].class)))
            .thenReturn(CompletableFuture.completedFuture(
                new PutResult(STREAM_ID_GENERATOR_PATH, version(generatedStreamId))));
        when(oxiaClient.put(eq(mappingPath), any(byte[].class), anySet()))
            .thenReturn(CompletableFuture.failedFuture(
                new KeyAlreadyExistsException(mappingPath)));

        CompletionException failure = org.junit.jupiter.api.Assertions.assertThrows(
            CompletionException.class,
            () -> storageApi(oxiaClient)
                .allocateStreamId(key, owner, Optional.empty()).join());

        assertThat(failure.getCause())
            .isInstanceOf(IllegalStateException.class)
            .hasMessageContaining("tombstone was not acknowledged");
        verify(oxiaClient, never()).put(eq(registrationPath), any(byte[].class), anySet());
    }

    @Test
    void sameOwnerConcurrentAllocationUsesTheWinningMappingIdempotently() {
        String key = "topic-partition-same-owner";
        long generatedStreamId = 1000L;
        long winningStreamId = 1001L;
        String mappingPath = STREAM_ID_GENERATOR_PATH + "/" + key;
        String registrationPath = STREAM_REGISTER_PATH + "/" + winningStreamId;
        StreamIdMappingOwner owner = owner("incarnation-a", "owner-a", 5L);
        GetResult active = versionedMapping(mappingPath, "ACTIVE", winningStreamId, owner);
        AsyncOxiaClient oxiaClient = mock(AsyncOxiaClient.class);
        when(oxiaClient.get(eq(mappingPath), anySet()))
            .thenReturn(
                CompletableFuture.completedFuture(null),
                CompletableFuture.completedFuture(active),
                CompletableFuture.completedFuture(active));
        when(oxiaClient.put(eq(STREAM_ID_GENERATOR_PATH), any(byte[].class)))
            .thenReturn(CompletableFuture.completedFuture(
                new PutResult(STREAM_ID_GENERATOR_PATH, version(generatedStreamId))));
        when(oxiaClient.put(eq(mappingPath), any(byte[].class), anySet()))
            .thenReturn(CompletableFuture.failedFuture(
                new KeyAlreadyExistsException(mappingPath)));
        when(oxiaClient.put(eq(registrationPath), any(byte[].class), anySet()))
            .thenReturn(CompletableFuture.completedFuture(
                new PutResult(registrationPath, version(2L))));

        StreamIdAllocation allocation = storageApi(oxiaClient)
            .allocateStreamId(key, owner, Optional.empty())
            .join();

        assertThat(allocation)
            .isEqualTo(new StreamIdAllocation(winningStreamId, false));
        verify(oxiaClient).put(eq(mappingPath), any(byte[].class), anySet());
    }

    @Test
    void newOwnerAllocatesOnlyAfterAcknowledgingExactFence() {
        String key = "topic-partition-acknowledged-fence";
        long newStreamId = 1101L;
        String mappingPath = STREAM_ID_GENERATOR_PATH + "/" + key;
        String registrationPath = STREAM_REGISTER_PATH + "/" + newStreamId;
        StreamIdMappingFence fence = new StreamIdMappingFence(
            1100L, owner("incarnation-a", "owner-a", 6L));
        StreamIdMappingOwner newOwner = owner("incarnation-b", "owner-b", 1L);
        GetResult tombstone = versionedMapping(
            mappingPath, "TOMBSTONE", fence.streamId(), fence.owner());
        GetResult active = versionedMapping(
            mappingPath, "ACTIVE", newStreamId, newOwner);
        AsyncOxiaClient oxiaClient = mock(AsyncOxiaClient.class);
        when(oxiaClient.get(eq(mappingPath), anySet()))
            .thenReturn(
                CompletableFuture.completedFuture(tombstone),
                CompletableFuture.completedFuture(active));
        when(oxiaClient.put(eq(STREAM_ID_GENERATOR_PATH), any(byte[].class)))
            .thenReturn(CompletableFuture.completedFuture(
                new PutResult(STREAM_ID_GENERATOR_PATH, version(newStreamId))));
        when(oxiaClient.put(eq(mappingPath), any(byte[].class), anySet()))
            .thenReturn(CompletableFuture.completedFuture(
                new PutResult(mappingPath, version(3L))));
        when(oxiaClient.put(eq(registrationPath), any(byte[].class), anySet()))
            .thenReturn(CompletableFuture.completedFuture(
                new PutResult(registrationPath, version(4L))));

        StreamIdAllocation allocation = storageApi(oxiaClient)
            .allocateStreamId(key, newOwner, Optional.of(fence))
            .join();

        assertThat(allocation).isEqualTo(new StreamIdAllocation(newStreamId, true));
    }

    @Test
    void wrongFenceAcknowledgementCannotAllocateOrBind() {
        String key = "topic-partition-wrong-fence-ack";
        long retiredStreamId = 1102L;
        String mappingPath = STREAM_ID_GENERATOR_PATH + "/" + key;
        StreamIdMappingOwner retiredOwner = owner("incarnation-a", "owner-a", 7L);
        StreamIdMappingOwner newOwner = owner("incarnation-b", "owner-b", 1L);
        StreamIdMappingFence actualFence = new StreamIdMappingFence(
            retiredStreamId, retiredOwner);
        StreamIdMappingFence wrongFence = new StreamIdMappingFence(
            retiredStreamId + 1, retiredOwner);
        GetResult tombstone = versionedMapping(
            mappingPath, "TOMBSTONE", actualFence.streamId(), actualFence.owner());
        AsyncOxiaClient oxiaClient = mock(AsyncOxiaClient.class);
        when(oxiaClient.get(eq(mappingPath), anySet()))
            .thenReturn(CompletableFuture.completedFuture(tombstone));
        PersistStorageApi storageApi = storageApi(oxiaClient);

        CompletionException allocationFailure = org.junit.jupiter.api.Assertions.assertThrows(
            CompletionException.class,
            () -> storageApi.allocateStreamId(
                key, newOwner, Optional.of(wrongFence)).join());
        CompletionException bindingFailure = org.junit.jupiter.api.Assertions.assertThrows(
            CompletionException.class,
            () -> storageApi.bindStreamIdMapping(
                key, 1104L, newOwner, Optional.of(wrongFence)).join());

        assertThat(allocationFailure.getCause())
            .isInstanceOf(IllegalStateException.class)
            .hasMessageContaining("tombstone was not acknowledged");
        assertThat(bindingFailure.getCause())
            .isInstanceOf(IllegalStateException.class)
            .hasMessageContaining("tombstone was not acknowledged");
        verify(oxiaClient, never()).put(eq(mappingPath), any(byte[].class), anySet());
    }

    @Test
    void bindAdoptsLegacyMappingOnlyForTheSameStreamId() {
        String key = "topic-partition-bind-legacy";
        long streamId = 1201L;
        String mappingPath = STREAM_ID_GENERATOR_PATH + "/" + key;
        String registrationPath = STREAM_REGISTER_PATH + "/" + streamId;
        StreamIdMappingOwner owner = owner("incarnation-a", "owner-a", 7L);
        AsyncOxiaClient oxiaClient = mock(AsyncOxiaClient.class);
        when(oxiaClient.get(eq(mappingPath), anySet()))
            .thenReturn(
                CompletableFuture.completedFuture(mapping(mappingPath, streamId)),
                CompletableFuture.completedFuture(
                    versionedMapping(mappingPath, "ACTIVE", streamId, owner)));
        when(oxiaClient.put(eq(mappingPath), any(byte[].class), anySet()))
            .thenReturn(CompletableFuture.completedFuture(
                new PutResult(mappingPath, version(2L))));
        when(oxiaClient.put(eq(registrationPath), any(byte[].class), anySet()))
            .thenReturn(CompletableFuture.completedFuture(
                new PutResult(registrationPath, version(3L))));

        storageApi(oxiaClient)
            .bindStreamIdMapping(key, streamId, owner, Optional.empty())
            .join();

        ArgumentCaptor<byte[]> value = ArgumentCaptor.forClass(byte[].class);
        verify(oxiaClient).put(eq(mappingPath), value.capture(), anySet());
        assertThat(new String(value.getValue(), StandardCharsets.UTF_8))
            .contains("\"state\":\"ACTIVE\"")
            .contains("\"streamId\":" + streamId)
            .contains("\"ownerToken\":\"owner-a\"");
    }

    @Test
    void bindDoesNotAdoptDifferentLegacyStreamId() {
        String key = "topic-partition-bind-different-legacy-id";
        long legacyStreamId = 1202L;
        long requestedStreamId = 1203L;
        String mappingPath = STREAM_ID_GENERATOR_PATH + "/" + key;
        StreamIdMappingOwner owner = owner("incarnation-a", "owner-a", 8L);
        AsyncOxiaClient oxiaClient = mock(AsyncOxiaClient.class);
        when(oxiaClient.get(eq(mappingPath), anySet()))
            .thenReturn(CompletableFuture.completedFuture(
                mapping(mappingPath, legacyStreamId)));

        CompletionException failure = org.junit.jupiter.api.Assertions.assertThrows(
            CompletionException.class,
            () -> storageApi(oxiaClient).bindStreamIdMapping(
                key, requestedStreamId, owner, Optional.empty()).join());

        assertThat(failure.getCause()).isInstanceOf(StreamIdMappingConflictException.class);
        StreamIdMappingConflictException conflict =
            (StreamIdMappingConflictException) failure.getCause();
        assertThat(conflict.activeMapping()).isEqualTo(
            new ActiveStreamIdMapping(legacyStreamId, StreamIdMappingOwner.legacy()));
        verify(oxiaClient, never()).put(eq(mappingPath), any(byte[].class), anySet());
    }

    @Test
    void bindingIsInvalidatedWhenSameOwnerFenceWinsDuringRegistration() {
        String key = "topic-partition-bind-fence-race";
        long streamId = 1202L;
        String mappingPath = STREAM_ID_GENERATOR_PATH + "/" + key;
        String registrationPath = STREAM_REGISTER_PATH + "/" + streamId;
        StreamIdMappingOwner owner = owner("incarnation-a", "owner-a", 8L);
        GetResult active = versionedMapping(mappingPath, "ACTIVE", streamId, owner);
        GetResult tombstone = versionedMapping(mappingPath, "TOMBSTONE", streamId, owner);
        CompletableFuture<PutResult> registration = new CompletableFuture<>();
        AsyncOxiaClient oxiaClient = mock(AsyncOxiaClient.class);
        when(oxiaClient.get(eq(mappingPath), anySet()))
            .thenReturn(
                CompletableFuture.completedFuture(null),
                CompletableFuture.completedFuture(active),
                CompletableFuture.completedFuture(tombstone));
        when(oxiaClient.put(eq(mappingPath), any(byte[].class), anySet()))
            .thenReturn(
                CompletableFuture.completedFuture(
                    new PutResult(mappingPath, version(2L))),
                CompletableFuture.completedFuture(
                    new PutResult(mappingPath, version(3L))));
        when(oxiaClient.put(eq(registrationPath), any(byte[].class), anySet()))
            .thenReturn(registration);
        PersistStorageApi storageApi = storageApi(oxiaClient);

        CompletableFuture<Void> binding = storageApi.bindStreamIdMapping(
            key, streamId, owner, Optional.empty());
        assertThat(binding).isNotDone();

        StreamIdMappingFenceResult fenceResult = storageApi
            .fenceStreamIdMappingState(key, streamId, owner)
            .join();
        assertThat(fenceResult).isEqualTo(new StreamIdMappingFenceResult.Fenced(
            new StreamIdMappingFence(streamId, owner)));

        registration.complete(new PutResult(registrationPath, version(4L)));
        CompletionException failure = org.junit.jupiter.api.Assertions.assertThrows(
            CompletionException.class, binding::join);
        assertThat(failure.getCause()).isInstanceOf(KeyedAllocationInvalidatedException.class);
        KeyedAllocationInvalidatedException invalidated =
            (KeyedAllocationInvalidatedException) failure.getCause();
        assertThat(invalidated.allocation())
            .isEqualTo(new StreamIdAllocation(streamId, true));
    }

    @Test
    void canonicalizationRequiresTheExactExistingTombstone() {
        String key = "topic-partition-canonical-fence";
        String mappingPath = STREAM_ID_GENERATOR_PATH + "/" + key;
        StreamIdMappingOwner owner = owner("incarnation-a", "owner-a", 8L);
        StreamIdMappingFence retired = new StreamIdMappingFence(1301L, owner);
        StreamIdMappingFence canonical = new StreamIdMappingFence(1302L, owner);
        AsyncOxiaClient oxiaClient = mock(AsyncOxiaClient.class);
        when(oxiaClient.get(eq(mappingPath), anySet()))
            .thenReturn(CompletableFuture.completedFuture(
                versionedMapping(mappingPath, "TOMBSTONE", retired.streamId(), owner)));
        when(oxiaClient.put(eq(mappingPath), any(byte[].class), anySet()))
            .thenReturn(CompletableFuture.completedFuture(
                new PutResult(mappingPath, version(2L))));

        storageApi(oxiaClient)
            .canonicalizeStreamIdMappingFence(key, retired, canonical)
            .join();

        ArgumentCaptor<byte[]> value = ArgumentCaptor.forClass(byte[].class);
        verify(oxiaClient).put(eq(mappingPath), value.capture(), anySet());
        assertThat(new String(value.getValue(), StandardCharsets.UTF_8))
            .contains("\"state\":\"TOMBSTONE\"")
            .contains("\"streamId\":" + canonical.streamId());
    }

    @Test
    void canonicalizationDoesNotOverwriteConcurrentActiveMapping() {
        String key = "topic-partition-canonical-fence-race";
        String mappingPath = STREAM_ID_GENERATOR_PATH + "/" + key;
        StreamIdMappingOwner retiredOwner = owner("incarnation-a", "owner-a", 9L);
        StreamIdMappingOwner newOwner = owner("incarnation-b", "owner-b", 1L);
        StreamIdMappingFence retired = new StreamIdMappingFence(1303L, retiredOwner);
        StreamIdMappingFence canonical = new StreamIdMappingFence(1304L, retiredOwner);
        AsyncOxiaClient oxiaClient = mock(AsyncOxiaClient.class);
        when(oxiaClient.get(eq(mappingPath), anySet()))
            .thenReturn(
                CompletableFuture.completedFuture(versionedMapping(
                    mappingPath, "TOMBSTONE", retired.streamId(), retired.owner())),
                CompletableFuture.completedFuture(versionedMapping(
                    mappingPath, "ACTIVE", 1305L, newOwner)));
        when(oxiaClient.put(eq(mappingPath), any(byte[].class), anySet()))
            .thenReturn(CompletableFuture.failedFuture(
                new UnexpectedVersionIdException(mappingPath, VERSION.versionId())));

        CompletionException failure = org.junit.jupiter.api.Assertions.assertThrows(
            CompletionException.class,
            () -> storageApi(oxiaClient)
                .canonicalizeStreamIdMappingFence(key, retired, canonical).join());

        assertThat(failure.getCause())
            .isInstanceOf(IllegalStateException.class)
            .hasMessageContaining("Cannot canonicalize active");
        verify(oxiaClient).put(eq(mappingPath), any(byte[].class), anySet());
    }

    @Test
    void canonicalizationRejectsDifferentOwnersWithoutIo() {
        String key = "topic-partition-invalid-canonical-owner";
        StreamIdMappingOwner ownerA = owner("incarnation-a", "owner-a", 10L);
        StreamIdMappingOwner ownerB = owner("incarnation-b", "owner-b", 1L);
        StreamIdMappingFence ownerAFence = new StreamIdMappingFence(1306L, ownerA);
        StreamIdMappingFence ownerBFence = new StreamIdMappingFence(1307L, ownerB);
        AsyncOxiaClient oxiaClient = mock(AsyncOxiaClient.class);
        PersistStorageApi storageApi = storageApi(oxiaClient);

        CompletionException differentOwnerFailure =
            org.junit.jupiter.api.Assertions.assertThrows(
                CompletionException.class,
                () -> storageApi.canonicalizeStreamIdMappingFence(
                    key, ownerAFence, ownerBFence).join());

        assertThat(differentOwnerFailure.getCause())
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("same lifecycle owner");
        verify(oxiaClient, never()).get(any(), anySet());
        verify(oxiaClient, never()).put(any(), any(byte[].class), anySet());
    }

    @Test
    void historicalLegacyFenceCanBeCanonicalizedWithinTheSameOwner() {
        String key = "topic-partition-legacy-canonical-owner";
        String mappingPath = STREAM_ID_GENERATOR_PATH + "/" + key;
        StreamIdMappingOwner legacyOwner = StreamIdMappingOwner.legacy();
        StreamIdMappingFence retired = new StreamIdMappingFence(1308L, legacyOwner);
        StreamIdMappingFence canonical = new StreamIdMappingFence(1309L, legacyOwner);
        AsyncOxiaClient oxiaClient = mock(AsyncOxiaClient.class);
        when(oxiaClient.get(eq(mappingPath), anySet()))
            .thenReturn(CompletableFuture.completedFuture(versionedMapping(
                mappingPath, "TOMBSTONE", retired.streamId(), legacyOwner)));
        when(oxiaClient.put(eq(mappingPath), any(byte[].class), anySet()))
            .thenReturn(CompletableFuture.completedFuture(
                new PutResult(mappingPath, version(2L))));

        storageApi(oxiaClient)
            .canonicalizeStreamIdMappingFence(key, retired, canonical)
            .join();

        ArgumentCaptor<byte[]> value = ArgumentCaptor.forClass(byte[].class);
        verify(oxiaClient).put(eq(mappingPath), value.capture(), anySet());
        assertThat(new String(value.getValue(), StandardCharsets.UTF_8))
            .contains("\"state\":\"TOMBSTONE\"")
            .contains("\"streamId\":" + canonical.streamId())
            .contains("\"ownerToken\":\"__legacy__\"");
    }

    @Test
    void lifecycleAwareOperationsRejectSyntheticLegacyOwnerWithoutIo() {
        String key = "topic-partition-legacy-caller-owner";
        StreamIdMappingOwner legacyOwner = StreamIdMappingOwner.legacy();
        AsyncOxiaClient oxiaClient = mock(AsyncOxiaClient.class);
        PersistStorageApi storageApi = storageApi(oxiaClient);

        CompletionException allocationFailure = org.junit.jupiter.api.Assertions.assertThrows(
            CompletionException.class,
            () -> storageApi.allocateStreamId(
                key, legacyOwner, Optional.empty()).join());
        CompletionException bindingFailure = org.junit.jupiter.api.Assertions.assertThrows(
            CompletionException.class,
            () -> storageApi.bindStreamIdMapping(
                key, 1402L, legacyOwner, Optional.empty()).join());
        CompletionException fenceFailure = org.junit.jupiter.api.Assertions.assertThrows(
            CompletionException.class,
            () -> storageApi.fenceStreamIdMappingState(
                key, -1L, legacyOwner).join());

        assertThat(allocationFailure.getCause())
            .isInstanceOf(IllegalArgumentException.class);
        assertThat(bindingFailure.getCause())
            .isInstanceOf(IllegalArgumentException.class);
        assertThat(fenceFailure.getCause())
            .isInstanceOf(IllegalArgumentException.class);
        verify(oxiaClient, never()).get(any(), anySet());
        verify(oxiaClient, never()).put(any(), any(byte[].class), anySet());
    }

    private static GetResult mapping(String path, long streamId) {
        return new GetResult(
            path,
            Long.toString(streamId).getBytes(StandardCharsets.UTF_8),
            VERSION);
    }

    private static GetResult versionedMapping(
            String path, String state, long streamId, StreamIdMappingOwner owner) {
        String value = "{\"version\":1,\"state\":\"" + state
            + "\",\"streamId\":" + streamId
            + ",\"incarnationId\":\"" + owner.incarnationId()
            + "\",\"ownerToken\":\"" + owner.ownerToken()
            + "\",\"ownerGeneration\":" + owner.ownerGeneration() + "}";
        return new GetResult(path, value.getBytes(StandardCharsets.UTF_8), VERSION);
    }

    private static StreamIdMappingOwner owner(
            String incarnationId, String ownerToken, long ownerGeneration) {
        return new StreamIdMappingOwner(incarnationId, ownerToken, ownerGeneration);
    }

    private static Version version(long versionId) {
        return new Version(versionId, 0, 0, 0, Optional.empty(), Optional.empty());
    }

    private PersistStorageApi storageApi(AsyncOxiaClient oxiaClient) {
        StorageConfig config = StorageConfig.builder().backendStorageType("local").build();
        return new PersistStorageApi(
            config,
            oxiaClient,
            mock(WalStorage.class),
            InstrumentProvider.NOOP,
            new StorageFormat(config),
            mock(LogStateManager.class));
    }
}
