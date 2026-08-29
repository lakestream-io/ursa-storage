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
import static org.mockito.Mockito.when;

import io.lakestream.api.LogStateManager;
import io.lakestream.ursa.metrics.InstrumentProvider;
import io.lakestream.ursa.storage.StorageApi.KeyedAllocationInvalidatedException;
import io.lakestream.ursa.storage.StorageApi.StreamIdAllocation;
import io.lakestream.ursa.storage.WalStorage;
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
    void conditionalDeleteFalseSucceedsOnlyAfterAbsentReadback() {
        String key = "topic-partition-delete-absent";
        long streamId = 505L;
        String mappingPath = STREAM_ID_GENERATOR_PATH + "/" + key;
        AsyncOxiaClient oxiaClient = mock(AsyncOxiaClient.class);
        when(oxiaClient.get(eq(mappingPath), anySet()))
            .thenReturn(
                CompletableFuture.completedFuture(mapping(mappingPath, streamId)),
                CompletableFuture.completedFuture(null));
        when(oxiaClient.delete(eq(mappingPath), anySet()))
            .thenReturn(CompletableFuture.completedFuture(false));

        storageApi(oxiaClient).deleteStreamIdMapping(key, streamId).join();

        verify(oxiaClient, times(2)).get(eq(mappingPath), anySet());
        verify(oxiaClient).delete(eq(mappingPath), anySet());
    }

    @Test
    void conditionalDeleteFalseSucceedsAfterDifferentMappingReadback() {
        String key = "topic-partition-delete-reassigned";
        long streamId = 606L;
        long reassignedStreamId = 607L;
        String mappingPath = STREAM_ID_GENERATOR_PATH + "/" + key;
        AsyncOxiaClient oxiaClient = mock(AsyncOxiaClient.class);
        when(oxiaClient.get(eq(mappingPath), anySet()))
            .thenReturn(
                CompletableFuture.completedFuture(mapping(mappingPath, streamId)),
                CompletableFuture.completedFuture(mapping(mappingPath, reassignedStreamId)));
        when(oxiaClient.delete(eq(mappingPath), anySet()))
            .thenReturn(CompletableFuture.completedFuture(false));

        storageApi(oxiaClient).deleteStreamIdMapping(key, streamId).join();

        verify(oxiaClient, times(2)).get(eq(mappingPath), anySet());
        verify(oxiaClient).delete(eq(mappingPath), anySet());
    }

    @Test
    void conditionalDeleteFalseFailsWhenExpectedMappingRemains() {
        String key = "topic-partition-delete-retained";
        long streamId = 707L;
        String mappingPath = STREAM_ID_GENERATOR_PATH + "/" + key;
        AsyncOxiaClient oxiaClient = mock(AsyncOxiaClient.class);
        when(oxiaClient.get(eq(mappingPath), anySet()))
            .thenReturn(CompletableFuture.completedFuture(mapping(mappingPath, streamId)));
        when(oxiaClient.delete(eq(mappingPath), anySet()))
            .thenReturn(CompletableFuture.completedFuture(false));

        CompletableFuture<Void> deletion =
            storageApi(oxiaClient).deleteStreamIdMapping(key, streamId);

        CompletionException failure = org.junit.jupiter.api.Assertions.assertThrows(
            CompletionException.class, deletion::join);
        assertThat(failure.getCause())
            .isInstanceOf(IllegalStateException.class)
            .hasMessageContaining("Conditional keyed stream-ID delete returned false");
        verify(oxiaClient, times(2)).get(eq(mappingPath), anySet());
        verify(oxiaClient).delete(eq(mappingPath), anySet());
    }

    @Test
    void conditionalDeleteRetriesVersionConflictAndThenSucceeds() {
        String key = "topic-partition-delete-conflict";
        long streamId = 808L;
        String mappingPath = STREAM_ID_GENERATOR_PATH + "/" + key;
        AsyncOxiaClient oxiaClient = mock(AsyncOxiaClient.class);
        when(oxiaClient.get(eq(mappingPath), anySet()))
            .thenReturn(CompletableFuture.completedFuture(mapping(mappingPath, streamId)));
        when(oxiaClient.delete(eq(mappingPath), anySet()))
            .thenReturn(
                CompletableFuture.failedFuture(
                    new UnexpectedVersionIdException(mappingPath, VERSION.versionId())),
                CompletableFuture.completedFuture(true));

        storageApi(oxiaClient).deleteStreamIdMapping(key, streamId).join();

        verify(oxiaClient, times(2)).get(eq(mappingPath), anySet());
        verify(oxiaClient, times(2)).delete(eq(mappingPath), anySet());
    }

    @Test
    void conditionalDeleteFailsAfterBoundedVersionConflictRetries() {
        String key = "topic-partition-delete-conflict-exhausted";
        long streamId = 909L;
        String mappingPath = STREAM_ID_GENERATOR_PATH + "/" + key;
        AsyncOxiaClient oxiaClient = mock(AsyncOxiaClient.class);
        when(oxiaClient.get(eq(mappingPath), anySet()))
            .thenReturn(CompletableFuture.completedFuture(mapping(mappingPath, streamId)));
        when(oxiaClient.delete(eq(mappingPath), anySet()))
            .thenReturn(CompletableFuture.failedFuture(
                new UnexpectedVersionIdException(mappingPath, VERSION.versionId())));

        CompletionException failure = org.junit.jupiter.api.Assertions.assertThrows(
            CompletionException.class,
            () -> storageApi(oxiaClient).deleteStreamIdMapping(key, streamId).join());

        assertThat(failure.getCause())
            .isInstanceOf(IllegalStateException.class)
            .hasMessageContaining("exhausted retries");
        verify(oxiaClient, times(4)).get(eq(mappingPath), anySet());
        verify(oxiaClient, times(4)).delete(eq(mappingPath), anySet());
    }

    private static GetResult mapping(String path, long streamId) {
        return new GetResult(
            path,
            Long.toString(streamId).getBytes(StandardCharsets.UTF_8),
            VERSION);
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
