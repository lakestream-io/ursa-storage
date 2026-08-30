/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.lakestream.impl;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.lakestream.api.CatalogPaths;
import io.lakestream.api.LifecycleState;
import io.lakestream.api.LogId;
import io.lakestream.api.LogStorage;
import io.lakestream.api.Namespace;
import io.lakestream.api.Partitioning;
import io.lakestream.api.PartitioningStrategy;
import io.lakestream.api.SchemaConfig;
import io.lakestream.api.Stream;
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
import io.lakestream.api.exception.StreamPermanentlyDeletedException;
import io.lakestream.ursa.catalog.metadata.LogMetadata;
import io.lakestream.ursa.catalog.metadata.LogMetadataSerde;
import io.lakestream.ursa.storage.StorageApi;
import io.lakestream.ursa.storage.StorageApi.ActiveStreamIdMapping;
import io.lakestream.ursa.storage.StorageApi.KeyedAllocationInvalidatedException;
import io.lakestream.ursa.storage.StorageApi.StreamIdAllocation;
import io.lakestream.ursa.storage.StorageApi.StreamIdMappingConflictException;
import io.lakestream.ursa.storage.StorageApi.StreamIdMappingFence;
import io.lakestream.ursa.storage.StorageApi.StreamIdMappingFenceResult;
import io.lakestream.ursa.storage.StorageApi.StreamIdMappingOwner;
import io.lakestream.ursa.storage.impl.exception.NoSuchKeyException;
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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.IntStream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class IndexedStreamCatalogTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final LogMetadataSerde LOG_METADATA_SERDE = LogMetadataSerde.INSTANCE;
    private static final Version DUMMY_VERSION = new Version(1, 0, 0, 0, Optional.empty(), Optional.empty());

    @Mock
    private AsyncOxiaClient oxiaClient;
    @Mock
    private LogStorage logStorage;

    private CatalogPaths catalogPaths;
    private IndexedStreamCatalog catalog;
    private FencedStorageHarness defaultStorage;
    private StreamIdentifier streamId;

    private long nextStreamId = 100L;

    @BeforeEach
    void setUp() {
        lenient().when(oxiaClient.get(anyString()))
            .thenReturn(CompletableFuture.completedFuture(null));
        catalogPaths = new DefaultCatalogPaths();
        String namespacePath = catalogPaths.namespacePath("public/default");
        lenient().when(oxiaClient.put(
                eq(namespacePath), any(byte[].class),
                eq(Set.of(PutOption.IfRecordDoesNotExist))))
            .thenReturn(CompletableFuture.completedFuture(
                new PutResult(namespacePath, DUMMY_VERSION)));
        defaultStorage = new FencedStorageHarness(
            key -> CompletableFuture.completedFuture(nextStreamId++));
        catalog = fencedCatalog(defaultStorage);
        catalog.initialize("test-catalog", Map.of()).join();
        streamId = new StreamIdentifier("public/default", "my-topic");
    }

    // --- createStream ---

    @Test
    void createStream_success() throws Exception {
        nextStreamId = 100L;
        String configPath = catalogPaths.streamConfigPath(streamId);
        String firstPartitionPath = catalogPaths.partitionMetadataPath(streamId, 0);
        String secondPartitionPath = catalogPaths.partitionMetadataPath(streamId, 1);
        mockVersionedConfig(configPath);
        mockCreateOnlyRecord(firstPartitionPath);
        mockCreateOnlyRecord(secondPartitionPath);

        Partitioning partitioning = new Partitioning(PartitioningStrategy.INDEXED, Map.of("numPartitions", "2"));
        Stream result = catalog.createStream(streamId, new StreamConfig(), partitioning,
            new SchemaConfig(), Map.of("key1", "val1")).get();

        assertEquals(streamId, result.identifier());
        assertEquals(LifecycleState.ACTIVE, result.state());
        assertEquals(Map.of("key1", "val1"), result.properties());

        verify(oxiaClient).put(eq(firstPartitionPath), any(byte[].class),
            eq(Set.of(PutOption.IfRecordDoesNotExist)));
        verify(oxiaClient).put(eq(secondPartitionPath), any(byte[].class),
            eq(Set.of(PutOption.IfRecordDoesNotExist)));
        verify(oxiaClient).put(eq(configPath), any(byte[].class),
            eq(Set.of(PutOption.IfRecordDoesNotExist)));
        verify(oxiaClient).put(eq(configPath), any(byte[].class),
            eq(Set.of(PutOption.IfVersionIdEquals(1L))));
        // Fenced storage does not need a separate capability preflight read. Creation reads the
        // durable mapping context before the metadata write, then reads once more for the write
        // and while building the returned layout.
        verify(oxiaClient, times(3)).get(firstPartitionPath);
        verify(oxiaClient, times(3)).get(secondPartitionPath);
    }

    @Test
    void createStream_alreadyExists() throws Exception {
        String configPath = "/admin/streams/public/default/my-topic";
        when(oxiaClient.get(configPath))
            .thenReturn(CompletableFuture.completedFuture(
                new GetResult(configPath, streamConfigBytes(1, Map.of(), false), DUMMY_VERSION)));

        Partitioning partitioning = new Partitioning(PartitioningStrategy.INDEXED, Map.of("numPartitions", "1"));
        ExecutionException ex = assertThrows(ExecutionException.class, () ->
            catalog.createStream(streamId, new StreamConfig(), partitioning,
                new SchemaConfig(), Map.of()).get());
        assertInstanceOf(AlreadyExistsException.class, ex.getCause());
        verify(oxiaClient, never()).put(eq(configPath), any(byte[].class), any());
    }

    @Test
    void createStream_permanentlyDeletedIdentityDoesNotCreatePartitions() {
        String configPath = "/admin/streams/public/default/my-topic";
        when(oxiaClient.get(configPath)).thenReturn(CompletableFuture.completedFuture(
            new GetResult(configPath,
                "{\"_externalStreamPermanentlyDeleted\":true}"
                    .getBytes(StandardCharsets.UTF_8),
                DUMMY_VERSION)));

        Partitioning partitioning = new Partitioning(
            PartitioningStrategy.INDEXED, Map.of("numPartitions", "1"));
        ExecutionException ex = assertThrows(ExecutionException.class, () ->
            catalog.createStream(streamId, new StreamConfig(), partitioning,
                new SchemaConfig(), Map.of()).get());

        assertInstanceOf(StreamPermanentlyDeletedException.class, ex.getCause());
        verify(oxiaClient, never()).put(any(), any(byte[].class));
        verify(oxiaClient, never()).put(eq(configPath), any(byte[].class), any());
    }

    @Test
    void createStream_takeoverFencesLateOwnerAndReusesStablePartitionId() throws Exception {
        String configPath = catalogPaths.streamConfigPath(streamId);
        String partitionPath = catalogPaths.partitionMetadataPath(streamId, 0);
        AtomicReference<VersionedValue> configState = mockVersionedConfig(configPath);
        mockCreateOnlyRecord(partitionPath);

        CompletableFuture<Long> delayedAllocation = new CompletableFuture<>();
        AtomicInteger allocationCalls = new AtomicInteger();
        List<String> allocationKeys = new java.util.concurrent.CopyOnWriteArrayList<>();
        IndexedStreamCatalog concurrentCatalog =
            IndexedStreamCatalog.withConditionalStreamIdMappingDeletion(
            oxiaClient, catalogPaths, logStorage,
            (name, logId, reader) -> null, null,
            key -> {
                allocationKeys.add(key.orElseThrow());
                return allocationCalls.getAndIncrement() == 0
                    ? delayedAllocation : CompletableFuture.completedFuture(100L);
            },
            ignored -> CompletableFuture.completedFuture(100L),
            (key, expectedStreamId) -> CompletableFuture.completedFuture(null),
            null, null, List.of());
        Partitioning partitioning = new Partitioning(
            PartitioningStrategy.INDEXED, Map.of("numPartitions", "1"));

        CompletableFuture<Stream> staleOwner = concurrentCatalog.createStream(
            streamId, new StreamConfig(), partitioning, new SchemaConfig(), Map.of());
        CompletableFuture<Stream> takeover = concurrentCatalog.createStream(
            streamId, new StreamConfig(), partitioning, new SchemaConfig(), Map.of());

        assertEquals(streamId, takeover.join().identifier());
        delayedAllocation.complete(100L);

        CompletionException failure = assertThrows(CompletionException.class, staleOwner::join);
        assertInstanceOf(
            IndexedStreamConfigStore.ProvisioningOwnershipLostException.class,
            failure.getCause());
        assertEquals(2, allocationCalls.get());
        assertEquals(List.of(
            "lakestream-native/" + streamId.fullName() + "/partition-0",
            "lakestream-native/" + streamId.fullName() + "/partition-0"),
            allocationKeys);
        verify(oxiaClient, times(1)).put(eq(partitionPath), any(byte[].class),
            eq(Set.of(PutOption.IfRecordDoesNotExist)));
        verify(oxiaClient, never()).delete(eq(partitionPath), any());
        verify(logStorage, never()).deleteLog(any());
    }

    @Test
    void lateNativeAllocationAfterCompletedDropCleansItsOwnMappingAndRegistration()
            throws Exception {
        String configPath = catalogPaths.streamConfigPath(streamId);
        String partitionPath = catalogPaths.partitionMetadataPath(streamId, 0);
        AtomicReference<VersionedValue> config = mockVersionedConfig(configPath);
        AtomicReference<VersionedValue> partition = mockCreateOnlyRecord(partitionPath);
        CompletableFuture<Long> allocation = new CompletableFuture<>();
        when(logStorage.deleteLog(LogId.of(701L)))
            .thenReturn(CompletableFuture.completedFuture(null));
        FencedStorageHarness mappings = new FencedStorageHarness(key -> allocation);
        IndexedStreamCatalog racedCatalog = fencedCatalog(mappings);
        Partitioning partitioning = new Partitioning(
            PartitioningStrategy.INDEXED, Map.of("numPartitions", "1"));

        CompletableFuture<Stream> create = racedCatalog.createStream(
            streamId, new StreamConfig(), partitioning, new SchemaConfig(), Map.of());
        assertFalse(create.isDone());
        assertTrue(racedCatalog.dropStream(streamId, false).join());
        allocation.complete(701L);

        CompletionException failure = assertThrows(CompletionException.class, create::join);
        assertInstanceOf(KeyedAllocationInvalidatedException.class, failure.getCause());
        String mappingKey = "lakestream-native/" + streamId.fullName() + "/partition-0";
        assertEquals(Optional.empty(), mappings.activeStreamId(mappingKey));
        assertEquals(701L, mappings.fence(mappingKey).orElseThrow().streamId());
        assertEquals("DROPPED", MAPPER.readTree(config.get().value())
            .get("_provisioningState").asText());
        assertTrue(LOG_METADATA_SERDE.deserialize(
            partitionPath, partition.get().value()).deleted());
        verify(logStorage).deleteLog(LogId.of(701L));
    }

    @Test
    void invalidatedNativeAllocationAfterCompletedDropIsCompensated()
            throws Exception {
        long allocatedStreamId = 703L;
        String configPath = catalogPaths.streamConfigPath(streamId);
        String partitionPath = catalogPaths.partitionMetadataPath(streamId, 0);
        AtomicReference<VersionedValue> config = mockVersionedConfig(configPath);
        AtomicReference<VersionedValue> partition = mockCreateOnlyRecord(partitionPath);
        CompletableFuture<Long> allocation = new CompletableFuture<>();
        RuntimeException validationFailure = new RuntimeException("mapping validation failed");
        KeyedAllocationInvalidatedException invalidated =
            new KeyedAllocationInvalidatedException(
                new StreamIdAllocation(allocatedStreamId, false), validationFailure);
        when(logStorage.deleteLog(LogId.of(allocatedStreamId)))
            .thenReturn(CompletableFuture.completedFuture(null));
        FencedStorageHarness mappings = new FencedStorageHarness(key -> allocation);
        IndexedStreamCatalog racedCatalog = fencedCatalog(mappings);
        Partitioning partitioning = new Partitioning(
            PartitioningStrategy.INDEXED, Map.of("numPartitions", "1"));

        CompletableFuture<Stream> create = racedCatalog.createStream(
            streamId, new StreamConfig(), partitioning, new SchemaConfig(), Map.of());
        assertFalse(create.isDone());
        assertTrue(racedCatalog.dropStream(streamId, false).join());
        allocation.completeExceptionally(invalidated);

        CompletionException failure = assertThrows(CompletionException.class, create::join);
        assertEquals(invalidated, failure.getCause());
        assertEquals(validationFailure, failure.getCause().getCause());
        assertEquals("DROPPED", MAPPER.readTree(config.get().value())
            .get("_provisioningState").asText());
        assertTrue(LOG_METADATA_SERDE.deserialize(
            partitionPath, partition.get().value()).deleted());
        verify(logStorage).deleteLog(LogId.of(allocatedStreamId));
        String mappingKey = "lakestream-native/" + streamId.fullName() + "/partition-0";
        assertEquals(allocatedStreamId,
            mappings.fence(mappingKey).orElseThrow().streamId());
    }

    @Test
    void nativeCleanupContextReadFailureIsSuppressedOnOwnershipFailure()
            throws Exception {
        long allocatedStreamId = 704L;
        String configPath = catalogPaths.streamConfigPath(streamId);
        String partitionPath = catalogPaths.partitionMetadataPath(streamId, 0);
        AtomicReference<VersionedValue> config = mockVersionedConfig(configPath);
        mockCreateOnlyRecord(partitionPath);
        CompletableFuture<Long> allocation = new CompletableFuture<>();
        FencedStorageHarness mappings = new FencedStorageHarness(key -> allocation);
        IndexedStreamCatalog racedCatalog = fencedCatalog(mappings);
        Partitioning partitioning = new Partitioning(
            PartitioningStrategy.INDEXED, Map.of("numPartitions", "1"));

        CompletableFuture<Stream> create = racedCatalog.createStream(
            streamId, new StreamConfig(), partitioning, new SchemaConfig(), Map.of());
        assertFalse(create.isDone());
        assertTrue(racedCatalog.dropStream(streamId, false).join());

        RuntimeException contextReadFailure =
            new RuntimeException("native cleanup context unavailable");
        AtomicInteger postDropReads = new AtomicInteger();
        when(oxiaClient.get(configPath)).thenAnswer(ignored -> {
            if (postDropReads.getAndIncrement() == 0) {
                VersionedValue current = config.get();
                return CompletableFuture.completedFuture(new GetResult(
                    configPath, current.value(), current.version()));
            }
            return CompletableFuture.failedFuture(contextReadFailure);
        });
        allocation.complete(allocatedStreamId);

        CompletionException failure = assertThrows(CompletionException.class, create::join);
        assertInstanceOf(KeyedAllocationInvalidatedException.class, failure.getCause());
        assertEquals(List.of(contextReadFailure),
            List.of(failure.getCause().getSuppressed()));
        assertEquals(1, mappings.fenceAttempts().size());
        verify(logStorage, never()).deleteLog(LogId.of(allocatedStreamId));
    }

    @Test
    void staleNativeOwnerPreservesPublishedLogAfterTakeoverAndNonPurgingDrop()
            throws Exception {
        assertStaleNativeOwnerCleanup(false);
    }

    @Test
    void staleNativeOwnerDeletesPublishedLogAfterPurgingDrop()
            throws Exception {
        assertStaleNativeOwnerCleanup(true);
    }

    private void assertStaleNativeOwnerCleanup(boolean purge) throws Exception {
        long reusedStreamId = 702L;
        if (purge) {
            when(logStorage.deleteLog(LogId.of(reusedStreamId)))
                .thenReturn(CompletableFuture.completedFuture(null));
        }
        String configPath = catalogPaths.streamConfigPath(streamId);
        String partitionPath = catalogPaths.partitionMetadataPath(streamId, 0);
        AtomicReference<VersionedValue> config = mockVersionedConfig(configPath);
        AtomicReference<VersionedValue> partition = mockCreateOnlyRecord(partitionPath);
        CompletableFuture<Long> staleAllocation = new CompletableFuture<>();
        AtomicInteger allocationCalls = new AtomicInteger();
        FencedStorageHarness mappings = new FencedStorageHarness(key ->
            allocationCalls.getAndIncrement() == 0
                ? staleAllocation
                : CompletableFuture.completedFuture(reusedStreamId));
        IndexedStreamCatalog racedCatalog = fencedCatalog(mappings);
        Partitioning partitioning = new Partitioning(
            PartitioningStrategy.INDEXED, Map.of("numPartitions", "1"));

        CompletableFuture<Stream> staleOwner = racedCatalog.createStream(
            streamId, new StreamConfig(), partitioning, new SchemaConfig(), Map.of());
        assertFalse(staleOwner.isDone());
        assertEquals(streamId, racedCatalog.createStream(
            streamId, new StreamConfig(), partitioning, new SchemaConfig(), Map.of()).join()
            .identifier());
        LogMetadata published = LOG_METADATA_SERDE.deserialize(
            partitionPath, partition.get().value());
        assertEquals(reusedStreamId, published.streamId());
        assertFalse(published.deleted());

        assertTrue(racedCatalog.dropStream(streamId, purge).join());
        LogMetadata tombstone = LOG_METADATA_SERDE.deserialize(
            partitionPath, partition.get().value());
        assertEquals(reusedStreamId, tombstone.streamId());
        assertTrue(tombstone.deleted());
        assertTrue(tombstone.retiredStreamIds().isEmpty());
        assertTrue(tombstone.purgeableRetiredStreamIds().isEmpty());
        assertTrue(tombstone.retiredStreamMappings().isEmpty());
        assertTrue(tombstone.retiredMappingKeys().isEmpty());
        if (purge) {
            verify(logStorage).deleteLog(LogId.of(reusedStreamId));
        } else {
            verify(logStorage, never()).deleteLog(LogId.of(reusedStreamId));
        }

        staleAllocation.complete(reusedStreamId);

        CompletionException failure = assertThrows(
            CompletionException.class, staleOwner::join);
        assertInstanceOf(KeyedAllocationInvalidatedException.class, failure.getCause());
        String mappingKey = "lakestream-native/" + streamId.fullName() + "/partition-0";
        assertEquals(Optional.empty(), mappings.activeStreamId(mappingKey));
        assertEquals(reusedStreamId,
            mappings.fence(mappingKey).orElseThrow().streamId());
        assertEquals(2, mappings.fenceAttempts().size());
        if (purge) {
            verify(logStorage, times(2)).deleteLog(LogId.of(reusedStreamId));
        } else {
            verify(logStorage, never()).deleteLog(LogId.of(reusedStreamId));
        }
    }

    @Test
    void createStream_partitionFailureRetainsRecoveryAnchors() throws Exception {
        String configPath = catalogPaths.streamConfigPath(streamId);
        String firstPartitionPath = catalogPaths.partitionMetadataPath(streamId, 0);
        String secondPartitionPath = catalogPaths.partitionMetadataPath(streamId, 1);
        RuntimeException partitionFailure = new RuntimeException("partition write failed");
        AtomicReference<VersionedValue> configState = mockVersionedConfig(configPath);
        AtomicReference<VersionedValue> firstPartitionState =
            mockCreateOnlyRecord(firstPartitionPath);
        when(oxiaClient.put(eq(secondPartitionPath), any(byte[].class),
                eq(Set.of(PutOption.IfRecordDoesNotExist))))
            .thenReturn(CompletableFuture.failedFuture(partitionFailure));
        when(oxiaClient.get(secondPartitionPath))
            .thenReturn(CompletableFuture.completedFuture(null));

        Partitioning partitioning = new Partitioning(
            PartitioningStrategy.INDEXED, Map.of("numPartitions", "2"));
        CompletionException failure = assertThrows(CompletionException.class, () ->
            catalog.createStream(streamId, new StreamConfig(), partitioning,
                new SchemaConfig(), Map.of()).join());

        assertEquals(partitionFailure, failure.getCause());
        assertTrue(MAPPER.readTree(configState.get().value())
            .get("_provisioning").asBoolean());
        assertNotNull(firstPartitionState.get());
        verify(oxiaClient, never()).delete(eq(firstPartitionPath), any());
        verify(oxiaClient, never()).delete(eq(configPath), any());
        verify(logStorage, never()).deleteLog(any());
    }

    @Test
    void createStream_retryAfterPostMetadataFailureReusesStableAnchors() throws Exception {
        String configPath = catalogPaths.streamConfigPath(streamId);
        String partitionPath = catalogPaths.partitionMetadataPath(streamId, 0);
        RuntimeException postMetadataFailure =
            new RuntimeException("crashed after partition metadata");
        AtomicReference<VersionedValue> partitionState =
            mockCreateOnlyRecord(partitionPath);
        AtomicReference<VersionedValue> configState = mockVersionedConfig(configPath);
        AtomicInteger postMetadataFailures = new AtomicInteger();
        when(oxiaClient.get(configPath)).thenAnswer(ignored -> {
            if (partitionState.get() != null
                    && postMetadataFailures.getAndIncrement() == 0) {
                return CompletableFuture.failedFuture(postMetadataFailure);
            }
            VersionedValue current = configState.get();
            return CompletableFuture.completedFuture(current == null ? null
                : new GetResult(configPath, current.value(), current.version()));
            });
        List<String> allocationKeys = new java.util.concurrent.CopyOnWriteArrayList<>();
        IndexedStreamCatalog recoveringCatalog =
            IndexedStreamCatalog.withConditionalStreamIdMappingDeletion(
            oxiaClient, catalogPaths, logStorage,
            (name, logId, reader) -> null, null,
            key -> {
                allocationKeys.add(key.orElseThrow());
                return CompletableFuture.completedFuture(100L);
            }, ignored -> CompletableFuture.completedFuture(100L),
            (key, expectedStreamId) -> CompletableFuture.completedFuture(null),
            null, null, List.of());

        Partitioning partitioning = new Partitioning(
            PartitioningStrategy.INDEXED, Map.of("numPartitions", "1"));
        CompletionException failure = assertThrows(CompletionException.class, () ->
            recoveringCatalog.createStream(streamId, new StreamConfig(), partitioning,
                new SchemaConfig(), Map.of()).join());

        assertEquals(postMetadataFailure, failure.getCause());
        assertEquals(streamId, recoveringCatalog.createStream(
            streamId, new StreamConfig(), partitioning,
            new SchemaConfig(), Map.of()).join().identifier());

        String expectedKey = "lakestream-native/" + streamId.fullName()
            + "/partition-0";
        assertEquals(List.of(expectedKey, expectedKey), allocationKeys);
        assertNotNull(partitionState.get());
        verify(oxiaClient).put(eq(partitionPath), any(byte[].class),
            eq(Set.of(PutOption.IfRecordDoesNotExist)));
        verify(oxiaClient, never()).delete(eq(configPath), any());
        verify(oxiaClient, never()).delete(eq(partitionPath), any());
        verify(logStorage, never()).deleteLog(any());
    }

    @Test
    void createStream_unknownFinalizeOutcomeDoesNotDeleteCreatedPartitions() {
        String configPath = catalogPaths.streamConfigPath(streamId);
        String partitionPath = catalogPaths.partitionMetadataPath(streamId, 0);
        RuntimeException finalizeFailure = new RuntimeException("finalize response lost");
        RuntimeException readFailure = new RuntimeException("readback unavailable");
        AtomicReference<byte[]> currentConfig = new AtomicReference<>();
        java.util.concurrent.atomic.AtomicBoolean finalizeAttempted =
            new java.util.concurrent.atomic.AtomicBoolean();
        when(oxiaClient.get(configPath)).thenAnswer(ignored -> {
            if (finalizeAttempted.get()) {
                return CompletableFuture.failedFuture(readFailure);
            }
            byte[] value = currentConfig.get();
            return CompletableFuture.completedFuture(value == null ? null
                : new GetResult(configPath, value, version(1)));
        });
        when(oxiaClient.put(eq(configPath), any(byte[].class),
                eq(Set.of(PutOption.IfRecordDoesNotExist))))
            .thenAnswer(invocation -> {
                currentConfig.set(invocation.getArgument(1, byte[].class));
                return CompletableFuture.completedFuture(
                    new PutResult(configPath, version(1)));
            });
        mockCreateOnlyRecord(partitionPath);
        when(oxiaClient.put(eq(configPath), any(byte[].class),
                eq(Set.of(PutOption.IfVersionIdEquals(1L)))))
            .thenAnswer(ignored -> {
                finalizeAttempted.set(true);
                return CompletableFuture.failedFuture(finalizeFailure);
            });

        Partitioning partitioning = new Partitioning(
            PartitioningStrategy.INDEXED, Map.of("numPartitions", "1"));
        CompletionException failure = assertThrows(CompletionException.class, () ->
            catalog.createStream(streamId, new StreamConfig(), partitioning,
                new SchemaConfig(), Map.of()).join());

        assertEquals(finalizeFailure, failure.getCause());
        assertTrue(List.of(finalizeFailure.getSuppressed()).contains(readFailure));
        verify(logStorage, never()).deleteLog(any());
        verify(oxiaClient, never()).delete(eq(partitionPath), any());
        verify(oxiaClient, never()).delete(eq(configPath), any());
    }

    // --- getLayout ---

    @Test
    void getLayout_success() throws Exception {
        // Partition count = 2
        mockStreamConfig(streamId, 2);

        // Partition 0 has streamId=100, partition 1 has streamId=101
        mockPartitionMetadata(streamId, 0, 100L, Map.of());
        mockPartitionMetadata(streamId, 1, 101L, Map.of());

        StreamLayout layout = catalog.getLayout(streamId).get();
        assertNotNull(layout);
        assertEquals(2, layout.logCount());

        List<LogId> logIds = layout.logIds().get();
        assertEquals(LogId.of(100), logIds.get(0));
        assertEquals(LogId.of(101), logIds.get(1));
    }

    // --- loadStream ---

    @Test
    void loadStream_success() throws Exception {
        mockStreamConfig(streamId, 3, Map.of("env", "prod"));
        // getLayout reads all partition metadata for log IDs
        mockPartitionMetadata(streamId, 0, 200L, Map.of());
        mockPartitionMetadata(streamId, 1, 201L, Map.of());
        mockPartitionMetadata(streamId, 2, 202L, Map.of());

        Stream stream = catalog.loadStream(streamId).get();
        assertEquals(streamId, stream.identifier());
        assertEquals(3, stream.partitioning().numPartitions());
        assertEquals(Map.of("env", "prod"), stream.properties());

        // Pin one active config snapshot for the layout, then verify it once after all metadata
        // reads. The config read count must not grow with the number of partitions.
        verify(oxiaClient, times(2)).get(catalogPaths.streamConfigPath(streamId));
        verify(oxiaClient).get(catalogPaths.partitionMetadataPath(streamId, 0));
        verify(oxiaClient).get(catalogPaths.partitionMetadataPath(streamId, 1));
        verify(oxiaClient).get(catalogPaths.partitionMetadataPath(streamId, 2));
    }

    @Test
    void loadStream_notFound() {
        when(oxiaClient.get("/admin/streams/public/default/my-topic"))
            .thenReturn(CompletableFuture.completedFuture(null));

        ExecutionException ex = assertThrows(ExecutionException.class, () ->
            catalog.loadStream(streamId).get());
        assertInstanceOf(NoSuchStreamException.class, ex.getCause());
    }

    @Test
    void loadStream_tolerantOfNotYetRegisteredSiblingPartitions() throws Exception {
        // External partitions register lazily (one per compaction task) and the count grows to the
        // highest index seen — so partition 2 can be registered while 0 and 1 are not yet written.
        // loadStream (used only by the materialization worker, which never touches the layout) must
        // still succeed so materializing partition 2 does not wait on its siblings being compacted.
        mockStreamConfig(streamId, 3, Map.of("env", "prod"));
        mockPartitionMetadata(streamId, 2, 202L, Map.of());
        when(oxiaClient.get(catalogPaths.partitionMetadataPath(streamId, 0)))
            .thenReturn(CompletableFuture.completedFuture(null));
        when(oxiaClient.get(catalogPaths.partitionMetadataPath(streamId, 1)))
            .thenReturn(CompletableFuture.completedFuture(null));

        Stream stream = catalog.loadStream(streamId).get();
        assertEquals(streamId, stream.identifier());
        assertEquals(3, stream.partitioning().numPartitions());
        assertEquals(Map.of("env", "prod"), stream.properties());
    }

    @Test
    void loadStreamRejectsPartitionFromDifferentIncarnation() throws Exception {
        String configPath = catalogPaths.streamConfigPath(streamId);
        String partitionPath = catalogPaths.partitionMetadataPath(streamId, 0);
        when(oxiaClient.get(configPath)).thenReturn(CompletableFuture.completedFuture(
            new GetResult(configPath, ownedStreamConfigBytes(
                1, Map.of(), "current-incarnation", "current-owner", "EXTERNAL"),
                DUMMY_VERSION)));
        byte[] staleMetadata = LOG_METADATA_SERDE.serialize(
            partitionPath, new LogMetadata(
                202L, Map.of(), OptionalLong.empty(),
                "stale-incarnation", "stale-owner", 1L, false));
        when(oxiaClient.get(partitionPath)).thenReturn(CompletableFuture.completedFuture(
            new GetResult(partitionPath, staleMetadata, DUMMY_VERSION)));

        ExecutionException failure = assertThrows(ExecutionException.class, () ->
            catalog.loadStream(streamId).get());

        assertInstanceOf(
            IndexedStreamCatalog.PartitionMetadataFenceViolationException.class,
            failure.getCause());
    }

    @Test
    void loadStreamRejectsPartitionTombstonedDuringLayoutRead() throws Exception {
        String configPath = catalogPaths.streamConfigPath(streamId);
        String partitionPath = catalogPaths.partitionMetadataPath(streamId, 0);
        mockVersionedConfig(configPath, streamConfigBytes(1, Map.of(), false));
        byte[] activeMetadata = LOG_METADATA_SERDE.serialize(
            partitionPath, new LogMetadata(203L, Map.of(), OptionalLong.empty()));
        AtomicReference<VersionedValue> partition =
            mockVersionedConfig(partitionPath, activeMetadata);
        CompletableFuture<GetResult> delayedLayoutRead = new CompletableFuture<>();
        AtomicInteger partitionReads = new AtomicInteger();
        when(oxiaClient.get(partitionPath)).thenAnswer(ignored -> {
            if (partitionReads.getAndIncrement() == 0) {
                return delayedLayoutRead;
            }
            VersionedValue current = partition.get();
            return CompletableFuture.completedFuture(new GetResult(
                partitionPath, current.value(), current.version()));
        });

        CompletableFuture<Stream> loading = catalog.loadStream(streamId);
        assertFalse(loading.isDone());
        assertTrue(catalog.dropStream(streamId, false).join());
        VersionedValue tombstone = partition.get();
        delayedLayoutRead.complete(new GetResult(
            partitionPath, tombstone.value(), tombstone.version()));

        CompletionException failure = assertThrows(CompletionException.class, loading::join);
        assertInstanceOf(
            IndexedStreamCatalog.PartitionMetadataFenceViolationException.class,
            failure.getCause());
    }

    @Test
    void getLayout_strictlyRequiresAllPartitions() {
        // The public getLayout stays strict: native readers need a complete layout, so a missing
        // partition surfaces as NoSuchStreamException rather than a placeholder.
        mockStreamConfig(streamId, 3, Map.of());
        mockPartitionMetadata(streamId, 2, 202L, Map.of());
        when(oxiaClient.get(catalogPaths.partitionMetadataPath(streamId, 0)))
            .thenReturn(CompletableFuture.completedFuture(null));
        when(oxiaClient.get(catalogPaths.partitionMetadataPath(streamId, 1)))
            .thenReturn(CompletableFuture.completedFuture(null));

        ExecutionException ex = assertThrows(ExecutionException.class, () ->
            catalog.getLayout(streamId).get());
        assertInstanceOf(NoSuchStreamException.class, ex.getCause());
    }

    // --- listStreams ---

    @Test
    void listStreams_success() throws Exception {
        String prefix = "/admin/streams/public/default/";
        when(oxiaClient.list(eq(prefix), eq(prefix + "\uffff")))
            .thenReturn(CompletableFuture.completedFuture(List.of(
                "/admin/streams/public/default/topic-a",
                "/admin/streams/public/default/topic-b"
            )));
        when(oxiaClient.get("/admin/streams/public/default/topic-a"))
            .thenReturn(CompletableFuture.completedFuture(
                new GetResult("topic-a", streamConfigBytes(1, Map.of(), false), DUMMY_VERSION)));
        when(oxiaClient.get("/admin/streams/public/default/topic-b"))
            .thenReturn(CompletableFuture.completedFuture(
                new GetResult("topic-b", streamConfigBytes(1, Map.of(), false), DUMMY_VERSION)));

        List<StreamIdentifier> streams = catalog.listStreams("public/default").get();
        assertEquals(2, streams.size());
        assertEquals("topic-a", streams.get(0).name());
        assertEquals("topic-b", streams.get(1).name());
    }

    @Test
    void listStreams_excludesPermanentDeletionTombstones() throws Exception {
        String prefix = "/admin/streams/public/default/";
        String activePath = prefix + "active";
        String deletedPath = prefix + "deleted";
        when(oxiaClient.list(eq(prefix), eq(prefix + "\uffff")))
            .thenReturn(CompletableFuture.completedFuture(List.of(activePath, deletedPath)));
        when(oxiaClient.get(activePath)).thenReturn(CompletableFuture.completedFuture(
            new GetResult(activePath, streamConfigBytes(1, Map.of(), false), DUMMY_VERSION)));
        when(oxiaClient.get(deletedPath)).thenReturn(CompletableFuture.completedFuture(
            new GetResult(deletedPath,
                "{\"_externalStreamPermanentlyDeleted\":true}"
                    .getBytes(StandardCharsets.UTF_8),
                DUMMY_VERSION)));

        assertEquals(
            List.of(new StreamIdentifier("public/default", "active")),
            catalog.listStreams("public/default").get());
    }

    @Test
    void listStreams_boundsConcurrentVisibilityReads() throws Exception {
        String prefix = "/admin/streams/public/default/";
        List<String> paths = IntStream.range(0, 34)
            .mapToObj(index -> prefix + "topic-" + index)
            .toList();
        when(oxiaClient.list(eq(prefix), eq(prefix + "\uffff")))
            .thenReturn(CompletableFuture.completedFuture(paths));
        Map<String, CompletableFuture<GetResult>> pendingReads = new HashMap<>();
        paths.forEach(path -> pendingReads.put(path, new CompletableFuture<>()));
        when(oxiaClient.get(any(String.class))).thenAnswer(invocation ->
            pendingReads.get(invocation.getArgument(0, String.class)));

        CompletableFuture<List<StreamIdentifier>> result = catalog.listStreams("public/default");

        verify(oxiaClient, times(32)).get(any(String.class));
        assertFalse(result.isDone());
        for (int index = 0; index < 32; index++) {
            String path = paths.get(index);
            byte[] value = index == 5
                ? "{\"_externalStreamPermanentlyDeleted\":true}"
                    .getBytes(StandardCharsets.UTF_8)
                : streamConfigBytes(1, Map.of(), false);
            pendingReads.get(path).complete(new GetResult(path, value, DUMMY_VERSION));
        }

        verify(oxiaClient, times(34)).get(any(String.class));
        assertFalse(result.isDone());
        pendingReads.get(paths.get(32)).complete(
            new GetResult(paths.get(32),
                streamConfigBytes(1, Map.of(), false), DUMMY_VERSION));
        pendingReads.get(paths.get(33)).complete(new GetResult(
            paths.get(33),
            "{\"_externalStreamPermanentlyDeleted\":true}"
                .getBytes(StandardCharsets.UTF_8),
            DUMMY_VERSION));

        assertEquals(32, result.get().size());
        assertFalse(result.get().contains(new StreamIdentifier("public/default", "topic-5")));
        assertFalse(result.get().contains(new StreamIdentifier("public/default", "topic-33")));
    }

    // --- streamExists ---

    @Test
    void streamExists_true() throws Exception {
        when(oxiaClient.get("/admin/streams/public/default/my-topic"))
            .thenReturn(CompletableFuture.completedFuture(
                new GetResult("key", streamConfigBytes(1, Map.of(), false), DUMMY_VERSION)));
        assertTrue(catalog.streamExists(streamId).get());
    }

    @Test
    void streamExists_false() throws Exception {
        when(oxiaClient.get("/admin/streams/public/default/my-topic"))
            .thenReturn(CompletableFuture.completedFuture(null));
        assertFalse(catalog.streamExists(streamId).get());
    }

    // --- dropStream ---

    @Test
    void dropStream_success() throws Exception {
        String configPath = catalogPaths.streamConfigPath(streamId);
        String partitionPath = catalogPaths.partitionMetadataPath(streamId, 0);
        AtomicReference<VersionedValue> config = mockVersionedConfig(
            configPath, ownedStreamConfigBytes(
                1, Map.of(), "drop-success-incarnation", "drop-success-owner",
                "NATIVE_CREATE"));
        AtomicReference<VersionedValue> partition = mockCreateOnlyRecord(partitionPath);

        boolean result = catalog.dropStream(streamId, false).get();

        assertTrue(result);
        assertEquals("DROPPED", MAPPER.readTree(config.get().value())
            .get("_provisioningState").asText());
        LogMetadata tombstone = LOG_METADATA_SERDE.deserialize(
            partitionPath, partition.get().value());
        assertTrue(tombstone.deleted());
        verify(logStorage, never()).deleteLog(any());
    }

    @Test
    void dropStream_withPurge() throws Exception {
        String configPath = catalogPaths.streamConfigPath(streamId);
        String partitionPath = catalogPaths.partitionMetadataPath(streamId, 0);
        String incarnation = "purging-drop-incarnation";
        String ownerToken = "purging-drop-owner";
        mockVersionedConfig(configPath, ownedStreamConfigBytes(
            1, Map.of(), incarnation, ownerToken, "NATIVE_CREATE"));
        byte[] metadata = LOG_METADATA_SERDE.serialize(
            partitionPath, new LogMetadata(
                300L, Map.of(), OptionalLong.empty(),
                incarnation, ownerToken, 1L, false));
        AtomicReference<VersionedValue> partition =
            mockVersionedConfig(partitionPath, metadata);
        when(logStorage.deleteLog(LogId.of(300L)))
            .thenReturn(CompletableFuture.completedFuture(null));

        boolean result = catalog.dropStream(streamId, true).get();

        assertTrue(result);
        verify(logStorage).deleteLog(LogId.of(300L));
        assertTrue(LOG_METADATA_SERDE.deserialize(
            partitionPath, partition.get().value()).deleted());
    }

    @Test
    void repeatedPurgingDropKeepsDurableFenceAndClearsCleanupJournal()
            throws Exception {
        long originalStreamId = 300L;
        long lateStreamId = 301L;
        String configPath = catalogPaths.streamConfigPath(streamId);
        String partitionPath = catalogPaths.partitionMetadataPath(streamId, 0);
        String mappingKey = catalogPaths.compactedReaderName(streamId, 0);
        String incarnation = "external-purge-recovery";
        AtomicReference<VersionedValue> config = mockVersionedConfig(
            configPath, ownedStreamConfigBytes(
                1, Map.of(), incarnation, "registration-owner", "EXTERNAL"));
        byte[] metadata = LOG_METADATA_SERDE.serialize(
            partitionPath, new LogMetadata(
                originalStreamId, Map.of(), OptionalLong.empty(),
                incarnation, "registration-owner", 1L, false));
        AtomicReference<VersionedValue> partition =
            mockVersionedConfig(partitionPath, metadata);
        when(logStorage.deleteLog(LogId.of(originalStreamId)))
            .thenReturn(CompletableFuture.completedFuture(null));
        FencedStorageHarness mappings = new FencedStorageHarness(
            key -> CompletableFuture.completedFuture(lateStreamId));
        StreamIdMappingOwner owner = new StreamIdMappingOwner(
            incarnation, "registration-owner", 1L);
        mappings.setActive(mappingKey, originalStreamId, owner);
        IndexedStreamCatalog recoveringCatalog = fencedCatalog(mappings);

        assertTrue(recoveringCatalog.dropStream(streamId, true).join());
        assertEquals(Optional.empty(), mappings.activeStreamId(mappingKey));
        StreamIdMappingFence durableFence = mappings.fence(mappingKey).orElseThrow();
        assertEquals(originalStreamId, durableFence.streamId());
        assertTrue(MAPPER.readTree(config.get().value())
            .path("_purgeRequested").asBoolean());

        CompletionException unacknowledged = assertThrows(CompletionException.class, () ->
            mappings.allocate(mappingKey, new StreamIdMappingOwner(
                "replacement-incarnation", "replacement-owner", 0L), Optional.empty()).join());
        assertInstanceOf(IllegalStateException.class, unacknowledged.getCause());
        assertFalse(recoveringCatalog.dropStream(streamId, true).join());

        LogMetadata recovered = LOG_METADATA_SERDE.deserialize(
            partitionPath, partition.get().value());
        assertTrue(recovered.retiredStreamIds().isEmpty());
        assertTrue(recovered.purgeableRetiredStreamIds().isEmpty());
        assertTrue(recovered.retiredStreamMappings().isEmpty());
        assertTrue(recovered.retiredMappingKeys().isEmpty());
        verify(logStorage).deleteLog(LogId.of(originalStreamId));
        verify(logStorage, never()).deleteLog(LogId.of(lateStreamId));
    }

    @Test
    void dropStream_notFound() throws Exception {
        mockStreamExistence(streamId, false);
        assertFalse(catalog.dropStream(streamId, false).get());
    }

    @Test
    void dropWithoutKeyedLifecycleCapabilityFailsBeforeWritingAbortingClaim()
            throws Exception {
        String configPath = catalogPaths.streamConfigPath(streamId);
        byte[] original = streamConfigBytes(1, Map.of(), false);
        AtomicReference<VersionedValue> config =
            mockVersionedConfig(configPath, original);
        IndexedStreamCatalog legacyFactoryCatalog = new IndexedStreamCatalog(
            oxiaClient, catalogPaths, logStorage, logId -> null, null,
            key -> CompletableFuture.completedFuture(1L), null, null, List.of());
        IndexedStreamCatalog namedFactoryCatalog = new IndexedStreamCatalog(
            oxiaClient, catalogPaths, logStorage,
            (name, logId, reader) -> null, null,
            key -> CompletableFuture.completedFuture(1L), null, null, List.of());

        for (IndexedStreamCatalog noCapabilities
                : List.of(legacyFactoryCatalog, namedFactoryCatalog)) {
            CompletionException failure = assertThrows(CompletionException.class, () ->
                noCapabilities.dropStream(streamId, false).join());
            assertInstanceOf(UnsupportedOperationException.class, failure.getCause());
            assertEquals(new String(original, StandardCharsets.UTF_8),
                new String(config.get().value(), StandardCharsets.UTF_8));
        }
    }

    @Test
    void disabledConditionalDeletionFailsDestructiveEntryPointsBeforeSideEffects() {
        AtomicInteger allocationAttempts = new AtomicInteger();
        AtomicInteger lookupAttempts = new AtomicInteger();
        AtomicInteger deletionAttempts = new AtomicInteger();
        IndexedStreamCatalog noCapabilities = new IndexedStreamCatalog(
            oxiaClient, catalogPaths, logStorage,
            (name, logId, reader) -> null, null,
            key -> CompletableFuture.completedFuture(1L),
            key -> {
                allocationAttempts.incrementAndGet();
                return CompletableFuture.completedFuture(
                    new StreamIdAllocation(1L, true));
            },
            key -> {
                lookupAttempts.incrementAndGet();
                return CompletableFuture.completedFuture(1L);
            },
            (key, expectedStreamId) -> {
                deletionAttempts.incrementAndGet();
                return CompletableFuture.completedFuture(null);
            }, null, null, List.of(), false);
        List<CompletableFuture<?>> mutations = List.of(
            noCapabilities.deleteExternalPartition(streamId, 0),
            noCapabilities.dropStream(streamId, true));

        for (CompletableFuture<?> mutation : mutations) {
            assertInstanceOf(UnsupportedOperationException.class,
                assertThrows(CompletionException.class, mutation::join).getCause());
        }
        assertEquals(0, allocationAttempts.get());
        assertEquals(0, lookupAttempts.get());
        assertEquals(0, deletionAttempts.get());
        verify(oxiaClient, never()).put(anyString(), any(byte[].class), any());
        verify(logStorage, never()).deleteLog(any());
    }

    @SuppressWarnings("deprecation")
    @Test
    void legacyUnconditionalMappingDeleterConstructorIsFailClosed() {
        AtomicInteger allocationAttempts = new AtomicInteger();
        AtomicInteger lookupAttempts = new AtomicInteger();
        AtomicInteger deletionAttempts = new AtomicInteger();
        IndexedStreamCatalog noCapabilities = new IndexedStreamCatalog(
            oxiaClient, catalogPaths, logStorage,
            (name, logId, reader) -> null, null,
            key -> {
                allocationAttempts.incrementAndGet();
                return CompletableFuture.completedFuture(1L);
            },
            key -> {
                lookupAttempts.incrementAndGet();
                return CompletableFuture.completedFuture(1L);
            },
            key -> {
                deletionAttempts.incrementAndGet();
                return CompletableFuture.completedFuture(null);
            }, null, null, List.of());

        CompletionException failure = assertThrows(CompletionException.class, () ->
            noCapabilities.deleteExternalPartition(streamId, 0).join());

        assertInstanceOf(UnsupportedOperationException.class, failure.getCause());
        assertEquals(0, allocationAttempts.get());
        assertEquals(0, lookupAttempts.get());
        assertEquals(0, deletionAttempts.get());
        verify(oxiaClient, never()).put(anyString(), any(byte[].class), any());
        verify(logStorage, never()).deleteLog(any());
    }

    @SuppressWarnings("deprecation")
    @Test
    void legacyConstructorStillAcceptsNullOptionalMappingCallbacks() {
        IndexedStreamCatalog noCapabilities = new IndexedStreamCatalog(
            oxiaClient, catalogPaths, logStorage,
            (name, logId, reader) -> null, null,
            key -> CompletableFuture.completedFuture(1L),
            null, null, null, null, List.of());

        CompletionException failure = assertThrows(CompletionException.class, () ->
            noCapabilities.deleteExternalPartition(streamId, 0).join());

        assertInstanceOf(UnsupportedOperationException.class, failure.getCause());
        verify(oxiaClient, never()).put(anyString(), any(byte[].class), any());
        verify(logStorage, never()).deleteLog(any());
    }

    @Test
    void nativeDropRetainsAbortingAnchorAndRetriesConditionalMappingCleanupWithoutPurge()
            throws Exception {
        String configPath = catalogPaths.streamConfigPath(streamId);
        String partitionPath = catalogPaths.partitionMetadataPath(streamId, 0);
        String incarnation = "native-incarnation";
        String mappingKey = "lakestream-native/" + streamId.fullName()
            + "/partition-0";
        AtomicReference<VersionedValue> config = mockVersionedConfig(
            configPath,
            ownedStreamConfigBytes(
                1, Map.of("tier", "hot"), incarnation, "create-owner", "NATIVE_CREATE"));
        byte[] metadata = LOG_METADATA_SERDE.serialize(
            partitionPath, new LogMetadata(
                300L, Map.of(), OptionalLong.empty(),
                incarnation, "create-owner", 1L, false));
        AtomicReference<VersionedValue> partition =
            mockVersionedConfig(partitionPath, metadata);
        RuntimeException firstCleanupFailure = new RuntimeException("mapping delete failed");
        FencedStorageHarness mappings = new FencedStorageHarness(
            key -> CompletableFuture.completedFuture(999L));
        mappings.setActive(mappingKey, 300L,
            new StreamIdMappingOwner(incarnation, "create-owner", 1L));
        mappings.failNextFence(firstCleanupFailure);
        IndexedStreamCatalog mappedCatalog = fencedCatalog(mappings);
        mappedCatalog.initialize("mapped-catalog", Map.of()).join();

        CompletionException firstFailure = assertThrows(CompletionException.class, () ->
            mappedCatalog.dropStream(streamId, false).join());

        assertEquals(firstCleanupFailure, firstFailure.getCause());
        assertNotNull(config.get());
        JsonNode abortingConfig = MAPPER.readTree(config.get().value());
        assertEquals("ABORTING", abortingConfig.get("_provisioningState").asText());
        assertEquals(incarnation, abortingConfig.get("_incarnationId").asText());
        assertFalse(mappedCatalog.streamExists(streamId).join());
        LogMetadata firstTombstone = LOG_METADATA_SERDE.deserialize(
            partitionPath, partition.get().value());
        assertTrue(firstTombstone.deleted());
        assertEquals(300L, firstTombstone.streamId());
        verify(logStorage, never()).deleteLog(any());

        assertTrue(mappedCatalog.dropStream(streamId, false).join());

        assertEquals("DROPPED", MAPPER.readTree(config.get().value())
            .get("_provisioningState").asText());
        assertEquals(2, mappings.fenceAttempts().size());
        assertEquals(300L, mappings.fence(mappingKey).orElseThrow().streamId());
        LogMetadata completedTombstone = LOG_METADATA_SERDE.deserialize(
            partitionPath, partition.get().value());
        assertTrue(completedTombstone.deleted());
        assertEquals(incarnation, completedTombstone.registrationIncarnationId());
        verify(logStorage, never()).deleteLog(any());
    }

    @Test
    void externalDropFencesKeyedMappingWithoutPurgingData() throws Exception {
        String configPath = catalogPaths.streamConfigPath(streamId);
        String partitionPath = catalogPaths.partitionMetadataPath(streamId, 0);
        String mappingKey = catalogPaths.compactedReaderName(streamId, 0);
        String incarnation = "external-incarnation";
        AtomicReference<VersionedValue> config = mockVersionedConfig(
            configPath,
            ownedStreamConfigBytes(
                1, Map.of(), incarnation, "registration-owner", "EXTERNAL"));
        byte[] metadata = LOG_METADATA_SERDE.serialize(
            partitionPath, new LogMetadata(
                300L, Map.of(), OptionalLong.empty(),
                incarnation, "registration-owner", 1L, false));
        AtomicReference<VersionedValue> partition =
            mockVersionedConfig(partitionPath, metadata);
        FencedStorageHarness mappings = new FencedStorageHarness(
            key -> CompletableFuture.completedFuture(999L));
        mappings.setActive(mappingKey, 300L,
            new StreamIdMappingOwner(incarnation, "registration-owner", 1L));
        IndexedStreamCatalog mappedCatalog = fencedCatalog(mappings);

        assertTrue(mappedCatalog.dropStream(streamId, false).join());

        assertEquals("DROPPED", MAPPER.readTree(config.get().value())
            .get("_provisioningState").asText());
        assertEquals(Optional.empty(), mappings.activeStreamId(mappingKey));
        assertEquals(300L, mappings.fence(mappingKey).orElseThrow().streamId());
        LogMetadata tombstone = LOG_METADATA_SERDE.deserialize(
            partitionPath, partition.get().value());
        assertTrue(tombstone.deleted());
        assertEquals(1L, tombstone.registrationOwnerGeneration());
        verify(logStorage, never()).deleteLog(any());
    }

    @Test
    void lateSealCannotOverwritePartitionTombstoneAfterDrop() throws Exception {
        String configPath = catalogPaths.streamConfigPath(streamId);
        String partitionPath = catalogPaths.partitionMetadataPath(streamId, 0);
        String incarnation = "external-incarnation";
        mockVersionedConfig(
            configPath,
            ownedStreamConfigBytes(
                1, Map.of(), incarnation, "registration-owner", "EXTERNAL"));
        byte[] initialMetadata = LOG_METADATA_SERDE.serialize(
            partitionPath, new LogMetadata(
                301L, Map.of(), OptionalLong.empty(),
                incarnation, "registration-owner", 1L, false));
        AtomicReference<VersionedValue> partition = new AtomicReference<>(
            new VersionedValue(initialMetadata, version(1)));
        AtomicLong nextPartitionVersion = new AtomicLong(1L);
        CompletableFuture<PutResult> delayedSealWrite = new CompletableFuture<>();
        AtomicInteger partitionWrites = new AtomicInteger();
        when(oxiaClient.get(partitionPath)).thenAnswer(ignored -> {
            VersionedValue current = partition.get();
            return CompletableFuture.completedFuture(new GetResult(
                partitionPath, current.value(), current.version()));
        });
        when(oxiaClient.put(eq(partitionPath), any(byte[].class), any()))
            .thenAnswer(invocation -> {
                if (partitionWrites.getAndIncrement() == 0) {
                    return delayedSealWrite;
                }
                @SuppressWarnings("unchecked")
                Set<PutOption> options = invocation.getArgument(2, Set.class);
                VersionedValue current = partition.get();
                if (!options.contains(
                        PutOption.IfVersionIdEquals(current.version().versionId()))) {
                    return CompletableFuture.failedFuture(
                        new UnexpectedVersionIdException(
                            partitionPath, current.version().versionId()));
                }
                Version next = version(nextPartitionVersion.incrementAndGet());
                byte[] value = invocation.getArgument(1, byte[].class);
                partition.set(new VersionedValue(value.clone(), next));
                return CompletableFuture.completedFuture(new PutResult(partitionPath, next));
            });

        CompletableFuture<Void> staleSeal = catalog.sealStream(streamId);
        assertFalse(staleSeal.isDone());
        assertTrue(catalog.dropStream(streamId, false).join());

        delayedSealWrite.completeExceptionally(
            new UnexpectedVersionIdException(partitionPath, version(1).versionId()));
        CompletionException sealFailure = assertThrows(CompletionException.class, staleSeal::join);
        assertInstanceOf(NoSuchStreamException.class, sealFailure.getCause());
        LogMetadata tombstone = LOG_METADATA_SERDE.deserialize(
            partitionPath, partition.get().value());
        assertTrue(tombstone.deleted());
        assertEquals(301L, tombstone.streamId());
    }

    @Test
    void newNativeIncarnationCasReplacesOldPartitionTombstone() throws Exception {
        String configPath = catalogPaths.streamConfigPath(streamId);
        String partitionPath = catalogPaths.partitionMetadataPath(streamId, 0);
        AtomicReference<VersionedValue> config = mockVersionedConfig(
            configPath, droppedStreamConfigBytes(
                1, "old-incarnation", "old-drop-owner", 2L, "NATIVE_CREATE"));
        byte[] oldTombstone = LOG_METADATA_SERDE.serialize(
            partitionPath, new LogMetadata(
                300L, Map.of(), OptionalLong.empty(),
                "old-incarnation", "old-drop-owner", 2L, true));
        AtomicReference<VersionedValue> partition =
            mockVersionedConfig(partitionPath, oldTombstone);
        nextStreamId = 400L;
        defaultStorage.setFence(
            "lakestream-native/" + streamId.fullName() + "/partition-0",
            new StreamIdMappingFence(
                300L, new StreamIdMappingOwner(
                    "old-incarnation", "old-drop-owner", 2L)));

        Stream created = catalog.createStream(
            streamId, new StreamConfig(),
            new Partitioning(PartitioningStrategy.INDEXED, Map.of("numPartitions", "1")),
            new SchemaConfig(), Map.of()).join();

        assertEquals(streamId, created.identifier());
        JsonNode activeConfig = MAPPER.readTree(config.get().value());
        String newIncarnation = activeConfig.get("_incarnationId").asText();
        assertFalse(activeConfig.has("_provisioning"));
        assertFalse(newIncarnation.equals("old-incarnation"));
        LogMetadata replacement = LOG_METADATA_SERDE.deserialize(
            partitionPath, partition.get().value());
        assertFalse(replacement.deleted());
        assertEquals(400L, replacement.streamId());
        assertEquals(newIncarnation, replacement.registrationIncarnationId());
        assertEquals(activeConfig.get("_ownerToken").asText(),
            replacement.registrationOwnerToken());
    }

    // --- Namespace CRUD ---

    @Test
    void createNamespace_success() throws Exception {
        when(oxiaClient.get("/admin/streams/_namespaces/my-ns"))
            .thenReturn(CompletableFuture.completedFuture(null));
        when(oxiaClient.put(any(), any(byte[].class)))
            .thenReturn(CompletableFuture.completedFuture(new PutResult("key", DUMMY_VERSION)));

        catalog.createNamespace(new Namespace("my-ns", Map.of("key", "val"))).get();

        verify(oxiaClient).put(eq("/admin/streams/_namespaces/my-ns"), any(byte[].class));
    }

    @Test
    void createNamespace_alreadyExists() {
        when(oxiaClient.get("/admin/streams/_namespaces/my-ns"))
            .thenReturn(CompletableFuture.completedFuture(
                new GetResult("key", new byte[]{}, DUMMY_VERSION)));

        ExecutionException ex = assertThrows(ExecutionException.class, () ->
            catalog.createNamespace(new Namespace("my-ns")).get());
        assertInstanceOf(AlreadyExistsException.class, ex.getCause());
    }

    @Test
    void listNamespaces_success() throws Exception {
        String prefix = "/admin/streams/_namespaces/";
        when(oxiaClient.list(eq(prefix), eq(prefix + "\uffff")))
            .thenReturn(CompletableFuture.completedFuture(List.of(
                "/admin/streams/_namespaces/ns-a",
                "/admin/streams/_namespaces/ns-b")));

        mockNamespaceMetadata("ns-a", Map.of());
        mockNamespaceMetadata("ns-b", Map.of("x", "y"));

        List<Namespace> namespaces = catalog.listNamespaces().get();
        assertEquals(2, namespaces.size());
        assertEquals("ns-a", namespaces.get(0).name());
        assertEquals("ns-b", namespaces.get(1).name());
    }

    @Test
    void loadNamespaceMetadata_notFound() {
        when(oxiaClient.get("/admin/streams/_namespaces/missing-ns"))
            .thenReturn(CompletableFuture.completedFuture(null));

        CompletionException ex = assertThrows(CompletionException.class, () ->
            catalog.loadNamespaceMetadata("missing-ns").join());
        assertInstanceOf(NoSuchNamespaceException.class, ex.getCause());
    }

    @Test
    void namespaceExists_true() throws Exception {
        when(oxiaClient.get("/admin/streams/_namespaces/my-ns"))
            .thenReturn(CompletableFuture.completedFuture(
                new GetResult("key", new byte[]{}, DUMMY_VERSION)));
        assertTrue(catalog.namespaceExists("my-ns").get());
    }

    @Test
    void namespaceExists_false() throws Exception {
        when(oxiaClient.get("/admin/streams/_namespaces/my-ns"))
            .thenReturn(CompletableFuture.completedFuture(null));
        assertFalse(catalog.namespaceExists("my-ns").get());
    }

    @Test
    void dropNamespace_success() throws Exception {
        mockNamespaceMetadata("my-ns", Map.of());
        // namespace has no streams — listStreams now scans streamConfigPrefix
        String configPrefix = "/admin/streams/my-ns/";
        when(oxiaClient.list(eq(configPrefix), eq(configPrefix + "\uffff")))
            .thenReturn(CompletableFuture.completedFuture(List.of()));
        when(oxiaClient.delete("/admin/streams/_namespaces/my-ns"))
            .thenReturn(CompletableFuture.completedFuture(true));

        assertTrue(catalog.dropNamespace("my-ns").get());
        verify(oxiaClient).delete("/admin/streams/_namespaces/my-ns");
    }

    @Test
    void dropNamespaceIgnoresRetainedUnregisteredStream() throws Exception {
        String namespaceName = "my-ns";
        StreamIdentifier externalStream =
            new StreamIdentifier(namespaceName, "some-topic");
        String streamPath = catalogPaths.streamConfigPath(externalStream);
        mockNamespaceMetadata(namespaceName, Map.of());
        AtomicReference<VersionedValue> config = mockVersionedConfig(
            streamPath, streamConfigBytes(1, Map.of(), false));
        String configPrefix = "/admin/streams/" + namespaceName + "/";
        when(oxiaClient.list(eq(configPrefix), eq(configPrefix + "\uffff")))
            .thenReturn(CompletableFuture.completedFuture(List.of(streamPath)));
        when(oxiaClient.delete("/admin/streams/_namespaces/" + namespaceName))
            .thenReturn(CompletableFuture.completedFuture(true));

        catalog.unregisterExternalStream(externalStream).join();

        assertEquals("UNREGISTERED", MAPPER.readTree(config.get().value())
            .path("_provisioningState").asText());
        assertTrue(catalog.dropNamespace(namespaceName).join());
        verify(oxiaClient).delete("/admin/streams/_namespaces/" + namespaceName);
    }

    @Test
    void dropNamespace_notEmpty() {
        mockNamespaceMetadata("my-ns", Map.of());
        String configPrefix = "/admin/streams/my-ns/";
        when(oxiaClient.list(eq(configPrefix), eq(configPrefix + "\uffff")))
            .thenReturn(CompletableFuture.completedFuture(
                List.of("/admin/streams/my-ns/some-topic")));
        when(oxiaClient.get("/admin/streams/my-ns/some-topic"))
            .thenReturn(CompletableFuture.completedFuture(new GetResult(
                "/admin/streams/my-ns/some-topic", new byte[]{}, DUMMY_VERSION)));

        ExecutionException ex = assertThrows(ExecutionException.class, () ->
            catalog.dropNamespace("my-ns").get());
        assertInstanceOf(NamespaceNotEmptyException.class, ex.getCause());
    }

    @Test
    void dropNamespaceConservativelyRejectsHiddenProvisioningStream() {
        mockNamespaceMetadata("my-ns", Map.of());
        String configPrefix = "/admin/streams/my-ns/";
        String streamPath = configPrefix + "provisioning-topic";
        when(oxiaClient.list(eq(configPrefix), eq(configPrefix + "\uffff")))
            .thenReturn(CompletableFuture.completedFuture(List.of(streamPath)));
        when(oxiaClient.get(streamPath)).thenReturn(CompletableFuture.completedFuture(
            new GetResult(streamPath,
                ("{\"partitions\":1,\"properties\":{},"
                    + "\"_incarnationId\":\"inc\",\"_ownerToken\":\"owner\","
                    + "\"_creationKind\":\"NATIVE_CREATE\",\"_provisioning\":true,"
                    + "\"_provisioningState\":\"PROVISIONING\"}")
                    .getBytes(StandardCharsets.UTF_8),
                DUMMY_VERSION)));

        ExecutionException failure = assertThrows(ExecutionException.class, () ->
            catalog.dropNamespace("my-ns").get());

        assertInstanceOf(NamespaceNotEmptyException.class, failure.getCause());
        verify(oxiaClient, never()).delete("/admin/streams/_namespaces/my-ns");
    }

    @Test
    void dropNamespaceConservativelyRejectsHiddenAbortingStream() {
        mockNamespaceMetadata("my-ns", Map.of());
        String configPrefix = "/admin/streams/my-ns/";
        String streamPath = configPrefix + "aborting-topic";
        when(oxiaClient.list(eq(configPrefix), eq(configPrefix + "\uffff")))
            .thenReturn(CompletableFuture.completedFuture(List.of(streamPath)));
        when(oxiaClient.get(streamPath)).thenReturn(CompletableFuture.completedFuture(
            new GetResult(streamPath,
                ("{\"partitions\":1,\"properties\":{},"
                    + "\"_incarnationId\":\"inc\",\"_ownerToken\":\"drop-owner\","
                    + "\"_creationKind\":\"NATIVE_CREATE\",\"_provisioning\":true,"
                    + "\"_provisioningState\":\"ABORTING\"}")
                    .getBytes(StandardCharsets.UTF_8),
                DUMMY_VERSION)));

        ExecutionException failure = assertThrows(ExecutionException.class, () ->
            catalog.dropNamespace("my-ns").get());

        assertInstanceOf(NamespaceNotEmptyException.class, failure.getCause());
        verify(oxiaClient, never()).delete("/admin/streams/_namespaces/my-ns");
    }

    @Test
    void dropNamespace_notFound() throws Exception {
        when(oxiaClient.get("/admin/streams/_namespaces/missing-ns"))
            .thenReturn(CompletableFuture.completedFuture(null));
        assertFalse(catalog.dropNamespace("missing-ns").get());
    }

    // --- Edge cases ---

    @Test
    void singlePartitionStream() throws Exception {
        mockStreamConfig(streamId, 1);
        mockPartitionMetadata(streamId, 0, 500L, Map.of());

        StreamLayout layout = catalog.getLayout(streamId).get();
        assertEquals(1, layout.logCount());
        List<LogId> logIds = layout.logIds().get();
        assertEquals(LogId.of(500), logIds.get(0));
    }

    @Test
    void name_returnsInitializedName() {
        assertEquals("test-catalog", catalog.name());
    }

    // --- setStreamProperties ---

    @Test
    void setStreamProperties_success() throws Exception {
        mockStreamConfig(streamId, 2, Map.of("a", "1"));

        when(oxiaClient.put(any(), any(byte[].class), any()))
            .thenReturn(CompletableFuture.completedFuture(new PutResult("key", DUMMY_VERSION)));

        catalog.setStreamProperties(streamId, Map.of("b", "2")).get();

        // Properties are now stored at stream config level, not per-partition
        String configPath = catalogPaths.streamConfigPath(streamId);
        verify(oxiaClient).put(eq(configPath), any(byte[].class),
            eq(Set.of(PutOption.IfVersionIdEquals(DUMMY_VERSION.versionId()))));
    }

    // --- removeStreamProperties ---

    @Test
    void removeStreamProperties_success() throws Exception {
        mockStreamConfig(streamId, 1, Map.of("a", "1", "b", "2"));

        when(oxiaClient.put(any(), any(byte[].class), any()))
            .thenReturn(CompletableFuture.completedFuture(new PutResult("key", DUMMY_VERSION)));

        catalog.removeStreamProperties(streamId, List.of("a")).get();

        // Properties are now stored at stream config level, not per-partition
        String configPath = catalogPaths.streamConfigPath(streamId);
        verify(oxiaClient).put(eq(configPath), any(byte[].class),
            eq(Set.of(PutOption.IfVersionIdEquals(DUMMY_VERSION.versionId()))));
    }

    @Test
    void setStreamProperties_retriesWithoutLosingConcurrentPartitionGrowth() throws Exception {
        String configPath = catalogPaths.streamConfigPath(streamId);
        Version grownVersion = new Version(2, 0, 0, 0, Optional.empty(), Optional.empty());
        when(oxiaClient.get(configPath))
            .thenReturn(CompletableFuture.completedFuture(new GetResult(
                configPath, streamConfigBytes(1, Map.of("a", "1"), false), DUMMY_VERSION)))
            .thenReturn(CompletableFuture.completedFuture(new GetResult(
                configPath, streamConfigBytes(4, Map.of("a", "1"), true), grownVersion)));
        when(oxiaClient.put(eq(configPath), any(byte[].class), any()))
            .thenReturn(CompletableFuture.failedFuture(
                new UnexpectedVersionIdException(configPath, DUMMY_VERSION.versionId())))
            .thenReturn(CompletableFuture.completedFuture(new PutResult(configPath, grownVersion)));

        catalog.setStreamProperties(streamId, Map.of("b", "2")).get();

        ArgumentCaptor<byte[]> writes = ArgumentCaptor.forClass(byte[].class);
        verify(oxiaClient, times(2)).put(eq(configPath), writes.capture(), any());
        JsonNode retried = MAPPER.readTree(writes.getAllValues().get(1));
        assertEquals(4, retried.get("partitions").asInt());
        assertEquals("1", retried.get("properties").get("a").asText());
        assertEquals("2", retried.get("properties").get("b").asText());
        assertTrue(retried.get("materialization").get("enabled").asBoolean());
    }

    @Test
    void removeStreamProperties_retriesAgainstLatestPropertiesAndPartitionCount() throws Exception {
        String configPath = catalogPaths.streamConfigPath(streamId);
        Version grownVersion = new Version(2, 0, 0, 0, Optional.empty(), Optional.empty());
        when(oxiaClient.get(configPath))
            .thenReturn(CompletableFuture.completedFuture(new GetResult(
                configPath, streamConfigBytes(1, Map.of("a", "1", "b", "2"), false),
                DUMMY_VERSION)))
            .thenReturn(CompletableFuture.completedFuture(new GetResult(
                configPath, streamConfigBytes(3, Map.of("a", "1", "b", "2", "c", "3"), false),
                grownVersion)));
        when(oxiaClient.put(eq(configPath), any(byte[].class), any()))
            .thenReturn(CompletableFuture.failedFuture(
                new UnexpectedVersionIdException(configPath, DUMMY_VERSION.versionId())))
            .thenReturn(CompletableFuture.completedFuture(new PutResult(configPath, grownVersion)));

        catalog.removeStreamProperties(streamId, List.of("a")).get();

        ArgumentCaptor<byte[]> writes = ArgumentCaptor.forClass(byte[].class);
        verify(oxiaClient, times(2)).put(eq(configPath), writes.capture(), any());
        JsonNode retried = MAPPER.readTree(writes.getAllValues().get(1));
        assertEquals(3, retried.get("partitions").asInt());
        assertFalse(retried.get("properties").has("a"));
        assertEquals("2", retried.get("properties").get("b").asText());
        assertEquals("3", retried.get("properties").get("c").asText());
    }

    @Test
    void clearStreamMaterialization_retriesWithoutLosingConcurrentConfigChanges() throws Exception {
        String configPath = catalogPaths.streamConfigPath(streamId);
        Version grownVersion = new Version(2, 0, 0, 0, Optional.empty(), Optional.empty());
        when(oxiaClient.get(configPath))
            .thenReturn(CompletableFuture.completedFuture(new GetResult(
                configPath, streamConfigBytes(1, Map.of("owner", "old"), true), DUMMY_VERSION)))
            .thenReturn(CompletableFuture.completedFuture(new GetResult(
                configPath, streamConfigBytes(5, Map.of("owner", "latest"), true), grownVersion)));
        when(oxiaClient.put(eq(configPath), any(byte[].class), any()))
            .thenReturn(CompletableFuture.failedFuture(
                new UnexpectedVersionIdException(configPath, DUMMY_VERSION.versionId())))
            .thenReturn(CompletableFuture.completedFuture(new PutResult(configPath, grownVersion)));

        catalog.clearStreamMaterialization(streamId).get();

        ArgumentCaptor<byte[]> writes = ArgumentCaptor.forClass(byte[].class);
        verify(oxiaClient, times(2)).put(eq(configPath), writes.capture(), any());
        JsonNode retried = MAPPER.readTree(writes.getAllValues().get(1));
        assertEquals(5, retried.get("partitions").asInt());
        assertEquals("latest", retried.get("properties").get("owner").asText());
        assertFalse(retried.has("materialization"));
    }

    // --- setNamespaceProperties ---

    @Test
    void setNamespaceProperties_success() throws Exception {
        mockNamespaceMetadata("my-ns", Map.of("a", "1"));

        when(oxiaClient.put(any(), any(byte[].class)))
            .thenReturn(CompletableFuture.completedFuture(new PutResult("key", DUMMY_VERSION)));

        catalog.setNamespaceProperties("my-ns", Map.of("b", "2")).get();

        verify(oxiaClient).put(eq("/admin/streams/_namespaces/my-ns"), any(byte[].class));
    }

    @Test
    void setNamespaceProperties_notFound() {
        when(oxiaClient.get("/admin/streams/_namespaces/missing-ns"))
            .thenReturn(CompletableFuture.completedFuture(null));

        CompletionException ex = assertThrows(CompletionException.class, () ->
            catalog.setNamespaceProperties("missing-ns", Map.of("a", "1")).join());
        assertInstanceOf(NoSuchNamespaceException.class, ex.getCause());
    }

    // --- removeNamespaceProperties ---

    @Test
    void removeNamespaceProperties_success() throws Exception {
        mockNamespaceMetadata("my-ns", Map.of("a", "1", "b", "2"));

        when(oxiaClient.put(any(), any(byte[].class)))
            .thenReturn(CompletableFuture.completedFuture(new PutResult("key", DUMMY_VERSION)));

        catalog.removeNamespaceProperties("my-ns", List.of("a")).get();

        verify(oxiaClient).put(eq("/admin/streams/_namespaces/my-ns"), any(byte[].class));
    }

    @Test
    void removeNamespaceProperties_notFound() {
        when(oxiaClient.get("/admin/streams/_namespaces/missing-ns"))
            .thenReturn(CompletableFuture.completedFuture(null));

        CompletionException ex = assertThrows(CompletionException.class, () ->
            catalog.removeNamespaceProperties("missing-ns", List.of("a")).join());
        assertInstanceOf(NoSuchNamespaceException.class, ex.getCause());
    }

    // --- sealStream ---

    @Test
    void sealStream_success() throws Exception {
        mockStreamConfig(streamId, 2);
        mockPartitionMetadata(streamId, 0, 100L, Map.of());
        mockPartitionMetadata(streamId, 1, 101L, Map.of());

        when(oxiaClient.put(any(), any(byte[].class), any()))
            .thenReturn(CompletableFuture.completedFuture(new PutResult("key", DUMMY_VERSION)));

        catalog.sealStream(streamId).get();

        verify(oxiaClient).put(eq("/streams/public/default/my-topic-partition-0"),
            any(byte[].class),
            eq(Set.of(PutOption.IfVersionIdEquals(DUMMY_VERSION.versionId()))));
        verify(oxiaClient).put(eq("/streams/public/default/my-topic-partition-1"),
            any(byte[].class),
            eq(Set.of(PutOption.IfVersionIdEquals(DUMMY_VERSION.versionId()))));
    }

    @Test
    void sealStream_preservesRetiredPhysicalStreamIds() throws Exception {
        mockStreamConfig(streamId, 1);
        String path = catalogPaths.partitionMetadataPath(streamId, 0);
        LogMetadata metadata = new LogMetadata(
            100L, Map.of(), OptionalLong.empty(), null, null, null, false,
            Set.of(90L, 91L));
        byte[] metadataBytes = LOG_METADATA_SERDE.serialize(path, metadata);
        when(oxiaClient.get(path)).thenReturn(CompletableFuture.completedFuture(
            new GetResult(path, metadataBytes, DUMMY_VERSION)));
        when(oxiaClient.put(eq(path), any(byte[].class), any()))
            .thenReturn(CompletableFuture.completedFuture(
                new PutResult(path, DUMMY_VERSION)));

        catalog.sealStream(streamId).get();

        ArgumentCaptor<byte[]> bytes = ArgumentCaptor.forClass(byte[].class);
        verify(oxiaClient).put(eq(path), bytes.capture(),
            eq(Set.of(PutOption.IfVersionIdEquals(DUMMY_VERSION.versionId()))));
        LogMetadata sealed = LOG_METADATA_SERDE.deserialize(path, bytes.getValue());
        assertEquals(OptionalLong.of(0L), sealed.terminatedOffset());
        assertEquals(Set.of(90L, 91L), sealed.retiredStreamIds());
    }

    @Test
    void sealStream_streamNotFound() {
        when(oxiaClient.get("/admin/streams/public/default/my-topic"))
            .thenReturn(CompletableFuture.completedFuture(null));

        ExecutionException ex = assertThrows(ExecutionException.class, () ->
            catalog.sealStream(streamId).get());
        assertInstanceOf(NoSuchStreamException.class, ex.getCause());
    }

    // --- truncateStream ---

    @Test
    void truncateStream_success() throws Exception {
        mockStreamConfig(streamId, 2);
        mockPartitionMetadata(streamId, 0, 100L, Map.of());
        mockPartitionMetadata(streamId, 1, 101L, Map.of());

        when(logStorage.deleteLog(any()))
            .thenReturn(CompletableFuture.completedFuture(null));

        catalog.truncateStream(streamId).get();

        verify(logStorage).deleteLog(LogId.of(100L));
        verify(logStorage).deleteLog(LogId.of(101L));
    }

    @Test
    void truncateStream_streamNotFound() {
        when(oxiaClient.get("/admin/streams/public/default/my-topic"))
            .thenReturn(CompletableFuture.completedFuture(null));

        ExecutionException ex = assertThrows(ExecutionException.class, () ->
            catalog.truncateStream(streamId).get());
        assertInstanceOf(NoSuchStreamException.class, ex.getCause());
    }

    // --- openWriter ---

    @Test
    void openWriter_success() throws Exception {
        mockStreamConfig(streamId, 1);
        mockPartitionMetadata(streamId, 0, 100L, Map.of());

        StreamWriter writer = catalog.openWriter(streamId).get();
        assertNotNull(writer);
        assertInstanceOf(StreamWriterImpl.class, writer);
    }

    @Test
    void openWriter_streamNotFound() {
        when(oxiaClient.get("/admin/streams/public/default/my-topic"))
            .thenReturn(CompletableFuture.completedFuture(null));

        ExecutionException ex = assertThrows(ExecutionException.class, () ->
            catalog.openWriter(streamId).get());
        assertInstanceOf(NoSuchStreamException.class, ex.getCause());
    }

    // --- openReader ---

    @Test
    void openReader_success() throws Exception {
        mockStreamConfig(streamId, 1);
        mockPartitionMetadata(streamId, 0, 100L, Map.of());

        StreamReader reader = catalog.openReader(streamId).get();
        assertNotNull(reader);
        assertInstanceOf(StreamReaderImpl.class, reader);
    }

    @Test
    void openReader_streamNotFound() {
        when(oxiaClient.get("/admin/streams/public/default/my-topic"))
            .thenReturn(CompletableFuture.completedFuture(null));

        ExecutionException ex = assertThrows(ExecutionException.class, () ->
            catalog.openReader(streamId).get());
        assertInstanceOf(NoSuchStreamException.class, ex.getCause());
    }

    // --- Helpers ---

    private void mockStreamConfig(StreamIdentifier id, int numPartitions) {
        mockStreamConfig(id, numPartitions, Map.of());
    }

    private void mockStreamConfig(StreamIdentifier id, int numPartitions,
                                    Map<String, String> properties) {
        String configPath = catalogPaths.streamConfigPath(id);
        try {
            byte[] bytes = MAPPER.writeValueAsBytes(
                Map.of("partitions", numPartitions, "properties", properties));
            when(oxiaClient.get(configPath))
                .thenReturn(CompletableFuture.completedFuture(
                    new GetResult(configPath, bytes, DUMMY_VERSION)));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private byte[] streamConfigBytes(
            int partitions, Map<String, String> properties, boolean materialization)
            throws Exception {
        ObjectNode config = MAPPER.createObjectNode();
        config.put("partitions", partitions);
        ObjectNode configProperties = config.putObject("properties");
        properties.forEach(configProperties::put);
        if (materialization) {
            config.putObject("materialization").put("enabled", true);
        }
        return MAPPER.writeValueAsBytes(config);
    }

    private byte[] ownedStreamConfigBytes(
            int partitions, Map<String, String> properties,
            String incarnationId, String ownerToken, String creationKind)
            throws Exception {
        ObjectNode config = MAPPER.createObjectNode();
        config.put("partitions", partitions);
        ObjectNode configProperties = config.putObject("properties");
        properties.forEach(configProperties::put);
        config.put("_incarnationId", incarnationId);
        config.put("_ownerToken", ownerToken);
        config.put("_ownerGeneration", 1L);
        config.put("_creationKind", creationKind);
        return MAPPER.writeValueAsBytes(config);
    }

    private byte[] droppedStreamConfigBytes(
            int partitions, String incarnationId, String ownerToken,
            long ownerGeneration, String creationKind) throws Exception {
        ObjectNode config = MAPPER.createObjectNode();
        config.put("partitions", partitions);
        config.putObject("properties");
        config.put("_incarnationId", incarnationId);
        config.put("_ownerToken", ownerToken);
        config.put("_ownerGeneration", ownerGeneration);
        config.put("_metadataSourceGeneration", ownerGeneration - 1);
        config.put("_creationKind", creationKind);
        config.put("_provisioning", true);
        config.put("_provisioningState", "DROPPED");
        return MAPPER.writeValueAsBytes(config);
    }

    private void mockPartitionMetadata(StreamIdentifier id, int partIdx, long partStreamId,
                                        Map<String, String> properties) {
        mockPartitionMetadata(
            id, partIdx, new LogMetadata(
                partStreamId, properties, OptionalLong.empty()));
    }

    private void mockPartitionMetadata(
            StreamIdentifier id, int partIdx, LogMetadata metadata) {
        String path = catalogPaths.partitionMetadataPath(id, partIdx);
        try {
            byte[] bytes = LOG_METADATA_SERDE.serialize(path, metadata);
            when(oxiaClient.get(path))
                .thenReturn(CompletableFuture.completedFuture(
                    new GetResult(path, bytes, DUMMY_VERSION)));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private AtomicReference<VersionedValue> mockVersionedConfig(String path) {
        return mockVersionedConfig(path, null);
    }

    private AtomicReference<VersionedValue> mockVersionedConfig(
            String path, byte[] initialValue) {
        AtomicReference<VersionedValue> state = new AtomicReference<>(initialValue == null
            ? null : new VersionedValue(initialValue.clone(), version(1)));
        AtomicLong nextVersion = new AtomicLong(initialValue == null ? 0L : 1L);
        lenient().when(oxiaClient.get(path)).thenAnswer(ignored -> {
            VersionedValue current = state.get();
            return CompletableFuture.completedFuture(current == null ? null
                : new GetResult(path, current.value(), current.version()));
        });
        lenient().when(oxiaClient.put(eq(path), any(byte[].class), any())).thenAnswer(invocation -> {
            byte[] value = invocation.getArgument(1, byte[].class);
            @SuppressWarnings("unchecked")
            Set<PutOption> options = invocation.getArgument(2, Set.class);
            VersionedValue current = state.get();
            if (options.contains(PutOption.IfRecordDoesNotExist) && current != null) {
                return CompletableFuture.failedFuture(new KeyAlreadyExistsException(path));
            }
            if (!options.contains(PutOption.IfRecordDoesNotExist)
                    && (current == null || !options.contains(
                        PutOption.IfVersionIdEquals(current.version().versionId())))) {
                return CompletableFuture.failedFuture(new UnexpectedVersionIdException(
                    path, current == null ? -1L : current.version().versionId()));
            }
            Version version = version(nextVersion.incrementAndGet());
            state.set(new VersionedValue(value.clone(), version));
            return CompletableFuture.completedFuture(new PutResult(path, version));
        });
        lenient().when(oxiaClient.delete(eq(path), any())).thenAnswer(invocation -> {
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

    private AtomicReference<VersionedValue> mockCreateOnlyRecord(String path) {
        AtomicReference<VersionedValue> state = new AtomicReference<>();
        AtomicLong nextVersion = new AtomicLong();
        lenient().when(oxiaClient.get(path)).thenAnswer(ignored -> {
            VersionedValue current = state.get();
            return CompletableFuture.completedFuture(current == null ? null
                : new GetResult(path, current.value(), current.version()));
        });
        lenient().when(oxiaClient.put(eq(path), any(byte[].class), any()))
            .thenAnswer(invocation -> {
                VersionedValue current = state.get();
                @SuppressWarnings("unchecked")
                Set<PutOption> options = invocation.getArgument(2, Set.class);
                if (options.contains(PutOption.IfRecordDoesNotExist) && current != null) {
                    return CompletableFuture.failedFuture(new KeyAlreadyExistsException(path));
                }
                if (!options.contains(PutOption.IfRecordDoesNotExist)
                        && (current == null || !options.contains(
                            PutOption.IfVersionIdEquals(current.version().versionId())))) {
                    return CompletableFuture.failedFuture(new UnexpectedVersionIdException(
                        path, current == null ? -1L : current.version().versionId()));
                }
                Version version = version(nextVersion.incrementAndGet());
                byte[] value = invocation.getArgument(1, byte[].class);
                state.set(new VersionedValue(value.clone(), version));
                return CompletableFuture.completedFuture(new PutResult(path, version));
            });
        return state;
    }

    private IndexedStreamCatalog fencedCatalog(FencedStorageHarness storage) {
        return new IndexedStreamCatalog(
            oxiaClient, catalogPaths, logStorage,
            (name, logId, reader) -> null, null, storage.storageApi(),
            null, null, List.of());
    }

    private final class FencedStorageHarness {

        private final StorageApi storageApi = mock(StorageApi.class);
        private final Function<String, CompletableFuture<Long>> streamIdAllocator;
        private final Map<String, Object> mappings = new HashMap<>();
        private final List<String> fenceAttempts = new ArrayList<>();
        private final AtomicReference<Throwable> nextFenceFailure = new AtomicReference<>();

        private FencedStorageHarness(
                Function<String, CompletableFuture<Long>> streamIdAllocator) {
            this.streamIdAllocator = streamIdAllocator;
            lenient().when(storageApi.supportsConditionalStreamIdMappingDeletion())
                .thenReturn(true);
            lenient().when(storageApi.supportsFencedStreamIdMappings()).thenReturn(true);
            lenient().when(storageApi.generateStreamId(any())).thenAnswer(invocation -> {
                Optional<String> key = invocation.getArgument(0);
                return streamIdAllocator.apply(key.orElse("unkeyed"));
            });
            lenient().when(storageApi.getStreamIdByKey(anyString()))
                .thenAnswer(invocation -> get(invocation.getArgument(0, String.class)));
            lenient().when(storageApi.allocateStreamId(
                    anyString(), any(StreamIdMappingOwner.class), any()))
                .thenAnswer(invocation -> allocate(
                    invocation.getArgument(0, String.class),
                    invocation.getArgument(1, StreamIdMappingOwner.class),
                    invocation.getArgument(2)));
            lenient().when(storageApi.bindStreamIdMapping(
                    anyString(), anyLong(), any(StreamIdMappingOwner.class), any()))
                .thenAnswer(invocation -> bind(
                    invocation.getArgument(0, String.class),
                    invocation.getArgument(1, Long.class),
                    invocation.getArgument(2, StreamIdMappingOwner.class),
                    invocation.getArgument(3)));
            lenient().when(storageApi.fenceStreamIdMappingState(
                    anyString(), anyLong(), any(StreamIdMappingOwner.class)))
                .thenAnswer(invocation -> fence(
                    invocation.getArgument(0, String.class),
                    invocation.getArgument(1, Long.class),
                    invocation.getArgument(2, StreamIdMappingOwner.class)));
            lenient().when(storageApi.canonicalizeStreamIdMappingFence(
                    anyString(), any(StreamIdMappingFence.class),
                    any(StreamIdMappingFence.class)))
                .thenAnswer(invocation -> canonicalize(
                    invocation.getArgument(0, String.class),
                    invocation.getArgument(1, StreamIdMappingFence.class),
                    invocation.getArgument(2, StreamIdMappingFence.class)));
            lenient().when(storageApi.deleteStreamIdMapping(anyString(), anyLong()))
                .thenReturn(CompletableFuture.completedFuture(null));
        }

        private StorageApi storageApi() {
            return storageApi;
        }

        private synchronized void setActive(
                String key, long streamId, StreamIdMappingOwner owner) {
            mappings.put(key, new ActiveMapping(streamId, owner));
        }

        private synchronized void setFence(
                String key, StreamIdMappingFence fence) {
            mappings.put(key, fence);
        }

        private synchronized Optional<Long> activeStreamId(String key) {
            Object mapping = mappings.get(key);
            return mapping instanceof ActiveMapping active
                ? Optional.of(active.streamId()) : Optional.empty();
        }

        private synchronized Optional<StreamIdMappingFence> fence(String key) {
            Object mapping = mappings.get(key);
            return mapping instanceof StreamIdMappingFence durableFence
                ? Optional.of(durableFence) : Optional.empty();
        }

        private synchronized List<String> fenceAttempts() {
            return List.copyOf(fenceAttempts);
        }

        private void failNextFence(Throwable failure) {
            nextFenceFailure.set(failure);
        }

        private synchronized CompletableFuture<Long> get(String key) {
            Object mapping = mappings.get(key);
            if (mapping instanceof ActiveMapping active) {
                return CompletableFuture.completedFuture(active.streamId());
            }
            return CompletableFuture.failedFuture(new NoSuchKeyException(key));
        }

        private CompletableFuture<StreamIdAllocation> allocate(
                String key, StreamIdMappingOwner owner,
                Optional<StreamIdMappingFence> acknowledgedFence) {
            final Object expectedMapping;
            synchronized (this) {
                Object current = mappings.get(key);
                if (current instanceof ActiveMapping active) {
                    if (active.owner().equals(owner)) {
                        return CompletableFuture.completedFuture(
                            new StreamIdAllocation(active.streamId(), false));
                    }
                    return mappingConflict(key, active);
                }
                if (current instanceof StreamIdMappingFence durableFence
                        && !acknowledgedFence.equals(Optional.of(durableFence))) {
                    return CompletableFuture.failedFuture(
                        new IllegalStateException("mapping fence was not acknowledged"));
                }
                if (current == null && acknowledgedFence.isPresent()) {
                    return CompletableFuture.failedFuture(
                        new IllegalStateException("acknowledged mapping fence is absent"));
                }
                expectedMapping = current;
            }
            return streamIdAllocator.apply(key).thenCompose(streamId -> {
                synchronized (this) {
                    Object current = mappings.get(key);
                    if (Objects.equals(current, expectedMapping)) {
                        mappings.put(key, new ActiveMapping(streamId, owner));
                        return CompletableFuture.completedFuture(
                            new StreamIdAllocation(streamId, true));
                    }
                    Throwable invalidation = current instanceof ActiveMapping active
                        ? new StreamIdMappingConflictException(
                            key, new ActiveStreamIdMapping(active.streamId(), active.owner()))
                        : new IllegalStateException(
                            "mapping was fenced while allocation was in flight");
                    return CompletableFuture.failedFuture(
                        new KeyedAllocationInvalidatedException(
                            new StreamIdAllocation(streamId, true), invalidation));
                }
            });
        }

        private synchronized CompletableFuture<Void> bind(
                String key, long streamId, StreamIdMappingOwner owner,
                Optional<StreamIdMappingFence> acknowledgedFence) {
            Object current = mappings.get(key);
            if (current instanceof ActiveMapping active) {
                if (active.streamId() == streamId
                        && (active.owner().equals(owner) || active.owner().isLegacy())) {
                    mappings.put(key, new ActiveMapping(streamId, owner));
                    return CompletableFuture.completedFuture(null);
                }
                return CompletableFuture.failedFuture(
                    new StreamIdMappingConflictException(
                        key, new ActiveStreamIdMapping(active.streamId(), active.owner())));
            }
            if (current instanceof StreamIdMappingFence durableFence
                    && !acknowledgedFence.equals(Optional.of(durableFence))) {
                return CompletableFuture.failedFuture(
                    new IllegalStateException("mapping fence was not acknowledged"));
            }
            mappings.put(key, new ActiveMapping(streamId, owner));
            return CompletableFuture.completedFuture(null);
        }

        private synchronized CompletableFuture<StreamIdMappingFenceResult> fence(
                String key, long expectedStreamId, StreamIdMappingOwner expectedOwner) {
            fenceAttempts.add(key + ":" + expectedStreamId + ":" + expectedOwner.ownerToken());
            Throwable failure = nextFenceFailure.getAndSet(null);
            if (failure != null) {
                return CompletableFuture.failedFuture(failure);
            }
            Object current = mappings.get(key);
            if (current == null) {
                StreamIdMappingFence durableFence =
                    new StreamIdMappingFence(-1L, expectedOwner);
                mappings.put(key, durableFence);
                return CompletableFuture.completedFuture(
                    new StreamIdMappingFenceResult.Fenced(durableFence));
            }
            if (current instanceof StreamIdMappingFence durableFence) {
                return CompletableFuture.completedFuture(
                    new StreamIdMappingFenceResult.Fenced(durableFence));
            }
            ActiveMapping active = (ActiveMapping) current;
            if (!active.owner().equals(expectedOwner)
                    || expectedStreamId >= 0 && active.streamId() != expectedStreamId) {
                return CompletableFuture.completedFuture(
                    new StreamIdMappingFenceResult.PreservedActive(
                        new ActiveStreamIdMapping(active.streamId(), active.owner())));
            }
            StreamIdMappingFence durableFence =
                new StreamIdMappingFence(active.streamId(), active.owner());
            mappings.put(key, durableFence);
            return CompletableFuture.completedFuture(
                new StreamIdMappingFenceResult.Fenced(durableFence));
        }

        private synchronized CompletableFuture<Void> canonicalize(
                String key, StreamIdMappingFence expected,
                StreamIdMappingFence canonical) {
            if (!Objects.equals(mappings.get(key), expected)) {
                return CompletableFuture.failedFuture(
                    new IllegalStateException("mapping fence changed"));
            }
            mappings.put(key, canonical);
            return CompletableFuture.completedFuture(null);
        }

        private CompletableFuture<StreamIdAllocation> mappingConflict(
                String key, ActiveMapping active) {
            return CompletableFuture.failedFuture(
                new StreamIdMappingConflictException(
                    key, new ActiveStreamIdMapping(active.streamId(), active.owner())));
        }

        private record ActiveMapping(long streamId, StreamIdMappingOwner owner) {
        }
    }

    private static Version version(long id) {
        return new Version(id, 0, 0, 0, Optional.empty(), Optional.empty());
    }

    private record VersionedValue(byte[] value, Version version) {
    }

    private void mockStreamExistence(StreamIdentifier id, boolean exists) {
        String configPath = catalogPaths.streamConfigPath(id);
        if (exists) {
            try {
                byte[] bytes = MAPPER.writeValueAsBytes(
                    Map.of("partitions", 1, "properties", Map.of()));
                when(oxiaClient.get(configPath))
                    .thenReturn(CompletableFuture.completedFuture(
                        new GetResult(configPath, bytes, DUMMY_VERSION)));
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        } else {
            when(oxiaClient.get(configPath))
                .thenReturn(CompletableFuture.completedFuture(null));
        }
    }

    private void mockNamespaceMetadata(String nsName, Map<String, String> props) {
        String path = catalogPaths.namespacePath(nsName);
        try {
            byte[] bytes = MAPPER.writeValueAsBytes(props);
            when(oxiaClient.get(path))
                .thenReturn(CompletableFuture.completedFuture(
                    new GetResult(path, bytes, DUMMY_VERSION)));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    // --- registerExternalStream (externally controlled logical streams) ---

    @Test
    void registerExternalStream_createsOnlyLogicalConfigWhenAbsent() throws Exception {
        String configPath = "/admin/streams/public/default/my-topic";
        mockVersionedConfig(configPath);

        catalog.registerExternalStream(streamId, 3, Map.of("owner", "kafka")).get();

        ArgumentCaptor<byte[]> config = ArgumentCaptor.forClass(byte[].class);
        verify(oxiaClient, times(2)).put(eq(configPath), config.capture(), any());
        verify(logStorage, never()).deleteLog(any());

        JsonNode claim = MAPPER.readTree(config.getAllValues().get(0));
        JsonNode active = MAPPER.readTree(config.getAllValues().get(1));
        assertTrue(claim.get("_provisioning").asBoolean());
        assertEquals("EXTERNAL", claim.get("_creationKind").asText());
        assertEquals(3, active.get("partitions").asInt());
        assertEquals("kafka", active.get("properties").get("owner").asText());
        assertFalse(active.has("materialization"));
        assertFalse(active.has("_provisioning"));
    }

    @Test
    void registerExternalStream_rejectsNonPositivePartitionCount() {
        assertThrows(IllegalArgumentException.class,
            () -> catalog.registerExternalStream(streamId, 0, Map.of()));
    }

    @Test
    void registerExternalStream_growsWithCasAndPreservesExistingConfig() throws Exception {
        String configPath = "/admin/streams/public/default/my-topic";
        ObjectNode existing = MAPPER.createObjectNode();
        existing.put("partitions", 2);
        existing.putObject("properties").put("owner", "existing");
        existing.putObject("materialization")
            .put("enabled", true)
            .putObject("connectionOverrides");
        byte[] existingBytes = MAPPER.writeValueAsBytes(existing);
        when(oxiaClient.get(configPath)).thenReturn(CompletableFuture.completedFuture(
            new GetResult(configPath, existingBytes, DUMMY_VERSION)));
        when(oxiaClient.put(any(), any(byte[].class), any()))
            .thenReturn(CompletableFuture.completedFuture(new PutResult("key", DUMMY_VERSION)));

        catalog.registerExternalStream(streamId, 4, Map.of("owner", "replacement")).get();

        ArgumentCaptor<byte[]> config = ArgumentCaptor.forClass(byte[].class);
        verify(oxiaClient).put(eq(configPath), config.capture(),
            eq(Set.of(PutOption.IfVersionIdEquals(DUMMY_VERSION.versionId()))));
        JsonNode node = MAPPER.readTree(config.getValue());
        assertEquals(4, node.get("partitions").asInt());
        assertEquals("existing", node.get("properties").get("owner").asText());
        assertTrue(node.get("materialization").get("enabled").asBoolean());
    }

    @Test
    void registerExternalStream_doesNotShrinkExistingConfig() throws Exception {
        String configPath = "/admin/streams/public/default/my-topic";
        ObjectNode existing = MAPPER.createObjectNode();
        existing.put("partitions", 5);
        existing.putObject("properties").put("owner", "existing");
        byte[] existingBytes = MAPPER.writeValueAsBytes(existing);
        when(oxiaClient.get(configPath)).thenReturn(CompletableFuture.completedFuture(
            new GetResult(configPath, existingBytes, DUMMY_VERSION)));

        catalog.registerExternalStream(streamId, 3, Map.of("owner", "replacement")).get();

        verify(oxiaClient, never()).put(eq(configPath), any(byte[].class), any());
    }

    @Test
    void unregisterExternalStreamRetainsLogicalRecoveryConfigAndIsIdempotent() throws Exception {
        String configPath = "/admin/streams/public/default/my-topic";
        byte[] config = streamConfigBytes(1, Map.of(), false);
        AtomicReference<VersionedValue> configState =
            mockVersionedConfig(configPath, config);

        catalog.unregisterExternalStream(streamId).get();
        catalog.unregisterExternalStream(streamId).get();

        JsonNode retained = MAPPER.readTree(configState.get().value());
        assertTrue(retained.path("_provisioning").asBoolean());
        assertEquals("UNREGISTERED", retained.path("_provisioningState").asText());
        verify(oxiaClient, never()).delete(eq(configPath), any());
        verify(oxiaClient, never()).delete("/streams/public/default/my-topic-partition-0");
        verify(logStorage, never()).deleteLog(any());
    }

    // --- registerExternalPartition (broker-created streams) ---

    @Test
    void registerExternalPartition_createsConfigWhenAbsent() throws Exception {
        String configPath = "/admin/streams/public/default/my-topic";
        String partPath = "/streams/public/default/my-topic-partition-0";
        var currentConfig = new java.util.concurrent.atomic.AtomicReference<byte[]>();
        when(oxiaClient.get(configPath)).thenAnswer(ignored ->
            CompletableFuture.completedFuture(currentConfig.get() == null ? null
                : new GetResult(configPath, currentConfig.get(), DUMMY_VERSION)));
        when(oxiaClient.put(eq(configPath), any(byte[].class), any()))
            .thenAnswer(invocation -> {
                currentConfig.set(invocation.getArgument(1, byte[].class));
                return CompletableFuture.completedFuture(
                    new PutResult(configPath, DUMMY_VERSION));
            });
        when(oxiaClient.put(eq(partPath), any(byte[].class), any()))
            .thenReturn(CompletableFuture.completedFuture(
                new PutResult(partPath, DUMMY_VERSION)));

        catalog.registerExternalPartition(streamId, 0, 100L, Map.of("k", "v")).get();

        // Partition metadata is written create-only (must not clobber the broker's real metadata).
        verify(oxiaClient).put(eq(partPath), any(byte[].class), eq(Set.of(PutOption.IfRecordDoesNotExist)));
        ArgumentCaptor<byte[]> cfg = ArgumentCaptor.forClass(byte[].class);
        verify(oxiaClient).put(eq(configPath), cfg.capture(), eq(Set.of(PutOption.IfRecordDoesNotExist)));
        JsonNode node = MAPPER.readTree(cfg.getValue());
        assertEquals(1, node.get("partitions").asInt());
    }

    @Test
    void registerExternalPartition_growsPartitionCountWithCas() throws Exception {
        String configPath = "/admin/streams/public/default/my-topic";
        byte[] existingBytes = ownedStreamConfigBytes(
            1, Map.of(), "external-grow-incarnation", "external-grow-owner", "EXTERNAL");
        when(oxiaClient.get(configPath)).thenReturn(CompletableFuture.completedFuture(
            new GetResult(configPath, existingBytes, DUMMY_VERSION)));
        when(oxiaClient.put(any(), any(byte[].class), any()))
            .thenReturn(CompletableFuture.completedFuture(new PutResult("key", DUMMY_VERSION)));

        catalog.registerExternalPartition(streamId, 2, 100L, Map.of()).get();

        // The grow is a version-guarded compare-and-set; partition count becomes index + 1.
        ArgumentCaptor<byte[]> cfg = ArgumentCaptor.forClass(byte[].class);
        verify(oxiaClient).put(eq(configPath), cfg.capture(),
            eq(Set.of(PutOption.IfVersionIdEquals(DUMMY_VERSION.versionId()))));
        JsonNode node = MAPPER.readTree(cfg.getValue());
        assertEquals(3, node.get("partitions").asInt());
    }

    @Test
    void registerExternalPartition_doesNotRestoreStalePropertiesWhenGrowing() throws Exception {
        String configPath = "/admin/streams/public/default/my-topic";
        byte[] existingBytes = ownedStreamConfigBytes(
            1, Map.of(), "external-properties-incarnation",
            "external-properties-owner", "EXTERNAL");
        when(oxiaClient.get(configPath)).thenReturn(CompletableFuture.completedFuture(
            new GetResult(configPath, existingBytes, DUMMY_VERSION)));
        when(oxiaClient.put(any(), any(byte[].class), any()))
            .thenReturn(CompletableFuture.completedFuture(new PutResult("key", DUMMY_VERSION)));

        catalog.registerExternalPartition(
            streamId, 2, 100L, Map.of("retention.ms", "stale-value")).get();

        ArgumentCaptor<byte[]> config = ArgumentCaptor.forClass(byte[].class);
        verify(oxiaClient).put(eq(configPath), config.capture(),
            eq(Set.of(PutOption.IfVersionIdEquals(DUMMY_VERSION.versionId()))));
        JsonNode node = MAPPER.readTree(config.getValue());
        assertEquals(3, node.get("partitions").asInt());
        assertTrue(node.get("properties").isEmpty());
    }

    @Test
    void registerExternalPartition_noopWhenCountAlreadySufficient() throws Exception {
        String configPath = "/admin/streams/public/default/my-topic";
        byte[] existingBytes = ownedStreamConfigBytes(
            5, Map.of(), "external-noop-incarnation", "external-noop-owner", "EXTERNAL");
        when(oxiaClient.get(configPath)).thenReturn(CompletableFuture.completedFuture(
            new GetResult(configPath, existingBytes, DUMMY_VERSION)));
        when(oxiaClient.put(any(), any(byte[].class), any()))
            .thenReturn(CompletableFuture.completedFuture(new PutResult("key", DUMMY_VERSION)));

        catalog.registerExternalPartition(streamId, 2, 100L, Map.of()).get();

        // Only the partition metadata is written; the config is already large enough.
        verify(oxiaClient).put(eq("/streams/public/default/my-topic-partition-2"),
            any(byte[].class), eq(Set.of(PutOption.IfRecordDoesNotExist)));
        verify(oxiaClient, never()).put(eq(configPath), any(byte[].class),
            eq(Set.of(PutOption.IfVersionIdEquals(DUMMY_VERSION.versionId()))));
    }

    @Test
    void registerExternalPartition_doesNotWriteMetadataForPermanentlyDeletedStream() {
        String configPath = "/admin/streams/public/default/my-topic";
        when(oxiaClient.get(configPath)).thenReturn(CompletableFuture.completedFuture(
            new GetResult(configPath,
                "{\"_externalStreamPermanentlyDeleted\":true}"
                    .getBytes(StandardCharsets.UTF_8),
                DUMMY_VERSION)));

        ExecutionException failure = assertThrows(ExecutionException.class, () ->
            catalog.registerExternalPartition(streamId, 2, 100L, Map.of()).get());

        assertInstanceOf(NoSuchStreamException.class, failure.getCause());
        verify(oxiaClient, never()).put(
            eq("/streams/public/default/my-topic-partition-2"),
            any(byte[].class), any());
    }

    @Test
    void registerExternalPartition_acceptsMatchingMetadataAfterCreateRace() throws Exception {
        String configPath = catalogPaths.streamConfigPath(streamId);
        String partPath = catalogPaths.partitionMetadataPath(streamId, 0);
        String incarnation = "external-matching-incarnation";
        String ownerToken = "external-matching-owner";
        when(oxiaClient.get(configPath)).thenReturn(CompletableFuture.completedFuture(
            new GetResult(configPath, ownedStreamConfigBytes(
                1, Map.of(), incarnation, ownerToken, "EXTERNAL"), DUMMY_VERSION)));
        mockPartitionMetadata(
            streamId, 0, new LogMetadata(
                100L, Map.of(), OptionalLong.empty(),
                incarnation, ownerToken, 1L, false));

        catalog.registerExternalPartition(streamId, 0, 100L, Map.of()).get();

        verify(oxiaClient, times(2)).get(partPath);
    }

    @Test
    void registerExternalPartition_rejectsDifferentMetadataAfterCreateRace() throws Exception {
        String configPath = catalogPaths.streamConfigPath(streamId);
        String incarnation = "external-conflict-incarnation";
        String ownerToken = "external-conflict-owner";
        when(oxiaClient.get(configPath)).thenReturn(CompletableFuture.completedFuture(
            new GetResult(configPath, ownedStreamConfigBytes(
                1, Map.of(), incarnation, ownerToken, "EXTERNAL"), DUMMY_VERSION)));
        mockPartitionMetadata(
            streamId, 0, new LogMetadata(
                200L, Map.of(), OptionalLong.empty(),
                incarnation, ownerToken, 1L, false));

        ExecutionException failure = assertThrows(ExecutionException.class, () ->
            catalog.registerExternalPartition(streamId, 0, 100L, Map.of()).get());

        assertInstanceOf(PartitionLifecycleFencedException.class, failure.getCause());
    }

    @Test
    void unregisterThenDifferentExternalIdDoesNotReviveStalePartitionMetadata() throws Exception {
        String configPath = catalogPaths.streamConfigPath(streamId);
        String partPath = catalogPaths.partitionMetadataPath(streamId, 0);
        var currentConfig = new java.util.concurrent.atomic.AtomicReference<byte[]>(
            streamConfigBytes(1, Map.of(), false));
        when(oxiaClient.get(configPath)).thenAnswer(ignored ->
            CompletableFuture.completedFuture(currentConfig.get() == null ? null
                : new GetResult(configPath, currentConfig.get(), DUMMY_VERSION)));
        when(oxiaClient.put(eq(configPath), any(byte[].class), any()))
            .thenAnswer(invocation -> {
                currentConfig.set(invocation.getArgument(1, byte[].class));
                return CompletableFuture.completedFuture(
                    new PutResult(configPath, DUMMY_VERSION));
            });
        byte[] staleMetadata = LOG_METADATA_SERDE.serialize(
            partPath, new LogMetadata(200L, Map.of(), OptionalLong.empty()));
        when(oxiaClient.get(partPath)).thenReturn(CompletableFuture.completedFuture(
            new GetResult(partPath, staleMetadata, DUMMY_VERSION)));

        catalog.unregisterExternalStream(streamId).join();
        CompletionException failure = assertThrows(CompletionException.class, () ->
            catalog.registerExternalPartition(streamId, 0, 100L, Map.of()).join());

        assertInstanceOf(PartitionLifecycleFencedException.class, failure.getCause());
        assertFalse(catalog.streamExists(streamId).join());
        verify(oxiaClient, never()).delete(eq(partPath), any());
    }

}
