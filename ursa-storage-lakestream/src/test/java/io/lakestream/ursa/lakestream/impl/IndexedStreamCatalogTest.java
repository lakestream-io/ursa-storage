/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.lakestream.impl;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
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
import io.lakestream.api.Log;
import io.lakestream.api.LogId;
import io.lakestream.api.LogStorage;
import io.lakestream.api.Namespace;
import io.lakestream.api.Partitioning;
import io.lakestream.api.PartitioningStrategy;
import io.lakestream.api.SchemaConfig;
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
import io.lakestream.api.exception.StreamPermanentlyDeletedException;
import io.lakestream.api.materialization.TableMaterializationPolicy;
import io.lakestream.ursa.catalog.metadata.LogMetadata;
import io.lakestream.ursa.catalog.metadata.LogMetadataSerde;
import io.lakestream.ursa.lakestream.impl.FakeOxiaRecord.VersionedValue;
import io.lakestream.ursa.storage.StorageApi;
import io.lakestream.ursa.storage.StorageApi.ActiveStreamIdMapping;
import io.lakestream.ursa.storage.StorageApi.KeyedAllocationInvalidatedException;
import io.lakestream.ursa.storage.StorageApi.StreamIdAllocation;
import io.lakestream.ursa.storage.StorageApi.StreamIdMappingConflictException;
import io.lakestream.ursa.storage.StorageApi.StreamIdMappingFence;
import io.lakestream.ursa.storage.StorageApi.StreamIdMappingFenceResult;
import io.lakestream.ursa.storage.StorageApi.StreamIdMappingOwner;
import io.lakestream.ursa.storage.StorageApi.StreamWriteLease;
import io.lakestream.ursa.storage.impl.exception.NoSuchKeyException;
import io.oxia.client.api.AsyncOxiaClient;
import io.oxia.client.api.GetResult;
import io.oxia.client.api.PutResult;
import io.oxia.client.api.Version;
import io.oxia.client.api.exceptions.UnexpectedVersionIdException;
import io.oxia.client.api.options.DeleteOption;
import io.oxia.client.api.options.PutOption;
import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
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
    private String tombstonePath;
    private AtomicReference<VersionedValue> tombstoneState;

    private long nextStreamId = 100L;

    @BeforeEach
    void setUp() {
        lenient().when(oxiaClient.get(anyString()))
            .thenReturn(CompletableFuture.completedFuture(null));
        catalogPaths = new DefaultCatalogPaths();
        defaultStorage = new FencedStorageHarness(
            key -> CompletableFuture.completedFuture(nextStreamId++));
        catalog = fencedCatalog(defaultStorage);
        catalog.initialize("test-catalog", Map.of()).join();
        streamId = new StreamIdentifier("public/default", "my-topic");
        tombstonePath = catalogPaths.streamTombstonePath(streamId);
        tombstoneState = mockVersionedConfig(tombstonePath);
    }

    // --- permanent deletion tombstones ---

    @Test
    void loadStreamReportsPermanentDeletionFromTombstonePath() throws Exception {
        mockVersionedConfig(tombstonePath, droppedStreamConfigBytes(
            1, "dropped-incarnation", "dropped-owner", 2L, "NATIVE_CREATE"));

        ExecutionException failure = assertThrows(ExecutionException.class,
            () -> catalog.loadStream(streamId).get());

        assertInstanceOf(StreamPermanentlyDeletedException.class, failure.getCause());
    }

    @Test
    void increasePartitionsReportsPermanentDeletionFromTombstonePath() throws Exception {
        mockVersionedConfig(tombstonePath, droppedStreamConfigBytes(
            1, "dropped-incarnation", "dropped-owner", 2L, "NATIVE_CREATE"));

        ExecutionException failure = assertThrows(ExecutionException.class,
            () -> catalog.increasePartitions(streamId, 4).get());

        assertInstanceOf(StreamPermanentlyDeletedException.class, failure.getCause());
    }

    @Test
    void replaceStreamPropertiesReportsPermanentDeletionFromTombstonePath() throws Exception {
        mockVersionedConfig(tombstonePath, droppedStreamConfigBytes(
            1, "dropped-incarnation", "dropped-owner", 2L, "NATIVE_CREATE"));

        ExecutionException failure = assertThrows(ExecutionException.class,
            () -> catalog.replaceStreamProperties(streamId, Map.of("tier", "hot"), 1L).get());

        assertInstanceOf(StreamPermanentlyDeletedException.class, failure.getCause());
    }

    @Test
    void createStreamRejectsTombstonedIdentifier() throws Exception {
        mockVersionedConfig(tombstonePath, droppedStreamConfigBytes(
            1, "dropped-incarnation", "dropped-owner", 2L, "NATIVE_CREATE"));
        Partitioning partitioning = new Partitioning(
            PartitioningStrategy.INDEXED, Map.of("numPartitions", "1"));

        ExecutionException failure = assertThrows(ExecutionException.class, () ->
            catalog.createStream(streamId, new StreamConfig(), partitioning,
                new SchemaConfig(), Map.of()).get());

        assertInstanceOf(StreamPermanentlyDeletedException.class, failure.getCause());
        verify(oxiaClient, never()).put(eq(catalogPaths.streamConfigPath(streamId)),
            any(byte[].class), any());
    }

    @Test
    void dropStreamWritesTombstoneOutsideConfigPrefixAndDeletesConfig() throws Exception {
        String configPath = catalogPaths.streamConfigPath(streamId);
        String configPrefix = catalogPaths.streamConfigPrefix(streamId.namespace());
        mockVersionedConfig(configPath, ownedStreamConfigBytes(
            2, Map.of(), "tombstone-incarnation", "tombstone-owner", "NATIVE_CREATE"));
        mockCreateOnlyRecord(catalogPaths.partitionMetadataPath(streamId, 0));
        mockCreateOnlyRecord(catalogPaths.partitionMetadataPath(streamId, 1));
        when(oxiaClient.list(configPrefix, configPrefix + "\uffff"))
            .thenReturn(CompletableFuture.completedFuture(List.of(configPath)));

        assertTrue(catalog.dropStream(streamId, true).get(10, TimeUnit.SECONDS));

        assertFalse(tombstonePath.startsWith(configPrefix));
        verify(oxiaClient).put(eq(tombstonePath), any(byte[].class),
            eq(Set.of(PutOption.IfRecordDoesNotExist)));
        verify(oxiaClient).delete(eq(configPath), any());
        assertEquals("DROPPED", MAPPER.readTree(tombstoneState.get().value())
            .path("_provisioningState").asText());
        assertTrue(catalog.listStreamEntries(streamId.namespace()).get().isEmpty());
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
        StreamMetadata result = catalog.createStream(streamId, new StreamConfig(), partitioning,
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
    void createStream_interleavedPartitionChainsMatchSequentialOnes() throws Exception {
        CreationOutcome sequential = createPartitionsSequentially(
            new StreamIdentifier("public/default", "sequential-topic"));
        CreationOutcome interleaved = createPartitionsCompletingOutOfOrder(
            new StreamIdentifier("public/default", "interleaved-topic"));

        // Completion order does not reach the committed result: the layout keeps partition order,
        // each partition keeps the stream ID its own allocation key resolves to, and neither run
        // fences anything.
        assertEquals(sequential, interleaved);
        assertEquals(List.of(100L, 101L), interleaved.logIds());
        assertEquals(Map.of(0, 100L, 1, 101L), interleaved.persistedStreamIds());
        assertEquals(List.of(), interleaved.fenceAttempts());
    }

    @Test
    void createStream_rejectsUnsupportedRangePartitioningBeforePersistence() {
        Partitioning partitioning = new Partitioning(
            PartitioningStrategy.RANGE, Map.of("numPartitions", "1"));

        IllegalArgumentException failure = assertThrows(IllegalArgumentException.class, () ->
            catalog.createStream(streamId, new StreamConfig(), partitioning,
                new SchemaConfig(), Map.of()));

        assertTrue(failure.getMessage().contains("only supports INDEXED"));
        verify(oxiaClient, never()).put(anyString(), any(byte[].class), any());
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

        CompletableFuture<StreamMetadata> staleOwner = concurrentCatalog.createStream(
            streamId, new StreamConfig(), partitioning, new SchemaConfig(), Map.of());
        CompletableFuture<StreamMetadata> takeover = concurrentCatalog.createStream(
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

        CompletableFuture<StreamMetadata> create = racedCatalog.createStream(
            streamId, new StreamConfig(), partitioning, new SchemaConfig(), Map.of());
        assertFalse(create.isDone());
        assertTrue(racedCatalog.dropStream(streamId, false).join());
        allocation.complete(701L);

        CompletionException failure = assertThrows(CompletionException.class, create::join);
        assertInstanceOf(KeyedAllocationInvalidatedException.class, failure.getCause());
        String mappingKey = "lakestream-native/" + streamId.fullName() + "/partition-0";
        assertEquals(Optional.empty(), mappings.activeStreamId(mappingKey));
        assertEquals(701L, mappings.fence(mappingKey).orElseThrow().streamId());
        assertNull(config.get());
        assertEquals("DROPPED", MAPPER.readTree(tombstoneState.get().value())
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

        CompletableFuture<StreamMetadata> create = racedCatalog.createStream(
            streamId, new StreamConfig(), partitioning, new SchemaConfig(), Map.of());
        assertFalse(create.isDone());
        assertTrue(racedCatalog.dropStream(streamId, false).join());
        allocation.completeExceptionally(invalidated);

        CompletionException failure = assertThrows(CompletionException.class, create::join);
        assertEquals(invalidated, failure.getCause());
        assertEquals(validationFailure, failure.getCause().getCause());
        assertNull(config.get());
        assertEquals("DROPPED", MAPPER.readTree(tombstoneState.get().value())
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

        CompletableFuture<StreamMetadata> create = racedCatalog.createStream(
            streamId, new StreamConfig(), partitioning, new SchemaConfig(), Map.of());
        assertFalse(create.isDone());
        assertTrue(racedCatalog.dropStream(streamId, false).join());
        assertNull(config.get());

        RuntimeException contextReadFailure =
            new RuntimeException("native cleanup context unavailable");
        AtomicInteger postDropReads = new AtomicInteger();
        when(oxiaClient.get(configPath)).thenAnswer(ignored -> {
            if (postDropReads.getAndIncrement() == 0) {
                return CompletableFuture.completedFuture(null);
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

        CompletableFuture<StreamMetadata> staleOwner = racedCatalog.createStream(
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
        assertEquals(purge ? -1L : reusedStreamId, tombstone.streamId());
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
    void createStream_retryAfterPostMetadataFailureConvergesLatestDesiredState()
            throws Exception {
        String configPath = catalogPaths.streamConfigPath(streamId);
        String partitionPath = catalogPaths.partitionMetadataPath(streamId, 0);
        String secondPartitionPath = catalogPaths.partitionMetadataPath(streamId, 1);
        RuntimeException postMetadataFailure =
            new RuntimeException("crashed after partition metadata");
        AtomicReference<VersionedValue> partitionState =
            mockCreateOnlyRecord(partitionPath);
        mockCreateOnlyRecord(secondPartitionPath);
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
                String allocationKey = key.orElseThrow();
                allocationKeys.add(allocationKey);
                return CompletableFuture.completedFuture(
                    allocationKey.endsWith("partition-1") ? 101L : 100L);
            }, key -> CompletableFuture.completedFuture(
                key.endsWith("partition-1") ? 101L : 100L),
            (key, expectedStreamId) -> CompletableFuture.completedFuture(null),
            null, null, List.of());

        Partitioning initialPartitioning = new Partitioning(
            PartitioningStrategy.INDEXED, Map.of("numPartitions", "1"));
        CompletionException failure = assertThrows(CompletionException.class, () ->
            recoveringCatalog.createStream(streamId, new StreamConfig(), initialPartitioning,
                new SchemaConfig(), Map.of("generation", "old")).join());

        assertEquals(postMetadataFailure, failure.getCause());
        Partitioning recoveredPartitioning = new Partitioning(
            PartitioningStrategy.INDEXED, Map.of("numPartitions", "2"));
        Optional<TableMaterializationPolicy> materialization =
            Optional.of(TableMaterializationPolicy.empty());
        StreamMetadata recovered = recoveringCatalog.createStream(
            streamId, new StreamConfig(), recoveredPartitioning,
            new SchemaConfig(), Map.of("generation", "new"), materialization).join();

        assertEquals(streamId, recovered.identifier());
        assertEquals(2, recovered.partitioning().numPartitions());
        assertEquals(Map.of("generation", "new"), recovered.properties());
        assertEquals(materialization, recovered.materialization());

        String expectedKey = "lakestream-native/" + streamId.fullName()
            + "/partition-0";
        assertEquals(List.of(expectedKey, expectedKey,
            "lakestream-native/" + streamId.fullName() + "/partition-1"), allocationKeys);
        assertNotNull(partitionState.get());
        verify(oxiaClient).put(eq(partitionPath), any(byte[].class),
            eq(Set.of(PutOption.IfRecordDoesNotExist)));
        verify(oxiaClient, never()).delete(eq(configPath), any());
        verify(oxiaClient, never()).delete(eq(partitionPath), any());
        verify(oxiaClient, never()).delete(eq(secondPartitionPath), any());
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
                : new GetResult(configPath, value, FakeOxiaRecord.version(1)));
        });
        when(oxiaClient.put(eq(configPath), any(byte[].class),
                eq(Set.of(PutOption.IfRecordDoesNotExist))))
            .thenAnswer(invocation -> {
                currentConfig.set(invocation.getArgument(1, byte[].class));
                return CompletableFuture.completedFuture(
                    new PutResult(configPath, FakeOxiaRecord.version(1)));
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

    @Test
    void increasePartitionsPublishesLayoutOnlyAfterAllNewMetadataIsDurable()
            throws Exception {
        String incarnation = "expansion-incarnation";
        String ownerToken = "expansion-owner";
        String configPath = catalogPaths.streamConfigPath(streamId);
        String firstPartitionPath = catalogPaths.partitionMetadataPath(streamId, 0);
        String secondPartitionPath = catalogPaths.partitionMetadataPath(streamId, 1);
        String thirdPartitionPath = catalogPaths.partitionMetadataPath(streamId, 2);
        AtomicReference<VersionedValue> config = mockVersionedConfig(
            configPath, ownedStreamConfigBytes(
                1, Map.of("tier", "hot"), incarnation, ownerToken, "NATIVE_CREATE"));
        byte[] firstMetadata = LOG_METADATA_SERDE.serialize(
            firstPartitionPath, new LogMetadata(
                300L, Map.of(), OptionalLong.empty(),
                incarnation, ownerToken, 1L, false));
        mockVersionedConfig(firstPartitionPath, firstMetadata);
        mockCreateOnlyRecord(secondPartitionPath);
        RuntimeException interrupted = new RuntimeException("partition metadata interrupted");
        mockCreateOnlyRecord(thirdPartitionPath, interrupted);
        List<String> physicalAllocations =
            new java.util.concurrent.CopyOnWriteArrayList<>();
        AtomicLong nextExpansionStreamId = new AtomicLong(400L);
        FencedStorageHarness mappings = new FencedStorageHarness(key -> {
            physicalAllocations.add(key);
            return CompletableFuture.completedFuture(
                nextExpansionStreamId.getAndIncrement());
        });
        IndexedStreamCatalog expandingCatalog = fencedCatalog(mappings);

        CompletionException failure = assertThrows(CompletionException.class, () ->
            expandingCatalog.increasePartitions(streamId, 3).join());

        assertEquals(interrupted, failure.getCause());
        JsonNode pending = MAPPER.readTree(config.get().value());
        assertEquals(1, pending.path("partitions").asInt());
        assertEquals(1, pending.path("_pendingExpansion")
            .path("basePartitions").asInt());
        assertEquals(3, pending.path("_pendingExpansion")
            .path("targetPartitions").asInt());
        StreamMetadata committed = expandingCatalog.loadStream(streamId).join();
        assertEquals(1, committed.partitioning().numPartitions());
        assertEquals(List.of(LogId.of(300L)), committed.layout().logIds().join());

        StreamMetadata expanded = expandingCatalog.increasePartitions(streamId, 3).join();

        // An active record answers the claim on its own; the tombstone costs a read only on the
        // absent and non-ACTIVE branches.
        verify(oxiaClient, never()).get(tombstonePath);
        assertEquals(3, expanded.partitioning().numPartitions());
        assertEquals(3, expanded.layout().logIds().join().size());
        JsonNode completed = MAPPER.readTree(config.get().value());
        assertEquals(3, completed.path("partitions").asInt());
        assertFalse(completed.has("_pendingExpansion"));
        assertEquals(List.of(
            "lakestream-native/" + streamId.fullName() + "/partition-1",
            "lakestream-native/" + streamId.fullName() + "/partition-2"),
            physicalAllocations);
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

        StreamMetadata stream = catalog.loadStream(streamId).get();
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
    void loadStreamReturnsConsistentNewSnapshotWhenPropertiesChangeDuringLayoutRead()
            throws Exception {
        String configPath = catalogPaths.streamConfigPath(streamId);
        String partitionPath = catalogPaths.partitionMetadataPath(streamId, 0);
        String incarnation = "property-update-incarnation";
        String ownerToken = "property-update-owner";
        AtomicReference<VersionedValue> config = mockVersionedConfig(
            configPath, ownedStreamConfigBytes(
                1, Map.of("owner", "old"), incarnation, ownerToken, "NATIVE_CREATE"));
        byte[] partitionBytes = LOG_METADATA_SERDE.serialize(
            partitionPath, new LogMetadata(
                210L, Map.of(), OptionalLong.empty(),
                incarnation, ownerToken, 1L, false));
        AtomicReference<VersionedValue> partition =
            mockVersionedConfig(partitionPath, partitionBytes);
        CompletableFuture<GetResult> delayedPartitionRead = new CompletableFuture<>();
        AtomicInteger partitionReads = new AtomicInteger();
        when(oxiaClient.get(partitionPath)).thenAnswer(ignored -> {
            if (partitionReads.getAndIncrement() == 0) {
                return delayedPartitionRead;
            }
            VersionedValue current = partition.get();
            return CompletableFuture.completedFuture(new GetResult(
                partitionPath, current.value(), current.version()));
        });

        CompletableFuture<StreamMetadata> loading = catalog.loadStream(streamId);
        assertFalse(loading.isDone());
        StreamMetadata updated = catalog.replaceStreamProperties(
            streamId, Map.of("owner", "new"), 1L).join();
        assertEquals(Map.of("owner", "new"), updated.properties());
        // An active record answers the write on its own; the tombstone costs a read only on the
        // absent and non-ACTIVE branches.
        verify(oxiaClient, never()).get(tombstonePath);

        VersionedValue readPartition = partition.get();
        delayedPartitionRead.complete(new GetResult(
            partitionPath, readPartition.value(), readPartition.version()));

        StreamMetadata loaded = loading.join();
        assertEquals(Map.of("owner", "new"), loaded.properties());
        assertEquals(config.get().version().versionId(), loaded.metadataVersion());
        assertEquals(List.of(LogId.of(210L)), loaded.layout().logIds().join());
    }

    @Test
    void getLayoutAllowsMaterializationUpdateDuringPartitionRead() throws Exception {
        String configPath = catalogPaths.streamConfigPath(streamId);
        String partitionPath = catalogPaths.partitionMetadataPath(streamId, 0);
        String incarnation = "materialization-update-incarnation";
        String ownerToken = "materialization-update-owner";
        AtomicReference<VersionedValue> config = mockVersionedConfig(
            configPath, ownedStreamConfigBytes(
                1, Map.of(), incarnation, ownerToken, "NATIVE_CREATE"));
        byte[] partitionBytes = LOG_METADATA_SERDE.serialize(
            partitionPath, new LogMetadata(
                211L, Map.of(), OptionalLong.empty(),
                incarnation, ownerToken, 1L, false));
        AtomicReference<VersionedValue> partition =
            mockVersionedConfig(partitionPath, partitionBytes);
        CompletableFuture<GetResult> delayedPartitionRead = new CompletableFuture<>();
        AtomicInteger partitionReads = new AtomicInteger();
        when(oxiaClient.get(partitionPath)).thenAnswer(ignored -> {
            if (partitionReads.getAndIncrement() == 0) {
                return delayedPartitionRead;
            }
            VersionedValue current = partition.get();
            return CompletableFuture.completedFuture(new GetResult(
                partitionPath, current.value(), current.version()));
        });

        CompletableFuture<StreamLayout> loading = catalog.getLayout(streamId);
        assertFalse(loading.isDone());
        catalog.setStreamMaterialization(
            streamId, TableMaterializationPolicy.empty()).join();
        assertEquals(2L, config.get().version().versionId());

        VersionedValue readPartition = partition.get();
        delayedPartitionRead.complete(new GetResult(
            partitionPath, readPartition.value(), readPartition.version()));

        assertEquals(List.of(LogId.of(211L)), loading.join().logIds().join());
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
    void loadStreamRejectsIncompleteCommittedLayout() throws Exception {
        mockStreamConfig(streamId, 3, Map.of("env", "prod"));
        mockPartitionMetadata(streamId, 2, 202L, Map.of());
        when(oxiaClient.get(catalogPaths.partitionMetadataPath(streamId, 0)))
            .thenReturn(CompletableFuture.completedFuture(null));
        when(oxiaClient.get(catalogPaths.partitionMetadataPath(streamId, 1)))
            .thenReturn(CompletableFuture.completedFuture(null));

        ExecutionException failure = assertThrows(ExecutionException.class, () ->
            catalog.loadStream(streamId).get());

        assertInstanceOf(NoSuchStreamException.class, failure.getCause());
    }

    @Test
    void loadStreamRejectsPartitionFromDifferentIncarnation() throws Exception {
        String configPath = catalogPaths.streamConfigPath(streamId);
        String partitionPath = catalogPaths.partitionMetadataPath(streamId, 0);
        when(oxiaClient.get(configPath)).thenReturn(CompletableFuture.completedFuture(
            new GetResult(configPath, ownedStreamConfigBytes(
                1, Map.of(), "current-incarnation", "current-owner", "NATIVE_CREATE"),
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
        mockVersionedConfig(configPath, ownedStreamConfigBytes(
            1, Map.of(), "current-incarnation", "current-owner", "NATIVE_CREATE"));
        byte[] activeMetadata = LOG_METADATA_SERDE.serialize(
            partitionPath, new LogMetadata(
                203L, Map.of(), OptionalLong.empty(),
                "current-incarnation", "current-owner", 1L, false));
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

        CompletableFuture<StreamMetadata> loading = catalog.loadStream(streamId);
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
    void listStreams_excludesDroppedNativeStreams() throws Exception {
        String prefix = "/admin/streams/public/default/";
        String activePath = prefix + "active";
        String deletedPath = prefix + "deleted";
        when(oxiaClient.list(eq(prefix), eq(prefix + "\uffff")))
            .thenReturn(CompletableFuture.completedFuture(List.of(activePath, deletedPath)));
        when(oxiaClient.get(activePath)).thenReturn(CompletableFuture.completedFuture(
            new GetResult(activePath, streamConfigBytes(1, Map.of(), false), DUMMY_VERSION)));
        when(oxiaClient.get(deletedPath)).thenReturn(CompletableFuture.completedFuture(
            new GetResult(deletedPath,
                droppedStreamConfigBytes(
                    1, "deleted-incarnation", "deleted-owner", 2L,
                    "NATIVE_CREATE"),
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
                ? droppedStreamConfigBytes(
                    1, "deleted-incarnation-5", "deleted-owner-5", 2L,
                    "NATIVE_CREATE")
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
            droppedStreamConfigBytes(
                1, "deleted-incarnation-33", "deleted-owner-33", 2L,
                "NATIVE_CREATE"),
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
        assertNull(config.get());
        assertEquals("DROPPED", MAPPER.readTree(tombstoneState.get().value())
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
        LogMetadata tombstone = LOG_METADATA_SERDE.deserialize(
            partitionPath, partition.get().value());
        assertTrue(tombstone.deleted());
        assertEquals(-1L, tombstone.streamId());
    }

    @Test
    void completedMetadataOnlyDropCanBeUpgradedToIdempotentPurge() throws Exception {
        long physicalStreamId = 300L;
        String configPath = catalogPaths.streamConfigPath(streamId);
        String partitionPath = catalogPaths.partitionMetadataPath(streamId, 0);
        String mappingKey = "lakestream-native/" + streamId.fullName()
            + "/partition-0";
        String incarnation = "metadata-only-then-purge";
        String ownerToken = "registration-owner";
        AtomicReference<VersionedValue> config = mockVersionedConfig(
            configPath, ownedStreamConfigBytes(
                1, Map.of(), incarnation, ownerToken, "NATIVE_CREATE"));
        byte[] metadata = LOG_METADATA_SERDE.serialize(
            partitionPath, new LogMetadata(
                physicalStreamId, Map.of(), OptionalLong.empty(),
                incarnation, ownerToken, 1L, false));
        AtomicReference<VersionedValue> partition =
            mockVersionedConfig(partitionPath, metadata);
        when(logStorage.deleteLog(LogId.of(physicalStreamId)))
            .thenReturn(CompletableFuture.completedFuture(null));
        FencedStorageHarness mappings = new FencedStorageHarness(
            key -> CompletableFuture.completedFuture(301L));
        mappings.setActive(mappingKey, physicalStreamId,
            new StreamIdMappingOwner(incarnation, ownerToken, 1L));
        IndexedStreamCatalog recoveringCatalog = fencedCatalog(mappings);

        assertTrue(recoveringCatalog.dropStream(streamId, false).join());

        LogMetadata retained = LOG_METADATA_SERDE.deserialize(
            partitionPath, partition.get().value());
        assertTrue(retained.deleted());
        assertEquals(physicalStreamId, retained.streamId());
        verify(logStorage, never()).deleteLog(any());

        // The identity is already permanently deleted, so the upgrade purges through the
        // tombstone instead of claiming a new deletion.
        assertFalse(recoveringCatalog.dropStream(streamId, true).join());

        LogMetadata purged = LOG_METADATA_SERDE.deserialize(
            partitionPath, partition.get().value());
        assertTrue(purged.deleted());
        assertEquals(-1L, purged.streamId());
        assertTrue(purged.retiredStreamIds().isEmpty());
        assertTrue(purged.purgeableRetiredStreamIds().isEmpty());
        assertNull(config.get());
        assertTrue(MAPPER.readTree(tombstoneState.get().value())
            .path("_purgeRequested").asBoolean());
        verify(logStorage).deleteLog(LogId.of(physicalStreamId));

        assertFalse(recoveringCatalog.dropStream(streamId, true).join());
        verify(logStorage).deleteLog(LogId.of(physicalStreamId));
    }

    @Test
    void failedPurgeUpgradeKeepsPurgeIntentAndCanBeRecovered() throws Exception {
        long physicalStreamId = 310L;
        String configPath = catalogPaths.streamConfigPath(streamId);
        String partitionPath = catalogPaths.partitionMetadataPath(streamId, 0);
        String mappingKey = "lakestream-native/" + streamId.fullName()
            + "/partition-0";
        String incarnation = "recoverable-purge-upgrade";
        String ownerToken = "registration-owner";
        AtomicReference<VersionedValue> config = mockVersionedConfig(
            configPath, ownedStreamConfigBytes(
                1, Map.of("source", "kafka"), incarnation, ownerToken,
                "NATIVE_CREATE"));
        byte[] metadata = LOG_METADATA_SERDE.serialize(
            partitionPath, new LogMetadata(
                physicalStreamId, Map.of(), OptionalLong.empty(),
                incarnation, ownerToken, 1L, false));
        mockVersionedConfig(partitionPath, metadata);
        RuntimeException cleanupFailure = new RuntimeException("delete failed");
        when(logStorage.deleteLog(LogId.of(physicalStreamId)))
            .thenReturn(CompletableFuture.failedFuture(cleanupFailure))
            .thenReturn(CompletableFuture.completedFuture(null));
        FencedStorageHarness mappings = new FencedStorageHarness(
            key -> CompletableFuture.completedFuture(311L));
        mappings.setActive(mappingKey, physicalStreamId,
            new StreamIdMappingOwner(incarnation, ownerToken, 1L));
        IndexedStreamCatalog recoveringCatalog = fencedCatalog(mappings);
        String configPrefix = catalogPaths.streamConfigPrefix(streamId.namespace());
        when(oxiaClient.list(configPrefix, configPrefix + "\uffff"))
            .thenReturn(CompletableFuture.completedFuture(List.of(configPath)));

        assertTrue(recoveringCatalog.dropStream(streamId, false).join());
        assertNull(config.get());

        CompletionException failedPurge = assertThrows(CompletionException.class, () ->
            recoveringCatalog.dropStream(streamId, true).join());
        assertEquals(cleanupFailure, failedPurge.getCause());
        // The purge intent is durable in the tombstone before cleanup runs, so a failed purge
        // upgrade is retryable even though the identity is already permanently deleted.
        JsonNode retryable = MAPPER.readTree(tombstoneState.get().value());
        assertEquals("DROPPED", retryable.path("_provisioningState").asText());
        assertTrue(retryable.path("_purgeRequested").asBoolean());
        assertTrue(recoveringCatalog.listStreamEntries(streamId.namespace()).join().isEmpty());

        assertFalse(recoveringCatalog.dropStream(streamId, true).join());

        JsonNode completed = MAPPER.readTree(tombstoneState.get().value());
        assertEquals("DROPPED", completed.path("_provisioningState").asText());
        assertTrue(completed.path("_purgeRequested").asBoolean());
        assertTrue(recoveringCatalog.listStreamEntries(streamId.namespace()).join().isEmpty());
        verify(logStorage, times(2)).deleteLog(LogId.of(physicalStreamId));
    }

    @Test
    void metadataOnlyDropFencesWritesAndRequiresOpenLeasesToDrainBeforeCompletion()
            throws Exception {
        long physicalStreamId = 320L;
        String configPath = catalogPaths.streamConfigPath(streamId);
        String partitionPath = catalogPaths.partitionMetadataPath(streamId, 0);
        String mappingKey = "lakestream-native/" + streamId.fullName()
            + "/partition-0";
        String incarnation = "write-fenced-drop";
        String ownerToken = "registration-owner";
        AtomicReference<VersionedValue> config = mockVersionedConfig(
            configPath, ownedStreamConfigBytes(
                1, Map.of(), incarnation, ownerToken, "NATIVE_CREATE"));
        byte[] metadata = LOG_METADATA_SERDE.serialize(
            partitionPath, new LogMetadata(
                physicalStreamId, Map.of(), OptionalLong.empty(),
                incarnation, ownerToken, 1L, false));
        AtomicReference<VersionedValue> partition =
            mockVersionedConfig(partitionPath, metadata);
        when(logStorage.deleteLog(LogId.of(physicalStreamId)))
            .thenReturn(CompletableFuture.completedFuture(null));
        FencedStorageHarness mappings = new FencedStorageHarness(
            key -> CompletableFuture.completedFuture(321L));
        mappings.setActive(mappingKey, physicalStreamId,
            new StreamIdMappingOwner(incarnation, ownerToken, 1L));
        IndexedStreamCatalog lifecycleCatalog = fencedCatalog(mappings);
        Log opened = lifecycleCatalog.openLog(
            streamId, LogId.of(physicalStreamId)).join();

        CompletionException blocked = assertThrows(CompletionException.class, () ->
            lifecycleCatalog.dropStream(streamId, false).join());

        assertInstanceOf(IllegalStateException.class, blocked.getCause());
        assertEquals("ABORTING", MAPPER.readTree(config.get().value())
            .path("_provisioningState").asText());
        verify(logStorage, never()).deleteLog(any());
        CompletionException fencedWhileOpen = assertThrows(CompletionException.class, () ->
            mappings.storageApi().acquireStreamWriteLease(physicalStreamId).join());
        assertInstanceOf(IllegalStateException.class, fencedWhileOpen.getCause());

        opened.close();
        assertTrue(lifecycleCatalog.dropStream(streamId, false).join());

        assertNull(config.get());
        assertEquals("DROPPED", MAPPER.readTree(tombstoneState.get().value())
            .path("_provisioningState").asText());
        LogMetadata retained = LOG_METADATA_SERDE.deserialize(
            partitionPath, partition.get().value());
        assertEquals(physicalStreamId, retained.streamId());
        verify(logStorage, never()).deleteLog(any());
        CompletionException fencedAfterClose = assertThrows(CompletionException.class, () ->
            mappings.storageApi().acquireStreamWriteLease(physicalStreamId).join());
        assertInstanceOf(IllegalStateException.class, fencedAfterClose.getCause());

        assertFalse(lifecycleCatalog.dropStream(streamId, true).join());

        assertEquals(-1L, LOG_METADATA_SERDE.deserialize(
            partitionPath, partition.get().value()).streamId());
        verify(mappings.storageApi(), times(3))
            .fenceAndDrainStreamWrites(physicalStreamId);
        verify(logStorage).deleteLog(LogId.of(physicalStreamId));
    }

    @Test
    void repeatedPurgingDropKeepsDurableFenceAndClearsCleanupJournal()
            throws Exception {
        long originalStreamId = 300L;
        long lateStreamId = 301L;
        String configPath = catalogPaths.streamConfigPath(streamId);
        String partitionPath = catalogPaths.partitionMetadataPath(streamId, 0);
        String mappingKey = "lakestream-native/" + streamId.fullName()
            + "/partition-0";
        String incarnation = "native-purge-recovery";
        AtomicReference<VersionedValue> config = mockVersionedConfig(
            configPath, ownedStreamConfigBytes(
                1, Map.of(), incarnation, "registration-owner", "NATIVE_CREATE"));
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
        assertNull(config.get());
        assertTrue(MAPPER.readTree(tombstoneState.get().value())
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
        assertEquals(-1L, recovered.streamId());
        verify(logStorage).deleteLog(LogId.of(originalStreamId));
        verify(logStorage, never()).deleteLog(LogId.of(lateStreamId));
    }

    @Test
    void dropStream_notFoundWritesPermanentIdentityFence() throws Exception {
        String configPath = catalogPaths.streamConfigPath(streamId);
        AtomicReference<VersionedValue> config = mockVersionedConfig(configPath);

        assertFalse(catalog.dropStream(streamId, false).get());

        assertNull(config.get());
        JsonNode tombstone = MAPPER.readTree(tombstoneState.get().value());
        assertEquals("DROPPED", tombstone.path("_provisioningState").asText());
        assertEquals(0, tombstone.path("partitions").asInt());
        CompletionException failure = assertThrows(CompletionException.class, () ->
            catalog.createStream(
                streamId, new StreamConfig(),
                new Partitioning(
                    PartitioningStrategy.INDEXED, Map.of("numPartitions", "1")),
                new SchemaConfig(), Map.of()).join());
        assertInstanceOf(StreamPermanentlyDeletedException.class, failure.getCause());
    }

    @Test
    void absentStreamPurgeIntentOnlyGrows() throws Exception {
        String configPath = catalogPaths.streamConfigPath(streamId);
        AtomicReference<VersionedValue> config = mockVersionedConfig(configPath);

        assertFalse(catalog.dropStream(streamId, false).get());
        assertNull(config.get());
        assertFalse(MAPPER.readTree(tombstoneState.get().value())
            .path("_purgeRequested").asBoolean());

        assertFalse(catalog.dropStream(streamId, true).get());
        assertTrue(MAPPER.readTree(tombstoneState.get().value())
            .path("_purgeRequested").asBoolean());

        assertFalse(catalog.dropStream(streamId, false).get());
        assertTrue(MAPPER.readTree(tombstoneState.get().value())
            .path("_purgeRequested").asBoolean());
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
        CompletableFuture<Boolean> mutation = noCapabilities.dropStream(streamId, true);

        assertInstanceOf(UnsupportedOperationException.class,
            assertThrows(CompletionException.class, mutation::join).getCause());
        assertEquals(0, allocationAttempts.get());
        assertEquals(0, lookupAttempts.get());
        assertEquals(0, deletionAttempts.get());
        verify(oxiaClient, never()).put(anyString(), any(byte[].class), any());
        verify(logStorage, never()).deleteLog(any());
    }

    @Test
    void dropRequiresDurableWriteFencingInAdditionToMappingFencing() {
        StorageApi mappingOnlyStorage = mock(StorageApi.class);
        when(mappingOnlyStorage.supportsConditionalStreamIdMappingDeletion())
            .thenReturn(true);
        when(mappingOnlyStorage.supportsFencedStreamIdMappings()).thenReturn(true);
        when(mappingOnlyStorage.supportsDurableStreamWriteFencing()).thenReturn(false);
        IndexedStreamCatalog mappingOnlyCatalog = new IndexedStreamCatalog(
            oxiaClient, catalogPaths, logStorage,
            (name, logId, reader) -> null, null, mappingOnlyStorage,
            null, null, List.of());

        CompletionException failure = assertThrows(CompletionException.class, () ->
            mappingOnlyCatalog.dropStream(streamId, false).join());

        assertInstanceOf(UnsupportedOperationException.class, failure.getCause());
        verify(oxiaClient, never()).put(anyString(), any(byte[].class), any());
        verify(mappingOnlyStorage, never()).fenceAndDrainStreamWrites(anyLong());
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
            noCapabilities.dropStream(streamId, false).join());

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
            noCapabilities.dropStream(streamId, false).join());

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

        assertNull(config.get());
        assertEquals("DROPPED", MAPPER.readTree(tombstoneState.get().value())
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
    void droppedNativeIdentityCannotBeRecreated() throws Exception {
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
        defaultStorage.setFence(
            "lakestream-native/" + streamId.fullName() + "/partition-0",
            new StreamIdMappingFence(
                300L, new StreamIdMappingOwner(
                    "old-incarnation", "old-drop-owner", 2L)));

        CompletionException failure = assertThrows(CompletionException.class, () ->
            catalog.createStream(
                streamId, new StreamConfig(),
                new Partitioning(
                    PartitioningStrategy.INDEXED, Map.of("numPartitions", "1")),
                new SchemaConfig(), Map.of()).join());

        assertInstanceOf(StreamPermanentlyDeletedException.class, failure.getCause());
        assertEquals("DROPPED", MAPPER.readTree(config.get().value())
            .path("_provisioningState").asText());
        assertEquals(oldTombstone.length, partition.get().value().length);
        assertTrue(LOG_METADATA_SERDE.deserialize(
            partitionPath, partition.get().value()).deleted());
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
                    + "\"_ownerGeneration\":1,"
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
                    + "\"_ownerGeneration\":2,\"_metadataSourceOwnerToken\":\"owner\","
                    + "\"_metadataSourceGeneration\":1,"
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

    // --- openWriter ---

    @Test
    void openWriter_success() throws Exception {
        mockStreamConfig(streamId, 1);
        mockPartitionMetadata(streamId, 0, 100L, Map.of());

        StreamWriter writer = catalog.openWriter(streamId).get();
        assertNotNull(writer);
        assertInstanceOf(StreamWriterImpl.class, writer);
        verify(defaultStorage.storageApi()).acquireStreamWriteLease(100L);
        writer.close();
        verify(defaultStorage.writeLeases().get(0)).closeAsync();
    }

    @Test
    void openLogAcquiresLeaseAndReleasesItOnClose() throws Exception {
        mockStreamConfig(streamId, 1);
        mockPartitionMetadata(streamId, 0, 100L, Map.of());

        Log opened = catalog.openLog(streamId, LogId.of(100L)).get();

        assertInstanceOf(LeasedLog.class, opened);
        verify(defaultStorage.storageApi()).acquireStreamWriteLease(100L);
        opened.close();
        verify(defaultStorage.writeLeases().get(0)).closeAsync();
    }

    @Test
    void openLogByPartitionIndexReadsLayoutOnce() throws Exception {
        mockStreamConfig(streamId, 4);
        // Only the opened partition is stubbed: reading any other one would be a strict-stub
        // failure as well as a violation of the never() checks below.
        mockPartitionMetadata(streamId, 1, 101L, Map.of());

        Log opened = catalog.openLog(streamId, 1).get(10, TimeUnit.SECONDS);

        assertNotNull(opened);
        verify(defaultStorage.storageApi()).acquireStreamWriteLease(101L);
        // Opening one partition costs a constant number of catalog reads however wide the stream
        // is: the active config, that one partition's metadata, and the lifecycle re-read that
        // fences it. Building the layout instead would read every partition.
        verify(oxiaClient, times(2)).get(catalogPaths.streamConfigPath(streamId));
        verify(oxiaClient).get(catalogPaths.partitionMetadataPath(streamId, 1));
        for (int untouched : List.of(0, 2, 3)) {
            verify(oxiaClient, never())
                .get(catalogPaths.partitionMetadataPath(streamId, untouched));
        }

        ExecutionException outOfRange = assertThrows(ExecutionException.class,
            () -> catalog.openLog(streamId, 4).get(10, TimeUnit.SECONDS));
        assertEquals(IllegalArgumentException.class, outOfRange.getCause().getClass());
        opened.close();
    }

    @Test
    void openLogByPartitionIndexSurvivesAConcurrentPropertyReplace() throws Exception {
        String configPath = catalogPaths.streamConfigPath(streamId);
        // Same lifecycle, same committed layout, newer config version: a controller replacing
        // properties between the catalog read and the partition read is routine on a stream a
        // broker is opening, and must not fail the open.
        when(oxiaClient.get(configPath))
            .thenReturn(CompletableFuture.completedFuture(new GetResult(
                configPath, streamConfigBytes(2, Map.of(), false), FakeOxiaRecord.version(1))))
            .thenReturn(CompletableFuture.completedFuture(new GetResult(
                configPath, streamConfigBytes(2, Map.of("tier", "hot"), false),
                FakeOxiaRecord.version(2))));
        mockPartitionMetadata(streamId, 1, 101L, Map.of());

        Log opened = catalog.openLog(streamId, 1).get(10, TimeUnit.SECONDS);

        assertNotNull(opened);
        verify(defaultStorage.storageApi()).acquireStreamWriteLease(101L);
        verify(oxiaClient, times(1)).get(catalogPaths.partitionMetadataPath(streamId, 1));
        opened.close();
    }

    @Test
    void openLogByPartitionIndexRereadsThePartitionWhenTheLayoutGrows() throws Exception {
        String configPath = catalogPaths.streamConfigPath(streamId);
        // An expansion landing mid-read changes the committed layout the first read was made
        // against, so the partition is read again under the config that moved it - the same
        // retry the layout read performs - rather than failing the open.
        when(oxiaClient.get(configPath))
            .thenReturn(CompletableFuture.completedFuture(new GetResult(
                configPath, streamConfigBytes(2, Map.of(), false), FakeOxiaRecord.version(1))))
            .thenReturn(CompletableFuture.completedFuture(new GetResult(
                configPath, streamConfigBytes(3, Map.of(), false), FakeOxiaRecord.version(2))));
        mockPartitionMetadata(streamId, 1, 101L, Map.of());

        Log opened = catalog.openLog(streamId, 1).get(10, TimeUnit.SECONDS);

        assertNotNull(opened);
        verify(defaultStorage.storageApi()).acquireStreamWriteLease(101L);
        verify(oxiaClient, times(3)).get(configPath);
        verify(oxiaClient, times(2)).get(catalogPaths.partitionMetadataPath(streamId, 1));
        opened.close();
    }

    @Test
    void openLogByPartitionIndexFailsWhenTheIdentityIsRecreatedMidRead() throws Exception {
        String configPath = catalogPaths.streamConfigPath(streamId);
        // A drop and recreate between the two reads is a different stream identity; the open must
        // not hand back a log belonging to the incarnation that is gone.
        when(oxiaClient.get(configPath))
            .thenReturn(CompletableFuture.completedFuture(new GetResult(
                configPath, streamConfigBytes(2, Map.of(), false), FakeOxiaRecord.version(1))))
            .thenReturn(CompletableFuture.completedFuture(new GetResult(
                configPath, ownedStreamConfigBytes(2, Map.of(), "second-incarnation",
                    "second-owner", "NATIVE_CREATE"), FakeOxiaRecord.version(2))));
        mockPartitionMetadata(streamId, 1, 101L, Map.of());

        ExecutionException failure = assertThrows(ExecutionException.class,
            () -> catalog.openLog(streamId, 1).get(10, TimeUnit.SECONDS));

        assertEquals(NoSuchStreamException.class, failure.getCause().getClass());
        verify(defaultStorage.storageApi(), never()).acquireStreamWriteLease(anyLong());
    }

    @Test
    void abandonedWriterCleanupGivesUpOnceItsExecutorIsShutDown() throws Exception {
        StreamWriter writer = mock(StreamWriter.class);
        // Closing the catalog shuts the cleanup executor down. A rejection from it can never
        // succeed on retry, so bounding the wait turns "would retry forever" into a failure.
        catalog.close();

        CompletableFuture<Void> cleanup = catalog.startAbandonedWriterCleanup(writer);

        ExecutionException rejected = assertThrows(ExecutionException.class,
            () -> cleanup.get(5, TimeUnit.SECONDS));
        assertInstanceOf(RejectedExecutionException.class, rejected.getCause());
        verify(writer, never()).close();
    }

    @Test
    void catalogCloseWaitsForReturnedLogHandle() throws Exception {
        mockStreamConfig(streamId, 1);
        mockPartitionMetadata(streamId, 0, 100L, Map.of());
        Log opened = catalog.openLog(streamId, LogId.of(100L)).get();

        assertThrows(IOException.class, () -> catalog.awaitOpenCleanupBeforeClose(25L));
        verify(defaultStorage.writeLeases().get(0), never()).closeAsync();

        opened.close();
        catalog.close();

        verify(defaultStorage.writeLeases().get(0)).closeAsync();
    }

    @Test
    void inlineOpenCompletionCanCloseLogAndCatalogWithoutWaitingOnItself()
            throws Exception {
        mockStreamConfig(streamId, 1);
        mockPartitionMetadata(streamId, 0, 100L, Map.of());

        catalog.openLog(streamId, LogId.of(100L)).thenAccept(opened -> {
            try {
                opened.close();
                catalog.awaitOpenCleanupBeforeClose(25L);
            } catch (Exception failure) {
                throw new CompletionException(failure);
            }
        }).get();

        catalog.close();
    }

    @Test
    void canceledOpenLogClosesLateHandleAndReleasesLease() throws Exception {
        mockStreamConfig(streamId, 1);
        mockPartitionMetadata(streamId, 0, 100L, Map.of());
        FencedStorageHarness storage = new FencedStorageHarness(
            key -> CompletableFuture.completedFuture(100L));
        Log rawLog = mock(Log.class);
        when(rawLog.id()).thenReturn(LogId.of(100L));
        IndexedStreamCatalog lifecycleCatalog = fencedCatalog(
            storage, (name, logId, reader) -> rawLog);
        CompletableFuture<StreamWriteLease> pendingLease = new CompletableFuture<>();
        when(storage.storageApi().acquireStreamWriteLease(100L)).thenReturn(pendingLease);
        StreamWriteLease lease = mock(StreamWriteLease.class);
        when(lease.streamId()).thenReturn(100L);
        when(lease.closeAsync()).thenReturn(CompletableFuture.completedFuture(null));

        CompletableFuture<Log> opening =
            lifecycleCatalog.openLog(streamId, LogId.of(100L));

        assertTrue(opening.cancel(false));
        pendingLease.complete(lease);
        lifecycleCatalog.close();

        verify(rawLog).close();
        verify(lease).closeAsync();
    }

    @Test
    void canceledOpenWriterClosesEveryLateLogAndLease() throws Exception {
        mockStreamConfig(streamId, 2);
        mockPartitionMetadata(streamId, 0, 100L, Map.of());
        mockPartitionMetadata(streamId, 1, 101L, Map.of());
        FencedStorageHarness storage = new FencedStorageHarness(
            key -> CompletableFuture.completedFuture(100L));
        Log firstRawLog = mock(Log.class);
        Log secondRawLog = mock(Log.class);
        when(firstRawLog.id()).thenReturn(LogId.of(100L));
        when(secondRawLog.id()).thenReturn(LogId.of(101L));
        IndexedStreamCatalog lifecycleCatalog = fencedCatalog(storage,
            (name, logId, reader) -> logId.equals(LogId.of(100L))
                ? firstRawLog : secondRawLog);
        StreamWriteLease firstLease = mock(StreamWriteLease.class);
        StreamWriteLease secondLease = mock(StreamWriteLease.class);
        when(firstLease.streamId()).thenReturn(100L);
        when(secondLease.streamId()).thenReturn(101L);
        when(firstLease.closeAsync()).thenReturn(CompletableFuture.completedFuture(null));
        when(secondLease.closeAsync()).thenReturn(CompletableFuture.completedFuture(null));
        CompletableFuture<StreamWriteLease> pendingSecondLease = new CompletableFuture<>();
        when(storage.storageApi().acquireStreamWriteLease(100L))
            .thenReturn(CompletableFuture.completedFuture(firstLease));
        when(storage.storageApi().acquireStreamWriteLease(101L))
            .thenReturn(pendingSecondLease);

        CompletableFuture<StreamWriter> opening = lifecycleCatalog.openWriter(streamId);

        assertTrue(opening.cancel(false));
        pendingSecondLease.complete(secondLease);
        lifecycleCatalog.close();

        verify(firstRawLog).close();
        verify(secondRawLog).close();
        verify(firstLease).closeAsync();
        verify(secondLease).closeAsync();
    }

    @Test
    void canceledOpenReaderClosesLateReaderBeforeCatalogResources() throws Exception {
        mockStreamConfig(streamId, 1);
        String partitionPath = catalogPaths.partitionMetadataPath(streamId, 0);
        byte[] partitionMetadata = LOG_METADATA_SERDE.serialize(
            partitionPath, new LogMetadata(100L, Map.of(), OptionalLong.empty()));
        CompletableFuture<GetResult> pendingPartition = new CompletableFuture<>();
        when(oxiaClient.get(partitionPath)).thenReturn(pendingPartition);

        CompletableFuture<StreamReader> opening = catalog.openReader(streamId);

        assertTrue(opening.cancel(false));
        pendingPartition.complete(
            new GetResult(partitionPath, partitionMetadata, DUMMY_VERSION));
        catalog.close();
    }

    @Test
    void openWriterFailureRetainsAndRetriesEveryAcquiredLease() throws Exception {
        mockStreamConfig(streamId, 2);
        mockPartitionMetadata(streamId, 0, 100L, Map.of());
        mockPartitionMetadata(streamId, 1, 101L, Map.of());
        RuntimeException secondOpenFailure = new RuntimeException("second log failed to open");
        IOException firstCloseFailure = new IOException("first log failed to close");
        CountDownLatch firstLogClosed = new CountDownLatch(1);
        AtomicInteger firstCloseAttempts = new AtomicInteger();
        Log firstLog = mock(Log.class);
        when(firstLog.id()).thenReturn(LogId.of(100L));
        doAnswer(ignored -> {
            if (firstCloseAttempts.getAndIncrement() == 0) {
                throw firstCloseFailure;
            }
            firstLogClosed.countDown();
            return null;
        }).when(firstLog).close();
        FencedStorageHarness storage = new FencedStorageHarness(
            key -> CompletableFuture.completedFuture(100L));
        IndexedStreamCatalog failingCatalog = fencedCatalog(storage,
            (name, logId, reader) -> {
                if (logId.equals(LogId.of(100L))) {
                    return firstLog;
                }
                throw secondOpenFailure;
            });

        ExecutionException failure = assertThrows(ExecutionException.class,
            () -> failingCatalog.openWriter(streamId).get());

        assertEquals(secondOpenFailure, failure.getCause());
        assertTrue(firstLogClosed.await(5, TimeUnit.SECONDS));
        failingCatalog.close();
        assertEquals(2, storage.writeLeases().size());
        verify(firstLog, times(2)).close();
        verify(storage.writeLeases().get(0)).closeAsync();
        verify(storage.writeLeases().get(1)).closeAsync();
    }

    @Test
    void failedOpenCleanupQueuedBehindABusyWorkerKeepsOwnership() throws Exception {
        mockStreamConfig(streamId, 1);
        mockPartitionMetadata(streamId, 0, 100L, Map.of());
        Log wrongLog = mock(Log.class);
        when(wrongLog.id()).thenReturn(LogId.of(999L));
        FencedStorageHarness storage = new FencedStorageHarness(
            key -> CompletableFuture.completedFuture(100L));
        IndexedStreamCatalog failingCatalog = fencedCatalog(
            storage, (name, logId, reader) -> wrongLog);
        ThreadPoolExecutor cleanupExecutor = failedOpenCleanupExecutor(failingCatalog);
        CountDownLatch cleanupWorkerStarted = new CountDownLatch(1);
        CountDownLatch releaseCleanupWorker = new CountDownLatch(1);
        // One thread and an unbounded queue: occupying the thread leaves the failed open's
        // cleanup waiting its turn rather than rejected, and it must still run in the end.
        cleanupExecutor.execute(() -> {
            cleanupWorkerStarted.countDown();
            try {
                releaseCleanupWorker.await();
            } catch (InterruptedException failure) {
                Thread.currentThread().interrupt();
            }
        });
        assertTrue(cleanupWorkerStarted.await(5, TimeUnit.SECONDS));

        ExecutionException failure = assertThrows(ExecutionException.class,
            () -> failingCatalog.openLog(streamId, LogId.of(100L)).get());

        assertInstanceOf(IllegalArgumentException.class, failure.getCause());
        verify(wrongLog, never()).close();
        verify(storage.writeLeases().get(0), never()).closeAsync();

        releaseCleanupWorker.countDown();
        failingCatalog.close();

        verify(wrongLog).close();
        verify(storage.writeLeases().get(0)).closeAsync();
    }

    @Test
    void catalogCloseKeepsOwnedResourcesAliveUntilFailedOpenCleanupDrains()
            throws Exception {
        mockStreamConfig(streamId, 1);
        mockPartitionMetadata(streamId, 0, 100L, Map.of());
        CountDownLatch rawCloseStarted = new CountDownLatch(1);
        CountDownLatch allowRawClose = new CountDownLatch(1);
        Log wrongLog = mock(Log.class);
        when(wrongLog.id()).thenReturn(LogId.of(999L));
        doAnswer(ignored -> {
            rawCloseStarted.countDown();
            allowRawClose.await();
            return null;
        }).when(wrongLog).close();
        AutoCloseable ownedResource = mock(AutoCloseable.class);
        FencedStorageHarness storage = new FencedStorageHarness(
            key -> CompletableFuture.completedFuture(100L));
        IndexedStreamCatalog failingCatalog = new IndexedStreamCatalog(
            oxiaClient, catalogPaths, logStorage,
            (name, logId, reader) -> wrongLog, null, storage.storageApi(),
            null, null, List.of(ownedResource));

        ExecutionException failure = assertThrows(ExecutionException.class,
            () -> failingCatalog.openLog(streamId, LogId.of(100L)).get());

        assertInstanceOf(IllegalArgumentException.class, failure.getCause());
        assertTrue(rawCloseStarted.await(5, TimeUnit.SECONDS));
        assertThrows(IOException.class,
            () -> failingCatalog.awaitOpenCleanupBeforeClose(25L));
        verify(ownedResource, never()).close();
        verify(storage.writeLeases().get(0), never()).closeAsync();

        allowRawClose.countDown();
        failingCatalog.close();

        verify(wrongLog).close();
        verify(storage.writeLeases().get(0)).closeAsync();
        verify(ownedResource).close();
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
        assertInstanceOf(CatalogOwnedStreamReader.class, reader);
        reader.close();
    }

    @Test
    void catalogCloseWaitsForReturnedReaderHandle() throws Exception {
        mockStreamConfig(streamId, 1);
        mockPartitionMetadata(streamId, 0, 100L, Map.of());
        StreamReader reader = catalog.openReader(streamId).get();

        assertThrows(IOException.class, () -> catalog.awaitOpenCleanupBeforeClose(25L));

        reader.close();
        catalog.close();
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

    private CreationOutcome createPartitionsSequentially(StreamIdentifier id) throws Exception {
        FencedStorageHarness storage = new FencedStorageHarness(
            IndexedStreamCatalogTest::allocateByPartitionIndex);
        IndexedStreamCatalog target = fencedCatalog(storage);
        mockVersionedConfig(catalogPaths.streamConfigPath(id));
        List<AtomicReference<VersionedValue>> partitions = List.of(
            mockCreateOnlyRecord(catalogPaths.partitionMetadataPath(id, 0)),
            mockCreateOnlyRecord(catalogPaths.partitionMetadataPath(id, 1)));

        StreamMetadata metadata = target.createStream(
                id, new StreamConfig(), indexedPartitioning(2), new SchemaConfig(), Map.of())
            .get(10, TimeUnit.SECONDS);

        return creationOutcome(id, storage, metadata, partitions);
    }

    /**
     * Creates a two-partition stream while partition 1's chain runs ahead of partition 0's.
     *
     * <p>Both chains park on their first partition read; partition 1's Oxia futures are then
     * completed to the end of its chain before partition 0's first read is answered at all.
     */
    private CreationOutcome createPartitionsCompletingOutOfOrder(StreamIdentifier id)
            throws Exception {
        FencedStorageHarness storage = new FencedStorageHarness(
            IndexedStreamCatalogTest::allocateByPartitionIndex);
        IndexedStreamCatalog target = fencedCatalog(storage);
        mockVersionedConfig(catalogPaths.streamConfigPath(id));
        DeferredRecord first = mockDeferredCreateOnlyRecord(
            catalogPaths.partitionMetadataPath(id, 0));
        DeferredRecord second = mockDeferredCreateOnlyRecord(
            catalogPaths.partitionMetadataPath(id, 1));

        CompletableFuture<StreamMetadata> create = target.createStream(
            id, new StreamConfig(), indexedPartitioning(2), new SchemaConfig(), Map.of());

        assertFalse(create.isDone());
        assertEquals(1, first.pendingCount());
        assertEquals(1, second.pendingCount());

        second.releaseAll();
        assertFalse(create.isDone());
        assertNotNull(second.state().get(), "partition 1 must have committed its metadata");
        assertNull(first.state().get(), "partition 0 must not have written anything yet");
        assertEquals(1, first.pendingCount(), "partition 0 must not have made any progress");

        first.releaseAll();
        // Finalization and the layout read that follows it touch both partitions again.
        while (!create.isDone()) {
            assertTrue(first.releaseAll() | second.releaseAll(),
                "creation stalled with no pending partition metadata calls");
        }

        return creationOutcome(id, storage, create.get(10, TimeUnit.SECONDS),
            List.of(first.state(), second.state()));
    }

    private CreationOutcome creationOutcome(
            StreamIdentifier id, FencedStorageHarness storage, StreamMetadata metadata,
            List<AtomicReference<VersionedValue>> partitions) throws Exception {
        List<Long> logIds = metadata.layout().logIds().join().stream()
            .map(LogId::id).toList();
        Map<Integer, Long> mappedStreamIds = new HashMap<>();
        Map<Integer, Long> persistedStreamIds = new HashMap<>();
        for (int index = 0; index < partitions.size(); index++) {
            String path = catalogPaths.partitionMetadataPath(id, index);
            mappedStreamIds.put(index, storage.activeStreamId(
                "lakestream-native/" + id.fullName() + "/partition-" + index).orElseThrow());
            persistedStreamIds.put(index, LOG_METADATA_SERDE.deserialize(
                path, partitions.get(index).get().value()).streamId());
        }
        List<String> fenceAttempts = storage.fenceAttempts().stream()
            .map(attempt -> attempt.replace(id.fullName(), "<stream>"))
            .toList();
        return new CreationOutcome(logIds, Map.copyOf(mappedStreamIds),
            Map.copyOf(persistedStreamIds), fenceAttempts, storage.writeLeases().size());
    }

    private static CompletableFuture<Long> allocateByPartitionIndex(String allocationKey) {
        String marker = "/partition-";
        int index = Integer.parseInt(
            allocationKey.substring(allocationKey.lastIndexOf(marker) + marker.length()));
        return CompletableFuture.completedFuture(100L + index);
    }

    private static Partitioning indexedPartitioning(int partitions) {
        return new Partitioning(
            PartitioningStrategy.INDEXED, Map.of("numPartitions", String.valueOf(partitions)));
    }

    private void mockStreamConfig(StreamIdentifier id, int numPartitions) {
        mockStreamConfig(id, numPartitions, Map.of());
    }

    private void mockStreamConfig(StreamIdentifier id, int numPartitions,
                                    Map<String, String> properties) {
        String configPath = catalogPaths.streamConfigPath(id);
        try {
            byte[] bytes = streamConfigBytes(numPartitions, properties, false);
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
        config.put("_creationKind", "NATIVE_CREATE");
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
        FakeOxiaRecord record = new FakeOxiaRecord(path, initialValue);
        lenient().when(oxiaClient.get(path))
            .thenAnswer(ignored -> CompletableFuture.completedFuture(record.applyGet()));
        lenient().when(oxiaClient.put(eq(path), any(byte[].class), any())).thenAnswer(invocation -> {
            byte[] value = invocation.getArgument(1, byte[].class);
            @SuppressWarnings("unchecked")
            Set<PutOption> options = invocation.getArgument(2, Set.class);
            return FakeOxiaRecord.settle(() -> record.applyPut(value, options));
        });
        lenient().when(oxiaClient.delete(eq(path), any())).thenAnswer(invocation -> {
            @SuppressWarnings("unchecked")
            Set<DeleteOption> options = invocation.getArgument(1, Set.class);
            return FakeOxiaRecord.settle(() -> record.applyDelete(options));
        });
        return record.state();
    }

    private AtomicReference<VersionedValue> mockCreateOnlyRecord(String path) {
        return mockCreateOnlyRecord(path, null);
    }

    private AtomicReference<VersionedValue> mockCreateOnlyRecord(
            String path, RuntimeException firstWriteFailure) {
        FakeOxiaRecord record = new FakeOxiaRecord(path, null);
        AtomicReference<RuntimeException> pendingFailure =
            new AtomicReference<>(firstWriteFailure);
        lenient().when(oxiaClient.get(path))
            .thenAnswer(ignored -> CompletableFuture.completedFuture(record.applyGet()));
        lenient().when(oxiaClient.put(eq(path), any(byte[].class), any()))
            .thenAnswer(invocation -> {
                RuntimeException failure = pendingFailure.getAndSet(null);
                if (failure != null) {
                    return CompletableFuture.failedFuture(failure);
                }
                byte[] value = invocation.getArgument(1, byte[].class);
                @SuppressWarnings("unchecked")
                Set<PutOption> options = invocation.getArgument(2, Set.class);
                return FakeOxiaRecord.settle(() -> record.applyPut(value, options));
            });
        return record.state();
    }

    /**
     * Stubs a create-only record whose reads and writes settle only when the test releases them.
     *
     * <p>Each call is evaluated against the record's state at release time, not at call time, so
     * releasing one path's queue ahead of another's reproduces an out-of-order completion.
     */
    private DeferredRecord mockDeferredCreateOnlyRecord(String path) {
        DeferredRecord deferred = new DeferredRecord(new FakeOxiaRecord(path, null));
        lenient().when(oxiaClient.get(path))
            .thenAnswer(ignored -> deferred.defer(deferred.record()::applyGet));
        lenient().when(oxiaClient.put(eq(path), any(byte[].class), any()))
            .thenAnswer(invocation -> {
                byte[] value = invocation.getArgument(1, byte[].class);
                @SuppressWarnings("unchecked")
                Set<PutOption> options = invocation.getArgument(2, Set.class);
                return deferred.defer(() -> deferred.record().applyPut(value, options));
            });
        return deferred;
    }

    private IndexedStreamCatalog fencedCatalog(FencedStorageHarness storage) {
        return fencedCatalog(storage, (name, logId, reader) -> {
            Log opened = mock(Log.class);
            lenient().when(opened.id()).thenReturn(logId);
            return opened;
        });
    }

    private IndexedStreamCatalog fencedCatalog(
            FencedStorageHarness storage, IndexedStreamCatalog.LogFactory logFactory) {
        return new IndexedStreamCatalog(
            oxiaClient, catalogPaths, logStorage,
            logFactory, null, storage.storageApi(),
            null, null, List.of());
    }

    private ThreadPoolExecutor failedOpenCleanupExecutor(IndexedStreamCatalog target)
            throws ReflectiveOperationException {
        Field field = IndexedStreamCatalog.class.getDeclaredField(
            "failedOpenCleanupExecutor");
        field.setAccessible(true);
        ExecutorService executor = (ExecutorService) field.get(target);
        return assertInstanceOf(ThreadPoolExecutor.class, executor);
    }

    private final class FencedStorageHarness {

        private final StorageApi storageApi = mock(StorageApi.class);
        private final Function<String, CompletableFuture<Long>> streamIdAllocator;
        private final Map<String, Object> mappings = new HashMap<>();
        private final List<String> fenceAttempts = new ArrayList<>();
        private final AtomicReference<Throwable> nextFenceFailure = new AtomicReference<>();
        private final List<StreamWriteLease> writeLeases = new ArrayList<>();
        private final Map<Long, Set<StreamWriteLease>> activeWriteLeases = new HashMap<>();
        private final Set<Long> writeFencedStreamIds = new HashSet<>();

        private FencedStorageHarness(
                Function<String, CompletableFuture<Long>> streamIdAllocator) {
            this.streamIdAllocator = streamIdAllocator;
            lenient().when(storageApi.supportsConditionalStreamIdMappingDeletion())
                .thenReturn(true);
            lenient().when(storageApi.supportsFencedStreamIdMappings()).thenReturn(true);
            lenient().when(storageApi.supportsDurableStreamWriteFencing()).thenReturn(true);
            lenient().when(storageApi.acquireStreamWriteLease(anyLong()))
                .thenAnswer(invocation -> acquireWriteLease(invocation.getArgument(0, Long.class)));
            lenient().when(storageApi.fenceAndDrainStreamWrites(anyLong()))
                .thenAnswer(invocation ->
                    fenceAndDrainStreamWrites(invocation.getArgument(0, Long.class)));
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

        private synchronized List<StreamWriteLease> writeLeases() {
            return List.copyOf(writeLeases);
        }

        private synchronized CompletableFuture<StreamWriteLease> acquireWriteLease(long streamId) {
            if (writeFencedStreamIds.contains(streamId)) {
                return CompletableFuture.failedFuture(new IllegalStateException(
                    "writes are permanently fenced for stream " + streamId));
            }
            StreamWriteLease lease = mock(StreamWriteLease.class);
            lenient().when(lease.streamId()).thenReturn(streamId);
            lenient().when(lease.closeAsync())
                .thenAnswer(ignored -> closeWriteLease(streamId, lease));
            writeLeases.add(lease);
            activeWriteLeases.computeIfAbsent(streamId, ignored -> new HashSet<>())
                .add(lease);
            return CompletableFuture.completedFuture(lease);
        }

        private synchronized CompletableFuture<Void> closeWriteLease(
                long streamId, StreamWriteLease lease) {
            Set<StreamWriteLease> leases = activeWriteLeases.get(streamId);
            if (leases != null && leases.remove(lease) && leases.isEmpty()) {
                activeWriteLeases.remove(streamId);
            }
            return CompletableFuture.completedFuture(null);
        }

        private synchronized CompletableFuture<Void> fenceAndDrainStreamWrites(long streamId) {
            writeFencedStreamIds.add(streamId);
            if (activeWriteLeases.containsKey(streamId)) {
                return CompletableFuture.failedFuture(new IllegalStateException(
                    "write leases are still active for stream " + streamId));
            }
            return CompletableFuture.completedFuture(null);
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

    /** The observable result of one stream creation, independent of chain completion order. */
    private record CreationOutcome(
            List<Long> logIds,
            Map<Integer, Long> mappedStreamIds,
            Map<Integer, Long> persistedStreamIds,
            List<String> fenceAttempts,
            int writeLeases) {
    }

    /** A record whose Oxia calls queue up until the test releases them, in call order. */
    private static final class DeferredRecord {

        private final Deque<Runnable> pending = new ArrayDeque<>();
        private final FakeOxiaRecord record;

        private DeferredRecord(FakeOxiaRecord record) {
            this.record = record;
        }

        private FakeOxiaRecord record() {
            return record;
        }

        private AtomicReference<VersionedValue> state() {
            return record.state();
        }

        private <T> CompletableFuture<T> defer(Callable<T> answer) {
            CompletableFuture<T> deferred = new CompletableFuture<>();
            pending.add(() -> {
                try {
                    deferred.complete(answer.call());
                } catch (Throwable failure) {
                    deferred.completeExceptionally(failure);
                }
            });
            return deferred;
        }

        private int pendingCount() {
            return pending.size();
        }

        /** Answers every queued call, including calls the answers themselves trigger. */
        private boolean releaseAll() {
            boolean released = false;
            while (!pending.isEmpty()) {
                pending.poll().run();
                released = true;
            }
            return released;
        }
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

}
