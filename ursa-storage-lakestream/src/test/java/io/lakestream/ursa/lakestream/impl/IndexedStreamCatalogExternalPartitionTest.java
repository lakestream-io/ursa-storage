/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.lakestream.impl;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.lakestream.api.Log;
import io.lakestream.api.LogId;
import io.lakestream.api.LogStorage;
import io.lakestream.api.Partitioning;
import io.lakestream.api.PartitioningStrategy;
import io.lakestream.api.SchemaConfig;
import io.lakestream.api.StreamConfig;
import io.lakestream.api.StreamIdentifier;
import io.lakestream.api.exception.NoSuchStreamException;
import io.lakestream.api.exception.PartitionLifecycleFencedException;
import io.lakestream.ursa.catalog.metadata.LogMetadata;
import io.lakestream.ursa.catalog.metadata.LogMetadataSerde;
import io.lakestream.ursa.lakestream.reader.CompactedObjectReader;
import io.lakestream.ursa.lakestream.reader.CompactedObjectReaderFactory;
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
import io.oxia.client.api.options.PutOption;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class IndexedStreamCatalogExternalPartitionTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final LogMetadataSerde LOG_METADATA_SERDE = LogMetadataSerde.INSTANCE;
    private static final Version VERSION = version(1);

    private final StreamIdentifier stream = new StreamIdentifier("public/default", "my-topic");
    private final DefaultCatalogPaths paths = new DefaultCatalogPaths();
    private AsyncOxiaClient oxiaClient;
    private LogStorage logStorage;
    private CompactedObjectReaderFactory readerFactory;
    private IndexedStreamCatalog.LogFactory logFactory;

    @BeforeEach
    void setUp() {
        oxiaClient = mock(AsyncOxiaClient.class);
        logStorage = mock(LogStorage.class);
        readerFactory = mock(CompactedObjectReaderFactory.class);
        logFactory = mock(IndexedStreamCatalog.LogFactory.class);
        when(logStorage.deleteLog(any()))
            .thenReturn(CompletableFuture.completedFuture(null));
    }

    @Test
    void opensUsingCatalogDerivedNameAndRegistersPartition() throws Exception {
        int partition = 2;
        String logName = paths.compactedReaderName(stream, partition);
        String configPath = paths.streamConfigPath(stream);
        String metadataPath = paths.partitionMetadataPath(stream, partition);
        VersionedRecord config = mockVersionedRecord(configPath, null);
        VersionedRecord metadata = mockVersionedRecord(metadataPath, null);
        AtomicReference<Optional<String>> generatedKey = new AtomicReference<>();
        CompactedObjectReader reader = mock(CompactedObjectReader.class);
        Log log = mock(Log.class);
        when(readerFactory.open(logName)).thenReturn(reader);
        when(logFactory.create(logName, LogId.of(41L), reader)).thenReturn(log);
        IndexedStreamCatalog catalog = catalog(
            key -> {
                generatedKey.set(key);
                return CompletableFuture.completedFuture(41L);
            },
            ignored -> CompletableFuture.completedFuture(41L),
            ignored -> CompletableFuture.completedFuture(null));

        assertThat(catalog.openExternalPartition(
                stream, partition, Map.of("owner", "kafka")).get())
            .isSameAs(log);

        JsonNode active = json(config);
        LogMetadata registered = metadata(metadata);
        assertThat(generatedKey.get()).contains(logName);
        assertThat(active.path("_provisioning").asBoolean(false)).isFalse();
        assertThat(registered.streamId()).isEqualTo(41L);
        assertThat(registered.registrationIncarnationId())
            .isEqualTo(active.path("_incarnationId").asText());
        assertThat(registered.registrationOwnerToken())
            .isEqualTo(active.path("_ownerToken").asText());
        assertThat(registered.registrationOwnerGeneration())
            .isEqualTo(active.path("_ownerGeneration").asLong());
        assertThat(registered.deleted()).isFalse();
        assertThat(metadata.successfulPuts()).isEqualTo(1);
        verify(readerFactory).open(logName);
    }

    @Test
    void permanentDeletionFailsBeforeGeneratingKeyedId() {
        int partition = 2;
        String configPath = paths.streamConfigPath(stream);
        when(oxiaClient.get(configPath)).thenReturn(CompletableFuture.completedFuture(
            permanentDeletion(configPath, VERSION)));
        AtomicInteger generationCount = new AtomicInteger();
        IndexedStreamCatalog catalog = catalog(
            ignored -> {
                generationCount.incrementAndGet();
                return CompletableFuture.completedFuture(41L);
            },
            ignored -> CompletableFuture.completedFuture(41L),
            ignored -> CompletableFuture.completedFuture(null));

        assertThatThrownBy(() -> catalog.openExternalPartition(stream, partition, Map.of()).join())
            .isInstanceOf(CompletionException.class)
            .hasCauseInstanceOf(NoSuchStreamException.class);

        assertThat(generationCount).hasValue(0);
        verify(readerFactory, never()).open(anyString());
        verify(oxiaClient, never()).put(anyString(), any(byte[].class), any());
    }

    @Test
    void permanentDeletionRetainsSpecAndAllowsPartitionCleanup() throws Exception {
        int partition = 0;
        String logName = paths.compactedReaderName(stream, partition);
        String configPath = paths.streamConfigPath(stream);
        String metadataPath = paths.partitionMetadataPath(stream, partition);
        String incarnation = "incarnation-permanent";
        VersionedRecord config = mockVersionedRecord(
            configPath,
            activeExternalConfigBytes(1, incarnation, "registration-owner"));
        VersionedRecord metadata = mockVersionedRecord(
            metadataPath,
            metadataBytes(metadataPath, 41L, incarnation, "registration-owner", 1L, false));
        AtomicReference<Long> mapping = new AtomicReference<>(41L);
        List<String> mappingDeletes = new ArrayList<>();
        IndexedStreamCatalog catalog = catalog(
            ignored -> CompletableFuture.completedFuture(41L),
            ignored -> CompletableFuture.completedFuture(41L),
            key -> mapping.get() == null
                ? CompletableFuture.failedFuture(new NoSuchKeyException(key))
                : CompletableFuture.completedFuture(mapping.get()),
            (key, streamId) -> {
                mappingDeletes.add(key + ":" + streamId);
                clearExpectedMapping(mapping, streamId);
                return CompletableFuture.completedFuture(null);
            });

        catalog.permanentlyDeleteExternalStream(stream).join();
        catalog.deleteExternalPartition(stream, partition).join();

        JsonNode retained = json(config);
        assertThat(retained.path("_provisioningState").asText())
            .isEqualTo("PERMANENTLY_DELETED");
        assertThat(retained.path("partitions").asInt()).isEqualTo(1);
        assertThat(retained.path("_incarnationId").asText()).isEqualTo(incarnation);
        assertThat(retained.path("_creationKind").asText()).isEqualTo("EXTERNAL");
        assertThat(retained.path("_metadataSourceGeneration").asLong()).isEqualTo(1L);
        assertThat(retained.path("_ownerGeneration").asLong()).isEqualTo(1L);
        assertThat(catalog.streamExists(stream).join()).isFalse();
        LogMetadata tombstone = metadata(metadata);
        assertThat(tombstone.deleted()).isTrue();
        assertThat(tombstone.registrationOwnerGeneration()).isEqualTo(1L);
        verify(logStorage).deleteLog(LogId.of(41L));
        assertThat(mappingDeletes).containsExactly(logName + ":41");
    }

    @Test
    void allocationFencedByPermanentDeletionCleansMetadataAndItsOwnMapping()
            throws Exception {
        int partition = 0;
        String logName = paths.compactedReaderName(stream, partition);
        String configPath = paths.streamConfigPath(stream);
        String metadataPath = paths.partitionMetadataPath(stream, partition);
        String incarnation = "incarnation-raced-delete";
        mockVersionedRecord(
            configPath,
            activeExternalConfigBytes(1, incarnation, "registration-owner"));
        VersionedRecord metadata = mockVersionedRecord(
            metadataPath,
            metadataBytes(metadataPath, 90L, incarnation, "registration-owner", 1L, false));
        CompletableFuture<Long> allocation = new CompletableFuture<>();
        AtomicReference<Long> mapping = new AtomicReference<>(90L);
        List<String> mappingDeletes = new ArrayList<>();
        IndexedStreamCatalog catalog = catalog(
            ignored -> CompletableFuture.completedFuture(91L),
            ignored -> allocation.thenApply(streamId -> {
                mapping.set(streamId);
                return streamId;
            }),
            key -> mapping.get() == null
                ? CompletableFuture.failedFuture(new NoSuchKeyException(key))
                : CompletableFuture.completedFuture(mapping.get()),
            (key, streamId) -> {
                mappingDeletes.add(key + ":" + streamId);
                clearExpectedMapping(mapping, streamId);
                return CompletableFuture.completedFuture(null);
            });

        CompletableFuture<Log> opened =
            catalog.openExternalPartition(stream, partition, Map.of());
        assertThat(opened).isNotDone();
        catalog.permanentlyDeleteExternalStream(stream).join();
        allocation.complete(91L);

        assertThatThrownBy(opened::join)
            .isInstanceOf(CompletionException.class)
            .hasCauseInstanceOf(NoSuchStreamException.class);
        LogMetadata tombstone = metadata(metadata);
        assertThat(tombstone.deleted()).isTrue();
        assertThat(tombstone.streamId()).isEqualTo(90L);
        assertThat(tombstone.registrationOwnerGeneration()).isEqualTo(1L);
        verify(logStorage).deleteLog(LogId.of(90L));
        verify(logStorage).deleteLog(LogId.of(91L));
        assertThat(mappingDeletes).containsExactly(logName + ":91");
        verify(readerFactory, never()).open(anyString());
    }

    @Test
    void deletionFenceAfterIdGenerationPreservesPotentiallySharedAllocation() throws Exception {
        int partition = 2;
        String logName = paths.compactedReaderName(stream, partition);
        String configPath = paths.streamConfigPath(stream);
        String metadataPath = paths.partitionMetadataPath(stream, partition);
        VersionedRecord config = mockVersionedRecord(
            configPath, activeLegacyConfigBytes(partition + 1));
        VersionedRecord metadata = mockVersionedRecord(metadataPath, null);
        CompletableFuture<Long> allocation = new CompletableFuture<>();
        @SuppressWarnings("unchecked")
        BiFunction<String, Long, CompletableFuture<Void>> mappingDeleter =
            mock(BiFunction.class);
        IndexedStreamCatalog catalog = catalog(
            ignored -> CompletableFuture.completedFuture(41L),
            ignored -> allocation,
            ignored -> CompletableFuture.completedFuture(41L), mappingDeleter, false);

        CompletableFuture<Log> opened =
            catalog.openExternalPartition(stream, partition, Map.of());
        assertThat(opened).isNotDone();
        catalog.permanentlyDeleteExternalStream(stream).join();
        allocation.complete(41L);

        assertThatThrownBy(opened::join)
            .isInstanceOf(CompletionException.class)
            .hasCauseInstanceOf(NoSuchStreamException.class);
        verify(logStorage).deleteLog(LogId.of(41L));
        verify(mappingDeleter).apply(logName, 41L);
        LogMetadata tombstone = metadata(metadata);
        assertThat(tombstone.deleted()).isTrue();
        assertThat(tombstone.streamId()).isEqualTo(41L);
        assertThat(tombstone.retiredStreamIds()).isEmpty();
        assertThat(tombstone.purgeableRetiredStreamIds()).isEmpty();
        assertThat(json(config).path("_provisioningState").asText())
            .isEqualTo("PERMANENTLY_DELETED");
    }

    @Test
    void rejectedOpenDoesNotDeleteReusedPartitionAfterUnregister() throws Exception {
        int partition = 0;
        long existingStreamId = 71L;
        String logName = paths.compactedReaderName(stream, partition);
        String configPath = paths.streamConfigPath(stream);
        String metadataPath = paths.partitionMetadataPath(stream, partition);
        String incarnation = "incarnation-reused-unregister";
        VersionedRecord config = mockVersionedRecord(
            configPath, activeExternalConfigBytes(
                1, incarnation, "registration-owner"));
        VersionedRecord metadata = mockVersionedRecord(
            metadataPath, metadataBytes(
                metadataPath, existingStreamId, incarnation,
                "registration-owner", 1L, false));
        CompletableFuture<Long> allocation = new CompletableFuture<>();
        @SuppressWarnings("unchecked")
        BiFunction<String, Long, CompletableFuture<Void>> mappingDeleter =
            mock(BiFunction.class);
        IndexedStreamCatalog catalog = catalog(
            ignored -> CompletableFuture.completedFuture(existingStreamId),
            ignored -> allocation,
            ignored -> CompletableFuture.completedFuture(existingStreamId),
            mappingDeleter, true);

        CompletableFuture<Log> opened =
            catalog.openExternalPartition(stream, partition, Map.of());
        catalog.unregisterExternalStream(stream).join();
        allocation.complete(existingStreamId);

        assertThatThrownBy(opened::join)
            .isInstanceOf(CompletionException.class)
            .hasCauseInstanceOf(NoSuchStreamException.class);
        assertThat(json(config).path("_provisioningState").asText())
            .isEqualTo("UNREGISTERED");
        assertThat(metadata(metadata).deleted()).isFalse();
        assertThat(metadata(metadata).streamId()).isEqualTo(existingStreamId);
        verify(logStorage, never()).deleteLog(LogId.of(existingStreamId));
        verify(mappingDeleter, never()).apply(logName, existingStreamId);
    }

    @Test
    void rejectedCreatedAllocationAfterUnregisterPreservesUnpublishedResources()
            throws Exception {
        int partition = 0;
        long allocatedStreamId = 75L;
        String logName = paths.compactedReaderName(stream, partition);
        String configPath = paths.streamConfigPath(stream);
        String metadataPath = paths.partitionMetadataPath(stream, partition);
        VersionedRecord config = mockVersionedRecord(
            configPath,
            activeExternalConfigBytes(
                1, "incarnation-unregistered-allocation", "registration-owner"));
        VersionedRecord metadata = mockVersionedRecord(metadataPath, null);
        CompletableFuture<Long> allocation = new CompletableFuture<>();
        @SuppressWarnings("unchecked")
        BiFunction<String, Long, CompletableFuture<Void>> mappingDeleter =
            mock(BiFunction.class);
        IndexedStreamCatalog catalog = catalog(
            ignored -> CompletableFuture.completedFuture(allocatedStreamId),
            ignored -> allocation,
            ignored -> CompletableFuture.completedFuture(allocatedStreamId),
            mappingDeleter, true);

        CompletableFuture<Log> opened =
            catalog.openExternalPartition(stream, partition, Map.of());
        catalog.unregisterExternalStream(stream).join();
        allocation.complete(allocatedStreamId);

        assertThatThrownBy(opened::join)
            .isInstanceOf(CompletionException.class)
            .hasCauseInstanceOf(NoSuchStreamException.class);
        assertThat(json(config).path("_provisioningState").asText())
            .isEqualTo("UNREGISTERED");
        assertThat(metadata.currentResult()).isNull();
        verify(logStorage, never()).deleteLog(LogId.of(allocatedStreamId));
        verify(mappingDeleter, never()).apply(logName, allocatedStreamId);
    }

    @Test
    void rejectedOpenCleansAllocationThatCompletesAfterDrop() throws Exception {
        int partition = 0;
        long allocatedStreamId = 72L;
        String logName = paths.compactedReaderName(stream, partition);
        String configPath = paths.streamConfigPath(stream);
        String metadataPath = paths.partitionMetadataPath(stream, partition);
        VersionedRecord config = mockVersionedRecord(configPath, null);
        VersionedRecord metadata = mockVersionedRecord(metadataPath, null);
        CompletableFuture<StreamIdAllocation> allocation = new CompletableFuture<>();
        FencedMappingHarness mappings = new FencedMappingHarness(
            logName, allocatedStreamId + 1);
        when(mappings.storageApi().allocateStreamId(
                anyString(), any(StreamIdMappingOwner.class), any()))
            .thenAnswer(invocation -> {
                mappings.installActive(allocatedStreamId,
                    invocation.getArgument(1, StreamIdMappingOwner.class));
                return allocation;
            });
        IndexedStreamCatalog catalog = fencedCatalog(mappings.storageApi());

        CompletableFuture<Log> opened =
            catalog.openExternalPartition(stream, partition, Map.of());
        assertThat(opened).isNotDone();
        assertThat(mappings.activeStreamId()).contains(allocatedStreamId);

        assertThat(catalog.dropStream(stream, true).join()).isTrue();
        assertThat(json(config).path("_provisioningState").asText())
            .isEqualTo("DROPPED");
        assertThat(metadata(metadata).streamId()).isEqualTo(allocatedStreamId);

        allocation.complete(new StreamIdAllocation(allocatedStreamId, true));

        assertThatThrownBy(opened::join)
            .isInstanceOf(CompletionException.class)
            .hasCauseInstanceOf(NoSuchStreamException.class);
        assertThat(json(config).path("_provisioningState").asText())
            .isEqualTo("DROPPED");
        assertThat(metadata(metadata).deleted()).isTrue();
        assertThat(metadata(metadata).streamId()).isEqualTo(allocatedStreamId);
        assertThat(metadata(metadata).retiredStreamIds()).isEmpty();
        assertThat(metadata(metadata).purgeableRetiredStreamIds()).isEmpty();
        assertThat(mappings.activeStreamId()).isEmpty();
        verify(logStorage, atLeastOnce()).deleteLog(LogId.of(allocatedStreamId));
        verify(readerFactory, never()).open(anyString());
    }

    @Test
    void rejectedOpenCompensatesInvalidatedReusedAllocationAfterDrop() throws Exception {
        int partition = 0;
        long allocatedStreamId = 78L;
        String logName = paths.compactedReaderName(stream, partition);
        String configPath = paths.streamConfigPath(stream);
        String metadataPath = paths.partitionMetadataPath(stream, partition);
        VersionedRecord config = mockVersionedRecord(configPath, null);
        VersionedRecord metadata = mockVersionedRecord(metadataPath, null);
        CompletableFuture<StreamIdAllocation> allocation = new CompletableFuture<>();
        RuntimeException validationFailure = new RuntimeException("mapping validation failed");
        KeyedAllocationInvalidatedException invalidated =
            new KeyedAllocationInvalidatedException(
                new StreamIdAllocation(allocatedStreamId, false), validationFailure);
        FencedMappingHarness mappings = new FencedMappingHarness(
            logName, allocatedStreamId + 1);
        when(mappings.storageApi().allocateStreamId(
                anyString(), any(StreamIdMappingOwner.class), any()))
            .thenAnswer(invocation -> {
                mappings.installActive(allocatedStreamId,
                    invocation.getArgument(1, StreamIdMappingOwner.class));
                return allocation;
            });
        IndexedStreamCatalog catalog = fencedCatalog(mappings.storageApi());

        CompletableFuture<Log> opened =
            catalog.openExternalPartition(stream, partition, Map.of());
        assertThat(opened).isNotDone();
        assertThat(mappings.activeStreamId()).contains(allocatedStreamId);

        assertThat(catalog.dropStream(stream, true).join()).isTrue();
        allocation.completeExceptionally(invalidated);

        assertThatThrownBy(opened::join)
            .isInstanceOf(CompletionException.class)
            .hasCause(invalidated);
        assertThat(invalidated.getCause()).isSameAs(validationFailure);
        assertThat(json(config).path("_provisioningState").asText())
            .isEqualTo("DROPPED");
        assertThat(metadata(metadata).streamId()).isEqualTo(allocatedStreamId);
        assertThat(metadata(metadata).retiredStreamIds()).isEmpty();
        assertThat(metadata(metadata).purgeableRetiredStreamIds()).isEmpty();
        assertThat(mappings.activeStreamId()).isEmpty();
        verify(logStorage, atLeastOnce()).deleteLog(LogId.of(allocatedStreamId));
        verify(readerFactory, never()).open(anyString());
    }

    @Test
    void rejectedOpenDoesNotCompensateOrdinaryAllocatorFailureAfterDrop() throws Exception {
        int partition = 0;
        long unprovenStreamId = 79L;
        String logName = paths.compactedReaderName(stream, partition);
        String configPath = paths.streamConfigPath(stream);
        String metadataPath = paths.partitionMetadataPath(stream, partition);
        VersionedRecord config = mockVersionedRecord(configPath, null);
        VersionedRecord metadata = mockVersionedRecord(metadataPath, null);
        CompletableFuture<Long> allocation = new CompletableFuture<>();
        RuntimeException allocatorFailure = new RuntimeException("allocator failed");
        @SuppressWarnings("unchecked")
        BiFunction<String, Long, CompletableFuture<Void>> mappingDeleter =
            mock(BiFunction.class);
        IndexedStreamCatalog catalog = catalog(
            ignored -> CompletableFuture.completedFuture(unprovenStreamId),
            ignored -> allocation,
            ignored -> CompletableFuture.failedFuture(new NoSuchKeyException(logName)),
            mappingDeleter, false);

        CompletableFuture<Log> opened =
            catalog.openExternalPartition(stream, partition, Map.of());
        assertThat(opened).isNotDone();

        assertThat(catalog.dropStream(stream, true).join()).isTrue();
        allocation.completeExceptionally(allocatorFailure);

        assertThatThrownBy(opened::join)
            .isInstanceOf(CompletionException.class)
            .hasCause(allocatorFailure);
        assertThat(json(config).path("_provisioningState").asText())
            .isEqualTo("DROPPED");
        assertThat(metadata(metadata).streamId()).isEqualTo(-1L);
        assertThat(metadata(metadata).retiredStreamIds()).isEmpty();
        verify(logStorage, never()).deleteLog(LogId.of(unprovenStreamId));
        verify(mappingDeleter, never()).apply(logName, unprovenStreamId);
        verify(readerFactory, never()).open(anyString());
    }

    @Test
    void rejectedOpenRetiresDistinctAllocationWithoutReplacingPublishedTombstone()
            throws Exception {
        int partition = 0;
        long publishedStreamId = 76L;
        long staleStreamId = 77L;
        String logName = paths.compactedReaderName(stream, partition);
        String configPath = paths.streamConfigPath(stream);
        String metadataPath = paths.partitionMetadataPath(stream, partition);
        String incarnation = "incarnation-distinct-stale-allocation";
        VersionedRecord config = mockVersionedRecord(
            configPath, activeExternalConfigBytes(
                1, incarnation, "registration-owner"));
        VersionedRecord metadata = mockVersionedRecord(
            metadataPath, metadataBytes(
                metadataPath, publishedStreamId, incarnation,
                "registration-owner", 1L, false));
        CompletableFuture<Long> staleAllocation = new CompletableFuture<>();
        AtomicReference<Long> mapping = new AtomicReference<>(publishedStreamId);
        List<Long> mappingDeletes = new ArrayList<>();
        IndexedStreamCatalog catalog = catalog(
            ignored -> CompletableFuture.completedFuture(staleStreamId),
            ignored -> staleAllocation,
            key -> {
                Long current = mapping.get();
                return current == null
                    ? CompletableFuture.failedFuture(new NoSuchKeyException(key))
                    : CompletableFuture.completedFuture(current);
            },
            (key, expectedStreamId) -> {
                assertThat(key).isEqualTo(logName);
                mappingDeletes.add(expectedStreamId);
                Long current = mapping.get();
                if (current != null
                        && current.longValue() == expectedStreamId.longValue()) {
                    mapping.compareAndSet(current, null);
                }
                return CompletableFuture.completedFuture(null);
            });

        CompletableFuture<Log> opened =
            catalog.openExternalPartition(stream, partition, Map.of());
        assertThat(opened).isNotDone();

        assertThat(catalog.dropStream(stream, false).join()).isTrue();
        LogMetadata initialTombstone = metadata(metadata);
        assertThat(initialTombstone.streamId()).isEqualTo(publishedStreamId);
        assertThat(initialTombstone.retiredStreamIds()).isEmpty();
        verify(logStorage, never()).deleteLog(LogId.of(publishedStreamId));

        mapping.set(staleStreamId);
        staleAllocation.complete(staleStreamId);

        assertThatThrownBy(opened::join)
            .isInstanceOf(CompletionException.class)
            .hasCauseInstanceOf(NoSuchStreamException.class);
        LogMetadata completedTombstone = metadata(metadata);
        assertThat(completedTombstone.streamId()).isEqualTo(publishedStreamId);
        assertThat(completedTombstone.retiredStreamIds()).isEmpty();
        verify(logStorage, never()).deleteLog(LogId.of(publishedStreamId));
        verify(logStorage).deleteLog(LogId.of(staleStreamId));
        assertThat(mapping).hasValue(null);
        assertThat(mappingDeletes)
            .contains(publishedStreamId, staleStreamId)
            .containsOnly(publishedStreamId, staleStreamId);
        assertThat(json(config).path("_provisioningState").asText())
            .isEqualTo("DROPPED");
    }

    @Test
    void reopenRecoversStaleAllocationWithoutPurgingNonPurgePrimary() throws Exception {
        int partition = 0;
        long publishedStreamId = 176L;
        long staleStreamId = 177L;
        long replacementStreamId = 178L;
        String logName = paths.compactedReaderName(stream, partition);
        String configPath = paths.streamConfigPath(stream);
        String metadataPath = paths.partitionMetadataPath(stream, partition);
        String incarnation = "incarnation-non-purge-recovery";
        VersionedRecord config = mockVersionedRecord(
            configPath, activeExternalConfigBytes(
                1, incarnation, "registration-owner"));
        VersionedRecord metadata = mockVersionedRecord(
            metadataPath, metadataBytes(
                metadataPath, publishedStreamId, incarnation,
                "registration-owner", 1L, false));
        CompletableFuture<Long> staleAllocation = new CompletableFuture<>();
        AtomicReference<Long> mapping = new AtomicReference<>(publishedStreamId);
        AtomicInteger staleLogDeletes = new AtomicInteger();
        RuntimeException interruptedCleanup = new RuntimeException("cleanup interrupted");
        when(logStorage.deleteLog(LogId.of(staleStreamId))).thenAnswer(ignored ->
            staleLogDeletes.incrementAndGet() == 1
                ? CompletableFuture.failedFuture(interruptedCleanup)
                : CompletableFuture.completedFuture(null));
        List<Long> mappingDeletes = new ArrayList<>();
        Function<String, CompletableFuture<Long>> mappingLookup = key -> {
            Long current = mapping.get();
            return current == null
                ? CompletableFuture.failedFuture(new NoSuchKeyException(key))
                : CompletableFuture.completedFuture(current);
        };
        BiFunction<String, Long, CompletableFuture<Void>> mappingDeleter =
            (key, expectedStreamId) -> {
                assertThat(key).isEqualTo(logName);
                mappingDeletes.add(expectedStreamId);
                Long current = mapping.get();
                if (current != null
                        && current.longValue() == expectedStreamId.longValue()) {
                    mapping.compareAndSet(current, null);
                }
                return CompletableFuture.completedFuture(null);
            };
        IndexedStreamCatalog staleCatalog = catalog(
            ignored -> CompletableFuture.completedFuture(staleStreamId),
            ignored -> staleAllocation, mappingLookup, mappingDeleter);

        CompletableFuture<Log> staleOpen =
            staleCatalog.openExternalPartition(stream, partition, Map.of());
        assertThat(staleCatalog.dropStream(stream, false).join()).isTrue();
        mapping.set(staleStreamId);
        staleAllocation.complete(staleStreamId);

        assertThatThrownBy(staleOpen::join)
            .isInstanceOf(CompletionException.class)
            .hasCauseInstanceOf(NoSuchStreamException.class);
        LogMetadata interruptedTombstone = metadata(metadata);
        assertThat(interruptedTombstone.retiredStreamIds())
            .containsExactly(publishedStreamId, staleStreamId);
        assertThat(interruptedTombstone.purgeableRetiredStreamIds())
            .containsExactly(staleStreamId);
        assertThat(mapping).hasValue(null);
        verify(logStorage, never()).deleteLog(LogId.of(publishedStreamId));

        CompactedObjectReader reader = mock(CompactedObjectReader.class);
        Log replacementLog = mock(Log.class);
        when(readerFactory.open(logName)).thenReturn(reader);
        when(logFactory.create(logName, LogId.of(replacementStreamId), reader))
            .thenReturn(replacementLog);
        IndexedStreamCatalog replacementCatalog = catalog(
            ignored -> CompletableFuture.completedFuture(replacementStreamId),
            ignored -> {
                assertThat(mapping.compareAndSet(null, replacementStreamId)).isTrue();
                return CompletableFuture.completedFuture(replacementStreamId);
            }, mappingLookup, mappingDeleter);

        assertThat(replacementCatalog.openExternalPartition(
            stream, partition, Map.of()).join()).isSameAs(replacementLog);

        LogMetadata replacement = metadata(metadata);
        assertThat(replacement.streamId()).isEqualTo(replacementStreamId);
        assertThat(replacement.retiredStreamIds()).isEmpty();
        assertThat(replacement.purgeableRetiredStreamIds()).isEmpty();
        verify(logStorage, never()).deleteLog(LogId.of(publishedStreamId));
        verify(logStorage, times(2)).deleteLog(LogId.of(staleStreamId));
        assertThat(mappingDeletes).containsExactly(publishedStreamId, staleStreamId);
    }

    @Test
    void nonPurgingDropDoesNotUpgradePreviouslyRetiredMappingToPurgeable()
            throws Exception {
        int partition = 0;
        long primaryStreamId = 181L;
        long retainedStreamId = 180L;
        String logName = paths.compactedReaderName(stream, partition);
        String configPath = paths.streamConfigPath(stream);
        String metadataPath = paths.partitionMetadataPath(stream, partition);
        String incarnation = "incarnation-retained-mapping";
        String owner = "registration-owner";
        mockVersionedRecord(
            configPath, activeExternalConfigBytes(1, incarnation, owner));
        VersionedRecord metadata = mockVersionedRecord(
            metadataPath, metadataBytes(
                metadataPath, primaryStreamId, incarnation, owner, 1L, false,
                Set.of(retainedStreamId), Set.of(),
                Set.of(new LogMetadata.RetiredStreamMapping(retainedStreamId, logName)),
                Set.of(logName)));
        AtomicReference<Long> mapping = new AtomicReference<>(retainedStreamId);
        List<Long> mappingDeletes = new ArrayList<>();
        IndexedStreamCatalog catalog = catalog(
            ignored -> CompletableFuture.completedFuture(primaryStreamId),
            ignored -> CompletableFuture.completedFuture(primaryStreamId),
            key -> mapping.get() == null
                ? CompletableFuture.failedFuture(new NoSuchKeyException(key))
                : CompletableFuture.completedFuture(mapping.get()),
            (key, expectedStreamId) -> {
                assertThat(key).isEqualTo(logName);
                mappingDeletes.add(expectedStreamId);
                clearExpectedMapping(mapping, expectedStreamId);
                return CompletableFuture.completedFuture(null);
            });

        assertThat(catalog.dropStream(stream, false).join()).isTrue();

        LogMetadata dropped = metadata(metadata);
        assertThat(dropped.retiredStreamIds()).isEmpty();
        assertThat(dropped.purgeableRetiredStreamIds()).isEmpty();
        assertThat(mapping).hasValue(null);
        assertThat(mappingDeletes).containsExactly(retainedStreamId);
        verify(logStorage, never()).deleteLog(LogId.of(primaryStreamId));
        verify(logStorage, never()).deleteLog(LogId.of(retainedStreamId));
    }

    @Test
    void purgingDropDeletesPreviouslyRetainedAndCurrentStreamIds() throws Exception {
        int partition = 0;
        long retainedStreamId = 182L;
        long primaryStreamId = 183L;
        String logName = paths.compactedReaderName(stream, partition);
        String configPath = paths.streamConfigPath(stream);
        String metadataPath = paths.partitionMetadataPath(stream, partition);
        String incarnation = "incarnation-purge-retained";
        String owner = "registration-owner";
        mockVersionedRecord(
            configPath, activeExternalConfigBytes(1, incarnation, owner));
        VersionedRecord metadata = mockVersionedRecord(
            metadataPath, metadataBytes(
                metadataPath, primaryStreamId, incarnation, owner, 1L, false,
                Set.of(retainedStreamId), Set.of(),
                Set.of(new LogMetadata.RetiredStreamMapping(retainedStreamId, logName)),
                Set.of(logName)));
        AtomicReference<Long> mapping = new AtomicReference<>(primaryStreamId);
        IndexedStreamCatalog catalog = catalog(
            ignored -> CompletableFuture.completedFuture(primaryStreamId),
            ignored -> CompletableFuture.completedFuture(primaryStreamId),
            key -> mapping.get() == null
                ? CompletableFuture.failedFuture(new NoSuchKeyException(key))
                : CompletableFuture.completedFuture(mapping.get()),
            (key, expectedStreamId) -> {
                assertThat(key).isEqualTo(logName);
                clearExpectedMapping(mapping, expectedStreamId);
                return CompletableFuture.completedFuture(null);
            });

        assertThat(catalog.dropStream(stream, true).join()).isTrue();

        LogMetadata dropped = metadata(metadata);
        assertThat(dropped.retiredStreamIds()).isEmpty();
        assertThat(dropped.purgeableRetiredStreamIds()).isEmpty();
        assertThat(mapping).hasValue(null);
        verify(logStorage).deleteLog(LogId.of(retainedStreamId));
        verify(logStorage).deleteLog(LogId.of(primaryStreamId));
    }

    @Test
    void rejectedOpenPreservesPublishedLogAfterNonPurgingDrop() throws Exception {
        int partition = 0;
        long reusedStreamId = 73L;
        String logName = paths.compactedReaderName(stream, partition);
        String configPath = paths.streamConfigPath(stream);
        String metadataPath = paths.partitionMetadataPath(stream, partition);
        String incarnation = "incarnation-published-reuse";
        VersionedRecord config = mockVersionedRecord(
            configPath,
            activeExternalConfigBytes(1, incarnation, "registration-owner"));
        VersionedRecord metadata = mockVersionedRecord(metadataPath, null);
        CompletableFuture<Long> staleAllocation = new CompletableFuture<>();
        AtomicInteger allocationCalls = new AtomicInteger();
        AtomicReference<Long> mapping = new AtomicReference<>();
        List<Long> mappingDeletes = new ArrayList<>();
        CompactedObjectReader reader = mock(CompactedObjectReader.class);
        Log log = mock(Log.class);
        when(readerFactory.open(logName)).thenReturn(reader);
        when(logFactory.create(logName, LogId.of(reusedStreamId), reader))
            .thenReturn(log);
        when(logStorage.deleteLog(LogId.of(reusedStreamId)))
            .thenReturn(CompletableFuture.completedFuture(null));
        IndexedStreamCatalog catalog = catalog(
            ignored -> CompletableFuture.completedFuture(reusedStreamId),
            key -> {
                assertThat(key).isEqualTo(logName);
                mapping.compareAndSet(null, reusedStreamId);
                return allocationCalls.getAndIncrement() == 0
                    ? staleAllocation
                    : CompletableFuture.completedFuture(reusedStreamId);
            },
            key -> {
                assertThat(key).isEqualTo(logName);
                Long current = mapping.get();
                return current == null
                    ? CompletableFuture.failedFuture(new NoSuchKeyException(key))
                    : CompletableFuture.completedFuture(current);
            },
            (key, expectedStreamId) -> {
                assertThat(key).isEqualTo(logName);
                mappingDeletes.add(expectedStreamId);
                Long current = mapping.get();
                if (current != null
                        && current.longValue() == expectedStreamId.longValue()) {
                    mapping.compareAndSet(current, null);
                }
                return CompletableFuture.completedFuture(null);
            });

        CompletableFuture<Log> staleOpen =
            catalog.openExternalPartition(stream, partition, Map.of());
        assertThat(staleOpen).isNotDone();
        assertThat(catalog.openExternalPartition(stream, partition, Map.of()).join())
            .isSameAs(log);
        assertThat(metadata(metadata).streamId()).isEqualTo(reusedStreamId);
        assertThat(metadata(metadata).deleted()).isFalse();

        assertThat(catalog.dropStream(stream, false).join()).isTrue();
        assertThat(json(config).path("_provisioningState").asText())
            .isEqualTo("DROPPED");
        assertThat(metadata(metadata).streamId()).isEqualTo(reusedStreamId);
        assertThat(metadata(metadata).deleted()).isTrue();
        verify(logStorage, never()).deleteLog(LogId.of(reusedStreamId));

        staleAllocation.complete(reusedStreamId);

        assertThatThrownBy(staleOpen::join)
            .isInstanceOf(CompletionException.class)
            .hasCauseInstanceOf(NoSuchStreamException.class);
        assertThat(mapping.get()).isNull();
        assertThat(mappingDeletes).containsOnly(reusedStreamId);
        verify(logStorage, never()).deleteLog(LogId.of(reusedStreamId));
    }

    @Test
    void rejectedOpenCannotDeleteLogPublishedAfterDropSnapshot() throws Exception {
        int partition = 0;
        long staleStreamId = 74L;
        long replacementStreamId = 75L;
        String logName = paths.compactedReaderName(stream, partition);
        String configPath = paths.streamConfigPath(stream);
        String metadataPath = paths.partitionMetadataPath(stream, partition);
        VersionedRecord config = mockVersionedRecord(
            configPath,
            activeExternalConfigBytes(
                1, "incarnation-drop-snapshot", "registration-owner"));
        VersionedRecord metadata = mockVersionedRecord(metadataPath, null);
        CompletableFuture<Long> staleAllocation = new CompletableFuture<>();
        AtomicInteger allocationCalls = new AtomicInteger();
        AtomicReference<Long> mapping = new AtomicReference<>();
        List<Long> mappingDeletes = new ArrayList<>();
        CompactedObjectReader reader = mock(CompactedObjectReader.class);
        Log log = mock(Log.class);
        when(readerFactory.open(logName)).thenReturn(reader);
        when(logFactory.create(logName, LogId.of(replacementStreamId), reader))
            .thenReturn(log);
        IndexedStreamCatalog catalog = catalog(
            ignored -> CompletableFuture.completedFuture(staleStreamId),
            key -> {
                if (allocationCalls.getAndIncrement() == 0) {
                    return staleAllocation.thenApply(streamId -> {
                        mapping.set(streamId);
                        return streamId;
                    });
                }
                assertThat(mapping.compareAndSet(null, replacementStreamId)).isTrue();
                return CompletableFuture.completedFuture(replacementStreamId);
            },
            key -> {
                assertThat(key).isEqualTo(logName);
                return mapping.get() == null
                    ? CompletableFuture.failedFuture(new NoSuchKeyException(key))
                    : CompletableFuture.completedFuture(mapping.get());
            },
            (key, expectedStreamId) -> {
                mappingDeletes.add(expectedStreamId);
                clearExpectedMapping(mapping, expectedStreamId);
                return CompletableFuture.completedFuture(null);
            });

        CompletableFuture<Log> staleOpen =
            catalog.openExternalPartition(stream, partition, Map.of());
        assertThat(staleOpen).isNotDone();
        assertThat(catalog.dropStream(stream, false).join()).isTrue();
        assertThat(mapping).hasValue(null);
        assertThat(metadata(metadata).streamId()).isEqualTo(-1L);

        CompletableFuture<GetResult> blockedClaimRead = new CompletableFuture<>();
        AtomicReference<GetResult> capturedClaimRead = new AtomicReference<>();
        AtomicBoolean blockNextMetadataRead = new AtomicBoolean(true);
        metadata.interceptReads(current -> {
            if (blockNextMetadataRead.compareAndSet(true, false)) {
                capturedClaimRead.set(current);
                return blockedClaimRead;
            }
            return CompletableFuture.completedFuture(current);
        });
        staleAllocation.complete(staleStreamId);
        assertThat(capturedClaimRead.get()).isNotNull();
        assertThat(staleOpen).isNotDone();

        assertThat(catalog.dropStream(stream, false).join()).isFalse();
        assertThat(mapping).hasValue(null);

        assertThat(catalog.openExternalPartition(stream, partition, Map.of()).join())
            .isSameAs(log);
        assertThat(metadata(metadata).streamId()).isEqualTo(replacementStreamId);
        assertThat(metadata(metadata).deleted()).isFalse();

        blockedClaimRead.complete(capturedClaimRead.get());

        assertThatThrownBy(staleOpen::join)
            .isInstanceOf(CompletionException.class)
            .hasCauseInstanceOf(NoSuchStreamException.class);
        assertThat(mapping).hasValue(replacementStreamId);
        assertThat(mappingDeletes).isNotEmpty().containsOnly(staleStreamId);
        assertThat(json(config).path("_provisioningState").asText("ACTIVE"))
            .isEqualTo("ACTIVE");
        verify(logStorage, never()).deleteLog(LogId.of(replacementStreamId));
    }

    @Test
    void metadataWriteIsRetainedWhenActiveConfigVersionChangesAfterWrite() throws Exception {
        int partition = 4;
        String logName = paths.compactedReaderName(stream, partition);
        String configPath = paths.streamConfigPath(stream);
        String metadataPath = paths.partitionMetadataPath(stream, partition);
        VersionedRecord config = mockVersionedRecord(
            configPath, activeLegacyConfigBytes(partition + 1));
        VersionedRecord metadata = mockVersionedRecord(metadataPath, null);
        AtomicBoolean invalidateRegistration = new AtomicBoolean();
        metadata.afterSuccessfulPut(ignored -> {
            if (!invalidateRegistration.compareAndSet(false, true)) {
                return;
            }
            GetResult current = config.currentResult();
            config.put(config.currentValue(), Set.of(PutOption.IfVersionIdEquals(
                current.version().versionId()))).join();
        });
        @SuppressWarnings("unchecked")
        BiFunction<String, Long, CompletableFuture<Void>> mappingDeleter =
            mock(BiFunction.class);
        IndexedStreamCatalog catalog = catalog(
            ignored -> CompletableFuture.completedFuture(51L),
            ignored -> CompletableFuture.completedFuture(51L),
            ignored -> CompletableFuture.completedFuture(51L), mappingDeleter);

        assertThatThrownBy(() ->
                catalog.openExternalPartition(stream, partition, Map.of()).join())
            .isInstanceOf(CompletionException.class)
            .hasCauseInstanceOf(NoSuchStreamException.class);

        assertThat(metadata(metadata).streamId()).isEqualTo(51L);
        assertThat(metadata(metadata).deleted()).isFalse();
        verify(logStorage, never()).deleteLog(LogId.of(51L));
        verify(mappingDeleter, never()).apply(logName, 51L);
        verify(readerFactory, never()).open(anyString());
    }

    @Test
    void permanentDeletionDoesNotLetActiveOpenDeleteAllocationReusedBySuccessfulOpen()
            throws Exception {
        int partition = 5;
        String logName = paths.compactedReaderName(stream, partition);
        String configPath = paths.streamConfigPath(stream);
        String metadataPath = paths.partitionMetadataPath(stream, partition);
        CompletableFuture<GetResult> firstOpenPostWriteVerification = new CompletableFuture<>();
        VersionedRecord config = mockVersionedRecord(
            configPath, activeLegacyConfigBytes(partition + 1));
        VersionedRecord metadata = mockVersionedRecord(metadataPath, null);
        AtomicBoolean blockNextConfigRead = new AtomicBoolean();
        metadata.afterSuccessfulPut(ignored -> blockNextConfigRead.set(true));
        config.interceptReads(current -> blockNextConfigRead.compareAndSet(true, false)
            ? firstOpenPostWriteVerification
            : CompletableFuture.completedFuture(current));
        AtomicInteger allocations = new AtomicInteger();
        @SuppressWarnings("unchecked")
        BiFunction<String, Long, CompletableFuture<Void>> mappingDeleter =
            mock(BiFunction.class);
        CompactedObjectReader reader = mock(CompactedObjectReader.class);
        Log log = mock(Log.class);
        when(readerFactory.open(logName)).thenReturn(reader);
        when(logFactory.create(logName, LogId.of(61L), reader)).thenReturn(log);
        IndexedStreamCatalog catalog = catalog(
            ignored -> CompletableFuture.completedFuture(61L),
            ignored -> {
                allocations.incrementAndGet();
                return CompletableFuture.completedFuture(61L);
            },
            ignored -> CompletableFuture.completedFuture(61L), mappingDeleter);

        CompletableFuture<Log> invalidated =
            catalog.openExternalPartition(stream, partition, Map.of());
        assertThat(invalidated).isNotDone();

        assertThat(catalog.openExternalPartition(stream, partition, Map.of()).get())
            .isSameAs(log);
        firstOpenPostWriteVerification.complete(
            permanentDeletion(configPath, config.currentResult().version()));

        assertThatThrownBy(invalidated::join)
            .isInstanceOf(CompletionException.class)
            .hasCauseInstanceOf(NoSuchStreamException.class);
        assertThat(allocations).hasValue(2);
        assertThat(metadata.successfulPuts()).isEqualTo(1);
        assertThat(metadata(metadata).deleted()).isFalse();
        verify(logStorage, never()).deleteLog(LogId.of(61L));
        verify(mappingDeleter, never()).apply(logName, 61L);
        verify(oxiaClient, never()).delete(eq(metadataPath), any());
    }

    @Test
    void metadataConflictDoesNotOpenLogWithDifferentId() throws Exception {
        int partition = 3;
        String logName = paths.compactedReaderName(stream, partition);
        String configPath = paths.streamConfigPath(stream);
        String metadataPath = paths.partitionMetadataPath(stream, partition);
        mockVersionedRecord(configPath, activeLegacyConfigBytes(4));
        VersionedRecord metadata = mockVersionedRecord(
            metadataPath, metadataBytes(metadataPath, 42L, null, null, false));
        AtomicReference<Long> mapping = new AtomicReference<>(41L);
        IndexedStreamCatalog catalog = catalog(
            ignored -> CompletableFuture.completedFuture(41L),
            ignored -> CompletableFuture.completedFuture(41L),
            key -> mapping.get() == null
                ? CompletableFuture.failedFuture(new NoSuchKeyException(key))
                : CompletableFuture.completedFuture(mapping.get()),
            (key, expectedStreamId) -> {
                clearExpectedMapping(mapping, expectedStreamId);
                return CompletableFuture.completedFuture(null);
            });

        assertThatThrownBy(() -> catalog.openExternalPartition(stream, partition, Map.of()).join())
            .isInstanceOf(CompletionException.class)
            .hasCauseInstanceOf(PartitionLifecycleFencedException.class);

        LogMetadata current = metadata(metadata);
        assertThat(current.streamId()).isEqualTo(42L);
        assertThat(current.deleted()).isFalse();
        assertThat(current.retiredStreamIds()).isEmpty();
        assertThat(mapping).hasValue(41L);
        verify(logStorage, never()).deleteLog(LogId.of(41L));
        verify(readerFactory, never()).open(logName);
        verify(logFactory, never()).create(anyString(), any(), any());
    }

    @Test
    void compatibleExternalProvisioningAdoptsDurableClaimAndFinalizesIt() throws Exception {
        int partition = 0;
        String logName = paths.compactedReaderName(stream, partition);
        String configPath = paths.streamConfigPath(stream);
        String metadataPath = paths.partitionMetadataPath(stream, partition);
        Map<String, String> properties = Map.of("owner", "kafka");
        VersionedRecord config = mockVersionedRecord(configPath, null);
        VersionedRecord metadata = mockVersionedRecord(metadataPath, null);
        CompletableFuture<GetResult> stalePostWriteVerification = new CompletableFuture<>();
        AtomicBoolean blockNextConfigRead = new AtomicBoolean();
        metadata.afterSuccessfulPut(ignored -> {
            if (metadata.successfulPuts() == 1) {
                blockNextConfigRead.set(true);
            }
        });
        config.interceptReads(current -> blockNextConfigRead.compareAndSet(true, false)
            ? stalePostWriteVerification : CompletableFuture.completedFuture(current));
        AtomicInteger allocations = new AtomicInteger();
        CompactedObjectReader reader = mock(CompactedObjectReader.class);
        Log recovered = mock(Log.class);
        when(readerFactory.open(logName)).thenReturn(reader);
        when(logFactory.create(logName, LogId.of(101L), reader)).thenReturn(recovered);
        IndexedStreamCatalog catalog = catalog(
            ignored -> CompletableFuture.completedFuture(101L),
            ignored -> {
                allocations.incrementAndGet();
                return CompletableFuture.completedFuture(101L);
            },
            ignored -> CompletableFuture.completedFuture(101L),
            (ignored, streamId) -> CompletableFuture.completedFuture(null));

        CompletableFuture<Log> ownerA =
            catalog.openExternalPartition(stream, partition, properties);
        assertThat(ownerA).isNotDone();
        JsonNode configOwnedByA = json(config);
        LogMetadata metadataOwnedByA = metadata(metadata);
        assertThat(configOwnedByA.path("_provisioning").asBoolean()).isTrue();
        assertThat(metadataOwnedByA.streamId()).isEqualTo(101L);
        assertThat(metadataOwnedByA.registrationOwnerToken())
            .isEqualTo(configOwnedByA.path("_ownerToken").asText());
        assertThat(metadataOwnedByA.registrationOwnerGeneration())
            .isEqualTo(configOwnedByA.path("_ownerGeneration").asLong());

        catalog.registerExternalPartition(stream, partition, 101L, properties).join();

        JsonNode configOwnedByB = json(config);
        LogMetadata metadataOwnedByB = metadata(metadata);
        assertThat(configOwnedByB.path("_provisioning").asBoolean(false)).isFalse();
        assertThat(configOwnedByB.path("_incarnationId").asText())
            .isEqualTo(configOwnedByA.path("_incarnationId").asText());
        assertThat(configOwnedByB.path("_ownerToken").asText())
            .isEqualTo(configOwnedByA.path("_ownerToken").asText());
        assertThat(configOwnedByB.path("_ownerGeneration").asLong())
            .isEqualTo(configOwnedByA.path("_ownerGeneration").asLong());
        assertThat(metadataOwnedByB.streamId()).isEqualTo(101L);
        assertThat(metadataOwnedByB.registrationOwnerToken())
            .isEqualTo(configOwnedByB.path("_ownerToken").asText());
        assertThat(metadataOwnedByB.registrationOwnerGeneration())
            .isEqualTo(configOwnedByB.path("_ownerGeneration").asLong());
        assertThat(metadataOwnedByB.registrationOwnerGeneration())
            .isEqualTo(metadataOwnedByA.registrationOwnerGeneration());

        assertThat(catalog.openExternalPartition(stream, partition, properties).join())
            .isSameAs(recovered);
        assertThat(metadata.successfulPuts()).isEqualTo(1);

        stalePostWriteVerification.complete(config.currentResult());
        assertThat(ownerA.join()).isSameAs(recovered);
        assertThat(allocations).hasValue(2);
        assertThat(metadata(metadata).streamId()).isEqualTo(101L);
        verify(logStorage, never()).deleteLog(any());
        verify(oxiaClient, never()).delete(eq(metadataPath), any());
    }

    @Test
    void adoptedProvisioningRejectsStaleDifferentIdWrite() throws Exception {
        int partition = 0;
        String logName = paths.compactedReaderName(stream, partition);
        String configPath = paths.streamConfigPath(stream);
        String metadataPath = paths.partitionMetadataPath(stream, partition);
        Map<String, String> properties = Map.of("owner", "kafka");
        VersionedRecord config = mockVersionedRecord(configPath, null);
        VersionedRecord metadata = mockVersionedRecord(metadataPath, null);
        CompletableFuture<GetResult> staleMetadataRead = new CompletableFuture<>();
        AtomicInteger metadataReads = new AtomicInteger();
        metadata.interceptReads(current -> metadataReads.incrementAndGet() == 3
            ? staleMetadataRead : CompletableFuture.completedFuture(current));
        FencedMappingHarness mappings = new FencedMappingHarness(logName, 101L);
        CompactedObjectReader reader = mock(CompactedObjectReader.class);
        Log recovered = mock(Log.class);
        when(readerFactory.open(logName)).thenReturn(reader);
        when(logFactory.create(logName, LogId.of(101L), reader)).thenReturn(recovered);
        IndexedStreamCatalog catalog = fencedCatalog(mappings.storageApi());

        CompletableFuture<Log> ownerA =
            catalog.openExternalPartition(stream, partition, properties);
        assertThat(ownerA).isNotDone();
        JsonNode ownerAConfig = json(config);
        assertThat(ownerAConfig.path("_ownerGeneration").asLong()).isEqualTo(1L);
        assertThat(mappings.activeStreamId()).contains(101L);

        assertThatThrownBy(() -> catalog.registerExternalPartition(
                stream, partition, 202L, properties).join())
            .isInstanceOf(CompletionException.class)
            .hasCauseInstanceOf(PartitionLifecycleFencedException.class)
            .hasMessageContaining("different durable owner or ID");

        JsonNode adoptedConfig = json(config);
        assertThat(adoptedConfig.path("_provisioning").asBoolean()).isTrue();
        assertThat(adoptedConfig.path("_ownerGeneration").asLong()).isEqualTo(1L);
        assertThat(adoptedConfig.path("_ownerToken").asText())
            .isEqualTo(ownerAConfig.path("_ownerToken").asText());
        assertThat(metadata.currentResult()).isNull();
        assertThat(mappings.activeStreamId()).contains(101L);

        staleMetadataRead.complete(null);
        assertThat(ownerA.join()).isSameAs(recovered);
        assertThat(json(config).path("_provisioning").asBoolean(false)).isFalse();
        assertThat(metadata(metadata).streamId()).isEqualTo(101L);
        assertThat(metadata(metadata).registrationOwnerGeneration()).isEqualTo(1L);
        LogMetadata current = metadata(metadata);
        assertThat(current.retiredStreamIds()).isEmpty();
        assertThat(current.purgeableRetiredStreamIds()).isEmpty();
        assertThat(current.retiredStreamMappings()).isEmpty();
        assertThat(current.retiredMappingKeys()).isEmpty();
        assertThat(mappings.bindCalls()).isEqualTo(1);
        assertThat(mappings.fenceCalls()).isZero();
        verify(logStorage, never()).deleteLog(LogId.of(101L));
        verify(logStorage, never()).deleteLog(LogId.of(202L));
    }

    @Test
    void deletesDataAfterCasTombstoneThenDeletesKeyedMapping() throws Exception {
        int partition = 1;
        String logName = paths.compactedReaderName(stream, partition);
        String configPath = paths.streamConfigPath(stream);
        String metadataPath = paths.partitionMetadataPath(stream, partition);
        String incarnation = "incarnation-1";
        String owner = "owner-1";
        mockVersionedRecord(configPath, activeExternalConfigBytes(2, incarnation, owner));
        VersionedRecord metadata = mockVersionedRecord(
            metadataPath, metadataBytes(metadataPath, 52L, incarnation, owner, false));
        List<String> deletionOrder = new ArrayList<>();
        metadata.afterSuccessfulPut(ignored -> deletionOrder.add("metadata-tombstone"));
        when(logStorage.deleteLog(LogId.of(52L))).thenAnswer(ignored -> {
            deletionOrder.add("data");
            return CompletableFuture.completedFuture(null);
        });
        AtomicReference<Long> mapping = new AtomicReference<>(52L);
        IndexedStreamCatalog catalog = catalog(
            ignored -> CompletableFuture.completedFuture(52L),
            key -> mapping.get() == null
                ? CompletableFuture.failedFuture(new NoSuchKeyException(key))
                : CompletableFuture.completedFuture(mapping.get()),
            key -> {
                deletionOrder.add("mapping:" + key);
                mapping.set(null);
                return CompletableFuture.completedFuture(null);
            });

        catalog.deleteExternalPartition(stream, partition).get();

        assertThat(deletionOrder)
            .containsSubsequence(
                "metadata-tombstone", "mapping:" + logName,
                "data", "metadata-tombstone");
        assertThat(metadata(metadata).deleted()).isTrue();
        assertThat(metadata(metadata).streamId()).isEqualTo(52L);
        verify(oxiaClient, never()).delete(eq(metadataPath), any());
    }

    @Test
    void deletionCasRetryUsesRecomputedTombstoneIdWhenMappingIsAbsent() throws Exception {
        int partition = 1;
        long concurrentStreamId = 151L;
        String logName = paths.compactedReaderName(stream, partition);
        String configPath = paths.streamConfigPath(stream);
        String metadataPath = paths.partitionMetadataPath(stream, partition);
        mockVersionedRecord(configPath,
            permanentDeletionOfLegacyConfigBytes(2));
        VersionedRecord metadata = mockVersionedRecord(metadataPath, null);
        metadata.conflictNextPutWith(metadataBytes(
            metadataPath, concurrentStreamId, null, null, false));
        List<Long> mappingDeletes = new ArrayList<>();
        IndexedStreamCatalog catalog = catalog(
            ignored -> CompletableFuture.completedFuture(concurrentStreamId),
            ignored -> CompletableFuture.completedFuture(concurrentStreamId),
            ignored -> CompletableFuture.failedFuture(new NoSuchKeyException(logName)),
            (key, expectedStreamId) -> {
                assertThat(key).isEqualTo(logName);
                mappingDeletes.add(expectedStreamId);
                return CompletableFuture.completedFuture(null);
            });

        catalog.deleteExternalPartition(stream, partition).join();

        LogMetadata tombstone = metadata(metadata);
        assertThat(tombstone.deleted()).isTrue();
        assertThat(tombstone.streamId()).isEqualTo(concurrentStreamId);
        verify(logStorage).deleteLog(LogId.of(concurrentStreamId));
        assertThat(mappingDeletes).isEmpty();
    }

    @Test
    void deletionMetadataCasStopsAfterBoundedBackoffRetries() {
        int partition = 1;
        long concurrentStreamId = 152L;
        String logName = paths.compactedReaderName(stream, partition);
        String configPath = paths.streamConfigPath(stream);
        String metadataPath = paths.partitionMetadataPath(stream, partition);
        mockVersionedRecord(configPath, permanentDeletionOfLegacyConfigBytes(2));
        VersionedRecord metadata = mockVersionedRecord(metadataPath, null);
        metadata.conflictEveryPutWith(metadataBytes(
            metadataPath, concurrentStreamId, null, null, false));
        @SuppressWarnings("unchecked")
        BiFunction<String, Long, CompletableFuture<Void>> mappingDeleter =
            mock(BiFunction.class);
        IndexedStreamCatalog catalog = catalog(
            ignored -> CompletableFuture.completedFuture(concurrentStreamId),
            ignored -> CompletableFuture.completedFuture(concurrentStreamId),
            ignored -> CompletableFuture.failedFuture(new NoSuchKeyException(logName)),
            mappingDeleter);

        assertThatThrownBy(() ->
                catalog.deleteExternalPartition(stream, partition).join())
            .isInstanceOf(CompletionException.class)
            .hasCauseInstanceOf(UnexpectedVersionIdException.class);

        assertThat(metadata.putAttempts()).isEqualTo(4);
        verify(logStorage, never()).deleteLog(any());
        verify(mappingDeleter, never()).apply(anyString(), any());
    }

    @Test
    void rejectedOpenRetriesContextReadAndCleansKnownAllocation() throws Exception {
        int partition = 0;
        long allocatedStreamId = 191L;
        String logName = paths.compactedReaderName(stream, partition);
        String configPath = paths.streamConfigPath(stream);
        String metadataPath = paths.partitionMetadataPath(stream, partition);
        RuntimeException registrationFailure = new RuntimeException("registration fenced");
        RuntimeException contextReadFailure = new RuntimeException("context read failed");
        VersionedRecord config = mockVersionedRecord(configPath, activeLegacyConfigBytes(1));
        VersionedRecord metadata = mockVersionedRecord(metadataPath, null);
        AtomicInteger postWriteReads = new AtomicInteger();
        config.interceptReads(current -> {
            if (metadata.successfulPuts() == 0) {
                return CompletableFuture.completedFuture(current);
            }
            return switch (postWriteReads.incrementAndGet()) {
                case 1 -> CompletableFuture.failedFuture(registrationFailure);
                case 2 -> CompletableFuture.failedFuture(contextReadFailure);
                default -> CompletableFuture.completedFuture(
                    new GetResult(configPath,
                        permanentDeletionOfLegacyConfigBytes(1), VERSION));
            };
        });
        AtomicReference<Long> mapping = new AtomicReference<>(allocatedStreamId);
        List<Long> mappingDeletes = new ArrayList<>();
        IndexedStreamCatalog catalog = catalog(
            ignored -> CompletableFuture.completedFuture(allocatedStreamId),
            ignored -> CompletableFuture.completedFuture(allocatedStreamId),
            key -> mapping.get() == null
                ? CompletableFuture.failedFuture(new NoSuchKeyException(key))
                : CompletableFuture.completedFuture(mapping.get()),
            (key, expectedStreamId) -> {
                assertThat(key).isEqualTo(logName);
                mappingDeletes.add(expectedStreamId);
                clearExpectedMapping(mapping, expectedStreamId);
                return CompletableFuture.completedFuture(null);
            });

        assertThatThrownBy(() ->
                catalog.openExternalPartition(stream, partition, Map.of()).join())
            .isInstanceOf(CompletionException.class)
            .hasCauseInstanceOf(
                IndexedStreamConfigStore.VerificationUnknownException.class)
            .satisfies(thrown -> {
                Throwable verificationFailure = thrown.getCause();
                assertThat(verificationFailure.getCause()).isSameAs(registrationFailure);
                assertThat(verificationFailure.getSuppressed()).contains(contextReadFailure);
            });
        assertThat(metadata(metadata).deleted()).isTrue();
        verify(logStorage).deleteLog(LogId.of(allocatedStreamId));
        assertThat(mappingDeletes).containsExactly(allocatedStreamId);
    }

    @Test
    void rejectedOpenPreservesReadAndCleanupFailuresAsSuppressed() {
        int partition = 0;
        long allocatedStreamId = 192L;
        String configPath = paths.streamConfigPath(stream);
        String metadataPath = paths.partitionMetadataPath(stream, partition);
        RuntimeException registrationFailure = new RuntimeException("registration fenced");
        RuntimeException contextReadFailure = new RuntimeException("context read failed");
        RuntimeException cleanupFailure = new RuntimeException("cleanup failed");
        VersionedRecord config = mockVersionedRecord(configPath, activeLegacyConfigBytes(1));
        VersionedRecord metadata = mockVersionedRecord(metadataPath, null);
        AtomicInteger postWriteReads = new AtomicInteger();
        config.interceptReads(current -> {
            if (metadata.successfulPuts() == 0) {
                return CompletableFuture.completedFuture(current);
            }
            return switch (postWriteReads.incrementAndGet()) {
                case 1 -> CompletableFuture.failedFuture(registrationFailure);
                case 2 -> CompletableFuture.failedFuture(contextReadFailure);
                default -> CompletableFuture.completedFuture(
                    new GetResult(configPath,
                        permanentDeletionOfLegacyConfigBytes(1), VERSION));
            };
        });
        when(logStorage.deleteLog(LogId.of(allocatedStreamId)))
            .thenReturn(CompletableFuture.failedFuture(cleanupFailure));
        IndexedStreamCatalog catalog = catalog(
            ignored -> CompletableFuture.completedFuture(allocatedStreamId),
            ignored -> CompletableFuture.completedFuture(allocatedStreamId),
            ignored -> CompletableFuture.completedFuture(allocatedStreamId),
            (key, expectedStreamId) -> CompletableFuture.completedFuture(null));

        assertThatThrownBy(() ->
                catalog.openExternalPartition(stream, partition, Map.of()).join())
            .isInstanceOf(CompletionException.class)
            .hasCauseInstanceOf(
                IndexedStreamConfigStore.VerificationUnknownException.class)
            .satisfies(thrown -> {
                Throwable verificationFailure = thrown.getCause();
                assertThat(verificationFailure.getCause()).isSameAs(registrationFailure);
                assertThat(verificationFailure.getSuppressed())
                    .containsExactly(contextReadFailure, cleanupFailure);
            });
    }

    @Test
    void deletionRetriesWithSuccessorContextAfterPermanentTransition() throws Exception {
        int partition = 1;
        String logName = paths.compactedReaderName(stream, partition);
        String configPath = paths.streamConfigPath(stream);
        String metadataPath = paths.partitionMetadataPath(stream, partition);
        String incarnation = "incarnation-context-transition";
        VersionedRecord config = mockVersionedRecord(
            configPath, activeExternalConfigBytes(2, incarnation, "registration-owner"));
        VersionedRecord metadata = mockVersionedRecord(
            metadataPath, metadataBytes(
                metadataPath, 152L, incarnation, "registration-owner", 1L, false));
        AtomicBoolean transitioned = new AtomicBoolean();
        AtomicReference<IndexedStreamCatalog> catalogRef = new AtomicReference<>();
        when(logStorage.deleteLog(LogId.of(152L))).thenAnswer(ignored -> {
            if (transitioned.compareAndSet(false, true)) {
                catalogRef.get().permanentlyDeleteExternalStream(stream).join();
            }
            return CompletableFuture.completedFuture(null);
        });
        AtomicReference<Long> mapping = new AtomicReference<>(152L);
        List<String> mappingDeletes = new ArrayList<>();
        IndexedStreamCatalog catalog = catalog(
            ignored -> CompletableFuture.completedFuture(152L),
            key -> mapping.get() == null
                ? CompletableFuture.failedFuture(new NoSuchKeyException(key))
                : CompletableFuture.completedFuture(mapping.get()),
            key -> {
                mappingDeletes.add(key);
                mapping.set(null);
                return CompletableFuture.completedFuture(null);
            });
        catalogRef.set(catalog);

        catalog.deleteExternalPartition(stream, partition).join();

        assertThat(json(config).path("_provisioningState").asText())
            .isEqualTo("PERMANENTLY_DELETED");
        LogMetadata tombstone = metadata(metadata);
        assertThat(tombstone.deleted()).isTrue();
        assertThat(tombstone.registrationOwnerGeneration()).isEqualTo(1L);
        verify(logStorage, times(2)).deleteLog(LogId.of(152L));
        assertThat(mappingDeletes).containsExactly(logName);
    }

    @Test
    void deletionDoesNotRetryAcrossARecreatedIncarnation() throws Exception {
        int partition = 1;
        String configPath = paths.streamConfigPath(stream);
        String metadataPath = paths.partitionMetadataPath(stream, partition);
        String incarnation = "incarnation-before-recreate";
        VersionedRecord config = mockVersionedRecord(
            configPath, activeExternalConfigBytes(2, incarnation, "registration-owner"));
        VersionedRecord metadata = mockVersionedRecord(
            metadataPath, metadataBytes(
                metadataPath, 153L, incarnation, "registration-owner", 1L, false));
        when(logStorage.deleteLog(LogId.of(153L))).thenAnswer(ignored -> {
            GetResult current = config.currentResult();
            config.put(activeExternalConfigBytes(
                    2, "different-incarnation", "different-owner", 3L),
                Set.of(PutOption.IfVersionIdEquals(
                    current.version().versionId()))).join();
            return CompletableFuture.completedFuture(null);
        });
        @SuppressWarnings("unchecked")
        Function<String, CompletableFuture<Void>> mappingDeleter = mock(Function.class);
        IndexedStreamCatalog catalog = catalog(
            ignored -> CompletableFuture.completedFuture(153L),
            ignored -> CompletableFuture.completedFuture(153L),
            mappingDeleter);

        assertThatThrownBy(() -> catalog.deleteExternalPartition(stream, partition).join())
            .isInstanceOf(CompletionException.class)
            .hasCauseInstanceOf(
                IndexedStreamConfigStore.ExternalDeletionContextInvalidatedException.class);

        assertThat(metadata(metadata).registrationIncarnationId()).isEqualTo(incarnation);
        verify(logStorage).deleteLog(LogId.of(153L));
        verify(mappingDeleter).apply(anyString());
    }

    @Test
    void reopenedPartitionRotatesPhysicalIdBeforeStaleDeletionResumes() throws Exception {
        int partition = 1;
        long retiredStreamId = 701L;
        long replacementStreamId = 702L;
        String logName = paths.compactedReaderName(stream, partition);
        String configPath = paths.streamConfigPath(stream);
        String metadataPath = paths.partitionMetadataPath(stream, partition);
        String incarnation = "incarnation-physical-id-rotation";
        VersionedRecord config = mockVersionedRecord(
            configPath, activeExternalConfigBytes(
                2, incarnation, "generation-1-owner", 1L));
        VersionedRecord metadata = mockVersionedRecord(
            metadataPath, metadataBytes(
                metadataPath, retiredStreamId, incarnation,
                "generation-1-owner", 1L, false));
        AtomicReference<Long> mapping = new AtomicReference<>(retiredStreamId);
        CompletableFuture<Void> stalePhysicalDelete = new CompletableFuture<>();
        AtomicInteger retiredLogDeletes = new AtomicInteger();
        when(logStorage.deleteLog(LogId.of(retiredStreamId))).thenAnswer(ignored ->
            retiredLogDeletes.incrementAndGet() == 1
                ? stalePhysicalDelete : CompletableFuture.completedFuture(null));
        List<Long> mappingDeletes = new ArrayList<>();
        CompactedObjectReader reader = mock(CompactedObjectReader.class);
        Log replacementLog = mock(Log.class);
        when(readerFactory.open(logName)).thenReturn(reader);
        when(logFactory.create(logName, LogId.of(replacementStreamId), reader))
            .thenReturn(replacementLog);
        IndexedStreamCatalog catalog = catalog(
            ignored -> CompletableFuture.completedFuture(retiredStreamId),
            key -> {
                assertThat(key).isEqualTo(logName);
                assertThat(mapping.compareAndSet(null, replacementStreamId)).isTrue();
                return CompletableFuture.completedFuture(replacementStreamId);
            },
            key -> {
                assertThat(key).isEqualTo(logName);
                Long streamId = mapping.get();
                return streamId == null
                    ? CompletableFuture.failedFuture(
                        new NoSuchKeyException(key))
                    : CompletableFuture.completedFuture(streamId);
            },
            (key, expectedStreamId) -> {
                assertThat(key).isEqualTo(logName);
                mappingDeletes.add(expectedStreamId);
                Long current = mapping.get();
                if (current != null
                        && current.longValue() == expectedStreamId.longValue()) {
                    mapping.compareAndSet(current, null);
                }
                return CompletableFuture.completedFuture(null);
            });

        CompletableFuture<Void> generationOneDeletion =
            catalog.deleteExternalPartition(stream, partition);
        assertThat(generationOneDeletion).isNotDone();
        assertThat(metadata(metadata).deleted()).isTrue();
        assertThat(metadata(metadata).streamId()).isEqualTo(retiredStreamId);
        assertThat(metadata(metadata).registrationOwnerGeneration()).isEqualTo(1L);

        catalog.unregisterExternalStream(stream).join();
        assertThat(catalog.openExternalPartition(stream, partition, Map.of()).join())
            .isSameAs(replacementLog);

        JsonNode active = json(config);
        LogMetadata replacement = metadata(metadata);
        assertThat(active.path("_provisioningState").asText("ACTIVE"))
            .isEqualTo("ACTIVE");
        assertThat(active.path("_ownerGeneration").asLong()).isEqualTo(2L);
        assertThat(replacement.deleted()).isFalse();
        assertThat(replacement.streamId()).isEqualTo(replacementStreamId);
        assertThat(replacement.registrationIncarnationId()).isEqualTo(incarnation);
        assertThat(replacement.registrationOwnerGeneration()).isEqualTo(2L);
        assertThat(replacement.retiredStreamIds()).isEmpty();
        assertThat(mapping).hasValue(replacementStreamId);
        assertThat(mappingDeletes).containsExactly(retiredStreamId);

        stalePhysicalDelete.complete(null);
        assertThatThrownBy(generationOneDeletion::join)
            .isInstanceOf(CompletionException.class)
            .hasCauseInstanceOf(
                IndexedStreamConfigStore.ExternalDeletionContextInvalidatedException.class);

        assertThat(mapping).hasValue(replacementStreamId);
        assertThat(metadata(metadata).streamId()).isEqualTo(replacementStreamId);
        assertThat(metadata(metadata).deleted()).isFalse();
        assertThat(retiredLogDeletes).hasValue(2);
        verify(logStorage, never()).deleteLog(LogId.of(replacementStreamId));
    }

    @Test
    void reopenedPartitionRejectsAllocatorThatReusesRetiredPhysicalId() throws Exception {
        int partition = 0;
        long retiredStreamId = 711L;
        String logName = paths.compactedReaderName(stream, partition);
        String configPath = paths.streamConfigPath(stream);
        String metadataPath = paths.partitionMetadataPath(stream, partition);
        String incarnation = "incarnation-reused-physical-id";
        VersionedRecord config = mockVersionedRecord(
            configPath, activeExternalConfigBytes(
                1, incarnation, "generation-1-owner", 1L));
        VersionedRecord metadata = mockVersionedRecord(
            metadataPath, metadataBytes(
                metadataPath, retiredStreamId, incarnation,
                "generation-1-owner", 1L, true));
        StreamIdMappingOwner retiredOwner = new StreamIdMappingOwner(
            incarnation, "generation-1-owner", 1L);
        FencedMappingHarness mappings = new FencedMappingHarness(
            logName, retiredStreamId, retiredOwner, retiredStreamId);
        mappings.fence(logName, retiredStreamId, retiredOwner).join();
        IndexedStreamCatalog catalog = fencedCatalog(mappings.storageApi());

        catalog.unregisterExternalStream(stream).join();

        assertThatThrownBy(() ->
                catalog.openExternalPartition(stream, partition, Map.of()).join())
            .isInstanceOf(CompletionException.class)
            .hasCauseInstanceOf(PartitionLifecycleFencedException.class)
            .hasMessageContaining("allocator returned a retired physical stream ID");

        assertThat(mappings.activeStreamId()).contains(retiredStreamId);
        LogMetadata retired = metadata(metadata);
        assertThat(retired.deleted()).isTrue();
        assertThat(retired.streamId()).isEqualTo(retiredStreamId);
        assertThat(retired.retiredStreamIds()).containsExactly(retiredStreamId);
        assertThat(retired.purgeableRetiredStreamIds()).isEmpty();
        assertThat(retired.retiredStreamMappings())
            .containsExactly(new LogMetadata.RetiredStreamMapping(retiredStreamId, logName));
        assertThat(retired.retiredMappingKeys()).containsExactly(logName);
        assertThat(metadata.successfulPuts()).isEqualTo(1);
        assertThat(json(config).path("_provisioningState").asText())
            .isEqualTo("PROVISIONING");
        assertThat(mappings.bindCalls()).isZero();
        assertThat(mappings.fenceCalls()).isBetween(2, 5);
        verify(logStorage, never()).deleteLog(LogId.of(retiredStreamId));
        verify(readerFactory, never()).open(anyString());
    }

    @Test
    void unregisteredExternalStreamRemainsPartitionCleanupCapable() throws Exception {
        int partition = 2;
        String logName = paths.compactedReaderName(stream, partition);
        String configPath = paths.streamConfigPath(stream);
        String metadataPath = paths.partitionMetadataPath(stream, partition);
        String incarnation = "incarnation-unregistered-cleanup";
        VersionedRecord config = mockVersionedRecord(
            configPath, activeExternalConfigBytes(3, incarnation, "registration-owner"));
        VersionedRecord metadata = mockVersionedRecord(
            metadataPath, metadataBytes(
                metadataPath, 154L, incarnation, "registration-owner", 1L, false));
        AtomicReference<Long> mapping = new AtomicReference<>(154L);
        List<String> mappingDeletes = new ArrayList<>();
        IndexedStreamCatalog catalog = catalog(
            ignored -> CompletableFuture.completedFuture(154L),
            key -> mapping.get() == null
                ? CompletableFuture.failedFuture(new NoSuchKeyException(key))
                : CompletableFuture.completedFuture(mapping.get()),
            key -> {
                mappingDeletes.add(key);
                mapping.set(null);
                return CompletableFuture.completedFuture(null);
            });

        catalog.unregisterExternalStream(stream).join();
        catalog.deleteExternalPartition(stream, partition).join();

        assertThat(json(config).path("_provisioningState").asText())
            .isEqualTo("UNREGISTERED");
        assertThat(metadata(metadata).deleted()).isTrue();
        verify(logStorage).deleteLog(LogId.of(154L));
        assertThat(mappingDeletes).containsExactly(logName);
    }

    @Test
    void tombstonesMissingMetadataBeforeDeletingHistoricalMapping() throws Exception {
        int partition = 3;
        String logName = paths.compactedReaderName(stream, partition);
        String configPath = paths.streamConfigPath(stream);
        String metadataPath = paths.partitionMetadataPath(stream, partition);
        String incarnation = "incarnation-2";
        String owner = "owner-2";
        mockVersionedRecord(configPath, activeExternalConfigBytes(4, incarnation, owner));
        VersionedRecord metadata = mockVersionedRecord(metadataPath, null);
        List<String> deletionOrder = new ArrayList<>();
        metadata.afterSuccessfulPut(ignored -> deletionOrder.add("metadata-tombstone"));
        when(logStorage.deleteLog(LogId.of(63L))).thenAnswer(ignored -> {
            deletionOrder.add("data");
            return CompletableFuture.completedFuture(null);
        });
        AtomicReference<Long> mapping = new AtomicReference<>(63L);
        IndexedStreamCatalog catalog = catalog(
            ignored -> CompletableFuture.completedFuture(63L),
            key -> mapping.get() == null
                ? CompletableFuture.failedFuture(new NoSuchKeyException(key))
                : CompletableFuture.completedFuture(mapping.get()),
            key -> {
                deletionOrder.add("mapping:" + key);
                mapping.set(null);
                return CompletableFuture.completedFuture(null);
            });

        catalog.deleteExternalPartition(stream, partition).get();

        assertThat(deletionOrder).containsExactly(
            "metadata-tombstone", "mapping:" + logName,
            "metadata-tombstone", "data", "metadata-tombstone");
        LogMetadata tombstone = metadata(metadata);
        assertThat(tombstone.streamId()).isEqualTo(63L);
        assertThat(tombstone.registrationIncarnationId()).isEqualTo(incarnation);
        assertThat(tombstone.registrationOwnerToken()).isEqualTo(owner);
        assertThat(tombstone.registrationOwnerGeneration()).isEqualTo(1L);
        assertThat(tombstone.deleted()).isTrue();
        assertThat(tombstone.retiredStreamIds()).isEmpty();
        assertThat(tombstone.purgeableRetiredStreamIds()).isEmpty();
        assertThat(tombstone.retiredStreamMappings()).isEmpty();
        assertThat(tombstone.retiredMappingKeys()).isEmpty();
    }

    @Test
    void dataDeletionFailurePreservesTombstoneAndMapping() throws Exception {
        int partition = 4;
        String configPath = paths.streamConfigPath(stream);
        String metadataPath = paths.partitionMetadataPath(stream, partition);
        String incarnation = "incarnation-3";
        String owner = "owner-3";
        mockVersionedRecord(configPath, activeExternalConfigBytes(5, incarnation, owner));
        VersionedRecord metadata = mockVersionedRecord(metadataPath, null);
        RuntimeException deleteFailure = new RuntimeException("data delete failed");
        when(logStorage.deleteLog(LogId.of(74L)))
            .thenReturn(CompletableFuture.failedFuture(deleteFailure));
        @SuppressWarnings("unchecked")
        Function<String, CompletableFuture<Void>> mappingDeleter = mock(Function.class);
        IndexedStreamCatalog catalog = catalog(
            ignored -> CompletableFuture.completedFuture(74L),
            key -> CompletableFuture.completedFuture(74L),
            mappingDeleter);

        assertThatThrownBy(() -> catalog.deleteExternalPartition(stream, partition).join())
            .isInstanceOf(CompletionException.class)
            .hasCause(deleteFailure);

        assertThat(metadata(metadata).deleted()).isTrue();
        LogMetadata tombstone = metadata(metadata);
        assertThat(tombstone.streamId()).isEqualTo(74L);
        assertThat(tombstone.retiredStreamIds()).containsExactly(74L);
        assertThat(tombstone.purgeableRetiredStreamIds()).containsExactly(74L);
        assertThat(tombstone.retiredStreamMappings()).containsExactly(
            new LogMetadata.RetiredStreamMapping(74L,
                paths.compactedReaderName(stream, partition), true));
        assertThat(tombstone.retiredMappingKeys()).containsExactly(
            paths.compactedReaderName(stream, partition));
        verify(mappingDeleter).apply(paths.compactedReaderName(stream, partition));
        verify(oxiaClient, never()).delete(eq(metadataPath), any());
    }

    @Test
    void metadataTombstoneFailurePreservesDataAndKeyedMapping() throws Exception {
        int partition = 6;
        String configPath = paths.streamConfigPath(stream);
        String metadataPath = paths.partitionMetadataPath(stream, partition);
        String incarnation = "incarnation-4";
        String owner = "owner-4";
        mockVersionedRecord(configPath, activeExternalConfigBytes(7, incarnation, owner));
        VersionedRecord metadata = mockVersionedRecord(
            metadataPath, metadataBytes(metadataPath, 96L, incarnation, owner, false));
        RuntimeException metadataFailure = new RuntimeException("metadata tombstone failed");
        metadata.failNextPut(metadataFailure);
        @SuppressWarnings("unchecked")
        Function<String, CompletableFuture<Void>> mappingDeleter = mock(Function.class);
        IndexedStreamCatalog catalog = catalog(
            ignored -> CompletableFuture.completedFuture(96L),
            ignored -> CompletableFuture.completedFuture(96L),
            mappingDeleter);

        assertThatThrownBy(() -> catalog.deleteExternalPartition(stream, partition).join())
            .isInstanceOf(CompletionException.class)
            .hasCause(metadataFailure);

        assertThat(metadata(metadata).deleted()).isFalse();
        assertThat(metadata.putAttempts()).isEqualTo(1);
        verify(logStorage, never()).deleteLog(any());
        verify(mappingDeleter, never()).apply(anyString());
    }

    @Test
    void retryAfterMappingFailureReusesTombstoneAndCompletesCleanup() throws Exception {
        int partition = 7;
        String logName = paths.compactedReaderName(stream, partition);
        String configPath = paths.streamConfigPath(stream);
        String metadataPath = paths.partitionMetadataPath(stream, partition);
        String incarnation = "incarnation-5";
        String owner = "owner-5";
        mockVersionedRecord(configPath, activeExternalConfigBytes(8, incarnation, owner));
        VersionedRecord metadata = mockVersionedRecord(
            metadataPath, metadataBytes(metadataPath, 107L, incarnation, owner, false));
        AtomicInteger lookupCount = new AtomicInteger();
        AtomicInteger mappingDeleteCount = new AtomicInteger();
        AtomicReference<Long> mapping = new AtomicReference<>(107L);
        RuntimeException mappingFailure = new RuntimeException("mapping delete failed");
        IndexedStreamCatalog catalog = catalog(
            ignored -> CompletableFuture.completedFuture(107L),
            key -> {
                assertThat(key).isEqualTo(logName);
                lookupCount.incrementAndGet();
                return mapping.get() == null
                    ? CompletableFuture.failedFuture(new NoSuchKeyException(key))
                    : CompletableFuture.completedFuture(mapping.get());
            },
            key -> {
                assertThat(key).isEqualTo(logName);
                if (mappingDeleteCount.getAndIncrement() == 0) {
                    return CompletableFuture.failedFuture(mappingFailure);
                }
                mapping.set(null);
                return CompletableFuture.completedFuture(null);
            });

        assertThatThrownBy(() -> catalog.deleteExternalPartition(stream, partition).join())
            .isInstanceOf(CompletionException.class)
            .hasCause(mappingFailure);
        catalog.deleteExternalPartition(stream, partition).get();

        assertThat(metadata.successfulPuts()).isEqualTo(2);
        assertThat(metadata(metadata).deleted()).isTrue();
        verify(logStorage).deleteLog(LogId.of(107L));
        assertThat(lookupCount).hasValue(2);
        assertThat(mappingDeleteCount).hasValue(2);
    }

    @Test
    void registrationFailureDoesNotOpenReaderAndRetryReusesKeyedId() {
        int partition = 8;
        String logName = paths.compactedReaderName(stream, partition);
        String configPath = paths.streamConfigPath(stream);
        String metadataPath = paths.partitionMetadataPath(stream, partition);
        mockVersionedRecord(configPath, null);
        VersionedRecord metadata = mockVersionedRecord(metadataPath, null);
        RuntimeException registrationFailure = new RuntimeException("registration failed");
        metadata.failNextPut(registrationFailure);
        CompactedObjectReader reader = mock(CompactedObjectReader.class);
        Log log = mock(Log.class);
        when(readerFactory.open(logName)).thenReturn(reader);
        when(logFactory.create(logName, LogId.of(118L), reader)).thenReturn(log);
        AtomicInteger generationCount = new AtomicInteger();
        IndexedStreamCatalog catalog = catalog(
            key -> {
                generationCount.incrementAndGet();
                assertThat(key).contains(logName);
                return CompletableFuture.completedFuture(118L);
            },
            ignored -> CompletableFuture.completedFuture(118L),
            ignored -> CompletableFuture.completedFuture(null));

        assertThatThrownBy(() -> catalog.openExternalPartition(stream, partition, Map.of()).join())
            .isInstanceOf(CompletionException.class)
            .hasCause(registrationFailure);
        verify(readerFactory, never()).open(anyString());

        assertThat(catalog.openExternalPartition(stream, partition, Map.of()).join()).isSameAs(log);
        assertThat(generationCount).hasValue(2);
        assertThat(metadata.putAttempts()).isEqualTo(2);
        assertThat(metadata.successfulPuts()).isEqualTo(1);
        verify(readerFactory).open(logName);
    }

    @Test
    void activeStreamPreservesReusedAllocationWhenMetadataWriteFails() throws Exception {
        assertActiveStreamPreservesUnpublishedAllocationWhenMetadataWriteFails(false);
    }

    @Test
    void activeStreamPreservesCreatedAllocationWhenMetadataWriteFails() throws Exception {
        assertActiveStreamPreservesUnpublishedAllocationWhenMetadataWriteFails(true);
    }

    private void assertActiveStreamPreservesUnpublishedAllocationWhenMetadataWriteFails(
            boolean createdKeyedMapping) throws Exception {
        int partition = 0;
        long streamId = 119L;
        String logName = paths.compactedReaderName(stream, partition);
        String configPath = paths.streamConfigPath(stream);
        String metadataPath = paths.partitionMetadataPath(stream, partition);
        mockVersionedRecord(configPath, activeLegacyConfigBytes(1));
        VersionedRecord metadata = mockVersionedRecord(metadataPath, null);
        RuntimeException metadataFailure = new RuntimeException("metadata write failed");
        metadata.failNextPut(metadataFailure);
        AtomicReference<Long> mapping = new AtomicReference<>(
            createdKeyedMapping ? null : streamId);
        @SuppressWarnings("unchecked")
        BiFunction<String, Long, CompletableFuture<Void>> mappingDeleter =
            mock(BiFunction.class);
        CompactedObjectReader reader = mock(CompactedObjectReader.class);
        Log log = mock(Log.class);
        when(readerFactory.open(logName)).thenReturn(reader);
        when(logFactory.create(logName, LogId.of(streamId), reader)).thenReturn(log);
        IndexedStreamCatalog catalog = catalog(
            ignored -> CompletableFuture.completedFuture(streamId),
            key -> {
                assertThat(key).isEqualTo(logName);
                mapping.compareAndSet(null, streamId);
                return CompletableFuture.completedFuture(streamId);
            },
            key -> mapping.get() == null
                ? CompletableFuture.failedFuture(new NoSuchKeyException(key))
                : CompletableFuture.completedFuture(mapping.get()),
            mappingDeleter, createdKeyedMapping);

        assertThatThrownBy(() ->
                catalog.openExternalPartition(stream, partition, Map.of()).join())
            .isInstanceOf(CompletionException.class)
            .hasCause(metadataFailure);

        assertThat(metadata.currentResult()).isNull();
        assertThat(mapping).hasValue(streamId);
        verify(logStorage, never()).deleteLog(LogId.of(streamId));
        verify(mappingDeleter, never()).apply(logName, streamId);

        assertThat(catalog.openExternalPartition(stream, partition, Map.of()).join())
            .isSameAs(log);
        assertThat(metadata(metadata).streamId()).isEqualTo(streamId);
        assertThat(metadata(metadata).deleted()).isFalse();
    }

    @Test
    void missingMetadataAndMappingCreatesOneIdempotentTombstone() throws Exception {
        int partition = 5;
        String logName = paths.compactedReaderName(stream, partition);
        String configPath = paths.streamConfigPath(stream);
        String metadataPath = paths.partitionMetadataPath(stream, partition);
        String incarnation = "incarnation-6";
        String owner = "owner-6";
        mockVersionedRecord(configPath, activeExternalConfigBytes(6, incarnation, owner));
        VersionedRecord metadata = mockVersionedRecord(metadataPath, null);
        AtomicInteger lookupCount = new AtomicInteger();
        @SuppressWarnings("unchecked")
        Function<String, CompletableFuture<Void>> mappingDeleter = mock(Function.class);
        IndexedStreamCatalog catalog = catalog(
            ignored -> CompletableFuture.completedFuture(85L),
            key -> {
                assertThat(key).isEqualTo(logName);
                lookupCount.incrementAndGet();
                return CompletableFuture.failedFuture(
                    new CompletionException(new NoSuchKeyException("missing")));
            },
            mappingDeleter);

        catalog.deleteExternalPartition(stream, partition).get();
        catalog.deleteExternalPartition(stream, partition).get();

        LogMetadata tombstone = metadata(metadata);
        assertThat(tombstone.streamId()).isEqualTo(-1L);
        assertThat(tombstone.registrationIncarnationId()).isEqualTo(incarnation);
        assertThat(tombstone.registrationOwnerToken()).isEqualTo(owner);
        assertThat(tombstone.registrationOwnerGeneration()).isEqualTo(1L);
        assertThat(tombstone.deleted()).isTrue();
        assertThat(metadata.successfulPuts()).isEqualTo(4);
        assertThat(lookupCount).hasValue(2);
        verify(logStorage, never()).deleteLog(any());
        verify(mappingDeleter, never()).apply(anyString());
        verify(oxiaClient, never()).delete(eq(metadataPath), any());
    }

    @Test
    void deleteExternalPartitionRequiresActiveConfigBeforeMappingLookup() {
        int partition = 0;
        String configPath = paths.streamConfigPath(stream);
        String metadataPath = paths.partitionMetadataPath(stream, partition);
        when(oxiaClient.get(configPath)).thenReturn(CompletableFuture.completedFuture(null));
        @SuppressWarnings("unchecked")
        Function<String, CompletableFuture<Long>> mappingLookup = mock(Function.class);
        IndexedStreamCatalog catalog = catalog(
            ignored -> CompletableFuture.completedFuture(1L),
            mappingLookup,
            ignored -> CompletableFuture.completedFuture(null));

        assertThatThrownBy(() -> catalog.deleteExternalPartition(stream, partition).join())
            .isInstanceOf(CompletionException.class)
            .hasCauseInstanceOf(NoSuchStreamException.class);

        verify(mappingLookup, never()).apply(anyString());
        verify(oxiaClient, never()).get(metadataPath);
        verify(logStorage, never()).deleteLog(any());
    }

    @Test
    void taggedActiveRegistrationRejectsLegacyPartitionMetadata() {
        int partition = 0;
        String configPath = paths.streamConfigPath(stream);
        String metadataPath = paths.partitionMetadataPath(stream, partition);
        mockVersionedRecord(configPath,
            activeExternalConfigBytes(1, "incarnation-current", "owner-current"));
        VersionedRecord metadata = mockVersionedRecord(
            metadataPath, metadataBytes(metadataPath, 201L, null, null, false));
        IndexedStreamCatalog catalog = catalog(
            ignored -> CompletableFuture.completedFuture(201L),
            ignored -> CompletableFuture.completedFuture(201L),
            ignored -> CompletableFuture.completedFuture(null));

        assertThatThrownBy(() -> catalog.registerExternalPartition(
                stream, partition, 201L, Map.of()).join())
            .isInstanceOf(CompletionException.class)
            .hasCauseInstanceOf(PartitionLifecycleFencedException.class);

        assertThat(metadata.successfulPuts()).isZero();
    }

    @Test
    void activeRegistrationRejectsPartitionMetadataFromOtherIncarnation() {
        int partition = 0;
        String configPath = paths.streamConfigPath(stream);
        String metadataPath = paths.partitionMetadataPath(stream, partition);
        mockVersionedRecord(configPath,
            activeExternalConfigBytes(1, "incarnation-current", "owner-current"));
        VersionedRecord metadata = mockVersionedRecord(metadataPath, metadataBytes(
            metadataPath, 211L, "incarnation-other", "owner-other", false));
        IndexedStreamCatalog catalog = catalog(
            ignored -> CompletableFuture.completedFuture(211L),
            ignored -> CompletableFuture.completedFuture(211L),
            ignored -> CompletableFuture.completedFuture(null));

        assertThatThrownBy(() -> catalog.registerExternalPartition(
                stream, partition, 211L, Map.of()).join())
            .isInstanceOf(CompletionException.class)
            .hasCauseInstanceOf(PartitionLifecycleFencedException.class);

        assertThat(metadata.successfulPuts()).isZero();
    }

    @Test
    void lateWriteAfterFinalizeIsInvisibleUntilCurrentOwnerRetagsSameId() throws Exception {
        int partition = 0;
        String configPath = paths.streamConfigPath(stream);
        String metadataPath = paths.partitionMetadataPath(stream, partition);
        String incarnation = "incarnation-current";
        String currentOwner = "owner-current";
        mockVersionedRecord(configPath,
            activeExternalConfigBytes(1, incarnation, currentOwner, 2L));
        VersionedRecord metadata = mockVersionedRecord(metadataPath,
            metadataBytes(metadataPath, 301L, Map.of("marker", "preserve"),
                OptionalLong.of(9L), incarnation, "owner-stale", 1L, false));
        IndexedStreamCatalog catalog = catalog(
            ignored -> CompletableFuture.completedFuture(301L),
            ignored -> CompletableFuture.completedFuture(301L),
            ignored -> CompletableFuture.completedFuture(null));

        assertThatThrownBy(() -> catalog.getLayout(stream).join())
            .isInstanceOf(CompletionException.class)
            .hasCauseInstanceOf(
                IndexedStreamCatalog.PartitionMetadataFenceViolationException.class);

        catalog.registerExternalPartition(stream, partition, 301L, Map.of()).join();

        LogMetadata retagged = metadata(metadata);
        assertThat(retagged.streamId()).isEqualTo(301L);
        assertThat(retagged.registrationIncarnationId()).isEqualTo(incarnation);
        assertThat(retagged.registrationOwnerToken()).isEqualTo(currentOwner);
        assertThat(retagged.registrationOwnerGeneration()).isEqualTo(2L);
        assertThat(retagged.properties()).containsEntry("marker", "preserve");
        assertThat(retagged.terminatedOffset()).hasValue(9L);
        assertThat(metadata.successfulPuts()).isEqualTo(1);
        assertThat(catalog.getLayout(stream).join().logIds().join())
            .containsExactly(LogId.of(301L));
    }

    @Test
    void activeRegistrationReplacesLowerGenerationOwnerWithDifferentId() throws Exception {
        int partition = 0;
        String configPath = paths.streamConfigPath(stream);
        String metadataPath = paths.partitionMetadataPath(stream, partition);
        String incarnation = "incarnation-current";
        mockVersionedRecord(configPath,
            activeExternalConfigBytes(1, incarnation, "owner-current", 2L));
        VersionedRecord metadata = mockVersionedRecord(metadataPath,
            metadataBytes(metadataPath, 311L, incarnation, "owner-stale", 1L, false));
        IndexedStreamCatalog catalog = catalog(
            ignored -> CompletableFuture.completedFuture(312L),
            ignored -> CompletableFuture.completedFuture(312L),
            ignored -> CompletableFuture.completedFuture(null));

        catalog.registerExternalPartition(stream, partition, 312L, Map.of()).join();

        assertThat(metadata(metadata).streamId()).isEqualTo(312L);
        assertThat(metadata(metadata).registrationOwnerToken()).isEqualTo("owner-current");
        assertThat(metadata(metadata).registrationOwnerGeneration()).isEqualTo(2L);
        assertThat(metadata.successfulPuts()).isEqualTo(1);
    }

    @Test
    void activeRegistrationRejectsRetiredPhysicalStreamId() throws Exception {
        int partition = 0;
        long retiredStreamId = 312L;
        String logName = paths.compactedReaderName(stream, partition);
        String configPath = paths.streamConfigPath(stream);
        String metadataPath = paths.partitionMetadataPath(stream, partition);
        String incarnation = "incarnation-current";
        mockVersionedRecord(configPath,
            activeExternalConfigBytes(1, incarnation, "owner-current", 2L));
        VersionedRecord metadata = mockVersionedRecord(metadataPath,
            metadataBytes(metadataPath, 311L, Map.of(), OptionalLong.empty(),
                incarnation, "owner-stale", 1L, false, Set.of(retiredStreamId)));
        StreamIdMappingOwner currentOwner = new StreamIdMappingOwner(
            incarnation, "owner-current", 2L);
        FencedMappingHarness mappings = new FencedMappingHarness(
            logName, 311L, currentOwner, 313L);
        IndexedStreamCatalog catalog = fencedCatalog(mappings.storageApi());

        assertThatThrownBy(() -> catalog.registerExternalPartition(
                stream, partition, retiredStreamId, Map.of()).join())
            .isInstanceOf(CompletionException.class)
            .hasCauseInstanceOf(PartitionLifecycleFencedException.class)
            .hasMessageContaining("physical stream ID was already retired");

        assertThat(metadata(metadata).streamId()).isEqualTo(311L);
        assertThat(metadata(metadata).retiredStreamIds()).isEmpty();
        assertThat(metadata.successfulPuts()).isEqualTo(1);
        assertThat(mappings.activeStreamId()).contains(311L);
        assertThat(mappings.bindCalls()).isZero();
        assertThat(mappings.fenceCalls()).isZero();
        verify(logStorage, never()).deleteLog(any());
    }

    @Test
    void activeRegistrationRejectsSameGenerationOwnerWithDifferentId() throws Exception {
        int partition = 0;
        String configPath = paths.streamConfigPath(stream);
        String metadataPath = paths.partitionMetadataPath(stream, partition);
        String incarnation = "incarnation-current";
        mockVersionedRecord(configPath,
            activeExternalConfigBytes(1, incarnation, "owner-current", 2L));
        VersionedRecord metadata = mockVersionedRecord(metadataPath,
            metadataBytes(metadataPath, 311L, incarnation, "owner-impossible", 2L, false));
        IndexedStreamCatalog catalog = catalog(
            ignored -> CompletableFuture.completedFuture(312L),
            ignored -> CompletableFuture.completedFuture(312L),
            ignored -> CompletableFuture.completedFuture(null));

        assertThatThrownBy(() -> catalog.registerExternalPartition(
                stream, partition, 312L, Map.of()).join())
            .isInstanceOf(CompletionException.class)
            .hasCauseInstanceOf(PartitionLifecycleFencedException.class);

        assertThat(metadata(metadata).streamId()).isEqualTo(311L);
        assertThat(metadata.successfulPuts()).isZero();
    }

    @Test
    void reversibleUnregisterOnlyAdoptsTheRetainedExternalStreamId() throws Exception {
        int partition = 0;
        String configPath = paths.streamConfigPath(stream);
        String metadataPath = paths.partitionMetadataPath(stream, partition);
        String incarnation = "incarnation-reregister";
        VersionedRecord config = mockVersionedRecord(
            configPath,
            activeExternalConfigBytes(1, incarnation, "registration-owner"));
        VersionedRecord metadata = mockVersionedRecord(
            metadataPath,
            metadataBytes(metadataPath, 71L, incarnation, "registration-owner", 1L, false));
        IndexedStreamCatalog catalog = catalog(
            ignored -> CompletableFuture.completedFuture(71L),
            ignored -> CompletableFuture.completedFuture(71L),
            ignored -> CompletableFuture.completedFuture(null));

        catalog.unregisterExternalStream(stream).join();
        assertThat(json(config).path("_provisioningState").asText())
            .isEqualTo("UNREGISTERED");

        assertThatThrownBy(() -> catalog.registerExternalPartition(
                stream, partition, 72L, Map.of()).join())
            .isInstanceOf(CompletionException.class)
            .hasCauseInstanceOf(PartitionLifecycleFencedException.class);
        assertThat(metadata(metadata).streamId()).isEqualTo(71L);

        catalog.registerExternalPartition(stream, partition, 71L, Map.of()).join();

        JsonNode active = json(config);
        assertThat(active.path("_provisioning").asBoolean(false)).isFalse();
        assertThat(active.path("_incarnationId").asText()).isEqualTo(incarnation);
        assertThat(active.path("_ownerGeneration").asLong()).isEqualTo(2L);
        assertThat(active.path("_metadataSourceGeneration").asLong()).isEqualTo(1L);
        LogMetadata adopted = metadata(metadata);
        assertThat(adopted.streamId()).isEqualTo(71L);
        assertThat(adopted.registrationIncarnationId()).isEqualTo(incarnation);
        assertThat(adopted.registrationOwnerGeneration()).isEqualTo(2L);
    }

    @Test
    void newIncarnationReplacesDifferentIncarnationTombstone() throws Exception {
        int partition = 0;
        String configPath = paths.streamConfigPath(stream);
        String metadataPath = paths.partitionMetadataPath(stream, partition);
        VersionedRecord config = mockVersionedRecord(
            configPath, droppedExternalConfigBytes(
                1, "incarnation-old", "owner-old", 2L));
        VersionedRecord metadata = mockVersionedRecord(metadataPath,
            metadataBytes(
                metadataPath, 401L, "incarnation-old", "owner-old", 2L, true));
        IndexedStreamCatalog catalog = catalog(
            ignored -> CompletableFuture.completedFuture(402L),
            ignored -> CompletableFuture.completedFuture(402L),
            ignored -> CompletableFuture.completedFuture(null));

        catalog.registerExternalPartition(stream, partition, 402L, Map.of()).join();

        JsonNode active = json(config);
        LogMetadata replacement = metadata(metadata);
        assertThat(replacement.streamId()).isEqualTo(402L);
        assertThat(replacement.deleted()).isFalse();
        assertThat(replacement.registrationIncarnationId())
            .isEqualTo(active.path("_incarnationId").asText())
            .isNotEqualTo("incarnation-old");
        assertThat(replacement.registrationOwnerToken())
            .isEqualTo(active.path("_ownerToken").asText());
        assertThat(replacement.retiredStreamIds()).isEmpty();
        assertThat(metadata.successfulPuts()).isEqualTo(1);
    }

    @Test
    void legacyTombstoneRequiresExplicitUnregisterBeforeReplacement() throws Exception {
        int partition = 0;
        String configPath = paths.streamConfigPath(stream);
        String metadataPath = paths.partitionMetadataPath(stream, partition);
        VersionedRecord config = mockVersionedRecord(configPath, null);
        VersionedRecord metadata = mockVersionedRecord(
            metadataPath, metadataBytes(metadataPath, 501L, null, null, true));
        IndexedStreamCatalog catalog = catalog(
            ignored -> CompletableFuture.completedFuture(502L),
            ignored -> CompletableFuture.completedFuture(502L),
            ignored -> CompletableFuture.completedFuture(null));

        assertThatThrownBy(() -> catalog.registerExternalPartition(
                stream, partition, 502L, Map.of()).join())
            .isInstanceOf(CompletionException.class)
            .hasCauseInstanceOf(PartitionLifecycleFencedException.class)
            .hasMessageContaining("unregistering the external stream");

        JsonNode recoveryAnchor = json(config);
        assertThat(config.successfulPuts()).isEqualTo(1);
        assertThat(recoveryAnchor.path("_provisioning").asBoolean(false)).isFalse();
        assertThat(recoveryAnchor.path("_incarnationId").asText()).isNotBlank();
        assertThat(recoveryAnchor.path("_ownerGeneration").asLong()).isEqualTo(1L);
        assertThat(metadata(metadata).streamId()).isEqualTo(501L);
        assertThat(metadata(metadata).deleted()).isTrue();
        assertThat(metadata.successfulPuts()).isZero();

        catalog.unregisterExternalStream(stream).join();
        assertThat(json(config).path("_provisioningState").asText())
            .isEqualTo("UNREGISTERED");

        catalog.registerExternalPartition(stream, partition, 502L, Map.of()).join();

        assertThat(json(config).path("_provisioning").asBoolean(false)).isFalse();
        assertThat(json(config).path("_ownerGeneration").asLong()).isEqualTo(2L);
        assertThat(metadata(metadata).streamId()).isEqualTo(502L);
        assertThat(metadata(metadata).deleted()).isFalse();
        assertThat(metadata(metadata).registrationOwnerGeneration()).isEqualTo(2L);
        verify(logStorage, never()).deleteLog(LogId.of(501L));
    }

    @Test
    void legacyTombstoneRequiresExplicitUnregisterBeforeOpenReplacement()
            throws Exception {
        int partition = 0;
        long retiredStreamId = 503L;
        long replacementStreamId = 504L;
        String logName = paths.compactedReaderName(stream, partition);
        String configPath = paths.streamConfigPath(stream);
        String metadataPath = paths.partitionMetadataPath(stream, partition);
        VersionedRecord config = mockVersionedRecord(configPath, null);
        VersionedRecord metadata = mockVersionedRecord(
            metadataPath, metadataBytes(metadataPath, retiredStreamId, null, null, true));
        AtomicInteger allocations = new AtomicInteger();
        CompactedObjectReader reader = mock(CompactedObjectReader.class);
        Log replacement = mock(Log.class);
        when(readerFactory.open(logName)).thenReturn(reader);
        when(logFactory.create(logName, LogId.of(replacementStreamId), reader))
            .thenReturn(replacement);
        IndexedStreamCatalog catalog = catalog(
            ignored -> CompletableFuture.completedFuture(replacementStreamId),
            ignored -> {
                allocations.incrementAndGet();
                return CompletableFuture.completedFuture(replacementStreamId);
            },
            ignored -> CompletableFuture.failedFuture(new NoSuchKeyException(logName)),
            (ignored, streamId) -> CompletableFuture.completedFuture(null));

        assertThatThrownBy(() -> catalog.openExternalPartition(
                stream, partition, Map.of()).join())
            .isInstanceOf(CompletionException.class)
            .hasCauseInstanceOf(PartitionLifecycleFencedException.class)
            .hasMessageContaining("unregistering the external stream");

        assertThat(allocations).hasValue(0);
        assertThat(config.successfulPuts()).isEqualTo(1);
        assertThat(json(config).path("_provisioning").asBoolean(false)).isFalse();
        assertThat(metadata.successfulPuts()).isZero();

        catalog.unregisterExternalStream(stream).join();
        assertThat(catalog.openExternalPartition(stream, partition, Map.of()).join())
            .isSameAs(replacement);

        assertThat(allocations).hasValue(1);
        assertThat(json(config).path("_ownerGeneration").asLong()).isEqualTo(2L);
        assertThat(metadata(metadata).streamId()).isEqualTo(replacementStreamId);
        assertThat(metadata(metadata).deleted()).isFalse();
        assertThat(metadata(metadata).registrationOwnerGeneration()).isEqualTo(2L);
        verify(logStorage, never()).deleteLog(LogId.of(retiredStreamId));
    }

    @Test
    void activeIncarnationDoesNotReplaceItsOwnTombstone() throws Exception {
        int partition = 0;
        String configPath = paths.streamConfigPath(stream);
        String metadataPath = paths.partitionMetadataPath(stream, partition);
        String incarnation = "incarnation-current";
        String owner = "owner-current";
        mockVersionedRecord(configPath, activeExternalConfigBytes(1, incarnation, owner));
        VersionedRecord metadata = mockVersionedRecord(
            metadataPath, metadataBytes(metadataPath, 601L, incarnation, owner, true));
        IndexedStreamCatalog catalog = catalog(
            ignored -> CompletableFuture.completedFuture(601L),
            ignored -> CompletableFuture.completedFuture(601L),
            ignored -> CompletableFuture.completedFuture(null));

        assertThatThrownBy(() -> catalog.registerExternalPartition(
                stream, partition, 601L, Map.of()).join())
            .isInstanceOf(CompletionException.class)
            .hasCauseInstanceOf(PartitionLifecycleFencedException.class)
            .hasMessageContaining("unregister the stream before recreating it");

        assertThat(metadata(metadata).deleted()).isTrue();
        assertThat(metadata.successfulPuts()).isZero();
    }

    @Test
    void fencedCleanupJournalRemainsBoundedAcrossOneHundredRecreateCycles()
            throws Exception {
        int partition = 0;
        String logName = paths.compactedReaderName(stream, partition);
        String configPath = paths.streamConfigPath(stream);
        String metadataPath = paths.partitionMetadataPath(stream, partition);
        String incarnation = "bounded-cleanup-incarnation";
        String owner = "bounded-cleanup-owner";
        long initialStreamId = 1_000L;
        VersionedRecord config = mockVersionedRecord(
            configPath, activeExternalConfigBytes(1, incarnation, owner));
        VersionedRecord metadata = mockVersionedRecord(
            metadataPath, metadataBytes(
                metadataPath, initialStreamId, incarnation, owner, 1L, false));
        FencedMappingHarness mappings = new FencedMappingHarness(
            logName, initialStreamId,
            new StreamIdMappingOwner(incarnation, owner, 1L), 2_000L);
        when(readerFactory.open(logName))
            .thenAnswer(ignored -> mock(CompactedObjectReader.class));
        when(logFactory.create(eq(logName), any(LogId.class),
                any(CompactedObjectReader.class)))
            .thenAnswer(ignored -> mock(Log.class));
        IndexedStreamCatalog catalog = fencedCatalog(mappings.storageApi());
        int maximumMetadataBytes = 0;

        for (int cycle = 0; cycle < 100; cycle++) {
            catalog.deleteExternalPartition(stream, partition).join();
            LogMetadata deleted = metadata(metadata);
            assertThat(deleted.purgeableRetiredStreamIds()).isEmpty();
            assertThat(deleted.retiredStreamMappings()).isEmpty();
            assertThat(deleted.retiredMappingKeys()).isEmpty();
            assertThat(deleted.retiredStreamIds()).isEmpty();
            maximumMetadataBytes = Math.max(
                maximumMetadataBytes, metadata.currentValue().length);

            catalog.unregisterExternalStream(stream).join();
            catalog.openExternalPartition(stream, partition, Map.of()).join();

            LogMetadata active = metadata(metadata);
            assertThat(active.deleted()).isFalse();
            assertThat(active.purgeableRetiredStreamIds()).isEmpty();
            assertThat(active.retiredStreamMappings()).isEmpty();
            assertThat(active.retiredMappingKeys()).isEmpty();
            assertThat(active.retiredStreamIds()).isEmpty();
            maximumMetadataBytes = Math.max(
                maximumMetadataBytes, metadata.currentValue().length);
        }

        assertThat(maximumMetadataBytes).isLessThan(512);
        assertThat(mappings.fenceCalls()).isLessThanOrEqualTo(100);
        assertThat(config.successfulPuts()).isGreaterThanOrEqualTo(200);
    }

    @Test
    void ownerAwareNonPurgingNativeDropAndRecreateKeepsJournalBounded()
            throws Exception {
        int partition = 0;
        String mappingKey = "lakestream-native/" + stream.fullName() + "/partition-0";
        String configPath = paths.streamConfigPath(stream);
        String metadataPath = paths.partitionMetadataPath(stream, partition);
        String incarnation = "native-bounded-incarnation";
        String ownerToken = "native-owner";
        long initialStreamId = 1_500L;
        StreamIdMappingOwner initialOwner = new StreamIdMappingOwner(
            incarnation, ownerToken, 1L);
        VersionedRecord config = mockVersionedRecord(configPath,
            ("{\"partitions\":1,\"properties\":{},"
                + "\"_incarnationId\":\"" + incarnation + "\","
                + "\"_ownerToken\":\"" + ownerToken + "\","
                + "\"_ownerGeneration\":1,"
                + "\"_creationKind\":\"NATIVE_CREATE\"}")
                .getBytes(StandardCharsets.UTF_8));
        VersionedRecord metadata = mockVersionedRecord(
            metadataPath, metadataBytes(
                metadataPath, initialStreamId, incarnation, ownerToken, 1L, false));
        FencedMappingHarness mappings = new FencedMappingHarness(
            mappingKey, initialStreamId, initialOwner, 2_000L);
        IndexedStreamCatalog catalog = fencedCatalog(mappings.storageApi());
        Partitioning partitioning = new Partitioning(
            PartitioningStrategy.INDEXED, Map.of("numPartitions", "1"));
        int maximumMetadataBytes = 0;

        for (int cycle = 0; cycle < 25; cycle++) {
            long droppedStreamId = mappings.activeStreamId().orElseThrow();
            assertThat(catalog.dropStream(stream, false).join()).isTrue();

            LogMetadata dropped = metadata(metadata);
            assertThat(dropped.deleted()).isTrue();
            assertThat(dropped.streamId()).isEqualTo(droppedStreamId);
            assertThat(dropped.retiredStreamIds()).isEmpty();
            assertThat(dropped.purgeableRetiredStreamIds()).isEmpty();
            assertThat(dropped.retiredStreamMappings()).isEmpty();
            assertThat(dropped.retiredMappingKeys()).isEmpty();
            maximumMetadataBytes = Math.max(
                maximumMetadataBytes, metadata.currentValue().length);
            verify(logStorage, never()).deleteLog(LogId.of(droppedStreamId));

            catalog.createStream(
                stream, new StreamConfig(), partitioning,
                new SchemaConfig(), Map.of()).join();

            LogMetadata active = metadata(metadata);
            assertThat(active.deleted()).isFalse();
            assertThat(active.retiredStreamIds()).isEmpty();
            assertThat(active.purgeableRetiredStreamIds()).isEmpty();
            assertThat(active.retiredStreamMappings()).isEmpty();
            assertThat(active.retiredMappingKeys()).isEmpty();
            maximumMetadataBytes = Math.max(
                maximumMetadataBytes, metadata.currentValue().length);
        }

        assertThat(maximumMetadataBytes).isLessThan(512);
        assertThat(mappings.fenceCalls()).isEqualTo(25);
        assertThat(config.successfulPuts()).isGreaterThanOrEqualTo(75);
    }

    @Test
    void registerThatOwnsMappingIsNotDeletedByRetiredCleanup() throws Exception {
        int partition = 0;
        String logName = paths.compactedReaderName(stream, partition);
        String configPath = paths.streamConfigPath(stream);
        String metadataPath = paths.partitionMetadataPath(stream, partition);
        String incarnation = "register-open-race";
        StreamIdMappingOwner retiredOwner = new StreamIdMappingOwner(
            incarnation, "retired-owner", 1L);
        StreamIdMappingOwner currentOwner = new StreamIdMappingOwner(
            incarnation, "broker-owner", 2L);
        long retiredStreamId = 910L;
        long brokerStreamId = 920L;
        mockVersionedRecord(configPath, ("{\"partitions\":1,\"properties\":{},"
            + "\"_incarnationId\":\"" + incarnation + "\","
            + "\"_ownerToken\":\"broker-owner\","
            + "\"_ownerGeneration\":2,"
            + "\"_metadataSourceOwnerToken\":\"retired-owner\","
            + "\"_metadataSourceGeneration\":1,"
            + "\"_creationKind\":\"EXTERNAL\"}")
            .getBytes(StandardCharsets.UTF_8));
        VersionedRecord metadata = mockVersionedRecord(
            metadataPath, LOG_METADATA_SERDE.serialize(metadataPath, new LogMetadata(
                retiredStreamId, Map.of(), OptionalLong.empty(),
                retiredOwner.incarnationId(), retiredOwner.ownerToken(),
                retiredOwner.ownerGeneration(), true, Set.of(retiredStreamId),
                Set.of(retiredStreamId),
                Set.of(new LogMetadata.RetiredStreamMapping(
                    retiredStreamId, logName)), Set.of(logName))));
        FencedMappingHarness mappings = new FencedMappingHarness(
            logName, brokerStreamId, currentOwner, 1_000L);
        IndexedStreamCatalog catalog = fencedCatalog(mappings.storageApi());

        catalog.registerExternalPartition(
            stream, partition, brokerStreamId, Map.of()).join();

        LogMetadata registered = metadata(metadata);
        assertThat(registered.streamId()).isEqualTo(brokerStreamId);
        assertThat(registered.registrationOwnerToken())
            .isEqualTo(currentOwner.ownerToken());
        assertThat(registered.retiredStreamMappings()).isEmpty();
        assertThat(registered.retiredMappingKeys()).isEmpty();
        verify(logStorage, never()).deleteLog(LogId.of(brokerStreamId));
        assertThat(mappings.activeStreamId()).contains(brokerStreamId);
    }

    @Test
    void activeKeysOnlyJournalCapturesLateAllocatorAndRestoresCanonicalMapping()
            throws Exception {
        int partition = 0;
        String logName = paths.compactedReaderName(stream, partition);
        String configPath = paths.streamConfigPath(stream);
        String metadataPath = paths.partitionMetadataPath(stream, partition);
        String incarnation = "active-keys-only";
        String ownerToken = "active-owner";
        long currentStreamId = 930L;
        long previouslyRetiredStreamId = 929L;
        long lateAllocatedStreamId = 931L;
        StreamIdMappingOwner owner = new StreamIdMappingOwner(
            incarnation, ownerToken, 1L);
        mockVersionedRecord(
            configPath, activeExternalConfigBytes(1, incarnation, ownerToken));
        VersionedRecord metadata = mockVersionedRecord(
            metadataPath, metadataBytes(
                metadataPath, currentStreamId, incarnation, ownerToken, 1L, false,
                Set.of(previouslyRetiredStreamId), Set.of(), Set.of(), Set.of(logName)));
        FencedMappingHarness mappings = new FencedMappingHarness(
            logName, lateAllocatedStreamId, owner, 1_000L);
        RuntimeException crashAfterCapture =
            new RuntimeException("crash after captured mapping was journaled");
        mappings.failNextCanonicalize(crashAfterCapture);
        IndexedStreamCatalog catalog = fencedCatalog(mappings.storageApi());

        assertThatThrownBy(() -> catalog.registerExternalPartition(
                stream, partition, currentStreamId, Map.of()).join())
            .isInstanceOf(CompletionException.class)
            .hasCause(crashAfterCapture);

        LogMetadata captured = metadata(metadata);
        assertThat(captured.retiredStreamMappings())
            .containsExactly(new LogMetadata.RetiredStreamMapping(
                lateAllocatedStreamId, logName));
        assertThat(captured.retiredStreamIds())
            .containsExactly(previouslyRetiredStreamId, lateAllocatedStreamId);
        assertThat(mappings.activeStreamId()).isEmpty();

        catalog.registerExternalPartition(
            stream, partition, currentStreamId, Map.of()).join();

        LogMetadata registered = metadata(metadata);
        assertThat(registered.streamId()).isEqualTo(currentStreamId);
        assertThat(registered.retiredStreamIds()).isEmpty();
        assertThat(registered.purgeableRetiredStreamIds()).isEmpty();
        assertThat(registered.retiredStreamMappings()).isEmpty();
        assertThat(registered.retiredMappingKeys()).isEmpty();
        assertThat(mappings.activeStreamId()).contains(currentStreamId);
        assertThat(mappings.fenceCalls()).isEqualTo(2);
        assertThat(mappings.canonicalizeCalls()).isEqualTo(2);
        assertThat(mappings.bindCalls()).isGreaterThanOrEqualTo(2);
        verify(logStorage, never()).deleteLog(LogId.of(currentStreamId));
        verify(logStorage, never()).deleteLog(LogId.of(lateAllocatedStreamId));
    }

    @Test
    void knownRetiredIdWithAbsentMappingCanonicalizesAndReplaysCleanup()
            throws Exception {
        int partition = 0;
        String logName = paths.compactedReaderName(stream, partition);
        String configPath = paths.streamConfigPath(stream);
        String metadataPath = paths.partitionMetadataPath(stream, partition);
        String incarnation = "known-id-absent-mapping";
        String ownerToken = "known-id-owner";
        long streamId = 935L;
        StreamIdMappingOwner owner = new StreamIdMappingOwner(
            incarnation, ownerToken, 1L);
        mockVersionedRecord(
            configPath, activeExternalConfigBytes(1, incarnation, ownerToken));
        VersionedRecord metadata = mockVersionedRecord(
            metadataPath, metadataBytes(
                metadataPath, streamId, incarnation, ownerToken, 1L, false));
        FencedMappingHarness mappings = new FencedMappingHarness(
            logName, streamId, owner, 1_000L);
        mappings.clearMapping();
        RuntimeException firstDeleteFailure =
            new RuntimeException("delete response failed after canonical fence");
        AtomicInteger deleteAttempts = new AtomicInteger();
        when(logStorage.deleteLog(LogId.of(streamId))).thenAnswer(ignored ->
            deleteAttempts.getAndIncrement() == 0
                ? CompletableFuture.failedFuture(firstDeleteFailure)
                : CompletableFuture.completedFuture(null));
        IndexedStreamCatalog catalog = fencedCatalog(mappings.storageApi());

        assertThatThrownBy(() ->
                catalog.deleteExternalPartition(stream, partition).join())
            .isInstanceOf(CompletionException.class)
            .hasCause(firstDeleteFailure);

        LogMetadata interrupted = metadata(metadata);
        assertThat(interrupted.deleted()).isTrue();
        assertThat(interrupted.streamId()).isEqualTo(streamId);
        assertThat(interrupted.retiredStreamIds()).containsExactly(streamId);
        assertThat(interrupted.purgeableRetiredStreamIds()).containsExactly(streamId);
        assertThat(interrupted.retiredStreamMappings())
            .containsExactly(new LogMetadata.RetiredStreamMapping(-1L, logName, true));
        assertThat(interrupted.retiredMappingKeys()).containsExactly(logName);
        assertThat(mappings.activeStreamId()).isEmpty();

        catalog.deleteExternalPartition(stream, partition).join();

        LogMetadata cleaned = metadata(metadata);
        assertThat(cleaned.deleted()).isTrue();
        assertThat(cleaned.streamId()).isEqualTo(streamId);
        assertThat(cleaned.retiredStreamIds()).isEmpty();
        assertThat(cleaned.purgeableRetiredStreamIds()).isEmpty();
        assertThat(cleaned.retiredStreamMappings()).isEmpty();
        assertThat(cleaned.retiredMappingKeys()).isEmpty();
        assertThat(deleteAttempts).hasValue(2);
        assertThat(mappings.fenceCalls()).isEqualTo(2);
        assertThat(mappings.canonicalizeCalls()).isEqualTo(1);
    }

    @Test
    void permanentDeletionRecoversProvisioningMappingWithoutPartitionMetadata()
            throws Exception {
        int partition = 0;
        String logName = paths.compactedReaderName(stream, partition);
        String configPath = paths.streamConfigPath(stream);
        String metadataPath = paths.partitionMetadataPath(stream, partition);
        String incarnation = "crashed-provisioner";
        String ownerToken = "provisioning-owner";
        long streamId = 938L;
        StreamIdMappingOwner owner = new StreamIdMappingOwner(
            incarnation, ownerToken, 1L);
        VersionedRecord config = mockVersionedRecord(configPath,
            ("{\"partitions\":1,\"properties\":{},"
                + "\"_incarnationId\":\"" + incarnation + "\","
                + "\"_ownerToken\":\"" + ownerToken + "\","
                + "\"_ownerGeneration\":1,"
                + "\"_creationKind\":\"EXTERNAL\","
                + "\"_provisioning\":true,"
                + "\"_provisioningState\":\"PROVISIONING\"}")
                .getBytes(StandardCharsets.UTF_8));
        VersionedRecord metadata = mockVersionedRecord(metadataPath, null);
        FencedMappingHarness mappings = new FencedMappingHarness(
            logName, streamId, owner, 1_000L);
        IndexedStreamCatalog catalog = fencedCatalog(mappings.storageApi());

        catalog.permanentlyDeleteExternalStream(stream).join();

        JsonNode permanent = json(config);
        assertThat(permanent.path("_provisioningState").asText())
            .isEqualTo("PERMANENTLY_DELETED");
        assertThat(permanent.path("_ownerToken").asText()).isEqualTo(ownerToken);
        assertThat(permanent.path("_ownerGeneration").asLong()).isEqualTo(1L);
        assertThat(permanent.path("_metadataSourceOwnerToken").asText())
            .isEqualTo(ownerToken);
        assertThat(permanent.path("_metadataSourceGeneration").asLong()).isEqualTo(1L);

        catalog.deleteExternalPartition(stream, partition).join();

        LogMetadata tombstone = metadata(metadata);
        assertThat(tombstone.deleted()).isTrue();
        assertThat(tombstone.streamId()).isEqualTo(streamId);
        assertThat(tombstone.registrationIncarnationId()).isEqualTo(incarnation);
        assertThat(tombstone.registrationOwnerToken()).isEqualTo(ownerToken);
        assertThat(tombstone.registrationOwnerGeneration()).isEqualTo(1L);
        assertThat(tombstone.retiredStreamIds()).isEmpty();
        assertThat(tombstone.purgeableRetiredStreamIds()).isEmpty();
        assertThat(tombstone.retiredStreamMappings()).isEmpty();
        assertThat(tombstone.retiredMappingKeys()).isEmpty();
        assertThat(mappings.activeStreamId()).isEmpty();
        assertThat(mappings.fenceCalls()).isEqualTo(1);
        verify(logStorage).deleteLog(LogId.of(streamId));
    }

    @Test
    void missingDurableFenceCapabilityFailsBeforeLifecycleSideEffects()
            throws Exception {
        int partition = 0;
        String configPath = paths.streamConfigPath(stream);
        String metadataPath = paths.partitionMetadataPath(stream, partition);
        String incarnation = "unsupported-fence";
        String ownerToken = "unsupported-owner";
        long streamId = 939L;
        VersionedRecord config = mockVersionedRecord(
            configPath, activeExternalConfigBytes(1, incarnation, ownerToken));
        VersionedRecord metadata = mockVersionedRecord(
            metadataPath, metadataBytes(
                metadataPath, streamId, incarnation, ownerToken, 1L, true,
                Set.of(streamId), Set.of(streamId), Set.of(), Set.of("mapping-key")));
        StorageApi unsupported = mock(StorageApi.class);
        when(unsupported.supportsConditionalStreamIdMappingDeletion()).thenReturn(true);
        when(unsupported.supportsFencedStreamIdMappings()).thenReturn(false);
        IndexedStreamCatalog catalog = fencedCatalog(unsupported);

        assertThatThrownBy(() ->
                catalog.permanentlyDeleteExternalStream(stream).join())
            .isInstanceOf(CompletionException.class)
            .hasCauseInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(() -> catalog.registerExternalPartition(
                stream, partition, streamId + 1, Map.of()).join())
            .isInstanceOf(CompletionException.class)
            .hasCauseInstanceOf(UnsupportedOperationException.class);

        assertThat(config.successfulPuts()).isZero();
        assertThat(metadata.successfulPuts()).isZero();
        verify(logStorage, never()).deleteLog(any());
        verify(unsupported, never()).fenceStreamIdMappingState(
            anyString(), anyLong(), any(StreamIdMappingOwner.class));
        verify(unsupported, never()).bindStreamIdMapping(
            anyString(), anyLong(), any(StreamIdMappingOwner.class), any());
    }

    @Test
    void nativeOwnedStreamIsTerminallyFencedAtExternalRegistrationBoundary()
            throws Exception {
        int partition = 0;
        String logName = paths.compactedReaderName(stream, partition);
        String configPath = paths.streamConfigPath(stream);
        String metadataPath = paths.partitionMetadataPath(stream, partition);
        String incarnation = "native-owner-conflict";
        String ownerToken = "native-owner";
        long streamId = 941L;
        VersionedRecord config = mockVersionedRecord(configPath,
            ("{\"partitions\":1,\"properties\":{},"
                + "\"_incarnationId\":\"" + incarnation + "\","
                + "\"_ownerToken\":\"" + ownerToken + "\","
                + "\"_ownerGeneration\":1,"
                + "\"_creationKind\":\"NATIVE_CREATE\"}")
                .getBytes(StandardCharsets.UTF_8));
        VersionedRecord metadata = mockVersionedRecord(metadataPath, null);
        FencedMappingHarness mappings = new FencedMappingHarness(
            logName, streamId,
            new StreamIdMappingOwner(incarnation, ownerToken, 1L), 1_000L);
        IndexedStreamCatalog catalog = fencedCatalog(mappings.storageApi());

        assertThatThrownBy(() -> catalog.registerExternalPartition(
                stream, partition, streamId, Map.of()).join())
            .isInstanceOf(CompletionException.class)
            .hasCauseInstanceOf(PartitionLifecycleFencedException.class)
            .hasMessageContaining("different catalog lifecycle");

        assertThat(config.successfulPuts()).isZero();
        assertThat(metadata.currentResult()).isNull();
        assertThat(mappings.bindCalls()).isZero();
        assertThat(mappings.fenceCalls()).isZero();
    }

    @Test
    void nativeOwnedStreamIsTerminallyFencedAtExternalOpenBoundary()
            throws Exception {
        int partition = 0;
        String logName = paths.compactedReaderName(stream, partition);
        String configPath = paths.streamConfigPath(stream);
        String metadataPath = paths.partitionMetadataPath(stream, partition);
        String incarnation = "native-open-owner-conflict";
        String ownerToken = "native-open-owner";
        long streamId = 944L;
        VersionedRecord config = mockVersionedRecord(configPath,
            ("{\"partitions\":1,\"properties\":{},"
                + "\"_incarnationId\":\"" + incarnation + "\","
                + "\"_ownerToken\":\"" + ownerToken + "\","
                + "\"_ownerGeneration\":1,"
                + "\"_creationKind\":\"NATIVE_CREATE\"}")
                .getBytes(StandardCharsets.UTF_8));
        VersionedRecord metadata = mockVersionedRecord(metadataPath, null);
        FencedMappingHarness mappings = new FencedMappingHarness(
            logName, streamId,
            new StreamIdMappingOwner(incarnation, ownerToken, 1L), 1_000L);
        IndexedStreamCatalog catalog = fencedCatalog(mappings.storageApi());

        assertThatThrownBy(() -> catalog.openExternalPartition(
                stream, partition, Map.of()).join())
            .isInstanceOf(CompletionException.class)
            .hasCauseInstanceOf(PartitionLifecycleFencedException.class)
            .hasMessageContaining("different catalog lifecycle");

        assertThat(config.successfulPuts()).isZero();
        assertThat(metadata.currentResult()).isNull();
        assertThat(mappings.bindCalls()).isZero();
        assertThat(mappings.fenceCalls()).isZero();
        verify(readerFactory, never()).open(anyString());
    }

    @Test
    void legacyActivePartitionDeletionFencesExactMetadataStreamId()
            throws Exception {
        int partition = 0;
        String logName = paths.compactedReaderName(stream, partition);
        String configPath = paths.streamConfigPath(stream);
        String metadataPath = paths.partitionMetadataPath(stream, partition);
        long streamId = 945L;
        mockVersionedRecord(configPath, activeLegacyConfigBytes(1));
        VersionedRecord metadata = mockVersionedRecord(
            metadataPath, metadataBytes(metadataPath, streamId, null, null, false));
        FencedMappingHarness mappings = new FencedMappingHarness(
            logName, streamId, StreamIdMappingOwner.legacy(), 1_000L);
        IndexedStreamCatalog catalog = fencedCatalog(mappings.storageApi());

        catalog.deleteExternalPartition(stream, partition).join();

        LogMetadata deleted = metadata(metadata);
        assertThat(deleted.deleted()).isTrue();
        assertThat(deleted.streamId()).isEqualTo(streamId);
        assertThat(deleted.retiredStreamIds()).isEmpty();
        assertThat(deleted.retiredStreamMappings()).isEmpty();
        assertThat(deleted.retiredMappingKeys()).isEmpty();
        assertThat(mappings.activeStreamId()).isEmpty();
        assertThat(mappings.fenceCalls()).isEqualTo(1);
        verify(logStorage).deleteLog(LogId.of(streamId));
    }

    @Test
    void legacyKeysOnlyJournalUsesExactMetadataStreamId()
            throws Exception {
        int partition = 0;
        String logName = paths.compactedReaderName(stream, partition);
        String configPath = paths.streamConfigPath(stream);
        String metadataPath = paths.partitionMetadataPath(stream, partition);
        long streamId = 946L;
        mockVersionedRecord(configPath, activeLegacyConfigBytes(1));
        VersionedRecord metadata = mockVersionedRecord(
            metadataPath, metadataBytes(
                metadataPath, streamId, null, null, null, true,
                Set.of(streamId), Set.of(streamId), Set.of(), Set.of(logName)));
        FencedMappingHarness mappings = new FencedMappingHarness(
            logName, streamId, StreamIdMappingOwner.legacy(), 1_000L);
        IndexedStreamCatalog catalog = fencedCatalog(mappings.storageApi());

        catalog.deleteExternalPartition(stream, partition).join();

        LogMetadata deleted = metadata(metadata);
        assertThat(deleted.deleted()).isTrue();
        assertThat(deleted.streamId()).isEqualTo(streamId);
        assertThat(deleted.retiredStreamIds()).isEmpty();
        assertThat(deleted.purgeableRetiredStreamIds()).isEmpty();
        assertThat(deleted.retiredStreamMappings()).isEmpty();
        assertThat(deleted.retiredMappingKeys()).isEmpty();
        assertThat(mappings.fenceCalls()).isEqualTo(1);
        verify(logStorage).deleteLog(LogId.of(streamId));
    }

    @Test
    void unknownLegacyMappingFailsClosedBeforeCleanupSideEffects()
            throws Exception {
        int partition = 0;
        String logName = paths.compactedReaderName(stream, partition);
        String configPath = paths.streamConfigPath(stream);
        String metadataPath = paths.partitionMetadataPath(stream, partition);
        mockVersionedRecord(configPath, activeLegacyConfigBytes(1));
        VersionedRecord metadata = mockVersionedRecord(
            metadataPath, metadataBytes(
                metadataPath, -1L, null, null, null, true,
                Set.of(), Set.of(), Set.of(), Set.of(logName)));
        FencedMappingHarness mappings = new FencedMappingHarness(logName, 1_000L);
        IndexedStreamCatalog catalog = fencedCatalog(mappings.storageApi());

        assertThatThrownBy(() ->
                catalog.deleteExternalPartition(stream, partition).join())
            .isInstanceOf(CompletionException.class)
            .hasCauseInstanceOf(PartitionLifecycleFencedException.class)
            .hasMessageContaining("without an exact physical stream ID");

        assertThat(metadata(metadata).retiredMappingKeys()).containsExactly(logName);
        assertThat(mappings.fenceCalls()).isZero();
        verify(logStorage, never()).deleteLog(any());
    }

    @Test
    void durableMappingConflictIsTerminallyFencedAtExternalRegistrationBoundary()
            throws Exception {
        int partition = 0;
        String logName = paths.compactedReaderName(stream, partition);
        String configPath = paths.streamConfigPath(stream);
        String metadataPath = paths.partitionMetadataPath(stream, partition);
        String incarnation = "mapping-owner-conflict";
        String ownerToken = "registration-owner";
        long requestedStreamId = 943L;
        long existingStreamId = 942L;
        mockVersionedRecord(
            configPath, activeExternalConfigBytes(1, incarnation, ownerToken));
        VersionedRecord metadata = mockVersionedRecord(metadataPath, null);
        FencedMappingHarness mappings = new FencedMappingHarness(
            logName, existingStreamId,
            new StreamIdMappingOwner(incarnation, "different-owner", 1L), 1_000L);
        IndexedStreamCatalog catalog = fencedCatalog(mappings.storageApi());

        assertThatThrownBy(() -> catalog.registerExternalPartition(
                stream, partition, requestedStreamId, Map.of()).join())
            .isInstanceOf(CompletionException.class)
            .hasCauseInstanceOf(PartitionLifecycleFencedException.class)
            .hasMessageContaining("different durable owner or ID");

        assertThat(metadata.currentResult()).isNull();
        assertThat(mappings.activeStreamId()).contains(existingStreamId);
        assertThat(mappings.bindCalls()).isEqualTo(1);
        assertThat(mappings.fenceCalls()).isZero();
        verify(logStorage, never()).deleteLog(any());
    }

    @Test
    void preservedActiveRetiredIdFencesRegistrationWithoutDeletingReplacement()
            throws Exception {
        int partition = 0;
        String logName = paths.compactedReaderName(stream, partition);
        String configPath = paths.streamConfigPath(stream);
        String metadataPath = paths.partitionMetadataPath(stream, partition);
        String incarnation = "preserved-active-retired";
        StreamIdMappingOwner retiredOwner = new StreamIdMappingOwner(
            incarnation, "retired-owner", 1L);
        StreamIdMappingOwner replacementOwner = new StreamIdMappingOwner(
            incarnation, "replacement-owner", 2L);
        long replacementStreamId = 940L;
        mockVersionedRecord(configPath, ("{\"partitions\":1,\"properties\":{},"
            + "\"_incarnationId\":\"" + incarnation + "\","
            + "\"_ownerToken\":\"replacement-owner\","
            + "\"_ownerGeneration\":2,"
            + "\"_metadataSourceOwnerToken\":\"retired-owner\","
            + "\"_metadataSourceGeneration\":1,"
            + "\"_creationKind\":\"EXTERNAL\"}")
            .getBytes(StandardCharsets.UTF_8));
        VersionedRecord metadata = mockVersionedRecord(
            metadataPath, LOG_METADATA_SERDE.serialize(metadataPath, new LogMetadata(
                replacementStreamId, Map.of(), OptionalLong.empty(),
                retiredOwner.incarnationId(), retiredOwner.ownerToken(),
                retiredOwner.ownerGeneration(), true, Set.of(replacementStreamId),
                Set.of(replacementStreamId),
                Set.of(new LogMetadata.RetiredStreamMapping(
                    replacementStreamId, logName, true)), Set.of(logName))));
        FencedMappingHarness mappings = new FencedMappingHarness(
            logName, replacementStreamId, replacementOwner, 1_000L);
        IndexedStreamCatalog catalog = fencedCatalog(mappings.storageApi());

        assertThatThrownBy(() -> catalog.registerExternalPartition(
                stream, partition, replacementStreamId, Map.of()).join())
            .isInstanceOf(CompletionException.class)
            .hasCauseInstanceOf(PartitionLifecycleFencedException.class)
            .hasMessageContaining("retired cleanup journal contains durable active stream ID");

        LogMetadata retained = metadata(metadata);
        assertThat(retained.deleted()).isTrue();
        assertThat(retained.retiredStreamIds()).containsExactly(replacementStreamId);
        assertThat(retained.purgeableRetiredStreamIds()).containsExactly(replacementStreamId);
        assertThat(retained.retiredStreamMappings()).hasSize(1);
        assertThat(mappings.activeStreamId()).contains(replacementStreamId);
        verify(logStorage, never()).deleteLog(LogId.of(replacementStreamId));
    }

    @Test
    void legacyDeletedPartitionRequiresUnregisterBeforeModernRegistration()
            throws Exception {
        int partition = 0;
        String logName = paths.compactedReaderName(stream, partition);
        String configPath = paths.streamConfigPath(stream);
        String metadataPath = paths.partitionMetadataPath(stream, partition);
        long retiredStreamId = 950L;
        long replacementStreamId = 951L;
        VersionedRecord config = mockVersionedRecord(
            configPath, activeLegacyConfigBytes(1));
        VersionedRecord metadata = mockVersionedRecord(
            metadataPath, LOG_METADATA_SERDE.serialize(metadataPath, new LogMetadata(
                retiredStreamId, Map.of(), OptionalLong.empty(),
                null, null, null, true, Set.of(retiredStreamId),
                Set.of(), Set.of(new LogMetadata.RetiredStreamMapping(
                    retiredStreamId, logName)), Set.of(logName))));
        FencedMappingHarness mappings = new FencedMappingHarness(
            logName, retiredStreamId, StreamIdMappingOwner.legacy(), 1_000L);
        IndexedStreamCatalog catalog = fencedCatalog(mappings.storageApi());

        assertThatThrownBy(() -> catalog.registerExternalPartition(
                stream, partition, replacementStreamId, Map.of()).join())
            .isInstanceOf(CompletionException.class)
            .hasCauseInstanceOf(PartitionLifecycleFencedException.class)
            .hasMessageContaining("requires unregistering the external stream");

        JsonNode stillLegacy = json(config);
        assertThat(stillLegacy.has("_incarnationId")).isFalse();
        assertThat(stillLegacy.path("_provisioning").asBoolean(false)).isFalse();
        assertThat(config.successfulPuts()).isZero();
        assertThat(mappings.fenceCalls()).isZero();

        catalog.unregisterExternalStream(stream).join();
        assertThat(json(config).path("_provisioningState").asText())
            .isEqualTo("UNREGISTERED");

        catalog.registerExternalPartition(
            stream, partition, replacementStreamId, Map.of()).join();

        JsonNode active = json(config);
        LogMetadata registered = metadata(metadata);
        assertThat(active.path("_provisioning").asBoolean(false)).isFalse();
        assertThat(active.path("_ownerGeneration").asLong()).isEqualTo(1L);
        assertThat(active.path("_metadataSourceGeneration").asLong()).isEqualTo(-1L);
        assertThat(registered.deleted()).isFalse();
        assertThat(registered.streamId()).isEqualTo(replacementStreamId);
        assertThat(registered.registrationOwnerGeneration()).isEqualTo(1L);
        assertThat(registered.retiredStreamIds()).isEmpty();
        assertThat(mappings.activeStreamId()).contains(replacementStreamId);
        verify(logStorage, never()).deleteLog(LogId.of(retiredStreamId));
    }

    private IndexedStreamCatalog fencedCatalog(StorageApi storageApi) {
        return new IndexedStreamCatalog(
            oxiaClient, paths, logStorage, logFactory, null, storageApi,
            readerFactory, null, List.of());
    }

    private IndexedStreamCatalog catalog(
            Function<Optional<String>, CompletableFuture<Long>> generator,
            Function<String, CompletableFuture<Long>> lookup,
            Function<String, CompletableFuture<Void>> mappingDeleter) {
        return catalog(generator,
            key -> generator.apply(Optional.of(key)),
            lookup, (key, ignored) -> mappingDeleter.apply(key));
    }

    private IndexedStreamCatalog catalog(
            Function<Optional<String>, CompletableFuture<Long>> generator,
            Function<String, CompletableFuture<Long>> keyedGenerator,
            Function<String, CompletableFuture<Long>> lookup,
            BiFunction<String, Long, CompletableFuture<Void>> mappingDeleter) {
        return catalog(generator, keyedGenerator, lookup, mappingDeleter, true);
    }

    private IndexedStreamCatalog catalog(
            Function<Optional<String>, CompletableFuture<Long>> generator,
            Function<String, CompletableFuture<Long>> keyedGenerator,
            Function<String, CompletableFuture<Long>> lookup,
            BiFunction<String, Long, CompletableFuture<Void>> mappingDeleter,
            boolean createdKeyedMapping) {
        Function<Optional<String>, CompletableFuture<Long>> combinedGenerator = key ->
            key.isPresent() ? keyedGenerator.apply(key.orElseThrow()) : generator.apply(key);
        StorageApi storageApi = lifecycleAwareStorage(
            combinedGenerator, keyedGenerator, lookup, mappingDeleter,
            createdKeyedMapping);
        return new IndexedStreamCatalog(
            oxiaClient, paths, logStorage, logFactory, null, storageApi,
            readerFactory, null, List.of());
    }

    private StorageApi lifecycleAwareStorage(
            Function<Optional<String>, CompletableFuture<Long>> generator,
            Function<String, CompletableFuture<Long>> keyedGenerator,
            Function<String, CompletableFuture<Long>> lookup,
            BiFunction<String, Long, CompletableFuture<Void>> mappingDeleter,
            boolean createdKeyedMapping) {
        StorageApi storageApi = mock(StorageApi.class);
        when(storageApi.supportsConditionalStreamIdMappingDeletion()).thenReturn(true);
        when(storageApi.supportsFencedStreamIdMappings()).thenReturn(true);
        when(storageApi.generateStreamId(any()))
            .thenAnswer(invocation -> generator.apply(invocation.getArgument(0)));
        when(storageApi.allocateStreamId(any()))
            .thenAnswer(invocation -> generator
                .apply(invocation.getArgument(0))
                .thenApply(streamId ->
                    new StreamIdAllocation(streamId, createdKeyedMapping)));
        when(storageApi.allocateStreamId(
                anyString(), any(StreamIdMappingOwner.class), any()))
            .thenAnswer(invocation -> keyedGenerator
                .apply(invocation.getArgument(0, String.class))
                .thenApply(streamId ->
                    new StreamIdAllocation(streamId, createdKeyedMapping)));
        when(storageApi.getStreamIdByKey(anyString()))
            .thenAnswer(invocation -> lookup.apply(
                invocation.getArgument(0, String.class)));
        when(storageApi.bindStreamIdMapping(
                anyString(), anyLong(), any(StreamIdMappingOwner.class), any()))
            .thenAnswer(invocation -> bindLegacyTestMapping(
                lookup,
                invocation.getArgument(0, String.class),
                invocation.getArgument(1, Long.class)));
        when(storageApi.fenceStreamIdMappingState(
                anyString(), anyLong(), any(StreamIdMappingOwner.class)))
            .thenAnswer(invocation -> fenceLegacyTestMapping(
                lookup, mappingDeleter,
                invocation.getArgument(0, String.class),
                invocation.getArgument(1, Long.class),
                invocation.getArgument(2, StreamIdMappingOwner.class)));
        when(storageApi.canonicalizeStreamIdMappingFence(
                anyString(), any(StreamIdMappingFence.class),
                any(StreamIdMappingFence.class)))
            .thenReturn(CompletableFuture.completedFuture(null));
        when(storageApi.deleteStreamIdMapping(anyString()))
            .thenAnswer(invocation -> mappingDeleter.apply(
                invocation.getArgument(0, String.class), -1L));
        when(storageApi.deleteStreamIdMapping(anyString(), anyLong()))
            .thenAnswer(invocation -> mappingDeleter.apply(
                invocation.getArgument(0, String.class),
                invocation.getArgument(1, Long.class)));
        return storageApi;
    }

    private CompletableFuture<Void> bindLegacyTestMapping(
            Function<String, CompletableFuture<Long>> lookup,
            String key, long streamId) {
        return lookup.apply(key).handle((mappedStreamId, failure) -> {
            if (failure == null) {
                if (mappedStreamId == null) {
                    return CompletableFuture.<Void>completedFuture(null);
                }
                if (mappedStreamId.longValue() == streamId) {
                    return CompletableFuture.<Void>completedFuture(null);
                }
                return CompletableFuture.<Void>failedFuture(
                    new StreamIdMappingConflictException(
                        key, new ActiveStreamIdMapping(
                            mappedStreamId, StreamIdMappingOwner.legacy())));
            }
            Throwable cause = unwrapCompletionFailure(failure);
            if (cause instanceof NoSuchKeyException) {
                return CompletableFuture.<Void>completedFuture(null);
            }
            return CompletableFuture.<Void>failedFuture(cause);
        }).thenCompose(Function.identity());
    }

    private CompletableFuture<StreamIdMappingFenceResult> fenceLegacyTestMapping(
            Function<String, CompletableFuture<Long>> lookup,
            BiFunction<String, Long, CompletableFuture<Void>> mappingDeleter,
            String key, long expectedStreamId, StreamIdMappingOwner expectedOwner) {
        return lookup.apply(key).handle((mappedStreamId, failure) -> {
            if (failure != null) {
                Throwable cause = unwrapCompletionFailure(failure);
                if (cause instanceof NoSuchKeyException) {
                    return CompletableFuture.<StreamIdMappingFenceResult>completedFuture(
                        new StreamIdMappingFenceResult.Fenced(
                            new StreamIdMappingFence(-1L, expectedOwner)));
                }
                return CompletableFuture.<StreamIdMappingFenceResult>failedFuture(cause);
            }
            if (mappedStreamId == null) {
                return CompletableFuture.<StreamIdMappingFenceResult>completedFuture(
                    new StreamIdMappingFenceResult.Fenced(
                        new StreamIdMappingFence(-1L, expectedOwner)));
            }
            if (expectedStreamId != -1L
                    && mappedStreamId.longValue() != expectedStreamId) {
                return CompletableFuture.<StreamIdMappingFenceResult>completedFuture(
                    new StreamIdMappingFenceResult.PreservedActive(
                        new ActiveStreamIdMapping(
                            mappedStreamId, StreamIdMappingOwner.legacy())));
            }
            CompletableFuture<Void> deletion = mappingDeleter.apply(key, mappedStreamId);
            return (deletion == null ? CompletableFuture.<Void>completedFuture(null) : deletion)
                .thenApply(ignored -> (StreamIdMappingFenceResult)
                    new StreamIdMappingFenceResult.Fenced(
                        new StreamIdMappingFence(mappedStreamId, expectedOwner)));
        }).thenCompose(Function.identity());
    }

    private static Throwable unwrapCompletionFailure(Throwable failure) {
        Throwable current = failure;
        while (current instanceof CompletionException && current.getCause() != null) {
            current = current.getCause();
        }
        return current;
    }

    private VersionedRecord mockVersionedRecord(String path, byte[] initialValue) {
        VersionedRecord record = new VersionedRecord(path, initialValue);
        when(oxiaClient.get(path)).thenAnswer(ignored -> record.read());
        when(oxiaClient.put(eq(path), any(byte[].class), any()))
            .thenAnswer(invocation -> record.put(
                invocation.getArgument(1, byte[].class),
                invocation.<Set<PutOption>>getArgument(2)));
        return record;
    }

    private JsonNode json(VersionedRecord record) throws Exception {
        return MAPPER.readTree(record.currentValue());
    }

    private LogMetadata metadata(VersionedRecord record) throws Exception {
        return LOG_METADATA_SERDE.deserialize(record.path(), record.currentValue());
    }

    private static void clearExpectedMapping(
            AtomicReference<Long> mapping, Long expectedStreamId) {
        Long current = mapping.get();
        if (current != null && current.longValue() == expectedStreamId.longValue()) {
            mapping.compareAndSet(current, null);
        }
    }

    private GetResult activeLegacyConfig(String path, Version version) {
        return new GetResult(path, activeLegacyConfigBytes(10), version);
    }

    private byte[] activeLegacyConfigBytes(int partitions) {
        return ("{\"partitions\":" + partitions + ",\"properties\":{}}")
            .getBytes(StandardCharsets.UTF_8);
    }

    private byte[] activeExternalConfigBytes(
            int partitions, String incarnationId, String ownerToken) {
        return activeExternalConfigBytes(partitions, incarnationId, ownerToken, 1L);
    }

    private byte[] activeExternalConfigBytes(
            int partitions, String incarnationId, String ownerToken, long ownerGeneration) {
        return ("{\"partitions\":" + partitions + ",\"properties\":{},"
            + "\"_incarnationId\":\"" + incarnationId + "\","
            + "\"_ownerToken\":\"" + ownerToken + "\","
            + "\"_ownerGeneration\":" + ownerGeneration + ","
            + "\"_creationKind\":\"EXTERNAL\"}")
            .getBytes(StandardCharsets.UTF_8);
    }

    private byte[] droppedExternalConfigBytes(
            int partitions, String incarnationId, String ownerToken,
            long ownerGeneration) {
        return ("{\"partitions\":" + partitions + ",\"properties\":{},"
            + "\"_incarnationId\":\"" + incarnationId + "\","
            + "\"_ownerToken\":\"" + ownerToken + "\","
            + "\"_ownerGeneration\":" + ownerGeneration + ","
            + "\"_metadataSourceGeneration\":" + (ownerGeneration - 1) + ","
            + "\"_creationKind\":\"EXTERNAL\","
            + "\"_provisioning\":true,"
            + "\"_provisioningState\":\"DROPPED\"}")
            .getBytes(StandardCharsets.UTF_8);
    }

    private GetResult permanentDeletion(String path, Version version) {
        return new GetResult(path,
            "{\"_externalStreamPermanentlyDeleted\":true}"
                .getBytes(StandardCharsets.UTF_8), version);
    }

    private byte[] permanentDeletionOfLegacyConfigBytes(int partitions) {
        return ("{\"partitions\":" + partitions + ",\"properties\":{},"
            + "\"_incarnationId\":\"legacy-deletion-incarnation\","
            + "\"_ownerToken\":\"legacy-deletion-owner\","
            + "\"_ownerGeneration\":1,"
            + "\"_metadataSourceGeneration\":-1,"
            + "\"_creationKind\":\"EXTERNAL\","
            + "\"_provisioning\":true,"
            + "\"_provisioningState\":\"PERMANENTLY_DELETED\","
            + "\"_externalStreamPermanentlyDeleted\":true}")
            .getBytes(StandardCharsets.UTF_8);
    }

    private byte[] metadataBytes(
            String path, long streamId, String incarnationId,
            String ownerToken, boolean deleted) {
        return metadataBytes(path, streamId, Map.of(), OptionalLong.empty(),
            incarnationId, ownerToken, incarnationId == null ? null : 1L, deleted);
    }

    private byte[] metadataBytes(
            String path, long streamId, String incarnationId,
            String ownerToken, Long ownerGeneration, boolean deleted) {
        return metadataBytes(path, streamId, Map.of(), OptionalLong.empty(),
            incarnationId, ownerToken, ownerGeneration, deleted);
    }

    private byte[] metadataBytes(
            String path, long streamId, Map<String, String> properties,
            OptionalLong terminatedOffset, String incarnationId,
            String ownerToken, boolean deleted) {
        return metadataBytes(path, streamId, properties, terminatedOffset,
            incarnationId, ownerToken, incarnationId == null ? null : 1L, deleted);
    }

    private byte[] metadataBytes(
            String path, long streamId, Map<String, String> properties,
            OptionalLong terminatedOffset, String incarnationId,
            String ownerToken, Long ownerGeneration, boolean deleted) {
        return metadataBytes(path, streamId, properties, terminatedOffset,
            incarnationId, ownerToken, ownerGeneration, deleted, Set.of());
    }

    private byte[] metadataBytes(
            String path, long streamId, Map<String, String> properties,
            OptionalLong terminatedOffset, String incarnationId,
            String ownerToken, Long ownerGeneration, boolean deleted,
            Set<Long> retiredStreamIds) {
        try {
            return LOG_METADATA_SERDE.serialize(path, new LogMetadata(
                streamId, properties, terminatedOffset,
                incarnationId, ownerToken, ownerGeneration, deleted,
                retiredStreamIds));
        } catch (Exception e) {
            throw new AssertionError(e);
        }
    }

    private byte[] metadataBytes(
            String path, long streamId, String incarnationId,
            String ownerToken, Long ownerGeneration, boolean deleted,
            Set<Long> retiredStreamIds, Set<Long> purgeableRetiredStreamIds,
            Set<LogMetadata.RetiredStreamMapping> retiredStreamMappings,
            Set<String> retiredMappingKeys) {
        try {
            return LOG_METADATA_SERDE.serialize(path, new LogMetadata(
                streamId, Map.of(), OptionalLong.empty(),
                incarnationId, ownerToken, ownerGeneration, deleted,
                retiredStreamIds, purgeableRetiredStreamIds,
                retiredStreamMappings, retiredMappingKeys));
        } catch (Exception e) {
            throw new AssertionError(e);
        }
    }

    private static Version version(long id) {
        return new Version(id, 0, 0, 0, Optional.empty(), Optional.empty());
    }

    private static final class FencedMappingHarness {

        private final StorageApi storageApi = mock(StorageApi.class);
        private final String mappingKey;
        private final AtomicLong nextStreamId;
        private final AtomicInteger fenceCalls = new AtomicInteger();
        private final AtomicInteger bindCalls = new AtomicInteger();
        private final AtomicInteger canonicalizeCalls = new AtomicInteger();
        private final AtomicReference<Throwable> nextCanonicalizeFailure =
            new AtomicReference<>();
        private Object mapping;

        private FencedMappingHarness(
                String mappingKey, long initialStreamId,
                StreamIdMappingOwner initialOwner, long nextStreamId) {
            this(mappingKey, nextStreamId);
            this.mapping = new ActiveMapping(initialStreamId, initialOwner);
        }

        private FencedMappingHarness(String mappingKey, long nextStreamId) {
            this.mappingKey = mappingKey;
            this.nextStreamId = new AtomicLong(nextStreamId);
            when(storageApi.supportsConditionalStreamIdMappingDeletion()).thenReturn(true);
            when(storageApi.supportsFencedStreamIdMappings()).thenReturn(true);
            when(storageApi.getStreamIdByKey(anyString()))
                .thenAnswer(ignored -> getStreamId());
            when(storageApi.allocateStreamId(
                    anyString(), any(StreamIdMappingOwner.class), any()))
                .thenAnswer(invocation -> allocate(
                    invocation.getArgument(0, String.class),
                    invocation.getArgument(1, StreamIdMappingOwner.class),
                    invocation.getArgument(2)));
            when(storageApi.bindStreamIdMapping(
                    anyString(), anyLong(), any(StreamIdMappingOwner.class), any()))
                .thenAnswer(invocation -> bind(
                    invocation.getArgument(0, String.class),
                    invocation.getArgument(1, Long.class),
                    invocation.getArgument(2, StreamIdMappingOwner.class),
                    invocation.getArgument(3)));
            when(storageApi.fenceStreamIdMappingState(
                    anyString(), anyLong(), any(StreamIdMappingOwner.class)))
                .thenAnswer(invocation -> fence(
                    invocation.getArgument(0, String.class),
                    invocation.getArgument(1, Long.class),
                    invocation.getArgument(2, StreamIdMappingOwner.class)));
            when(storageApi.canonicalizeStreamIdMappingFence(
                    anyString(), any(StreamIdMappingFence.class),
                    any(StreamIdMappingFence.class)))
                .thenAnswer(invocation -> canonicalize(
                    invocation.getArgument(0, String.class),
                    invocation.getArgument(1, StreamIdMappingFence.class),
                    invocation.getArgument(2, StreamIdMappingFence.class)));
            when(storageApi.deleteStreamIdMapping(anyString(), anyLong()))
                .thenReturn(CompletableFuture.completedFuture(null));
        }

        private StorageApi storageApi() {
            return storageApi;
        }

        private int fenceCalls() {
            return fenceCalls.get();
        }

        private int bindCalls() {
            return bindCalls.get();
        }

        private int canonicalizeCalls() {
            return canonicalizeCalls.get();
        }

        private synchronized void clearMapping() {
            mapping = null;
        }

        private synchronized void installActive(
                long streamId, StreamIdMappingOwner owner) {
            mapping = new ActiveMapping(streamId, owner);
        }

        private void failNextCanonicalize(Throwable failure) {
            nextCanonicalizeFailure.set(failure);
        }

        private synchronized Optional<Long> activeStreamId() {
            return mapping instanceof ActiveMapping active
                ? Optional.of(active.streamId()) : Optional.empty();
        }

        private synchronized CompletableFuture<Long> getStreamId() {
            if (mapping instanceof ActiveMapping active) {
                return CompletableFuture.completedFuture(active.streamId());
            }
            return CompletableFuture.failedFuture(
                new NoSuchKeyException(mappingKey));
        }

        private synchronized CompletableFuture<StreamIdAllocation> allocate(
                String key, StreamIdMappingOwner owner,
                Optional<StreamIdMappingFence> acknowledgedFence) {
            assertThat(key).isEqualTo(mappingKey);
            if (mapping instanceof ActiveMapping active) {
                if (active.owner().equals(owner)) {
                    return CompletableFuture.completedFuture(
                        new StreamIdAllocation(active.streamId(), false));
                }
                return CompletableFuture.failedFuture(
                    new StreamIdMappingConflictException(
                        key, new ActiveStreamIdMapping(active.streamId(), active.owner())));
            }
            if (mapping == null && acknowledgedFence.isEmpty()) {
                long allocated = nextStreamId.getAndIncrement();
                mapping = new ActiveMapping(allocated, owner);
                return CompletableFuture.completedFuture(
                    new StreamIdAllocation(allocated, true));
            }
            if (mapping instanceof StreamIdMappingFence fence
                    && acknowledgedFence.equals(Optional.of(fence))) {
                long allocated = nextStreamId.getAndIncrement();
                mapping = new ActiveMapping(allocated, owner);
                return CompletableFuture.completedFuture(
                    new StreamIdAllocation(allocated, true));
            }
            return CompletableFuture.failedFuture(
                new IllegalStateException("mapping fence was not acknowledged"));
        }

        private synchronized CompletableFuture<Void> bind(
                String key, long streamId, StreamIdMappingOwner owner,
                Optional<StreamIdMappingFence> acknowledgedFence) {
            assertThat(key).isEqualTo(mappingKey);
            bindCalls.incrementAndGet();
            if (mapping instanceof ActiveMapping active) {
                if (active.streamId() == streamId && active.owner().equals(owner)) {
                    return CompletableFuture.completedFuture(null);
                }
                return CompletableFuture.failedFuture(
                    new StreamIdMappingConflictException(
                        key, new ActiveStreamIdMapping(active.streamId(), active.owner())));
            }
            if (mapping instanceof StreamIdMappingFence fence
                    && acknowledgedFence.equals(Optional.of(fence))) {
                mapping = new ActiveMapping(streamId, owner);
                return CompletableFuture.completedFuture(null);
            }
            return CompletableFuture.failedFuture(
                new IllegalStateException("mapping fence was not acknowledged"));
        }

        private synchronized CompletableFuture<StreamIdMappingFenceResult> fence(
                String key, long expectedStreamId, StreamIdMappingOwner expectedOwner) {
            assertThat(key).isEqualTo(mappingKey);
            if (expectedOwner.isLegacy() && expectedStreamId == -1L) {
                return CompletableFuture.failedFuture(new IllegalArgumentException(
                    "Legacy mapping owner requires an exact stream ID"));
            }
            fenceCalls.incrementAndGet();
            if (mapping == null) {
                StreamIdMappingFence fence = new StreamIdMappingFence(-1L, expectedOwner);
                mapping = fence;
                return CompletableFuture.completedFuture(
                    new StreamIdMappingFenceResult.Fenced(fence));
            }
            if (mapping instanceof StreamIdMappingFence fence) {
                return CompletableFuture.completedFuture(
                    new StreamIdMappingFenceResult.Fenced(fence));
            }
            ActiveMapping active = (ActiveMapping) mapping;
            if (!active.owner().equals(expectedOwner)
                    || expectedStreamId >= 0 && active.streamId() != expectedStreamId) {
                return CompletableFuture.completedFuture(
                    new StreamIdMappingFenceResult.PreservedActive(
                        new ActiveStreamIdMapping(active.streamId(), active.owner())));
            }
            StreamIdMappingFence fence = new StreamIdMappingFence(
                active.streamId(), active.owner());
            mapping = fence;
            return CompletableFuture.completedFuture(
                new StreamIdMappingFenceResult.Fenced(fence));
        }

        private synchronized CompletableFuture<Void> canonicalize(
                String key, StreamIdMappingFence expected,
                StreamIdMappingFence canonical) {
            assertThat(key).isEqualTo(mappingKey);
            canonicalizeCalls.incrementAndGet();
            Throwable failure = nextCanonicalizeFailure.getAndSet(null);
            if (failure != null) {
                return CompletableFuture.failedFuture(failure);
            }
            if (!Objects.equals(mapping, expected)) {
                return CompletableFuture.failedFuture(
                    new IllegalStateException("mapping fence changed"));
            }
            mapping = canonical;
            return CompletableFuture.completedFuture(null);
        }

        private record ActiveMapping(long streamId, StreamIdMappingOwner owner) {
        }
    }

    private final class VersionedRecord {

        private final String path;
        private final AtomicReference<VersionedValue> state;
        private final AtomicLong nextVersion;
        private final AtomicReference<Throwable> nextPutFailure = new AtomicReference<>();
        private final AtomicReference<byte[]> nextConflictingValue = new AtomicReference<>();
        private final AtomicReference<byte[]> everyConflictingValue = new AtomicReference<>();
        private final AtomicInteger putAttempts = new AtomicInteger();
        private final AtomicInteger successfulPuts = new AtomicInteger();
        private volatile Function<GetResult, CompletableFuture<GetResult>> readInterceptor =
            CompletableFuture::completedFuture;
        private volatile Consumer<VersionedValue> successfulPutObserver = ignored -> { };

        private VersionedRecord(String path, byte[] initialValue) {
            this.path = path;
            this.state = new AtomicReference<>(initialValue == null ? null
                : new VersionedValue(initialValue.clone(), VERSION));
            this.nextVersion = new AtomicLong(initialValue == null ? 0L : VERSION.versionId());
        }

        private CompletableFuture<GetResult> read() {
            return readInterceptor.apply(currentResult());
        }

        private CompletableFuture<PutResult> put(byte[] value, Set<PutOption> options) {
            putAttempts.incrementAndGet();
            byte[] conflictingValue = nextConflictingValue.getAndSet(null);
            if (conflictingValue != null) {
                Version next = version(nextVersion.incrementAndGet());
                state.set(new VersionedValue(conflictingValue.clone(), next));
                if (options.contains(PutOption.IfRecordDoesNotExist)) {
                    return CompletableFuture.failedFuture(
                        new KeyAlreadyExistsException(path));
                }
                return CompletableFuture.failedFuture(
                    new UnexpectedVersionIdException(path, next.versionId()));
            }
            byte[] persistentConflict = everyConflictingValue.get();
            if (persistentConflict != null) {
                Version next = version(nextVersion.incrementAndGet());
                state.set(new VersionedValue(persistentConflict.clone(), next));
                if (options.contains(PutOption.IfRecordDoesNotExist)) {
                    return CompletableFuture.failedFuture(
                        new KeyAlreadyExistsException(path));
                }
                return CompletableFuture.failedFuture(
                    new UnexpectedVersionIdException(path, next.versionId()));
            }
            Throwable injectedFailure = nextPutFailure.getAndSet(null);
            if (injectedFailure != null) {
                return CompletableFuture.failedFuture(injectedFailure);
            }
            VersionedValue current = state.get();
            if (options.contains(PutOption.IfRecordDoesNotExist)) {
                if (current != null) {
                    return CompletableFuture.failedFuture(new KeyAlreadyExistsException(path));
                }
            } else if (current == null || !options.contains(
                    PutOption.IfVersionIdEquals(current.version().versionId()))) {
                return CompletableFuture.failedFuture(new UnexpectedVersionIdException(
                    path, current == null ? -1L : current.version().versionId()));
            }
            Version next = version(nextVersion.incrementAndGet());
            VersionedValue written = new VersionedValue(value.clone(), next);
            state.set(written);
            successfulPuts.incrementAndGet();
            successfulPutObserver.accept(written);
            return CompletableFuture.completedFuture(new PutResult(path, next));
        }

        private void failNextPut(Throwable failure) {
            nextPutFailure.set(failure);
        }

        private void conflictNextPutWith(byte[] value) {
            nextConflictingValue.set(value.clone());
        }

        private void conflictEveryPutWith(byte[] value) {
            everyConflictingValue.set(value.clone());
        }

        private void interceptReads(
                Function<GetResult, CompletableFuture<GetResult>> interceptor) {
            readInterceptor = interceptor;
        }

        private void afterSuccessfulPut(Consumer<VersionedValue> observer) {
            successfulPutObserver = observer;
        }

        private String path() {
            return path;
        }

        private byte[] currentValue() {
            VersionedValue current = state.get();
            assertThat(current).isNotNull();
            return current.value().clone();
        }

        private GetResult currentResult() {
            VersionedValue current = state.get();
            return current == null ? null
                : new GetResult(path, current.value().clone(), current.version());
        }

        private int putAttempts() {
            return putAttempts.get();
        }

        private int successfulPuts() {
            return successfulPuts.get();
        }
    }

    private record VersionedValue(byte[] value, Version version) {
    }
}
