/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.lakestream.impl;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
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
import io.lakestream.api.StreamIdentifier;
import io.lakestream.api.exception.AlreadyExistsException;
import io.lakestream.api.exception.NoSuchStreamException;
import io.lakestream.ursa.catalog.metadata.LogMetadata;
import io.lakestream.ursa.catalog.metadata.LogMetadataSerde;
import io.lakestream.ursa.lakestream.reader.CompactedObjectReader;
import io.lakestream.ursa.lakestream.reader.CompactedObjectReaderFactory;
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
        List<String> mappingDeletes = new ArrayList<>();
        IndexedStreamCatalog catalog = catalog(
            ignored -> CompletableFuture.completedFuture(41L),
            ignored -> CompletableFuture.completedFuture(41L),
            ignored -> CompletableFuture.completedFuture(41L),
            (key, streamId) -> {
                mappingDeletes.add(key + ":" + streamId);
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
        assertThat(retained.path("_ownerGeneration").asLong()).isEqualTo(2L);
        assertThat(catalog.streamExists(stream).join()).isFalse();
        LogMetadata tombstone = metadata(metadata);
        assertThat(tombstone.deleted()).isTrue();
        assertThat(tombstone.registrationOwnerGeneration()).isEqualTo(2L);
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
        List<String> mappingDeletes = new ArrayList<>();
        IndexedStreamCatalog catalog = catalog(
            ignored -> CompletableFuture.completedFuture(91L),
            ignored -> allocation,
            ignored -> CompletableFuture.completedFuture(91L),
            (key, streamId) -> {
                mappingDeletes.add(key + ":" + streamId);
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
        assertThat(tombstone.registrationOwnerGeneration()).isEqualTo(2L);
        verify(logStorage).deleteLog(LogId.of(90L));
        verify(logStorage).deleteLog(LogId.of(91L));
        assertThat(mappingDeletes).containsExactly(logName + ":91");
        verify(readerFactory, never()).open(anyString());
    }

    @Test
    void deletionFenceAfterIdGenerationPreservesPotentiallySharedAllocation() {
        int partition = 2;
        String logName = paths.compactedReaderName(stream, partition);
        String configPath = paths.streamConfigPath(stream);
        when(oxiaClient.get(configPath))
            .thenReturn(CompletableFuture.completedFuture(activeLegacyConfig(configPath, VERSION)))
            .thenReturn(CompletableFuture.completedFuture(
                permanentDeletion(configPath, VERSION)));
        CompletableFuture<Long> allocation = new CompletableFuture<>();
        @SuppressWarnings("unchecked")
        BiFunction<String, Long, CompletableFuture<Void>> mappingDeleter =
            mock(BiFunction.class);
        IndexedStreamCatalog catalog = catalog(
            ignored -> CompletableFuture.completedFuture(41L),
            ignored -> allocation,
            ignored -> CompletableFuture.completedFuture(41L), mappingDeleter);

        CompletableFuture<Log> opened =
            catalog.openExternalPartition(stream, partition, Map.of());
        allocation.complete(41L);

        assertThatThrownBy(opened::join)
            .isInstanceOf(CompletionException.class)
            .hasCauseInstanceOf(NoSuchStreamException.class);
        verify(logStorage, never()).deleteLog(LogId.of(41L));
        verify(mappingDeleter, never()).apply(logName, 41L);
        verify(oxiaClient, never()).get(paths.partitionMetadataPath(stream, partition));
    }

    @Test
    void metadataWriteIsRetainedWhenActiveConfigVersionChangesAfterWrite() throws Exception {
        int partition = 4;
        String logName = paths.compactedReaderName(stream, partition);
        String configPath = paths.streamConfigPath(stream);
        String metadataPath = paths.partitionMetadataPath(stream, partition);
        Version changedVersion = version(2);
        when(oxiaClient.get(configPath))
            .thenReturn(CompletableFuture.completedFuture(activeLegacyConfig(configPath, VERSION)))
            .thenReturn(CompletableFuture.completedFuture(activeLegacyConfig(configPath, VERSION)))
            .thenReturn(CompletableFuture.completedFuture(activeLegacyConfig(configPath, VERSION)))
            .thenReturn(CompletableFuture.completedFuture(activeLegacyConfig(configPath, VERSION)))
            .thenReturn(CompletableFuture.completedFuture(
                activeLegacyConfig(configPath, changedVersion)));
        VersionedRecord metadata = mockVersionedRecord(metadataPath, null);
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
        AtomicInteger configReadCount = new AtomicInteger();
        when(oxiaClient.get(configPath)).thenAnswer(ignored -> {
            if (configReadCount.incrementAndGet() == 4) {
                return firstOpenPostWriteVerification;
            }
            return CompletableFuture.completedFuture(activeLegacyConfig(configPath, VERSION));
        });
        VersionedRecord metadata = mockVersionedRecord(metadataPath, null);
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
        firstOpenPostWriteVerification.complete(permanentDeletion(configPath, VERSION));

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
    void metadataConflictDoesNotOpenLogWithDifferentId() {
        int partition = 3;
        String logName = paths.compactedReaderName(stream, partition);
        String configPath = paths.streamConfigPath(stream);
        String metadataPath = paths.partitionMetadataPath(stream, partition);
        mockVersionedRecord(configPath, activeLegacyConfigBytes(4));
        VersionedRecord metadata = mockVersionedRecord(
            metadataPath, metadataBytes(metadataPath, 42L, null, null, false));
        IndexedStreamCatalog catalog = catalog(
            ignored -> CompletableFuture.completedFuture(41L),
            ignored -> CompletableFuture.completedFuture(41L),
            ignored -> CompletableFuture.completedFuture(null));

        assertThatThrownBy(() -> catalog.openExternalPartition(stream, partition, Map.of()).join())
            .isInstanceOf(CompletionException.class)
            .hasCauseInstanceOf(AlreadyExistsException.class);

        assertThat(metadata.successfulPuts()).isZero();
        verify(readerFactory, never()).open(logName);
        verify(logFactory, never()).create(anyString(), any(), any());
    }

    @Test
    void externalProvisioningTakeoverFencesAThenFinalizesBAndLetsCAdopt() throws Exception {
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
        when(logFactory.create(logName, LogId.of(202L), reader)).thenReturn(recovered);
        IndexedStreamCatalog catalog = catalog(
            ignored -> CompletableFuture.completedFuture(101L),
            ignored -> CompletableFuture.completedFuture(
                allocations.getAndIncrement() == 0 ? 101L : 202L),
            ignored -> CompletableFuture.completedFuture(202L),
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

        catalog.registerExternalPartition(stream, partition, 202L, properties).join();

        JsonNode configOwnedByB = json(config);
        LogMetadata metadataOwnedByB = metadata(metadata);
        assertThat(configOwnedByB.path("_provisioning").asBoolean(false)).isFalse();
        assertThat(configOwnedByB.path("_incarnationId").asText())
            .isEqualTo(configOwnedByA.path("_incarnationId").asText());
        assertThat(configOwnedByB.path("_ownerToken").asText())
            .isNotEqualTo(configOwnedByA.path("_ownerToken").asText());
        assertThat(metadataOwnedByB.streamId()).isEqualTo(202L);
        assertThat(metadataOwnedByB.registrationOwnerToken())
            .isEqualTo(configOwnedByB.path("_ownerToken").asText());
        assertThat(metadataOwnedByB.registrationOwnerGeneration())
            .isEqualTo(configOwnedByB.path("_ownerGeneration").asLong());
        assertThat(metadataOwnedByB.registrationOwnerGeneration())
            .isGreaterThan(metadataOwnedByA.registrationOwnerGeneration());

        assertThat(catalog.openExternalPartition(stream, partition, properties).join())
            .isSameAs(recovered);
        assertThat(metadata.successfulPuts()).isEqualTo(2);

        stalePostWriteVerification.complete(config.currentResult());
        assertThatThrownBy(ownerA::join)
            .isInstanceOf(CompletionException.class)
            .hasCauseInstanceOf(NoSuchStreamException.class);
        assertThat(allocations).hasValue(2);
        assertThat(metadata(metadata).streamId()).isEqualTo(202L);
        verify(logStorage, never()).deleteLog(any());
        verify(oxiaClient, never()).delete(eq(metadataPath), any());
    }

    @Test
    void staleOwnerCannotPutAfterNewGenerationFinalizes() throws Exception {
        int partition = 0;
        String configPath = paths.streamConfigPath(stream);
        String metadataPath = paths.partitionMetadataPath(stream, partition);
        Map<String, String> properties = Map.of("owner", "kafka");
        VersionedRecord config = mockVersionedRecord(configPath, null);
        VersionedRecord metadata = mockVersionedRecord(metadataPath, null);
        CompletableFuture<GetResult> staleMetadataRead = new CompletableFuture<>();
        AtomicInteger metadataReads = new AtomicInteger();
        metadata.interceptReads(current -> metadataReads.incrementAndGet() == 2
            ? staleMetadataRead : CompletableFuture.completedFuture(current));
        IndexedStreamCatalog catalog = catalog(
            ignored -> CompletableFuture.completedFuture(101L),
            ignored -> CompletableFuture.completedFuture(101L),
            ignored -> CompletableFuture.completedFuture(202L),
            (ignored, streamId) -> CompletableFuture.completedFuture(null));

        CompletableFuture<Log> ownerA =
            catalog.openExternalPartition(stream, partition, properties);
        assertThat(ownerA).isNotDone();
        JsonNode ownerAConfig = json(config);
        assertThat(ownerAConfig.path("_ownerGeneration").asLong()).isEqualTo(1L);

        catalog.registerExternalPartition(stream, partition, 202L, properties).join();

        JsonNode ownerBConfig = json(config);
        assertThat(ownerBConfig.path("_provisioning").asBoolean(false)).isFalse();
        assertThat(ownerBConfig.path("_ownerGeneration").asLong()).isEqualTo(2L);
        assertThat(metadata(metadata).streamId()).isEqualTo(202L);
        assertThat(metadata(metadata).registrationOwnerGeneration()).isEqualTo(2L);

        staleMetadataRead.complete(null);
        assertThatThrownBy(ownerA::join)
            .isInstanceOf(CompletionException.class)
            .hasCauseInstanceOf(NoSuchStreamException.class);
        assertThat(metadata(metadata).streamId()).isEqualTo(202L);
        assertThat(metadata(metadata).registrationOwnerGeneration()).isEqualTo(2L);
        assertThat(metadata.successfulPuts()).isEqualTo(1);
        assertThat(metadata.putAttempts()).isEqualTo(2);
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
        IndexedStreamCatalog catalog = catalog(
            ignored -> CompletableFuture.completedFuture(52L),
            ignored -> CompletableFuture.completedFuture(52L),
            key -> {
                deletionOrder.add("mapping:" + key);
                return CompletableFuture.completedFuture(null);
            });

        catalog.deleteExternalPartition(stream, partition).get();

        assertThat(deletionOrder)
            .containsExactly("metadata-tombstone", "data", "mapping:" + logName);
        assertThat(metadata(metadata).deleted()).isTrue();
        assertThat(metadata(metadata).streamId()).isEqualTo(52L);
        verify(oxiaClient, never()).delete(eq(metadataPath), any());
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
        List<String> mappingDeletes = new ArrayList<>();
        IndexedStreamCatalog catalog = catalog(
            ignored -> CompletableFuture.completedFuture(152L),
            ignored -> CompletableFuture.completedFuture(152L),
            key -> {
                mappingDeletes.add(key);
                return CompletableFuture.completedFuture(null);
            });
        catalogRef.set(catalog);

        catalog.deleteExternalPartition(stream, partition).join();

        assertThat(json(config).path("_provisioningState").asText())
            .isEqualTo("PERMANENTLY_DELETED");
        LogMetadata tombstone = metadata(metadata);
        assertThat(tombstone.deleted()).isTrue();
        assertThat(tombstone.registrationOwnerGeneration()).isEqualTo(2L);
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
        verify(mappingDeleter, never()).apply(anyString());
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
                        new AssertionError("Expected an external stream-ID mapping"))
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
        AtomicReference<Long> mapping = new AtomicReference<>(retiredStreamId);
        IndexedStreamCatalog catalog = catalog(
            ignored -> CompletableFuture.completedFuture(retiredStreamId),
            ignored -> CompletableFuture.completedFuture(retiredStreamId),
            ignored -> CompletableFuture.completedFuture(retiredStreamId),
            (key, expectedStreamId) -> {
                assertThat(key).isEqualTo(logName);
                mapping.set(null);
                return CompletableFuture.completedFuture(null);
            });

        catalog.unregisterExternalStream(stream).join();

        assertThatThrownBy(() ->
                catalog.openExternalPartition(stream, partition, Map.of()).join())
            .isInstanceOf(CompletionException.class)
            .hasCauseInstanceOf(AlreadyExistsException.class)
            .hasMessageContaining("must use a fresh physical stream ID");

        assertThat(mapping.get()).isNull();
        assertThat(metadata(metadata).deleted()).isTrue();
        assertThat(metadata(metadata).streamId()).isEqualTo(retiredStreamId);
        assertThat(metadata.successfulPuts()).isZero();
        assertThat(json(config).path("_provisioningState").asText())
            .isEqualTo("PROVISIONING");
        verify(logStorage).deleteLog(LogId.of(retiredStreamId));
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
        List<String> mappingDeletes = new ArrayList<>();
        IndexedStreamCatalog catalog = catalog(
            ignored -> CompletableFuture.completedFuture(154L),
            ignored -> CompletableFuture.completedFuture(154L),
            key -> {
                mappingDeletes.add(key);
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
        IndexedStreamCatalog catalog = catalog(
            ignored -> CompletableFuture.completedFuture(63L),
            key -> CompletableFuture.completedFuture(63L),
            key -> {
                deletionOrder.add("mapping:" + key);
                return CompletableFuture.completedFuture(null);
            });

        catalog.deleteExternalPartition(stream, partition).get();

        assertThat(deletionOrder)
            .containsExactly("metadata-tombstone", "data", "mapping:" + logName);
        LogMetadata tombstone = metadata(metadata);
        assertThat(tombstone.streamId()).isEqualTo(63L);
        assertThat(tombstone.registrationIncarnationId()).isEqualTo(incarnation);
        assertThat(tombstone.registrationOwnerToken()).isEqualTo(owner);
        assertThat(tombstone.registrationOwnerGeneration()).isEqualTo(1L);
        assertThat(tombstone.deleted()).isTrue();
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
        assertThat(metadata(metadata).streamId()).isEqualTo(74L);
        verify(mappingDeleter, never()).apply(anyString());
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
        RuntimeException mappingFailure = new RuntimeException("mapping delete failed");
        IndexedStreamCatalog catalog = catalog(
            ignored -> CompletableFuture.completedFuture(107L),
            key -> {
                assertThat(key).isEqualTo(logName);
                lookupCount.incrementAndGet();
                return CompletableFuture.completedFuture(107L);
            },
            key -> {
                assertThat(key).isEqualTo(logName);
                return mappingDeleteCount.getAndIncrement() == 0
                    ? CompletableFuture.failedFuture(mappingFailure)
                    : CompletableFuture.completedFuture(null);
            });

        assertThatThrownBy(() -> catalog.deleteExternalPartition(stream, partition).join())
            .isInstanceOf(CompletionException.class)
            .hasCause(mappingFailure);
        catalog.deleteExternalPartition(stream, partition).get();

        assertThat(metadata.successfulPuts()).isEqualTo(1);
        assertThat(metadata(metadata).deleted()).isTrue();
        verify(logStorage, times(2)).deleteLog(LogId.of(107L));
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
        assertThat(metadata.successfulPuts()).isEqualTo(1);
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
            .hasCauseInstanceOf(AlreadyExistsException.class);

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
            .hasCauseInstanceOf(AlreadyExistsException.class);

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
            .hasCauseInstanceOf(NoSuchStreamException.class);

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
            .hasCauseInstanceOf(AlreadyExistsException.class);

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
            .hasCauseInstanceOf(AlreadyExistsException.class);
        assertThat(metadata(metadata).streamId()).isEqualTo(71L);

        catalog.registerExternalPartition(stream, partition, 71L, Map.of()).join();

        JsonNode active = json(config);
        assertThat(active.path("_provisioning").asBoolean(false)).isFalse();
        assertThat(active.path("_incarnationId").asText()).isEqualTo(incarnation);
        assertThat(active.path("_ownerGeneration").asLong()).isEqualTo(3L);
        assertThat(active.path("_metadataSourceGeneration").asLong()).isEqualTo(1L);
        LogMetadata adopted = metadata(metadata);
        assertThat(adopted.streamId()).isEqualTo(71L);
        assertThat(adopted.registrationIncarnationId()).isEqualTo(incarnation);
        assertThat(adopted.registrationOwnerGeneration()).isEqualTo(3L);
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
        assertThat(metadata.successfulPuts()).isEqualTo(1);
    }

    @Test
    void newIncarnationReplacesLegacyTombstone() throws Exception {
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

        catalog.registerExternalPartition(stream, partition, 502L, Map.of()).join();

        JsonNode active = json(config);
        LogMetadata replacement = metadata(metadata);
        assertThat(replacement.streamId()).isEqualTo(502L);
        assertThat(replacement.deleted()).isFalse();
        assertThat(replacement.registrationIncarnationId())
            .isEqualTo(active.path("_incarnationId").asText());
        assertThat(replacement.registrationOwnerToken())
            .isEqualTo(active.path("_ownerToken").asText());
        assertThat(metadata.successfulPuts()).isEqualTo(1);
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
            .hasCauseInstanceOf(AlreadyExistsException.class);

        assertThat(metadata(metadata).deleted()).isTrue();
        assertThat(metadata.successfulPuts()).isZero();
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
        Function<Optional<String>, CompletableFuture<Long>> combinedGenerator = key ->
            key.isPresent() ? keyedGenerator.apply(key.orElseThrow()) : generator.apply(key);
        return new IndexedStreamCatalog(
            oxiaClient, paths, logStorage, logFactory, null, combinedGenerator,
            lookup, mappingDeleter,
            readerFactory, null, List.of());
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
        try {
            return LOG_METADATA_SERDE.serialize(path, new LogMetadata(
                streamId, properties, terminatedOffset,
                incarnationId, ownerToken, ownerGeneration, deleted));
        } catch (Exception e) {
            throw new AssertionError(e);
        }
    }

    private static Version version(long id) {
        return new Version(id, 0, 0, 0, Optional.empty(), Optional.empty());
    }

    private final class VersionedRecord {

        private final String path;
        private final AtomicReference<VersionedValue> state;
        private final AtomicLong nextVersion;
        private final AtomicReference<Throwable> nextPutFailure = new AtomicReference<>();
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
