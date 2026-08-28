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

import io.lakestream.api.Log;
import io.lakestream.api.LogId;
import io.lakestream.api.LogStorage;
import io.lakestream.api.StreamIdentifier;
import io.lakestream.ursa.catalog.metadata.LogMetadata;
import io.lakestream.ursa.catalog.metadata.LogMetadataSerde;
import io.lakestream.ursa.lakestream.reader.CompactedObjectReader;
import io.lakestream.ursa.lakestream.reader.CompactedObjectReaderFactory;
import io.lakestream.ursa.storage.impl.exception.NoSuchKeyException;
import io.oxia.client.api.AsyncOxiaClient;
import io.oxia.client.api.GetResult;
import io.oxia.client.api.PutResult;
import io.oxia.client.api.Version;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class IndexedStreamCatalogExternalPartitionTest {

    private static final Version VERSION = new Version(1, 0, 0, 0, Optional.empty(), Optional.empty());

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
    }

    @Test
    void opensUsingCatalogDerivedNameAndRegistersPartition() throws Exception {
        String logName = paths.compactedReaderName(stream, 2);
        AtomicReference<Optional<String>> generatedKey = new AtomicReference<>();
        CompactedObjectReader reader = mock(CompactedObjectReader.class);
        Log log = mock(Log.class);
        when(oxiaClient.put(anyString(), any(byte[].class), any()))
            .thenReturn(CompletableFuture.completedFuture(new PutResult("key", VERSION)));
        when(oxiaClient.get(paths.streamConfigPath(stream)))
            .thenReturn(CompletableFuture.completedFuture(null));
        when(readerFactory.open(logName)).thenReturn(reader);
        when(logFactory.create(logName, LogId.of(41L), reader)).thenReturn(log);
        IndexedStreamCatalog catalog = catalog(
            key -> {
                generatedKey.set(key);
                return CompletableFuture.completedFuture(41L);
            },
            ignored -> CompletableFuture.completedFuture(41L),
            ignored -> CompletableFuture.completedFuture(null));

        assertThat(catalog.openExternalPartition(stream, 2, Map.of("owner", "kafka")).get())
            .isSameAs(log);
        assertThat(generatedKey.get()).contains(logName);
        verify(oxiaClient).put(eq(paths.partitionMetadataPath(stream, 2)), any(byte[].class), any());
        verify(readerFactory).open(logName);
    }

    @Test
    void deletesDataThenCatalogMetadataThenKeyedMapping() throws Exception {
        int partition = 1;
        String logName = paths.compactedReaderName(stream, partition);
        String metadataPath = paths.partitionMetadataPath(stream, partition);
        byte[] metadata = LogMetadataSerde.INSTANCE.serialize(
            metadataPath, new LogMetadata(52L, Map.of(), OptionalLong.empty()));
        List<String> deletionOrder = new ArrayList<>();
        when(oxiaClient.get(metadataPath)).thenReturn(CompletableFuture.completedFuture(
            new GetResult(metadataPath, metadata, VERSION)));
        when(logStorage.deleteLog(LogId.of(52L))).thenAnswer(__ -> {
            deletionOrder.add("data");
            return CompletableFuture.completedFuture(null);
        });
        when(oxiaClient.delete(metadataPath)).thenAnswer(__ -> {
            deletionOrder.add("metadata");
            return CompletableFuture.completedFuture(null);
        });
        IndexedStreamCatalog catalog = catalog(
            ignored -> CompletableFuture.completedFuture(52L),
            ignored -> CompletableFuture.failedFuture(new AssertionError("mapping lookup must not run")),
            key -> {
                deletionOrder.add("mapping:" + key);
                return CompletableFuture.completedFuture(null);
            });

        catalog.deleteExternalPartition(stream, partition).get();

        assertThat(deletionOrder).containsExactly("data", "metadata", "mapping:" + logName);
    }

    @Test
    void deletesHistoricalMappingWhenPartitionMetadataIsMissing() throws Exception {
        int partition = 3;
        String logName = paths.compactedReaderName(stream, partition);
        String metadataPath = paths.partitionMetadataPath(stream, partition);
        List<String> deletionOrder = new ArrayList<>();
        when(oxiaClient.get(metadataPath)).thenReturn(CompletableFuture.completedFuture(null));
        when(logStorage.deleteLog(LogId.of(63L))).thenAnswer(__ -> {
            deletionOrder.add("data");
            return CompletableFuture.completedFuture(null);
        });
        when(oxiaClient.delete(metadataPath)).thenAnswer(__ -> {
            deletionOrder.add("metadata");
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

        assertThat(deletionOrder).containsExactly("data", "metadata", "mapping:" + logName);
    }

    @Test
    void dataDeletionFailurePreservesMetadataAndMapping() {
        int partition = 4;
        String metadataPath = paths.partitionMetadataPath(stream, partition);
        RuntimeException deleteFailure = new RuntimeException("data delete failed");
        when(oxiaClient.get(metadataPath)).thenReturn(CompletableFuture.completedFuture(null));
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
        verify(oxiaClient, never()).delete(metadataPath);
        verify(mappingDeleter, never()).apply(anyString());
    }

    @Test
    void metadataDeletionFailurePreservesKeyedMapping() throws Exception {
        int partition = 6;
        String metadataPath = paths.partitionMetadataPath(stream, partition);
        byte[] metadata = LogMetadataSerde.INSTANCE.serialize(
            metadataPath, new LogMetadata(96L, Map.of(), OptionalLong.empty()));
        RuntimeException metadataFailure = new RuntimeException("metadata delete failed");
        when(oxiaClient.get(metadataPath)).thenReturn(CompletableFuture.completedFuture(
            new GetResult(metadataPath, metadata, VERSION)));
        when(logStorage.deleteLog(LogId.of(96L))).thenReturn(CompletableFuture.completedFuture(null));
        when(oxiaClient.delete(metadataPath)).thenReturn(CompletableFuture.failedFuture(metadataFailure));
        @SuppressWarnings("unchecked")
        Function<String, CompletableFuture<Void>> mappingDeleter = mock(Function.class);
        IndexedStreamCatalog catalog = catalog(
            ignored -> CompletableFuture.completedFuture(96L),
            ignored -> CompletableFuture.completedFuture(96L),
            mappingDeleter);

        assertThatThrownBy(() -> catalog.deleteExternalPartition(stream, partition).join())
            .isInstanceOf(CompletionException.class)
            .hasCause(metadataFailure);
        verify(mappingDeleter, never()).apply(anyString());
    }

    @Test
    void retryAfterMappingFailureUsesResidualMappingAndCompletesCleanup() throws Exception {
        int partition = 7;
        String logName = paths.compactedReaderName(stream, partition);
        String metadataPath = paths.partitionMetadataPath(stream, partition);
        byte[] metadata = LogMetadataSerde.INSTANCE.serialize(
            metadataPath, new LogMetadata(107L, Map.of(), OptionalLong.empty()));
        when(oxiaClient.get(metadataPath))
            .thenReturn(CompletableFuture.completedFuture(
                new GetResult(metadataPath, metadata, VERSION)))
            .thenReturn(CompletableFuture.completedFuture(null));
        when(logStorage.deleteLog(LogId.of(107L))).thenReturn(CompletableFuture.completedFuture(null));
        when(oxiaClient.delete(metadataPath)).thenReturn(CompletableFuture.completedFuture(null));
        AtomicInteger lookupCount = new AtomicInteger();
        AtomicInteger mappingDeleteCount = new AtomicInteger();
        RuntimeException mappingFailure = new RuntimeException("mapping delete failed");
        IndexedStreamCatalog catalog = catalog(
            ignored -> CompletableFuture.completedFuture(107L),
            key -> {
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

        verify(logStorage, times(2)).deleteLog(LogId.of(107L));
        assertThat(lookupCount).hasValue(1);
        assertThat(mappingDeleteCount).hasValue(2);
    }

    @Test
    void registrationFailureDoesNotOpenReaderAndRetryReusesKeyedId() {
        int partition = 8;
        String logName = paths.compactedReaderName(stream, partition);
        String metadataPath = paths.partitionMetadataPath(stream, partition);
        String configPath = paths.streamConfigPath(stream);
        RuntimeException registrationFailure = new RuntimeException("registration failed");
        when(oxiaClient.put(eq(metadataPath), any(byte[].class), any()))
            .thenReturn(CompletableFuture.failedFuture(registrationFailure))
            .thenReturn(CompletableFuture.completedFuture(new PutResult(metadataPath, VERSION)));
        when(oxiaClient.get(configPath)).thenReturn(CompletableFuture.completedFuture(null));
        when(oxiaClient.put(eq(configPath), any(byte[].class), any()))
            .thenReturn(CompletableFuture.completedFuture(new PutResult(configPath, VERSION)));
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
        verify(readerFactory).open(logName);
    }

    @Test
    void missingMetadataAndMappingIsIdempotent() throws Exception {
        int partition = 5;
        String logName = paths.compactedReaderName(stream, partition);
        String metadataPath = paths.partitionMetadataPath(stream, partition);
        when(oxiaClient.get(metadataPath)).thenReturn(CompletableFuture.completedFuture(null));
        when(oxiaClient.delete(metadataPath)).thenReturn(CompletableFuture.completedFuture(null));
        AtomicReference<String> deletedMapping = new AtomicReference<>();
        IndexedStreamCatalog catalog = catalog(
            ignored -> CompletableFuture.completedFuture(85L),
            key -> CompletableFuture.failedFuture(
                new CompletionException(new NoSuchKeyException("missing"))),
            key -> {
                deletedMapping.set(key);
                return CompletableFuture.completedFuture(null);
            });

        catalog.deleteExternalPartition(stream, partition).get();

        verify(logStorage, never()).deleteLog(any());
        assertThat(deletedMapping.get()).isEqualTo(logName);
    }

    private IndexedStreamCatalog catalog(
            Function<Optional<String>, CompletableFuture<Long>> generator,
            Function<String, CompletableFuture<Long>> lookup,
            Function<String, CompletableFuture<Void>> mappingDeleter) {
        return new IndexedStreamCatalog(
            oxiaClient, paths, logStorage, logFactory, null, generator, lookup, mappingDeleter,
            readerFactory, null, List.of());
    }
}
