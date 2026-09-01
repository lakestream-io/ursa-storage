/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.storage;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import io.lakestream.api.Partitioning;
import io.lakestream.api.PartitioningStrategy;
import io.lakestream.api.SchemaConfig;
import io.lakestream.api.StreamConfig;
import io.lakestream.api.StreamIdentifier;
import io.lakestream.api.StreamMetadata;
import io.lakestream.ursa.lakestream.impl.DefaultCatalogPaths;
import io.lakestream.ursa.lakestream.impl.IndexedStreamCatalog;
import io.lakestream.ursa.lakestream.impl.LakestreamBootstrap;
import io.lakestream.ursa.storage.impl.StorageConfig;
import io.opentelemetry.api.OpenTelemetry;
import io.oxia.client.api.AsyncOxiaClient;
import io.oxia.client.api.GetResult;
import io.oxia.client.api.PutResult;
import io.oxia.client.api.Version;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;

class UrsaStorageStreamCatalogMetadataOnlyTest {

    @Test
    void completeCatalogMetadataLifecycleDoesNotInitializeDataPlane() throws Exception {
        AsyncOxiaClient storageOxia = inMemoryOxiaClient();
        AsyncOxiaClient catalogOxia = inMemoryOxiaClient();
        UrsaStorage.DataPlaneFactory dataPlaneFactory =
            mock(UrsaStorage.DataPlaneFactory.class);
        UrsaStorage storage = new UrsaStorage(
            persistentConfig(), OpenTelemetry.noop(), storageOxia, dataPlaneFactory);
        StorageApi storageApi = storage.getDefaultStorageApi();
        storageApi.startWALCleanupService();

        IndexedStreamCatalog catalog = new IndexedStreamCatalog(
            catalogOxia, new DefaultCatalogPaths(),
            LakestreamBootstrap.createLogStorage(storageApi),
            (name, logId, reader) -> {
                throw new AssertionError("metadata lifecycle opened log " + logId);
            },
            LakestreamBootstrap.createStateManager(storageApi), storageApi,
            null, null, new ArrayList<>(List.of(storage)));
        catalog.initialize("metadata-only", Map.of()).join();

        StreamIdentifier retained =
            new StreamIdentifier("public/default", "retained-topic");
        StreamIdentifier purged =
            new StreamIdentifier("public/default", "purged-topic");
        Partitioning onePartition = new Partitioning(
            PartitioningStrategy.INDEXED, Map.of("numPartitions", "1"));

        StreamMetadata created = catalog.createStream(
            retained, new StreamConfig(), onePartition,
            new SchemaConfig(), Map.of("stage", "created")).join();
        assertEquals(retained, created.identifier());
        assertEquals(retained, catalog.loadStream(retained).join().identifier());
        assertEquals(2, catalog.increasePartitions(retained, 2)
            .join().layout().logCount());
        assertEquals(Map.of("stage", "updated"),
            catalog.replaceStreamProperties(
                retained, Map.of("stage", "updated"), 1L).join().properties());
        assertEquals(List.of(retained), catalog.listStreams("public/default").join());
        assertTrue(catalog.dropStream(retained, false).join());

        catalog.createStream(
            purged, new StreamConfig(), onePartition,
            new SchemaConfig(), Map.of()).join();
        assertTrue(catalog.dropStream(purged, true).join());
        assertTrue(catalog.listStreams("public/default").join().isEmpty());

        verifyNoInteractions(dataPlaneFactory);
        catalog.close();
        verifyNoInteractions(dataPlaneFactory);
    }

    private static StorageConfig persistentConfig() {
        Properties properties = new Properties();
        properties.setProperty("backendStorageType", "S3");
        return StorageConfig.fromProperties(properties);
    }

    @SuppressWarnings("unchecked")
    private static AsyncOxiaClient inMemoryOxiaClient() {
        AsyncOxiaClient client = mock(AsyncOxiaClient.class);
        Map<String, StoredValue> values = new HashMap<>();
        AtomicLong nextVersion = new AtomicLong(100L);

        when(client.get(anyString())).thenAnswer(invocation ->
            CompletableFuture.completedFuture(getResult(
                invocation.getArgument(0, String.class), values)));
        when(client.get(anyString(), any(Set.class))).thenAnswer(invocation ->
            CompletableFuture.completedFuture(getResult(
                invocation.getArgument(0, String.class), values)));
        when(client.put(anyString(), any(byte[].class))).thenAnswer(invocation ->
            putResult(invocation.getArgument(0, String.class),
                invocation.getArgument(1, byte[].class), values, nextVersion));
        when(client.put(anyString(), any(byte[].class), any(Set.class))).thenAnswer(invocation ->
            putResult(invocation.getArgument(0, String.class),
                invocation.getArgument(1, byte[].class), values, nextVersion));
        when(client.delete(anyString())).thenAnswer(invocation ->
            CompletableFuture.completedFuture(
                values.remove(invocation.getArgument(0, String.class)) != null));
        when(client.delete(anyString(), any(Set.class))).thenAnswer(invocation ->
            CompletableFuture.completedFuture(
                values.remove(invocation.getArgument(0, String.class)) != null));
        when(client.deleteRange(anyString(), anyString(), any(Set.class)))
            .thenReturn(CompletableFuture.completedFuture(null));
        when(client.list(anyString(), anyString())).thenAnswer(invocation -> {
            String prefix = invocation.getArgument(0, String.class);
            return CompletableFuture.completedFuture(values.keySet().stream()
                .filter(key -> key.startsWith(prefix))
                .sorted()
                .toList());
        });
        when(client.list(anyString(), anyString(), any(Set.class))).thenAnswer(invocation -> {
            String prefix = invocation.getArgument(0, String.class);
            return CompletableFuture.completedFuture(values.keySet().stream()
                .filter(key -> key.startsWith(prefix))
                .sorted()
                .toList());
        });
        return client;
    }

    private static CompletableFuture<PutResult> putResult(
            String key, byte[] value, Map<String, StoredValue> values,
            AtomicLong nextVersion) {
        long versionId = nextVersion.incrementAndGet();
        Version version = version(versionId);
        values.put(key, new StoredValue(value.clone(), version));
        return CompletableFuture.completedFuture(new PutResult(key, version));
    }

    private static GetResult getResult(
            String key, Map<String, StoredValue> values) {
        StoredValue value = values.get(key);
        return value == null ? null
            : new GetResult(key, value.value().clone(), value.version());
    }

    private static Version version(long versionId) {
        return new Version(
            versionId, 0, 0, 0, Optional.empty(), Optional.empty());
    }

    private record StoredValue(byte[] value, Version version) {
    }
}
