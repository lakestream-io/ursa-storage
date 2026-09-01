/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.lakestream.impl;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.core.JsonProcessingException;
import io.lakestream.api.CatalogPaths;
import io.lakestream.api.LogStorage;
import io.lakestream.api.Partitioning;
import io.lakestream.api.PartitioningStrategy;
import io.lakestream.api.SchemaConfig;
import io.lakestream.api.StreamConfig;
import io.lakestream.api.StreamIdentifier;
import io.lakestream.api.StreamMetadata;
import io.lakestream.api.exception.NoSuchStreamException;
import io.lakestream.api.materialization.EvolutionPolicy;
import io.lakestream.api.materialization.TableCatalog;
import io.lakestream.api.materialization.TableCatalogType;
import io.lakestream.api.materialization.TableIdentifier;
import io.lakestream.api.materialization.TableMaterializationPolicy;
import io.oxia.client.api.AsyncOxiaClient;
import io.oxia.client.api.GetResult;
import io.oxia.client.api.PutResult;
import io.oxia.client.api.Version;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class IndexedStreamCatalogStreamPolicyTest {

    private static final Version DUMMY_VERSION =
        new Version(1, 0, 0, 0, Optional.empty(), Optional.empty());

    @Mock
    private AsyncOxiaClient oxiaClient;
    @Mock
    private LogStorage logStorage;

    private CatalogPaths catalogPaths;
    private IndexedStreamCatalog catalog;
    private StreamIdentifier streamId;
    private final Map<String, byte[]> store = new HashMap<>();
    private long nextStreamId = 100L;

    @BeforeEach
    void setUp() {
        catalogPaths = new DefaultCatalogPaths();
        catalog = IndexedStreamCatalog.withConditionalStreamIdMappingDeletion(
            oxiaClient, catalogPaths, logStorage,
            (name, logId, reader) -> null, null,
            key -> CompletableFuture.completedFuture(nextStreamId++),
            key -> CompletableFuture.completedFuture(-1L),
            (key, expectedStreamId) -> CompletableFuture.completedFuture(null),
            null, null, List.of());
        catalog.initialize("test-catalog", Map.of()).join();
        streamId = new StreamIdentifier("public/default", "mat-topic");

        when(oxiaClient.get(any(String.class))).thenAnswer(inv -> {
            String key = inv.getArgument(0);
            byte[] value = store.get(key);
            if (value == null) {
                return CompletableFuture.completedFuture(null);
            }
            return CompletableFuture.completedFuture(new GetResult(key, value, DUMMY_VERSION));
        });
        when(oxiaClient.put(any(String.class), any(byte[].class))).thenAnswer(inv -> {
            String key = inv.getArgument(0);
            byte[] value = inv.getArgument(1);
            store.put(key, value);
            return CompletableFuture.completedFuture(new PutResult(key, DUMMY_VERSION));
        });
        when(oxiaClient.put(any(String.class), any(byte[].class), any())).thenAnswer(inv -> {
            String key = inv.getArgument(0);
            byte[] value = inv.getArgument(1);
            store.put(key, value);
            return CompletableFuture.completedFuture(new PutResult(key, DUMMY_VERSION));
        });
        when(oxiaClient.delete(any(String.class))).thenAnswer(inv -> {
            String key = inv.getArgument(0);
            boolean removed = store.remove(key) != null;
            return CompletableFuture.completedFuture(removed);
        });
        when(oxiaClient.list(any(String.class), any(String.class))).thenAnswer(inv -> {
            String start = inv.getArgument(0);
            String end = inv.getArgument(1);
            List<String> keys = new ArrayList<>();
            for (String key : store.keySet()) {
                if (key.compareTo(start) >= 0 && key.compareTo(end) < 0) {
                    keys.add(key);
                }
            }
            keys.sort(String::compareTo);
            return CompletableFuture.completedFuture(keys);
        });
    }

    private static TableMaterializationPolicy samplePolicy() {
        return new TableMaterializationPolicy(
            Optional.of("iceberg-prod"),
            Optional.empty(),
            Optional.of(new TableIdentifier("warehouse", "topic-table")),
            Optional.of(true),
            Optional.empty(),
            Optional.of(EvolutionPolicy.forIceberg()),
            Optional.of(List.of("id")),
            Optional.empty(),
            Optional.empty(),
            Map.of("warehouse", "s3://override"));
    }

    private void createMaterializedStream() throws Exception {
        Partitioning partitioning = new Partitioning(PartitioningStrategy.INDEXED,
            Map.of("numPartitions", "1"));
        catalog.createStream(streamId, new StreamConfig(), partitioning,
            new SchemaConfig(), Map.of(), Optional.of(samplePolicy())).get();
    }

    @Test
    void createStream_withMaterialization_persistsPolicy() throws Exception {
        TableMaterializationPolicy policy = samplePolicy();
        Partitioning partitioning = new Partitioning(PartitioningStrategy.INDEXED,
            Map.of("numPartitions", "1"));

        StreamMetadata created = catalog.createStream(streamId, new StreamConfig(), partitioning,
            new SchemaConfig(), Map.of(), Optional.of(policy)).get();
        assertThat(created.materialization()).contains(policy);

        StreamMetadata reloaded = catalog.loadStream(streamId).get();
        assertThat(reloaded.materialization()).contains(policy);
    }

    @Test
    void resolveMaterialization_allowsMissingNamespace() throws Exception {
        createMaterializedStream();
        catalog.registerTableCatalog(new TableCatalog(
            "iceberg-prod", TableCatalogType.ICEBERG, Map.of(), Map.of())).get();

        assertThat(catalog.resolveMaterialization(streamId).get()).isPresent();
    }

    @Test
    void resolveMaterialization_propagatesNamespaceReadFailure() throws Exception {
        createMaterializedStream();
        RuntimeException readFailure = new RuntimeException("namespace read failed");
        when(oxiaClient.get(catalogPaths.namespacePath(streamId.namespace())))
            .thenReturn(CompletableFuture.failedFuture(readFailure));

        assertThatThrownBy(() -> catalog.resolveMaterialization(streamId).join())
            .isInstanceOf(CompletionException.class)
            .hasCause(readFailure);
    }

    @Test
    void resolveMaterialization_propagatesNamespaceDeserializationFailure()
            throws Exception {
        createMaterializedStream();
        store.put(catalogPaths.namespacePath(streamId.namespace()),
            "not-json".getBytes(StandardCharsets.UTF_8));

        assertThatThrownBy(() -> catalog.resolveMaterialization(streamId).join())
            .isInstanceOf(CompletionException.class)
            .hasCauseInstanceOf(RuntimeException.class)
            .hasRootCauseInstanceOf(JsonProcessingException.class);
    }

    @Test
    void resolveMaterialization_propagatesTableCatalogReadFailure() throws Exception {
        createMaterializedStream();
        RuntimeException readFailure = new RuntimeException("catalog read failed");
        when(oxiaClient.get(catalogPaths.tableCatalogPath("iceberg-prod")))
            .thenReturn(CompletableFuture.failedFuture(readFailure));

        assertThatThrownBy(() -> catalog.resolveMaterialization(streamId).join())
            .isInstanceOf(CompletionException.class)
            .hasCause(readFailure);
    }

    @Test
    void resolveMaterialization_propagatesTableCatalogDeserializationFailure()
            throws Exception {
        createMaterializedStream();
        store.put(catalogPaths.tableCatalogPath("iceberg-prod"),
            "not-json".getBytes(StandardCharsets.UTF_8));

        assertThatThrownBy(() -> catalog.resolveMaterialization(streamId).join())
            .isInstanceOf(CompletionException.class)
            .hasCauseInstanceOf(RuntimeException.class)
            .hasRootCauseInstanceOf(JsonProcessingException.class);
    }

    @Test
    void createStream_withoutMaterialization_isEmpty() throws Exception {
        Partitioning partitioning = new Partitioning(PartitioningStrategy.INDEXED,
            Map.of("numPartitions", "1"));
        catalog.createStream(streamId, new StreamConfig(), partitioning,
            new SchemaConfig(), Map.of()).get();

        StreamMetadata reloaded = catalog.loadStream(streamId).get();
        assertThat(reloaded.materialization()).isEmpty();
    }

    @Test
    void setStreamMaterialization_roundTrip() throws Exception {
        Partitioning partitioning = new Partitioning(PartitioningStrategy.INDEXED,
            Map.of("numPartitions", "1"));
        catalog.createStream(streamId, new StreamConfig(), partitioning,
            new SchemaConfig(), Map.of()).get();

        catalog.setStreamMaterialization(streamId, samplePolicy()).get();

        StreamMetadata reloaded = catalog.loadStream(streamId).get();
        assertThat(reloaded.materialization()).contains(samplePolicy());
    }

    @Test
    void clearStreamMaterialization_makesItEmpty() throws Exception {
        Partitioning partitioning = new Partitioning(PartitioningStrategy.INDEXED,
            Map.of("numPartitions", "1"));
        catalog.createStream(streamId, new StreamConfig(), partitioning,
            new SchemaConfig(), Map.of(), Optional.of(samplePolicy())).get();

        catalog.clearStreamMaterialization(streamId).get();

        StreamMetadata reloaded = catalog.loadStream(streamId).get();
        assertThat(reloaded.materialization()).isEmpty();
    }

    @Test
    void setStreamMaterialization_unknownStreamFails() {
        assertThatThrownBy(() ->
                catalog.setStreamMaterialization(streamId, samplePolicy()).join())
            .isInstanceOf(CompletionException.class)
            .hasCauseInstanceOf(NoSuchStreamException.class);
    }

    @Test
    void clearStreamMaterialization_unknownStreamFails() {
        assertThatThrownBy(() -> catalog.clearStreamMaterialization(streamId).join())
            .isInstanceOf(CompletionException.class)
            .hasCauseInstanceOf(NoSuchStreamException.class);
    }

}
