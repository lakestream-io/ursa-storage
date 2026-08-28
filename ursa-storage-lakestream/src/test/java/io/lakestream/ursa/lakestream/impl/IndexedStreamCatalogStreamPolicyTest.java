/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.lakestream.impl;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

import io.lakestream.api.CatalogPaths;
import io.lakestream.api.LogStorage;
import io.lakestream.api.Partitioning;
import io.lakestream.api.PartitioningStrategy;
import io.lakestream.api.SchemaConfig;
import io.lakestream.api.Stream;
import io.lakestream.api.StreamConfig;
import io.lakestream.api.StreamIdentifier;
import io.lakestream.api.exception.NoSuchStreamException;
import io.lakestream.api.materialization.EvolutionPolicy;
import io.lakestream.api.materialization.TableIdentifier;
import io.lakestream.api.materialization.TableMaterializationPolicy;
import io.oxia.client.api.AsyncOxiaClient;
import io.oxia.client.api.GetResult;
import io.oxia.client.api.PutResult;
import io.oxia.client.api.Version;
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
        catalog = new IndexedStreamCatalog(oxiaClient, catalogPaths, logStorage,
            logId -> null, null,
            key -> CompletableFuture.completedFuture(nextStreamId++),
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

    @Test
    void createStream_withMaterialization_persistsPolicy() throws Exception {
        TableMaterializationPolicy policy = samplePolicy();
        Partitioning partitioning = new Partitioning(PartitioningStrategy.INDEXED,
            Map.of("numPartitions", "1"));

        Stream created = catalog.createStream(streamId, new StreamConfig(), partitioning,
            new SchemaConfig(), Map.of(), Optional.of(policy)).get();
        assertThat(created.materialization()).contains(policy);

        Stream reloaded = catalog.loadStream(streamId).get();
        assertThat(reloaded.materialization()).contains(policy);
    }

    @Test
    void createStream_withoutMaterialization_isEmpty() throws Exception {
        Partitioning partitioning = new Partitioning(PartitioningStrategy.INDEXED,
            Map.of("numPartitions", "1"));
        catalog.createStream(streamId, new StreamConfig(), partitioning,
            new SchemaConfig(), Map.of()).get();

        Stream reloaded = catalog.loadStream(streamId).get();
        assertThat(reloaded.materialization()).isEmpty();
    }

    @Test
    void setStreamMaterialization_roundTrip() throws Exception {
        Partitioning partitioning = new Partitioning(PartitioningStrategy.INDEXED,
            Map.of("numPartitions", "1"));
        catalog.createStream(streamId, new StreamConfig(), partitioning,
            new SchemaConfig(), Map.of()).get();

        catalog.setStreamMaterialization(streamId, samplePolicy()).get();

        Stream reloaded = catalog.loadStream(streamId).get();
        assertThat(reloaded.materialization()).contains(samplePolicy());
    }

    @Test
    void clearStreamMaterialization_makesItEmpty() throws Exception {
        Partitioning partitioning = new Partitioning(PartitioningStrategy.INDEXED,
            Map.of("numPartitions", "1"));
        catalog.createStream(streamId, new StreamConfig(), partitioning,
            new SchemaConfig(), Map.of(), Optional.of(samplePolicy())).get();

        catalog.clearStreamMaterialization(streamId).get();

        Stream reloaded = catalog.loadStream(streamId).get();
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

    @Test
    void setStreamProperties_preservesMaterialization() throws Exception {
        Partitioning partitioning = new Partitioning(PartitioningStrategy.INDEXED,
            Map.of("numPartitions", "1"));
        catalog.createStream(streamId, new StreamConfig(), partitioning,
            new SchemaConfig(), Map.of(), Optional.of(samplePolicy())).get();

        catalog.setStreamProperties(streamId, Map.of("env", "prod")).get();

        Stream reloaded = catalog.loadStream(streamId).get();
        assertThat(reloaded.properties()).isEqualTo(Map.of("env", "prod"));
        assertThat(reloaded.materialization()).contains(samplePolicy());
    }
}
