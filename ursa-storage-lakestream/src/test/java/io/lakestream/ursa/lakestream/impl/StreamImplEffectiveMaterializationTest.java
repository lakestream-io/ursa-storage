/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.lakestream.impl;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

import io.lakestream.api.CatalogPaths;
import io.lakestream.api.LogStorage;
import io.lakestream.api.Namespace;
import io.lakestream.api.Partitioning;
import io.lakestream.api.PartitioningStrategy;
import io.lakestream.api.SchemaConfig;
import io.lakestream.api.Stream;
import io.lakestream.api.StreamConfig;
import io.lakestream.api.StreamIdentifier;
import io.lakestream.api.materialization.ResolvedMaterialization;
import io.lakestream.api.materialization.TableCatalog;
import io.lakestream.api.materialization.TableCatalogType;
import io.lakestream.api.materialization.TableIdentifier;
import io.lakestream.api.materialization.TableMaterializationPolicy;
import io.lakestream.api.materialization.TableNaming;
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
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class StreamImplEffectiveMaterializationTest {

    private static final Version DUMMY_VERSION =
        new Version(1, 0, 0, 0, Optional.empty(), Optional.empty());
    private static final TableCatalog ICEBERG_PROD = new TableCatalog(
        "iceberg-prod", TableCatalogType.ICEBERG,
        Map.of("uri", "https://catalog/"), Map.of());

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
            (name, logId, reader) -> null, null,
            key -> CompletableFuture.completedFuture(nextStreamId++),
            key -> CompletableFuture.completedFuture(-1L),
            (key, expectedStreamId) -> CompletableFuture.completedFuture(null),
            null, null, List.of());
        catalog.initialize("test-catalog", Map.of()).join();
        streamId = new StreamIdentifier("ns-1", "topic-a");

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

    private static TableMaterializationPolicy streamPolicyWithCatalog() {
        return new TableMaterializationPolicy(
            Optional.of("iceberg-prod"),
            Optional.empty(),
            Optional.of(new TableIdentifier("ns-1", "topic-a")),
            Optional.of(true),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Map.of());
    }

    private static TableMaterializationPolicy namespacePolicyWithNaming() {
        return new TableMaterializationPolicy(
            Optional.of("iceberg-prod"),
            Optional.of(new TableNaming(Optional.of("warehouse"), "${stream.name}_table")),
            Optional.empty(),
            Optional.of(true),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Map.of());
    }

    private Stream createStream(Optional<TableMaterializationPolicy> mat) throws Exception {
        Partitioning partitioning = new Partitioning(PartitioningStrategy.INDEXED,
            Map.of("numPartitions", "1"));
        return catalog.createStream(streamId, new StreamConfig(), partitioning,
            new SchemaConfig(), Map.of(), mat).get();
    }

    @Test
    void streamPolicyWithRegisteredCatalog_resolves() throws Exception {
        catalog.registerTableCatalog(ICEBERG_PROD).get();
        catalog.createNamespace(new Namespace("ns-1")).get();
        Stream stream = createStream(Optional.of(streamPolicyWithCatalog()));

        Optional<ResolvedMaterialization> resolved = stream.effectiveMaterialization();

        assertThat(resolved).isPresent();
        assertThat(resolved.get().catalog()).isEqualTo(ICEBERG_PROD);
        assertThat(resolved.get().effectivePolicy().catalogRef()).contains("iceberg-prod");
        assertThat(resolved.get().tableIdentifier()).isEqualTo(new TableIdentifier("ns-1", "topic-a"));
    }

    @Test
    void namespacePolicyAlone_derivesTableFromNaming() throws Exception {
        catalog.registerTableCatalog(ICEBERG_PROD).get();
        catalog.createNamespace(new Namespace("ns-1")).get();
        catalog.setNamespaceMaterialization("ns-1", namespacePolicyWithNaming()).get();
        Stream stream = createStream(Optional.empty());

        Optional<ResolvedMaterialization> resolved = stream.effectiveMaterialization();

        assertThat(resolved).isPresent();
        assertThat(resolved.get().tableIdentifier())
            .isEqualTo(new TableIdentifier("warehouse", "topic-a_table"));
        assertThat(resolved.get().effectivePolicy().catalogRef()).contains("iceberg-prod");
    }

    @Test
    void streamDisablesWhileNamespaceEnables_returnsEmpty() throws Exception {
        catalog.registerTableCatalog(ICEBERG_PROD).get();
        catalog.createNamespace(new Namespace("ns-1")).get();
        catalog.setNamespaceMaterialization("ns-1", namespacePolicyWithNaming()).get();

        TableMaterializationPolicy disable = new TableMaterializationPolicy(
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.of(false),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Map.of());
        Stream stream = createStream(Optional.of(disable));

        assertThat(stream.effectiveMaterialization()).isEmpty();
    }

    @Test
    void unregisteredCatalog_returnsEmpty() throws Exception {
        // Catalog "iceberg-prod" is intentionally NOT registered.
        catalog.createNamespace(new Namespace("ns-1")).get();
        Stream stream = createStream(Optional.of(streamPolicyWithCatalog()));

        assertThat(stream.effectiveMaterialization()).isEmpty();
    }

    @Test
    void noPolicyAtEitherLayer_returnsEmpty() throws Exception {
        catalog.createNamespace(new Namespace("ns-1")).get();
        Stream stream = createStream(Optional.empty());

        assertThat(stream.effectiveMaterialization()).isEmpty();
    }

    @Test
    void clusterDefaultPolicy_resolvesWhenNeitherStreamNorNamespaceHasOne() throws Exception {
        // No stream policy, no namespace policy — only a cluster-wide default. effectiveMaterialization
        // must fall back to it (the lowest-priority baseline) for a stream in any namespace.
        catalog.registerTableCatalog(ICEBERG_PROD).get();
        catalog.createNamespace(new Namespace("ns-1")).get();
        catalog.setClusterDefaultMaterialization(namespacePolicyWithNaming()).get();
        Stream stream = createStream(Optional.empty());

        Optional<ResolvedMaterialization> resolved = stream.effectiveMaterialization();

        assertThat(resolved).isPresent();
        assertThat(resolved.get().tableIdentifier())
            .isEqualTo(new TableIdentifier("warehouse", "topic-a_table"));
        assertThat(resolved.get().effectivePolicy().catalogRef()).contains("iceberg-prod");
    }

    @Test
    void namespacePolicyTakesPrecedenceOverClusterDefault() throws Exception {
        // A namespace-scoped policy must win over the cluster-wide default.
        catalog.registerTableCatalog(ICEBERG_PROD).get();
        catalog.createNamespace(new Namespace("ns-1")).get();
        catalog.setNamespaceMaterialization("ns-1", namespacePolicyWithNaming()).get();
        TableMaterializationPolicy clusterDefault = new TableMaterializationPolicy(
            Optional.of("iceberg-prod"),
            Optional.of(new TableNaming(Optional.of("cluster"), "${stream.name}_cluster")),
            Optional.empty(),
            Optional.of(true),
            Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty(),
            Map.of());
        catalog.setClusterDefaultMaterialization(clusterDefault).get();
        Stream stream = createStream(Optional.empty());

        Optional<ResolvedMaterialization> resolved = stream.effectiveMaterialization();

        assertThat(resolved).isPresent();
        // The namespace policy's naming wins, not the cluster default's.
        assertThat(resolved.get().tableIdentifier())
            .isEqualTo(new TableIdentifier("warehouse", "topic-a_table"));
    }
}
