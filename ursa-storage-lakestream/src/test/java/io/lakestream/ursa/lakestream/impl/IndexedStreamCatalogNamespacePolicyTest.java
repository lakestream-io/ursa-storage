/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.lakestream.impl;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.lakestream.api.CatalogPaths;
import io.lakestream.api.LogStorage;
import io.lakestream.api.Namespace;
import io.lakestream.api.exception.NoSuchNamespaceException;
import io.lakestream.api.materialization.FrameworkConf;
import io.lakestream.api.materialization.TableMaterializationPolicy;
import io.lakestream.api.materialization.TableNaming;
import io.lakestream.api.materialization.WriteMode;
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
class IndexedStreamCatalogNamespacePolicyTest {

    private static final Version DUMMY_VERSION =
        new Version(1, 0, 0, 0, Optional.empty(), Optional.empty());
    private static final ObjectMapper MAPPER = new ObjectMapper();

    @Mock
    private AsyncOxiaClient oxiaClient;
    @Mock
    private LogStorage logStorage;

    private CatalogPaths catalogPaths;
    private IndexedStreamCatalog catalog;
    private final Map<String, byte[]> store = new HashMap<>();

    @BeforeEach
    void setUp() {
        catalogPaths = new DefaultCatalogPaths();
        catalog = new IndexedStreamCatalog(oxiaClient, catalogPaths, logStorage,
            logId -> null, null,
            key -> CompletableFuture.completedFuture(1L),
            null, null, List.of());
        catalog.initialize("test-catalog", Map.of()).join();

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
            Optional.of(new TableNaming(Optional.of("warehouse"),
                "${stream.namespace}_${stream.name}")),
            Optional.empty(),
            Optional.of(true),
            Optional.of(new FrameworkConf(
                Optional.of(WriteMode.APPEND),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty())),
            Optional.empty(),
            Optional.of(List.of("id")),
            Optional.of(1L),
            Optional.empty(),
            Map.of("warehouse", "s3://override"));
    }

    @Test
    void createNamespace_defaultMaterializationIsEmpty() throws Exception {
        catalog.createNamespace(new Namespace("ns-1", Map.of("k", "v"))).get();

        Namespace loaded = catalog.loadNamespaceMetadata("ns-1").get();
        assertThat(loaded.name()).isEqualTo("ns-1");
        assertThat(loaded.properties()).isEqualTo(Map.of("k", "v"));
        assertThat(loaded.materialization()).isEmpty();
    }

    @Test
    void setAndLoadNamespaceMaterialization_roundTrip() throws Exception {
        catalog.createNamespace(new Namespace("ns-1")).get();
        TableMaterializationPolicy policy = samplePolicy();

        catalog.setNamespaceMaterialization("ns-1", policy).get();

        Namespace loaded = catalog.loadNamespaceMetadata("ns-1").get();
        assertThat(loaded.materialization()).contains(policy);
    }

    @Test
    void clearNamespaceMaterialization_makesItEmpty() throws Exception {
        catalog.createNamespace(new Namespace("ns-1")).get();
        catalog.setNamespaceMaterialization("ns-1", samplePolicy()).get();
        catalog.clearNamespaceMaterialization("ns-1").get();

        Namespace loaded = catalog.loadNamespaceMetadata("ns-1").get();
        assertThat(loaded.materialization()).isEmpty();
    }

    @Test
    void setNamespaceMaterialization_unknownNamespaceFails() {
        TableMaterializationPolicy policy = samplePolicy();

        assertThatThrownBy(() ->
                catalog.setNamespaceMaterialization("missing", policy).join())
            .isInstanceOf(CompletionException.class)
            .hasCauseInstanceOf(NoSuchNamespaceException.class);
    }

    @Test
    void clearNamespaceMaterialization_unknownNamespaceFails() {
        assertThatThrownBy(() -> catalog.clearNamespaceMaterialization("missing").join())
            .isInstanceOf(CompletionException.class)
            .hasCauseInstanceOf(NoSuchNamespaceException.class);
    }

    @Test
    void legacyMapShapeDeserialisesWithEmptyMaterialization() throws Exception {
        // Inject a legacy "Map<String, String>" namespace record directly. We expect
        // the deserializer to tolerate it and return materialization = Optional.empty().
        String path = catalogPaths.namespacePath("legacy-ns");
        byte[] legacy = MAPPER.writeValueAsBytes(Map.of("env", "staging", "team", "data"));
        store.put(path, legacy);

        Namespace loaded = catalog.loadNamespaceMetadata("legacy-ns").get();
        assertThat(loaded.name()).isEqualTo("legacy-ns");
        assertThat(loaded.properties()).isEqualTo(Map.of("env", "staging", "team", "data"));
        assertThat(loaded.materialization()).isEmpty();
    }

    @Test
    void setNamespaceProperties_preservesMaterialization() throws Exception {
        catalog.createNamespace(new Namespace("ns-1")).get();
        catalog.setNamespaceMaterialization("ns-1", samplePolicy()).get();
        catalog.setNamespaceProperties("ns-1", Map.of("k", "v")).get();

        Namespace loaded = catalog.loadNamespaceMetadata("ns-1").get();
        assertThat(loaded.properties()).isEqualTo(Map.of("k", "v"));
        assertThat(loaded.materialization()).contains(samplePolicy());
    }
}
