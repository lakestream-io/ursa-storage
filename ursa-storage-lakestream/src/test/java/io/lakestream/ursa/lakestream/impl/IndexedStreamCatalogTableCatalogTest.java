/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.lakestream.impl;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

import io.lakestream.api.CatalogPaths;
import io.lakestream.api.LogStorage;
import io.lakestream.api.materialization.TableCatalog;
import io.lakestream.api.materialization.TableCatalogType;
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
class IndexedStreamCatalogTableCatalogTest {

    private static final Version DUMMY_VERSION =
        new Version(1, 0, 0, 0, Optional.empty(), Optional.empty());

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
        // In-memory backing for the keys we care about.
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

    @Test
    void registerAndGetTableCatalog() throws Exception {
        TableCatalog tc = new TableCatalog("iceberg-prod", TableCatalogType.ICEBERG,
            Map.of("uri", "https://catalog/", "warehouse", "s3://bucket"),
            Map.of("target-file-size-bytes", "134217728"));
        catalog.registerTableCatalog(tc).get();

        TableCatalog loaded = catalog.getTableCatalog("iceberg-prod").get();
        assertThat(loaded).isEqualTo(tc);
    }

    @Test
    void listTableCatalogs_includesRegisteredEntry() throws Exception {
        TableCatalog tc1 = new TableCatalog("iceberg-prod", TableCatalogType.ICEBERG,
            Map.of("uri", "https://catalog/"), Map.of());
        TableCatalog tc2 = new TableCatalog("delta-staging", TableCatalogType.DELTA,
            Map.of("path", "s3://staging"), Map.of("foo", "bar"));
        catalog.registerTableCatalog(tc1).get();
        catalog.registerTableCatalog(tc2).get();

        List<TableCatalog> all = catalog.listTableCatalogs().get();
        assertThat(all).containsExactlyInAnyOrder(tc1, tc2);
    }

    @Test
    void getTableCatalog_returnsNullWhenMissing() throws Exception {
        TableCatalog missing = catalog.getTableCatalog("nope").get();
        assertThat(missing).isNull();
    }

    @Test
    void unregisterTableCatalog_returnsTrueWhenExisting() throws Exception {
        TableCatalog tc = new TableCatalog("iceberg-prod", TableCatalogType.ICEBERG,
            Map.of(), Map.of());
        catalog.registerTableCatalog(tc).get();

        assertThat(catalog.unregisterTableCatalog("iceberg-prod").get()).isTrue();
        assertThat(catalog.getTableCatalog("iceberg-prod").get()).isNull();
    }

    @Test
    void unregisterTableCatalog_returnsFalseWhenMissing() throws Exception {
        assertThat(catalog.unregisterTableCatalog("nope").get()).isFalse();
    }

    @Test
    void registerTableCatalog_overwritesExisting() throws Exception {
        TableCatalog v1 = new TableCatalog("iceberg-prod", TableCatalogType.ICEBERG,
            Map.of("uri", "https://old/"), Map.of());
        TableCatalog v2 = new TableCatalog("iceberg-prod", TableCatalogType.ICEBERG,
            Map.of("uri", "https://new/"), Map.of("k", "v"));

        catalog.registerTableCatalog(v1).get();
        catalog.registerTableCatalog(v2).get();

        TableCatalog loaded = catalog.getTableCatalog("iceberg-prod").get();
        assertThat(loaded).isEqualTo(v2);
    }

    @Test
    void emptyListWhenNoneRegistered() throws Exception {
        // Avoid the byKey path entirely so we exercise the empty-keyspace branch.
        String prefix = catalogPaths.tableCatalogsPrefix();
        when(oxiaClient.list(eq(prefix), any(String.class)))
            .thenReturn(CompletableFuture.completedFuture(List.of()));

        List<TableCatalog> all = catalog.listTableCatalogs().get();
        assertThat(all).isEmpty();
    }
}
