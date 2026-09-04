/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.lakehouse.cleaner;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.lakestream.api.materialization.ResolvedMaterialization;
import io.lakestream.api.materialization.TableCatalog;
import io.lakestream.api.materialization.TableCatalogType;
import io.lakestream.api.materialization.TableConf;
import io.lakestream.api.materialization.TableIdentifier;
import io.lakestream.api.materialization.TableMaterializationPolicy;
import io.lakestream.api.materialization.TableMode;
import io.lakestream.ursa.lakehouse.LakehouseConfiguration;
import io.lakestream.ursa.lakehouse.utils.StreamTableNaming;
import io.lakestream.ursa.storage.FileStorage;
import io.lakestream.ursa.storage.StorageApi;
import io.lakestream.ursa.storage.impl.StorageConfig;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

class CompactedDataCleanupHandlerTest {

    private CompactedDataCleanupHandler handler;

    @AfterEach
    void tearDown() {
        if (handler != null) {
            handler.stop();
        }
    }

    @Test
    void managedPolicyUsesResolvedCatalogIdentifierInsteadOfStorageName() {
        StorageConfig storageConfig = storageConfig("EXTERNAL", "DELTA");
        TableIdentifier destination = new TableIdentifier("archive", "orders_history");
        ResolvedMaterialization resolved = resolvedMaterialization(TableMode.MANAGED, destination);
        handler = new CompactedDataCleanupHandler(
                storageConfig, mock(StorageApi.class), mock(FileStorage.class),
                __ -> Optional.of(resolved));
        TopicCleanupTask task = new TopicCleanupTask(
                "default/orders-topic-id-abc-partition-2", 1L, 10L);

        LakehouseConfiguration cleanupConfig = handler.getLakehouseConfiguration(task).orElseThrow();

        assertEquals(LakehouseConfiguration.StreamTableMode.MANAGED,
                cleanupConfig.getStreamTableMode());
        assertEquals(LakehouseConfiguration.LakehouseType.ICEBERG,
                cleanupConfig.getLakehouseType());
        assertEquals(destination,
                StreamTableNaming.resolve(task.getCompactionTopic(), cleanupConfig.getProperties()));
    }

    @Test
    void externalPolicyDoesNotCreateManagedTableCommitterConfiguration() {
        StorageConfig storageConfig = storageConfig("MANAGED", "ICEBERG");
        ResolvedMaterialization resolved = resolvedMaterialization(
                TableMode.EXTERNAL, new TableIdentifier("default", "orders"));
        handler = new CompactedDataCleanupHandler(
                storageConfig, mock(StorageApi.class), mock(FileStorage.class),
                __ -> Optional.of(resolved));

        assertTrue(handler.getLakehouseConfiguration(
                new TopicCleanupTask("default/orders-id-partition-0", 1L, 10L)).isEmpty());
    }

    private static StorageConfig storageConfig(String mode, String lakehouseType) {
        Properties properties = new Properties();
        properties.setProperty("streamTableMode", mode);
        properties.setProperty("lakehouseType", lakehouseType);
        StorageConfig config = mock(StorageConfig.class);
        when(config.getProperties()).thenReturn(properties);
        when(config.getCompactedDataCleanupThreadNum()).thenReturn(1);
        return config;
    }

    private static ResolvedMaterialization resolvedMaterialization(
            TableMode mode, TableIdentifier identifier) {
        TableCatalog catalog = new TableCatalog(
                "resolved-catalog", TableCatalogType.ICEBERG, Map.of(), Map.of());
        TableConf table = new TableConf(
                Optional.of(mode), Optional.empty(), Optional.empty(),
                Optional.empty(), Optional.empty(), Optional.empty());
        TableMaterializationPolicy policy = new TableMaterializationPolicy(
                Optional.of(catalog.name()), Optional.empty(), Optional.of(identifier),
                Optional.of(Boolean.TRUE), Optional.empty(), Optional.empty(),
                Optional.empty(), Optional.empty(), Optional.of(table), Map.of());
        return new ResolvedMaterialization(catalog, identifier, policy);
    }
}
