/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.lakehouse.delta;

import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.databricks.sdk.service.catalog.CatalogInfo;
import com.databricks.sdk.service.catalog.TableInfo;
import com.databricks.sdk.service.catalog.TableType;
import io.lakestream.ursa.lakehouse.LakehouseConfiguration;
import io.lakestream.ursa.lakehouse.catalog.unity.MockUnityCatalog;
import io.lakestream.ursa.lakehouse.catalog.unity.UnityCatalogApi;
import java.util.Optional;
import java.util.Properties;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class ExternalDeltaTableFactoryTest {

    @Mock
    private UnityCatalogApi unityCatalogApi;

    @Test
    void testGetUCTableThrowsWhenUnityCatalogDisabled() {
        LakehouseConfiguration config = new LakehouseConfiguration();
        try (MockedStatic<UnityCatalogApi> mockedApi = mockStatic(UnityCatalogApi.class)) {
            mockedApi.when(() -> UnityCatalogApi.getInstance(config)).thenReturn(unityCatalogApi);
            when(unityCatalogApi.isEnableUnityCatalog()).thenReturn(false);

            assertThrows(IllegalArgumentException.class,
                () -> ExternalDeltaTableFactory.getDeltaTable(config, "test-topic"));
            }
    }

    @Test
    void testGetUCTableReturnsUCExternalTableWhenNoExistingTableAndManagedCommitDisabled() {
        Properties props = new Properties();
        props.put("mockUnityCatalog", "true");
        props.put("unityCatalogUri", "http://localhost:8080");
        props.put("unityCatalogName", "default-catalog");
        props.put("catalog.name", "topic-catalog");
        props.put("delta.catalog.topic-catalog.unityCatalogName", "topic-catalog");
        props.put("mockedUnityCatalogRootStorage", "/tmp/test");
        LakehouseConfiguration config = new LakehouseConfiguration(props);

        try (MockedStatic<UnityCatalogApi> mockedApi = mockStatic(UnityCatalogApi.class)) {
            mockedApi.when(() -> UnityCatalogApi.getInstance(config)).thenReturn(unityCatalogApi);
            when(unityCatalogApi.isEnableUnityCatalog()).thenReturn(true);
            when(unityCatalogApi.getTable(anyString(), any())).thenReturn(Optional.empty());
            CatalogInfo catalogInfo = new CatalogInfo();
            catalogInfo.setStorageRoot("/tmp/test");
            when(unityCatalogApi.getCatalog(anyString())).thenReturn(Optional.of(catalogInfo));

            ExternalDeltaTable result = ExternalDeltaTableFactory.getDeltaTable(config, "ns/topic");
            assertInstanceOf(UCExternalTable.class, result);
            verify(unityCatalogApi).getCatalog(eq("topic-catalog"));
        }
    }

    @Test
    void testGetUCTableReturnsUCExternalTableForExistingExternalTable() {
        Properties props = new Properties();
        props.put("mockUnityCatalog", "true");
        props.put("unityCatalogUri", "http://localhost:8080");
        props.put("unityCatalogName", "test-catalog");
        props.put("mockedUnityCatalogRootStorage", "/tmp/test");
        LakehouseConfiguration config = new LakehouseConfiguration(props);

        TableInfo externalTable = new TableInfo();
        externalTable.setTableType(TableType.EXTERNAL);
        externalTable.setStorageLocation("/tmp/test/table");
        externalTable.setTableId("table-id-1");

        MockUnityCatalog.MockedGenerateTemporaryTableCredentialResponse cred =
            new MockUnityCatalog.MockedGenerateTemporaryTableCredentialResponse();
        cred.setExpirationTime(Long.MAX_VALUE);

        try (MockedStatic<UnityCatalogApi> mockedApi = mockStatic(UnityCatalogApi.class)) {
            mockedApi.when(() -> UnityCatalogApi.getInstance(config)).thenReturn(unityCatalogApi);
            when(unityCatalogApi.isEnableUnityCatalog()).thenReturn(true);
            when(unityCatalogApi.getTable(anyString(), any())).thenReturn(Optional.of(externalTable));
            when(unityCatalogApi.getTemporaryTableCredentials(anyString(), any()))
                .thenReturn(cred);

            ExternalDeltaTable result = ExternalDeltaTableFactory.getDeltaTable(config, "ns/topic");
            assertInstanceOf(UCExternalTable.class, result);
        }
    }

    @Test
    void testGetDeltaTableReturnsDirectExternalTableWhenUnityCatalogDisabledInExternalMode() {
        Properties props = new Properties();
        props.put("streamTableMode", "EXTERNAL");
        props.put("directExternalStoragePath", "/tmp/test");
        LakehouseConfiguration config = new LakehouseConfiguration(props);

        try (MockedStatic<UnityCatalogApi> mockedApi = mockStatic(UnityCatalogApi.class)) {
            mockedApi.when(() -> UnityCatalogApi.getInstance(config)).thenReturn(unityCatalogApi);
            when(unityCatalogApi.isEnableUnityCatalog()).thenReturn(false);

            ExternalDeltaTable result = ExternalDeltaTableFactory.getDeltaTable(config, "ns/topic");
            assertInstanceOf(DirectExternalTable.class, result);
        }
    }

}
