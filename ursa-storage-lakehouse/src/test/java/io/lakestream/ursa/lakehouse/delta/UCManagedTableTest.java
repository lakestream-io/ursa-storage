/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.lakehouse.delta;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.databricks.sdk.WorkspaceClient;
import com.databricks.sdk.core.DatabricksConfig;
import com.databricks.sdk.core.oauth.OpenIDConnectEndpoints;
import com.databricks.sdk.service.catalog.TableInfo;
import com.databricks.sdk.service.catalog.TableOperation;
import io.delta.kernel.Snapshot;
import io.delta.kernel.Transaction;
import io.delta.kernel.TransactionCommitResult;
import io.delta.kernel.data.Row;
import io.delta.kernel.hook.PostCommitHook;
import io.delta.kernel.internal.SnapshotImpl;
import io.delta.kernel.transaction.UpdateTableTransactionBuilder;
import io.delta.kernel.types.LongType;
import io.delta.kernel.types.StringType;
import io.delta.kernel.types.StructType;
import io.delta.kernel.unitycatalog.UCCatalogManagedClient;
import io.lakestream.ursa.lakehouse.LakehouseConfiguration;
import io.lakestream.ursa.lakehouse.catalog.unity.DatabricksUnityCatalog;
import io.lakestream.ursa.lakehouse.catalog.unity.MockUnityCatalog;
import io.lakestream.ursa.lakehouse.catalog.unity.UnityCatalogApi;
import io.lakestream.ursa.lakehouse.iceberg.exception.SchemaEvolutionException;
import io.unitycatalog.client.model.StagingTableInfo;
import java.io.IOException;
import java.lang.reflect.Field;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class UCManagedTableTest {

    private static final String TOPIC = "ns/managed-test";

    private DatabricksUnityCatalog mockCatalogApi() throws IOException {
        DatabricksUnityCatalog catalogApi = mock(DatabricksUnityCatalog.class);
        when(catalogApi.isEnableUnityCatalog()).thenReturn(true);

        WorkspaceClient workspaceClient = mock(WorkspaceClient.class);
        DatabricksConfig databricksConfig = mock(DatabricksConfig.class);
        when(workspaceClient.config()).thenReturn(databricksConfig);
        OpenIDConnectEndpoints oidcEndpoints = mock(OpenIDConnectEndpoints.class);
        when(oidcEndpoints.getTokenEndpoint()).thenReturn("https://accounts.example.com/oidc/v1/token");
        when(databricksConfig.getOidcEndpoints()).thenReturn(oidcEndpoints);
        when(catalogApi.getWorkspaceClient()).thenReturn(Optional.of(workspaceClient));

        return catalogApi;
    }

    private LakehouseConfiguration createConfig() {
        Properties props = new Properties();
        props.put("storagePath", "/tmp/test");
        props.put("partitionKey", "none");
        props.put("unityCatalogUri", "https://example.databricks.com");
        props.put("unityCatalogName", "test-catalog");
        props.put("unityCatalogClientId", "client-id");
        props.put("unityCatalogClientSecret", "client-secret");
        props.put("deltaSupportManagedCommit", "true");
        return new LakehouseConfiguration(props);
    }

    private UCManagedTable createTableWithMock(DatabricksUnityCatalog catalogApi) {
        LakehouseConfiguration config = createConfig();
        try (MockedStatic<UnityCatalogApi> mockedApi = mockStatic(UnityCatalogApi.class)) {
            mockedApi.when(() -> UnityCatalogApi.getInstance(config)).thenReturn(catalogApi);
            return new UCManagedTable(config, TOPIC);
        }
    }

    private UCManagedTable createTableWithMockAndTableInfo(DatabricksUnityCatalog catalogApi,
                                                           TableInfo tableInfo) {
        LakehouseConfiguration config = createConfig();
        MockUnityCatalog.MockedGenerateTemporaryTableCredentialResponse cred =
            new MockUnityCatalog.MockedGenerateTemporaryTableCredentialResponse();
        cred.setExpirationTime(Long.MAX_VALUE);
        when(catalogApi.getTemporaryTableCredentials(anyString(), any(TableOperation.class)))
            .thenReturn(cred);

        try (MockedStatic<UnityCatalogApi> mockedApi = mockStatic(UnityCatalogApi.class)) {
            mockedApi.when(() -> UnityCatalogApi.getInstance(config)).thenReturn(catalogApi);
            return new UCManagedTable(config, TOPIC, tableInfo);
        }
    }

    @Test
    void testConstructorRequiresUnityCatalogEnabled() {
        LakehouseConfiguration config = createConfig();
        DatabricksUnityCatalog catalogApi = mock(DatabricksUnityCatalog.class);
        when(catalogApi.isEnableUnityCatalog()).thenReturn(false);

        try (MockedStatic<UnityCatalogApi> mockedApi = mockStatic(UnityCatalogApi.class)) {
            mockedApi.when(() -> UnityCatalogApi.getInstance(config)).thenReturn(catalogApi);
            assertThrows(IllegalArgumentException.class,
                () -> new UCManagedTable(config, TOPIC));
        }
    }

    @Test
    void testConstructorRequiresDatabricksUnityCatalog() {
        LakehouseConfiguration config = createConfig();
        UnityCatalogApi nonDatabricksCatalog = mock(UnityCatalogApi.class);
        when(nonDatabricksCatalog.isEnableUnityCatalog()).thenReturn(true);

        try (MockedStatic<UnityCatalogApi> mockedApi = mockStatic(UnityCatalogApi.class)) {
            mockedApi.when(() -> UnityCatalogApi.getInstance(config)).thenReturn(nonDatabricksCatalog);
            IllegalStateException ex = assertThrows(IllegalStateException.class,
                () -> new UCManagedTable(config, TOPIC));
            assertTrue(ex.getMessage().contains("DatabricksUnityCatalog"));
        }
    }

    @Test
    void testConstructorRequiresWorkspaceClient() {
        LakehouseConfiguration config = createConfig();
        DatabricksUnityCatalog catalogApi = mock(DatabricksUnityCatalog.class);
        when(catalogApi.isEnableUnityCatalog()).thenReturn(true);
        when(catalogApi.getWorkspaceClient()).thenReturn(Optional.empty());

        try (MockedStatic<UnityCatalogApi> mockedApi = mockStatic(UnityCatalogApi.class)) {
            mockedApi.when(() -> UnityCatalogApi.getInstance(config)).thenReturn(catalogApi);
            IllegalStateException ex = assertThrows(IllegalStateException.class,
                () -> new UCManagedTable(config, TOPIC));
            assertTrue(ex.getMessage().contains("WorkspaceClient"));
        }
    }

    @Test
    void testConstructorRequiresOidcEndpoints() throws IOException {
        LakehouseConfiguration config = createConfig();
        DatabricksUnityCatalog catalogApi = mock(DatabricksUnityCatalog.class);
        when(catalogApi.isEnableUnityCatalog()).thenReturn(true);
        WorkspaceClient workspaceClient = mock(WorkspaceClient.class);
        DatabricksConfig databricksConfig = mock(DatabricksConfig.class);
        when(workspaceClient.config()).thenReturn(databricksConfig);
        when(databricksConfig.getOidcEndpoints()).thenReturn(null);
        when(catalogApi.getWorkspaceClient()).thenReturn(Optional.of(workspaceClient));

        try (MockedStatic<UnityCatalogApi> mockedApi = mockStatic(UnityCatalogApi.class)) {
            mockedApi.when(() -> UnityCatalogApi.getInstance(config)).thenReturn(catalogApi);
            IllegalStateException ex = assertThrows(IllegalStateException.class,
                () -> new UCManagedTable(config, TOPIC));
            assertTrue(ex.getMessage().contains("OIDC"));
        }
    }

    @Test
    void testTableExistsReturnsFalseWhenUnityTableNull() throws IOException {
        DatabricksUnityCatalog catalogApi = mockCatalogApi();
        UCManagedTable table = createTableWithMock(catalogApi);
        assertFalse(table.tableExists());
    }

    @Test
    void testTableExistsReturnsTrueWhenUnityTableSet() throws IOException {
        DatabricksUnityCatalog catalogApi = mockCatalogApi();
        TableInfo tableInfo = new TableInfo();
        tableInfo.setTableId("table-123");
        tableInfo.setStorageLocation("/tmp/test/table");
        UCManagedTable table = createTableWithMockAndTableInfo(catalogApi, tableInfo);
        assertTrue(table.tableExists());
    }

    @Test
    void testCreateDeltaTableSkipsWhenTableExists() throws IOException {
        DatabricksUnityCatalog catalogApi = mockCatalogApi();
        TableInfo tableInfo = new TableInfo();
        tableInfo.setTableId("table-123");
        tableInfo.setStorageLocation("/tmp/test/table");
        UCManagedTable table = createTableWithMockAndTableInfo(catalogApi, tableInfo);

        StructType schema = new StructType()
            .add("id", LongType.LONG);

        // Should return early without calling createStagingTable
        table.createDeltaTable(null, schema);
        verify(catalogApi, never()).createStagingTable(anyString(), any());
    }

    @Test
    void testCreateDeltaTableCallsStagingAndManagedTable() throws IOException {
        DatabricksUnityCatalog catalogApi = mockCatalogApi();
        UCManagedTable table = createTableWithMock(catalogApi);

        StagingTableInfo stagingTable = new StagingTableInfo();
        stagingTable.setId("staging-123");
        stagingTable.setStagingLocation("file:/tmp/staging/table");
        when(catalogApi.createStagingTable(anyString(), any())).thenReturn(stagingTable);

        MockUnityCatalog.MockedGenerateTemporaryTableCredentialResponse cred =
            new MockUnityCatalog.MockedGenerateTemporaryTableCredentialResponse();
        cred.setExpirationTime(Long.MAX_VALUE);
        when(catalogApi.getTemporaryTableCredentials(anyString(), any(TableOperation.class)))
            .thenReturn(cred);

        TableInfo managedTable = new TableInfo();
        managedTable.setTableId("managed-123");
        managedTable.setStorageLocation("file:/tmp/test/managed");
        when(catalogApi.createManagedTable(anyString(), any(), anyString(), anyString(), any()))
            .thenReturn(managedTable);

        StructType schema = new StructType()
            .add("id", LongType.LONG)
            .add("name", StringType.STRING);

        table.createDeltaTable(null, schema);

        verify(catalogApi).createStagingTable(anyString(), any());
        verify(catalogApi).createManagedTable(anyString(), any(), eq("staging-123"), anyString(), any());
        assertTrue(table.tableExists());
    }

    @Test
    void testCreateDeltaTableWithSchemaVersion() throws IOException {
        DatabricksUnityCatalog catalogApi = mockCatalogApi();
        UCManagedTable table = createTableWithMock(catalogApi);

        StagingTableInfo stagingTable = new StagingTableInfo();
        stagingTable.setId("staging-456");
        stagingTable.setStagingLocation("file:/tmp/staging/table2");
        when(catalogApi.createStagingTable(anyString(), any())).thenReturn(stagingTable);

        MockUnityCatalog.MockedGenerateTemporaryTableCredentialResponse cred =
            new MockUnityCatalog.MockedGenerateTemporaryTableCredentialResponse();
        cred.setExpirationTime(Long.MAX_VALUE);
        when(catalogApi.getTemporaryTableCredentials(anyString(), any(TableOperation.class)))
            .thenReturn(cred);

        TableInfo managedTable = new TableInfo();
        managedTable.setTableId("managed-456");
        managedTable.setStorageLocation("file:/tmp/test/managed2");
        when(catalogApi.createManagedTable(anyString(), any(), anyString(), anyString(), any()))
            .thenReturn(managedTable);

        StructType schema = new StructType()
            .add("id", LongType.LONG);

        table.createDeltaTable(1L, schema);

        verify(catalogApi).createStagingTable(anyString(), any());
        assertTrue(table.tableExists());
    }

    @Test
    void testGetLatestSnapshotReturnsNullWhenTableNotCreated() throws IOException {
        DatabricksUnityCatalog catalogApi = mockCatalogApi();
        UCManagedTable table = createTableWithMock(catalogApi);
        assertNull(table.getLatestSnapshot());
    }

    @Test
    void testGetLatestSnapshotDelegatesToManagedClient() throws Exception {
        DatabricksUnityCatalog catalogApi = mockCatalogApi();
        TableInfo tableInfo = new TableInfo();
        tableInfo.setTableId("table-789");
        tableInfo.setStorageLocation("/tmp/test/table");
        UCManagedTable table = createTableWithMockAndTableInfo(catalogApi, tableInfo);

        // Access the ucCatalogManagedClient field to mock it
        UCCatalogManagedClient mockClient = mock(UCCatalogManagedClient.class);
        SnapshotImpl mockSnapshot = mock(SnapshotImpl.class);
        when(mockClient.loadSnapshot(any(), eq("table-789"), anyString(), any(), any()))
            .thenReturn(mockSnapshot);

        Field clientField = UCManagedTable.class.getDeclaredField("ucCatalogManagedClient");
        clientField.setAccessible(true);
        clientField.set(table, mockClient);

        Snapshot result = table.getLatestSnapshot();
        assertNotNull(result);
        assertEquals(mockSnapshot, result);
    }

    @Test
    void testCommitSnapshotEmptyActionsReturnsEarly() throws Exception {
        DatabricksUnityCatalog catalogApi = mockCatalogApi();
        TableInfo tableInfo = new TableInfo();
        tableInfo.setTableId("table-commit");
        tableInfo.setStorageLocation("/tmp/test/table");
        UCManagedTable table = createTableWithMockAndTableInfo(catalogApi, tableInfo);

        // Should return without error
        table.commitSnapshot(Collections.emptyList());
    }

    @Test
    void testCommitSnapshotWithActions() throws Exception {
        DatabricksUnityCatalog catalogApi = mockCatalogApi();
        TableInfo tableInfo = new TableInfo();
        tableInfo.setTableId("table-commit2");
        tableInfo.setStorageLocation("/tmp/test/table");
        UCManagedTable table = createTableWithMockAndTableInfo(catalogApi, tableInfo);

        // Mock the managed client
        UCCatalogManagedClient mockClient = mock(UCCatalogManagedClient.class);
        SnapshotImpl mockSnapshot = mock(SnapshotImpl.class);
        when(mockClient.loadSnapshot(any(), anyString(), anyString(), any(), any()))
            .thenReturn(mockSnapshot);

        Transaction mockTxn = mock(Transaction.class);
        TransactionCommitResult mockResult = mock(TransactionCommitResult.class);
        when(mockResult.getPostCommitHooks()).thenReturn(Collections.emptyList());
        when(mockResult.getVersion()).thenReturn(1L);
        when(mockTxn.commit(any(), any())).thenReturn(mockResult);

        UpdateTableTransactionBuilder mockBuilder = mock(UpdateTableTransactionBuilder.class);
        when(mockBuilder.build(any())).thenReturn(mockTxn);
        when(mockSnapshot.buildUpdateTableTransaction(anyString(), any(io.delta.kernel.Operation.class)))
            .thenReturn(mockBuilder);

        Field clientField = UCManagedTable.class.getDeclaredField("ucCatalogManagedClient");
        clientField.setAccessible(true);
        clientField.set(table, mockClient);

        Row addAction = DeltaTableUtils.buildAddFileAction(
            "test-file.parquet", 1024, System.currentTimeMillis(),
            Collections.emptyMap(), true, null, Collections.emptyMap());

        table.commitSnapshot(List.of(addAction));

        verify(mockTxn).commit(any(), any());
    }

    @Test
    void testCommitSnapshotProcessesCheckpointHooks() throws Exception {
        DatabricksUnityCatalog catalogApi = mockCatalogApi();
        TableInfo tableInfo = new TableInfo();
        tableInfo.setTableId("table-hooks");
        tableInfo.setStorageLocation("/tmp/test/table");
        UCManagedTable table = createTableWithMockAndTableInfo(catalogApi, tableInfo);

        UCCatalogManagedClient mockClient = mock(UCCatalogManagedClient.class);
        SnapshotImpl mockSnapshot = mock(SnapshotImpl.class);
        when(mockClient.loadSnapshot(any(), anyString(), anyString(), any(), any()))
            .thenReturn(mockSnapshot);

        Transaction mockTxn = mock(Transaction.class);
        TransactionCommitResult mockResult = mock(TransactionCommitResult.class);

        PostCommitHook nonCheckpointHook = mock(PostCommitHook.class);
        when(mockResult.getPostCommitHooks()).thenReturn(List.of(nonCheckpointHook));
        when(mockResult.getVersion()).thenReturn(2L);
        when(mockTxn.commit(any(), any())).thenReturn(mockResult);

        UpdateTableTransactionBuilder mockBuilder = mock(UpdateTableTransactionBuilder.class);
        when(mockBuilder.build(any())).thenReturn(mockTxn);
        when(mockSnapshot.buildUpdateTableTransaction(anyString(), any(io.delta.kernel.Operation.class)))
            .thenReturn(mockBuilder);

        Field clientField = UCManagedTable.class.getDeclaredField("ucCatalogManagedClient");
        clientField.setAccessible(true);
        clientField.set(table, mockClient);

        Row addAction = DeltaTableUtils.buildAddFileAction(
            "test-file.parquet", 1024, System.currentTimeMillis(),
            Collections.emptyMap(), true, null, Collections.emptyMap());

        // Should not throw even with non-checkpoint hooks
        table.commitSnapshot(List.of(addAction));
    }

    @Test
    void testEvolveSchemaWithVersionThrowsSchemaEvolutionException() throws IOException {
        DatabricksUnityCatalog catalogApi = mockCatalogApi();
        UCManagedTable table = createTableWithMock(catalogApi);

        StructType schema = new StructType()
            .add("id", LongType.LONG);

        assertThrows(SchemaEvolutionException.class,
            () -> table.evolveSchemaWithVersion(1L, schema));
    }

    @Test
    void testConstructorWithTableInfoSetsFields() throws IOException {
        DatabricksUnityCatalog catalogApi = mockCatalogApi();
        TableInfo tableInfo = new TableInfo();
        tableInfo.setTableId("table-fields");
        tableInfo.setStorageLocation("s3://bucket/path/to/table");
        UCManagedTable table = createTableWithMockAndTableInfo(catalogApi, tableInfo);

        assertTrue(table.tableExists());
        // s3:// should be normalized to s3a://
        assertEquals("s3a://bucket/path/to/table", table.getTableLocation());
    }

    @Test
    void testBuildAddFileActionIteratesDeltaFiles() throws IOException {
        DatabricksUnityCatalog catalogApi = mockCatalogApi();
        UCManagedTable table = createTableWithMock(catalogApi);

        io.lakestream.ursa.lakehouse.writer.ParquetFileStat fileStat =
            io.lakestream.ursa.lakehouse.writer.ParquetFileStat.builder()
                .filePath("outer.parquet")
                .fileSize(2048L)
                .partitionValues(Collections.emptyMap())
                .stats(null)
                .tags(Collections.emptyMap())
                .deltaFiles(Collections.emptyList())
                .build();

        // UCTable.buildAddFileAction iterates deltaFiles, which is empty
        List<Row> rows = table.buildAddFileAction(List.of(fileStat));
        assertTrue(rows.isEmpty());
    }

    @Test
    void testBuildAddFileActionWithDeltaFiles() throws IOException {
        DatabricksUnityCatalog catalogApi = mockCatalogApi();
        UCManagedTable table = createTableWithMock(catalogApi);

        io.lakestream.ursa.lakehouse.writer.ParquetFileStat innerFile =
            io.lakestream.ursa.lakehouse.writer.ParquetFileStat.builder()
                .filePath("inner.parquet")
                .fileSize(512L)
                .partitionValues(Collections.emptyMap())
                .stats(null)
                .tags(Collections.emptyMap())
                .deltaFiles(Collections.emptyList())
                .build();

        io.lakestream.ursa.lakehouse.writer.ParquetFileStat outerFile =
            io.lakestream.ursa.lakehouse.writer.ParquetFileStat.builder()
                .filePath("outer.parquet")
                .fileSize(2048L)
                .partitionValues(Collections.emptyMap())
                .stats(null)
                .tags(Collections.emptyMap())
                .deltaFiles(List.of(innerFile))
                .build();

        List<Row> rows = table.buildAddFileAction(List.of(outerFile));
        assertEquals(1, rows.size());
    }
}
