/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.lakehouse.catalog.unity;

import com.databricks.sdk.core.UserAgent;
import com.databricks.sdk.service.catalog.CatalogInfo;
import com.databricks.sdk.service.catalog.ColumnInfo;
import com.databricks.sdk.service.catalog.ColumnTypeName;
import com.databricks.sdk.service.catalog.DataSourceFormat;
import com.databricks.sdk.service.catalog.GenerateTemporaryTableCredentialResponse;
import com.databricks.sdk.service.catalog.TableInfo;
import com.databricks.sdk.service.catalog.TableOperation;
import com.databricks.sdk.service.catalog.TableType;
import io.delta.kernel.types.StructType;
import io.lakestream.ursa.lakehouse.LakehouseConfiguration;
import io.unitycatalog.client.model.StagingTableInfo;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;


@Slf4j
public class MockUnityCatalog implements UnityCatalogApi {

    private static final ConcurrentHashMap<String, MockUnityCatalog> MOCK_CATALOGS = new ConcurrentHashMap<>();

    public static MockUnityCatalog getInstance(LakehouseConfiguration config) {
        if (StringUtils.isNotBlank(config.getUnityCatalogUserAgent())) {
            UserAgent.withPartner(config.getUnityCatalogUserAgent());
        }
        return MOCK_CATALOGS.computeIfAbsent(buildCacheKey(config), key -> new MockUnityCatalog(config));
    }

    public static void resetInstance() {
        MOCK_CATALOGS.clear();
    }

    private static String buildCacheKey(LakehouseConfiguration config) {
        return String.join("|",
            StringUtils.defaultString(config.getMockedUnityCatalogRootStorage()),
            StringUtils.defaultString(config.getStoragePath()));
    }

    private final Map<String, TableInfo> mockTables = new ConcurrentHashMap<>();

    private final String storageRoot;

    private MockUnityCatalog(LakehouseConfiguration config) {
        if (StringUtils.isNotBlank(config.getMockedUnityCatalogRootStorage())) {
            storageRoot = config.getMockedUnityCatalogRootStorage();
        } else {
            storageRoot = config.getStoragePath();
        }
    }

    @Override
    public boolean isEnableUnityCatalog() {
        return true;
    }

    @Override
    public TableInfo createExternalTable(String catalogName, UnityTableIdentifier identifier, String location,
                                         StructType structType) {
        List<ColumnInfo> columnInfos = UnityCatalogUtil.convertDeltaSchemaToColumns(structType);
        List<com.databricks.sdk.service.catalog.ColumnInfo> columnList =
            columnInfos.stream().map(ele -> {
                com.databricks.sdk.service.catalog.ColumnInfo columnInfo =
                    new com.databricks.sdk.service.catalog.ColumnInfo();
                columnInfo.setName(ele.getName());
                columnInfo.setTypeText(ele.getTypeText());
                columnInfo.setTypeJson(ele.getTypeJson());
                if (ele.getTypeName() != null) {
                    columnInfo.setTypeName(ColumnTypeName.valueOf(ele.getTypeName().name()));
                }
                if (ele.getTypePrecision() != null) {
                    columnInfo.setTypePrecision(Long.valueOf(ele.getTypePrecision()));
                }
                if (ele.getTypeScale() != null) {
                    columnInfo.setTypeScale(Long.valueOf(ele.getTypeScale()));
                }
                columnInfo.setTypeIntervalType(ele.getTypeIntervalType());
                if (ele.getPosition() != null) {
                    columnInfo.setPosition(Long.valueOf(ele.getPosition()));
                }
                columnInfo.setComment(ele.getComment());
                columnInfo.setNullable(ele.getNullable());
                if (ele.getPartitionIndex() != null) {
                    columnInfo.setPartitionIndex(Long.valueOf(ele.getPartitionIndex()));
                }
                return columnInfo;
            }).toList();

        TableInfo mockTableInfo = new TableInfo().setName(identifier.getTable()).setCatalogName(catalogName)
            .setSchemaName(identifier.getSchema())
            .setDataSourceFormat(DataSourceFormat.DELTA)
            .setColumns(columnList)
            .setTableType(TableType.EXTERNAL).setStorageLocation(location).setCreatedBy("mock_user")
            .setCreatedAt(System.currentTimeMillis());
        mockTables.put(identifier.getTableFullName(catalogName), mockTableInfo);
        return mockTableInfo;
    }

    @Override
    public TableInfo createManagedTable(String catalogName, UnityTableIdentifier identifier, String tableId,
                                        String location, StructType structType) {
        return null;
    }

    @Override
    public StagingTableInfo createStagingTable(String catalogName, UnityTableIdentifier identifier) {
        return null;
    }

    @Override
    public Optional<TableInfo> getTable(String catalogName, UnityTableIdentifier identifier) {
        return Optional.ofNullable(mockTables.get(identifier.getTableFullName(catalogName)));
    }

    @Override
    public void updateExternalTable(String catalogName, UnityTableIdentifier identifier, String location,
                                    StructType structType) {
        mockTables.remove(identifier.getTableFullName(catalogName));

        createExternalTable(catalogName, identifier, location, structType);
    }

    @Override
    public Optional<CatalogInfo> getCatalog(String catalogName) {
        CatalogInfo mockedCatalog = new CatalogInfo();
        mockedCatalog.setName(StringUtils.defaultIfBlank(catalogName, "mockCatalog"));
        mockedCatalog.setStorageRoot(storageRoot);
        return Optional.of(mockedCatalog);
    }

    @Override
    public GenerateTemporaryTableCredentialResponse getTemporaryTableCredentials(String tableId,
                                                                                 TableOperation operation) {
        MockedGenerateTemporaryTableCredentialResponse credentialResponse =
            new MockedGenerateTemporaryTableCredentialResponse();
        credentialResponse.setExpirationTime(Long.MAX_VALUE);
        return credentialResponse;
    }


    @Override
    public void createOrUpdateExternalLineage(UnityCatalogExternalLineageRequest request) {
        log.debug("Mock: createOrUpdateExternalLineage for topic {} with table {}",
                request.getTopicName(), request.getTableFullName());
    }

    public static class MockedGenerateTemporaryTableCredentialResponse
        extends GenerateTemporaryTableCredentialResponse {

    }
}
