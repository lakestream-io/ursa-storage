/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.lakehouse.catalog.unity;

import com.databricks.sdk.WorkspaceClient;
import com.databricks.sdk.core.ApiClient;
import com.databricks.sdk.core.DatabricksConfig;
import com.databricks.sdk.core.DatabricksException;
import com.databricks.sdk.core.UserAgent;
import com.databricks.sdk.core.http.Request;
import com.databricks.sdk.service.catalog.CatalogInfo;
import com.databricks.sdk.service.catalog.CreateExternalLineageRelationshipRequest;
import com.databricks.sdk.service.catalog.CreateExternalMetadataRequest;
import com.databricks.sdk.service.catalog.CreateRequestExternalLineage;
import com.databricks.sdk.service.catalog.CreateSchema;
import com.databricks.sdk.service.catalog.CreateTableRequest;
import com.databricks.sdk.service.catalog.DataSourceFormat;
import com.databricks.sdk.service.catalog.ExternalLineageExternalMetadata;
import com.databricks.sdk.service.catalog.ExternalLineageObject;
import com.databricks.sdk.service.catalog.ExternalLineageTable;
import com.databricks.sdk.service.catalog.ExternalMetadata;
import com.databricks.sdk.service.catalog.GenerateTemporaryTableCredentialRequest;
import com.databricks.sdk.service.catalog.GenerateTemporaryTableCredentialResponse;
import com.databricks.sdk.service.catalog.TableInfo;
import com.databricks.sdk.service.catalog.TableOperation;
import com.databricks.sdk.service.catalog.TableType;
import com.databricks.sdk.service.catalog.UpdateExternalMetadataRequest;
import com.google.common.annotations.VisibleForTesting;
import io.delta.kernel.types.StructType;
import io.lakestream.ursa.lakehouse.LakehouseConfiguration;
import io.unitycatalog.client.model.CreateStagingTable;
import io.unitycatalog.client.model.StagingTableInfo;
import java.io.IOException;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

@Slf4j
public class DatabricksUnityCatalog implements UnityCatalogApi {

    private static final ConcurrentHashMap<String, DatabricksUnityCatalog> CATALOGS = new ConcurrentHashMap<>();

    private boolean isEnableUnityCatalog;

    @Getter
    private final Optional<WorkspaceClient> workspaceClient;

    private DatabricksUnityCatalog(LakehouseConfiguration config) {
        if (StringUtils.isNotBlank(config.getUnityCatalogUri())) {
            DatabricksConfig databricksConfig = new DatabricksConfig();
            databricksConfig.setHost(config.getUnityCatalogUri());
            String unityCatalogClientId = config.getUnityCatalogClientId();
            String unityCatalogClientSecret = config.getUnityCatalogClientSecret();
            if (StringUtils.isNotBlank(unityCatalogClientId) && StringUtils.isNotBlank(unityCatalogClientSecret)) {
                databricksConfig.setClientId(config.getUnityCatalogClientId());
                databricksConfig.setClientSecret(config.getUnityCatalogClientSecret());
                databricksConfig.setAuthType("oauth-m2m");
            } else {
                databricksConfig.setToken(config.getUnityCatalogToken());
            }
            this.isEnableUnityCatalog = true;
            this.workspaceClient = Optional.of(new WorkspaceClient(databricksConfig));
        } else {
            this.workspaceClient = Optional.empty();
        }
    }

    @VisibleForTesting
    DatabricksUnityCatalog(LakehouseConfiguration config, WorkspaceClient workspaceClient) {
        if (workspaceClient != null) {
            this.isEnableUnityCatalog = true;
            this.workspaceClient = Optional.of(workspaceClient);
        } else {
            this.workspaceClient = Optional.empty();
        }
    }

    @VisibleForTesting
    static void resetInstance() {
        CATALOGS.clear();
    }

    @Override
    public boolean isEnableUnityCatalog() {
        return isEnableUnityCatalog;
    }

    public static DatabricksUnityCatalog getInstance(LakehouseConfiguration config) {
        if (StringUtils.isNotBlank(config.getUnityCatalogUserAgent())) {
            UserAgent.withPartner(config.getUnityCatalogUserAgent());
        }
        return CATALOGS.computeIfAbsent(buildCacheKey(config), key -> new DatabricksUnityCatalog(config));
    }

    private static String buildCacheKey(LakehouseConfiguration config) {
        return String.join("|",
            StringUtils.defaultString(config.getUnityCatalogUri()),
            StringUtils.defaultString(config.getUnityCatalogClientId()),
            StringUtils.defaultString(config.getUnityCatalogClientSecret()),
            StringUtils.defaultString(config.getUnityCatalogToken()));
    }

    private static void checkCatalogName(String catalogName) {
        if (catalogName == null) {
            throw new IllegalArgumentException("catalogName must not be null");
        }
    }

    @Override
    public TableInfo createExternalTable(String catalogName, UnityTableIdentifier identifier, String location,
                                         StructType structType) {
        checkCatalogName(catalogName);
        if (!isEnableUnityCatalog || workspaceClient.isEmpty()) {
            throw new IllegalStateException("Unity catalog is not enabled");
        }

        try {
            workspaceClient.get().schemas().get(identifier.getSchemaFullName(catalogName));
        } catch (com.databricks.sdk.core.error.platform.NotFound e) {
            CreateSchema createSchema = new CreateSchema();
            createSchema.setCatalogName(catalogName);
            createSchema.setName(identifier.getSchema());
            workspaceClient.get().schemas().create(createSchema);
        }

        CreateTableRequest createTableRequest = new CreateTableRequest();
        createTableRequest.setName(identifier.getTable());
        createTableRequest.setCatalogName(catalogName);
        createTableRequest.setSchemaName(identifier.getSchema());
        createTableRequest.setColumns(UnityCatalogUtil.convertDeltaSchemaToColumns(structType));
        createTableRequest.setTableType(TableType.EXTERNAL);
        createTableRequest.setDataSourceFormat(DataSourceFormat.DELTA);
        createTableRequest.setStorageLocation(location);
        return workspaceClient.get().tables().create(createTableRequest);
    }

    @Override
    public TableInfo createManagedTable(String catalogName, UnityTableIdentifier identifier, String tableId,
                                        String location, StructType structType) {
        checkCatalogName(catalogName);
        if (!isEnableUnityCatalog || workspaceClient.isEmpty()) {
            throw new IllegalStateException("Unity catalog is not enabled");
        }

        try {
            workspaceClient.get().schemas().get(identifier.getSchemaFullName(catalogName));
        } catch (com.databricks.sdk.core.error.platform.NotFound e) {
            CreateSchema createSchema = new CreateSchema();
            createSchema.setCatalogName(catalogName);
            createSchema.setName(identifier.getSchema());
            workspaceClient.get().schemas().create(createSchema);
        }
        CreateTableRequest createTableRequest = new CreateTableRequest();
        createTableRequest.setName(identifier.getTable());
        createTableRequest.setCatalogName(catalogName);
        createTableRequest.setSchemaName(identifier.getSchema());
        createTableRequest.setColumns(UnityCatalogUtil.convertDeltaSchemaToColumns(structType));
        createTableRequest.setTableType(TableType.MANAGED);
        createTableRequest.setDataSourceFormat(DataSourceFormat.DELTA);
        createTableRequest.setStorageLocation(location);

        Map<String, String> properties = new HashMap<>();
        properties.put("location", location);
        properties.put("io.unitycatalog.tableId", tableId);
        properties.put("is_managed_location", "true");
        properties.put("delta.feature.catalogManaged", "supported");
        createTableRequest.setProperties(properties);

        return workspaceClient.get().tables().create(createTableRequest);
    }

    @Override
    public StagingTableInfo createStagingTable(String catalogName, UnityTableIdentifier identifier) {
        checkCatalogName(catalogName);
        if (!isEnableUnityCatalog || workspaceClient.isEmpty()) {
            throw new IllegalStateException("Unity catalog is not enabled");
        }

        try {
            workspaceClient.get().schemas().get(identifier.getSchemaFullName(catalogName));
        } catch (com.databricks.sdk.core.error.platform.NotFound e) {
            CreateSchema createSchema = new CreateSchema();
            createSchema.setCatalogName(catalogName);
            createSchema.setName(identifier.getSchema());
            workspaceClient.get().schemas().create(createSchema);
        }

        String path = "/api/2.1/unity-catalog/staging-tables";
        ApiClient apiClient = workspaceClient.get().apiClient();
        CreateStagingTable createStagingTable = new CreateStagingTable();
        createStagingTable.setCatalogName(catalogName);
        createStagingTable.setSchemaName(identifier.getSchema());
        createStagingTable.setName(identifier.getTable());
        try {
            Request req = new Request("POST", path, apiClient.serialize(createStagingTable));
            ApiClient.setQuery(req, createStagingTable);
            req.withHeader("Accept", "application/json");
            req.withHeader("Content-Type", "application/json");
            return (StagingTableInfo) apiClient.execute(req, StagingTableInfo.class);
        } catch (IOException e) {
            throw new DatabricksException("IO error: " + e.getMessage(), e);
        }
    }

    @Override
    public void updateExternalTable(String catalogName, UnityTableIdentifier identifier, String location,
                                    StructType structType) {
        checkCatalogName(catalogName);
        if (!isEnableUnityCatalog || workspaceClient.isEmpty()) {
            throw new IllegalStateException("Unity catalog is not enabled");
        }
        workspaceClient.get().tables().delete(identifier.getTableFullName(catalogName));
        createExternalTable(catalogName, identifier, location, structType);
    }

    @Override
    public Optional<TableInfo> getTable(String catalogName, UnityTableIdentifier identifier) {
        checkCatalogName(catalogName);
        if (!isEnableUnityCatalog || workspaceClient.isEmpty()) {
            throw new IllegalStateException("Unity catalog is not enabled");
        }
        try {
            return Optional.of(
                    workspaceClient.get().tables().get(identifier.getTableFullName(catalogName)));
        } catch (com.databricks.sdk.core.error.platform.NotFound e) {
            return Optional.empty();
        }
    }

    @Override
    public Optional<CatalogInfo> getCatalog(String catalogName) {
        checkCatalogName(catalogName);
        if (!isEnableUnityCatalog || workspaceClient.isEmpty()) {
            throw new IllegalStateException("Unity catalog is not enabled");
        }
        try {
            return Optional.of(workspaceClient.get().catalogs().get(catalogName));
        } catch (com.databricks.sdk.core.error.platform.NotFound e) {
            return Optional.empty();
        }
    }


    @Override
    public GenerateTemporaryTableCredentialResponse getTemporaryTableCredentials(String tableId,
                                                                                 TableOperation operation) {
        if (!isEnableUnityCatalog || workspaceClient.isEmpty()) {
            throw new IllegalStateException("Unity catalog is not enabled");
        }
        GenerateTemporaryTableCredentialRequest request =
                new GenerateTemporaryTableCredentialRequest();
        request.setTableId(tableId);
        request.setOperation(operation);
        return workspaceClient.get().temporaryTableCredentials().generateTemporaryTableCredentials(request);
    }

    @Override
    public void createOrUpdateExternalLineage(UnityCatalogExternalLineageRequest request) {
        if (!isEnableUnityCatalog || workspaceClient.isEmpty()) {
            return;
        }

        try {
            ExternalMetadata metadata = new ExternalMetadata()
                    .setName(request.getTopicMetadataName())
                    .setSystemType(request.getSystemType())
                    .setEntityType("Topic")
                    .setDescription("Lakestream Ursa topic: " + request.getTopicName())
                    .setProperties(buildLineageProperties(request));

            byolCreateOrUpdateMetadata(workspaceClient.get(), metadata);

            ExternalLineageObject source = new ExternalLineageObject()
                    .setExternalMetadata(new ExternalLineageExternalMetadata().setName(request.getTopicMetadataName()));
            ExternalLineageObject target = new ExternalLineageObject()
                    .setTable(new ExternalLineageTable().setName(request.getTableFullName()));
            byolCreateLineageIfNotExists(workspaceClient.get(), source, target);
        } catch (Exception e) {
            log.warn("Failed to report external lineage for topic {}", request.getTopicName(), e);
        }
    }

    private Map<String, String> buildLineageProperties(UnityCatalogExternalLineageRequest request) {
        Map<String, String> props = new HashMap<>();
        props.put("last_run_utc", Instant.now().toString());
        props.put("source_system", "Lakestream");
        props.put("source_topic", request.getTopicName());
        props.put("source_lakestream_cluster", request.getClusterName());
        return props;
    }

    private void byolCreateOrUpdateMetadata(WorkspaceClient wsClient, ExternalMetadata metadata) {
        String name = metadata.getName();
        try {
            wsClient.externalMetadata().getExternalMetadata(name);
            // Exists - update
            UpdateExternalMetadataRequest updateReq = new UpdateExternalMetadataRequest()
                    .setName(name)
                    .setExternalMetadata(metadata)
                    .setUpdateMask("name,system_type,entity_type,description,properties");
            wsClient.externalMetadata().updateExternalMetadata(updateReq);
            log.debug("Updated external metadata: {}", name);
        } catch (com.databricks.sdk.core.error.platform.NotFound e) {
            CreateExternalMetadataRequest createReq = new CreateExternalMetadataRequest()
                    .setExternalMetadata(metadata);
            wsClient.externalMetadata().createExternalMetadata(createReq);
            log.debug("Created external metadata: {}", name);
        }
    }

    private void byolCreateLineageIfNotExists(WorkspaceClient wsClient,
                                               ExternalLineageObject source,
                                               ExternalLineageObject target) {
        try {
            CreateRequestExternalLineage lineage = new CreateRequestExternalLineage()
                    .setSource(source)
                    .setTarget(target);
            CreateExternalLineageRelationshipRequest request = new CreateExternalLineageRelationshipRequest()
                    .setExternalLineageRelationship(lineage);
            wsClient.externalLineage().createExternalLineageRelationship(request);
            log.debug("Created external lineage relationship");
        } catch (DatabricksException e) {
            if (e.getMessage() != null && e.getMessage().contains("already exists")) {
                log.debug("External lineage relationship already exists, skipping");
            } else {
                throw e;
            }
        }
    }
}
