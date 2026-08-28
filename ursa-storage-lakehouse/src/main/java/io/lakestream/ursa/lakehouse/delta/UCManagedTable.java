/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.lakehouse.delta;

import com.databricks.sdk.WorkspaceClient;
import com.databricks.sdk.core.oauth.OpenIDConnectEndpoints;
import com.databricks.sdk.service.catalog.GenerateTemporaryTableCredentialResponse;
import com.databricks.sdk.service.catalog.TableInfo;
import com.databricks.sdk.service.catalog.TableOperation;
import io.delta.kernel.Operation;
import io.delta.kernel.Snapshot;
import io.delta.kernel.TableManager;
import io.delta.kernel.Transaction;
import io.delta.kernel.TransactionCommitResult;
import io.delta.kernel.data.Row;
import io.delta.kernel.defaults.engine.DefaultEngine;
import io.delta.kernel.exceptions.ConcurrentWriteException;
import io.delta.kernel.expressions.Column;
import io.delta.kernel.hook.PostCommitHook;
import io.delta.kernel.internal.hook.CheckpointHook;
import io.delta.kernel.internal.tablefeatures.TableFeatures;
import io.delta.kernel.transaction.CreateTableTransactionBuilder;
import io.delta.kernel.transaction.DataLayoutSpec;
import io.delta.kernel.types.StructType;
import io.delta.kernel.unitycatalog.UCCatalogManagedClient;
import io.delta.kernel.unitycatalog.UCCatalogManagedCommitter;
import io.delta.kernel.utils.CloseableIterable;
import io.delta.storage.commit.uccommitcoordinator.UCTokenBasedRestClient;
import io.lakestream.ursa.lakehouse.LakehouseConfiguration;
import io.lakestream.ursa.lakehouse.catalog.unity.DatabricksUnityCatalog;
import io.lakestream.ursa.lakehouse.catalog.unity.UnityCatalogUtil;
import io.lakestream.ursa.lakehouse.iceberg.exception.SchemaEvolutionException;
import io.lakestream.ursa.lakehouse.iceberg.exception.SchemaMappingException;
import io.unitycatalog.client.auth.TokenProvider;
import io.unitycatalog.client.model.StagingTableInfo;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.hadoop.conf.Configuration;

@Slf4j
public class UCManagedTable extends UCTable {

    private UCTokenBasedRestClient ucTokenBasedRestClient;

    private UCCatalogManagedClient ucCatalogManagedClient;

    public UCManagedTable(LakehouseConfiguration config, String parentTopic) {
        super(config, parentTopic);
        if (!unityCatalogApi.isEnableUnityCatalog()) {
            throw new IllegalArgumentException("Delta managed table must enable Unity catalog.");
        }
        initUnityClient();
    }

    public UCManagedTable(LakehouseConfiguration config, String parentTopic, TableInfo unityTable) {
        super(config, parentTopic);
        if (!unityCatalogApi.isEnableUnityCatalog()) {
            throw new IllegalArgumentException("Delta managed table must enable Unity catalog.");
        }
        initUnityClient();
        this.unityTable = unityTable;
        tableLocation = DeltaTableUtils.normalizeStorageLocation(unityTable.getStorageLocation());
        tmpCredential =
            unityCatalogApi.getTemporaryTableCredentials(unityTable.getTableId(),
                TableOperation.READ_WRITE);
        Configuration externalHadoopConfig =
            UnityCatalogUtil.generateExternalHadoopConfig(config, tmpCredential);
        engine = DefaultEngine.create(externalHadoopConfig);
    }

    private void initUnityClient() {
        if (!(unityCatalogApi instanceof DatabricksUnityCatalog databricksUnityCatalog)) {
            throw new IllegalStateException(
                "UCManagedTable requires DatabricksUnityCatalog, but got: "
                    + unityCatalogApi.getClass().getSimpleName());
        }
        WorkspaceClient workspaceClient = databricksUnityCatalog.getWorkspaceClient()
            .orElseThrow(() -> new IllegalStateException(
                "WorkspaceClient is not configured. Ensure Unity Catalog OAuth credentials are set."));
        OpenIDConnectEndpoints oidcEndpoints;
        try {
            oidcEndpoints = workspaceClient.config().getOidcEndpoints();
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to parse the databricks oidc endpoint.", e);
        }
        if (oidcEndpoints == null) {
            throw new IllegalStateException("OIDC endpoints are not available from the workspace client.");
        }
        String url = config.getUnityCatalogUri();

        Map<String, String> configs = new HashMap<>();
        configs.put("type", "oauth");
        configs.put("oauth.uri", oidcEndpoints.getTokenEndpoint());
        configs.put("oauth.clientId", config.getUnityCatalogClientId());
        configs.put("oauth.clientSecret", config.getUnityCatalogClientSecret());

        TokenProvider tokenProvider = TokenProvider.create(configs);
        this.ucTokenBasedRestClient = new UCTokenBasedRestClient(url, tokenProvider, Collections.emptyMap());
        this.ucCatalogManagedClient = new UCCatalogManagedClient(ucTokenBasedRestClient);
    }

    @Override
    public boolean tableExists() {
        return unityTable != null;
    }

    @Override
    public void createDeltaTable(Long schemaVersion, StructType deltaSchema) {
        if (tableExists()) {
            return;
        }
        StagingTableInfo stagingTable = unityCatalogApi.createStagingTable(getUnityCatalogName(),
            getUnityTableIdentifier());
        String stagingLocation = stagingTable.getStagingLocation();
        String tmpStagingLocation = DeltaTableUtils.normalizeStorageLocation(stagingLocation);
        GenerateTemporaryTableCredentialResponse temporaryTableCredentials =
            unityCatalogApi.getTemporaryTableCredentials(stagingTable.getId(), TableOperation.READ_WRITE);
        Configuration hadoopConfig = UnityCatalogUtil.generateExternalHadoopConfig(config, temporaryTableCredentials);
        DefaultEngine engine = DefaultEngine.create(hadoopConfig);

        final Map<String, String> properties;
        try {
            properties = buildCreateTableProperties(schemaVersion);
        } catch (SchemaMappingException e) {
            throw new IllegalStateException("Failed to create delta table for topic: " + parentTopic, e);
        }
        properties.put(
            TableFeatures.CATALOG_MANAGED_RW_FEATURE.getTableFeatureSupportKey(),
            TableFeatures.SET_TABLE_FEATURE_SUPPORTED_VALUE);
        properties.put(UCCatalogManagedClient.UC_TABLE_ID_KEY, stagingTable.getId());
        properties.put(TableFeatures.VACUUM_PROTOCOL_CHECK_RW_FEATURE.getTableFeatureSupportKey(),
            TableFeatures.SET_TABLE_FEATURE_SUPPORTED_VALUE);
        deltaSchema = CustomColumnMapping.assignColumnIdAndPhysicalNameForCreateTable(deltaSchema,
            new AtomicInteger(0));
        CreateTableTransactionBuilder txBuilder =
            TableManager.buildCreateTableTransaction(tmpStagingLocation, deltaSchema, URSA_DELTA_ENGINE)
                .withCommitter(
                    new UCCatalogManagedCommitter(ucTokenBasedRestClient, stagingTable.getId(), tmpStagingLocation))
                .withTableProperties(properties);
        if (!CollectionUtils.isEmpty(partitionKeys)) {
            List<Column> columns = partitionKeys.stream()
                .map(Column::new)
                .toList();
            txBuilder.withDataLayoutSpec(DataLayoutSpec.partitioned(columns));
        }
        Transaction tx = txBuilder.build(engine);

        try {
            tx.commit(engine, CloseableIterable.emptyIterable());
        } catch (ConcurrentWriteException e) {
            throw new IllegalStateException("Failed to create delta table for topic: " + parentTopic, e);
        }
        unityTable =
            unityCatalogApi.createManagedTable(getUnityCatalogName(), getUnityTableIdentifier(),
                stagingTable.getId(), stagingLocation, deltaSchema);
        tableLocation = DeltaTableUtils.normalizeStorageLocation(unityTable.getStorageLocation());
        tmpCredential =
            unityCatalogApi.getTemporaryTableCredentials(unityTable.getTableId(),
                TableOperation.READ_WRITE);
        Configuration externalHadoopConfig =
            UnityCatalogUtil.generateExternalHadoopConfig(config, tmpCredential);
        this.engine = DefaultEngine.create(externalHadoopConfig);
    }

    @Override
    public void evolveSchemaWithVersion(long versionId, StructType deltaSchema)
        throws SchemaMappingException, SchemaEvolutionException {
        throw new SchemaEvolutionException("The UC Managed Table not supported schema evolution now.");
    }

    @Override
    public Snapshot getLatestSnapshot() {
        if (unityTable == null) {
            return null;
        }
        refreshTable();
        return
            ucCatalogManagedClient.loadSnapshot(engine, unityTable.getTableId(), tableLocation, Optional.empty(),
                Optional.empty());
    }

    @Override
    public synchronized void commitSnapshot(List<Row> actions) {
        if (actions.isEmpty()) {
            return;
        }
        refreshTable();
        Snapshot latestSnapshot =
            ucCatalogManagedClient.loadSnapshot(engine, unityTable.getTableId(), tableLocation, Optional.empty(),
                Optional.empty());
        Transaction transaction =
            latestSnapshot.buildUpdateTableTransaction(URSA_DELTA_ENGINE, Operation.WRITE).build(engine);
        TransactionCommitResult commitResult = transaction.commit(engine, DeltaTableUtils.toCloseableIterable(actions));
        List<PostCommitHook> hooks = commitResult.getPostCommitHooks();
        for (PostCommitHook hook : hooks) {
            if (hook instanceof CheckpointHook checkpointHook) {
                try {
                    checkpointHook.threadSafeInvoke(engine);
                } catch (IOException e) {
                    log.warn("Failed to checkpoint for version {}.", commitResult.getVersion(), e);
                }
            }
        }
        log.info("Commit to delta table succeed for fileStat size: {}, commit version: {}", actions.size(),
            commitResult.getVersion());
    }
}
