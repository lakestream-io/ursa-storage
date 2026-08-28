/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.lakehouse.delta;

import com.databricks.sdk.service.catalog.CatalogInfo;
import com.databricks.sdk.service.catalog.ColumnInfo;
import com.databricks.sdk.service.catalog.TableInfo;
import com.databricks.sdk.service.catalog.TableOperation;
import com.google.common.annotations.VisibleForTesting;
import io.delta.kernel.Operation;
import io.delta.kernel.Snapshot;
import io.delta.kernel.Table;
import io.delta.kernel.Transaction;
import io.delta.kernel.TransactionCommitResult;
import io.delta.kernel.data.Row;
import io.delta.kernel.defaults.engine.DefaultEngine;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.exceptions.ConcurrentWriteException;
import io.delta.kernel.exceptions.TableNotFoundException;
import io.delta.kernel.hook.PostCommitHook;
import io.delta.kernel.internal.hook.CheckpointHook;
import io.delta.kernel.types.StructType;
import io.delta.kernel.utils.CloseableIterable;
import io.lakestream.ursa.lakehouse.LakehouseConfiguration;
import io.lakestream.ursa.lakehouse.catalog.unity.UnityCatalogUtil;
import io.lakestream.ursa.lakehouse.iceberg.exception.SchemaEvolutionException;
import io.lakestream.ursa.lakehouse.iceberg.exception.SchemaMappingException;
import io.lakestream.ursa.lakehouse.utils.TopicName;
import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;

@Slf4j
public class UCExternalTable extends UCTable {

    @VisibleForTesting
    @Getter
    protected Table table;

    public UCExternalTable(LakehouseConfiguration config, String parentTopic) {
        super(config, parentTopic);
        if (!unityCatalogApi.isEnableUnityCatalog()) {
            throw new IllegalArgumentException("Delta external table must enable Unity catalog.");
        }
        TopicName topicName = TopicName.get(parentTopic);
        //external table
        Optional<CatalogInfo> catalog = unityCatalogApi.getCatalog(config.getUnityCatalogName());
        if (catalog.isEmpty()) {
            throw new IllegalStateException(
                "The delta external table must define catalog in advance.");
        }
        String storageRoot = catalog.get().getStorageRoot();
        if (storageRoot.endsWith("/")) {
            storageRoot = storageRoot.substring(0, storageRoot.length() - 1);
        }
        tableLocation = DeltaTableUtils.generateTableLocation(storageRoot, topicName);
    }

    public UCExternalTable(LakehouseConfiguration config, String parentTopic,
                           TableInfo unityTable) {
        super(config, parentTopic);
        this.unityTable = unityTable;
        tableLocation = DeltaTableUtils.normalizeStorageLocation(unityTable.getStorageLocation());
        tmpCredential =
            unityCatalogApi.getTemporaryTableCredentials(unityTable.getTableId(),
                TableOperation.READ_WRITE);
        Configuration externalHadoopConfig =
            UnityCatalogUtil.generateExternalHadoopConfig(config, tmpCredential);
        engine = DefaultEngine.create(externalHadoopConfig);
        table = DeltaTableUtils.loadTable(engine, tableLocation);
        compareUnityAndDeltaTable();
    }


    private void compareUnityAndDeltaTable() {
        StructType deltaTableSchema;
        try {
            deltaTableSchema = table.getLatestSnapshot(engine).getSchema();
        } catch (TableNotFoundException e) {
            //We created unity catalog first, the delta table may be not created yet.
            return;
        }
        List<ColumnInfo> deltaTableColumns =
            UnityCatalogUtil.convertDeltaSchemaToColumns(deltaTableSchema);
        Collection<ColumnInfo> unityTableColumns = unityTable.getColumns();
        if (deltaTableColumns.size() != unityTableColumns.size()) {
            log.info("Trigger Unity external table update for topic {} because column count differs. "
                    + "unityColumns={}, deltaColumns={}",
                parentTopic, unityTableColumns, deltaTableColumns);
            unityCatalogApi.updateExternalTable(getUnityCatalogName(), getUnityTableIdentifier(),
                tableLocation, deltaTableSchema);
            return;
        }

        Map<String, ColumnInfo> unityColumnMap = unityTableColumns.stream()
            .collect(Collectors.toMap(
                ColumnInfo::getName,
                column -> column,
                (existing, replacement) -> existing
            ));
        for (ColumnInfo deltaTableColumn : deltaTableColumns) {
            String name = deltaTableColumn.getName();
            ColumnInfo unityColumn = unityColumnMap.get(name);
            if (unityColumn == null) {
                log.info("Trigger Unity external table update for topic {} because column {} is missing in "
                        + "Unity table. unityColumns={}, deltaColumns={}",
                    parentTopic, name, unityTableColumns, deltaTableColumns);
                unityCatalogApi.updateExternalTable(getUnityCatalogName(), getUnityTableIdentifier(),
                    tableLocation, deltaTableSchema);
                break;
            }
            if (!deltaTableColumn.equals(unityColumn)) {
                log.info("Trigger Unity external table update for topic {} because column {} differs. "
                        + "unityColumn={}, deltaColumn={}",
                    parentTopic, name, unityColumn, deltaTableColumn);
                unityCatalogApi.updateExternalTable(getUnityCatalogName(), getUnityTableIdentifier(),
                    tableLocation, deltaTableSchema);
                break;
            }
        }
    }

    @Override
    public void createDeltaTable(Long schemaVersion, StructType deltaSchema) {
        refreshTable();
        Optional<TableInfo> tableOpt = unityCatalogApi.getTable(getUnityCatalogName(), getUnityTableIdentifier());
        if (tableOpt.isEmpty()) {
            unityTable = unityCatalogApi.createExternalTable(getUnityCatalogName(), getUnityTableIdentifier(),
                tableLocation, deltaSchema);
            tableLocation = DeltaTableUtils.normalizeStorageLocation(unityTable.getStorageLocation());
            tmpCredential =
                unityCatalogApi.getTemporaryTableCredentials(unityTable.getTableId(),
                    TableOperation.READ_WRITE);
            Configuration externalHadoopConfig =
                UnityCatalogUtil.generateExternalHadoopConfig(config, tmpCredential);
            engine = DefaultEngine.create(externalHadoopConfig);
            table = DeltaTableUtils.loadTable(engine, tableLocation);
            log.info("create unity catalog table for topic {} succeed. schema: \n {}", parentTopic, deltaSchema);
        } else {
            if (unityTable == null) {
                unityTable = tableOpt.get();
                tableLocation = DeltaTableUtils.normalizeStorageLocation(unityTable.getStorageLocation());
                tmpCredential =
                    unityCatalogApi.getTemporaryTableCredentials(unityTable.getTableId(),
                        TableOperation.READ_WRITE);
                Configuration externalHadoopConfig =
                    UnityCatalogUtil.generateExternalHadoopConfig(config, tmpCredential);
                engine = DefaultEngine.create(externalHadoopConfig);
                table = DeltaTableUtils.loadTable(engine, tableLocation);
            }
        }
        if (!DeltaTableUtils.isTableExists(table, engine)) {
            try {
                Map<String, String> prop = buildCreateTableProperties(schemaVersion);
                deltaSchema = CustomColumnMapping.assignColumnIdAndPhysicalNameForCreateTable(deltaSchema,
                    new AtomicInteger(0));
                Transaction txn = table.createTransactionBuilder(this.engine, URSA_DELTA_ENGINE,
                        Operation.CREATE_TABLE).withSchema(this.engine, deltaSchema)
                    .withPartitionColumns(this.engine, partitionKeys)
                    .withTableProperties(this.engine, prop)
                    .build(this.engine);

                txn.commit(engine, CloseableIterable.emptyIterable());
                log.info("create delta table for topic {} succeed. schema: \n {}", parentTopic, deltaSchema);
            } catch (ConcurrentWriteException e) {
                if (!DeltaTableUtils.isTableExists(table, engine)) {
                    throw new IllegalStateException("Failed to create delta table for topic: " + parentTopic,
                        e);
                }
            } catch (SchemaMappingException e) {
                throw new IllegalStateException("Failed to create delta table for topic: " + parentTopic, e);
            }
        }
    }

    @Override
    public void evolveSchemaWithVersion(long versionId, StructType deltaSchema) throws SchemaMappingException,
        SchemaEvolutionException {
        super.evolveSchemaWithVersion(versionId, deltaSchema);
        unityCatalogApi.updateExternalTable(getUnityCatalogName(), getUnityTableIdentifier(), tableLocation,
            table.getLatestSnapshot(engine).getSchema());
    }

    @Override
    public Snapshot getLatestSnapshot() {
        refreshTable();
        if (table == null) {
            return null;
        }
        try {
            return table.getLatestSnapshot(engine);
        } catch (TableNotFoundException e) {
            return null;
        }
    }

    @Override
    public synchronized void commitSnapshot(List<Row> actions) {
        refreshTable();
        if (actions.isEmpty()) {
            return;
        }
        Transaction txn =
            table.createTransactionBuilder(engine, URSA_DELTA_ENGINE, Operation.WRITE)
                .build(engine);
        TransactionCommitResult commitResult =
            txn.commit(engine, DeltaTableUtils.toCloseableIterable(actions));

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

    @Override
    synchronized void refreshTable() {
        Engine previousEngine = this.engine;
        super.refreshTable();
        if (previousEngine != this.engine) {
            table = DeltaTableUtils.loadTable(engine, tableLocation);
        }
    }

    @Override
    public boolean tableExists() {
        refreshTable();
        Optional<TableInfo> tableOpt = unityCatalogApi.getTable(getUnityCatalogName(), getUnityTableIdentifier());
        if (tableOpt.isEmpty()) {
            return false;
        }
        if (unityTable == null) {
            unityTable = tableOpt.get();
            tableLocation = DeltaTableUtils.normalizeStorageLocation(unityTable.getStorageLocation());
            tmpCredential =
                unityCatalogApi.getTemporaryTableCredentials(unityTable.getTableId(),
                    TableOperation.READ_WRITE);
            Configuration externalHadoopConfig =
                UnityCatalogUtil.generateExternalHadoopConfig(config, tmpCredential);
            engine = DefaultEngine.create(externalHadoopConfig);
            table = DeltaTableUtils.loadTable(engine, tableLocation);
        }
        return DeltaTableUtils.isTableExists(table, engine);
    }
}
