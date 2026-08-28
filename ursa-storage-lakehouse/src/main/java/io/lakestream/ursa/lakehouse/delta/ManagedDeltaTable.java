/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.lakehouse.delta;

import com.google.common.annotations.VisibleForTesting;
import io.delta.kernel.Operation;
import io.delta.kernel.Snapshot;
import io.delta.kernel.Table;
import io.delta.kernel.Transaction;
import io.delta.kernel.TransactionCommitResult;
import io.delta.kernel.data.Row;
import io.delta.kernel.defaults.engine.DefaultEngine;
import io.delta.kernel.exceptions.ConcurrentWriteException;
import io.delta.kernel.exceptions.TableNotFoundException;
import io.delta.kernel.hook.PostCommitHook;
import io.delta.kernel.internal.hook.CheckpointHook;
import io.delta.kernel.types.StructType;
import io.delta.kernel.utils.CloseableIterable;
import io.lakestream.ursa.lakehouse.LakehouseConfiguration;
import io.lakestream.ursa.lakehouse.iceberg.exception.SchemaEvolutionException;
import io.lakestream.ursa.lakehouse.iceberg.exception.SchemaMappingException;
import io.lakestream.ursa.lakehouse.utils.TopicName;
import io.lakestream.ursa.lakehouse.writer.ParquetFileStat;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;

@Slf4j
public class ManagedDeltaTable extends DeltaTable {

    @VisibleForTesting
    @Getter
    protected Table table;

    public ManagedDeltaTable(LakehouseConfiguration config, String parentTopic) {
        super(config, parentTopic);
        TopicName topicName = TopicName.get(parentTopic);
        Configuration hadoopConfig = config.getHadoopConfiguration();
        tableLocation = DeltaTableUtils.generateTableLocation(config.getStoragePath(), topicName);
        engine = DefaultEngine.create(hadoopConfig);
        table = DeltaTableUtils.loadTable(engine, tableLocation);
    }

    @Override
    void refreshTable() {
    }

    @Override
    public boolean tableExists() {
        if (unityCatalogApi.isEnableUnityCatalog()) {
            if (unityCatalogApi.getTable(getUnityCatalogName(), getUnityTableIdentifier()).isEmpty()) {
                return false;
            }
        }
        return DeltaTableUtils.isTableExists(table, engine);
    }

    @Override
    public void createDeltaTable(Long schemaVersion, StructType deltaSchema) {
        refreshTable();
        if (unityCatalogApi.isEnableUnityCatalog()) {
            if (unityCatalogApi.getTable(getUnityCatalogName(), getUnityTableIdentifier()).isEmpty()) {
                unityCatalogApi.createExternalTable(getUnityCatalogName(), getUnityTableIdentifier(),
                    tableLocation, deltaSchema);
                log.info("create unity catalog table for topic {} succeed. schema: \n {}", parentTopic, deltaSchema);
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
                    throw new IllegalStateException("Failed to create delta table for path: " + table.getPath(engine),
                        e);
                }
            } catch (SchemaMappingException e) {
                throw new IllegalStateException("Failed to create delta table for path: " + table.getPath(engine), e);
            }
        }
    }

    @Override
    public Snapshot getLatestSnapshot() {
        try {
            return table.getLatestSnapshot(engine);
        } catch (TableNotFoundException e) {
            // Return null when the table has not been created yet, for consistency
            // with other DeltaTable implementations and their callers.
            return null;
        }
    }

    @Override
    public void evolveSchemaWithVersion(long versionId, StructType deltaSchema)
        throws SchemaMappingException, SchemaEvolutionException {
        super.evolveSchemaWithVersion(versionId, deltaSchema);
        if (unityCatalogApi.isEnableUnityCatalog()) {
            unityCatalogApi.updateExternalTable(getUnityCatalogName(), getUnityTableIdentifier(), tableLocation,
                table.getLatestSnapshot(engine).getSchema());
        }
    }

    @Override
    public synchronized void commitSnapshot(List<Row> actions) {
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
    public List<Row> buildAddFileAction(List<ParquetFileStat> fileStats) {
        if (fileStats.isEmpty()) {
            return Collections.emptyList();
        }
        List<Row> rows = new ArrayList<>();
        for (ParquetFileStat fileStat : fileStats) {
            log.info("add filePath: {}, partitionValues: {}, fileSize: {}", fileStat.getFilePath(),
                fileStat.getPartitionValues(), fileStat.getFileSize());
            Map<String, String> filteredTags;
            if (fileStat.getTags() != null) {
                filteredTags = fileStat.getTags().entrySet()
                    .stream()
                    .filter(entry -> TAG_KEYS.contains(entry.getKey()))
                    .collect(Collectors.toMap(
                        Map.Entry::getKey,
                        Map.Entry::getValue));
            } else {
                filteredTags = new HashMap<>();
            }
            filteredTags.put(ORDER_TAG, "true");
            Row addFileRow = DeltaTableUtils.buildAddFileAction(
                fileStat.getFilePath(), fileStat.getFileSize(), System.currentTimeMillis(),
                fileStat.getPartitionValues(),
                true, fileStat.getStats(), filteredTags);
            rows.add(addFileRow);
        }
        return rows;
    }
}
