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
import io.lakestream.ursa.lakehouse.iceberg.exception.SchemaMappingException;
import io.lakestream.ursa.lakehouse.utils.TopicName;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;

@Slf4j
public class DirectExternalTable extends ExternalDeltaTable {

    @VisibleForTesting
    @Getter
    protected Table table;

    protected Configuration externalHadoopConf;

    public DirectExternalTable(LakehouseConfiguration config, String parentTopic) {
        super(config, parentTopic);
        TopicName topicName = TopicName.get(parentTopic);
        String directExternalStoragePath = config.getDirectExternalStoragePath();
        externalHadoopConf = buildExternalHadoopConfiguration(directExternalStoragePath);
        tableLocation = DeltaTableUtils.generateTableLocation(directExternalStoragePath, topicName);
        engine = DefaultEngine.create(externalHadoopConf);
        table = DeltaTableUtils.loadTable(engine, tableLocation);
    }

    private Configuration buildExternalHadoopConfiguration(String directExternalStoragePath) {
        Properties properties = new Properties();
        properties.putAll(config.getProperties());
        properties.put(LakehouseConfiguration.STORAGE_PATH, directExternalStoragePath);
        Configuration hadoopConf = LakehouseConfiguration.generateHadoopConfiguration(properties);

        // Customer-provided hadoop.* settings should win over generated defaults.
        properties.entrySet().stream()
            .filter(e -> StringUtils.startsWith(e.getKey().toString(), LakehouseConfiguration.HADOOP_CONF_PREFIX))
            .forEach(e -> hadoopConf.set(
                e.getKey().toString().replaceFirst(LakehouseConfiguration.HADOOP_CONF_PREFIX, ""),
                e.getValue().toString()));
        return hadoopConf;
    }

    @Override
    void refreshTable() {
    }

    @Override
    public boolean tableExists() {
        return DeltaTableUtils.isTableExists(table, engine);
    }

    @Override
    public void createDeltaTable(Long schemaVersion, StructType deltaSchema) {
        refreshTable();
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
                log.info("create delta external table for topic {} succeed. schema: \n {}", parentTopic, deltaSchema);
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
            return null;
        }
    }

    @Override
    public Configuration getTableHadoopConfiguration() {
        return externalHadoopConf;
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

}
