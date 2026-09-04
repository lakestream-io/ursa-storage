/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.lakehouse.compact;

import io.lakestream.api.materialization.TableIdentifier;
import io.lakestream.ursa.compaction.CompactTaskManager;
import io.lakestream.ursa.compaction.metrics.CompactionMetrics;
import io.lakestream.ursa.compaction.task.CompactStreamTask;
import io.lakestream.ursa.exception.ExceptionCode;
import io.lakestream.ursa.exception.LakehouseOptException;
import io.lakestream.ursa.lakehouse.DeltaCommitter;
import io.lakestream.ursa.lakehouse.IcebergCommitter;
import io.lakestream.ursa.lakehouse.LakehouseCommitter;
import io.lakestream.ursa.lakehouse.LakehouseConfiguration;
import io.lakestream.ursa.lakehouse.catalog.unity.UnityCatalogApi;
import io.lakestream.ursa.lakehouse.catalog.unity.UnityCatalogExternalLineageRequest;
import io.lakestream.ursa.lakehouse.catalog.unity.UnityCatalogLineageUtil;
import io.lakestream.ursa.lakehouse.delta.DeltaCompactStreamTask;
import io.lakestream.ursa.lakehouse.exception.IcebergTableCorruptedException;
import io.lakestream.ursa.lakehouse.iceberg.IcebergCompactStreamTask;
import io.lakestream.ursa.lakehouse.utils.StreamTableNaming;
import io.lakestream.ursa.lakehouse.writer.ParquetFileStat;
import io.lakestream.ursa.storage.StorageApi;
import io.lakestream.ursa.storage.impl.StorageConfig;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.io.WriteResult;

@Slf4j
public class UpsertCommitFileRunner extends AbstractCommitRunner implements CommitRunner {

    private final CompactionMetrics compactionMetrics;
    private final LakehouseCommitter lakehouseCommitter;
    private final UnityCatalogApi unityCatalogApi;

    public UpsertCommitFileRunner(StorageApi storageApi,
                                  CompactTaskManager compactTaskManager,
                                  StorageConfig storageConfig,
                                  String parentTopic,
                                  CompactionMetrics compactionMetrics) {
        super(storageApi, compactTaskManager, parentTopic, storageConfig, compactionMetrics);

        // TODO: optimize here
        // Resolve tolerantly: the SDT sink may be a non-lakehouse, inline-commit sink (e.g. ClickHouse)
        // selected via the materialization catalog, in which case lakehouseType is not an Iceberg/Delta
        // managed format. A null committer means "no external lakehouse commit" — the managed Compacted
        // Object is still registered (compactOxiaIndex) and the offload cursor advanced.
        switch (config.getLakehouseTypeOrNone()) {
            case ICEBERG:
                this.lakehouseCommitter = new IcebergCommitter(config, parentTopic);
                log.info("Lakehouse type is ICEBERG, parentTopic: {}", parentTopic);
                break;
            case DELTA:
                lakehouseCommitter = new DeltaCommitter(config, parentTopic);
                log.info("Lakehouse type is DELTA, parentTopic: {}", parentTopic);
                break;
            default:
                this.lakehouseCommitter = null;
                log.info("No external lakehouse committer (lakehouseType={}); SBT-only / inline-commit "
                        + "SDT sink for parentTopic: {}", config.getLakehouseTypeOrNone(), parentTopic);
                break;
        }

        this.compactionMetrics = compactionMetrics;
        LakehouseConfiguration lakehouseConfig = new LakehouseConfiguration(storageConfig.getProperties());
        this.unityCatalogApi = lakehouseConfig.isUnityCatalogByolEnabled()
                ? UnityCatalogApi.getInstance(lakehouseConfig) : null;
    }

    public void commitFiles(List<CompactStreamTask> tasks) throws LakehouseOptException {
        List<CompactStreamTask> needCommitTasks = new ArrayList<>();
        List<CompletableFuture<Void>> updateFutures = new ArrayList<>();
        handleTaskStats(tasks, needCommitTasks);

        if (needCommitTasks.isEmpty()) {
            return;
        }

        for (CompactStreamTask needCommitTask : needCommitTasks) {
            if (needCommitTask.getStatus() == CompactStreamTask.PREPARED_COMMIT) {
                continue;
            }
            needCommitTask.setStatus(CompactStreamTask.PREPARED_COMMIT);
            updateFutures.add(compactTaskManager.updateCompactTask(needCommitTask));
        }
        try {
            CompletableFuture.allOf(updateFutures.toArray(new CompletableFuture[0])).get();
            updateFutures.clear();
        } catch (Exception e) {
            throw new LakehouseOptException(ExceptionCode.COMPACTION_UPDATE_TASK_ERROR,
                "Failed to update compact tasks to prepared commit status", e);
        }

        commitToLakehouse(needCommitTasks);

        for (CompactStreamTask needCommitTask : needCommitTasks) {
            // TODO: we serialize and deserialize the task for updating status, which is not efficient.
            // We should optimize it in the future.
            needCommitTask.setStatus(CompactStreamTask.COMMITTED);
            updateFutures.add(compactTaskManager.updateCompactTask(needCommitTask));
            if (needCommitTask.getMessageWrittenToUrsaTime() > 0) {
                compactionMetrics.getMessageEndToEndCompactLatency().recordSuccess(
                        (System.currentTimeMillis() - needCommitTask.getMessageWrittenToUrsaTime()) * 1_000_000);
            }
            assert needCommitTask.getTopic() != null;
            updateLastCommitOffset(needCommitTask);
        }
        try {
            CompletableFuture.allOf(updateFutures.toArray(new CompletableFuture[0])).get();
            updateFutures.clear();
        } catch (Exception e) {
            throw new LakehouseOptException(ExceptionCode.COMPACTION_UPDATE_TASK_ERROR,
                "Failed to update compact tasks to committed status", e);
        }

        // delete the compact task after commit to lakehouse
        for (CompactStreamTask needCommitTask : needCommitTasks) {
            if (needCommitTask.getFilePath() != null) {
                //compactOxiaIndex is idempotent, we don't need to record the state for it.
                compactOxiaIndex(needCommitTask);
            }
            updateFutures.add(compactTaskManager.deleteCompactTask(needCommitTask));
        }
        try {
            CompletableFuture.allOf(updateFutures.toArray(new CompletableFuture[0])).get();
            updateFutures.clear();
        } catch (Exception e) {
            log.warn("Failed to delete committed compact tasks");
        }

        lastCommitOffset.forEach((topic, offset) ->
                compactionMetrics.getLastCompactedOffset()
                        .set(offset.offset(), Attributes.of(AttributeKey.stringKey("topic"), topic)));
    }

    public void commitToLakehouse(List<CompactStreamTask> tasks) throws LakehouseOptException {
        log.info("Start to commit {} to the lakehouse", tasks.size());
        long start = System.currentTimeMillis();
        if (tasks.isEmpty()) {
            log.info("No task to commit");
            return;
        }

        List<ParquetFileStat> fileStats = new ArrayList<>();
        List<ParquetFileStat> dltFileStats = new ArrayList<>();

        Map<String, String> tableProperties = null;
        for (CompactStreamTask task : tasks) {
            // record the committed parquet file size for iceberg
            if (task instanceof IcebergCompactStreamTask icebergCompactStreamTask) {
                // check if the writeResults is not null
                List<WriteResult> writeResults = icebergCompactStreamTask.getWriteResults();
                if (writeResults != null && !writeResults.isEmpty()) {
                    for (WriteResult writeResult : writeResults) {
                        if (writeResult != null && writeResult.dataFiles() != null) {
                            for (DataFile dataFile : writeResult.dataFiles()) {
                                compactionMetrics.getCommittedParquetFileBytes().set(dataFile.fileSizeInBytes());
                            }
                        }
                    }
                } else if (icebergCompactStreamTask.getWriteResult() != null) {
                    DataFile[] dataFiles = icebergCompactStreamTask.getWriteResult().dataFiles();
                    if (dataFiles != null) {
                        for (DataFile dataFile : dataFiles) {
                            compactionMetrics.getCommittedParquetFileBytes().set(dataFile.fileSizeInBytes());
                        }
                    }
                }
            }

            // record the committed parquet file size for delta
            if (task instanceof DeltaCompactStreamTask deltaCompactStreamTask) {
                List<ParquetFileStat> deltaFiles = deltaCompactStreamTask.getDeltaFiles();
                if (deltaFiles != null) {
                    for (ParquetFileStat deltaFile : deltaFiles) {
                        compactionMetrics.getCommittedParquetFileBytes().set(deltaFile.getFileSize());
                    }
                }
            }

            if (hasFiles(task) && !dataCommittedTaskIds.contains(task.getTaskName())) {
                ParquetFileStat stat = generateParquetFileStat(task);
                if (stat != null) {
                    fileStats.add(stat);
                }
            }

            if (hasDltFiles(task) && !dltCommittedTaskIds.contains(task.getTaskName())) {
                ParquetFileStat dltFileStat = generateDLTParquetFileStat(task);
                if (dltFileStat != null) {
                    dltFileStats.add(dltFileStat);
                }
            }

            if (tableProperties == null) {
                tableProperties = task.getProperties();
            }
        }

        try {
            long now = System.nanoTime();
            if (!fileStats.isEmpty() && lakehouseCommitter != null) {
                long medataFileSize = lakehouseCommitter.commit(fileStats);
                if (medataFileSize > 0) {
                    compactionMetrics.getLakehouseMetadataFileSize()
                        .set(medataFileSize, Attributes.of(AttributeKey.stringKey("topic"), parentTopic));
                }
            }

            if (!dltFileStats.isEmpty()) {
                initDLTLakehouseCommitterIfNeeded();
                long dltMetadataFileSize = dltLakehouseCommitter.commit(dltFileStats);
                if (dltMetadataFileSize > 0) {
                    compactionMetrics.getLakehouseMetadataFileSize()
                        .set(dltMetadataFileSize, Attributes.of(AttributeKey.stringKey("topic"), dltTopic));
                }
            }
            compactionMetrics.getCommitToLakehouseLatency().recordSuccess(System.nanoTime() - now);
        } catch (Throwable e) {
            ExceptionCode exceptionCode = ExceptionCode.LAKEHOUSE_COMMIT_ERROR;
            if (e instanceof IcebergTableCorruptedException) {
                exceptionCode = ExceptionCode.LAKEHOUSE_TABLE_CORRUPTED_ERROR;
            }

            throw new LakehouseOptException(exceptionCode,
                    "Failed to commit to the lakehouse for topic " + parentTopic, e);
        }

        compactionMetrics.getCommitTaskBatchSize().set(tasks.size());
        log.info("Commit {} to the lakehouse successfully, tasks {} ms", tasks.size(),
            System.currentTimeMillis() - start);

        reportExternalLineage();
    }

    private void reportExternalLineage() {
        if (unityCatalogApi == null || !config.isUnityCatalogByolEnabled()) {
            return;
        }
        try {
            for (UnityCatalogExternalLineageRequest request
                    : UnityCatalogLineageUtil.buildRequests(config, parentTopic, config.getCatalogName())) {
                unityCatalogApi.createOrUpdateExternalLineage(request);
            }
        } catch (Exception e) {
            log.warn("Failed to report external lineage for topic {}", parentTopic, e);
        }
    }

    private void initDLTLakehouseCommitterIfNeeded() throws LakehouseOptException {
        if (dltLakehouseCommitter == null) {
            dltLakehouseCommitter = newDltLakehouseCommitter();
        }
    }

    private LakehouseCommitter newDltLakehouseCommitter() {
        TableIdentifier mainIdentifier = StreamTableNaming.resolve(parentTopic, config.getProperties());
        TableIdentifier dltIdentifier = StreamTableNaming.deadLetterTable(
                mainIdentifier, config.getDltSuffix());
        return switch (config.getLakehouseType()) {
            case DELTA -> new DeltaCommitter(config, dltTopic, dltIdentifier);
            case ICEBERG -> new IcebergCommitter(config, dltTopic, dltIdentifier);
            default -> throw new IllegalArgumentException(
                    "Unsupported lakehouse type for DLT: " + config.getLakehouseType());
        };
    }

    static ParquetFileStat generateParquetFileStat(CompactStreamTask compactStreamTask) {
        Map<String, String> tags = generateTags(compactStreamTask);

        if (compactStreamTask instanceof IcebergCompactStreamTask icebergCompactStreamTask) {
            if (icebergCompactStreamTask.getWriteResults() != null
                    && !icebergCompactStreamTask.getWriteResults().isEmpty()) {
                return ParquetFileStat.fromWriteResults(icebergCompactStreamTask.getWriteResults(), tags);
            }
            if (icebergCompactStreamTask.getWriteResult() != null) {
                return ParquetFileStat.fromWriteResults(List.of(icebergCompactStreamTask.getWriteResult()), tags);
            }
        }
        if (compactStreamTask instanceof DeltaCompactStreamTask deltaCompactStreamTask) {
            return ParquetFileStat.fromDeltaFiles(deltaCompactStreamTask.getDeltaFiles(), tags);
        }
        return null;
    }

    static ParquetFileStat generateDLTParquetFileStat(CompactStreamTask compactStreamTask) {
        if (compactStreamTask instanceof IcebergCompactStreamTask icebergCompactStreamTask) {
            List<WriteResult> writeResults = icebergCompactStreamTask.getDltWriteResults();
            if (writeResults != null && !writeResults.isEmpty()) {
                return ParquetFileStat.fromWriteResults(writeResults, generateTags(icebergCompactStreamTask));
            }
        }
        if (compactStreamTask instanceof DeltaCompactStreamTask deltaCompactStreamTask) {
            List<ParquetFileStat> parquetFileStats = deltaCompactStreamTask.getDltDeltaFiles();
            if (parquetFileStats != null && !parquetFileStats.isEmpty()) {
                return ParquetFileStat.fromDeltaFiles(parquetFileStats, generateTags(deltaCompactStreamTask));
            }
        }
        return null;
    }

    private static Map<String, String> generateTags(CompactStreamTask compactStreamTask) {
        Map<String, String> tags = new HashMap<>();
        tags.put("streamId", String.valueOf(compactStreamTask.getStreamId()));
        tags.put("startOffset", String.valueOf(compactStreamTask.getStartOffset()));
        tags.put("endOffset", String.valueOf(compactStreamTask.getEndOffset()));
        tags.put("totalSize", String.valueOf(compactStreamTask.getTotalSize()));
        tags.put("cumulativeSize", String.valueOf(compactStreamTask.getCumulativeSize()));
        tags.put("totalMessage",
                String.valueOf(compactStreamTask.getEndOffset() - compactStreamTask.getStartOffset()));
        tags.put("realStartOffset", String.valueOf(compactStreamTask.getRealStartOffset()));
        tags.put("realEndOffset", String.valueOf(compactStreamTask.getRealEndOffset()));
        // Add taskId tag for easier traceability
        tags.put("taskId", compactStreamTask.getTaskName());
        // Record topic name (with partition info)
        tags.put("topic", compactStreamTask.getTopic());
        return tags;
    }

    protected boolean isCompactStreamTaskCommittedToDataTable(CompactStreamTask task)
            throws LakehouseOptException {
        try {
            return lakehouseCommitter.isTheCompactStreamTaskCommitted(task);
        } catch (IOException e) {
            throw new LakehouseOptException(ExceptionCode.LAKEHOUSE_CHECK_COMMITTED_ERROR,
                    String.format("Failed to check whether the compact task %s is committed to lakehouse.",
                            task.getTaskName()), e);
        }
    }

    private void handleTaskStats(List<CompactStreamTask> tasks, List<CompactStreamTask> needCommitTasks)
            throws LakehouseOptException {
        List<CompletableFuture<Void>> updateFutures = new ArrayList<>();
        for (CompactStreamTask task : tasks) {
            if (CompactStreamTask.COMPACTED == task.getStatus()) {
                needCommitTasks.add(task);
            } else if (CompactStreamTask.PREPARED_COMMIT == task.getStatus()) {
                boolean committedToDataTable;
                boolean committedToDLTTable = true;
                try {
                    if (hasFiles(task)) {
                        committedToDataTable = isCompactStreamTaskCommittedToDataTable(task);
                    } else {
                        committedToDataTable = true;
                    }
                    if (task instanceof IcebergCompactStreamTask icebergCompactStreamTask) {
                        if (icebergCompactStreamTask.getDltWriteResults() != null
                                && !icebergCompactStreamTask.getDltWriteResults().isEmpty()) {
                            if (dltLakehouseCommitter == null) {
                                dltLakehouseCommitter = newDltLakehouseCommitter();
                            }
                            committedToDLTTable = dltLakehouseCommitter.isTheCompactStreamTaskCommitted(task);
                        }
                    }

                    if (task instanceof DeltaCompactStreamTask deltaCompactStreamTask) {
                        if (deltaCompactStreamTask.getDltDeltaFiles() != null
                                && !deltaCompactStreamTask.getDltDeltaFiles().isEmpty()) {
                            if (dltLakehouseCommitter == null) {
                                dltLakehouseCommitter = newDltLakehouseCommitter();
                            }
                            committedToDLTTable = dltLakehouseCommitter.isTheCompactStreamTaskCommitted(task);
                        }
                    }
                } catch (IOException e) {
                    throw new LakehouseOptException(ExceptionCode.LAKEHOUSE_COMMIT_ERROR,
                            String.format("Failed to check whether the compact task %s is committed to lakehouse.",
                                    task.getTaskName()), e);
                }
                if (committedToDataTable && committedToDLTTable) {
                    task.setStatus(CompactStreamTask.COMMITTED);
                    try {
                        compactTaskManager.updateCompactTask(task).get();
                    } catch (Exception e) {
                        throw new LakehouseOptException(ExceptionCode.COMPACTION_UPDATE_TASK_ERROR,
                                String.format("Failed update task %s to committed status", task.getTaskName()), e);
                    }
                    if (task.getFilePath() != null) {
                        // compactOxiaIndex is idempotent, we don't need to record the state for it.
                        compactOxiaIndex(task);
                    }
                    updateFutures.add(compactTaskManager.deleteCompactTask(task));
                } else {
                    needCommitTasks.add(task);
                    if (committedToDataTable) {
                        dataCommittedTaskIds.add(task.getTaskName());
                    }
                    if (committedToDLTTable) {
                        dltCommittedTaskIds.add(task.getTaskName());
                    }
                }
            } else if (CompactStreamTask.COMMITTED == task.getStatus()) {
                if (task.getFilePath() != null) {
                    // compactOxiaIndex is idempotent, we don't need to record the state for it.
                    compactOxiaIndex(task);
                }
                updateFutures.add(compactTaskManager.deleteCompactTask(task));
            }
        }
        if (!updateFutures.isEmpty()) {
            try {
                CompletableFuture.allOf(updateFutures.toArray(new CompletableFuture[0])).get();
                updateFutures.clear();
            } catch (Exception e) {
                log.warn("Failed to delete committed compact tasks");
            }
        }
    }

    private boolean hasFiles(CompactStreamTask task) {
        if (task instanceof IcebergCompactStreamTask icebergCompactStreamTask) {
            return CollectionUtils.isNotEmpty(icebergCompactStreamTask.getWriteResults());
        }
        if (task instanceof DeltaCompactStreamTask deltaCompactStreamTask) {
            return CollectionUtils.isNotEmpty(deltaCompactStreamTask.getDeltaFiles());
        }
        return false;
    }

    private boolean hasDltFiles(CompactStreamTask task) {
        if (task instanceof IcebergCompactStreamTask icebergCompactStreamTask) {
            return CollectionUtils.isNotEmpty(icebergCompactStreamTask.getDltWriteResults());
        }
        if (task instanceof DeltaCompactStreamTask deltaCompactStreamTask) {
            return CollectionUtils.isNotEmpty(deltaCompactStreamTask.getDltDeltaFiles());
        }
        return false;
    }

    @Override
    public void close() {
        if (lakehouseCommitter == null) {
            return;
        }
        try {
            lakehouseCommitter.close();
        } catch (Throwable e) {
            log.error("Failed to close lakehouse", e);
        }
    }
}
