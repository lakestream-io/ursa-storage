/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.lakehouse.compact;

import io.lakestream.ursa.compaction.CompactTaskManager;
import io.lakestream.ursa.compaction.metrics.CompactionMetrics;
import io.lakestream.ursa.compaction.task.CompactStreamTask;
import io.lakestream.ursa.exception.ExceptionCode;
import io.lakestream.ursa.exception.LakehouseOptException;
import io.lakestream.ursa.lakehouse.LakehouseCommitter;
import io.lakestream.ursa.lakehouse.LakehouseConfiguration;
import io.lakestream.ursa.lakehouse.catalog.unity.UnityCatalogApi;
import io.lakestream.ursa.lakehouse.catalog.unity.UnityCatalogExternalLineageRequest;
import io.lakestream.ursa.lakehouse.catalog.unity.UnityCatalogLineageUtil;
import io.lakestream.ursa.lakehouse.exception.IcebergTableCorruptedException;
import io.lakestream.ursa.lakehouse.exception.LakehouseException;
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
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.CompletableFuture;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class AppendCommitParquetFileRunner extends AbstractCommitRunner implements CommitRunner {

    private final List<LakehouseCommitter> lakehouseCommitters;
    private final CompactionMetrics compactionMetrics;
    private final UnityCatalogApi unityCatalogApi;
    private Map<String, String> latestTableProperties;

    public AppendCommitParquetFileRunner(StorageApi storageApi,
                                         CompactTaskManager compactTaskManager,
                                         StorageConfig config,
                                         String parentTopic,
                                         CompactionMetrics compactionMetrics) {
        super(storageApi, compactTaskManager, parentTopic, config, compactionMetrics);
        LakehouseConfiguration lakehouseConfig = new LakehouseConfiguration(config.getProperties());
        this.lakehouseCommitters = LakehouseCommitter.get(lakehouseConfig, parentTopic);
        this.compactionMetrics = compactionMetrics;
        this.unityCatalogApi = lakehouseConfig.isUnityCatalogByolEnabled()
                ? UnityCatalogApi.getInstance(lakehouseConfig) : null;
    }

    public void commitFiles(List<CompactStreamTask> compactStreamTasks) throws LakehouseOptException {
        //Need check table data task
        List<CompactStreamTask> needCommitTasks = new ArrayList<>();
        Map<String, List<CompactStreamTask>> unCommittedTasks = new HashMap<>();
        List<CompletableFuture<Void>> futures = new ArrayList<>();
        for (CompactStreamTask compactStreamTask : compactStreamTasks) {
            if (CompactStreamTask.COMPACTED == compactStreamTask.getStatus()) {
                needCommitTasks.add(compactStreamTask);
            } else if (CompactStreamTask.PREPARED_COMMIT == compactStreamTask.getStatus()) {
                boolean committed = true;
                for (LakehouseCommitter lakehouseCommitter : lakehouseCommitters) {
                    try {
                        if (!lakehouseCommitter.isTheCompactStreamTaskCommitted(compactStreamTask)) {
                            unCommittedTasks.computeIfAbsent(lakehouseCommitter.getName(), k -> new ArrayList<>())
                                    .add(compactStreamTask);
                            committed = false;
                        }
                    } catch (IOException e) {
                        throw new LakehouseOptException(ExceptionCode.LAKEHOUSE_COMMIT_ERROR,
                            String.format("Failed to check whether the compact task %s is committed to lakehouse.",
                                compactStreamTask.getTaskName()), e);
                    }

                }
                if (committed) {
                    compactStreamTask.setStatus(CompactStreamTask.COMMITTED);
                    try {
                        compactTaskManager.updateCompactTask(compactStreamTask).get();
                    } catch (Exception e) {
                        throw new LakehouseOptException(ExceptionCode.COMPACTION_UPDATE_TASK_ERROR,
                            String.format("Failed update task %s to committed status",
                                compactStreamTask.getTaskName()), e);
                    }
                    compactOxiaIndex(compactStreamTask);
                    futures.add(compactTaskManager.deleteCompactTask(compactStreamTask));
                }
            } else if (CompactStreamTask.COMMITTED == compactStreamTask.getStatus()) {
                compactOxiaIndex(compactStreamTask);
                futures.add(compactTaskManager.deleteCompactTask(compactStreamTask));
            }
        }
        if (!futures.isEmpty()) {
            try {
                CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).get();
                futures.clear();
            } catch (Exception e) {
                log.warn("Failed to delete committed compact tasks");
            }
        }

        if (!needCommitTasks.isEmpty() || !unCommittedTasks.isEmpty()) {
            for (CompactStreamTask needCommitTask : needCommitTasks) {
                needCommitTask.setStatus(CompactStreamTask.PREPARED_COMMIT);
                futures.add(compactTaskManager.updateCompactTask(needCommitTask));
            }
            try {
                CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).get();
                futures.clear();
            } catch (Exception e) {
                throw new LakehouseOptException(ExceptionCode.COMPACTION_UPDATE_TASK_ERROR,
                    "Failed to update compact tasks to prepared commit status", e);
            }

            long start = System.nanoTime();
            commitToLakehouse(needCommitTasks, unCommittedTasks);
            compactionMetrics.getCommitToLakehouseLatency().recordSuccess(System.nanoTime() - start);

            Set<CompactStreamTask> totalNeedCommitTask = new TreeSet<>(needCommitTasks);
            unCommittedTasks.values().forEach(totalNeedCommitTask::addAll);

            for (CompactStreamTask needCommitTask : totalNeedCommitTask) {
                needCommitTask.setStatus(CompactStreamTask.COMMITTED);
                futures.add(compactTaskManager.updateCompactTask(needCommitTask));
                if (needCommitTask.getMessageWrittenToUrsaTime() > 0) {
                    compactionMetrics.getMessageEndToEndCompactLatency().recordSuccess(
                            (System.currentTimeMillis() - needCommitTask.getMessageWrittenToUrsaTime()) * 1_000_000);
                }
                assert needCommitTask.getTopic() != null;
                updateLastCommitOffset(needCommitTask);
            }

            try {
                CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).get();
                futures.clear();
            } catch (Exception e) {
                throw new LakehouseOptException(ExceptionCode.COMPACTION_UPDATE_TASK_ERROR,
                    "Failed to update compact tasks to committed status", e);
            }

            for (CompactStreamTask needCommitTask : totalNeedCommitTask) {
                compactOxiaIndex(needCommitTask);
                futures.add(compactTaskManager.deleteCompactTask(needCommitTask));
            }

            try {
                CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).get();
                futures.clear();
            } catch (Exception e) {
                log.warn("Failed to delete committed compact tasks");
            }

            // record last commit offset
            lastCommitOffset.forEach((topic, commitOffset) -> {
                    compactionMetrics.getLastCompactedOffset()
                        .set(commitOffset.offset(), Attributes.of(AttributeKey.stringKey("topic"), topic));
            });
        }
    }

    private void commitToLakehouse(List<CompactStreamTask> compactStreamTasks,
                                   Map<String, List<CompactStreamTask>> unCommittedTasks)
        throws LakehouseOptException {
        if (lakehouseCommitters.isEmpty()) {
            return;
        }

        log.info("Start to commit {} to the lakehouse", compactStreamTasks);
        List<ParquetFileStat> fileStats = new ArrayList<>();
        Map<String, String> tableProperties = null;
        for (CompactStreamTask compactStreamTask : compactStreamTasks) {
            compactionMetrics.getCommittedParquetFileBytes().set(compactStreamTask.getFileSize());
            fileStats.add(generateParquetFileStat(compactStreamTask));
            if (tableProperties == null) {
                tableProperties = compactStreamTask.getProperties();
            }
        }

        if (!unCommittedTasks.isEmpty()) {
            for (LakehouseCommitter lakehouseCommitter : lakehouseCommitters) {
                List<CompactStreamTask> unCommittedTask = unCommittedTasks.get(lakehouseCommitter.getName());
                if (unCommittedTask != null) {
                    List<ParquetFileStat> unCommittedFileStats = new ArrayList<>(fileStats);
                    for (CompactStreamTask compactStreamTask : unCommittedTask) {
                        unCommittedFileStats.add(generateParquetFileStat(compactStreamTask));
                    }
                    commitTasksToLakehouse(lakehouseCommitter, unCommittedFileStats);
                    compactionMetrics.getCommitTaskBatchSize().set(unCommittedFileStats.size());
                } else {
                    commitTasksToLakehouse(lakehouseCommitter, fileStats);
                    compactionMetrics.getCommitTaskBatchSize().set(fileStats.size());
                }
            }
        } else {
            for (LakehouseCommitter lakehouseCommitter : lakehouseCommitters) {
                commitTasksToLakehouse(lakehouseCommitter, fileStats);
                compactionMetrics.getCommitTaskBatchSize().set(fileStats.size());
            }
        }

        latestTableProperties = tableProperties;
        log.info("Complete commit {} to the lakehouse", compactStreamTasks);

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

    private void updateTableProperties(Map<String, String> tableProperties) {
        for (LakehouseCommitter lakehouseCommitter : lakehouseCommitters) {
            try {
                lakehouseCommitter.updateTablePropertiesIfNeeded(tableProperties);
            } catch (LakehouseException e) {
                log.error("Failed to update table properties for topic {}", parentTopic, e);
            }
        }
    }

    public void commitTasksToLakehouse(LakehouseCommitter committer, List<ParquetFileStat> fileStats)
            throws LakehouseOptException {
        try {
            if (committer != null) {
                long metadataFileSize = committer.commit(fileStats);
                if (metadataFileSize > 0) {
                    compactionMetrics.getLakehouseMetadataFileSize().set(metadataFileSize,
                            Attributes.of(AttributeKey.stringKey("topic"), parentTopic));
                }
                compactionMetrics.getCommitTaskBatchSize().set(fileStats.size());
            } else {
                log.warn("Lakehouse committer is null, cannot commit tasks to lakehouse.");
            }
        } catch (Exception e) {
            ExceptionCode exceptionCode = ExceptionCode.LAKEHOUSE_COMMIT_ERROR;
            if (e instanceof IcebergTableCorruptedException) {
                exceptionCode = ExceptionCode.LAKEHOUSE_TABLE_CORRUPTED_ERROR;
            }

            throw new LakehouseOptException(exceptionCode,
                "Failed to commit to the lakehouse for topic " + parentTopic, e);
        }
    }

    static ParquetFileStat generateParquetFileStat(CompactStreamTask compactStreamTask) {
        String filePath = compactStreamTask.getFilePath();
        Map<String, String> tags = new HashMap<>();
        tags.put("streamId", String.valueOf(compactStreamTask.getStreamId()));
        tags.put("startOffset", String.valueOf(compactStreamTask.getStartOffset()));
        tags.put("endOffset", String.valueOf(compactStreamTask.getEndOffset()));
        tags.put("totalSize", String.valueOf(compactStreamTask.getTotalSize()));
        tags.put("cumulativeSize", String.valueOf(compactStreamTask.getCumulativeSize()));
        tags.put("totalMessage",
                String.valueOf(compactStreamTask.getEndOffset() - compactStreamTask.getStartOffset()));
        tags.put("filePath", filePath);
        tags.put("realStartOffset", String.valueOf(compactStreamTask.getRealStartOffset()));
        tags.put("realEndOffset", String.valueOf(compactStreamTask.getRealEndOffset()));
        tags.put("topic", compactStreamTask.getTopic());
        return new ParquetFileStat(compactStreamTask.getFilePath(), compactStreamTask.getFileFullPath(),
                compactStreamTask.getFileSize(), compactStreamTask.getStats(),
                compactStreamTask.getPartitionValues(), tags);
    }

    @Override
    public void close() {
        for (LakehouseCommitter lakehouseCommitter : lakehouseCommitters) {
            try {
                lakehouseCommitter.close();
            } catch (Throwable e) {
                log.error("Failed to close lakehouseCommitter: {}", lakehouseCommitter, e);
            }
        }
    }
}
