/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.lakehouse.compact;

import static io.lakestream.ursa.lakehouse.v2.AbstractLakehouseWriter.BATCH_MESSAGE_COUNT;
import static io.lakestream.ursa.lakehouse.v2.AbstractLakehouseWriter.LAST_BATCH_ID_IN_FILE;
import static io.lakestream.ursa.lakehouse.v2.AbstractLakehouseWriter.LAST_ENTRY_ID_IN_FILE;

import io.lakestream.ursa.compaction.CompactTaskManager;
import io.lakestream.ursa.compaction.task.CompactStreamTask;
import io.lakestream.ursa.compaction.task.ManagedWriteResult;
import io.lakestream.ursa.exception.ExceptionCode;
import io.lakestream.ursa.exception.ExceptionWithCode;
import io.lakestream.ursa.lakehouse.delta.DeltaCompactStreamTask;
import io.lakestream.ursa.lakehouse.iceberg.IcebergCompactStreamTask;
import io.lakestream.ursa.lakehouse.v2.IWriteResult;
import io.lakestream.ursa.lakehouse.v2.delta.DeltaWriteResult;
import io.lakestream.ursa.lakehouse.v2.iceberg.IcebergWriteResult;
import io.lakestream.ursa.lakehouse.v2.io.parquet.ParquetWriteResult;
import io.lakestream.ursa.lakehouse.writer.ParquetFileStat;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.iceberg.io.WriteResult;

/**
 * Finalizes a compaction/materialization task by recording its write results onto the
 * {@link CompactStreamTask} and persisting it as {@link CompactStreamTask#COMPACTED} via the
 * {@link CompactTaskManager}. The downstream {@code CompactedTaskRunner} then reads {@code COMPACTED}
 * tasks from Oxia, checks status, and applies the batched (group) catalog commit.
 *
 * <p>Extracted from {@code LakehouseCompactionWorker.completeCompaction} so every materialization
 * dispatch reuses the exact same task-completion and persistence logic (rather than committing per
 * task, which would bypass the group-commit runner).
 *
 * <p>Every managed (stream-backed table) Parquet file is always recorded as a {@link ManagedWriteResult}
 * so the commit runner can publish a {@code ManagedTableFileIndex} in the compacted entry index. Both
 * compacted-object readers ({@code LakehouseKafkaReaderV2} and the Kafka-only
 * {@code KafkaLakehouseReader}) require that index to locate the Parquet file for an offset; a compacted
 * range without it is unreadable.
 */
@Slf4j
public class CompactionTaskCompleter {

    /**
     * Historical property that used to gate {@code ManagedTableFileIndex} emission. It is now a no-op:
     * the index is always written because no reader can consume a compacted range without it.
     */
    public static final String MANAGED_TABLE_SCHEMA_EVOLUTION_ENABLED = "managedTableSchemaEvolutionEnabled";

    private final CompactTaskManager compactTaskManager;

    public CompactionTaskCompleter(CompactTaskManager compactTaskManager) {
        this.compactTaskManager = compactTaskManager;
    }

    /**
     * @deprecated the {@code managedTableSchemaEvolutionEnabled} flag no longer has any effect; use
     *     {@link #CompactionTaskCompleter(CompactTaskManager)}.
     */
    @Deprecated
    public CompactionTaskCompleter(CompactTaskManager compactTaskManager,
                                   boolean managedTableSchemaEvolutionEnabled) {
        this(compactTaskManager);
    }

    /**
     * Logs a warning when the deprecated {@code managedTableSchemaEvolutionEnabled} property is present
     * so operators know it can be removed from their configuration.
     */
    public static void warnIfDeprecatedFlagConfigured(Object configuredValue) {
        if (configuredValue != null) {
            log.warn("Property '{}' (={}) is deprecated and ignored: the ManagedTableFileIndex is now always "
                     + "written for compacted stream-backed tables. Remove it from the configuration.",
                MANAGED_TABLE_SCHEMA_EVOLUTION_ENABLED, configuredValue);
        }
    }

    /**
     * Records the managed / external write results on the task and persists it as
     * {@code COMPACTED}. At least one of the result lists must be non-empty.
     */
    public void completeCompaction(CompactStreamTask task, List<IWriteResult> managedResults,
                                   List<IWriteResult> externalResults, List<IWriteResult> externalDLTResults)
            throws Exception {

        if (managedResults.isEmpty() && externalResults.isEmpty() && externalDLTResults.isEmpty()) {
            throw new ExceptionWithCode(ExceptionCode.COMPACTION_NO_WRITE_RESULT,
                String.format("[%s] No write results found for the compaction task %s. It should only happen on "
                              + "both the sbt and sdt disabled. Please check the configuration of the compaction "
                              + "or the task properties.", task.getTopic(), task.getTaskName()));
        }

        completeManagedCompaction(task, managedResults);

        Optional<CompactStreamTask> compactStreamTask = Optional.empty();
        if (!externalResults.isEmpty() || !externalDLTResults.isEmpty()) {
            compactStreamTask = completeExternalCompaction(task, externalResults, externalDLTResults);
        }

        if (compactStreamTask.isPresent()) {
            compactTaskManager.updateCompactTask(compactStreamTask.get()).get();
        } else {
            compactTaskManager.updateCompactTask(task).get();
        }
    }

    private void completeManagedCompaction(CompactStreamTask task, List<IWriteResult> writeResults) {
        task.setStatus(CompactStreamTask.COMPACTED);
        // Use completion time because the source append timestamp is not available here.
        task.setMessageWrittenToUrsaTime(System.currentTimeMillis());
        task.setRealStartOffset(task.getStartOffset());
        task.setRealEndOffset(task.getEndOffset());

        if (writeResults.isEmpty()) {
            return;
        }

        var wr = (ParquetWriteResult) writeResults.get(0);
        var stat = createParquetFileStat(writeResults);
        task.setFilePath(stat.getFilePath());
        task.setFileFullPath(stat.getFileFullPath());
        task.setFileSize(stat.getFileSize());
        var messages = (AtomicLong) wr.getExtraMetadata().get(BATCH_MESSAGE_COUNT);
        task.setNumberOfRecordsInCompactedFile(Math.toIntExact(messages.get()));
        task.setStats(stat.getStats());
        task.setPartitionValues(Collections.emptyMap());

        TreeSet<ManagedWriteResult> managedWriteResults = new TreeSet<>();
        for (IWriteResult writeResult : writeResults) {
            if (writeResult instanceof ParquetWriteResult pwr) {
                var filePath = pwr.getDataFile();
                var fileFullPath = pwr.getDirectory().resolve(pwr.getDataFile()).toString();
                var fileSize = pwr.getDataFileSize();
                var messageCount = (AtomicLong) pwr.getExtraMetadata().get(BATCH_MESSAGE_COUNT);
                var messageCountIntValue = Math.toIntExact(messageCount.get());
                long lastEntryId = (long) pwr.getExtraMetadata().getOrDefault(LAST_ENTRY_ID_IN_FILE, -1L);
                long lastBatchId = (long) pwr.getExtraMetadata().getOrDefault(LAST_BATCH_ID_IN_FILE, -1L);
                var mwr = ManagedWriteResult.builder()
                    .filePath(filePath)
                    .fullFilePath(fileFullPath)
                    .fileSize(fileSize)
                    .numberOfMessages(messageCountIntValue)
                    .lastEntryId(lastEntryId)
                    .lastBatchId(lastBatchId)
                    .build();
                managedWriteResults.add(mwr);
            }
        }
        task.setManagedWriteResults(managedWriteResults);
    }

    private Optional<CompactStreamTask> completeExternalCompaction(CompactStreamTask task,
                                                                   List<IWriteResult> writeResults,
                                                                   List<IWriteResult> dltWriteResults) {

        IWriteResult first = !writeResults.isEmpty()
                ? writeResults.get(0)
                : dltWriteResults.get(0);

        if (first instanceof IcebergWriteResult) {
            IcebergCompactStreamTask icebergTask = new IcebergCompactStreamTask(task);
            icebergTask.setWriteResults(collectIcebergResults(writeResults));
            icebergTask.setDltWriteResults(collectIcebergResults(dltWriteResults));
            return Optional.of(icebergTask);

        } else if (first instanceof DeltaWriteResult) {
            DeltaCompactStreamTask deltaTask = new DeltaCompactStreamTask(task);
            deltaTask.setDeltaFiles(collectDeltaResults(writeResults));
            deltaTask.setDltDeltaFiles(collectDeltaResults(dltWriteResults));
            return Optional.of(deltaTask);
        }

        throw new IllegalArgumentException(
                "Unsupported write result type for external compaction: " + first.getClass().getName());
    }

    private List<WriteResult> collectIcebergResults(List<IWriteResult> results) {
        return results.stream()
                .map(r -> {
                    if (r instanceof IcebergWriteResult iwr) {
                        return iwr.getWriteResult();
                    }
                    throw new IllegalArgumentException("Mixed write result type: " + r.getClass().getName());
                })
                .collect(Collectors.toList());
    }

    private List<ParquetFileStat> collectDeltaResults(List<IWriteResult> results) {
        return results.stream()
                .map(r -> {
                    if (r instanceof DeltaWriteResult dwr) {
                        return dwr.getWriteResult();
                    }
                    throw new IllegalArgumentException("Mixed write result type: " + r.getClass().getName());
                })
                .flatMap(List::stream)
                .collect(Collectors.toList());
    }

    public static ParquetFileStat createParquetFileStat(List<IWriteResult> writeResults) {
        var wr = (ParquetWriteResult) writeResults.get(0);
        return ParquetFileStat.builder()
            .filePath(wr.getDataFile())
            .fileFullPath(wr.getDirectory().resolve(wr.getDataFile()).toString())
            .fileSize(wr.getDataFileSize())
            .partitionValues(Collections.emptyMap())
            .stats("")
            .tags(Collections.emptyMap())
            .build();
    }
}
