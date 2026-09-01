/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.lakehouse.compact;

import static io.lakestream.ursa.storage.proto.IndexType.COMPACT;

import io.lakestream.api.Position;
import io.lakestream.ursa.compaction.CompactTaskManager;
import io.lakestream.ursa.compaction.common.ManagedTableFileIndex;
import io.lakestream.ursa.compaction.metrics.CompactionMetrics;
import io.lakestream.ursa.compaction.task.CompactStreamTask;
import io.lakestream.ursa.compaction.task.ManagedWriteResult;
import io.lakestream.ursa.compaction.task.OffsetRange;
import io.lakestream.ursa.exception.ExceptionCode;
import io.lakestream.ursa.exception.ExceptionWithCode;
import io.lakestream.ursa.exception.LakehouseOptException;
import io.lakestream.ursa.lakehouse.LakehouseCommitter;
import io.lakestream.ursa.lakehouse.LakehouseConfiguration;
import io.lakestream.ursa.lakehouse.iceberg.IcebergCompactStreamTask;
import io.lakestream.ursa.lakehouse.utils.TopicName;
import io.lakestream.ursa.storage.StorageApi;
import io.lakestream.ursa.storage.Value;
import io.lakestream.ursa.storage.impl.StorageConfig;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.common.AttributesBuilder;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Predicate;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.iceberg.io.WriteResult;

@Slf4j
public class AbstractCommitRunner {

    protected final StorageApi storageApi;
    protected final CompactTaskManager compactTaskManager;
    protected final Map<String, CommitOffset> lastCommitOffset = new ConcurrentHashMap<>();
    @Getter
    private boolean tableUnCommittable = false;
    protected final CompactionMetrics compactionMetrics;
    protected final String parentTopic;
    protected String dltTopic;
    protected final LakehouseConfiguration config;

    private final int maxTaskCombineSize;

    protected final Set<String> dltCommittedTaskIds = new HashSet<>();
    protected final Set<String> dataCommittedTaskIds = new HashSet<>();
    // Lasy init
    protected LakehouseCommitter dltLakehouseCommitter = null;

    record CommitOffset(long id, long offset) implements Comparable<CommitOffset> {

        @Override
        public int compareTo(CommitOffset other) {
            // First, compare by 'id'
            int idComparison = Long.compare(this.id, other.id);

            // If IDs are different, return the result of the ID comparison
            if (idComparison != 0) {
                return idComparison;
            }

            // If IDs are the same, compare by 'offset'
            return Long.compare(this.offset, other.offset);
        }
    }

    AbstractCommitRunner(StorageApi storageApi, CompactTaskManager compactTaskManager) {
        this(storageApi, compactTaskManager, "null", new StorageConfig(), CompactionMetrics.NOOP);
    }

    AbstractCommitRunner(StorageApi storageApi, CompactTaskManager compactTaskManager, String parentTopic,
                         StorageConfig storageConfig, CompactionMetrics compactionMetrics) {
        this.storageApi = storageApi;
        this.compactTaskManager = compactTaskManager;
        this.parentTopic = parentTopic;
        this.maxTaskCombineSize = storageConfig.getMaxTaskCombineSize();
        this.compactionMetrics = compactionMetrics;
        this.config = new LakehouseConfiguration(storageConfig.getProperties());
        this.dltTopic = TopicName.get(parentTopic).getPartitionedTopicName() + config.getDltSuffix();
    }

    public void commit(List<CompactStreamTask> tasks) throws ExceptionWithCode {
        // if tasks has an upsert case
        var hasUpsertCase = tasks.parallelStream().filter(this::hasIcebergDeleteFiles).findAny();
        if (hasUpsertCase.isPresent()) {
            commitIcebergWithUpsertCase(tasks);
        } else {
            commitNormally(tasks);
        }
    }

    private boolean hasIcebergDeleteFiles(CompactStreamTask task) {
        if (!(task instanceof IcebergCompactStreamTask icebergTask)) {
            return false;
        }

        // Helper to check if any write result has delete files
        Predicate<List<WriteResult>> hasDeleteFiles = results ->
                results != null && results.stream()
                        .anyMatch(r -> r.deleteFiles() != null && r.deleteFiles().length > 0);

        if (hasDeleteFiles.test(icebergTask.getWriteResults())
                || hasDeleteFiles.test(icebergTask.getDltWriteResults())) {
            return true;
        }

        var singleResult = icebergTask.getWriteResult();
        return singleResult != null
                && singleResult.deleteFiles() != null
                && singleResult.deleteFiles().length > 0;
    }

    // todo: improve this by include them in one commit transaction

    /**
     * Commit tasks with upsert case, which means the tasks contain iceberg delete files.
     * We will commit the tasks in batches, each batch contains tasks
     * that have delete files until the next task that has delete files.
     * For example, if we have tasks A, B, C, D, E, F, G,
     * where A, E have delete files,
     * We will commit them in batches like this:
     * * 1. commit(A)
     * * 2. commit(B, C, D)
     * * 3. commit(E)
     * * 4. commit(F, G)
     * * This is to ensure that the delete files are committed before the data files,
     * * so that the data files can be properly upserted.
     *
     * @param tasks
     */
    protected void commitIcebergWithUpsertCase(List<CompactStreamTask> tasks) throws ExceptionWithCode {
        int lastDeleteFileTaskIndex = -1;

        for (int i = 0; i < tasks.size(); i++) {
            CompactStreamTask currentTask = tasks.get(i);
            // If the current task contains delete files
            if (hasIcebergDeleteFiles(currentTask)) {
                // commit all the tasks since the last delete file task
                List<CompactStreamTask> batchToCommit = tasks.subList(lastDeleteFileTaskIndex + 1, i);
                commitNormally(batchToCommit);

                // commit the delete file task
                commitNormally(List.of(currentTask));

                updateLastCommitOffset(currentTask);
                // Update the last delete file task index
                lastDeleteFileTaskIndex = i;
            }
        }

        // Commit any remaining tasks after the last delete file task
        if (lastDeleteFileTaskIndex < tasks.size() - 1) {
            List<CompactStreamTask> remainingTasks = tasks.subList(lastDeleteFileTaskIndex + 1, tasks.size());
            updateLastCommitOffset(tasks.get(tasks.size() - 1));
            commitTasks(remainingTasks);
        }
    }

    protected void updateLastCommitOffset(CompactStreamTask needCommitTask) {
        lastCommitOffset.merge(needCommitTask.getTopic(),
            new CommitOffset(needCommitTask.getStreamId(), OffsetRange.lastIncludedOffset(
                    needCommitTask.getStartOffset(), needCommitTask.getEndOffset())),
            (existing, incoming) -> {
                // If the incoming CommitOffset is "greater" (as defined by compareTo)
                // than the existing one, we return the incoming one to be stored.
                // Otherwise, we keep the existing one.
                if (incoming.compareTo(existing) > 0) {
                    return incoming;
                } else {
                    return existing;
                }
                // A more concise way: return incoming.compareTo(existing) > 0 ? incoming : existing;
                // Or even shorter using Math.max (if your CommitOffset was just one number
                // and could be represented as such)
                // But for composite objects like CommitOffset, the compareTo approach is correct.
            });
    }

    /**
     * Commit tasks normally, if the size of tasks is larger than maxTaskCombineSize,
     * we will split them into multiple batches and commit each batch separately.
     *
     * @param tasks the list of tasks to commit
     */
    protected void commitNormally(List<CompactStreamTask> tasks) throws ExceptionWithCode {
        if (tasks.size() > maxTaskCombineSize) {
            int maxSize = Math.toIntExact(maxTaskCombineSize);
            for (int startIdx = 0; startIdx < tasks.size(); startIdx += maxSize) {
                int endIdx = Math.min(startIdx + maxSize, tasks.size());
                commitTasks(tasks.subList(startIdx, endIdx));
            }
        } else {
            commitTasks(tasks);
        }
    }

    public void commitTasks(List<CompactStreamTask> needCommitTasks) throws ExceptionWithCode {
        if (tableUnCommittable) {
            return;
        }

        long start = System.nanoTime();
        var topic = TopicName.get(parentTopic);
        AttributesBuilder attrBuilder = Attributes.builder()
            .put(AttributeKey.stringKey("namespace"), topic.getNamespace())
            .put(AttributeKey.stringKey("topic"), parentTopic);
        try {
            commitFiles(needCommitTasks);
            compactionMetrics.getCompactTaskCommitLatency().recordSuccess(System.nanoTime() - start);
        } catch (Throwable throwable) {
            compactionMetrics.getCompactTaskCommitLatency().recordFailure(System.nanoTime() - start);
            log.error("Failed to commit files for task: {}, count: {}, topic: {} ",
                needCommitTasks.get(0).getTaskName(), needCommitTasks.size(), parentTopic, throwable);
            int errorCode = ExceptionCode.UNKNOWN.getCode();
            if (throwable instanceof ExceptionWithCode ewc) {
                errorCode = ewc.getExceptionCode().getCode();
                if (ewc.getExceptionCode() == ExceptionCode.LAKEHOUSE_TABLE_CORRUPTED_ERROR) {
                    tableUnCommittable = true;
                    compactionMetrics.getTableCorruptedCount().increment();
                    log.warn("{} encountered LakehouseTableCorruptedException, "
                        + "the remaining tasks in this topic will be published to DLQ", parentTopic);
                }
            }
            var attr = attrBuilder.put(AttributeKey.longKey("errorCode"), (long) errorCode).build();
            compactionMetrics.getCompactionErrorHappenTime().set(System.currentTimeMillis(), attr);
            throw propagateCommitFailure(throwable);
        }
        compactionMetrics.getLastCommitTime().set(System.currentTimeMillis(), attrBuilder.build());
    }

    private ExceptionWithCode propagateCommitFailure(Throwable throwable) {
        if (throwable instanceof Error error) {
            throw error;
        }
        if (throwable instanceof ExceptionWithCode exceptionWithCode) {
            return exceptionWithCode;
        }
        return new LakehouseOptException(ExceptionCode.UNKNOWN,
            "Failed to commit files for topic " + parentTopic, throwable);
    }

    public boolean needToPublishToDLQ() {
        return tableUnCommittable;
    }

    void commitFiles(List<CompactStreamTask> needCommitTasks) throws LakehouseOptException {}

    protected void compactOxiaIndex(CompactStreamTask compactStreamTask) throws LakehouseOptException {
        long streamId = compactStreamTask.getStreamId();
        String filePath = compactStreamTask.getFilePath();
        long realStartOffset = compactStreamTask.getRealStartOffset();
        long realEndOffset = compactStreamTask.getRealEndOffset();
        long cumulativeSize = compactStreamTask.getCumulativeSize();
        long realTotalMessage = realEndOffset - realStartOffset;
        long totalSize = compactStreamTask.getTotalSize();
        Position position = new Position(filePath, totalSize, 0, Position.FileType.PARQUET);
        if (realStartOffset != compactStreamTask.getStartOffset()
            || realEndOffset != compactStreamTask.getEndOffset()) {
            log.warn("Real start offset {} and end offset {} not match with compact stream task start offset {} "
                     + "and end offset {}", realStartOffset, realEndOffset, compactStreamTask.getStartOffset(),
                compactStreamTask.getEndOffset());
        }

        try {
            if (storageApi == null) {
                log.info("Storage API not available, no index update");
                return;
            }

            var value = new Value(realTotalMessage, totalSize, 1, COMPACT, position,
                Optional.empty(), Optional.of(new HashMap<>()));
            if (compactStreamTask.getManagedWriteResults() != null
                && !compactStreamTask.getManagedWriteResults().isEmpty()) {
                var extraMetadata = value.extraData().get();
                ManagedTableFileIndex fileIndex = new ManagedTableFileIndex();
                for (ManagedWriteResult wr : compactStreamTask.getManagedWriteResults()) {
                    fileIndex.append(wr.getLastEntryId(), wr.getFilePath());
                }
                extraMetadata.put(ManagedTableFileIndex.NAME, fileIndex.serializeToString());
            }
            storageApi.withStreamWriteLease(streamId, ignoredLease ->
                storageApi.compactEntryIndex(
                    streamId, realStartOffset + 1, realEndOffset,
                    cumulativeSize, value)).get();
        } catch (Exception e) {
            String msg = String.format("Failed to update oxia location for stream %d "
                                       + "startOffset %d endOffset %d to %s",
                streamId, realStartOffset, realEndOffset, position);
            throw new LakehouseOptException(ExceptionCode.COMPACTION_UPDATE_INDEX_ERROR, msg, e);
        }
    }

}
