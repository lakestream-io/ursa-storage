/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.storage.impl.compaction;

import io.lakestream.ursa.compaction.CompactTaskManager;
import io.lakestream.ursa.compaction.metrics.CompactionMetrics;
import io.lakestream.ursa.compaction.task.CompactStreamTask;
import io.lakestream.ursa.compaction.task.PackagedCompactStreamTask;
import io.lakestream.ursa.storage.impl.StorageConfig;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class CommitTaskProvider {

    private final CompactTaskManager manager;
    final Map<String, ConcurrentSkipListSet<CompactStreamTask>> tasks = new ConcurrentHashMap<>();
    final Map<String, ConcurrentSkipListSet<CompactStreamTask>> dLQTasks = new ConcurrentHashMap<>();
    private final StorageConfig config;
    private TaskStat currentTaskStat;
    private final CompactionMetrics compactionMetrics;
    private final int recordNonCommittableTaskThreshold;

    public CommitTaskProvider(StorageConfig config,
                              CompactTaskManager taskManager,
                              CompactionMetrics compactionMetrics) {
        this.manager = taskManager;
        this.config = config;
        this.compactionMetrics = compactionMetrics;
        this.currentTaskStat = new TaskStat(0, 0);
        this.recordNonCommittableTaskThreshold = config.getRecordNonCommittableTaskThreshold();
    }

    record TaskStat(int total, int readyToCommit) {
        // Calculate ready percentage
        public double getReadyPercentage() {
            return total == 0 ? 0.0 : (double) readyToCommit / total;
        }

        public boolean isEmpty() {
            return total == 0;
        }
    }

    public Map<String, List<CompactStreamTask>> getTask() throws Exception {
        fetchTasks();
        var executableTasks = getExecuteTasks(tasks);
        long taskCount = executableTasks.values().stream()
                .mapToInt(List::size)
                .sum();
        log.info("Got {} executable tasks, Fetched tasks stats: ready to commit: {}, all tasks: {}",
                taskCount, currentTaskStat.readyToCommit, currentTaskStat.total);
        return executableTasks;
    }

    public Map<String, List<CompactStreamTask>> getDLQTask() throws Exception {
        fetchDLQTasks();
        return getExecuteTasks(dLQTasks);
    }

    /**
     * Get tasks that are ready to be executed.
     *
     * The map is keyed by the partitioned topic name, the value is all tasks from all the partitions
     *
     * @param source
     *      the source map is keyed by topic partitions because we collect tasks from all partitions
     *      and we sorted the tasks in each partition by their stream id and start offset.
     * @return
     */
    Map<String, List<CompactStreamTask>> getExecuteTasks(
            Map<String, ConcurrentSkipListSet<CompactStreamTask>> source) {
        Map<String, List<CompactStreamTask>> result = new HashMap<>();
        for (Map.Entry<String, ConcurrentSkipListSet<CompactStreamTask>> entry : source.entrySet()) {
            String topic = entry.getKey();
            var taskInTopic = entry.getValue();
            List<CompactStreamTask> executeTasksInTopic = new ArrayList<>();
            while (!taskInTopic.isEmpty()) {
                var task = taskInTopic.pollFirst();
                if (task == null || task.getStatus() == CompactStreamTask.INIT) {
                    break;
                }
                executeTasksInTopic.add(task);
            }
            int nonCommittableTaskCount = taskInTopic.size();
            compactionMetrics.getNonCommittableTaskHistogram().recordSuccess(nonCommittableTaskCount);
            if (nonCommittableTaskCount > recordNonCommittableTaskThreshold) {
                compactionMetrics.getNonCommittableTaskCount().set(nonCommittableTaskCount, Attributes.of(
                        AttributeKey.stringKey("topic"), topic));
            }
            if (!executeTasksInTopic.isEmpty()) {
                var partitionedTopicName = partitionedTopicName(topic);
                result.computeIfAbsent(partitionedTopicName, k -> new ArrayList<>())
                        .addAll(executeTasksInTopic);
            }
        }
        return result;
    }

    static String partitionedTopicName(String topic) {
        return StreamNames.partitionedTopicName(topic);
    }

    private void fetchTasks() throws Exception {
        tasks.clear();
        fetchTasks(manager.getAllTasks().get(), tasks);
    }

    private void fetchDLQTasks() throws Exception {
        dLQTasks.clear();
        fetchTasks(manager.getAllDLQTasks().get(), dLQTasks);
    }

    private void fetchTasks(List<PackagedCompactStreamTask> tasks,
                            Map<String, ConcurrentSkipListSet<CompactStreamTask>> taskMap) throws Exception {
        int totalTasks = 0;
        int readyToCommitTasks = 0;
        int tasksInInit = 0;
        int tasksInCompacted = 0;
        int tasksInPreparedCommit = 0;
        int tasksInCommitted = 0;
        for (PackagedCompactStreamTask packagedCompactStreamTask : tasks) {
            boolean deleteTask = true;
            for (String subTask : packagedCompactStreamTask.getSubTasks()) {
                CompactStreamTask task = null;
                try {
                    task = manager.getCompactStreamTask(subTask).get();
                } catch (Exception e) {
                    log.error("Failed to get CompactStreamTask for subTask {}: {}", subTask, e.getMessage());
                    deleteTask = false;
                    continue;
                }

                if (task != null) {
                    deleteTask = false;
                    totalTasks++;
                    if (task.getStatus() != CompactStreamTask.INIT) {
                        readyToCommitTasks++;
                    }
                    // Count tasks in different statuses
                    switch (task.getStatus()) {
                        case CompactStreamTask.INIT -> tasksInInit++;
                        case CompactStreamTask.COMPACTED -> tasksInCompacted++;
                        case CompactStreamTask.PREPARED_COMMIT -> tasksInPreparedCommit++;
                        case CompactStreamTask.COMMITTED -> tasksInCommitted++;
                    }

                    var topic = task.getTopic();
                    if (topic != null) {
                        taskMap.computeIfAbsent(topic, k -> new ConcurrentSkipListSet<>()).add(task);
                    } else {
                        log.warn("CompactStreamTask {} has no topic, skip it", task.getTaskName());
                    }
                }
            }
            if (deleteTask) {
                manager.deletePackagedTaskName(packagedCompactStreamTask.getTaskName());
            }
        }
        currentTaskStat = new TaskStat(totalTasks, readyToCommitTasks);
        log.info("Fetched {} tasks in {} partitioned topics - Total: {}, Init: {}, Compacted: {}, "
                        + "Prepared Commit: {}, Committed: {}",
                totalTasks, tasks.size(), totalTasks, tasksInInit, tasksInCompacted,
                tasksInPreparedCommit, tasksInCommitted);
        // update metrics
        compactionMetrics.getTasksInInitState().set(tasksInInit);
        compactionMetrics.getTasksInCompactedState().set(tasksInCompacted);
        compactionMetrics.getTasksInPreparedCommitState().set(tasksInPreparedCommit);
        compactionMetrics.getTasksInCommittedState().set(tasksInCommitted);
    }
}
