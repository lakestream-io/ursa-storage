/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.storage.impl.compaction;

import com.google.common.annotations.VisibleForTesting;
import io.lakestream.ursa.compaction.CompactTaskManager;
import io.lakestream.ursa.compaction.metrics.CompactionMetrics;
import io.lakestream.ursa.compaction.task.CompactStreamTask;
import io.lakestream.ursa.compaction.task.PackagedCompactStreamTask;
import io.lakestream.ursa.storage.impl.StorageConfig;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class CompactionTaskProvider {

    private final Map<String, PackagedCompactStreamTask> pendingTasks = new HashMap<>();
    private final Queue<PackagedCompactStreamTask> currentTasks = new ConcurrentLinkedQueue<>();
    private final PriorityQueue<QuarantinedTask> quarantinedTask =
        new PriorityQueue<>(Comparator.comparingLong(QuarantinedTask::getKey));
    @VisibleForTesting
    @Getter
    private final Map<String, Long> quarantinedTopics = new ConcurrentHashMap<>();
    private final CompactTaskManager taskManager;
    private final CompactionMetrics compactionMetrics;

    public CompactionTaskProvider(StorageConfig config,
                                  CompactTaskManager taskManager,
                                  CompactionMetrics compactionMetrics) {
        this.taskManager = taskManager;
        this.compactionMetrics = compactionMetrics;
    }

    public void updateTasks(List<PackagedCompactStreamTask> newTasks) {
        synchronized (pendingTasks) {
            pendingTasks.clear();
            newTasks.forEach(task -> pendingTasks.put(task.getTaskName(), task));
        }
    }

    public PackagedCompactStreamTask getTask() {
        synchronized (pendingTasks) {
            // todo: shuffle the task list by stream id to reduce the conflict of the same stream
            //  in the different workers
            if (currentTasks.isEmpty() && !pendingTasks.isEmpty()) {
                if (taskManager != null) {
                    List<CompactStreamTask> tasks = new ArrayList<>();
                    for (PackagedCompactStreamTask task : pendingTasks.values()) {
                        try {
                            for (String subTask : task.getSubTasks()) {
                                CompactStreamTask t = null;
                                try {
                                    t = taskManager.getCompactStreamTask(subTask).get();
                                } catch (Exception e) {
                                    log.warn("Failed to get compact stream task for subtask: {}", subTask, e);
                                    continue;
                                }
                                if (t != null) {
                                    tasks.add(t);
                                }
                            }
                        } catch (Exception e) {
                            currentTasks.add(task);
                        }
                    }
                    tasks.sort(CompactStreamTask::compareTo);
                    currentTasks.addAll(tasks.stream()
                        .map(t -> pendingTasks.get(t.getTaskName()))
                        .toList());
                } else {
                    currentTasks.addAll(pendingTasks.values());
                    pendingTasks.clear();
                }
            }
        }

        long currentTime = System.currentTimeMillis();
        synchronized (quarantinedTask) {
            if (!quarantinedTask.isEmpty()) {
                QuarantinedTask entry = quarantinedTask.peek();
                if (entry != null && entry.getKey() <= currentTime) {
                    String taskName = quarantinedTask.poll().getTaskName();
                    PackagedCompactStreamTask taskInPendingTasks;
                    synchronized (pendingTasks) {
                        taskInPendingTasks = pendingTasks.remove(taskName);
                    }
                    PackagedCompactStreamTask taskInCurrentTasks = currentTasks.stream()
                        .filter(t -> t.getTaskName().equals(taskName))
                        .findFirst()
                        .orElse(null);

                    if (taskInPendingTasks != null) {
                        return taskInPendingTasks;
                    } else if (taskInCurrentTasks != null) {
                        currentTasks.remove(taskInCurrentTasks);
                        return taskInCurrentTasks;
                    }
                }
            }
        }

        while (!currentTasks.isEmpty()) {
            PackagedCompactStreamTask task = currentTasks.poll();
            if (task != null) {
                boolean isNotQuarantined;
                synchronized (quarantinedTask) {
                    isNotQuarantined = quarantinedTask.stream()
                        .noneMatch(t -> t.getTaskName().equals(task.getTaskName()));
                }
                if (isNotQuarantined) {
                    return task;
                }
            }
        }

        return null;
    }

    public void quarantineTask(long quarantineUntil, String taskName) {
        synchronized (quarantinedTask) {
            quarantinedTask.removeIf(t -> t.getTaskName().equals(taskName));
            quarantinedTask.offer(new QuarantinedTask(quarantineUntil, taskName));
            if (log.isDebugEnabled()) {
                log.debug("Quarantined task size: {}", quarantinedTask.size());
            }
        }
    }

    public void quarantineTopic(String topic, long quarantineUntil) {
        long current = System.currentTimeMillis();
        // clean up the quarantined topics which are expired
        quarantinedTopics.entrySet().removeIf(entry -> entry.getValue() < current);

        quarantinedTopics.put(topic, quarantineUntil);
        compactionMetrics.getQuarantinedTopicsCount().set(quarantinedTopics.size());
    }

    public Long getQuarantinedTopic(String topic) {
        return quarantinedTopics.get(topic);
    }

    public void removeQuarantinedTopic(String topic) {
        quarantinedTopics.remove(topic);
        compactionMetrics.getQuarantinedTopicsCount().set(quarantinedTopics.size());
    }

    public int getNumTasks() {
        return pendingTasks.isEmpty() ? currentTasks.size() : pendingTasks.size();
    }

    @Getter
    private static class QuarantinedTask implements Comparable<QuarantinedTask> {
        private final Long key;
        private final String taskName;

        public QuarantinedTask(Long key, String taskName) {
            this.key = key;
            this.taskName = taskName;
        }

        @Override
        public int compareTo(QuarantinedTask other) {
            return Long.compare(this.key, other.key);
        }
    }
}
