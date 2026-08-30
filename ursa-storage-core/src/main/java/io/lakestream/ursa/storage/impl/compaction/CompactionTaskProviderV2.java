/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.storage.impl.compaction;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.RateLimiter;
import io.lakestream.ursa.compaction.CompactTaskManager;
import io.lakestream.ursa.compaction.metrics.CompactionMetrics;
import io.lakestream.ursa.compaction.task.CompactStreamTask;
import io.lakestream.ursa.compaction.task.PackagedCompactStreamTask;
import io.lakestream.ursa.storage.impl.StorageConfig;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class CompactionTaskProviderV2 {

    private final long minPrintLogIntervalInMillis = Duration.ofMinutes(1).toMillis();
    private final Queue<PackagedCompactStreamTask> currentTasks = new ConcurrentLinkedQueue<>();
    private final PriorityQueue<QuarantinedTask> quarantinedTask =
        new PriorityQueue<>(Comparator.comparingLong(QuarantinedTask::getKey));
    @VisibleForTesting
    @Getter
    private final Map<String, Long> quarantinedTopics = new ConcurrentHashMap<>();
    private final CompactTaskManager taskManager;
    private final CompactionMetrics compactionMetrics;
    private final long minFetchIntervalMs;

    private volatile long lastFetchTimestamp = 0;
    private volatile long lastFetchEmptyTimestamp = 0;
    private final Object fetchLock = new Object();
    private final RateLimiter fetchRateLimiter;

    public CompactionTaskProviderV2(StorageConfig config,
                                  CompactTaskManager taskManager,
                                  CompactionMetrics compactionMetrics) {
        this.taskManager = taskManager;
        this.compactionMetrics = compactionMetrics;
        this.minFetchIntervalMs = Duration.ofSeconds(config.getRefreshLocalTaskIntervalInSeconds()).toMillis();
        this.fetchRateLimiter = RateLimiter.create(config.getMetastoreRequestRateLimitPerSecond());
    }

    @VisibleForTesting
    CompactionTaskProviderV2(CompactTaskManager taskManager,
                             CompactionMetrics compactionMetrics,
                             long minFetchIntervalMs,
                             double rateLimit) {
        this.taskManager = taskManager;
        this.compactionMetrics = compactionMetrics;
        this.minFetchIntervalMs = minFetchIntervalMs;
        this.fetchRateLimiter = RateLimiter.create(rateLimit);
    }

    public PackagedCompactStreamTask getTask() {
        // Check and release quarantined tasks first
        releaseQuarantinedTasks();

        // Try to get a task from current queue
        PackagedCompactStreamTask task = pollNonQuarantinedTask();
        if (task != null) {
            return task;
        }

        // If no tasks available, try to fetch new tasks
        fetchTasksIfNeeded();

        // Try again after potential fetch
        return pollNonQuarantinedTask();
    }

    private void releaseQuarantinedTasks() {
        long currentTime = System.currentTimeMillis();
        synchronized (quarantinedTask) {
            while (!quarantinedTask.isEmpty()) {
                QuarantinedTask entry = quarantinedTask.peek();
                if (entry != null && entry.getKey() <= currentTime) {
                    String taskName = quarantinedTask.poll().getTaskName();
                    // Add the released task back to the queue if it's still valid
                    // Note: The task should already be in the queue, just quarantined
                    if (log.isDebugEnabled()) {
                        log.debug("Released quarantined task: {}", taskName);
                    }
                } else {
                    break;
                }
            }
        }
    }

    private PackagedCompactStreamTask pollNonQuarantinedTask() {
        while (!currentTasks.isEmpty()) {
            PackagedCompactStreamTask task = currentTasks.poll();
            if (task != null && !isTaskQuarantined(task.getTaskName())) {
                return task;
            }
        }
        return null;
    }

    private boolean isTaskQuarantined(String taskName) {
        synchronized (quarantinedTask) {
            return quarantinedTask.stream()
                .anyMatch(t -> t.getTaskName().equals(taskName));
        }
    }

    private void fetchTasksIfNeeded() {
        long currentTime = System.currentTimeMillis();

        // Check if we need to fetch (queue is empty and enough time has passed)
        if (shouldSkipFetch(currentTime)) {
            return;
        }

        synchronized (fetchLock) {
            // Double-check after acquiring lock
            if (shouldSkipFetch(System.currentTimeMillis())) {
                return;
            }

            try {
                List<PackagedCompactStreamTask> newTasks = fetchAndSortTasks();
                if (!newTasks.isEmpty()) {
                    currentTasks.addAll(newTasks);
                    compactionMetrics.getOngoingCompactionTaskCount().set(newTasks.size());
                    log.info("Fetched {} packaged compaction tasks", newTasks.size());
                } else if (System.currentTimeMillis() - lastFetchEmptyTimestamp > minPrintLogIntervalInMillis) {
                    log.info("Fetched {} packaged compaction tasks", newTasks.size());
                    lastFetchEmptyTimestamp = System.currentTimeMillis();
                }
                lastFetchTimestamp = System.currentTimeMillis();
            } catch (Exception e) {
                log.error("Failed to fetch compaction tasks", e);
            }
        }
    }

    private boolean shouldSkipFetch(long currentTime) {
        return !currentTasks.isEmpty()
            || (currentTime - lastFetchTimestamp) < minFetchIntervalMs;
    }

    private List<PackagedCompactStreamTask> fetchAndSortTasks() throws Exception {
        List<PackagedCompactStreamTask> allTasks = taskManager.getAllTasks().get();

        if (allTasks.isEmpty()) {
            return allTasks;
        }

        // A package marker is written only after all of its subtasks are durable. Therefore a
        // marker with no subtask is an orphan (for example after a terminal source task is
        // deleted). Remove it here so it cannot be quarantined and rediscovered forever.
        List<CompletableFuture<Boolean>> orphanDeletes = allTasks.stream()
            .filter(task -> task.getSubTasks().isEmpty())
            .map(task -> taskManager.deletePackagedTaskNameIfEmpty(task.getTaskName()))
            .toList();
        if (!orphanDeletes.isEmpty()) {
            CompletableFuture.allOf(orphanDeletes.toArray(new CompletableFuture[0])).get();
            allTasks = allTasks.stream()
                .filter(task -> !task.getSubTasks().isEmpty())
                .toList();
            if (allTasks.isEmpty()) {
                return List.of();
            }
        }

        // Collect all sub-task futures asynchronously
        List<CompletableFuture<CompactStreamTask>> futures = new ArrayList<>();
        for (PackagedCompactStreamTask task : allTasks) {
            for (String subTask : task.getSubTasks()) {
                fetchRateLimiter.acquire();
                CompletableFuture<CompactStreamTask> future =
                    taskManager.getCompactStreamTask(subTask)
                        .exceptionally(e -> {
                            log.warn("Failed to get compact stream task for subtask: {}", subTask, e);
                            return null;
                        });
                futures.add(future);
            }
        }

        // Wait for all futures to complete
        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).get();

        // Collect non-null results
        List<CompactStreamTask> sortedSubTasks = futures.stream()
            .map(CompletableFuture::join)
            .filter(Objects::nonNull)
            .sorted(CompactStreamTask::compareTo)
            .toList();

        if (sortedSubTasks.isEmpty()) {
            return allTasks;
        }

        // Build result list
        Map<String, PackagedCompactStreamTask> taskMap = new java.util.HashMap<>();
        allTasks.forEach(task -> taskMap.put(task.getTaskName(), task));

        return sortedSubTasks.stream()
            .map(t -> taskMap.get(t.getTaskName()))
            .distinct()
            .toList();
    }

    public void quarantineTask(long quarantineUntil, String taskName) {
        synchronized (quarantinedTask) {
            quarantinedTask.removeIf(t -> t.getTaskName().equals(taskName));
            quarantinedTask.offer(new QuarantinedTask(quarantineUntil, taskName));
            if (log.isDebugEnabled()) {
                log.debug("Quarantined task: {}, until: {}, queue size: {}",
                    taskName, quarantineUntil, quarantinedTask.size());
            }
        }
    }

    public void quarantineTopic(String topic, long quarantineUntil) {
        long current = System.currentTimeMillis();
        // Clean up expired quarantined topics
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
        return currentTasks.size();
    }

    @VisibleForTesting
    public long getLastFetchTimestamp() {
        return lastFetchTimestamp;
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
