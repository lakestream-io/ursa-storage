/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.lakehouse.compact;

import io.lakestream.ursa.compaction.CompactTaskManager;
import io.lakestream.ursa.compaction.metrics.CompactionMetrics;
import io.lakestream.ursa.compaction.task.CompactStreamTask;
import io.lakestream.ursa.exception.ExceptionWithCode;
import io.lakestream.ursa.lakehouse.LakehouseConfiguration;
import io.lakestream.ursa.lakehouse.utils.TopicName;
import io.lakestream.ursa.storage.StorageApi;
import io.lakestream.ursa.storage.impl.StorageConfig;
import io.lakestream.ursa.storage.impl.compaction.CommitTaskProvider;
import io.lakestream.ursa.storage.impl.compaction.StartStopRunner;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class CompactedTaskRunner implements Runnable, StartStopRunner {

    private volatile boolean isCancel = false;
    private volatile boolean firstRound = true;
    private final CommitTaskProvider commitTaskProvider;
    private Future<?> commitFileFuture;
    private final ExecutorService compactedTaskExecutor;
    private final ExecutorService commitParquetFileExecutor;
    private final StorageApi storageApi;
    private final CompactTaskManager compactTaskManager;
    private final StorageConfig storageConfig;
    private final CompactionMetrics compactionMetrics;
    // Leadership gate: commits only proceed while this returns true. A demoted leader stops committing promptly
    // instead of draining in-flight work while the new leader commits the same tasks. In-memory check, no Oxia load.
    private final BooleanSupplier isLeader;
    private final boolean replayDLQTasks;
    private final int commitTimeoutInSeconds;
    private final int commitIntervalInSeconds;
    private Set<String> dlqTopics = new ConcurrentSkipListSet<>();

    private final Set<String> bannedTopics;

    public CompactedTaskRunner(StorageApi storageApi,
                               CommitTaskProvider commitTaskProvider,
                               CompactTaskManager compactTaskManager,
                               ExecutorService compactedTaskExecutor,
                               ExecutorService commitParquetFileExecutor,
                               StorageConfig storageConfig,
                               CompactionMetrics compactionMetrics) {
        // Default to always-leader for manual/admin and test usage that is not leadership-gated.
        this(storageApi, commitTaskProvider, compactTaskManager, compactedTaskExecutor,
            commitParquetFileExecutor, storageConfig, compactionMetrics, () -> true);
    }

    public CompactedTaskRunner(StorageApi storageApi,
                               CommitTaskProvider commitTaskProvider,
                               CompactTaskManager compactTaskManager,
                               ExecutorService compactedTaskExecutor,
                               ExecutorService commitParquetFileExecutor,
                               StorageConfig storageConfig,
                               CompactionMetrics compactionMetrics,
                               BooleanSupplier isLeader) {
        this.isLeader = isLeader;
        this.storageApi = storageApi;
        this.commitTaskProvider = commitTaskProvider;
        this.compactTaskManager = compactTaskManager;
        this.compactedTaskExecutor = compactedTaskExecutor;
        this.commitParquetFileExecutor = commitParquetFileExecutor;
        this.storageConfig = storageConfig;
        this.compactionMetrics = compactionMetrics;
        this.replayDLQTasks = storageConfig.isReplayDLQTasksEnabled();
        this.commitTimeoutInSeconds = Integer.parseInt(
            storageConfig.getProperties().getProperty("commitTimeoutInSeconds", "1800"));
        this.commitIntervalInSeconds = storageConfig.getMaxCommitIntervalInSeconds();
        this.bannedTopics = storageConfig.getBlackTopicOfCompact()
            .stream()
            .map(t -> TopicName.get(t).getPartitionedTopicName())
            .collect(Collectors.toSet());
    }

    @Override
    public void run() {
        if (shouldStop()) {
            return;
        }
        long start = System.currentTimeMillis();
        boolean replayDLQRound = firstRound && replayDLQTasks;
        try {
            var tasks = replayDLQRound ? commitTaskProvider.getDLQTask() : commitTaskProvider.getTask();
            if (tasks.isEmpty()) {
                return;
            }
            List<CompletableFuture<Void>> commitResults = new ArrayList<>();
            for (Map.Entry<String, List<CompactStreamTask>> entry : tasks.entrySet()) {
                if (shouldStop()) {
                    log.info("Stop dispatching commits: no longer the leader or runner cancelled");
                    break;
                }
                String partitionedTopic = entry.getKey();
                if (bannedTopics.contains(partitionedTopic)) {
                    log.info("Skipped commit the topic {} because it is in the banned list", partitionedTopic);
                    continue;
                }
                var topicTasks = entry.getValue();
                var future = CompletableFuture.runAsync(() -> {
                    try {
                        runTask(partitionedTopic, topicTasks);
                    } catch (ExceptionWithCode e) {
                        throw new CompletionException(e);
                    }
                }, commitParquetFileExecutor);
                commitResults.add(future);
            }

            // Wait for all commit tasks to complete.
            // Since commit tasks will update the state in Oxia, we avoid checking states while tasks are still running.
            // During this waiting period, we also accumulate tasks so they can be combined into a single commit.
            // Therefore, making this call synchronized is acceptable here.
            try {
                CompletableFuture.allOf(commitResults.toArray(new CompletableFuture[0]))
                    .get(commitTimeoutInSeconds, TimeUnit.SECONDS);
            } catch (Throwable e) {
                log.error("Error committing compacted tasks", e);
            } finally {
                int totalCommitRuns = commitResults.size();
                long successfulCommits = commitResults.stream()
                    .filter(CompletableFuture::isDone)
                    .filter(future -> !future.isCompletedExceptionally())
                    .count();
                long elapsedTime = Duration.ofMillis(System.currentTimeMillis() - start).toSeconds();
                log.info("Commit completed: {}/{} successful commits in {} seconds",
                    successfulCommits, totalCommitRuns, elapsedTime);
            }
        } catch (Throwable e) {
            log.error("Unexpected Error", e);
        } finally {
            if (firstRound) {
                firstRound = false;
            }

            // We don't want to commit the tasks too frequently, so we wait for a while if there are a few tasks need
            // to commit. This also impacted the visibility of the files to the lakehouse table. So if you have
            // a requirement for the visibility, you can turn the `commitIntervalInSeconds` to a proper value.
            long elapsedTime = Duration.ofMillis(System.currentTimeMillis() - start).toSeconds();
            if (!replayDLQRound && elapsedTime < commitIntervalInSeconds) {
                try {
                    TimeUnit.SECONDS.sleep(commitIntervalInSeconds - elapsedTime);
                } catch (InterruptedException e) {
                    log.warn("CompactedTaskRunner interrupted while sleeping", e);
                    Thread.currentThread().interrupt();
                }
            }
            start();
        }
    }

    public void runTask(String partitionedTopicName, List<CompactStreamTask> tasks) throws ExceptionWithCode {
        if (tasks.isEmpty()) {
            // return if no tasks
            return;
        }

        if (shouldStop()) {
            // Leadership may have changed since this task was dispatched. Do not start committing a topic we no
            // longer own — the current leader will commit it. Prevents two pods committing the same task.
            log.info("Skip committing topic {}: no longer the leader or runner cancelled", partitionedTopicName);
            return;
        }

        if (dlqTopics.contains(partitionedTopicName)) {
            compactTaskManager.moveTaskToDLQ(tasks);
            return;
        }

        int startIdx = 0;
        Map<String, String> lastProperties = tasks.get(0).getProperties();

        for (int i = 1; i < tasks.size(); i++) {
            var taskProperties = tasks.get(i).getProperties();
            if (!Objects.equals(taskProperties, lastProperties)) {
                if (shouldStop()) {
                    return;
                }
                commit(partitionedTopicName, tasks.subList(startIdx, i));
                startIdx = i;
                lastProperties = taskProperties;
            }
        }

        if (startIdx < tasks.size()) {
            if (shouldStop()) {
                return;
            }
            commit(partitionedTopicName, tasks.subList(startIdx, tasks.size()));
        }
    }

    void commit(String partitionedTopicName, List<CompactStreamTask> tasks) throws ExceptionWithCode {
        StorageConfig config = storageConfig.withOverrides(tasks.get(0).getProperties());
        var runner = createCommitRunner(config, partitionedTopicName);
        try {
            runner.commit(tasks);
            if (runner.needToPublishToDLQ()) {
                compactTaskManager.moveTaskToDLQ(tasks);
                dlqTopics.add(partitionedTopicName);
            }
        } catch (ExceptionWithCode e) {
            if (runner.needToPublishToDLQ()) {
                compactTaskManager.moveTaskToDLQ(tasks);
                dlqTopics.add(partitionedTopicName);
            }
            throw e;
        } finally {
            runner.close();
        }
    }

    private CommitRunner createCommitRunner(StorageConfig config, String parentTopic) {
        var isManagedMode = getStreamTableMode(config) == LakehouseConfiguration.StreamTableMode.MANAGED;
        if (isManagedMode) {
            return new AppendCommitParquetFileRunner(
                storageApi,
                compactTaskManager,
                config,
                parentTopic,
                compactionMetrics
            );
        } else {
            return new UpsertCommitFileRunner(
                storageApi,
                compactTaskManager,
                config,
                parentTopic,
                compactionMetrics
            );
        }
    }

    @Override
    public void start() {
        if (!isCancel && compactedTaskExecutor != null) {
            commitFileFuture = compactedTaskExecutor.submit(this);
        }
    }

    @Override
    public void stop() {
        isCancel = true;
        if (commitFileFuture != null) {
            // Interrupt so the orchestrator does not stay blocked in allOf().get(commitTimeoutInSeconds) after
            // demotion. The dispatched commit tasks cooperatively bail via shouldStop() before each commit.
            commitFileFuture.cancel(true);
        }
    }

    // True when this runner must stop committing: explicitly cancelled, or this node is no longer the leader.
    private boolean shouldStop() {
        return isCancel || !isLeader.getAsBoolean();
    }

    public LakehouseConfiguration.StreamTableMode getStreamTableMode(StorageConfig config) {
        return LakehouseConfiguration.StreamTableMode.valueOf(config.getStreamTableMode().toUpperCase(Locale.ROOT));
    }
}
