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
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class CompactedTaskRunner implements Runnable, StartStopRunner {

    private volatile boolean isCancel = false;
    private volatile boolean firstRound = true;
    private final Object stateLock = new Object();
    private final CommitTaskProvider commitTaskProvider;
    // Registration and snapshots are coordinated by stateLock. Futures are never cancelled, so a
    // completed future proves that its commit runnable has actually returned.
    private final Set<CompletableFuture<Void>> inFlightCommits = ConcurrentHashMap.newKeySet();
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
    private final Consumer<Throwable> fatalErrorHandler;
    private final Object fatalStopLock = new Object();
    private IllegalStateException fatalStopFailure;
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
            commitParquetFileExecutor, storageConfig, compactionMetrics, () -> true, failure -> { });
    }

    public CompactedTaskRunner(StorageApi storageApi,
                               CommitTaskProvider commitTaskProvider,
                               CompactTaskManager compactTaskManager,
                               ExecutorService compactedTaskExecutor,
                               ExecutorService commitParquetFileExecutor,
                               StorageConfig storageConfig,
                               CompactionMetrics compactionMetrics,
                               BooleanSupplier isLeader) {
        this(storageApi, commitTaskProvider, compactTaskManager, compactedTaskExecutor,
            commitParquetFileExecutor, storageConfig, compactionMetrics, isLeader, failure -> { });
    }

    CompactedTaskRunner(StorageApi storageApi,
                        CommitTaskProvider commitTaskProvider,
                        CompactTaskManager compactTaskManager,
                        ExecutorService compactedTaskExecutor,
                        ExecutorService commitParquetFileExecutor,
                        StorageConfig storageConfig,
                        CompactionMetrics compactionMetrics,
                        BooleanSupplier isLeader,
                        Consumer<Throwable> fatalErrorHandler) {
        this.isLeader = isLeader;
        this.fatalErrorHandler = Objects.requireNonNull(fatalErrorHandler, "fatalErrorHandler");
        this.storageApi = storageApi;
        this.commitTaskProvider = commitTaskProvider;
        this.compactTaskManager = compactTaskManager;
        if (compactedTaskExecutor != null
                && compactedTaskExecutor == commitParquetFileExecutor) {
            throw new IllegalArgumentException(
                    "Compacted-task orchestration and lakehouse commit work require separate executors");
        }
        this.compactedTaskExecutor = compactedTaskExecutor;
        this.commitParquetFileExecutor = commitParquetFileExecutor;
        this.storageConfig = storageConfig;
        this.compactionMetrics = compactionMetrics;
        this.replayDLQTasks = storageConfig.isReplayDLQTasksEnabled();
        this.commitTimeoutInSeconds = Integer.parseInt(
            storageConfig.getProperties().getProperty("commitTimeoutInSeconds", "1800"));
        if (commitTimeoutInSeconds <= 0) {
            throw new IllegalArgumentException("commitTimeoutInSeconds must be greater than zero");
        }
        this.commitIntervalInSeconds = storageConfig.getMaxCommitIntervalInSeconds();
        this.bannedTopics = storageConfig.getBlackTopicOfCompact()
            .stream()
            .map(CompactedTaskRunner::parseBannedTopic)
            .flatMap(Optional::stream)
            .collect(Collectors.toSet());
    }

    static Optional<String> parseBannedTopic(String configuredName) {
        try {
            return Optional.of(TopicName.get(configuredName).getPartitionedTopicName());
        } catch (RuntimeException invalidName) {
            log.warn("Ignoring invalid blackTopicOfCompact entry '{}'", configuredName, invalidName);
            return Optional.empty();
        }
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
                CompletableFuture<Void> future = dispatchCommit(partitionedTopic, topicTasks);
                if (future == null) {
                    break;
                }
                commitResults.add(future);
            }

            // Wait for all commit tasks to complete.
            // Since commit tasks will update the state in Oxia, we avoid checking states while tasks are still running.
            // During this waiting period, we also accumulate tasks so they can be combined into a single commit.
            // Therefore, making this call synchronized is acceptable here.
            try {
                CompletableFuture.allOf(commitResults.toArray(new CompletableFuture[0]))
                    .get(commitTimeoutInSeconds, TimeUnit.SECONDS);
            } catch (TimeoutException timeout) {
                failStopForUndrainedCommits(commitResults, "scheduled commit round", timeout);
            } catch (InterruptedException interrupted) {
                log.info("Compacted task orchestration interrupted while waiting for commits to drain");
                boolean drained = awaitCommitsWithinDeadline(
                        commitResults, commitTimeoutInSeconds, TimeUnit.SECONDS);
                Thread.currentThread().interrupt();
                if (!drained) {
                    failStopForUndrainedCommits(
                            commitResults, "interrupted commit round", interrupted);
                }
            } catch (ExecutionException failure) {
                log.error("Error committing compacted tasks", failure.getCause());
            } catch (Throwable e) {
                log.error("Error committing compacted tasks", e);
                if (!awaitCommitsWithinDeadline(
                        commitResults, commitTimeoutInSeconds, TimeUnit.SECONDS)) {
                    failStopForUndrainedCommits(
                            commitResults, "failed commit round", e);
                }
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

    private CompletableFuture<Void> dispatchCommit(
            String partitionedTopic, List<CompactStreamTask> topicTasks) {
        synchronized (stateLock) {
            if (shouldStop()) {
                return null;
            }
            CompletableFuture<Void> future = CompletableFuture.runAsync(() -> {
                try {
                    runTask(partitionedTopic, topicTasks);
                } catch (ExceptionWithCode e) {
                    throw new CompletionException(e);
                }
            }, commitParquetFileExecutor);
            inFlightCommits.add(future);
            future.whenComplete((ignored, failure) -> inFlightCommits.remove(future));
            return future;
        }
    }

    private static boolean awaitCommitsWithinDeadline(
            List<CompletableFuture<Void>> commits, long timeout, TimeUnit unit) {
        if (commits.isEmpty()) {
            return true;
        }
        CompletableFuture<Void> completion = CompletableFuture.allOf(
                commits.toArray(new CompletableFuture[0]));
        long timeoutNanos = Math.max(0L, unit.toNanos(timeout));
        long deadline = System.nanoTime() + timeoutNanos;
        boolean interrupted = false;
        try {
            while (true) {
                if (completion.isDone()) {
                    return true;
                }
                long remainingNanos = deadline - System.nanoTime();
                if (remainingNanos <= 0) {
                    return false;
                }
                try {
                    completion.get(remainingNanos, TimeUnit.NANOSECONDS);
                    return true;
                } catch (InterruptedException e) {
                    interrupted = true;
                } catch (ExecutionException e) {
                    // allOf only completes exceptionally after every child is terminal.
                    return true;
                } catch (TimeoutException e) {
                    return false;
                }
            }
        } finally {
            if (interrupted) {
                Thread.currentThread().interrupt();
            }
        }
    }

    private void failStopForUndrainedCommits(
            List<CompletableFuture<Void>> commits, String operation, Throwable cause) {
        synchronized (stateLock) {
            isCancel = true;
        }
        long unfinishedCommits = commits.stream().filter(future -> !future.isDone()).count();
        IllegalStateException detectedFailure = new IllegalStateException(
                String.format("%d lakehouse commit(s) did not drain within %d seconds during %s; "
                                + "the compaction process must fail-stop before its leader lease is released",
                        unfinishedCommits, commitTimeoutInSeconds, operation),
                cause);
        IllegalStateException failure;
        synchronized (fatalStopLock) {
            if (fatalStopFailure == null) {
                fatalStopFailure = detectedFailure;
                try {
                    log.error("Unsafe compaction leader handoff prevented; fail-stopping the process",
                            fatalStopFailure);
                } catch (Throwable observabilityFailure) {
                    fatalStopFailure.addSuppressed(observabilityFailure);
                }
                try {
                    compactionMetrics.getCommitDrainTimeoutCount().increment();
                } catch (Throwable observabilityFailure) {
                    fatalStopFailure.addSuppressed(observabilityFailure);
                }
                try {
                    fatalErrorHandler.accept(fatalStopFailure);
                } catch (Throwable handlerFailure) {
                    if (handlerFailure != fatalStopFailure) {
                        fatalStopFailure.addSuppressed(handlerFailure);
                    }
                }
            }
            failure = fatalStopFailure;
        }
        throw failure;
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
        synchronized (stateLock) {
            if (!isCancel && compactedTaskExecutor != null) {
                commitFileFuture = compactedTaskExecutor.submit(this);
            }
        }
    }

    @Override
    public void stop() {
        Future<?> outerFuture;
        List<CompletableFuture<Void>> commits;
        synchronized (stateLock) {
            isCancel = true;
            outerFuture = commitFileFuture;
            commits = List.copyOf(inFlightCommits);
        }
        if (outerFuture != null) {
            // Interrupt so the orchestrator does not stay blocked in allOf().get(commitTimeoutInSeconds) after
            // demotion. Child futures are deliberately not cancelled: cancellation may mark a
            // Future done while its lakehouse commit is still running, which is not a safe handoff.
            outerFuture.cancel(true);
        }
        if (!awaitCommitsWithinDeadline(commits, commitTimeoutInSeconds, TimeUnit.SECONDS)) {
            failStopForUndrainedCommits(
                    commits,
                    "leader demotion",
                    new TimeoutException("Timed out draining lakehouse commits during leader demotion"));
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
