/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.lakehouse.compact;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.lakestream.ursa.compaction.CompactTaskManager;
import io.lakestream.ursa.compaction.metrics.CompactionMetrics;
import io.lakestream.ursa.compaction.task.CompactStreamTask;
import io.lakestream.ursa.storage.StorageApi;
import io.lakestream.ursa.storage.impl.StorageConfig;
import io.lakestream.ursa.storage.impl.compaction.CommitTaskProvider;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag("lakehouse")
class CompactedTaskRunnerTest {

    @Test
    void malformedBlacklistEntryIsIgnored() {
        assertThat(CompactedTaskRunner.parseBannedTopic("bad://topic")).isEmpty();
    }

    @Test
    void blacklistEntryUsesPartitionedStreamName() {
        assertThat(CompactedTaskRunner.parseBannedTopic("public/default/orders-partition-3"))
                .contains("public/default/orders");
    }

    @Test
    void stopWaitsForStartedCommitRunnableToActuallyReturn() throws Exception {
        ExecutorService orchestrationExecutor = Executors.newSingleThreadExecutor();
        ExecutorService commitExecutor = Executors.newSingleThreadExecutor();
        CountDownLatch commitStarted = new CountDownLatch(1);
        CountDownLatch releaseCommit = new CountDownLatch(1);
        CommitTaskProvider provider = mock(CommitTaskProvider.class);
        CompactStreamTask task = new CompactStreamTask();
        task.setProperties(Map.of());
        when(provider.getTask()).thenReturn(Map.of("default/orders", List.of(task)));
        BlockingCommitRunner runner = new BlockingCommitRunner(
                provider, orchestrationExecutor, commitExecutor, commitStarted, releaseCommit);
        try {
            runner.start();
            assertTrue(commitStarted.await(5, TimeUnit.SECONDS));

            CompletableFuture<Void> stopped = CompletableFuture.runAsync(runner::stop);
            assertThrows(TimeoutException.class, () -> stopped.get(200, TimeUnit.MILLISECONDS));
            verify(provider, times(1)).getTask();

            releaseCommit.countDown();
            stopped.get(5, TimeUnit.SECONDS);
        } finally {
            releaseCommit.countDown();
            runner.stop();
            orchestrationExecutor.shutdownNow();
            commitExecutor.shutdownNow();
        }
    }

    @Test
    void stopFailStopsWhenStartedCommitCannotDrainByDeadline() throws Exception {
        ExecutorService orchestrationExecutor = Executors.newSingleThreadExecutor();
        ExecutorService commitExecutor = Executors.newSingleThreadExecutor();
        CountDownLatch commitStarted = new CountDownLatch(1);
        CountDownLatch releaseCommit = new CountDownLatch(1);
        AtomicReference<Throwable> fatalFailure = new AtomicReference<>();
        CommitTaskProvider provider = mock(CommitTaskProvider.class);
        CompactStreamTask task = new CompactStreamTask();
        task.setProperties(Map.of());
        when(provider.getTask()).thenReturn(Map.of("default/orders", List.of(task)));
        BlockingCommitRunner runner = new BlockingCommitRunner(
                provider,
                orchestrationExecutor,
                commitExecutor,
                commitStarted,
                releaseCommit,
                configWithCommitTimeout(1),
                fatalFailure::set);
        try {
            runner.start();
            assertTrue(commitStarted.await(5, TimeUnit.SECONDS));

            IllegalStateException timeout = assertThrows(IllegalStateException.class, runner::stop);

            assertThat(timeout).hasMessageContaining("did not drain within 1 seconds");
            assertThat(fatalFailure.get()).isSameAs(timeout);
        } finally {
            releaseCommit.countDown();
            orchestrationExecutor.shutdownNow();
            commitExecutor.shutdownNow();
        }
    }

    @Test
    void scheduledCommitTimeoutIsSupervisedInsteadOfSilentlyKillingRunner() throws Exception {
        ExecutorService orchestrationExecutor = Executors.newSingleThreadExecutor();
        ExecutorService commitExecutor = Executors.newSingleThreadExecutor();
        CountDownLatch commitStarted = new CountDownLatch(1);
        CountDownLatch releaseCommit = new CountDownLatch(1);
        CountDownLatch fatalTriggered = new CountDownLatch(1);
        AtomicReference<Throwable> fatalFailure = new AtomicReference<>();
        CommitTaskProvider provider = mock(CommitTaskProvider.class);
        CompactStreamTask task = new CompactStreamTask();
        task.setProperties(Map.of());
        when(provider.getTask()).thenReturn(Map.of("default/orders", List.of(task)));
        BlockingCommitRunner runner = new BlockingCommitRunner(
                provider,
                orchestrationExecutor,
                commitExecutor,
                commitStarted,
                releaseCommit,
                configWithCommitTimeout(1),
                failure -> {
                    fatalFailure.set(failure);
                    fatalTriggered.countDown();
                });
        try {
            runner.start();
            assertTrue(commitStarted.await(5, TimeUnit.SECONDS));

            assertTrue(fatalTriggered.await(5, TimeUnit.SECONDS));
            assertThat(fatalFailure.get())
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("scheduled commit round");
            verify(provider, times(1)).getTask();
        } finally {
            releaseCommit.countDown();
            runner.stop();
            orchestrationExecutor.shutdownNow();
            commitExecutor.shutdownNow();
        }
    }

    @Test
    void orchestrationAndCommitWorkRequireSeparateExecutors() {
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            assertThrows(IllegalArgumentException.class, () -> new CompactedTaskRunner(
                    mock(StorageApi.class),
                    mock(CommitTaskProvider.class),
                    mock(CompactTaskManager.class),
                    executor,
                    executor,
                    new StorageConfig(),
                    CompactionMetrics.NOOP));
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    void commitTimeoutMustProvideAPositiveSafetyDeadline() {
        ExecutorService orchestrationExecutor = Executors.newSingleThreadExecutor();
        ExecutorService commitExecutor = Executors.newSingleThreadExecutor();
        try {
            assertThrows(IllegalArgumentException.class, () -> new CompactedTaskRunner(
                    mock(StorageApi.class),
                    mock(CommitTaskProvider.class),
                    mock(CompactTaskManager.class),
                    orchestrationExecutor,
                    commitExecutor,
                    configWithCommitTimeout(0),
                    CompactionMetrics.NOOP));
        } finally {
            orchestrationExecutor.shutdownNow();
            commitExecutor.shutdownNow();
        }
    }

    private static StorageConfig configWithCommitTimeout(int seconds) {
        Properties properties = new Properties();
        properties.setProperty("commitTimeoutInSeconds", Integer.toString(seconds));
        return StorageConfig.fromProperties(properties);
    }

    private static final class BlockingCommitRunner extends CompactedTaskRunner {
        private final CountDownLatch commitStarted;
        private final CountDownLatch releaseCommit;

        private BlockingCommitRunner(
                CommitTaskProvider provider,
                ExecutorService orchestrationExecutor,
                ExecutorService commitExecutor,
                CountDownLatch commitStarted,
                CountDownLatch releaseCommit) {
            this(
                    provider,
                    orchestrationExecutor,
                    commitExecutor,
                    commitStarted,
                    releaseCommit,
                    new StorageConfig(),
                    failure -> { });
        }

        private BlockingCommitRunner(
                CommitTaskProvider provider,
                ExecutorService orchestrationExecutor,
                ExecutorService commitExecutor,
                CountDownLatch commitStarted,
                CountDownLatch releaseCommit,
                StorageConfig config,
                Consumer<Throwable> fatalErrorHandler) {
            super(
                    mock(StorageApi.class),
                    provider,
                    mock(CompactTaskManager.class),
                    orchestrationExecutor,
                    commitExecutor,
                    config,
                    CompactionMetrics.NOOP,
                    () -> true,
                    fatalErrorHandler);
            this.commitStarted = commitStarted;
            this.releaseCommit = releaseCommit;
        }

        @Override
        void commit(String partitionedTopicName, List<CompactStreamTask> tasks) {
            commitStarted.countDown();
            boolean interrupted = false;
            while (true) {
                try {
                    releaseCommit.await();
                    break;
                } catch (InterruptedException e) {
                    interrupted = true;
                }
            }
            if (interrupted) {
                Thread.currentThread().interrupt();
            }
        }
    }
}
