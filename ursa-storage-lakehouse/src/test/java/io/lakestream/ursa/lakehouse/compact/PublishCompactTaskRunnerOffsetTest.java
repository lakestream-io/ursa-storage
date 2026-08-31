/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.lakehouse.compact;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.lakestream.api.EntryHeader;
import io.lakestream.api.LifecycleState;
import io.lakestream.api.Log;
import io.lakestream.api.LogId;
import io.lakestream.api.LogOffset;
import io.lakestream.api.Namespace;
import io.lakestream.api.Stream;
import io.lakestream.api.StreamCatalog;
import io.lakestream.api.StreamIdentifier;
import io.lakestream.api.StreamLayout;
import io.lakestream.ursa.compaction.CompactionManager;
import io.lakestream.ursa.compaction.LegacyPublishedOffsetException;
import io.lakestream.ursa.compaction.metrics.CompactionMetrics;
import io.lakestream.ursa.compaction.task.CompactStreamTask;
import io.lakestream.ursa.compaction.task.PreparedCompactStreamTask;
import io.lakestream.ursa.metrics.Counter;
import io.lakestream.ursa.storage.impl.StorageConfig;
import io.lakestream.ursa.storage.impl.compaction.MemoryCompactTaskManager;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.metrics.LongGauge;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Delayed;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongSupplier;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

class PublishCompactTaskRunnerOffsetTest {

    private static final String NAMESPACE = "default";
    private final List<ExecutorService> executors = new ArrayList<>();

    @AfterEach
    void shutdownExecutors() {
        executors.forEach(ExecutorService::shutdownNow);
    }

    @Test
    void discoversNativeAndExternalPartitionsAndPreservesProperties() throws Exception {
        MutableCatalog catalog = new MutableCatalog();
        catalog.add(stream("native", Map.of("custom", "native"),
                partition(11L, new LogOffset(0L, 3, 1L, 30, 30L))));
        catalog.add(stream("orders-topic-id-uuid", Map.of(
                        "entryFormat", "URSA",
                        "sourceSchemaTopic", "orders",
                        "sdt.enabled", "false"),
                partition(21L, new LogOffset(0L, 2, 1L, 20, 20L)),
                partition(22L, new LogOffset(0L, 4, 1L, 40, 40L))));

        Properties base = new Properties();
        base.setProperty("dataSourceForCompaction", "KAFKA");
        base.setProperty("clusterSdtEnabled", "true");
        MemoryCompactTaskManager tasks = new MemoryCompactTaskManager();
        PublishCompactTaskRunner runner = runner(catalog.catalog(), tasks, base);

        runner.scanCatalogOnce();

        List<CompactStreamTask> published = tasks(tasks);
        assertThat(published).extracting(CompactStreamTask::getTopic)
                .containsExactlyInAnyOrder(
                        "default/native-partition-0",
                        "default/orders-topic-id-uuid-partition-0",
                        "default/orders-topic-id-uuid-partition-1");
        assertThat(published).extracting(CompactStreamTask::getStreamId)
                .containsExactlyInAnyOrder(11L, 21L, 22L);

        CompactStreamTask nativeTask = taskFor(published, 11L);
        assertThat(nativeTask.getStartOffset()).isZero();
        assertThat(nativeTask.getEndOffset()).isEqualTo(3L);
        assertThat(nativeTask.getProperties())
                .containsEntry("custom", "native")
                .containsEntry("entryFormat", "KAFKA")
                .containsEntry("sdt.enabled", "true");

        CompactStreamTask externalTask = taskFor(published, 21L);
        assertThat(externalTask.getProperties())
                .containsEntry("entryFormat", "URSA")
                .containsEntry("sourceSchemaTopic", "orders")
                .containsEntry("sdt.enabled", "false");
    }

    @Test
    void publishesContinuousEndExclusiveRangesAcrossScansAndRestart() throws Exception {
        MutableCatalog catalog = new MutableCatalog();
        MutablePartition partition = partition(42L, new LogOffset(0L, 10, 1L, 100, 100L));
        partition.metadata(9L, new EntryHeader(0L, 10, 1L, 100, 100L));
        catalog.add(stream("events", Map.of(), partition));
        ReleaseTrackingMemoryCompactTaskManager tasks =
                new ReleaseTrackingMemoryCompactTaskManager(1);

        PublishCompactTaskRunner first = runner(catalog.catalog(), tasks, new Properties());
        first.scanCatalogOnce();
        assertThat(tasks.getPublishedOffset("default/events-partition-0").getOffset()).isEqualTo(9L);
        assertThat(tasks.getPublishedOffset("default/events-partition-0").getCumulativeSize())
                .isEqualTo(100L);
        first.stop();
        assertThat(tasks.awaitExpectedReleases()).isTrue();
        assertThat(tasks.getPublishedOffset("default/events-partition-0").getOffset()).isEqualTo(9L);
        partition.removeMetadata(9L);

        PublishCompactTaskRunner restarted = runner(catalog.catalog(), tasks, new Properties());
        restarted.scanCatalogOnce();
        assertThat(tasks(tasks)).hasSize(1);

        partition.lastOffset(new LogOffset(10L, 5, 2L, 50, 150L));
        restarted.scanCatalogOnce();

        List<CompactStreamTask> published = tasks(tasks).stream()
                .sorted(Comparator.comparingLong(CompactStreamTask::getStartOffset))
                .toList();
        assertThat(published).extracting(CompactStreamTask::getStartOffset)
                .containsExactly(0L, 10L);
        assertThat(published).extracting(CompactStreamTask::getEndOffset)
                .containsExactly(10L, 15L);
        assertThat(published.get(1).getTotalSize()).isEqualTo(50L);
        assertThat(published.get(1).getCumulativeSize()).isEqualTo(150L);
        assertThat(published.get(1).getTotalSize()).isEqualTo(
                published.get(1).getCumulativeSize() - published.get(0).getCumulativeSize());
        restarted.stop();
    }

    @Test
    void replacingPhysicalLogResetsCursorForNewStreamIncarnation() throws Exception {
        MutableCatalog catalog = new MutableCatalog();
        MutableStream stream = stream("recreated", Map.of(),
                partition(71L, new LogOffset(0L, 5, 1L, 50, 50L)));
        catalog.add(stream);
        MemoryCompactTaskManager tasks = new MemoryCompactTaskManager();
        PublishCompactTaskRunner runner = runner(catalog.catalog(), tasks, new Properties());

        runner.scanCatalogOnce();
        stream.partitions(List.of(partition(72L, new LogOffset(0L, 7, 2L, 70, 70L))));
        runner.scanCatalogOnce();

        List<CompactStreamTask> published = tasks(tasks);
        assertThat(published).extracting(CompactStreamTask::getStreamId)
                .containsExactlyInAnyOrder(71L, 72L);
        CompactStreamTask recreated = taskFor(published, 72L);
        assertThat(recreated.getStartOffset()).isZero();
        assertThat(recreated.getEndOffset()).isEqualTo(7L);
        assertThat(tasks.getPublishedOffset("default/recreated-partition-0").getId()).isEqualTo(72L);
        assertThat(runner.sessionCount()).isOne();
    }

    @Test
    void deletedStreamAndStopFenceSessionsBeforeReleasingThem() throws Exception {
        MutableCatalog catalog = new MutableCatalog();
        catalog.add(stream("deleted", Map.of(),
                partition(81L, new LogOffset(0L, 1, 1L, 10, 10L))));
        ReleaseTrackingMemoryCompactTaskManager tasks =
                new ReleaseTrackingMemoryCompactTaskManager(2);
        PublishCompactTaskRunner runner = runner(catalog.catalog(), tasks, new Properties());

        runner.scanCatalogOnce();
        assertThat(runner.sessionCount()).isOne();
        catalog.clear();
        runner.scanCatalogOnce();
        assertThat(runner.sessionCount()).isZero();

        catalog.add(stream("deleted", Map.of(),
                partition(82L, new LogOffset(0L, 1, 1L, 10, 10L))));
        runner.scanCatalogOnce();
        runner.stop();
        assertThat(runner.sessionCount()).isZero();
        assertThat(tasks.awaitExpectedReleases()).isTrue();

        // A successor proves that stop released the exact fenced lease and reset the incarnation cursor.
        PublishCompactTaskRunner successor = runner(catalog.catalog(), tasks, new Properties());
        successor.scanCatalogOnce();
        assertThat(successor.sessionCount()).isOne();
        assertThat(tasks.getPublishedOffset("default/deleted-partition-0").getId()).isEqualTo(82L);
    }

    @Test
    void preservesFileSizeAndTailVisibilityBatching() throws Exception {
        long now = System.currentTimeMillis();
        MutableCatalog catalog = new MutableCatalog();
        MutablePartition partition = partition(91L, new LogOffset(4L, 2, now, 10, 60L));
        partition.metadata(1L, new EntryHeader(0L, 2, now, 20, 20L));
        partition.metadata(3L, new EntryHeader(2L, 2, now, 30, 50L));
        partition.metadata(4L, new EntryHeader(4L, 2, now, 10, 60L));
        partition.metadata(5L, new EntryHeader(4L, 2, now, 10, 60L));
        catalog.add(stream("batched", Map.of(), partition));

        StorageConfig config = new StorageConfig();
        config.setCheckCompactMessageStepLength(2);
        config.setCompactedFileSizeLimit(50L);
        config.setTailCompactDataVisibilityIntervalInSeconds((int) TimeUnit.HOURS.toSeconds(1));
        MemoryCompactTaskManager tasks = new MemoryCompactTaskManager();
        PublishCompactTaskRunner runner = runner(catalog.catalog(), tasks, config);

        runner.scanCatalogOnce();
        List<CompactStreamTask> firstBatch = tasks(tasks);
        assertThat(firstBatch).hasSize(1);
        assertThat(firstBatch.get(0).getStartOffset()).isZero();
        assertThat(firstBatch.get(0).getEndOffset()).isEqualTo(4L);
        assertThat(firstBatch.get(0).getTotalSize()).isEqualTo(50L);

        runner.scanCatalogOnce();
        assertThat(tasks(tasks)).hasSize(1);

        long oldTimestamp = now - TimeUnit.HOURS.toMillis(2);
        partition.lastOffset(new LogOffset(4L, 2, oldTimestamp, 10, 60L));
        partition.metadata(4L, new EntryHeader(4L, 2, oldTimestamp, 10, 60L));
        partition.metadata(5L, new EntryHeader(4L, 2, oldTimestamp, 10, 60L));
        runner.scanCatalogOnce();

        List<CompactStreamTask> allBatches = tasks(tasks).stream()
                .sorted(Comparator.comparingLong(CompactStreamTask::getStartOffset))
                .toList();
        assertThat(allBatches).extracting(CompactStreamTask::getStartOffset)
                .containsExactly(0L, 4L);
        assertThat(allBatches).extracting(CompactStreamTask::getEndOffset)
                .containsExactly(4L, 6L);
        runner.stop();
    }

    @Test
    void drainsMultipleReadyBatchesWithoutRepeatingCatalogDiscovery() throws Exception {
        long oldTimestamp = System.currentTimeMillis() - TimeUnit.HOURS.toMillis(2);
        MutableCatalog catalog = new MutableCatalog();
        MutablePartition partition = partition(92L, new LogOffset(4L, 2, oldTimestamp, 10, 60L));
        partition.metadata(1L, new EntryHeader(0L, 2, oldTimestamp, 20, 20L));
        partition.metadata(3L, new EntryHeader(2L, 2, oldTimestamp, 30, 50L));
        partition.metadata(4L, new EntryHeader(4L, 2, oldTimestamp, 10, 60L));
        partition.metadata(5L, new EntryHeader(4L, 2, oldTimestamp, 10, 60L));
        catalog.add(stream("drain", Map.of(), partition));
        StorageConfig config = new StorageConfig();
        config.setCheckCompactMessageStepLength(2);
        config.setCompactedFileSizeLimit(50L);
        config.setTailCompactDataVisibilityIntervalInSeconds(0);
        config.setPublishThreadPendingTasks(1);
        MemoryCompactTaskManager tasks = new MemoryCompactTaskManager();
        PublishCompactTaskRunner runner = runner(catalog.catalog(), tasks, config);

        runner.run();

        List<CompactStreamTask> published = tasks(tasks).stream()
                .sorted(Comparator.comparingLong(CompactStreamTask::getStartOffset))
                .toList();
        assertThat(published).extracting(CompactStreamTask::getStartOffset)
                .containsExactly(0L, 4L);
        assertThat(published).extracting(CompactStreamTask::getEndOffset)
                .containsExactly(4L, 6L);
        assertThat(catalog.listNamespacesCalls()).isOne();
        runner.stop();
    }

    @Test
    void invalidExcludedStreamDoesNotPreventCatalogPublication() throws Exception {
        MutableCatalog catalog = new MutableCatalog();
        catalog.add(stream("included", Map.of(),
                partition(111L, new LogOffset(0L, 1, 1L, 10, 10L))));
        StorageConfig config = new StorageConfig();
        config.setBlackTopicOfCompact(Set.of("invalid://topic"));
        MemoryCompactTaskManager tasks = new MemoryCompactTaskManager();

        PublishCompactTaskRunner runner = runner(catalog.catalog(), tasks, config);
        runner.scanCatalogOnce();

        assertThat(tasks(tasks)).extracting(CompactStreamTask::getStreamId).containsExactly(111L);
        runner.stop();
    }

    @Test
    void invalidExcludedStreamDoesNotDisableValidExclusions() throws Exception {
        MutableCatalog catalog = new MutableCatalog();
        catalog.add(stream("excluded-valid", Map.of(),
                partition(127L, new LogOffset(0L, 1, 1L, 10, 10L))));
        catalog.add(stream("included-valid", Map.of(),
                partition(128L, new LogOffset(0L, 1, 1L, 10, 10L))));
        StorageConfig config = new StorageConfig();
        config.setBlackTopicOfCompact(Set.of(
                "invalid://topic", "default/excluded-valid-partition-0"));
        MemoryCompactTaskManager tasks = new MemoryCompactTaskManager();

        PublishCompactTaskRunner runner = runner(catalog.catalog(), tasks, config);
        runner.scanCatalogOnce();

        assertThat(tasks(tasks)).extracting(CompactStreamTask::getStreamId)
                .containsExactly(128L);
        runner.stop();
    }

    @Test
    void reportsLeaseContentionOncePerUnavailableTransition() throws Exception {
        MutableCatalog catalog = new MutableCatalog();
        catalog.add(stream("contended", Map.of(),
                partition(112L, new LogOffset(0L, 1, 1L, 10, 10L))));
        ReleaseTrackingMemoryCompactTaskManager tasks =
                new ReleaseTrackingMemoryCompactTaskManager(1);
        PublishCompactTaskRunner owner = runner(catalog.catalog(), tasks, new Properties());
        owner.scanCatalogOnce();

        CompactionMetrics metrics = mock(CompactionMetrics.class);
        Counter unavailable = mock(Counter.class);
        LongGauge ongoingStreams = mock(LongGauge.class);
        when(metrics.getPublicationLeaseUnavailableCount()).thenReturn(unavailable);
        when(metrics.getOngoingCompactionTopicCount()).thenReturn(ongoingStreams);
        PublishCompactTaskRunner contender = runner(
                catalog.catalog(),
                tasks,
                new StorageConfig(),
                metrics,
                Executors.newSingleThreadScheduledExecutor());

        contender.scanCatalogOnce();
        contender.scanCatalogOnce();

        verify(unavailable, times(1)).increment(Attributes.of(
                AttributeKey.stringKey("topic"), "default/contended-partition-0"));
        verify(ongoingStreams, times(2)).set(0L);
        contender.stop();
        owner.stop();
        assertThat(tasks.awaitExpectedReleases()).isTrue();
    }

    @Test
    void ongoingTopicMetricCountsLogicalStreamsRatherThanPartitions() throws Exception {
        MutableCatalog catalog = new MutableCatalog();
        catalog.add(stream("first", Map.of(),
                partition(113L, LogOffset.NOT_FOUND),
                partition(114L, LogOffset.NOT_FOUND)));
        catalog.add(stream("second", Map.of(),
                partition(115L, LogOffset.NOT_FOUND)));
        MemoryCompactTaskManager tasks = new MemoryCompactTaskManager();
        CompactionMetrics metrics = mock(CompactionMetrics.class);
        LongGauge ongoingStreams = mock(LongGauge.class);
        when(metrics.getOngoingCompactionTopicCount()).thenReturn(ongoingStreams);
        PublishCompactTaskRunner runner = runner(
                catalog.catalog(),
                tasks,
                new StorageConfig(),
                metrics,
                Executors.newScheduledThreadPool(3));

        runner.scanCatalogOnce();

        verify(ongoingStreams).set(2L);
        assertThat(runner.sessionCount()).isEqualTo(3);
        runner.stop();
    }

    @Test
    void namespaceDiscoveryFailureReportsRetainedActivePublishers() throws Exception {
        MutableCatalog catalog = new MutableCatalog();
        catalog.add(stream("retained", Map.of(),
                partition(118L, LogOffset.NOT_FOUND)));
        MemoryCompactTaskManager tasks = new MemoryCompactTaskManager();
        CompactionMetrics metrics = mock(CompactionMetrics.class);
        LongGauge ongoingStreams = mock(LongGauge.class);
        Counter publicationFailures = mock(Counter.class);
        when(metrics.getOngoingCompactionTopicCount()).thenReturn(ongoingStreams);
        when(metrics.getPublishTaskFailedCount()).thenReturn(publicationFailures);
        PublishCompactTaskRunner runner = runner(
                catalog.catalog(),
                tasks,
                new StorageConfig(),
                metrics,
                Executors.newSingleThreadScheduledExecutor());
        runner.scanCatalogOnce();
        catalog.failNamespaceListing();

        runner.run();

        verify(ongoingStreams, times(2)).set(1L);
        verify(publicationFailures).increment();
        runner.stop();
    }

    @Test
    void skipsUnallocatedLayoutPlaceholders() throws Exception {
        MutableCatalog catalog = new MutableCatalog();
        catalog.add(stream("growing", Map.of(),
                partition(-1L, LogOffset.NOT_FOUND),
                partition(102L, new LogOffset(0L, 1, 1L, 10, 10L))));
        MemoryCompactTaskManager tasks = new MemoryCompactTaskManager();
        PublishCompactTaskRunner runner = runner(catalog.catalog(), tasks, new Properties());

        runner.scanCatalogOnce();

        assertThat(tasks(tasks)).extracting(CompactStreamTask::getStreamId).containsExactly(102L);
        assertThat(tasks(tasks)).extracting(CompactStreamTask::getTopic)
                .containsExactly("default/growing-partition-1");
        assertThat(runner.sessionCount()).isOne();
        runner.stop();
    }

    @Test
    void excludesConfiguredNamespacesFromCatalogDiscovery() throws Exception {
        MutableCatalog catalog = new MutableCatalog();
        catalog.add(stream("excluded", Map.of(),
                partition(107L, new LogOffset(0L, 1, 1L, 10, 10L))));
        StorageConfig config = new StorageConfig();
        config.setBlackNamespaceOfCompact(Set.of(NAMESPACE));
        MemoryCompactTaskManager tasks = new MemoryCompactTaskManager();
        PublishCompactTaskRunner runner = runner(catalog.catalog(), tasks, config);

        runner.scanCatalogOnce();

        assertThat(tasks(tasks)).isEmpty();
        assertThat(runner.sessionCount()).isZero();
        runner.stop();
    }

    @Test
    void excludesConfiguredLogicalStreamFromCatalogDiscovery() throws Exception {
        MutableCatalog catalog = new MutableCatalog();
        catalog.add(stream("excluded", Map.of(),
                partition(108L, new LogOffset(0L, 1, 1L, 10, 10L)),
                partition(109L, new LogOffset(0L, 1, 1L, 10, 10L)),
                partition(110L, new LogOffset(0L, 1, 1L, 10, 10L))));
        StorageConfig config = new StorageConfig();
        config.setBlackTopicOfCompact(Set.of("default/excluded-partition-2"));
        MemoryCompactTaskManager tasks = new MemoryCompactTaskManager();
        PublishCompactTaskRunner runner = runner(catalog.catalog(), tasks, config);

        runner.scanCatalogOnce();

        assertThat(tasks(tasks)).isEmpty();
        assertThat(runner.sessionCount()).isZero();
        runner.stop();
    }

    @Test
    void isolatesCatalogFailureAndRetainsTheExistingSession() throws Exception {
        MutableCatalog catalog = new MutableCatalog();
        MutableStream broken = stream("broken", Map.of(),
                partition(103L, new LogOffset(0L, 1, 1L, 10, 10L)));
        MutableStream healthy = stream("healthy", Map.of(),
                partition(104L, new LogOffset(0L, 1, 1L, 10, 10L)));
        catalog.add(broken);
        catalog.add(healthy);
        catalog.fail(broken.identifier());
        MemoryCompactTaskManager tasks = new MemoryCompactTaskManager();
        PublishCompactTaskRunner runner = runner(catalog.catalog(), tasks, new Properties());

        runner.scanCatalogOnce();
        assertThat(tasks(tasks)).extracting(CompactStreamTask::getStreamId).containsExactly(104L);

        catalog.restore(broken.identifier());
        runner.scanCatalogOnce();
        assertThat(runner.sessionCount()).isEqualTo(2);
        catalog.fail(broken.identifier());
        runner.scanCatalogOnce();
        assertThat(runner.sessionCount()).isEqualTo(2);
        runner.stop();
    }

    @Test
    void unknownTimestampWaitsForTailVisibilityWindow() throws Exception {
        MutableCatalog catalog = new MutableCatalog();
        MutablePartition partition = partition(105L, new LogOffset(0L, 1, 0L, 10, 10L));
        partition.metadata(0L, new EntryHeader(0L, 1, 0L, 10, 10L));
        catalog.add(stream("unknown-time", Map.of(), partition));
        StorageConfig config = new StorageConfig();
        config.setCompactedFileSizeLimit(Long.MAX_VALUE);
        config.setTailCompactDataVisibilityIntervalInSeconds(1);
        MemoryCompactTaskManager tasks = new MemoryCompactTaskManager();
        PublishCompactTaskRunner runner = runner(catalog.catalog(), tasks, config);

        runner.scanCatalogOnce();
        assertThat(tasks(tasks)).isEmpty();
        Thread.sleep(1_100L);
        runner.scanCatalogOnce();
        assertThat(tasks(tasks)).extracting(CompactStreamTask::getStreamId).containsExactly(105L);
        runner.stop();
    }

    @Test
    void retriesTransientPublicationSessionCloseFailure() throws Exception {
        MutableCatalog catalog = new MutableCatalog();
        catalog.add(stream("close-retry", Map.of(),
                partition(106L, new LogOffset(0L, 1, 1L, 10, 10L))));
        FlakyReleaseMemoryCompactTaskManager tasks = new FlakyReleaseMemoryCompactTaskManager();
        PublishCompactTaskRunner runner = runner(catalog.catalog(), tasks, new Properties());
        runner.scanCatalogOnce();

        runner.stop();

        assertThat(tasks.releaseRetried.await(5, TimeUnit.SECONDS)).isTrue();
        PublishCompactTaskRunner successor = runner(catalog.catalog(), tasks, new Properties());
        successor.scanCatalogOnce();
        assertThat(successor.sessionCount()).isOne();
        successor.stop();
    }

    @Test
    void releasesLeaseWhenCursorClaimFailsBeforeLeadershipLoss() throws Exception {
        MutableCatalog catalog = new MutableCatalog();
        catalog.add(stream("claim-failure", Map.of(),
                partition(108L, new LogOffset(0L, 1, 1L, 10, 10L))));
        FlakyClaimMemoryCompactTaskManager tasks = new FlakyClaimMemoryCompactTaskManager();
        PublishCompactTaskRunner failedLeader = runner(catalog.catalog(), tasks, new Properties());

        failedLeader.scanCatalogOnce();
        failedLeader.stop();

        assertThat(tasks.releaseRetried.await(5, TimeUnit.SECONDS)).isTrue();
        PublishCompactTaskRunner successor = runner(catalog.catalog(), tasks, new Properties());
        successor.scanCatalogOnce();
        assertThat(successor.sessionCount()).isOne();
        successor.stop();
    }

    @Test
    void stopClosesSessionAcquiredAfterLeadershipLoss() throws Exception {
        MutableCatalog catalog = new MutableCatalog();
        catalog.add(stream("late-owner", Map.of(),
                partition(101L, new LogOffset(0L, 1, 1L, 10, 10L))));
        BlockingMemoryCompactTaskManager tasks = new BlockingMemoryCompactTaskManager();
        PublishCompactTaskRunner runner = runner(catalog.catalog(), tasks, new Properties());

        CompletableFuture<Void> scan = CompletableFuture.runAsync(() -> {
            try {
                runner.scanCatalogOnce();
            } catch (Exception error) {
                throw new CompletionException(error);
            }
        });
        assertThat(tasks.acquireStarted.await(5, TimeUnit.SECONDS)).isTrue();
        runner.stop();
        tasks.allowAcquire.countDown();
        scan.get(5, TimeUnit.SECONDS);
        assertThat(runner.sessionCount()).isZero();

        PublishCompactTaskRunner successor = runner(catalog.catalog(), tasks, new Properties());
        successor.scanCatalogOnce();
        assertThat(successor.sessionCount()).isOne();
        successor.stop();
    }

    @Test
    void opensColdStartPublicationLeasesConcurrently() throws Exception {
        MutableCatalog catalog = new MutableCatalog();
        catalog.add(stream("parallel-open", Map.of(),
                partition(116L, LogOffset.NOT_FOUND),
                partition(117L, LogOffset.NOT_FOUND)));
        ConcurrentAcquireMemoryCompactTaskManager tasks =
                new ConcurrentAcquireMemoryCompactTaskManager(2);
        PublishCompactTaskRunner runner = runner(
                catalog.catalog(),
                tasks,
                new StorageConfig(),
                CompactionMetrics.NOOP,
                Executors.newScheduledThreadPool(2));

        CompletableFuture<Void> scan = CompletableFuture.runAsync(() -> {
            try {
                runner.scanCatalogOnce();
            } catch (Exception error) {
                throw new CompletionException(error);
            }
        });

        boolean concurrentlyStarted = tasks.allAcquiresStarted.await(5, TimeUnit.SECONDS);
        tasks.allowAcquire.countDown();
        assertThat(concurrentlyStarted).isTrue();
        scan.get(5, TimeUnit.SECONDS);
        assertThat(runner.sessionCount()).isEqualTo(2);
        runner.stop();
    }

    @Test
    void fatalScanFencesEveryLeaseAndSchedulesFreshScan() throws Exception {
        MutableCatalog catalog = new MutableCatalog();
        catalog.add(stream("fatal-restart", Map.of(),
                partition(119L, LogOffset.NOT_FOUND),
                partition(120L, LogOffset.NOT_FOUND)));
        FatalValidationMemoryCompactTaskManager tasks =
                new FatalValidationMemoryCompactTaskManager(2);
        CompactionMetrics metrics = mock(CompactionMetrics.class);
        Counter publicationFailures = mock(Counter.class);
        LongGauge ongoingStreams = mock(LongGauge.class);
        when(metrics.getPublishTaskFailedCount()).thenReturn(publicationFailures);
        when(metrics.getOngoingCompactionTopicCount()).thenReturn(ongoingStreams);
        StorageConfig config = new StorageConfig();
        config.setRefreshLocalTaskIntervalInSeconds(1);
        PublishCompactTaskRunner runner = runner(
                catalog.catalog(),
                tasks,
                config,
                metrics,
                Executors.newScheduledThreadPool(2));
        runner.scanCatalogOnce();
        assertThat(runner.sessionCount()).isEqualTo(2);

        tasks.failNextValidation();
        assertThatThrownBy(runner::run)
                .isInstanceOf(AssertionError.class)
                .hasMessage("fatal publication validation failure");

        assertThat(runner.sessionCount()).isZero();
        assertThat(tasks.releaseCount()).isEqualTo(2);
        assertThat(tasks.awaitFreshScan()).isTrue();
        verify(publicationFailures).increment();
        runner.stop();
    }

    @Test
    void staleQueuedPublicationCannotAcquireAfterFatalReset() throws Exception {
        MutableCatalog catalog = new MutableCatalog();
        catalog.add(stream("queued-fatal", Map.of(),
                partition(129L, LogOffset.NOT_FOUND),
                partition(130L, LogOffset.NOT_FOUND)));
        FatalValidationMemoryCompactTaskManager tasks =
                new FatalValidationMemoryCompactTaskManager(2);
        PausingScheduledExecutor publishExecutor = new PausingScheduledExecutor();
        PublishCompactTaskRunner runner = runner(
                catalog.catalog(), tasks, new StorageConfig(), CompactionMetrics.NOOP, publishExecutor);
        runner.scanCatalogOnce();
        assertThat(tasks.acquisitionCount()).isEqualTo(2);

        publishExecutor.pauseSecondTask();
        tasks.failNextValidation();
        assertThatThrownBy(runner::run)
                .isInstanceOf(AssertionError.class)
                .hasMessage("fatal publication validation failure");
        assertThat(publishExecutor.awaitPausedTask()).isTrue();
        assertThat(tasks.releaseCount()).isEqualTo(2);

        publishExecutor.allowPausedTask();
        assertThat(publishExecutor.awaitPausedTaskCompletion()).isTrue();
        assertThat(tasks.acquisitionCount()).isEqualTo(2);
        assertThat(runner.sessionCount()).isZero();
        runner.stop();
    }

    @Test
    void acquireBlockedAcrossFatalResetClosesLateSession() throws Exception {
        MutableCatalog catalog = new MutableCatalog();
        catalog.add(stream("existing-fatal", Map.of(),
                partition(131L, LogOffset.NOT_FOUND)));
        FatalBlockingAcquireMemoryCompactTaskManager tasks =
                new FatalBlockingAcquireMemoryCompactTaskManager(132L);
        PublishCompactTaskRunner runner = runner(
                catalog.catalog(), tasks, new StorageConfig(), CompactionMetrics.NOOP,
                Executors.newScheduledThreadPool(2));
        runner.scanCatalogOnce();
        catalog.add(stream("late-fatal", Map.of(),
                partition(132L, LogOffset.NOT_FOUND)));

        tasks.armFatalScan();
        assertThatThrownBy(runner::run)
                .isInstanceOf(AssertionError.class)
                .hasMessage("fatal publication validation failure");
        assertThat(tasks.awaitBlockedAcquire()).isTrue();
        assertThat(tasks.releaseCount()).isOne();

        tasks.allowBlockedAcquire();
        assertThat(tasks.awaitLateRelease()).isTrue();
        assertThat(tasks.acquisitionCount()).isEqualTo(2);
        assertThat(tasks.releaseCount()).isEqualTo(2);
        assertThat(runner.sessionCount()).isZero();
        runner.stop();
    }

    @Test
    void laterFatalCompletionResetsEpochWhileEarlierAcquireIsBlocked() throws Exception {
        MutableCatalog catalog = new MutableCatalog();
        MutableStream existing = stream("later-fatal", Map.of(),
                partition(136L, LogOffset.NOT_FOUND));
        catalog.add(existing);
        FatalBlockingAcquireMemoryCompactTaskManager tasks =
                new FatalBlockingAcquireMemoryCompactTaskManager(135L);
        PublishCompactTaskRunner runner = runner(
                catalog.catalog(), tasks, new StorageConfig(), CompactionMetrics.NOOP,
                Executors.newScheduledThreadPool(2));
        runner.scanCatalogOnce();

        catalog.clear();
        catalog.add(stream("blocked-first", Map.of(),
                partition(135L, LogOffset.NOT_FOUND)));
        catalog.add(existing);
        tasks.armFatalScan();
        CompletableFuture<Void> scan = CompletableFuture.runAsync(
                runner, track(Executors.newSingleThreadExecutor()));

        try {
            Throwable fatal = catchThrowable(() -> scan.orTimeout(5, TimeUnit.SECONDS).join());
            assertThat(fatal)
                    .isInstanceOf(CompletionException.class)
                    .hasRootCauseInstanceOf(AssertionError.class)
                    .hasRootCauseMessage("fatal publication validation failure");
            assertThat(tasks.awaitBlockedAcquire()).isTrue();
            assertThat(tasks.releaseCount()).isOne();
            assertThat(runner.sessionCount()).isZero();

            tasks.allowBlockedAcquire();
            assertThat(tasks.awaitLateRelease()).isTrue();
            assertThat(tasks.acquisitionCount()).isEqualTo(2);
            assertThat(tasks.releaseCount()).isEqualTo(2);
            assertThat(runner.sessionCount()).isZero();
        } finally {
            tasks.allowBlockedAcquire();
            runner.stop();
        }
    }

    @Test
    void queuedPublicationReceivesDeadlineFromActualWorkerStart() throws Exception {
        MutableCatalog catalog = new MutableCatalog();
        catalog.add(stream("per-start-deadline", Map.of(),
                partition(145L, LogOffset.NOT_FOUND),
                partition(146L, LogOffset.NOT_FOUND)));
        SequentialBlockingValidationMemoryCompactTaskManager tasks =
                new SequentialBlockingValidationMemoryCompactTaskManager(145L, 146L);
        StorageConfig config = new StorageConfig();
        config.setPublishThreadNum(2);
        PublishCompactTaskRunner runner = runner(
                catalog.catalog(), tasks, config, CompactionMetrics.NOOP,
                Executors.newSingleThreadExecutor(), System::currentTimeMillis, 1_000L);
        runner.scanCatalogOnce();

        tasks.arm();
        CompletableFuture<Void> scan = CompletableFuture.runAsync(() -> {
            try {
                runner.scanCatalogOnce();
            } catch (Exception error) {
                throw new CompletionException(error);
            }
        }, track(Executors.newSingleThreadExecutor()));

        try {
            assertThat(tasks.awaitFirstValidation()).isTrue();
            Thread.sleep(600L);
            tasks.allowFirstValidation();
            assertThat(tasks.awaitSecondValidation()).isTrue();
            Thread.sleep(600L);
            assertThat(scan).isNotDone();
            tasks.allowSecondValidation();
            scan.get(2, TimeUnit.SECONDS);
            assertThat(runner.sessionCount()).isEqualTo(2);
            assertThat(tasks.releaseCount()).isZero();
        } finally {
            tasks.allowFirstValidation();
            tasks.allowSecondValidation();
            runner.stop();
        }
    }

    @Test
    void inlineTimeoutBeforeScheduleReturnsDoesNotLeakPhysicalIdentity() throws Exception {
        MutableCatalog catalog = new MutableCatalog();
        catalog.add(stream("inline-timeout", Map.of(),
                partition(150L, LogOffset.NOT_FOUND)));
        MemoryCompactTaskManager tasks = new MemoryCompactTaskManager();
        StorageConfig config = new StorageConfig();
        config.setPublishThreadNum(1);
        AtomicLong now = new AtomicLong(1_000L);
        InlineFirstScheduledExecutor control = track(new InlineFirstScheduledExecutor());
        ExecutorService workers = track(Executors.newCachedThreadPool());
        PublishCompactTaskRunner.PublicationCoordinator coordinator =
                new PublishCompactTaskRunner.PublicationCoordinator(1);
        PublishCompactTaskRunner runner = new PublishCompactTaskRunner(
                catalog.catalog(), new CompactionManager(tasks, CompactionMetrics.NOOP),
                track(Executors.newSingleThreadExecutor()), control, workers, config,
                CompactionMetrics.NOOP, coordinator, now::get, 100L);

        control.runNextScheduleBeforeReturning();
        runner.scanCatalogOnce();
        assertThat(runner.sessionCount()).isZero();

        now.addAndGet(PublishCompactTaskRunner.MIN_PUBLICATION_RECOVERY_BACKOFF_MILLIS);
        runner.scanCatalogOnce();
        assertThat(runner.sessionCount()).isOne();
        runner.stop();
    }

    @Test
    void workerSubmissionFatalReleasesCoordinatorSlot() throws Exception {
        MutableCatalog catalog = new MutableCatalog();
        catalog.add(stream("worker-submit-fatal", Map.of(),
                partition(151L, LogOffset.NOT_FOUND)));
        FailFirstWorkerExecutor workers = track(new FailFirstWorkerExecutor());
        PublishCompactTaskRunner runner = runner(
                catalog.catalog(), new MemoryCompactTaskManager(), new StorageConfig(),
                CompactionMetrics.NOOP, workers);

        assertThatThrownBy(runner::run)
                .isInstanceOf(AssertionError.class)
                .hasMessage("worker submission fatal");

        runner.scanCatalogOnce();
        assertThat(runner.sessionCount()).isOne();
        runner.stop();
    }

    @Test
    void controlSchedulingFatalCompletesAttemptAndReleasesCoordinatorSlot() throws Exception {
        MutableCatalog catalog = new MutableCatalog();
        catalog.add(stream("control-schedule-fatal", Map.of(),
                partition(152L, LogOffset.NOT_FOUND)));
        FailFirstScheduledExecutor control = track(new FailFirstScheduledExecutor());
        MemoryCompactTaskManager tasks = new MemoryCompactTaskManager();
        StorageConfig config = new StorageConfig();
        PublishCompactTaskRunner runner = new PublishCompactTaskRunner(
                catalog.catalog(), new CompactionManager(tasks, CompactionMetrics.NOOP),
                track(Executors.newSingleThreadExecutor()), control,
                track(Executors.newCachedThreadPool()), config, CompactionMetrics.NOOP,
                new PublishCompactTaskRunner.PublicationCoordinator(config.getPublishThreadNum()));

        assertThatThrownBy(runner::run)
                .isInstanceOf(AssertionError.class)
                .hasMessage("control scheduling fatal");

        runner.scanCatalogOnce();
        assertThat(runner.sessionCount()).isOne();
        runner.stop();
    }

    @Test
    void scanSubmissionFatalFencesAndReleasesExistingPublishers() throws Exception {
        MutableCatalog catalog = new MutableCatalog();
        catalog.add(stream("scan-submit-fatal", Map.of(),
                partition(156L, LogOffset.NOT_FOUND)));
        ReleaseTrackingMemoryCompactTaskManager tasks =
                new ReleaseTrackingMemoryCompactTaskManager(1);
        FailFirstWorkerExecutor scanExecutor = track(new FailFirstWorkerExecutor());
        StorageConfig config = new StorageConfig();
        PublishCompactTaskRunner runner = new PublishCompactTaskRunner(
                catalog.catalog(), new CompactionManager(tasks, CompactionMetrics.NOOP),
                scanExecutor, track(Executors.newSingleThreadScheduledExecutor()),
                track(Executors.newCachedThreadPool()), config, CompactionMetrics.NOOP,
                new PublishCompactTaskRunner.PublicationCoordinator(config.getPublishThreadNum()));
        runner.scanCatalogOnce();
        assertThat(runner.sessionCount()).isOne();

        assertThatThrownBy(runner::start)
                .isInstanceOf(AssertionError.class)
                .hasMessage("worker submission fatal");

        assertThat(runner.sessionCount()).isZero();
        assertThat(tasks.awaitExpectedReleases()).isTrue();
        runner.stop();
    }

    @Test
    void nextScanSchedulingFatalFencesAndReleasesExistingPublishers() throws Exception {
        MutableCatalog catalog = new MutableCatalog();
        catalog.add(stream("next-scan-fatal", Map.of(),
                partition(157L, LogOffset.NOT_FOUND)));
        ReleaseTrackingMemoryCompactTaskManager tasks =
                new ReleaseTrackingMemoryCompactTaskManager(1);
        FailNextScanScheduledExecutor control = track(new FailNextScanScheduledExecutor());
        StorageConfig config = new StorageConfig();
        config.setRefreshLocalTaskIntervalInSeconds(1);
        PublishCompactTaskRunner runner = new PublishCompactTaskRunner(
                catalog.catalog(), new CompactionManager(tasks, CompactionMetrics.NOOP),
                track(Executors.newSingleThreadExecutor()), control,
                track(Executors.newCachedThreadPool()), config, CompactionMetrics.NOOP,
                new PublishCompactTaskRunner.PublicationCoordinator(config.getPublishThreadNum()));
        runner.scanCatalogOnce();
        assertThat(runner.sessionCount()).isOne();

        control.failNextScanSchedule();
        assertThatThrownBy(runner::run)
                .isInstanceOf(AssertionError.class)
                .hasMessage("next scan scheduling fatal");

        assertThat(runner.sessionCount()).isZero();
        assertThat(tasks.awaitExpectedReleases()).isTrue();
        runner.stop();
    }

    @Test
    void hungPartitionTimesOutWithoutStoppingHealthyPublication() throws Exception {
        MutableCatalog catalog = new MutableCatalog();
        MutablePartition blocked = partition(141L, LogOffset.NOT_FOUND);
        MutablePartition healthy = partition(142L, LogOffset.NOT_FOUND);
        catalog.add(stream("bounded-publication", Map.of(), blocked, healthy));
        BlockingValidationMemoryCompactTaskManager tasks =
                new BlockingValidationMemoryCompactTaskManager(141L);
        StorageConfig config = new StorageConfig();
        config.setPublishThreadNum(1);
        PublishCompactTaskRunner runner = runner(
                catalog.catalog(), tasks, config, CompactionMetrics.NOOP,
                Executors.newCachedThreadPool(), System::currentTimeMillis, 100L);
        runner.scanCatalogOnce();
        assertThat(runner.sessionCount()).isEqualTo(2);

        tasks.blockNextValidation();
        CompletableFuture<Void> scan = CompletableFuture.runAsync(() -> {
            try {
                runner.scanCatalogOnce();
            } catch (Exception error) {
                throw new CompletionException(error);
            }
        }, track(Executors.newSingleThreadExecutor()));

        try {
            assertThat(tasks.awaitBlockedValidation()).isTrue();
            scan.get(2, TimeUnit.SECONDS);
            assertThat(runner.sessionCount()).isOne();
            assertThat(tasks.awaitBlockedLeaseRelease()).isTrue();

            healthy.lastOffset(new LogOffset(0L, 2, 1L, 20, 20L));
            runner.scanCatalogOnce();

            assertThat(tasks(tasks)).extracting(CompactStreamTask::getStreamId)
                    .containsExactly(142L);
        } finally {
            tasks.allowBlockedValidation();
            runner.stop();
        }
    }

    @Test
    void stickyHungLimitStopsLaunchingMorePublicationWorkers() throws Exception {
        MutableCatalog catalog = new MutableCatalog();
        catalog.add(stream("sticky-limit", Map.of(),
                partition(153L, LogOffset.NOT_FOUND),
                partition(154L, LogOffset.NOT_FOUND),
                partition(155L, LogOffset.NOT_FOUND)));
        MultiBlockingValidationMemoryCompactTaskManager tasks =
                new MultiBlockingValidationMemoryCompactTaskManager(Set.of(153L, 154L, 155L), 2);
        CompactionMetrics metrics = mock(CompactionMetrics.class);
        Counter publicationFailures = mock(Counter.class);
        LongGauge ongoingStreams = mock(LongGauge.class);
        when(metrics.getPublishTaskFailedCount()).thenReturn(publicationFailures);
        when(metrics.getOngoingCompactionTopicCount()).thenReturn(ongoingStreams);
        StorageConfig config = new StorageConfig();
        config.setPublishThreadNum(1);
        PublishCompactTaskRunner runner = runner(
                catalog.catalog(), tasks, config, metrics,
                Executors.newCachedThreadPool(), System::currentTimeMillis, 100L);
        runner.scanCatalogOnce();

        tasks.arm();
        CompletableFuture<Void> scan = CompletableFuture.runAsync(() -> {
            try {
                runner.scanCatalogOnce();
            } catch (Exception error) {
                throw new CompletionException(error);
            }
        }, track(Executors.newSingleThreadExecutor()));

        try {
            assertThat(tasks.awaitExpectedValidations()).isTrue();
            scan.get(2, TimeUnit.SECONDS);
            verify(publicationFailures, timeout(5_000).times(3)).increment();
            assertThat(tasks.validationCount()).isEqualTo(2);
            assertThat(runner.sessionCount()).isOne();
        } finally {
            tasks.allowValidations();
            runner.stop();
        }
    }

    @Test
    void lateFatalAfterTimeoutSupervisesCurrentPublisherEpoch() throws Exception {
        MutableCatalog catalog = new MutableCatalog();
        catalog.add(stream("late-timeout-fatal", Map.of(),
                partition(147L, LogOffset.NOT_FOUND),
                partition(148L, LogOffset.NOT_FOUND)));
        LateFatalBlockingValidationMemoryCompactTaskManager tasks =
                new LateFatalBlockingValidationMemoryCompactTaskManager(147L, 2);
        CompactionMetrics metrics = mock(CompactionMetrics.class);
        Counter publicationFailures = mock(Counter.class);
        LongGauge ongoingStreams = mock(LongGauge.class);
        when(metrics.getPublishTaskFailedCount()).thenReturn(publicationFailures);
        when(metrics.getOngoingCompactionTopicCount()).thenReturn(ongoingStreams);
        StorageConfig config = new StorageConfig();
        config.setPublishThreadNum(1);
        PublishCompactTaskRunner runner = runner(
                catalog.catalog(), tasks, config, metrics,
                Executors.newCachedThreadPool(), System::currentTimeMillis, 100L);
        runner.scanCatalogOnce();

        tasks.arm();
        runner.scanCatalogOnce();
        assertThat(tasks.awaitBlockedValidation()).isTrue();
        assertThat(runner.sessionCount()).isOne();

        tasks.allowBlockedValidation();
        verify(publicationFailures, timeout(5_000).times(2)).increment();
        assertThat(runner.sessionCount()).isZero();
        assertThat(tasks.awaitExpectedReleases()).isTrue();
        assertThat(tasks.releaseCount()).isEqualTo(2);
        runner.stop();
    }

    @Test
    void sharedCoordinatorPreventsResubmissionAndReportsStaleLateFatal() throws Exception {
        MutableCatalog catalog = new MutableCatalog();
        catalog.add(stream("recreated-timeout-fatal", Map.of(),
                partition(149L, LogOffset.NOT_FOUND)));
        LateFatalBlockingValidationMemoryCompactTaskManager tasks =
                new LateFatalBlockingValidationMemoryCompactTaskManager(149L, 1);
        CompactionMetrics metrics = mock(CompactionMetrics.class);
        Counter publicationFailures = mock(Counter.class);
        LongGauge ongoingStreams = mock(LongGauge.class);
        when(metrics.getPublishTaskFailedCount()).thenReturn(publicationFailures);
        when(metrics.getOngoingCompactionTopicCount()).thenReturn(ongoingStreams);
        StorageConfig config = new StorageConfig();
        config.setPublishThreadNum(1);
        CompactionManager manager = new CompactionManager(tasks, metrics);
        PublishCompactTaskRunner.PublicationCoordinator coordinator =
                new PublishCompactTaskRunner.PublicationCoordinator(1);
        ScheduledExecutorService control = track(Executors.newSingleThreadScheduledExecutor());
        ExecutorService workers = track(Executors.newCachedThreadPool());
        PublishCompactTaskRunner first = new PublishCompactTaskRunner(
                catalog.catalog(), manager, track(Executors.newSingleThreadExecutor()),
                control, workers, config, metrics, coordinator, System::currentTimeMillis, 100L);
        PublishCompactTaskRunner successor = new PublishCompactTaskRunner(
                catalog.catalog(), manager, track(Executors.newSingleThreadExecutor()),
                control, workers, config, metrics, coordinator, System::currentTimeMillis, 100L);
        first.scanCatalogOnce();

        tasks.arm();
        first.scanCatalogOnce();
        first.stop();
        successor.scanCatalogOnce();

        assertThat(tasks.validationCount()).isOne();
        assertThat(successor.sessionCount()).isZero();

        tasks.allowBlockedValidation();
        verify(publicationFailures, timeout(5_000).times(2)).increment();
        assertThat(tasks.awaitExpectedReleases()).isTrue();
        successor.scanCatalogOnce();
        assertThat(successor.sessionCount()).isOne();
        successor.stop();
    }

    @Test
    void fatalMetricErrorCannotPreventLeaseCleanup() throws Exception {
        MutableCatalog catalog = new MutableCatalog();
        catalog.add(stream("fatal-metric", Map.of(),
                partition(133L, LogOffset.NOT_FOUND)));
        FatalValidationMemoryCompactTaskManager tasks =
                new FatalValidationMemoryCompactTaskManager(1);
        CompactionMetrics metrics = mock(CompactionMetrics.class);
        Counter publicationFailures = mock(Counter.class);
        LongGauge ongoingStreams = mock(LongGauge.class);
        when(metrics.getPublishTaskFailedCount()).thenReturn(publicationFailures);
        when(metrics.getOngoingCompactionTopicCount()).thenReturn(ongoingStreams);
        doThrow(new AssertionError("publication metric failure"))
                .when(publicationFailures).increment();
        PublishCompactTaskRunner runner = runner(
                catalog.catalog(), tasks, new StorageConfig(), metrics,
                Executors.newSingleThreadScheduledExecutor());
        runner.scanCatalogOnce();

        tasks.failNextValidation();
        Throwable fatal = catchThrowable(runner::run);

        assertThat(fatal)
                .isInstanceOf(AssertionError.class)
                .hasMessage("fatal publication validation failure");
        assertThat(fatal.getSuppressed()).extracting(Throwable::getMessage)
                .contains("publication metric failure");
        assertThat(tasks.releaseCount()).isOne();
        assertThat(runner.sessionCount()).isZero();
        runner.stop();
    }

    @Test
    void blockedPublisherCannotDelayFatalCleanupForOtherLeases() throws Exception {
        MutableCatalog catalog = new MutableCatalog();
        catalog.add(stream("blocked-publisher", Map.of(),
                partition(137L, LogOffset.NOT_FOUND)));
        catalog.add(stream("fatal-publisher", Map.of(),
                partition(138L, LogOffset.NOT_FOUND)));
        FatalBlockedPublisherMemoryCompactTaskManager tasks =
                new FatalBlockedPublisherMemoryCompactTaskManager(137L, 138L);
        StorageConfig config = new StorageConfig();
        config.setRefreshLocalTaskIntervalInSeconds(60);
        PublishCompactTaskRunner runner = runner(
                catalog.catalog(), tasks, config, CompactionMetrics.NOOP,
                Executors.newScheduledThreadPool(2));
        runner.scanCatalogOnce();
        assertThat(runner.sessionCount()).isEqualTo(2);

        tasks.armFatalScan();
        CompletableFuture<Void> fatalScan = CompletableFuture.runAsync(
                runner, track(Executors.newSingleThreadExecutor()));
        try {
            assertThat(tasks.awaitBlockedValidation()).isTrue();
            Throwable fatal = catchThrowable(() -> fatalScan.orTimeout(5, TimeUnit.SECONDS).join());
            assertThat(fatal)
                    .isInstanceOf(CompletionException.class)
                    .hasRootCauseInstanceOf(AssertionError.class)
                    .hasRootCauseMessage("fatal publication validation failure");
            assertThat(tasks.fatalLeaseReleased()).isTrue();
            assertThat(tasks.awaitBlockedLeaseRelease()).isTrue();
            assertThat(tasks.releaseCount()).isEqualTo(2);
            assertThat(runner.sessionCount()).isZero();

            tasks.allowBlockedValidation();
            assertThat(tasks.releaseCount()).isEqualTo(2);
        } finally {
            tasks.allowBlockedValidation();
            runner.stop();
        }
    }

    @Test
    void stopInitiatesOtherLeaseReleasesWhenOneRemoteDeleteHangs() throws Exception {
        MutableCatalog catalog = new MutableCatalog();
        catalog.add(stream("blocked-release", Map.of(),
                partition(143L, LogOffset.NOT_FOUND),
                partition(144L, LogOffset.NOT_FOUND)));
        BlockingReleaseMemoryCompactTaskManager tasks =
                new BlockingReleaseMemoryCompactTaskManager(143L, 144L);
        PublishCompactTaskRunner runner = runner(
                catalog.catalog(), tasks, new StorageConfig(), CompactionMetrics.NOOP,
                Executors.newSingleThreadScheduledExecutor());
        runner.scanCatalogOnce();
        assertThat(runner.sessionCount()).isEqualTo(2);

        runner.stop();

        assertThat(runner.sessionCount()).isZero();
        assertThat(tasks.awaitBlockedRelease()).isTrue();
        assertThat(tasks.healthyLeaseReleased()).isTrue();

        tasks.completeBlockedRelease();
        assertThat(tasks.awaitBlockedReleaseCompletion()).isTrue();
    }

    @Test
    void closeFailureMetricErrorCannotPreventLeaseReleaseRetry() throws Exception {
        MutableCatalog catalog = new MutableCatalog();
        catalog.add(stream("close-metric", Map.of(),
                partition(134L, LogOffset.NOT_FOUND)));
        FlakyReleaseMemoryCompactTaskManager tasks = new FlakyReleaseMemoryCompactTaskManager();
        CompactionMetrics metrics = mock(CompactionMetrics.class);
        Counter publicationFailures = mock(Counter.class);
        LongGauge ongoingStreams = mock(LongGauge.class);
        when(metrics.getPublishTaskFailedCount()).thenReturn(publicationFailures);
        when(metrics.getOngoingCompactionTopicCount()).thenReturn(ongoingStreams);
        doThrow(new AssertionError("close metric failure"))
                .when(publicationFailures).increment();
        PublishCompactTaskRunner runner = runner(
                catalog.catalog(), tasks, new StorageConfig(), metrics,
                Executors.newSingleThreadScheduledExecutor());
        runner.scanCatalogOnce();

        runner.stop();

        assertThat(tasks.releaseRetried.await(5, TimeUnit.SECONDS)).isTrue();
    }

    @Test
    void pendingLeaseReleaseErrorIsObservedAndRetried() throws Exception {
        MutableCatalog catalog = new MutableCatalog();
        catalog.add(stream("observed-release-error", Map.of(),
                partition(145L, LogOffset.NOT_FOUND)));
        FlakyReleaseMemoryCompactTaskManager tasks = new FlakyReleaseMemoryCompactTaskManager();
        CompactionMetrics metrics = mock(CompactionMetrics.class);
        Counter publicationFailures = mock(Counter.class);
        LongGauge ongoingStreams = mock(LongGauge.class);
        when(metrics.getPublishTaskFailedCount()).thenReturn(publicationFailures);
        when(metrics.getOngoingCompactionTopicCount()).thenReturn(ongoingStreams);
        StorageConfig config = new StorageConfig();
        config.setRefreshLocalTaskIntervalInSeconds(1);
        PublishCompactTaskRunner runner = runner(
                catalog.catalog(), tasks, config, metrics,
                Executors.newSingleThreadScheduledExecutor());

        runner.scanCatalogOnce();
        runner.stop();

        assertThat(tasks.releaseRetried.await(5, TimeUnit.SECONDS)).isTrue();
        verify(publicationFailures).increment();
    }

    @Test
    void legacyCursorIsReleasedAndRetriedOnlyAfterBackoff() throws Exception {
        String publicationName = "default/legacy-cursor-partition-0";
        MutableCatalog catalog = new MutableCatalog();
        catalog.add(stream("legacy-cursor", Map.of(),
                partition(121L, LogOffset.NOT_FOUND)));
        LegacyCursorMemoryCompactTaskManager tasks = new LegacyCursorMemoryCompactTaskManager();
        tasks.seedLegacyCursor(publicationName, 121L, 5L);
        AtomicLong now = new AtomicLong(1_000L);
        CompactionMetrics metrics = mock(CompactionMetrics.class);
        Counter publicationFailures = mock(Counter.class);
        LongGauge ongoingStreams = mock(LongGauge.class);
        when(metrics.getPublishTaskFailedCount()).thenReturn(publicationFailures);
        when(metrics.getOngoingCompactionTopicCount()).thenReturn(ongoingStreams);
        StorageConfig config = new StorageConfig();
        config.setRefreshLocalTaskIntervalInSeconds(1);
        PublishCompactTaskRunner runner = runner(
                catalog.catalog(),
                tasks,
                config,
                metrics,
                Executors.newSingleThreadScheduledExecutor(),
                now::get);

        runner.scanCatalogOnce();
        assertThat(runner.sessionCount()).isZero();
        assertThat(tasks.acquisitionCount()).isOne();
        assertThat(tasks.releaseCount()).isOne();
        verify(publicationFailures).increment();

        runner.scanCatalogOnce();
        tasks.updatePublishedOffset(publicationName, 121L, 5L, 50L);
        runner.scanCatalogOnce();
        assertThat(tasks.acquisitionCount()).isOne();

        now.addAndGet(PublishCompactTaskRunner.MIN_PUBLICATION_RECOVERY_BACKOFF_MILLIS);
        runner.scanCatalogOnce();
        assertThat(tasks.acquisitionCount()).isEqualTo(2);
        assertThat(runner.sessionCount()).isOne();
        verify(publicationFailures).increment();
        runner.stop();
    }

    @Test
    void quarantineMetricFailureCannotRetainPublicationLease() throws Exception {
        String publicationName = "default/quarantine-metric-partition-0";
        MutableCatalog catalog = new MutableCatalog();
        catalog.add(stream("quarantine-metric", Map.of(),
                partition(139L, LogOffset.NOT_FOUND)));
        LegacyCursorMemoryCompactTaskManager tasks = new LegacyCursorMemoryCompactTaskManager();
        tasks.seedLegacyCursor(publicationName, 139L, 5L);
        CompactionMetrics metrics = mock(CompactionMetrics.class);
        Counter publicationFailures = mock(Counter.class);
        LongGauge ongoingStreams = mock(LongGauge.class);
        when(metrics.getPublishTaskFailedCount()).thenReturn(publicationFailures);
        when(metrics.getOngoingCompactionTopicCount()).thenReturn(ongoingStreams);
        doThrow(new IllegalStateException("quarantine metric failure"))
                .when(publicationFailures).increment();
        PublishCompactTaskRunner runner = runner(
                catalog.catalog(), tasks, new StorageConfig(), metrics,
                Executors.newSingleThreadScheduledExecutor());

        runner.scanCatalogOnce();

        assertThat(tasks.releaseCount()).isOne();
        assertThat(runner.sessionCount()).isZero();
        runner.stop();
    }

    @Test
    void legacyCursorQuarantineDoesNotBlockHealthyPartitions() throws Exception {
        MutableCatalog catalog = new MutableCatalog();
        catalog.add(stream("mixed-legacy", Map.of(),
                partition(125L, LogOffset.NOT_FOUND),
                partition(126L, LogOffset.NOT_FOUND)));
        LegacyCursorMemoryCompactTaskManager tasks = new LegacyCursorMemoryCompactTaskManager();
        tasks.seedLegacyCursor("default/mixed-legacy-partition-0", 125L, 5L);
        PublishCompactTaskRunner runner = runner(
                catalog.catalog(), tasks, new StorageConfig());

        runner.scanCatalogOnce();

        assertThat(tasks.acquisitionCount()).isEqualTo(2);
        assertThat(tasks.releaseCount()).isOne();
        assertThat(runner.sessionCount()).isOne();
        runner.stop();
    }

    @Test
    void inconsistentPreparedTaskIsReleasedAndRetriedOnlyAfterBackoff() throws Exception {
        MutableCatalog catalog = new MutableCatalog();
        catalog.add(stream("invalid-prepared", Map.of(),
                partition(127L, LogOffset.NOT_FOUND)));
        RecoveryFailureMemoryCompactTaskManager tasks =
                new RecoveryFailureMemoryCompactTaskManager(
                        "default/invalid-prepared-partition-0", 127L);
        AtomicLong now = new AtomicLong(1_000L);
        PublishCompactTaskRunner runner = runner(
                catalog.catalog(), tasks, new StorageConfig(), CompactionMetrics.NOOP,
                Executors.newSingleThreadScheduledExecutor(), now::get);

        runner.scanCatalogOnce();

        assertThat(tasks.acquisitionCount()).isOne();
        assertThat(tasks.releaseCount()).isOne();
        assertThat(runner.sessionCount()).isZero();

        runner.scanCatalogOnce();
        assertThat(tasks.acquisitionCount()).isOne();

        now.addAndGet(PublishCompactTaskRunner.MIN_PUBLICATION_RECOVERY_BACKOFF_MILLIS);
        runner.scanCatalogOnce();
        assertThat(tasks.acquisitionCount()).isEqualTo(2);
        assertThat(tasks.releaseCount()).isEqualTo(2);
        runner.stop();
    }

    @Test
    void cursorBeyondLogCumulativeSizeIsReleasedAndQuarantined() throws Exception {
        String publicationName = "default/oversized-cursor-partition-0";
        MutableCatalog catalog = new MutableCatalog();
        catalog.add(stream("oversized-cursor", Map.of(),
                partition(140L, new LogOffset(5L, 5, 1L, 50, 80L))));
        LegacyCursorMemoryCompactTaskManager tasks = new LegacyCursorMemoryCompactTaskManager();
        tasks.updatePublishedOffset(publicationName, 140L, 4L, 100L);
        AtomicLong now = new AtomicLong(1_000L);
        PublishCompactTaskRunner runner = runner(
                catalog.catalog(), tasks, new StorageConfig(), CompactionMetrics.NOOP,
                Executors.newSingleThreadScheduledExecutor(), now::get);

        runner.scanCatalogOnce();

        assertThat(tasks.acquisitionCount()).isOne();
        assertThat(tasks.releaseCount()).isOne();
        assertThat(runner.sessionCount()).isZero();
        assertThat(tasks(tasks)).isEmpty();

        runner.scanCatalogOnce();
        assertThat(tasks.acquisitionCount()).isOne();
        runner.stop();
    }

    @Test
    void physicalLogReplacementClearsLegacyCursorBackoff() throws Exception {
        String publicationName = "default/replaced-legacy-partition-0";
        MutableCatalog catalog = new MutableCatalog();
        MutableStream stream = stream("replaced-legacy", Map.of(),
                partition(122L, LogOffset.NOT_FOUND));
        catalog.add(stream);
        LegacyCursorMemoryCompactTaskManager tasks = new LegacyCursorMemoryCompactTaskManager();
        tasks.seedLegacyCursor(publicationName, 122L, 5L);
        AtomicLong now = new AtomicLong(1_000L);
        PublishCompactTaskRunner runner = runner(
                catalog.catalog(), tasks, new StorageConfig(), CompactionMetrics.NOOP,
                Executors.newSingleThreadScheduledExecutor(), now::get);

        runner.scanCatalogOnce();
        stream.partitions(List.of(partition(123L, LogOffset.NOT_FOUND)));
        runner.scanCatalogOnce();

        assertThat(tasks.acquisitionCount()).isEqualTo(2);
        assertThat(runner.sessionCount()).isOne();
        runner.stop();
    }

    @Test
    void streamRemovalClearsLegacyCursorBackoff() throws Exception {
        String publicationName = "default/removed-legacy-partition-0";
        MutableCatalog catalog = new MutableCatalog();
        catalog.add(stream("removed-legacy", Map.of(),
                partition(124L, LogOffset.NOT_FOUND)));
        LegacyCursorMemoryCompactTaskManager tasks = new LegacyCursorMemoryCompactTaskManager();
        tasks.seedLegacyCursor(publicationName, 124L, 5L);
        AtomicLong now = new AtomicLong(1_000L);
        PublishCompactTaskRunner runner = runner(
                catalog.catalog(), tasks, new StorageConfig(), CompactionMetrics.NOOP,
                Executors.newSingleThreadScheduledExecutor(), now::get);

        runner.scanCatalogOnce();
        catalog.clear();
        runner.scanCatalogOnce();
        tasks.updatePublishedOffset(publicationName, 124L, 5L, 50L);
        catalog.add(stream("removed-legacy", Map.of(),
                partition(124L, LogOffset.NOT_FOUND)));
        runner.scanCatalogOnce();

        assertThat(tasks.acquisitionCount()).isEqualTo(2);
        assertThat(runner.sessionCount()).isOne();
        runner.stop();
    }

    private PublishCompactTaskRunner runner(StreamCatalog catalog,
                                            MemoryCompactTaskManager tasks,
                                            Properties properties) {
        StorageConfig config = new StorageConfig();
        config.setProperties(properties);
        return runner(catalog, tasks, config);
    }

    private PublishCompactTaskRunner runner(StreamCatalog catalog,
                                            MemoryCompactTaskManager tasks,
                                            StorageConfig config) {
        return runner(
                catalog,
                tasks,
                config,
                CompactionMetrics.NOOP,
                Executors.newSingleThreadScheduledExecutor());
    }

    private PublishCompactTaskRunner runner(StreamCatalog catalog,
                                            MemoryCompactTaskManager tasks,
                                            StorageConfig config,
                                            CompactionMetrics metrics,
                                            ExecutorService publicationWorkerExecutor) {
        return new PublishCompactTaskRunner(
                catalog,
                tasks,
                track(Executors.newSingleThreadExecutor()),
                track(Executors.newSingleThreadScheduledExecutor()),
                track(publicationWorkerExecutor),
                config,
                metrics);
    }

    private PublishCompactTaskRunner runner(StreamCatalog catalog,
                                            CompactionManager compactionManager,
                                            StorageConfig config,
                                            CompactionMetrics metrics,
                                            ExecutorService publicationWorkerExecutor) {
        return new PublishCompactTaskRunner(
                catalog,
                compactionManager,
                track(Executors.newSingleThreadExecutor()),
                track(Executors.newSingleThreadScheduledExecutor()),
                track(publicationWorkerExecutor),
                config,
                metrics,
                new PublishCompactTaskRunner.PublicationCoordinator(config.getPublishThreadNum()));
    }

    private PublishCompactTaskRunner runner(StreamCatalog catalog,
                                            MemoryCompactTaskManager tasks,
                                            StorageConfig config,
                                            CompactionMetrics metrics,
                                            ExecutorService publicationWorkerExecutor,
                                            LongSupplier currentTimeMillis) {
        return new PublishCompactTaskRunner(
                catalog,
                new CompactionManager(tasks, metrics),
                track(Executors.newSingleThreadExecutor()),
                track(Executors.newSingleThreadScheduledExecutor()),
                track(publicationWorkerExecutor),
                config,
                metrics,
                new PublishCompactTaskRunner.PublicationCoordinator(config.getPublishThreadNum()),
                currentTimeMillis);
    }

    private PublishCompactTaskRunner runner(StreamCatalog catalog,
                                            MemoryCompactTaskManager tasks,
                                            StorageConfig config,
                                            CompactionMetrics metrics,
                                            ExecutorService publicationWorkerExecutor,
                                            LongSupplier currentTimeMillis,
                                            long publicationTaskTimeoutMillis) {
        return new PublishCompactTaskRunner(
                catalog,
                new CompactionManager(tasks, metrics),
                track(Executors.newSingleThreadExecutor()),
                track(Executors.newSingleThreadScheduledExecutor()),
                track(publicationWorkerExecutor),
                config,
                metrics,
                new PublishCompactTaskRunner.PublicationCoordinator(config.getPublishThreadNum()),
                currentTimeMillis,
                publicationTaskTimeoutMillis);
    }

    private <T extends ExecutorService> T track(T executor) {
        executors.add(executor);
        return executor;
    }

    private static final class ReleaseTrackingMemoryCompactTaskManager extends MemoryCompactTaskManager {
        private final CountDownLatch expectedReleases;

        private ReleaseTrackingMemoryCompactTaskManager(int expectedReleaseCount) {
            this.expectedReleases = new CountDownLatch(expectedReleaseCount);
        }

        @Override
        public synchronized boolean releasePublicationLease(PublicationLease lease) {
            boolean released = super.releasePublicationLease(lease);
            if (released) {
                expectedReleases.countDown();
            }
            return released;
        }

        private boolean awaitExpectedReleases() throws InterruptedException {
            return expectedReleases.await(5, TimeUnit.SECONDS);
        }
    }

    private static final class BlockingMemoryCompactTaskManager extends MemoryCompactTaskManager {
        private final AtomicBoolean blockNextAcquire = new AtomicBoolean(true);
        private final CountDownLatch acquireStarted = new CountDownLatch(1);
        private final CountDownLatch allowAcquire = new CountDownLatch(1);

        @Override
        public Optional<PublicationLease> tryAcquirePublicationLease(String name, long streamId) {
            if (blockNextAcquire.compareAndSet(true, false)) {
                acquireStarted.countDown();
                try {
                    allowAcquire.await();
                } catch (InterruptedException error) {
                    Thread.currentThread().interrupt();
                    throw new IllegalStateException("Interrupted while acquiring publication lease", error);
                }
            }
            return super.tryAcquirePublicationLease(name, streamId);
        }
    }

    private static final class ConcurrentAcquireMemoryCompactTaskManager extends MemoryCompactTaskManager {
        private final CountDownLatch allAcquiresStarted;
        private final CountDownLatch allowAcquire = new CountDownLatch(1);

        private ConcurrentAcquireMemoryCompactTaskManager(int expectedAcquires) {
            this.allAcquiresStarted = new CountDownLatch(expectedAcquires);
        }

        @Override
        public Optional<PublicationLease> tryAcquirePublicationLease(String name, long streamId) {
            allAcquiresStarted.countDown();
            try {
                allowAcquire.await();
            } catch (InterruptedException error) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException("Interrupted while acquiring publication lease", error);
            }
            return super.tryAcquirePublicationLease(name, streamId);
        }
    }

    private static final class PausingScheduledExecutor extends ScheduledThreadPoolExecutor {
        private final AtomicBoolean armed = new AtomicBoolean();
        private final AtomicInteger executionsAfterArm = new AtomicInteger();
        private final CountDownLatch pausedTaskStarted = new CountDownLatch(1);
        private final CountDownLatch allowPausedTask = new CountDownLatch(1);
        private final CountDownLatch pausedTaskCompleted = new CountDownLatch(1);
        private volatile Runnable pausedTask;

        private PausingScheduledExecutor() {
            super(1);
        }

        private void pauseSecondTask() {
            armed.set(true);
        }

        @Override
        protected void beforeExecute(Thread thread, Runnable task) {
            super.beforeExecute(thread, task);
            if (armed.get() && executionsAfterArm.incrementAndGet() == 2) {
                pausedTask = task;
                pausedTaskStarted.countDown();
                try {
                    allowPausedTask.await();
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                    throw new AssertionError("Interrupted while pausing queued publication", interrupted);
                }
            }
        }

        @Override
        protected void afterExecute(Runnable task, Throwable error) {
            try {
                if (task == pausedTask) {
                    pausedTaskCompleted.countDown();
                }
            } finally {
                super.afterExecute(task, error);
            }
        }

        private boolean awaitPausedTask() throws InterruptedException {
            return pausedTaskStarted.await(5, TimeUnit.SECONDS);
        }

        private void allowPausedTask() {
            allowPausedTask.countDown();
        }

        private boolean awaitPausedTaskCompletion() throws InterruptedException {
            return pausedTaskCompleted.await(5, TimeUnit.SECONDS);
        }
    }

    private static final class InlineFirstScheduledExecutor extends ScheduledThreadPoolExecutor {
        private static final ScheduledFuture<?> COMPLETED_SCHEDULE = new CompletedScheduledFuture();
        private final AtomicBoolean runNextBeforeReturning = new AtomicBoolean();

        private InlineFirstScheduledExecutor() {
            super(1);
        }

        private void runNextScheduleBeforeReturning() {
            runNextBeforeReturning.set(true);
        }

        @Override
        public ScheduledFuture<?> schedule(
                Runnable command, long delay, TimeUnit unit) {
            if (!runNextBeforeReturning.compareAndSet(true, false)) {
                return super.schedule(command, delay, unit);
            }
            command.run();
            return COMPLETED_SCHEDULE;
        }
    }

    private static final class CompletedScheduledFuture implements ScheduledFuture<Object> {
        @Override
        public long getDelay(TimeUnit unit) {
            return 0L;
        }

        @Override
        public int compareTo(Delayed ignored) {
            return 0;
        }

        @Override
        public boolean cancel(boolean mayInterruptIfRunning) {
            return false;
        }

        @Override
        public boolean isCancelled() {
            return false;
        }

        @Override
        public boolean isDone() {
            return true;
        }

        @Override
        public Object get() {
            return null;
        }

        @Override
        public Object get(long timeout, TimeUnit unit) {
            return null;
        }
    }

    private static final class FailFirstWorkerExecutor extends ThreadPoolExecutor {
        private final AtomicBoolean failNextSubmission = new AtomicBoolean(true);

        private FailFirstWorkerExecutor() {
            super(1, 1, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>());
        }

        @Override
        public void execute(Runnable command) {
            if (failNextSubmission.compareAndSet(true, false)) {
                throw new AssertionError("worker submission fatal");
            }
            super.execute(command);
        }
    }

    private static final class FailFirstScheduledExecutor extends ScheduledThreadPoolExecutor {
        private final AtomicBoolean failNextSchedule = new AtomicBoolean(true);

        private FailFirstScheduledExecutor() {
            super(1);
        }

        @Override
        public ScheduledFuture<?> schedule(
                Runnable command, long delay, TimeUnit unit) {
            if (failNextSchedule.compareAndSet(true, false)) {
                throw new AssertionError("control scheduling fatal");
            }
            return super.schedule(command, delay, unit);
        }
    }

    private static final class FailNextScanScheduledExecutor extends ScheduledThreadPoolExecutor {
        private final AtomicBoolean failNextScanSchedule = new AtomicBoolean();

        private FailNextScanScheduledExecutor() {
            super(1);
        }

        private void failNextScanSchedule() {
            failNextScanSchedule.set(true);
        }

        @Override
        public ScheduledFuture<?> schedule(
                Runnable command, long delay, TimeUnit unit) {
            if (unit.toMillis(delay) == TimeUnit.SECONDS.toMillis(1)
                    && failNextScanSchedule.compareAndSet(true, false)) {
                throw new AssertionError("next scan scheduling fatal");
            }
            return super.schedule(command, delay, unit);
        }
    }

    private static final class FatalValidationMemoryCompactTaskManager extends MemoryCompactTaskManager {
        private final int initialPublisherCount;
        private final AtomicBoolean failNextValidation = new AtomicBoolean();
        private final AtomicInteger acquisitionCount = new AtomicInteger();
        private final AtomicInteger releaseCount = new AtomicInteger();
        private final CountDownLatch freshScanAcquisitions;

        private FatalValidationMemoryCompactTaskManager(int initialPublisherCount) {
            this.initialPublisherCount = initialPublisherCount;
            this.freshScanAcquisitions = new CountDownLatch(initialPublisherCount);
        }

        private void failNextValidation() {
            failNextValidation.set(true);
        }

        @Override
        public synchronized Optional<PublicationLease> tryAcquirePublicationLease(
                String name, long streamId) {
            Optional<PublicationLease> acquired = super.tryAcquirePublicationLease(name, streamId);
            if (acquired.isPresent() && acquisitionCount.incrementAndGet() > initialPublisherCount) {
                freshScanAcquisitions.countDown();
            }
            return acquired;
        }

        @Override
        public synchronized boolean validatePublicationLease(PublicationLease lease) {
            if (failNextValidation.compareAndSet(true, false)) {
                throw new AssertionError("fatal publication validation failure");
            }
            return super.validatePublicationLease(lease);
        }

        @Override
        public synchronized boolean releasePublicationLease(PublicationLease lease) {
            boolean released = super.releasePublicationLease(lease);
            if (released) {
                releaseCount.incrementAndGet();
            }
            return released;
        }

        private int releaseCount() {
            return releaseCount.get();
        }

        private int acquisitionCount() {
            return acquisitionCount.get();
        }

        private boolean awaitFreshScan() throws InterruptedException {
            return freshScanAcquisitions.await(5, TimeUnit.SECONDS);
        }
    }

    private static final class FatalBlockingAcquireMemoryCompactTaskManager extends MemoryCompactTaskManager {
        private final long blockedStreamId;
        private final AtomicBoolean blockNextAcquire = new AtomicBoolean(true);
        private final AtomicBoolean failNextValidation = new AtomicBoolean();
        private final AtomicInteger acquisitionCount = new AtomicInteger();
        private final AtomicInteger releaseCount = new AtomicInteger();
        private final CountDownLatch blockedAcquireStarted = new CountDownLatch(1);
        private final CountDownLatch allowBlockedAcquire = new CountDownLatch(1);
        private final CountDownLatch lateRelease = new CountDownLatch(1);

        private FatalBlockingAcquireMemoryCompactTaskManager(long blockedStreamId) {
            this.blockedStreamId = blockedStreamId;
        }

        private void armFatalScan() {
            failNextValidation.set(true);
        }

        @Override
        public Optional<PublicationLease> tryAcquirePublicationLease(String name, long streamId) {
            if (streamId == blockedStreamId && blockNextAcquire.compareAndSet(true, false)) {
                blockedAcquireStarted.countDown();
                SequentialBlockingValidationMemoryCompactTaskManager.awaitUninterruptibly(
                        allowBlockedAcquire);
            }
            Optional<PublicationLease> acquired = super.tryAcquirePublicationLease(name, streamId);
            if (acquired.isPresent()) {
                acquisitionCount.incrementAndGet();
            }
            return acquired;
        }

        @Override
        public synchronized boolean validatePublicationLease(PublicationLease lease) {
            if (failNextValidation.compareAndSet(true, false)) {
                try {
                    if (!blockedAcquireStarted.await(5, TimeUnit.SECONDS)) {
                        throw new AssertionError("blocked publication lease acquire did not start");
                    }
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                    throw new AssertionError(
                            "Interrupted while waiting for blocked publication lease acquire",
                            interrupted);
                }
                throw new AssertionError("fatal publication validation failure");
            }
            return super.validatePublicationLease(lease);
        }

        @Override
        public synchronized boolean releasePublicationLease(PublicationLease lease) {
            boolean released = super.releasePublicationLease(lease);
            if (released) {
                releaseCount.incrementAndGet();
                if (lease.streamId() == blockedStreamId) {
                    lateRelease.countDown();
                }
            }
            return released;
        }

        private boolean awaitBlockedAcquire() throws InterruptedException {
            return blockedAcquireStarted.await(5, TimeUnit.SECONDS);
        }

        private void allowBlockedAcquire() {
            allowBlockedAcquire.countDown();
        }

        private boolean awaitLateRelease() throws InterruptedException {
            return lateRelease.await(5, TimeUnit.SECONDS);
        }

        private int acquisitionCount() {
            return acquisitionCount.get();
        }

        private int releaseCount() {
            return releaseCount.get();
        }
    }

    private static final class FatalBlockedPublisherMemoryCompactTaskManager
            extends MemoryCompactTaskManager {
        private final long blockedStreamId;
        private final long fatalStreamId;
        private final AtomicBoolean armed = new AtomicBoolean();
        private final AtomicBoolean blockNextValidation = new AtomicBoolean();
        private final AtomicBoolean failNextValidation = new AtomicBoolean();
        private final AtomicInteger releaseCount = new AtomicInteger();
        private final AtomicBoolean fatalLeaseReleased = new AtomicBoolean();
        private final CountDownLatch blockedValidationStarted = new CountDownLatch(1);
        private final CountDownLatch allowBlockedValidation = new CountDownLatch(1);
        private final CountDownLatch blockedLeaseReleased = new CountDownLatch(1);

        private FatalBlockedPublisherMemoryCompactTaskManager(
                long blockedStreamId, long fatalStreamId) {
            this.blockedStreamId = blockedStreamId;
            this.fatalStreamId = fatalStreamId;
        }

        private void armFatalScan() {
            blockNextValidation.set(true);
            failNextValidation.set(true);
            armed.set(true);
        }

        @Override
        public boolean validatePublicationLease(PublicationLease lease) {
            if (armed.get()
                    && lease.streamId() == blockedStreamId
                    && blockNextValidation.compareAndSet(true, false)) {
                blockedValidationStarted.countDown();
                SequentialBlockingValidationMemoryCompactTaskManager.awaitUninterruptibly(
                        allowBlockedValidation);
            }
            if (armed.get()
                    && lease.streamId() == fatalStreamId
                    && failNextValidation.compareAndSet(true, false)) {
                try {
                    if (!blockedValidationStarted.await(5, TimeUnit.SECONDS)) {
                        throw new AssertionError("blocked publication validation did not start");
                    }
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                    throw new AssertionError(
                            "Interrupted while waiting for blocked publication validation",
                            interrupted);
                }
                throw new AssertionError("fatal publication validation failure");
            }
            return super.validatePublicationLease(lease);
        }

        @Override
        public synchronized boolean releasePublicationLease(PublicationLease lease) {
            boolean released = super.releasePublicationLease(lease);
            if (released) {
                releaseCount.incrementAndGet();
                if (lease.streamId() == blockedStreamId) {
                    blockedLeaseReleased.countDown();
                } else if (lease.streamId() == fatalStreamId) {
                    fatalLeaseReleased.set(true);
                }
            }
            return released;
        }

        private boolean awaitBlockedValidation() throws InterruptedException {
            return blockedValidationStarted.await(5, TimeUnit.SECONDS);
        }

        private void allowBlockedValidation() {
            allowBlockedValidation.countDown();
        }

        private boolean awaitBlockedLeaseRelease() throws InterruptedException {
            return blockedLeaseReleased.await(5, TimeUnit.SECONDS);
        }

        private boolean fatalLeaseReleased() {
            return fatalLeaseReleased.get();
        }

        private int releaseCount() {
            return releaseCount.get();
        }
    }

    private static final class BlockingValidationMemoryCompactTaskManager
            extends MemoryCompactTaskManager {
        private final long blockedStreamId;
        private final AtomicBoolean blockNextValidation = new AtomicBoolean();
        private final CountDownLatch blockedValidationStarted = new CountDownLatch(1);
        private final CountDownLatch allowBlockedValidation = new CountDownLatch(1);
        private final CountDownLatch blockedLeaseReleased = new CountDownLatch(1);

        private BlockingValidationMemoryCompactTaskManager(long blockedStreamId) {
            this.blockedStreamId = blockedStreamId;
        }

        private void blockNextValidation() {
            blockNextValidation.set(true);
        }

        @Override
        public boolean validatePublicationLease(PublicationLease lease) {
            if (lease.streamId() == blockedStreamId
                    && blockNextValidation.compareAndSet(true, false)) {
                blockedValidationStarted.countDown();
                boolean interrupted = false;
                while (allowBlockedValidation.getCount() > 0) {
                    try {
                        allowBlockedValidation.await();
                    } catch (InterruptedException ignored) {
                        interrupted = true;
                    }
                }
                if (interrupted) {
                    Thread.currentThread().interrupt();
                }
            }
            return super.validatePublicationLease(lease);
        }

        @Override
        public synchronized boolean releasePublicationLease(PublicationLease lease) {
            boolean released = super.releasePublicationLease(lease);
            if (released && lease.streamId() == blockedStreamId) {
                blockedLeaseReleased.countDown();
            }
            return released;
        }

        private boolean awaitBlockedValidation() throws InterruptedException {
            return blockedValidationStarted.await(5, TimeUnit.SECONDS);
        }

        private void allowBlockedValidation() {
            allowBlockedValidation.countDown();
        }

        private boolean awaitBlockedLeaseRelease() throws InterruptedException {
            return blockedLeaseReleased.await(5, TimeUnit.SECONDS);
        }
    }

    private static final class MultiBlockingValidationMemoryCompactTaskManager
            extends MemoryCompactTaskManager {
        private final Set<Long> blockedStreamIds;
        private final AtomicBoolean armed = new AtomicBoolean();
        private final AtomicInteger validationCount = new AtomicInteger();
        private final CountDownLatch expectedValidations;
        private final CountDownLatch allowValidations = new CountDownLatch(1);

        private MultiBlockingValidationMemoryCompactTaskManager(
                Set<Long> blockedStreamIds, int expectedValidationCount) {
            this.blockedStreamIds = Set.copyOf(blockedStreamIds);
            expectedValidations = new CountDownLatch(expectedValidationCount);
        }

        private void arm() {
            armed.set(true);
        }

        @Override
        public boolean validatePublicationLease(PublicationLease lease) {
            if (armed.get() && blockedStreamIds.contains(lease.streamId())) {
                validationCount.incrementAndGet();
                expectedValidations.countDown();
                SequentialBlockingValidationMemoryCompactTaskManager.awaitUninterruptibly(
                        allowValidations);
            }
            return super.validatePublicationLease(lease);
        }

        private boolean awaitExpectedValidations() throws InterruptedException {
            return expectedValidations.await(5, TimeUnit.SECONDS);
        }

        private void allowValidations() {
            allowValidations.countDown();
        }

        private int validationCount() {
            return validationCount.get();
        }
    }

    private static final class LateFatalBlockingValidationMemoryCompactTaskManager
            extends MemoryCompactTaskManager {
        private final long blockedStreamId;
        private final AtomicBoolean failNextValidation = new AtomicBoolean();
        private final AtomicInteger validationCount = new AtomicInteger();
        private final AtomicInteger releaseCount = new AtomicInteger();
        private final CountDownLatch expectedReleases;
        private final CountDownLatch blockedValidationStarted = new CountDownLatch(1);
        private final CountDownLatch allowBlockedValidation = new CountDownLatch(1);

        private LateFatalBlockingValidationMemoryCompactTaskManager(
                long blockedStreamId, int expectedReleaseCount) {
            this.blockedStreamId = blockedStreamId;
            expectedReleases = new CountDownLatch(expectedReleaseCount);
        }

        private void arm() {
            failNextValidation.set(true);
        }

        @Override
        public boolean validatePublicationLease(PublicationLease lease) {
            if (lease.streamId() == blockedStreamId
                    && failNextValidation.compareAndSet(true, false)) {
                validationCount.incrementAndGet();
                blockedValidationStarted.countDown();
                SequentialBlockingValidationMemoryCompactTaskManager.awaitUninterruptibly(
                        allowBlockedValidation);
                throw new AssertionError("late fatal publication validation failure");
            }
            return super.validatePublicationLease(lease);
        }

        @Override
        public synchronized boolean releasePublicationLease(PublicationLease lease) {
            boolean released = super.releasePublicationLease(lease);
            if (released) {
                releaseCount.incrementAndGet();
                expectedReleases.countDown();
            }
            return released;
        }

        private boolean awaitBlockedValidation() throws InterruptedException {
            return blockedValidationStarted.await(5, TimeUnit.SECONDS);
        }

        private void allowBlockedValidation() {
            allowBlockedValidation.countDown();
        }

        private boolean awaitExpectedReleases() throws InterruptedException {
            return expectedReleases.await(5, TimeUnit.SECONDS);
        }

        private int validationCount() {
            return validationCount.get();
        }

        private int releaseCount() {
            return releaseCount.get();
        }
    }

    private static final class SequentialBlockingValidationMemoryCompactTaskManager
            extends MemoryCompactTaskManager {
        private final long firstStreamId;
        private final long secondStreamId;
        private final AtomicBoolean armed = new AtomicBoolean();
        private final AtomicInteger releaseCount = new AtomicInteger();
        private final CountDownLatch firstValidationStarted = new CountDownLatch(1);
        private final CountDownLatch allowFirstValidation = new CountDownLatch(1);
        private final CountDownLatch secondValidationStarted = new CountDownLatch(1);
        private final CountDownLatch allowSecondValidation = new CountDownLatch(1);

        private SequentialBlockingValidationMemoryCompactTaskManager(
                long firstStreamId, long secondStreamId) {
            this.firstStreamId = firstStreamId;
            this.secondStreamId = secondStreamId;
        }

        private void arm() {
            armed.set(true);
        }

        @Override
        public boolean validatePublicationLease(PublicationLease lease) {
            if (armed.get() && lease.streamId() == firstStreamId) {
                firstValidationStarted.countDown();
                awaitUninterruptibly(allowFirstValidation);
            } else if (armed.get() && lease.streamId() == secondStreamId) {
                secondValidationStarted.countDown();
                awaitUninterruptibly(allowSecondValidation);
            }
            return super.validatePublicationLease(lease);
        }

        @Override
        public synchronized boolean releasePublicationLease(PublicationLease lease) {
            boolean released = super.releasePublicationLease(lease);
            if (released) {
                releaseCount.incrementAndGet();
            }
            return released;
        }

        private static void awaitUninterruptibly(CountDownLatch latch) {
            boolean interrupted = false;
            while (latch.getCount() > 0) {
                try {
                    latch.await();
                } catch (InterruptedException ignored) {
                    interrupted = true;
                }
            }
            if (interrupted) {
                Thread.currentThread().interrupt();
            }
        }

        private boolean awaitFirstValidation() throws InterruptedException {
            return firstValidationStarted.await(5, TimeUnit.SECONDS);
        }

        private void allowFirstValidation() {
            allowFirstValidation.countDown();
        }

        private boolean awaitSecondValidation() throws InterruptedException {
            return secondValidationStarted.await(5, TimeUnit.SECONDS);
        }

        private void allowSecondValidation() {
            allowSecondValidation.countDown();
        }

        private int releaseCount() {
            return releaseCount.get();
        }
    }

    private static final class BlockingReleaseMemoryCompactTaskManager
            extends MemoryCompactTaskManager {
        private final long blockedStreamId;
        private final long healthyStreamId;
        private final CompletableFuture<Boolean> blockedRelease = new CompletableFuture<>();
        private final AtomicBoolean healthyLeaseReleased = new AtomicBoolean();
        private final CountDownLatch blockedReleaseStarted = new CountDownLatch(1);
        private final CountDownLatch blockedReleaseCompleted = new CountDownLatch(1);
        private volatile PublicationLease blockedLease;

        private BlockingReleaseMemoryCompactTaskManager(
                long blockedStreamId, long healthyStreamId) {
            this.blockedStreamId = blockedStreamId;
            this.healthyStreamId = healthyStreamId;
            blockedRelease.whenComplete((ignored, failure) -> blockedReleaseCompleted.countDown());
        }

        @Override
        public CompletableFuture<Boolean> releasePublicationLeaseAsync(PublicationLease lease) {
            if (lease.streamId() == blockedStreamId) {
                blockedLease = lease;
                blockedReleaseStarted.countDown();
                return blockedRelease;
            }
            boolean released = super.releasePublicationLease(lease);
            if (released && lease.streamId() == healthyStreamId) {
                healthyLeaseReleased.set(true);
            }
            return CompletableFuture.completedFuture(released);
        }

        private boolean awaitBlockedRelease() throws InterruptedException {
            return blockedReleaseStarted.await(5, TimeUnit.SECONDS);
        }

        private boolean healthyLeaseReleased() {
            return healthyLeaseReleased.get();
        }

        private void completeBlockedRelease() {
            PublicationLease lease = blockedLease;
            if (lease == null) {
                throw new IllegalStateException("Blocked release was not started");
            }
            blockedRelease.complete(super.releasePublicationLease(lease));
        }

        private boolean awaitBlockedReleaseCompletion() throws InterruptedException {
            return blockedReleaseCompleted.await(5, TimeUnit.SECONDS);
        }
    }

    private static final class LegacyCursorMemoryCompactTaskManager extends MemoryCompactTaskManager {
        private final AtomicInteger acquisitionCount = new AtomicInteger();
        private final AtomicInteger releaseCount = new AtomicInteger();
        private String legacyPublicationName;
        private long legacyStreamId;
        private long legacyOffset;

        private synchronized void seedLegacyCursor(String name, long streamId, long offset) {
            legacyPublicationName = name;
            legacyStreamId = streamId;
            legacyOffset = offset;
        }

        @Override
        public synchronized boolean repairLegacyPublishedOffset(PublicationLease lease) {
            if (lease.name().equals(legacyPublicationName) && lease.streamId() == legacyStreamId) {
                throw new LegacyPublishedOffsetException(
                        lease.name(), lease.streamId(), legacyOffset,
                        "test fixture has no durable prepared task");
            }
            return false;
        }

        @Override
        public synchronized void updatePublishedOffset(
                String name, long streamId, long offset, long cumulativeSize) {
            super.updatePublishedOffset(name, streamId, offset, cumulativeSize);
            if (name.equals(legacyPublicationName) && streamId == legacyStreamId
                    && offset >= 0 && cumulativeSize > 0) {
                legacyPublicationName = null;
            }
        }

        @Override
        public synchronized Optional<PublicationLease> tryAcquirePublicationLease(
                String name, long streamId) {
            Optional<PublicationLease> acquired = super.tryAcquirePublicationLease(name, streamId);
            if (acquired.isPresent()) {
                acquisitionCount.incrementAndGet();
            }
            return acquired;
        }

        @Override
        public synchronized boolean releasePublicationLease(PublicationLease lease) {
            boolean released = super.releasePublicationLease(lease);
            if (released) {
                releaseCount.incrementAndGet();
            }
            return released;
        }

        private int acquisitionCount() {
            return acquisitionCount.get();
        }

        private int releaseCount() {
            return releaseCount.get();
        }
    }

    private static final class RecoveryFailureMemoryCompactTaskManager
            extends MemoryCompactTaskManager {
        private final AtomicInteger acquisitionCount = new AtomicInteger();
        private final AtomicInteger releaseCount = new AtomicInteger();
        private final String publicationName;
        private final PreparedTaskClaim preparedTaskClaim;

        private RecoveryFailureMemoryCompactTaskManager(String publicationName, long streamId) {
            this.publicationName = publicationName;
            PreparedCompactStreamTask task = new PreparedCompactStreamTask(
                    streamId,
                    0L,
                    10L,
                    11L,
                    10L,
                    PreparedCompactStreamTask.INIT,
                    "invalid-prepared-task",
                    publicationName,
                    Map.of());
            this.preparedTaskClaim = new PreparedTaskClaim(task, 1L);
        }

        @Override
        public synchronized Optional<PublicationLease> tryAcquirePublicationLease(
                String name, long acquiredStreamId) {
            Optional<PublicationLease> acquired =
                    super.tryAcquirePublicationLease(name, acquiredStreamId);
            if (acquired.isPresent()) {
                acquisitionCount.incrementAndGet();
            }
            return acquired;
        }

        @Override
        public synchronized Optional<PreparedTaskClaim> getPreparedTaskClaim(String name) {
            if (publicationName.equals(name)) {
                return Optional.of(preparedTaskClaim);
            }
            return Optional.empty();
        }

        @Override
        public synchronized boolean releasePublicationLease(PublicationLease lease) {
            boolean released = super.releasePublicationLease(lease);
            if (released) {
                releaseCount.incrementAndGet();
            }
            return released;
        }

        private int acquisitionCount() {
            return acquisitionCount.get();
        }

        private int releaseCount() {
            return releaseCount.get();
        }
    }

    private static final class FlakyReleaseMemoryCompactTaskManager extends MemoryCompactTaskManager {
        private final AtomicBoolean failNextRelease = new AtomicBoolean(true);
        private final CountDownLatch releaseRetried = new CountDownLatch(1);

        @Override
        public boolean releasePublicationLease(PublicationLease lease) {
            if (failNextRelease.compareAndSet(true, false)) {
                throw new IllegalStateException("temporary lease release failure");
            }
            boolean released = super.releasePublicationLease(lease);
            releaseRetried.countDown();
            return released;
        }
    }

    private static final class FlakyClaimMemoryCompactTaskManager extends MemoryCompactTaskManager {
        private final AtomicBoolean failNextClaim = new AtomicBoolean(true);
        private final AtomicBoolean failNextRelease = new AtomicBoolean(true);
        private final CountDownLatch releaseRetried = new CountDownLatch(1);

        @Override
        public synchronized PublishedOffsetClaim claimPublishedOffset(PublicationLease lease) {
            if (failNextClaim.compareAndSet(true, false)) {
                throw new IllegalStateException("temporary cursor claim failure");
            }
            return super.claimPublishedOffset(lease);
        }

        @Override
        public synchronized boolean releasePublicationLease(PublicationLease lease) {
            if (failNextRelease.compareAndSet(true, false)) {
                throw new IllegalStateException("temporary lease release failure");
            }
            boolean released = super.releasePublicationLease(lease);
            releaseRetried.countDown();
            return released;
        }
    }

    private static List<CompactStreamTask> tasks(MemoryCompactTaskManager manager) {
        return manager.getAllTasks().join().stream()
                .flatMap(packaged -> packaged.getSubTasks().stream())
                .distinct()
                .map(name -> manager.getCompactStreamTask(name).join())
                .toList();
    }

    private static CompactStreamTask taskFor(List<CompactStreamTask> tasks, long streamId) {
        return tasks.stream().filter(task -> task.getStreamId() == streamId).findFirst().orElseThrow();
    }

    private static MutableStream stream(String name,
                                        Map<String, String> properties,
                                        MutablePartition... partitions) {
        return new MutableStream(StreamIdentifier.of(NAMESPACE, name), properties, List.of(partitions));
    }

    private static MutablePartition partition(long id, LogOffset lastOffset) {
        return new MutablePartition(LogId.of(id), lastOffset);
    }

    private static final class MutableCatalog {
        private final List<MutableStream> streams = new ArrayList<>();
        private final Set<StreamIdentifier> failedStreams = new HashSet<>();
        private final AtomicInteger listNamespacesCalls = new AtomicInteger();
        private final AtomicBoolean namespaceListingFails = new AtomicBoolean();
        private final StreamCatalog catalog = proxy(StreamCatalog.class, (method, args) -> switch (method) {
            case "listNamespaces" -> {
                listNamespacesCalls.incrementAndGet();
                yield namespaceListingFails.get()
                        ? CompletableFuture.failedFuture(
                                new IllegalStateException("temporary namespace listing failure"))
                        : CompletableFuture.completedFuture(streams.isEmpty()
                                ? List.of()
                                : List.of(new Namespace(NAMESPACE)));
            }
            case "listStreams" -> CompletableFuture.completedFuture(
                    streams.stream().map(MutableStream::identifier).toList());
            case "loadStream" -> failedStreams.contains(args[0])
                    ? CompletableFuture.failedFuture(
                            new IllegalStateException("temporary catalog failure for " + args[0]))
                    : CompletableFuture.completedFuture(streams.stream()
                            .filter(stream -> stream.identifier().equals(args[0]))
                            .findFirst()
                            .orElseThrow()
                            .mock());
            case "close" -> null;
            default -> throw new UnsupportedOperationException(method);
        });

        private MutableCatalog() {
        }

        private StreamCatalog catalog() {
            return catalog;
        }

        private void add(MutableStream stream) {
            streams.add(stream);
        }

        private void clear() {
            streams.clear();
        }

        private void fail(StreamIdentifier identifier) {
            failedStreams.add(identifier);
        }

        private void restore(StreamIdentifier identifier) {
            failedStreams.remove(identifier);
        }

        private int listNamespacesCalls() {
            return listNamespacesCalls.get();
        }

        private void failNamespaceListing() {
            namespaceListingFails.set(true);
        }
    }

    private static final class MutableStream {
        private final StreamIdentifier identifier;
        private final Map<String, String> properties;
        private volatile List<MutablePartition> partitions;
        private final StreamLayout layout;
        private final Stream stream;

        private MutableStream(StreamIdentifier identifier,
                              Map<String, String> properties,
                              List<MutablePartition> partitions) {
            this.identifier = identifier;
            this.properties = properties;
            this.partitions = partitions;
            this.layout = proxy(StreamLayout.class, (method, args) -> switch (method) {
                case "logIds" -> CompletableFuture.completedFuture(
                        this.partitions.stream().map(MutablePartition::id).toList());
                default -> throw new UnsupportedOperationException(method);
            });
            this.stream = proxy(Stream.class, (method, args) -> switch (method) {
                case "state" -> LifecycleState.ACTIVE;
                case "properties" -> this.properties;
                case "layout" -> layout;
                case "getLog" -> this.partitions.stream()
                        .filter(partition -> partition.id().equals(args[0]))
                        .findFirst()
                        .orElseThrow()
                        .mock();
                case "close" -> null;
                default -> throw new UnsupportedOperationException(method);
            });
        }

        private StreamIdentifier identifier() {
            return identifier;
        }

        private Stream mock() {
            return stream;
        }

        private void partitions(List<MutablePartition> partitions) {
            this.partitions = partitions;
        }
    }

    private static final class MutablePartition {
        private final LogId id;
        private volatile LogOffset lastOffset;
        private final Map<Long, EntryHeader> metadata = new java.util.concurrent.ConcurrentHashMap<>();
        private final Log log;

        private MutablePartition(LogId id, LogOffset lastOffset) {
            this.id = id;
            this.lastOffset = lastOffset;
            this.log = proxy(Log.class, (method, args) -> switch (method) {
                case "getLastOffset" -> CompletableFuture.completedFuture(this.lastOffset);
                case "getEntryMetadata" -> CompletableFuture.completedFuture(metadata.get((Long) args[0]));
                case "close" -> null;
                default -> throw new UnsupportedOperationException(method);
            });
        }

        private LogId id() {
            return id;
        }

        private Log mock() {
            return log;
        }

        private void lastOffset(LogOffset lastOffset) {
            this.lastOffset = lastOffset;
        }

        private void metadata(long offset, EntryHeader header) {
            metadata.put(offset, header);
        }

        private void removeMetadata(long offset) {
            metadata.remove(offset);
        }
    }

    @FunctionalInterface
    private interface MethodHandler {
        Object invoke(String method, Object[] args) throws Throwable;
    }

    private static <T> T proxy(Class<T> type, MethodHandler handler) {
        Object proxy = java.lang.reflect.Proxy.newProxyInstance(
                type.getClassLoader(),
                new Class<?>[]{type},
                (ignored, method, args) -> {
                    if (method.getDeclaringClass() == Object.class) {
                        return switch (method.getName()) {
                            case "toString" -> type.getSimpleName() + "Proxy";
                            case "hashCode" -> System.identityHashCode(ignored);
                            case "equals" -> ignored == args[0];
                            default -> throw new UnsupportedOperationException(method.getName());
                        };
                    }
                    return handler.invoke(method.getName(), args == null ? new Object[0] : args);
                });
        return type.cast(proxy);
    }
}
