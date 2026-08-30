/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.lakehouse.compact;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
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
import io.lakestream.ursa.compaction.metrics.CompactionMetrics;
import io.lakestream.ursa.compaction.task.CompactStreamTask;
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
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class PublishCompactTaskRunnerOffsetTest {

    private static final String NAMESPACE = "default";

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

    private static PublishCompactTaskRunner runner(StreamCatalog catalog,
                                                   MemoryCompactTaskManager tasks,
                                                   Properties properties) {
        StorageConfig config = new StorageConfig();
        config.setProperties(properties);
        return runner(catalog, tasks, config);
    }

    private static PublishCompactTaskRunner runner(StreamCatalog catalog,
                                                   MemoryCompactTaskManager tasks,
                                                   StorageConfig config) {
        return runner(
                catalog,
                tasks,
                config,
                CompactionMetrics.NOOP,
                Executors.newSingleThreadScheduledExecutor());
    }

    private static PublishCompactTaskRunner runner(StreamCatalog catalog,
                                                   MemoryCompactTaskManager tasks,
                                                   StorageConfig config,
                                                   CompactionMetrics metrics,
                                                   ScheduledExecutorService publishExecutor) {
        return new PublishCompactTaskRunner(
                catalog,
                tasks,
                Executors.newSingleThreadExecutor(),
                publishExecutor,
                config,
                metrics);
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
