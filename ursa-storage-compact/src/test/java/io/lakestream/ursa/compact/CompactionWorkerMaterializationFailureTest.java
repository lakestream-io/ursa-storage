/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.compact;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.lakestream.api.LogId;
import io.lakestream.api.Stream;
import io.lakestream.api.StreamCatalog;
import io.lakestream.api.StreamIdentifier;
import io.lakestream.api.StreamLayout;
import io.lakestream.api.exception.NoSuchStreamException;
import io.lakestream.api.exception.PartitionLifecycleFencedException;
import io.lakestream.api.exception.StreamPermanentlyDeletedException;
import io.lakestream.api.materialization.ResolvedMaterialization;
import io.lakestream.api.materialization.TableCatalog;
import io.lakestream.api.materialization.TableCatalogType;
import io.lakestream.api.materialization.TableIdentifier;
import io.lakestream.api.materialization.TableMaterializationPolicy;
import io.lakestream.ursa.compaction.CompactTaskManager;
import io.lakestream.ursa.compaction.metrics.CompactionMetrics;
import io.lakestream.ursa.compaction.task.CompactStreamTask;
import io.lakestream.ursa.compaction.task.PackagedCompactStreamTask;
import io.lakestream.ursa.exception.ExceptionCode;
import io.lakestream.ursa.materialization.MaterializationException;
import io.lakestream.ursa.materialization.MaterializationService;
import io.lakestream.ursa.materialization.MaterializationTask;
import io.lakestream.ursa.storage.impl.StorageConfig;
import io.lakestream.ursa.storage.impl.compaction.CompactionService;
import io.lakestream.ursa.storage.impl.compaction.CompactionTaskProviderV2;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

/**
 * T10 smoke test for the sink-neutral failure path.
 *
 * <p>When {@link MaterializationService#materialize(MaterializationTask)} throws a
 * {@link MaterializationException} with a non-retryable {@link ExceptionCode}, the worker must
 * call {@link MaterializationService#invalidate(io.lakestream.api.StreamIdentifier)}
 * so the sink can drop cached writer state.
 */
@ExtendWith(MockitoExtension.class)
public class CompactionWorkerMaterializationFailureTest {

    @Mock
    private CompactTaskManager compactTaskManager;

    @Mock
    private CompactionService compactionService;

    @Mock
    private CompactionTaskProviderV2 compactionTaskProvider;

    @Mock
    private MaterializationService materializationService;

    @Mock
    private StreamCatalog streamCatalog;

    @Mock
    private Stream stream;

    @Mock
    private StreamLayout streamLayout;

    private CompactionWorker createWorker() {
        return createWorker(true);
    }

    private CompactionWorker createWorker(boolean materializationEnabled) {
        StorageConfig config = StorageConfig.builder()
                .retryableQuarantineInSeconds(10)
                .nonRetryableQuarantineInSeconds(60)
                .refreshLocalTaskIntervalInSeconds(5)
                .materializationEnabled(materializationEnabled)
                .blackTopicOfCompact(new HashSet<>())
                .build();
        return new CompactionWorker(compactTaskManager, compactionService,
                materializationService, streamCatalog,
                compactionTaskProvider, config, CompactionMetrics.NOOP);
    }

    @Test
    public void invalidateCalledOnNonRetryableMaterializationFailure() throws Exception {
        CompactionWorker worker = createWorker();

        String topic = "default/failure-topic-partition-0";
        CompactStreamTask task = new CompactStreamTask();
        task.setTopic(topic);
        task.setStatus(CompactStreamTask.INIT);
        task.setStartOffset(0L);
        task.setEndOffset(5L);

        PackagedCompactStreamTask packagedTask = new PackagedCompactStreamTask();
        packagedTask.setTaskName("task-failure");
        packagedTask.setSubTasks(List.of("sub-1"));

        when(compactionTaskProvider.getTask())
                .thenReturn(packagedTask)
                .thenReturn(null);
        when(compactTaskManager.getCompactStreamTask("sub-1"))
                .thenReturn(CompletableFuture.completedFuture(task));
        when(compactionTaskProvider.getQuarantinedTopic(topic)).thenReturn(null);
        when(compactTaskManager.tryLockTask(packagedTask.getTaskName())).thenReturn(true);

        TableCatalog catalog = new TableCatalog("delta-cat", TableCatalogType.DELTA, Map.of(), Map.of());
        ResolvedMaterialization resolved = new ResolvedMaterialization(
                catalog,
                new TableIdentifier("ns", "table"),
                TableMaterializationPolicy.empty());
        when(streamCatalog.registerExternalPartition(any(), anyInt(), anyLong(), any()))
                .thenReturn(CompletableFuture.completedFuture(null));
        stubExternalStream(task);
        when(stream.effectiveMaterialization()).thenReturn(Optional.of(resolved));

        doThrow(new MaterializationException(ExceptionCode.INTERNAL_ERROR,
                "kaboom"))
                .when(materializationService).materialize(any(MaterializationTask.class));

        Thread thread = new Thread(worker);
        thread.start();
        Thread.sleep(500);
        thread.interrupt();
        thread.join(2000);

        verify(materializationService, atLeastOnce()).invalidate(argThat(id ->
                id.namespace().equals("default")
                        && id.name().equals("failure-topic")));
    }

    @Test
    public void noSuchLogMaterializationFailureDeletesTerminalTask() throws Exception {
        CompactionWorker worker = createWorker();
        String topic = "default/recreated-topic-partition-0";
        CompactStreamTask task = new CompactStreamTask();
        task.setTopic(topic);
        task.setTaskName("terminal-task");
        task.setStatus(CompactStreamTask.INIT);
        task.setStartOffset(0L);
        task.setEndOffset(5L);

        PackagedCompactStreamTask packagedTask = new PackagedCompactStreamTask();
        packagedTask.setTaskName("terminal-package");
        packagedTask.setSubTasks(List.of("sub-terminal"));
        when(compactionTaskProvider.getTask()).thenReturn(packagedTask).thenReturn(null);
        when(compactTaskManager.getCompactStreamTask("sub-terminal"))
                .thenReturn(CompletableFuture.completedFuture(task));
        when(compactionTaskProvider.getQuarantinedTopic(topic)).thenReturn(null);
        when(compactTaskManager.tryLockTask(packagedTask.getTaskName())).thenReturn(true);
        CountDownLatch taskDeleted = new CountDownLatch(1);
        when(compactTaskManager.deleteCompactTask(task)).thenAnswer(invocation -> {
            taskDeleted.countDown();
            return CompletableFuture.completedFuture(null);
        });
        when(streamCatalog.registerExternalPartition(any(), anyInt(), anyLong(), any()))
                .thenReturn(CompletableFuture.completedFuture(null));
        stubExternalStream(task);
        when(stream.effectiveMaterialization()).thenReturn(Optional.of(new ResolvedMaterialization(
                new TableCatalog("delta-cat", TableCatalogType.DELTA, Map.of(), Map.of()),
                new TableIdentifier("ns", "table"),
                TableMaterializationPolicy.empty())));
        doThrow(new MaterializationException(ExceptionCode.NO_SUCH_LOG,
                "Kafka topic incarnation mismatch"))
                .when(materializationService).materialize(any(MaterializationTask.class));

        Thread thread = new Thread(worker);
        thread.start();
        org.junit.jupiter.api.Assertions.assertTrue(taskDeleted.await(10, TimeUnit.SECONDS));
        thread.interrupt();
        thread.join(2000);

        verify(compactTaskManager).deleteCompactTask(task);
        verify(compactionTaskProvider, never()).quarantineTopic(any(), anyLong());
    }

    @Test
    public void permanentlyDeletedRegistrationDeletesTerminalTask() throws Exception {
        StreamIdentifier id = new StreamIdentifier("default", "deleted-topic");
        runCatalogFailureAndAssertTaskDeleted(
            new StreamPermanentlyDeletedException(id), true);
    }

    @Test
    public void partitionLifecycleFenceDeletesTerminalTask() throws Exception {
        StreamIdentifier id = new StreamIdentifier("default", "fenced-topic");
        runCatalogFailureAndAssertTaskDeleted(
            new PartitionLifecycleFencedException(
                id, 0, "partition was deleted in this registration generation"), true);
    }

    @Test
    public void staleNativeLogIdentityDeletesTerminalTask() throws Exception {
        CompactionWorker worker = createWorker();
        String topic = "default/stale-native-partition-0";
        CompactStreamTask task = new CompactStreamTask();
        task.setTopic(topic);
        task.setTaskName("stale-native-task");
        task.setStatus(CompactStreamTask.INIT);
        task.setStreamId(41L);
        task.setStartOffset(0L);
        task.setEndOffset(5L);
        PackagedCompactStreamTask packagedTask = new PackagedCompactStreamTask();
        packagedTask.setTaskName("stale-native-package");
        packagedTask.setSubTasks(List.of("stale-native-subtask"));
        when(compactionTaskProvider.getTask()).thenReturn(packagedTask).thenReturn(null);
        when(compactTaskManager.getCompactStreamTask("stale-native-subtask"))
                .thenReturn(CompletableFuture.completedFuture(task));
        when(compactionTaskProvider.getQuarantinedTopic(topic)).thenReturn(null);
        when(compactTaskManager.tryLockTask(packagedTask.getTaskName())).thenReturn(true);
        when(streamCatalog.loadStream(any())).thenReturn(CompletableFuture.completedFuture(stream));
        when(stream.layout()).thenReturn(streamLayout);
        when(streamLayout.logIds())
                .thenReturn(CompletableFuture.completedFuture(List.of(LogId.of(42L))));
        CountDownLatch taskDeleted = new CountDownLatch(1);
        when(compactTaskManager.deleteCompactTask(task)).thenAnswer(invocation -> {
            taskDeleted.countDown();
            return CompletableFuture.completedFuture(null);
        });

        Thread thread = new Thread(worker);
        thread.start();
        assertTrue(taskDeleted.await(10, TimeUnit.SECONDS));
        thread.interrupt();
        thread.join(2_000L);

        verify(compactTaskManager).deleteCompactTask(task);
        verify(compactionTaskProvider, never()).quarantineTopic(any(), anyLong());
        verify(materializationService, never()).materialize(any());
        verify(stream).close();
    }

    @Test
    public void missingStreamDuringLoadKeepsTaskForRetry() throws Exception {
        StreamIdentifier id = new StreamIdentifier("default", "missing-topic");
        runCatalogFailureAndAssertTaskRetained(new NoSuchStreamException(id));
    }

    private void runCatalogFailureAndAssertTaskDeleted(
            RuntimeException catalogFailure, boolean failRegistration) throws Exception {
        CompactionWorker worker = createWorker();
        String topic = "default/catalog-failure-topic-partition-0";
        CompactStreamTask task = new CompactStreamTask();
        task.setTopic(topic);
        task.setTaskName("catalog-failure-task");
        task.setStatus(CompactStreamTask.INIT);
        task.setStreamId(17L);
        task.setStartOffset(0L);
        task.setEndOffset(5L);

        PackagedCompactStreamTask packagedTask = new PackagedCompactStreamTask();
        packagedTask.setTaskName("catalog-failure-package");
        packagedTask.setSubTasks(List.of("catalog-failure-subtask"));
        when(compactionTaskProvider.getTask()).thenReturn(packagedTask).thenReturn(null);
        when(compactTaskManager.getCompactStreamTask("catalog-failure-subtask"))
            .thenReturn(CompletableFuture.completedFuture(task));
        when(compactionTaskProvider.getQuarantinedTopic(topic)).thenReturn(null);
        when(compactTaskManager.tryLockTask(packagedTask.getTaskName())).thenReturn(true);

        StreamIdentifier id = CompactionWorker.toStreamIdentifier(topic);
        if (failRegistration) {
            when(streamCatalog.loadStream(any()))
                .thenReturn(CompletableFuture.failedFuture(new NoSuchStreamException(id)));
            when(streamCatalog.registerExternalPartition(any(), anyInt(), anyLong(), any()))
                .thenReturn(CompletableFuture.failedFuture(catalogFailure));
        } else {
            when(streamCatalog.registerExternalPartition(any(), anyInt(), anyLong(), any()))
                .thenReturn(CompletableFuture.completedFuture(null));
            when(streamCatalog.loadStream(any()))
                .thenReturn(CompletableFuture.failedFuture(new NoSuchStreamException(id)))
                .thenReturn(CompletableFuture.failedFuture(catalogFailure));
        }

        CountDownLatch taskDeleted = new CountDownLatch(1);
        when(compactTaskManager.deleteCompactTask(task)).thenAnswer(invocation -> {
            taskDeleted.countDown();
            return CompletableFuture.completedFuture(null);
        });

        Thread thread = new Thread(worker);
        thread.start();
        assertTrue(taskDeleted.await(10, TimeUnit.SECONDS));
        thread.interrupt();
        thread.join(2_000L);

        verify(compactTaskManager).deleteCompactTask(task);
        verify(compactionTaskProvider, never()).quarantineTopic(any(), anyLong());
        verify(materializationService, never()).materialize(any());
    }

    private void runCatalogFailureAndAssertTaskRetained(
            RuntimeException catalogFailure) throws Exception {
        CompactionWorker worker = createWorker();
        String topic = "default/catalog-retry-topic-partition-0";
        CompactStreamTask task = new CompactStreamTask();
        task.setTopic(topic);
        task.setTaskName("catalog-retry-task");
        task.setStatus(CompactStreamTask.INIT);
        task.setStreamId(18L);
        task.setStartOffset(0L);
        task.setEndOffset(5L);

        PackagedCompactStreamTask packagedTask = new PackagedCompactStreamTask();
        packagedTask.setTaskName("catalog-retry-package");
        packagedTask.setSubTasks(List.of("catalog-retry-subtask"));
        when(compactionTaskProvider.getTask()).thenReturn(packagedTask).thenReturn(null);
        when(compactTaskManager.getCompactStreamTask("catalog-retry-subtask"))
            .thenReturn(CompletableFuture.completedFuture(task));
        when(compactionTaskProvider.getQuarantinedTopic(topic)).thenReturn(null);
        when(compactTaskManager.tryLockTask(packagedTask.getTaskName())).thenReturn(true);
        when(streamCatalog.registerExternalPartition(any(), anyInt(), anyLong(), any()))
            .thenReturn(CompletableFuture.completedFuture(null));
        StreamIdentifier id = CompactionWorker.toStreamIdentifier(topic);
        when(streamCatalog.loadStream(any()))
            .thenReturn(CompletableFuture.failedFuture(new NoSuchStreamException(id)))
            .thenReturn(CompletableFuture.failedFuture(catalogFailure));
        CountDownLatch quarantined = new CountDownLatch(1);
        doAnswer(invocation -> {
            quarantined.countDown();
            return null;
        }).when(compactionTaskProvider).quarantineTopic(any(), anyLong());

        Thread thread = new Thread(worker);
        thread.start();
        assertTrue(quarantined.await(10, TimeUnit.SECONDS));
        thread.interrupt();
        thread.join(2_000L);

        verify(compactTaskManager, never()).deleteCompactTask(task);
        verify(compactionTaskProvider).quarantineTopic(eq(topic), anyLong());
        verify(materializationService, never()).materialize(any());
    }

    @Test
    public void interruptedTerminalTaskDeletionStopsWorker() throws Exception {
        CompactionWorker worker = createWorker();
        String topic = "default/interrupted-delete-partition-0";
        CompactStreamTask task = new CompactStreamTask();
        task.setTaskName("interrupted-terminal-task");
        task.setTopic(topic);
        task.setStatus(CompactStreamTask.INIT);
        task.setStartOffset(0L);
        task.setEndOffset(5L);
        PackagedCompactStreamTask packagedTask = new PackagedCompactStreamTask();
        packagedTask.setTaskName("interrupted-terminal-package");
        packagedTask.setSubTasks(List.of("sub-interrupted-terminal"));
        when(compactionTaskProvider.getTask()).thenReturn(packagedTask);
        when(compactTaskManager.getCompactStreamTask("sub-interrupted-terminal"))
                .thenReturn(CompletableFuture.completedFuture(task));
        when(compactionTaskProvider.getQuarantinedTopic(topic)).thenReturn(null);
        when(compactTaskManager.tryLockTask(packagedTask.getTaskName())).thenReturn(true);
        when(streamCatalog.registerExternalPartition(any(), anyInt(), anyLong(), any()))
                .thenReturn(CompletableFuture.completedFuture(null));
        stubExternalStream(task);
        when(stream.effectiveMaterialization()).thenReturn(Optional.of(new ResolvedMaterialization(
                new TableCatalog("delta-cat", TableCatalogType.DELTA, Map.of(), Map.of()),
                new TableIdentifier("ns", "table"),
                TableMaterializationPolicy.empty())));
        doThrow(new MaterializationException(ExceptionCode.NO_SUCH_LOG,
                "Kafka topic incarnation mismatch"))
                .when(materializationService).materialize(any(MaterializationTask.class));
        CompletableFuture<Void> pendingDeletion = new CompletableFuture<>();
        CountDownLatch deleteStarted = new CountDownLatch(1);
        when(compactTaskManager.deleteCompactTask(task)).thenAnswer(invocation -> {
            deleteStarted.countDown();
            return pendingDeletion;
        });

        Thread runner = new Thread(worker);
        runner.start();
        assertTrue(deleteStarted.await(10, TimeUnit.SECONDS));
        runner.interrupt();
        runner.join(10_000L);

        assertFalse(runner.isAlive());
        assertTrue(runner.isInterrupted());
        verify(compactionTaskProvider, never()).quarantineTopic(any(), anyLong());
    }

    @Test
    public void interruptedMaterializationDisabledFallbackStopsWorker() throws Exception {
        CompactionWorker worker = createWorker(false);
        String topic = "default/legacy-interrupted-partition-0";
        CompactStreamTask task = new CompactStreamTask();
        task.setTaskName("legacy-interrupted-task");
        task.setTopic(topic);
        task.setStatus(CompactStreamTask.INIT);
        task.setStartOffset(0L);
        task.setEndOffset(5L);
        PackagedCompactStreamTask packagedTask = new PackagedCompactStreamTask();
        packagedTask.setTaskName("legacy-interrupted-package");
        packagedTask.setSubTasks(List.of("sub-legacy-interrupted"));
        when(compactionTaskProvider.getTask()).thenReturn(packagedTask);
        when(compactTaskManager.getCompactStreamTask("sub-legacy-interrupted"))
                .thenReturn(CompletableFuture.completedFuture(task));
        when(compactionTaskProvider.getQuarantinedTopic(topic)).thenReturn(null);
        when(compactTaskManager.tryLockTask(packagedTask.getTaskName())).thenReturn(true);
        CountDownLatch fallbackStarted = new CountDownLatch(1);
        CompletableFuture<Void> pendingFallback = new CompletableFuture<>();
        doAnswer(invocation -> {
            fallbackStarted.countDown();
            try {
                pendingFallback.get();
            } catch (InterruptedException interrupted) {
                // Mirrors the legacy LakehouseCompactionWorker terminal-task deletion path,
                // which restores the signal before propagating it to this runner.
                Thread.currentThread().interrupt();
                throw interrupted;
            }
            return null;
        }).when(compactionService).compactStream(task);

        Thread runner = new Thread(worker);
        runner.start();
        assertTrue(fallbackStarted.await(10, TimeUnit.SECONDS));
        runner.interrupt();
        runner.join(10_000L);

        assertFalse(runner.isAlive());
        assertTrue(runner.isInterrupted());
        verify(materializationService, never()).materialize(any());
    }

    private void stubExternalStream(CompactStreamTask task) {
        StreamIdentifier id = CompactionWorker.toStreamIdentifier(task.getTopic());
        when(streamCatalog.loadStream(any()))
                .thenReturn(CompletableFuture.failedFuture(new NoSuchStreamException(id)))
                .thenReturn(CompletableFuture.completedFuture(stream));
        when(stream.layout()).thenReturn(streamLayout);
        when(streamLayout.logIds())
                .thenReturn(CompletableFuture.completedFuture(List.of(LogId.of(task.getStreamId()))));
    }
}
