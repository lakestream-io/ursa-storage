/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.compact;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.lakestream.api.LogId;
import io.lakestream.api.Stream;
import io.lakestream.api.StreamCatalog;
import io.lakestream.api.StreamIdentifier;
import io.lakestream.api.StreamLayout;
import io.lakestream.api.exception.NoSuchStreamException;
import io.lakestream.api.materialization.ResolvedMaterialization;
import io.lakestream.api.materialization.TableCatalog;
import io.lakestream.api.materialization.TableCatalogType;
import io.lakestream.api.materialization.TableIdentifier;
import io.lakestream.api.materialization.TableMaterializationPolicy;
import io.lakestream.ursa.compaction.CompactTaskManager;
import io.lakestream.ursa.compaction.metrics.CompactionMetrics;
import io.lakestream.ursa.compaction.task.CompactStreamTask;
import io.lakestream.ursa.compaction.task.PackagedCompactStreamTask;
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
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

/**
 * T10 smoke tests for the {@link CompactionWorker} → {@link MaterializationService} dispatch.
 *
 * <p>Verifies the two end states:
 * <ul>
 *   <li>When the stream resolves a {@link ResolvedMaterialization}, the worker hands the task
 *       to {@link MaterializationService#materialize(MaterializationTask)}.</li>
 *   <li>When {@link Stream#effectiveMaterialization()} returns empty, the worker skips the
 *       materialize call entirely.</li>
 * </ul>
 */
@ExtendWith(MockitoExtension.class)
public class CompactionWorkerMaterializationDispatchTest {

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
        StorageConfig config = StorageConfig.builder()
                .retryableQuarantineInSeconds(10)
                .nonRetryableQuarantineInSeconds(60)
                .refreshLocalTaskIntervalInSeconds(5)
                .materializationEnabled(true)
                .blackTopicOfCompact(new HashSet<>())
                .build();
        return new CompactionWorker(compactTaskManager, compactionService,
                materializationService, streamCatalog,
                compactionTaskProvider, config, CompactionMetrics.NOOP);
    }

    @Test
    public void materializeCalledWhenPolicyResolved() throws Exception {
        CompactionWorker worker = createWorker();

        String topic = "default/dispatch-topic-partition-0";
        CompactStreamTask task = new CompactStreamTask();
        task.setTopic(topic);
        task.setStatus(CompactStreamTask.INIT);
        task.setStartOffset(0L);
        task.setEndOffset(10L);

        PackagedCompactStreamTask packagedTask = new PackagedCompactStreamTask();
        packagedTask.setTaskName("task-dispatch");
        packagedTask.setSubTasks(List.of("sub-1"));

        when(compactionTaskProvider.getTask())
                .thenReturn(packagedTask)
                .thenReturn(null);
        when(compactTaskManager.getCompactStreamTask("sub-1"))
                .thenReturn(CompletableFuture.completedFuture(task));
        when(compactionTaskProvider.getQuarantinedTopic(topic)).thenReturn(null);
        when(compactTaskManager.tryLockTask(packagedTask.getTaskName())).thenReturn(true);

        TableCatalog catalog = new TableCatalog("delta-cat", TableCatalogType.DELTA, Map.of(), Map.of());
        TableMaterializationPolicy effective = TableMaterializationPolicy.empty();
        ResolvedMaterialization resolved = new ResolvedMaterialization(
                catalog,
                new TableIdentifier("ns", "table"),
                effective);
        when(streamCatalog.registerExternalPartition(any(), anyInt(), anyLong(), any()))
                .thenReturn(CompletableFuture.completedFuture(null));
        stubExternalStream(task);
        when(stream.effectiveMaterialization()).thenReturn(Optional.of(resolved));
        // The worker no longer reads entries — it submits the offset range and the
        // MaterializationService reads + decodes. We only assert the dispatch happens here.

        Thread thread = new Thread(worker);
        thread.start();
        Thread.sleep(500);
        thread.interrupt();
        thread.join(2000);

        verify(materializationService).materialize(any(MaterializationTask.class));
        // The worker lazily registers the (broker-created) stream before loading it, so loadStream
        // resolves regardless of source. Partition 0 of "dispatch-topic-partition-0".
        verify(streamCatalog).registerExternalPartition(any(), anyInt(), anyLong(), any());
        verify(stream, never()).close();
    }

    @Test
    public void nativeCatalogStreamDoesNotUseExternalRegistration() throws Exception {
        CompactionWorker worker = createWorker();
        String topic = "public/default/native-topic-partition-2";
        CompactStreamTask task = new CompactStreamTask();
        task.setTopic(topic);
        task.setStatus(CompactStreamTask.INIT);
        task.setStreamId(33L);
        task.setStartOffset(0L);
        task.setEndOffset(10L);
        PackagedCompactStreamTask packagedTask = new PackagedCompactStreamTask();
        packagedTask.setTaskName("task-native");
        packagedTask.setSubTasks(List.of("sub-native"));
        when(compactionTaskProvider.getTask()).thenReturn(packagedTask).thenReturn(null);
        when(compactTaskManager.getCompactStreamTask("sub-native"))
                .thenReturn(CompletableFuture.completedFuture(task));
        when(compactionTaskProvider.getQuarantinedTopic(topic)).thenReturn(null);
        when(compactTaskManager.tryLockTask(packagedTask.getTaskName())).thenReturn(true);

        StreamIdentifier id = StreamIdentifier.of("public/default", "native-topic");
        when(streamCatalog.loadStream(id)).thenReturn(CompletableFuture.completedFuture(stream));
        when(stream.layout()).thenReturn(streamLayout);
        when(streamLayout.logIds()).thenReturn(CompletableFuture.completedFuture(
                List.of(LogId.of(31L), LogId.of(32L), LogId.of(33L))));
        when(stream.effectiveMaterialization()).thenReturn(Optional.of(new ResolvedMaterialization(
                new TableCatalog("delta-cat", TableCatalogType.DELTA, Map.of(), Map.of()),
                new TableIdentifier("ns", "table"),
                TableMaterializationPolicy.empty())));
        CountDownLatch materialized = new CountDownLatch(1);
        doAnswer(invocation -> {
            materialized.countDown();
            return null;
        }).when(materializationService).materialize(any(MaterializationTask.class));

        Thread thread = new Thread(worker);
        thread.start();
        org.junit.jupiter.api.Assertions.assertTrue(materialized.await(10, TimeUnit.SECONDS));
        thread.interrupt();
        thread.join(2_000L);

        ArgumentCaptor<MaterializationTask> captor = ArgumentCaptor.forClass(MaterializationTask.class);
        verify(materializationService).materialize(captor.capture());
        assertEquals(id, captor.getValue().stream());
        assertEquals(33L, captor.getValue().streamId());
        verify(streamCatalog, never()).registerExternalPartition(any(), anyInt(), anyLong(), any());
    }

    @Test
    public void taskPropertyFallbackPrefersRegisteredCatalog() throws Exception {
        // No stream/namespace/cluster policy resolves, so the worker falls back to task-property
        // resolution. In the new model the task carries only the catalog NAME; the catalog DEFINITION
        // is registered at startup from the compaction-service properties. The worker must therefore
        // materialize into the REGISTERED catalog (with its connection), not the placeholder the
        // fallback synthesizes — while keeping the task-derived table identifier + policy.
        CompactionWorker worker = createWorker();

        String topic = "default/registry-topic-partition-0";
        CompactStreamTask task = new CompactStreamTask();
        task.setTopic(topic);
        task.setStatus(CompactStreamTask.INIT);
        task.setStartOffset(0L);
        task.setEndOffset(10L);

        PackagedCompactStreamTask packagedTask = new PackagedCompactStreamTask();
        packagedTask.setTaskName("task-registry");
        packagedTask.setSubTasks(List.of("sub-1"));

        when(compactionTaskProvider.getTask())
                .thenReturn(packagedTask)
                .thenReturn(null);
        when(compactTaskManager.getCompactStreamTask("sub-1"))
                .thenReturn(CompletableFuture.completedFuture(task));
        when(compactionTaskProvider.getQuarantinedTopic(topic)).thenReturn(null);
        when(compactTaskManager.tryLockTask(packagedTask.getTaskName())).thenReturn(true);
        when(streamCatalog.registerExternalPartition(any(), anyInt(), anyLong(), any()))
                .thenReturn(CompletableFuture.completedFuture(null));
        stubExternalStream(task);
        when(stream.effectiveMaterialization()).thenReturn(Optional.empty());

        // Task-property fallback resolves the catalog NAME ("ch") but no connection — a placeholder.
        TableCatalog synthesized = new TableCatalog("ch", TableCatalogType.CLICKHOUSE, Map.of(), Map.of());
        ResolvedMaterialization fromProps = new ResolvedMaterialization(
                synthesized,
                new TableIdentifier("default", "public.default.registry-topic"),
                TableMaterializationPolicy.empty());
        when(materializationService.resolveFromTaskProperties(any(), any(), any()))
                .thenReturn(Optional.of(fromProps));

        // The catalog DEFINITION lives in the registry (loaded at startup), keyed by name.
        TableCatalog registered = new TableCatalog("ch", TableCatalogType.CLICKHOUSE,
                Map.of("dsn", "jdbc:clickhouse://ch:8123/default", "user", "ursa"), Map.of());
        when(streamCatalog.getTableCatalog("ch"))
                .thenReturn(CompletableFuture.completedFuture(registered));

        Thread thread = new Thread(worker);
        thread.start();
        Thread.sleep(500);
        thread.interrupt();
        thread.join(2000);

        ArgumentCaptor<MaterializationTask> captor = ArgumentCaptor.forClass(MaterializationTask.class);
        verify(materializationService).materialize(captor.capture());
        ResolvedMaterialization dispatched = captor.getValue().resolvedMaterialization();
        // Registered definition (with connection) wins over the synthesized placeholder…
        assertEquals("jdbc:clickhouse://ch:8123/default", dispatched.catalog().connection().get("dsn"));
        assertEquals("ursa", dispatched.catalog().connection().get("user"));
        // …while the task-derived table identifier + policy are preserved.
        assertEquals("default", dispatched.tableIdentifier().namespace());
        assertEquals("public.default.registry-topic", dispatched.tableIdentifier().name());
    }

    @Test
    public void materializeSkippedWhenNoPolicy() throws Exception {
        CompactionWorker worker = createWorker();

        String topic = "default/no-policy-topic-partition-0";
        CompactStreamTask task = new CompactStreamTask();
        task.setTopic(topic);
        task.setStatus(CompactStreamTask.INIT);

        PackagedCompactStreamTask packagedTask = new PackagedCompactStreamTask();
        packagedTask.setTaskName("task-no-policy");
        packagedTask.setSubTasks(List.of("sub-1"));

        when(compactionTaskProvider.getTask())
                .thenReturn(packagedTask)
                .thenReturn(null);
        when(compactTaskManager.getCompactStreamTask("sub-1"))
                .thenReturn(CompletableFuture.completedFuture(task));
        when(compactionTaskProvider.getQuarantinedTopic(topic)).thenReturn(null);
        when(compactTaskManager.tryLockTask(packagedTask.getTaskName())).thenReturn(true);
        when(streamCatalog.registerExternalPartition(any(), anyInt(), anyLong(), any()))
                .thenReturn(CompletableFuture.completedFuture(null));
        stubExternalStream(task);
        when(stream.effectiveMaterialization()).thenReturn(Optional.empty());

        Thread thread = new Thread(worker);
        thread.start();
        Thread.sleep(500);
        thread.interrupt();
        thread.join(2000);

        verify(materializationService, never()).materialize(any());
        verify(stream).close();
    }

    @Test
    public void failedStreamRegistrationLeavesWorkerToCloseHandleExactlyOnce() throws Exception {
        CompactionWorker worker = createWorker();
        String topic = "default/closed-service-partition-0";
        CompactStreamTask task = new CompactStreamTask();
        task.setTopic(topic);
        task.setStatus(CompactStreamTask.INIT);
        PackagedCompactStreamTask packagedTask = new PackagedCompactStreamTask();
        packagedTask.setTaskName("task-closed-service");
        packagedTask.setSubTasks(List.of("sub-closed-service"));
        when(compactionTaskProvider.getTask()).thenReturn(packagedTask).thenReturn(null);
        when(compactTaskManager.getCompactStreamTask("sub-closed-service"))
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
        doThrow(new IllegalStateException("Materialization service is closed"))
                .when(materializationService).registerActiveStream(any(), any());
        CountDownLatch streamClosed = new CountDownLatch(1);
        doAnswer(invocation -> {
            streamClosed.countDown();
            return null;
        }).when(stream).close();

        Thread thread = new Thread(worker);
        thread.start();
        org.junit.jupiter.api.Assertions.assertTrue(streamClosed.await(10, TimeUnit.SECONDS));
        thread.interrupt();
        thread.join(2000);

        verify(stream, times(1)).close();
        verify(materializationService, never()).materialize(any());
    }

    @Test
    public void legacyCompactStreamUsedWhenMaterializationDisabled() throws Exception {
        // materializationEnabled=false → the worker takes the legacy fallback path:
        // compactionService.compactStream(task) is called and the SPI dispatch is skipped.
        StorageConfig config = StorageConfig.builder()
                .retryableQuarantineInSeconds(10)
                .nonRetryableQuarantineInSeconds(60)
                .refreshLocalTaskIntervalInSeconds(5)
                .materializationEnabled(false)
                .blackTopicOfCompact(new HashSet<>())
                .build();
        CompactionWorker worker = new CompactionWorker(compactTaskManager, compactionService,
                materializationService, streamCatalog,
                compactionTaskProvider, config, CompactionMetrics.NOOP);

        String topic = "default/legacy-topic-partition-0";
        CompactStreamTask task = new CompactStreamTask();
        task.setTopic(topic);
        task.setStatus(CompactStreamTask.INIT);

        PackagedCompactStreamTask packagedTask = new PackagedCompactStreamTask();
        packagedTask.setTaskName("task-legacy");
        packagedTask.setSubTasks(List.of("sub-1"));

        when(compactionTaskProvider.getTask())
                .thenReturn(packagedTask)
                .thenReturn(null);
        when(compactTaskManager.getCompactStreamTask("sub-1"))
                .thenReturn(CompletableFuture.completedFuture(task));
        when(compactionTaskProvider.getQuarantinedTopic(topic)).thenReturn(null);
        when(compactTaskManager.tryLockTask(packagedTask.getTaskName())).thenReturn(true);

        Thread thread = new Thread(worker);
        thread.start();
        Thread.sleep(500);
        thread.interrupt();
        thread.join(2000);

        verify(compactionService).compactStream(task);
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
