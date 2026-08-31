/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.compact;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.lakestream.api.StreamIdentifier;
import io.lakestream.ursa.compaction.CompactTaskManager;
import io.lakestream.ursa.compaction.metrics.CompactionMetrics;
import io.lakestream.ursa.compaction.task.CompactStreamTask;
import io.lakestream.ursa.compaction.task.PackagedCompactStreamTask;
import io.lakestream.ursa.exception.DataSourceException;
import io.lakestream.ursa.exception.ExceptionCode;
import io.lakestream.ursa.storage.impl.StorageConfig;
import io.lakestream.ursa.storage.impl.compaction.CompactionService;
import io.lakestream.ursa.storage.impl.compaction.CompactionTaskProviderV2;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class CompactionWorkerTest {

    @Mock
    private CompactTaskManager compactTaskManager;

    @Mock
    private CompactionService compactionService;

    @Mock
    private CompactionTaskProviderV2 compactionTaskProvider;

    private CompactionWorker createWorker(Set<String> blackTopics) {
        StorageConfig config = StorageConfig.builder()
                .retryableQuarantineInSeconds(10)
                .nonRetryableQuarantineInSeconds(60)
                .refreshLocalTaskIntervalInSeconds(5)
                .blackTopicOfCompact(blackTopics)
                .build();
        return new CompactionWorker(compactTaskManager, compactionService,
                compactionTaskProvider, config, CompactionMetrics.NOOP);
    }

    @Test
    public void parsesNestedNamespaceAndPartitionSuffix() {
        String taskTopic = "public/default/orders-partition-2";

        assertEquals(StreamIdentifier.of("public/default", "orders"),
                CompactionWorker.toStreamIdentifier(taskTopic));
        assertEquals(2, CompactionWorker.partitionIndexOf(taskTopic));
    }

    @Test
    public void testBlacklistedTopicIsSkipped() throws Exception {
        Set<String> blackTopics = new HashSet<>();
        blackTopics.add("default/my-topic-partition-2");
        blackTopics.add("bad://topic");
        CompactionWorker worker = createWorker(blackTopics);

        // The topic in the task uses persistence naming encoding with partition suffix
        String persistenceTopic = "default/my-topic-partition-0";

        CompactStreamTask task = new CompactStreamTask();
        task.setTopic(persistenceTopic);
        task.setStatus(CompactStreamTask.INIT);

        PackagedCompactStreamTask packagedTask = new PackagedCompactStreamTask();
        packagedTask.setTaskName("task-1");
        packagedTask.setSubTasks(List.of("sub-1"));

        // First call returns the task, second call returns null to exit the loop via InterruptedException
        when(compactionTaskProvider.getTask())
                .thenReturn(packagedTask)
                .thenReturn(null);
        when(compactTaskManager.getCompactStreamTask("sub-1"))
                .thenReturn(CompletableFuture.completedFuture(task));

        // Run in a thread with a timeout so the test doesn't hang
        Thread thread = new Thread(worker);
        thread.start();
        Thread.sleep(500);
        thread.interrupt();
        thread.join(2000);

        // The blacklisted topic should cause all subtasks to be filtered out,
        // resulting in an empty validCompactTasks list -> task gets quarantined
        verify(compactionTaskProvider).quarantineTask(anyLong(), eq("task-1"));
        // compactStream should never be called for the blacklisted topic
        verify(compactionService, never()).compactStream(any());
    }

    @Test
    public void testNonBlacklistedTopicIsCompacted() throws Exception {
        Set<String> blackTopics = new HashSet<>();
        blackTopics.add("default/blocked-topic");
        CompactionWorker worker = createWorker(blackTopics);

        String persistenceTopic = "default/allowed-topic-partition-0";

        CompactStreamTask task = new CompactStreamTask();
        task.setTopic(persistenceTopic);
        task.setStatus(CompactStreamTask.INIT);

        PackagedCompactStreamTask packagedTask = new PackagedCompactStreamTask();
        packagedTask.setTaskName("task-2");
        packagedTask.setSubTasks(List.of("sub-1"));

        when(compactionTaskProvider.getTask())
                .thenReturn(packagedTask)
                .thenReturn(null);
        when(compactTaskManager.getCompactStreamTask("sub-1"))
                .thenReturn(CompletableFuture.completedFuture(task));
        when(compactionTaskProvider.getQuarantinedTopic(persistenceTopic))
                .thenReturn(null);
        when(compactTaskManager.tryLockTask("task-2"))
                .thenReturn(true);

        Thread thread = new Thread(worker);
        thread.start();
        Thread.sleep(500);
        thread.interrupt();
        thread.join(2000);

        // The non-blacklisted topic should be compacted
        verify(compactionService).compactStream(task);
    }

    @Test
    public void testEmptyBlacklistAllowsAll() throws Exception {
        CompactionWorker worker = createWorker(new HashSet<>());

        String persistenceTopic = "default/my-topic-partition-0";

        CompactStreamTask task = new CompactStreamTask();
        task.setTopic(persistenceTopic);
        task.setStatus(CompactStreamTask.INIT);

        PackagedCompactStreamTask packagedTask = new PackagedCompactStreamTask();
        packagedTask.setTaskName("task-3");
        packagedTask.setSubTasks(List.of("sub-1"));

        when(compactionTaskProvider.getTask())
                .thenReturn(packagedTask)
                .thenReturn(null);
        when(compactTaskManager.getCompactStreamTask("sub-1"))
                .thenReturn(CompletableFuture.completedFuture(task));
        when(compactionTaskProvider.getQuarantinedTopic(persistenceTopic))
                .thenReturn(null);
        when(compactTaskManager.tryLockTask("task-3"))
                .thenReturn(true);

        Thread thread = new Thread(worker);
        thread.start();
        Thread.sleep(500);
        thread.interrupt();
        thread.join(2000);

        verify(compactionService).compactStream(task);
    }

    @Test
    public void testBlacklistFiltersPartialSubTasks() throws Exception {
        Set<String> blackTopics = new HashSet<>();
        blackTopics.add("default/blocked-topic");
        CompactionWorker worker = createWorker(blackTopics);

        // sub-1: blacklisted topic
        CompactStreamTask blockedTask = new CompactStreamTask();
        blockedTask.setTopic("default/blocked-topic-partition-0");
        blockedTask.setStatus(CompactStreamTask.INIT);

        // sub-2: allowed topic
        String allowedTopic = "default/allowed-topic-partition-1";
        CompactStreamTask allowedTask = new CompactStreamTask();
        allowedTask.setTopic(allowedTopic);
        allowedTask.setStatus(CompactStreamTask.INIT);

        PackagedCompactStreamTask packagedTask = new PackagedCompactStreamTask();
        packagedTask.setTaskName("task-4");
        packagedTask.setSubTasks(List.of("sub-1", "sub-2"));

        when(compactionTaskProvider.getTask())
                .thenReturn(packagedTask)
                .thenReturn(null);
        when(compactTaskManager.getCompactStreamTask("sub-1"))
                .thenReturn(CompletableFuture.completedFuture(blockedTask));
        when(compactTaskManager.getCompactStreamTask("sub-2"))
                .thenReturn(CompletableFuture.completedFuture(allowedTask));
        when(compactionTaskProvider.getQuarantinedTopic(allowedTopic))
                .thenReturn(null);
        when(compactTaskManager.tryLockTask("task-4"))
                .thenReturn(true);

        Thread thread = new Thread(worker);
        thread.start();
        Thread.sleep(500);
        thread.interrupt();
        thread.join(2000);

        // Only the allowed task should be compacted
        verify(compactionService).compactStream(allowedTask);
        verify(compactionService, never()).compactStream(blockedTask);
    }

    @Test
    public void interruptWhileLoadingSubTaskStopsWorker() throws Exception {
        CompactionWorker worker = createWorker(new HashSet<>());
        PackagedCompactStreamTask packagedTask = new PackagedCompactStreamTask();
        packagedTask.setTaskName("interrupt-load-task");
        packagedTask.setSubTasks(List.of("pending-sub-task"));
        CountDownLatch loadStarted = new CountDownLatch(1);
        CompletableFuture<CompactStreamTask> pendingLoad = new CompletableFuture<>();
        when(compactionTaskProvider.getTask()).thenReturn(packagedTask);
        when(compactTaskManager.getCompactStreamTask("pending-sub-task")).thenAnswer(invocation -> {
            loadStarted.countDown();
            return pendingLoad;
        });

        Thread thread = new Thread(worker);
        thread.start();
        assertTrue(loadStarted.await(5, TimeUnit.SECONDS));

        thread.interrupt();
        thread.join(5_000L);

        assertFalse(thread.isAlive());
        assertTrue(thread.isInterrupted());
        verify(compactionService, never()).compactStream(any());
        verify(compactionTaskProvider, never()).quarantineTask(anyLong(), any());
    }

    @Test
    public void testMessageConsumeError_noQuarantine() throws Exception {
        runFailingTaskAndAssertQuarantine(ExceptionCode.SOURCE_READ_ERROR, null);
    }

    @Test
    public void testNoMorePermitsAvailable_noQuarantine() throws Exception {
        runFailingTaskAndAssertQuarantine(ExceptionCode.SOURCE_THROTTLED, null);
    }

    @Test
    public void testNoMoreMessages_retryableQuarantine() throws Exception {
        runFailingTaskAndAssertQuarantine(ExceptionCode.NO_MORE_RECORDS, TimeUnit.SECONDS.toMillis(10));
    }

    @Test
    public void testClientError_retryableQuarantine() throws Exception {
        runFailingTaskAndAssertQuarantine(ExceptionCode.SOURCE_CLIENT_ERROR, TimeUnit.SECONDS.toMillis(10));
    }

    @Test
    public void testInternalError_nonRetryableQuarantine() throws Exception {
        runFailingTaskAndAssertQuarantine(ExceptionCode.INTERNAL_ERROR, TimeUnit.SECONDS.toMillis(60));
    }

    /**
     * Runs the worker against a task whose compactStream call throws a {@link DataSourceException}
     * with {@code code}, then asserts that the topic was either not quarantined ({@code expectedMs}
     * is null) or quarantined for approximately {@code expectedMs} milliseconds.
     */
    private void runFailingTaskAndAssertQuarantine(ExceptionCode code, Long expectedMs) throws Exception {
        CompactionWorker worker = createWorker(new HashSet<>());
        String topic = "default/failing-topic-" + code.name();

        CompactStreamTask task = new CompactStreamTask();
        task.setTopic(topic);
        task.setStatus(CompactStreamTask.INIT);

        PackagedCompactStreamTask packagedTask = new PackagedCompactStreamTask();
        packagedTask.setTaskName("task-" + code.name());
        packagedTask.setSubTasks(List.of("sub-1"));

        when(compactionTaskProvider.getTask())
                .thenReturn(packagedTask)
                .thenReturn(null);
        when(compactTaskManager.getCompactStreamTask("sub-1"))
                .thenReturn(CompletableFuture.completedFuture(task));
        when(compactionTaskProvider.getQuarantinedTopic(topic)).thenReturn(null);
        when(compactTaskManager.tryLockTask(packagedTask.getTaskName())).thenReturn(true);
        doThrow(new DataSourceException(code, "boom")).when(compactionService).compactStream(task);

        long beforeMs = System.currentTimeMillis();
        Thread thread = new Thread(worker);
        thread.start();
        Thread.sleep(500);
        thread.interrupt();
        thread.join(2000);
        long afterMs = System.currentTimeMillis();

        if (expectedMs == null) {
            verify(compactionTaskProvider, never()).quarantineTopic(eq(topic), anyLong());
        } else {
            ArgumentCaptor<Long> untilCaptor = ArgumentCaptor.forClass(Long.class);
            verify(compactionTaskProvider).quarantineTopic(eq(topic), untilCaptor.capture());
            long until = untilCaptor.getValue();
            // The recorded "until" is now-at-quarantine + expectedMs; bound by the test's clock window.
            assertTrue(until >= beforeMs + expectedMs,
                "quarantine until=" + until + " < beforeMs+expected=" + (beforeMs + expectedMs));
            assertTrue(until <= afterMs + expectedMs,
                "quarantine until=" + until + " > afterMs+expected=" + (afterMs + expectedMs));
        }
    }
}
