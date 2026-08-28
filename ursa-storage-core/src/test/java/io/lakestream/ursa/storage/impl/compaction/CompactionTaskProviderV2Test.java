/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.storage.impl.compaction;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.lakestream.ursa.compaction.CompactTaskManager;
import io.lakestream.ursa.compaction.metrics.CompactionMetrics;
import io.lakestream.ursa.compaction.task.CompactStreamTask;
import io.lakestream.ursa.compaction.task.PackagedCompactStreamTask;
import io.opentelemetry.api.metrics.LongGauge;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

class CompactionTaskProviderV2Test {

    @Mock
    private CompactTaskManager taskManager;

    @Mock
    private CompactionMetrics compactionMetrics;

    @Mock
    private LongGauge ongoingTaskGauge;

    @Mock
    private LongGauge quarantinedTopicsGauge;

    private CompactionTaskProviderV2 provider;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
        when(compactionMetrics.getOngoingCompactionTaskCount()).thenReturn(ongoingTaskGauge);
        when(compactionMetrics.getQuarantinedTopicsCount()).thenReturn(quarantinedTopicsGauge);
    }

    @Test
    void testGetTask_WhenQueueIsEmpty_ShouldFetchNewTasks() throws Exception {
        // Given
        provider = new CompactionTaskProviderV2(taskManager, compactionMetrics, 1000, 10.0);

        PackagedCompactStreamTask task1 = createMockPackagedTask("task1", List.of("sub1"));
        CompactStreamTask subTask1 = createMockCompactStreamTask("task1", "sub1", 1);

        when(taskManager.getAllTasks()).thenReturn(CompletableFuture.completedFuture(List.of(task1)));
        when(taskManager.getCompactStreamTask("sub1")).thenReturn(CompletableFuture.completedFuture(subTask1));

        // When
        PackagedCompactStreamTask result = provider.getTask();

        // Then
        assertNotNull(result);
        assertEquals("task1", result.getTaskName());
        verify(taskManager).getAllTasks();
        verify(taskManager).getCompactStreamTask("sub1");
    }

    @Test
    void testGetTask_WhenTasksExist_ShouldReturnFromQueue() throws Exception {
        // Given
        provider = new CompactionTaskProviderV2(taskManager, compactionMetrics, 1000, 10.0);

        PackagedCompactStreamTask task1 = createMockPackagedTask("task1", List.of("sub1"));
        CompactStreamTask subTask1 = createMockCompactStreamTask("task1", "sub1", 1);

        when(taskManager.getAllTasks()).thenReturn(CompletableFuture.completedFuture(List.of(task1)));
        when(taskManager.getCompactStreamTask("sub1")).thenReturn(CompletableFuture.completedFuture(subTask1));

        // First fetch
        provider.getTask();

        // When - second call should not fetch again
        PackagedCompactStreamTask result = provider.getTask();

        // Then
        assertNull(result); // Queue is now empty
        verify(taskManager, times(1)).getAllTasks(); // Only called once
    }

    @Test
    void testGetTask_RespectMinFetchInterval() throws Exception {
        // Given
        provider = new CompactionTaskProviderV2(taskManager, compactionMetrics, 5000, 10.0);

        PackagedCompactStreamTask task1 = createMockPackagedTask("task1", List.of("sub1"));
        CompactStreamTask subTask1 = createMockCompactStreamTask("task1", "sub1", 1);

        when(taskManager.getAllTasks()).thenReturn(CompletableFuture.completedFuture(List.of(task1)));
        when(taskManager.getCompactStreamTask("sub1")).thenReturn(CompletableFuture.completedFuture(subTask1));

        // First fetch
        provider.getTask();

        // Immediately try to fetch again (should be skipped due to interval)
        PackagedCompactStreamTask result = provider.getTask();

        // Then
        assertNull(result);
        verify(taskManager, times(1)).getAllTasks(); // Should only fetch once
    }

    @Test
    void testGetTask_AfterMinFetchInterval_ShouldFetchAgain() throws Exception {
        // Given
        provider = new CompactionTaskProviderV2(taskManager, compactionMetrics, 100, 10.0);

        PackagedCompactStreamTask task1 = createMockPackagedTask("task1", List.of("sub1"));
        PackagedCompactStreamTask task2 = createMockPackagedTask("task2", List.of("sub2"));
        CompactStreamTask subTask1 = createMockCompactStreamTask("task1", "sub1", 1);
        CompactStreamTask subTask2 = createMockCompactStreamTask("task2", "sub2", 1);

        when(taskManager.getAllTasks())
            .thenReturn(CompletableFuture.completedFuture(List.of(task1)))
            .thenReturn(CompletableFuture.completedFuture(List.of(task2)));
        when(taskManager.getCompactStreamTask("sub1")).thenReturn(CompletableFuture.completedFuture(subTask1));
        when(taskManager.getCompactStreamTask("sub2")).thenReturn(CompletableFuture.completedFuture(subTask2));

        // First fetch
        PackagedCompactStreamTask result1 = provider.getTask();
        assertNotNull(result1);

        // Wait for interval to pass
        Thread.sleep(150);

        // Second fetch after interval
        PackagedCompactStreamTask result2 = provider.getTask();

        // Then
        assertNotNull(result2);
        verify(taskManager, times(2)).getAllTasks();
    }

    @Test
    void testQuarantineTask_ShouldNotReturnQuarantinedTask() throws Exception {
        // Given
        provider = new CompactionTaskProviderV2(taskManager, compactionMetrics, 1000, 10.0);

        PackagedCompactStreamTask task1 = createMockPackagedTask("task1", List.of("sub1"));
        PackagedCompactStreamTask task2 = createMockPackagedTask("task2", List.of("sub2"));
        CompactStreamTask subTask1 = createMockCompactStreamTask("task1", "sub1", 1);
        CompactStreamTask subTask2 = createMockCompactStreamTask("task2", "sub2", 2);

        when(taskManager.getAllTasks()).thenReturn(
            CompletableFuture.completedFuture(List.of(task1, task2)));
        when(taskManager.getCompactStreamTask("sub1")).thenReturn(CompletableFuture.completedFuture(subTask1));
        when(taskManager.getCompactStreamTask("sub2")).thenReturn(CompletableFuture.completedFuture(subTask2));

        // Quarantine task1
        provider.quarantineTask(System.currentTimeMillis() + 5000, "task1");

        // When
        PackagedCompactStreamTask result1 = provider.getTask();
        PackagedCompactStreamTask result2 = provider.getTask();

        // Then
        assertNotNull(result1);
        assertEquals("task2", result1.getTaskName()); // Should skip task1
        assertNull(result2); // Only one non-quarantined task
    }

    @Test
    void testQuarantineTask_ShouldReleaseAfterTimeout() throws Exception {
        // Given
        provider = new CompactionTaskProviderV2(taskManager, compactionMetrics, 100, 10.0);

        PackagedCompactStreamTask task1 = createMockPackagedTask("task1", List.of("sub1"));
        CompactStreamTask subTask1 = createMockCompactStreamTask("task1", "sub1", 1);

        when(taskManager.getAllTasks()).thenReturn(CompletableFuture.completedFuture(List.of(task1)));
        when(taskManager.getCompactStreamTask("sub1")).thenReturn(CompletableFuture.completedFuture(subTask1));

        // First, load the tasks into the queue
        PackagedCompactStreamTask firstTask = provider.getTask();
        assertNotNull(firstTask);
        assertEquals("task1", firstTask.getTaskName());

        // Now the queue is empty, quarantine task1 for 200ms
        provider.quarantineTask(System.currentTimeMillis() + 200, "task1");

        // Wait for min fetch interval to pass
        Thread.sleep(150);

        // Try to get task - should fetch but task is quarantined
        PackagedCompactStreamTask result1 = provider.getTask();
        assertNull(result1);

        // Wait for quarantine to expire
        Thread.sleep(100);

        // When - try to get task again after min interval
        Thread.sleep(100); // Ensure min fetch interval has passed
        PackagedCompactStreamTask result2 = provider.getTask();

        // Then - should now get the task
        assertNotNull(result2);
        assertEquals("task1", result2.getTaskName());
    }

    @Test
    void testQuarantineTopic() {
        // Given
        provider = new CompactionTaskProviderV2(taskManager, compactionMetrics, 1000, 10.0);
        long quarantineUntil = System.currentTimeMillis() + 5000;

        // When
        provider.quarantineTopic("topic1", quarantineUntil);

        // Then
        assertEquals(quarantineUntil, provider.getQuarantinedTopic("topic1"));
        verify(quarantinedTopicsGauge).set(1);
    }

    @Test
    void testQuarantineTopic_ShouldCleanupExpired() throws Exception {
        // Given
        provider = new CompactionTaskProviderV2(taskManager, compactionMetrics, 1000, 10.0);

        // Add expired topic
        provider.quarantineTopic("topic1", System.currentTimeMillis() - 1000);

        // Add valid topic
        Thread.sleep(50);
        provider.quarantineTopic("topic2", System.currentTimeMillis() + 5000);

        // Then - expired topic should be removed
        assertNull(provider.getQuarantinedTopic("topic1"));
        assertNotNull(provider.getQuarantinedTopic("topic2"));
        verify(quarantinedTopicsGauge, atLeastOnce()).set(1);
    }

    @Test
    void testRemoveQuarantinedTopic() {
        // Given
        provider = new CompactionTaskProviderV2(taskManager, compactionMetrics, 1000, 10.0);
        provider.quarantineTopic("topic1", System.currentTimeMillis() + 5000);

        // When
        provider.removeQuarantinedTopic("topic1");

        // Then
        assertNull(provider.getQuarantinedTopic("topic1"));
        verify(quarantinedTopicsGauge).set(0);
    }

    @Test
    void testFetchAndSortTasks_ShouldSortByPriority() throws Exception {
        // Given
        provider = new CompactionTaskProviderV2(taskManager, compactionMetrics, 1000, 100.0);

        PackagedCompactStreamTask task1 = createMockPackagedTask("task1", List.of("sub1"));
        PackagedCompactStreamTask task2 = createMockPackagedTask("task2", List.of("sub2"));
        PackagedCompactStreamTask task3 = createMockPackagedTask("task3", List.of("sub3"));

        // Sub-tasks with priorities matching task numbers (task1=1, task2=2, task3=3)
        // Lower number = higher priority, so order should be: task1, task2, task3
        CompactStreamTask subTask1 = createMockCompactStreamTask("task1", "sub1", 1);
        CompactStreamTask subTask2 = createMockCompactStreamTask("task2", "sub2", 2);
        CompactStreamTask subTask3 = createMockCompactStreamTask("task3", "sub3", 3);

        when(taskManager.getAllTasks()).thenReturn(
            CompletableFuture.completedFuture(List.of(task3, task1, task2))); // Unsorted order
        when(taskManager.getCompactStreamTask("sub1")).thenReturn(CompletableFuture.completedFuture(subTask1));
        when(taskManager.getCompactStreamTask("sub2")).thenReturn(CompletableFuture.completedFuture(subTask2));
        when(taskManager.getCompactStreamTask("sub3")).thenReturn(CompletableFuture.completedFuture(subTask3));

        // When
        PackagedCompactStreamTask result1 = provider.getTask();
        PackagedCompactStreamTask result2 = provider.getTask();
        PackagedCompactStreamTask result3 = provider.getTask();

        // Then - should be sorted by priority (task1, task2, task3)
        assertEquals("task1", result1.getTaskName());
        assertEquals("task2", result2.getTaskName());
        assertEquals("task3", result3.getTaskName());
    }

    @Test
    void testRateLimiting_ShouldControlRequestRate() throws Exception {
        // Given - 10 requests per second
        provider = new CompactionTaskProviderV2(taskManager, compactionMetrics, 1000, 10.0);

        List<PackagedCompactStreamTask> tasks = new ArrayList<>();
        for (int i = 0; i < 20; i++) {
            tasks.add(createMockPackagedTask("task" + i, List.of("sub" + i)));
        }

        when(taskManager.getAllTasks()).thenReturn(CompletableFuture.completedFuture(tasks));

        AtomicInteger requestCount = new AtomicInteger(0);
        for (int i = 0; i < 20; i++) {
            int index = i;
            when(taskManager.getCompactStreamTask("sub" + i)).thenAnswer(inv -> {
                requestCount.incrementAndGet();
                return CompletableFuture.completedFuture(
                    createMockCompactStreamTask("task" + index, "sub" + index, index));
            });
        }

        // When
        long startTime = System.currentTimeMillis();
        provider.getTask();
        long duration = System.currentTimeMillis() - startTime;

        // Then - with 20 requests at 10 req/sec, should take at least 1 second
        assertTrue(duration >= 1000, "Duration was " + duration + "ms, expected >= 1000ms");
        assertEquals(20, requestCount.get());
    }

    @Test
    @Timeout(10)
    void testConcurrentGetTask_ShouldNotFetchMultipleTimes() throws Exception {
        // Given
        provider = new CompactionTaskProviderV2(taskManager, compactionMetrics, 1000, 50.0);

        PackagedCompactStreamTask task1 = createMockPackagedTask("task1", List.of("sub1"));
        CompactStreamTask subTask1 = createMockCompactStreamTask("task1", "sub1", 1);

        when(taskManager.getAllTasks()).thenReturn(CompletableFuture.completedFuture(List.of(task1)));
        when(taskManager.getCompactStreamTask("sub1")).thenReturn(CompletableFuture.completedFuture(subTask1));

        // When - multiple threads try to get task simultaneously
        int threadCount = 10;
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch doneLatch = new CountDownLatch(threadCount);
        AtomicInteger successCount = new AtomicInteger(0);

        for (int i = 0; i < threadCount; i++) {
            executor.submit(() -> {
                try {
                    startLatch.await(); // Wait for all threads to be ready
                    PackagedCompactStreamTask task = provider.getTask();
                    if (task != null) {
                        successCount.incrementAndGet();
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                } finally {
                    doneLatch.countDown();
                }
            });
        }

        startLatch.countDown(); // Start all threads
        doneLatch.await(5, TimeUnit.SECONDS);
        executor.shutdown();

        // Then - only one fetch should happen, only one thread gets the task
        verify(taskManager, times(1)).getAllTasks();
        assertEquals(1, successCount.get());
    }

    @Test
    @Timeout(10)
    void testConcurrentGetTaskSkipsQuarantinedTasks() throws Exception {
        // Given
        provider = new CompactionTaskProviderV2(taskManager, compactionMetrics, 1000, 50.0);

        List<PackagedCompactStreamTask> tasks = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            tasks.add(createMockPackagedTask("task" + i, List.of("sub" + i)));
        }

        when(taskManager.getAllTasks()).thenReturn(CompletableFuture.completedFuture(tasks));
        for (int i = 0; i < 10; i++) {
            doReturn(CompletableFuture.completedFuture(
                    createMockCompactStreamTask("task" + i, "sub" + i, i)))
                .when(taskManager).getCompactStreamTask("sub" + i);
        }

        // When - quarantine known tasks before the concurrent getters start
        ExecutorService executor = Executors.newFixedThreadPool(5);
        CountDownLatch doneLatch = new CountDownLatch(5);
        CountDownLatch quarantineComplete = new CountDownLatch(1);
        List<PackagedCompactStreamTask> retrievedTasks = new ArrayList<>();

        // Quarantine thread
        executor.submit(() -> {
            try {
                for (int i = 0; i < 5; i++) {
                    provider.quarantineTask(System.currentTimeMillis() + 5000, "task" + i);
                    Thread.sleep(10);
                }
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                quarantineComplete.countDown();
                doneLatch.countDown();
            }
        });

        assertTrue(quarantineComplete.await(5, TimeUnit.SECONDS), "Timed out quarantining tasks");

        // Get task threads
        for (int i = 0; i < 4; i++) {
            executor.submit(() -> {
                try {
                    PackagedCompactStreamTask task = provider.getTask();
                    if (task != null) {
                        synchronized (retrievedTasks) {
                            retrievedTasks.add(task);
                        }
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                } finally {
                    doneLatch.countDown();
                }
            });
        }

        assertTrue(doneLatch.await(5, TimeUnit.SECONDS), "Timed out getting tasks");
        executor.shutdown();

        // Then - should not get any quarantined tasks
        assertEquals(4, retrievedTasks.size(), "Each getter should retrieve a task");
        for (PackagedCompactStreamTask task : retrievedTasks) {
            int taskNum = Integer.parseInt(task.getTaskName().replace("task", ""));
            assertTrue(taskNum >= 5, "Got quarantined task: " + task.getTaskName());
        }
    }

    @Test
    void testHandleFailedSubTaskFetch() throws Exception {
        // Given
        provider = new CompactionTaskProviderV2(taskManager, compactionMetrics, 1000, 10.0);

        PackagedCompactStreamTask task1 = createMockPackagedTask("task1", List.of("sub1", "sub2"));
        CompactStreamTask subTask1 = createMockCompactStreamTask("task1", "sub1", 1);

        when(taskManager.getAllTasks()).thenReturn(CompletableFuture.completedFuture(List.of(task1)));
        when(taskManager.getCompactStreamTask("sub1")).thenReturn(CompletableFuture.completedFuture(subTask1));
        when(taskManager.getCompactStreamTask("sub2")).thenReturn(
            CompletableFuture.failedFuture(new RuntimeException("Failed to fetch")));

        // When
        PackagedCompactStreamTask result = provider.getTask();

        // Then - should still return task even if one sub-task fails
        assertNotNull(result);
        assertEquals("task1", result.getTaskName());
    }

    @Test
    void testGetNumTasks() throws Exception {
        // Given
        provider = new CompactionTaskProviderV2(taskManager, compactionMetrics, 1000, 10.0);

        PackagedCompactStreamTask task1 = createMockPackagedTask("task1", List.of("sub1"));
        PackagedCompactStreamTask task2 = createMockPackagedTask("task2", List.of("sub2"));
        CompactStreamTask subTask1 = createMockCompactStreamTask("task1", "sub1", 1);
        CompactStreamTask subTask2 = createMockCompactStreamTask("task2", "sub2", 2);

        when(taskManager.getAllTasks()).thenReturn(
            CompletableFuture.completedFuture(List.of(task1, task2)));
        when(taskManager.getCompactStreamTask("sub1")).thenReturn(CompletableFuture.completedFuture(subTask1));
        when(taskManager.getCompactStreamTask("sub2")).thenReturn(CompletableFuture.completedFuture(subTask2));

        // When
        provider.getTask(); // Fetch tasks

        // Then
        assertEquals(1, provider.getNumTasks()); // One task consumed, one remaining
    }

    // Helper methods

    private PackagedCompactStreamTask createMockPackagedTask(String taskName, List<String> subTasks) {
        PackagedCompactStreamTask task = mock(PackagedCompactStreamTask.class);
        when(task.getTaskName()).thenReturn(taskName);
        when(task.getSubTasks()).thenReturn(subTasks);
        return task;
    }

    private CompactStreamTask createMockCompactStreamTask(String taskName, String subTaskName, int priority) {
        CompactStreamTask task = mock(CompactStreamTask.class);
        lenient().when(task.getTaskName()).thenReturn(taskName);

        // Store priority for comparison using lenient stubbing to avoid issues with concurrent tests
        lenient().when(task.compareTo(any(CompactStreamTask.class))).thenAnswer(inv -> {
            CompactStreamTask other = inv.getArgument(0);
            if (other == task) {
                return 0;
            }

            // Get the other task's priority by checking its task name
            try {
                String otherTaskName = other.getTaskName();
                if (otherTaskName == null) {
                    return -1;
                }
                int otherPriority = extractPriorityFromTaskName(otherTaskName);
                return Integer.compare(priority, otherPriority);
            } catch (Exception e) {
                // If we can't determine priority, consider them equal
                return 0;
            }
        });

        return task;
    }

    private int extractPriorityFromTaskName(String taskName) {
        // For test tasks like "task1", "task2", etc., use the number as priority
        if (taskName != null && taskName.startsWith("task")) {
            try {
                return Integer.parseInt(taskName.replace("task", ""));
            } catch (NumberFormatException e) {
                return Integer.MAX_VALUE;
            }
        }
        return Integer.MAX_VALUE;
    }
}
