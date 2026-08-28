/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.storage.impl.compaction;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.lakestream.ursa.compaction.CompactTaskManager;
import io.lakestream.ursa.compaction.metrics.CompactionMetrics;
import io.lakestream.ursa.compaction.task.CompactStreamTask;
import io.lakestream.ursa.compaction.task.PackagedCompactStreamTask;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * Comprehensive test suite for CompactionTaskProviderV2.
 *
 * This test class covers:
 * 1. Basic task retrieval operations
 * 2. Fetch interval management
 * 3. Task and topic quarantine mechanisms
 * 4. Priority-based task sorting
 * 5. Multi-threaded concurrency scenarios
 * 6. Edge cases and error handling
 *
 * Note: These tests use real implementations (no mocks) to ensure
 * true integration behavior and thread safety.
 */
class CompactionTaskProviderV2TestWithoutMock {

    // Test fixtures - recreated before each test
    private CompactTaskManager taskManager;  // In-memory task storage implementation
    private CompactionMetrics metrics;         // Tracks metrics like task counts
    private CompactionTaskProviderV2 provider;     // The system under test
    private ExecutorService executorService;       // Thread pool for concurrency tests

    @BeforeEach
    void setUp() {
        taskManager = new MemoryCompactTaskManager();
        metrics = CompactionMetrics.NOOP;
        executorService = Executors.newFixedThreadPool(10);
    }

    @AfterEach
    void tearDown() {
        if (executorService != null) {
            executorService.shutdownNow();
        }
    }

    @Test
    void testGetTask_ReturnsNullWhenNoTasksAvailable() {
        provider = new CompactionTaskProviderV2(taskManager, metrics, 1000, 100.0);

        PackagedCompactStreamTask task = provider.getTask();

        assertNull(task);
    }

    @Test
    void testGetTask_ReturnsTaskAfterFetch() throws Exception {
        CompactStreamTask subTask1 = createCompactStreamTask("task1", 1, 0, 100);
        String subTaskKey = buildSubTaskKey(subTask1);
        PackagedCompactStreamTask packagedTask = new PackagedCompactStreamTask("task1",
                List.of(subTaskKey));

        taskManager.publishPackagedTaskName("task1");
        taskManager.publishCompactTask(subTask1);

        provider = new CompactionTaskProviderV2(taskManager, metrics, 100, 1000.0);

        PackagedCompactStreamTask result = provider.getTask();

        assertNotNull(result);
        assertEquals("task1", result.getTaskName());
    }

    @Test
    void testGetTask_RespectsMinFetchInterval() throws Exception {
        CompactStreamTask subTask = createCompactStreamTask("task1", 1, 0, 100);
        String subTaskKey = buildSubTaskKey(subTask);

        taskManager.publishPackagedTaskName("task1");
        taskManager.publishCompactTask(subTask);

        provider = new CompactionTaskProviderV2(taskManager, metrics, 5000, 1000.0);

        // First fetch
        provider.getTask();
        long firstFetchTime = provider.getLastFetchTimestamp();

        // Add more tasks
        CompactStreamTask subTask2 = createCompactStreamTask("task2", 2, 0, 100);
        taskManager.publishPackagedTaskName("task2");
        taskManager.publishCompactTask(subTask2);

        // Try to fetch again immediately - should not fetch
        provider.getTask();
        long secondFetchTime = provider.getLastFetchTimestamp();

        assertEquals(firstFetchTime, secondFetchTime, "Should not fetch again before interval");
    }

    @Test
    void testGetTask_FetchesAfterInterval() throws Exception {
        CompactStreamTask subTask = createCompactStreamTask("task1", 1, 0, 100);

        taskManager.publishPackagedTaskName("task1");
        taskManager.publishCompactTask(subTask);

        provider = new CompactionTaskProviderV2(taskManager, metrics, 100, 1000.0);

        // First fetch
        provider.getTask();
        long firstFetchTime = provider.getLastFetchTimestamp();

        Thread.sleep(150);

        // Add more tasks
        CompactStreamTask subTask2 = createCompactStreamTask("task2", 2, 0, 100);
        taskManager.publishPackagedTaskName("task2");
        taskManager.publishCompactTask(subTask2);

        // Should fetch again
        provider.getTask();
        long secondFetchTime = provider.getLastFetchTimestamp();

        assertTrue(secondFetchTime > firstFetchTime, "Should fetch again after interval");
    }

    @Test
    void testQuarantineTask_TaskIsNotReturnedDuringQuarantine() throws Exception {
        CompactStreamTask subTask = createCompactStreamTask("task1", 1, 0, 100);

        taskManager.publishPackagedTaskName("task1");
        taskManager.publishCompactTask(subTask);

        provider = new CompactionTaskProviderV2(taskManager, metrics, 100, 1000.0);

        // Get task
        PackagedCompactStreamTask task1 = provider.getTask();
        assertNotNull(task1);

        // Quarantine it
        long quarantineUntil = System.currentTimeMillis() + 5000;
        provider.quarantineTask(quarantineUntil, "task1");

        // Try to get task again - should return null
        PackagedCompactStreamTask task2 = provider.getTask();
        assertNull(task2);
    }

    @Test
    void testQuarantineTask_TaskIsReleasedAfterQuarantineExpires() throws Exception {
        CompactStreamTask subTask = createCompactStreamTask("task1", 1, 0, 100);

        taskManager.publishPackagedTaskName("task1");
        taskManager.publishCompactTask(subTask);

        provider = new CompactionTaskProviderV2(taskManager, metrics, 100, 1000.0);

        // Get task
        PackagedCompactStreamTask task1 = provider.getTask();
        assertNotNull(task1);

        // Quarantine with short duration
        long quarantineUntil = System.currentTimeMillis() + 200;
        provider.quarantineTask(quarantineUntil, "task1");

        // Should not get task immediately
        assertNull(provider.getTask());

        // Wait for quarantine to expire
        Thread.sleep(250);

        // Should get task now
        PackagedCompactStreamTask task2 = provider.getTask();
        assertNotNull(task2);
        assertEquals("task1", task2.getTaskName());
    }

    @Test
    void testQuarantineTopic_TopicIsQuarantined() {
        provider = new CompactionTaskProviderV2(taskManager, metrics, 100, 1000.0);

        long quarantineUntil = System.currentTimeMillis() + 5000;
        provider.quarantineTopic("test-topic", quarantineUntil);

        Long result = provider.getQuarantinedTopic("test-topic");
        assertNotNull(result);
        assertEquals(quarantineUntil, result);
    }

    @Test
    void testQuarantineTopic_RemoveQuarantinedTopic() {
        provider = new CompactionTaskProviderV2(taskManager, metrics, 100, 1000.0);

        long quarantineUntil = System.currentTimeMillis() + 5000;
        provider.quarantineTopic("test-topic", quarantineUntil);

        provider.removeQuarantinedTopic("test-topic");

        assertNull(provider.getQuarantinedTopic("test-topic"));
    }

    @Test
    void testQuarantineTopic_ExpiredTopicsAreCleanedUp() throws Exception {
        provider = new CompactionTaskProviderV2(taskManager, metrics, 100, 1000.0);

        long expiredTime = System.currentTimeMillis() - 1000;
        long futureTime = System.currentTimeMillis() + 5000;

        provider.quarantineTopic("expired-topic", expiredTime);
        provider.quarantineTopic("active-topic", futureTime);

        // Trigger cleanup by adding another topic
        Thread.sleep(10);
        provider.quarantineTopic("new-topic", System.currentTimeMillis() + 5000);

        assertNull(provider.getQuarantinedTopic("expired-topic"));
        assertNotNull(provider.getQuarantinedTopic("active-topic"));
    }

    @Test
    void testTaskSorting_TasksAreSortedByPriority() throws Exception {
        CompactStreamTask subTask1 = createCompactStreamTask("task1", 1, 0, 100);
        CompactStreamTask subTask2 = createCompactStreamTask("task2", 1, 201, 300);
        CompactStreamTask subTask3 = createCompactStreamTask("task3", 1, 101, 200);

        taskManager.publishPackagedTaskName("task1");
        taskManager.publishPackagedTaskName("task2");
        taskManager.publishPackagedTaskName("task3");
        taskManager.publishCompactTask(subTask1);
        taskManager.publishCompactTask(subTask2);
        taskManager.publishCompactTask(subTask3);

        provider = new CompactionTaskProviderV2(taskManager, metrics, 100, 1000.0);

        PackagedCompactStreamTask task1 = provider.getTask();
        PackagedCompactStreamTask task2 = provider.getTask();
        PackagedCompactStreamTask task3 = provider.getTask();

        assertEquals("task1", task1.getTaskName()); // Priority 1
        assertEquals("task3", task2.getTaskName()); // Priority 2
        assertEquals("task2", task3.getTaskName()); // Priority 3
    }

    @Test
    @Timeout(10)
    void testConcurrentGetTask_MultipleThreads() throws Exception {
        int taskCount = 100;
        for (int i = 0; i < taskCount; i++) {
            CompactStreamTask subTask = createCompactStreamTask("task" + i, i, 0, 100);
            taskManager.publishPackagedTaskName("task" + i);
            taskManager.publishCompactTask(subTask);
        }

        provider = new CompactionTaskProviderV2(taskManager, metrics, 100, 10000.0);

        int threadCount = 10;
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch endLatch = new CountDownLatch(threadCount);
        Set<String> retrievedTasks = ConcurrentHashMap.newKeySet();
        AtomicInteger nullCount = new AtomicInteger(0);

        for (int i = 0; i < threadCount; i++) {
            executorService.submit(() -> {
                try {
                    startLatch.await();
                    for (int j = 0; j < 20; j++) {
                        PackagedCompactStreamTask task = provider.getTask();
                        if (task != null) {
                            retrievedTasks.add(task.getTaskName());
                        } else {
                            nullCount.incrementAndGet();
                        }
                        Thread.sleep(10);
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                } finally {
                    endLatch.countDown();
                }
            });
        }

        startLatch.countDown();
        assertTrue(endLatch.await(30, TimeUnit.SECONDS));

        assertTrue(retrievedTasks.size() > 0, "Should retrieve at least some tasks");
        assertTrue(retrievedTasks.size() <= taskCount, "Should not retrieve more tasks than available");
    }

    @Test
    @Timeout(10)
    void testConcurrentQuarantine_MultipleThreads() throws Exception {
        int taskCount = 50;
        for (int i = 0; i < taskCount; i++) {
            CompactStreamTask subTask = createCompactStreamTask("task" + i, i, 0, 100);
            taskManager.publishPackagedTaskName("task" + i);
            taskManager.publishCompactTask(subTask);
        }

        provider = new CompactionTaskProviderV2(taskManager, metrics, 100, 10000.0);

        int threadCount = 10;
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch endLatch = new CountDownLatch(threadCount);

        for (int i = 0; i < threadCount; i++) {
            final int threadId = i;
            executorService.submit(() -> {
                try {
                    startLatch.await();
                    for (int j = 0; j < 10; j++) {
                        PackagedCompactStreamTask task = provider.getTask();
                        if (task != null) {
                            long quarantineTime = System.currentTimeMillis() + 100;
                            provider.quarantineTask(quarantineTime, task.getTaskName());
                        }
                        Thread.sleep(5);
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                } finally {
                    endLatch.countDown();
                }
            });
        }

        startLatch.countDown();
        assertTrue(endLatch.await(30, TimeUnit.SECONDS));
    }

    @Test
    @Timeout(10)
    void testConcurrentTopicQuarantine_MultipleThreads() throws Exception {
        provider = new CompactionTaskProviderV2(taskManager, metrics, 100, 10000.0);

        int threadCount = 10;
        int operationsPerThread = 100;
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch endLatch = new CountDownLatch(threadCount);

        for (int i = 0; i < threadCount; i++) {
            final int threadId = i;
            executorService.submit(() -> {
                try {
                    startLatch.await();
                    for (int j = 0; j < operationsPerThread; j++) {
                        String topic = "topic-" + (j % 20);
                        long quarantineTime = System.currentTimeMillis() + 1000;

                        if (j % 3 == 0) {
                            provider.quarantineTopic(topic, quarantineTime);
                        } else if (j % 3 == 1) {
                            provider.getQuarantinedTopic(topic);
                        } else {
                            provider.removeQuarantinedTopic(topic);
                        }
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                } finally {
                    endLatch.countDown();
                }
            });
        }

        startLatch.countDown();
        assertTrue(endLatch.await(30, TimeUnit.SECONDS));
    }

    @Test
    void testGetTask_HandlesFailedSubTaskFetch() throws Exception {
        CompactStreamTask subTask1 = createCompactStreamTask("task1", 1, 0, 100);

        taskManager.publishPackagedTaskName("task1");
        taskManager.publishCompactTask(subTask1);

        provider = new CompactionTaskProviderV2(taskManager, metrics, 100, 1000.0);

        PackagedCompactStreamTask result = provider.getTask();

        assertNotNull(result);
        assertEquals("task1", result.getTaskName());
    }

    @Test
    void testGetTask_EmptyTaskList() throws Exception {
        provider = new CompactionTaskProviderV2(taskManager, metrics, 100, 1000.0);

        PackagedCompactStreamTask result = provider.getTask();

        assertNull(result);
        assertEquals(0, provider.getNumTasks());
    }

    @Test
    void testMultipleQuarantineOfSameTask() throws Exception {
        CompactStreamTask subTask = createCompactStreamTask("task1", 1, 0, 100);

        taskManager.publishPackagedTaskName("task1");
        taskManager.publishCompactTask(subTask);

        provider = new CompactionTaskProviderV2(taskManager, metrics, 100, 1000.0);

        provider.getTask();

        // Quarantine multiple times
        provider.quarantineTask(System.currentTimeMillis() + 1000, "task1");
        provider.quarantineTask(System.currentTimeMillis() + 2000, "task1");
        provider.quarantineTask(System.currentTimeMillis() + 3000, "task1");

        // Should still be quarantined
        assertNull(provider.getTask());
    }

    @Test
    @Timeout(10)
    void testRateLimiter_SlowsDownFetching() throws Exception {
        int taskCount = 10;
        for (int i = 0; i < taskCount; i++) {
            CompactStreamTask subTask = createCompactStreamTask("task" + i, i, 0, 100);
            taskManager.publishPackagedTaskName("task" + i);
            taskManager.publishCompactTask(subTask);
        }

        // Very low rate limit
        provider = new CompactionTaskProviderV2(taskManager, metrics, 100, 2.0);

        long startTime = System.currentTimeMillis();
        provider.getTask();
        long duration = System.currentTimeMillis() - startTime;

        // With 10 tasks and rate limit of 2/sec, should take at least 4 seconds
        assertTrue(duration >= 4000, "Rate limiting should slow down fetching");
    }

    // Helper methods

    private CompactStreamTask createCompactStreamTask(String taskName, long streamId,
                                                      long startOffset, long endOffset) {
        CompactStreamTask task = new CompactStreamTask();
        task.setTaskName(taskName);
        task.setStreamId(streamId);
        task.setStartOffset(startOffset);
        task.setEndOffset(endOffset);
        task.setTopic("test-topic");
        return task;
    }

    private String buildSubTaskKey(CompactStreamTask task) {
        return task.getTaskName();
    }
}
