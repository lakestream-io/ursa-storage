/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.storage.impl.compaction;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import io.lakestream.ursa.compaction.metrics.CompactionMetrics;
import io.lakestream.ursa.compaction.task.PackagedCompactStreamTask;
import io.lakestream.ursa.storage.impl.StorageConfig;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class CompactionTaskProviderTest {

    private CompactionTaskProvider provider;
    private PackagedCompactStreamTask task1;
    private PackagedCompactStreamTask task2;
    private PackagedCompactStreamTask task3;
    private PackagedCompactStreamTask task4;
    private PackagedCompactStreamTask task5;

    @BeforeEach
    void setUp() {
        StorageConfig conf = new StorageConfig();
        provider = new CompactionTaskProvider(conf, null, CompactionMetrics.NOOP);
        task1 = new PackagedCompactStreamTask("task1", List.of("sub1"));
        task2 = new PackagedCompactStreamTask("task2", List.of("sub2"));
        task3 = new PackagedCompactStreamTask("task3", List.of("sub3"));
        task4 = new PackagedCompactStreamTask("task4", List.of("sub4"));
        task5 = new PackagedCompactStreamTask("task5", List.of("sub5"));
    }

    @Test
    void testMultiThreadedUpdateAndGetTask() throws InterruptedException, ExecutionException {
        // Initialize task list for updating
        List<PackagedCompactStreamTask> initialTasks = List.of(task1, task2, task3);
        List<PackagedCompactStreamTask> newTasks = List.of(task1, task2, task3, task4, task5);
        provider.updateTasks(initialTasks);

        // Set up an ExecutorService with multiple threads
        int numThreads = 50;
        ExecutorService executorService = Executors.newFixedThreadPool(numThreads);
        List<Future<PackagedCompactStreamTask>> futures = new ArrayList<>();
        AtomicInteger quarantineCount = new AtomicInteger();
        Set<String> addedTasks = new HashSet<>();

        // Simulate concurrent `getTask`, `updateTasks`, and `quarantineTask` operations
        int successfulTasks = 0;
        while (addedTasks.size() != 5 && provider.getNumTasks() != 0) {
            futures.clear();
            for (int i = 0; i < numThreads; i++) {
                int finalI = i;
                futures.add(executorService.submit(() -> {
                    // Randomly perform different operations
                    if (finalI == 3 || ThreadLocalRandom.current().nextInt(3) == 0) {
                        // Update tasks with a new list
                        provider.updateTasks(newTasks);
                        return null;
                    }

                    PackagedCompactStreamTask task = provider.getTask();
                    if (task == null) {
                        return null;
                    }
                    if (finalI == 1 || ThreadLocalRandom.current().nextInt(3) == 1) {
                        // Quarantine a task
                        long quarantineTime = System.currentTimeMillis() + ThreadLocalRandom.current().nextInt(500);
                        provider.quarantineTask(quarantineTime, task.getTaskName());
                        quarantineCount.incrementAndGet();
                        return null;
                    } else {
                        if (addedTasks.add(task.getTaskName())) {
                            return task;
                        } else {
                            return null;
                        }
                    }
                }));
            }
            for (Future<PackagedCompactStreamTask> future : futures) {
                try {
                    PackagedCompactStreamTask task = future.get();
                    if (task != null) {
                        successfulTasks++;
                        assertNotNull(task, "Task should not be null");
                        assertTrue(newTasks.contains(task), "Unexpected task retrieved");
                    }
                } catch (InterruptedException | ExecutionException e) {
                    throw new RuntimeException(e);
                }
            }
        }

        // Wait for all tasks to complete
        executorService.shutdown();
        executorService.awaitTermination(5, TimeUnit.SECONDS);

        // Verify that at least some tasks were retrieved and some were quarantined
        System.out.println("Successful tasks: " + successfulTasks);
        assertTrue(successfulTasks > 0 , "Expected some tasks to be retrieved successfully.");
        assertTrue(quarantineCount.get() > 0, "Expected some tasks to be quarantined.");

        // Ensure no remaining tasks in currentTasks
//        assertNull(provider.getTask(), "Expected no tasks left in currentTasks after retrieval.");
    }

    @Test
    void testConcurrentQuarantineTaskAccess() throws InterruptedException {
        int numThreads = 10;
        ExecutorService executorService = Executors.newFixedThreadPool(numThreads);
        long quarantineUntil = System.currentTimeMillis() + 500; // 500ms in the future
        provider.updateTasks(List.of(task1));

        // Add the same task to quarantine from multiple threads
        for (int i = 0; i < numThreads; i++) {
            executorService.execute(() -> provider.quarantineTask(quarantineUntil, task1.getTaskName()));
        }

        executorService.shutdown();
        executorService.awaitTermination(1, TimeUnit.SECONDS);

        // Check that only one task is retrieved from quarantine after the delay
        TimeUnit.MILLISECONDS.sleep(600); // Wait for the quarantine period to end
        provider.updateTasks(List.of(task1));
        PackagedCompactStreamTask quarantinedTask = provider.getTask();

        assertNotNull(quarantinedTask, "Expected the quarantined task to be retrieved after delay.");
        assertEquals(task1.getTaskName(), quarantinedTask.getTaskName(), "Expected the quarantined task to match the original.");
        assertNull(provider.getTask(), "Expected no more tasks left after retrieving the quarantined task.");
    }


    @Test
    void quarantineTopic_addsAndCleansExpired() {
        long currentTime = System.currentTimeMillis();
        String expiredTopic = "expiredTopic";
        String validTopic = "validTopic";
        CompactionTaskProvider compactionTaskProvider =
                new CompactionTaskProvider(new StorageConfig(), null, CompactionMetrics.NOOP);

        // Add an expired topic
        compactionTaskProvider.quarantineTopic(expiredTopic, currentTime - 1000);
        assertEquals(1, compactionTaskProvider.getQuarantinedTopics().size());
        assertEquals(currentTime - 1000, compactionTaskProvider.getQuarantinedTopic(expiredTopic));

        // Add a valid topic, which should clean up the expired one
        compactionTaskProvider.quarantineTopic(validTopic, currentTime + 1000);
        assertEquals(1, compactionTaskProvider.getQuarantinedTopics().size());
        assertEquals(currentTime + 1000, compactionTaskProvider.getQuarantinedTopic(validTopic));
        assertNull(compactionTaskProvider.getQuarantinedTopic(expiredTopic));
    }

    @Test
    void getQuarantinedTopic_returnsDefaultIfAbsent() {
        CompactionTaskProvider compactionTaskProvider =
                new CompactionTaskProvider(new StorageConfig(), null, CompactionMetrics.NOOP);
        assertNull(compactionTaskProvider.getQuarantinedTopic("nonexistentTopic"));
    }

    @Test
    void removeQuarantinedTopic_removesTopicAndUpdatesMetric() {
        CompactionTaskProvider compactionTaskProvider =
                new CompactionTaskProvider(new StorageConfig(), null, CompactionMetrics.NOOP);
        String topic = "testTopic";
        compactionTaskProvider.quarantineTopic(topic, System.currentTimeMillis() + 1000);
        compactionTaskProvider.removeQuarantinedTopic(topic);
        assertEquals(0, compactionTaskProvider.getQuarantinedTopics().size());
    }

    @Test
    void concurrentQuarantineTopicAdds() throws Exception {
        int threadCount = 100;
        CompactionTaskProvider compactionTaskProvider =
                new CompactionTaskProvider(new StorageConfig(), null, CompactionMetrics.NOOP);
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        CountDownLatch latch = new CountDownLatch(1);
        List<String> topics = IntStream.range(0, threadCount)
                .mapToObj(i -> "topic" + i)
                .toList();

        long quarantineUntil = System.currentTimeMillis() + 10_000;

        topics.forEach(topic -> executor.execute(() -> {
            try {
                latch.await();
                compactionTaskProvider.quarantineTopic(topic, quarantineUntil);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }));

        latch.countDown();
        executor.shutdown();
        assertTrue(executor.awaitTermination(1, TimeUnit.SECONDS));

        assertEquals(threadCount, compactionTaskProvider.getQuarantinedTopics().size());
    }

    @Test
    void concurrentRemoveQuarantinedTopic() throws Exception {
        String topic = "concurrentRemoveTopic";
        long quarantineUntil = System.currentTimeMillis() + 10_000;
        CompactionTaskProvider compactionTaskProvider =
                new CompactionTaskProvider(new StorageConfig(), null, CompactionMetrics.NOOP);
        compactionTaskProvider.quarantineTopic(topic, quarantineUntil);

        int threadCount = 10;
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        CountDownLatch latch = new CountDownLatch(1);

        IntStream.range(0, threadCount).forEach(i -> executor.execute(() -> {
            try {
                latch.await();
                compactionTaskProvider.removeQuarantinedTopic(topic);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }));

        latch.countDown();
        executor.shutdown();
        assertTrue(executor.awaitTermination(1, TimeUnit.SECONDS));

        assertEquals(0, compactionTaskProvider.getQuarantinedTopics().size());
    }

    @Test
    void concurrentGetAndPut() throws Exception {
        String topic = "concurrentGetPutTopic";
        long quarantineUntil = System.currentTimeMillis() + 10_000;
        int threadCount = 10;
        ExecutorService executor = Executors.newFixedThreadPool(threadCount + 1);
        CountDownLatch latch = new CountDownLatch(1);
        CompactionTaskProvider compactionTaskProvider =
                new CompactionTaskProvider(new StorageConfig(), null, CompactionMetrics.NOOP);

        List<Future<Long>> futures = new ArrayList<>();
        for (int i = 0; i < threadCount; i++) {
            futures.add(executor.submit(() -> {
                latch.await();
                return compactionTaskProvider.getQuarantinedTopic(topic);
            }));
        }

        executor.execute(() -> {
            try {
                latch.await();
                compactionTaskProvider.quarantineTopic(topic, quarantineUntil);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });

        latch.countDown();
        executor.shutdown();
        assertTrue(executor.awaitTermination(1, TimeUnit.SECONDS));

        int zeros = 0;
        int correctValues = 0;
        for (Future<Long> future : futures) {
            Long value = future.get();
            if (value == null) {
                zeros++;
            } else if (value == quarantineUntil) {
                correctValues++;
            } else {
                fail("Unexpected value: " + value);
            }
        }

        assertTrue(zeros + correctValues == threadCount);
        assertEquals(quarantineUntil, compactionTaskProvider.getQuarantinedTopic(topic));
    }

}
