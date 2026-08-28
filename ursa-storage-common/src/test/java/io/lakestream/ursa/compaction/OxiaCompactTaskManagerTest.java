/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.compaction;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.lakestream.ursa.compaction.task.CompactStreamTask;
import io.lakestream.ursa.compaction.task.CompactedOffset;
import io.lakestream.ursa.compaction.task.PackagedCompactStreamTask;
import io.lakestream.ursa.compaction.task.PreparedCompactStreamTask;
import io.lakestream.ursa.utils.lock.AsyncLock;
import io.lakestream.ursa.utils.lock.LockManager;
import io.oxia.client.api.AsyncOxiaClient;
import io.oxia.client.api.GetResult;
import io.oxia.client.api.OxiaClientBuilder;
import io.oxia.client.api.options.DeleteOption;
import io.oxia.client.api.options.GetOption;
import io.oxia.client.api.options.PutOption;
import io.oxia.testcontainers.OxiaContainer;
import java.lang.reflect.Method;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.utility.DockerImageName;

public class OxiaCompactTaskManagerTest {

    private OxiaContainer oxiaContainer;
    private AsyncOxiaClient asyncOxiaClient;
    private OxiaCompactTaskManager taskManager;


    @BeforeEach
    public void setup() throws Exception {
        // Setup and start the Oxia container
        oxiaContainer = new OxiaContainer(DockerImageName.parse("oxia/oxia:latest"));
        oxiaContainer.setCommand("oxia standalone -s 32 --wal-sync-data=false");
        oxiaContainer.start();


        // Create an AsyncOxiaClient
        var oxiaClient = OxiaClientBuilder.create(oxiaContainer.getServiceAddress()).asyncClient().get();
        asyncOxiaClient = spy(oxiaClient);


        // Mock LockManager for locking tests
        LockManager lockManager = mock(LockManager.class);
        AsyncLock asyncLock = mock(AsyncLock.class);
        when(lockManager.getThreadSimpleLock(anyString())).thenReturn(asyncLock);
        when(asyncLock.tryLock()).thenReturn(CompletableFuture.completedFuture(null));
        when(asyncLock.unlock()).thenReturn(CompletableFuture.completedFuture(null));


        // Initialize the task manager
        taskManager = new OxiaCompactTaskManager(asyncOxiaClient, lockManager);
    }


    @AfterEach
    public void cleanup() throws Exception {
        if (asyncOxiaClient != null) {
            asyncOxiaClient.close();
        }
        if (oxiaContainer != null) {
            oxiaContainer.stop();
        }
    }


    @Test
    public void testPackagedTaskNameLifecycle() throws ExecutionException, InterruptedException {
        String taskName = "test-packaged-task";
        String taskKey = "/compact-stream-tasks/" + taskName;


        // Publish a packaged task name
        taskManager.publishPackagedTaskName(taskName);


        // Verify that the task name was published
        GetResult result = asyncOxiaClient.get(taskKey).get();
        assertNotNull(result, "The packaged task name should be present in Oxia.");


        // Delete the packaged task name
        taskManager.deletePackagedTaskName(taskName).get();


        // Verify that the task name was deleted
        result = asyncOxiaClient.get(taskKey).get();
        assertNull(result, "The packaged task name should be deleted from Oxia.");
    }


    @Test
    public void testCompactTaskLifecycle() throws Exception {
        CompactStreamTask
                task = new CompactStreamTask(1L, 0L, 100L, 1024L, 0L,
                "test-topic", "test-task", CompactStreamTask.INIT, Collections.emptyMap());


        // Publish a compact task
        taskManager.publishCompactTask(task);


        // Retrieve and verify the task
        String subTaskKey = OxiaCompactTaskManager.buildSubTaskKey(task);
        CompactStreamTask retrievedTask = taskManager.getCompactStreamTask(subTaskKey).get();
        assertNotNull(retrievedTask, "The retrieved task should not be null.");
        assertEquals(task.getStreamId(), retrievedTask.getStreamId(), "Stream ID should match.");
        assertEquals(task.getStartOffset(), retrievedTask.getStartOffset(), "Start offset should match.");


        // Update the task status
        task.setStatus(CompactStreamTask.COMPACTED);
        taskManager.updateCompactTask(task).get();
        retrievedTask = taskManager.getCompactStreamTask(subTaskKey).get();
        assertEquals(CompactStreamTask.COMPACTED, retrievedTask.getStatus(), "Task status should be updated.");


        // Delete the task
        taskManager.deleteCompactTask(task).get();
        retrievedTask = taskManager.getCompactStreamTask(subTaskKey).get();
        assertNull(retrievedTask, "The task should be deleted.");
    }

    private String generateTaskName() {
        return UUID.randomUUID().toString();
    }

    @Test
    public void testGetAllTasks() throws ExecutionException, InterruptedException {
        // Initially, there should be no tasks
        assertTrue(taskManager.getAllTasks().get().isEmpty(), "Initially, the task list should be empty.");


        // Publish some tasks
        String taskName1 = generateTaskName();
        String taskName2 = generateTaskName();
        taskManager.publishPackagedTaskName(taskName1);
        taskManager.publishPackagedTaskName(taskName2);


        CompactStreamTask subTask1 = new CompactStreamTask(1L, 0, 10, 100, 0,
                "topic1", taskName1, 0, Map.of());
        CompactStreamTask subTask2 = new CompactStreamTask(2L, 0, 20, 200, 0,
                "topic2", taskName2, 0, Map.of());
        asyncOxiaClient.put(OxiaCompactTaskManager.buildSubTaskKey(subTask1), new byte[0]).get();
        asyncOxiaClient.put(OxiaCompactTaskManager.buildSubTaskKey(subTask2), new byte[0]).get();

        // Retrieve all tasks
        List<PackagedCompactStreamTask> allTasks = taskManager.getAllTasks().get();
        assertEquals(2, allTasks.size(), "Should retrieve two packaged tasks.");


        // Clean up
        taskManager.deletePackagedTaskName(taskName1).get();
        taskManager.deletePackagedTaskName(taskName2).get();
    }

    @Test
    public void testGetAllTasksWithDelete() throws ExecutionException, InterruptedException {
        // Initially, there should be no tasks
        assertTrue(taskManager.getAllTasks().get().isEmpty(), "Initially, the task list should be empty.");


        // Publish some tasks
        String taskName1 = generateTaskName();
        String taskName2 = generateTaskName();
        taskManager.publishPackagedTaskName(taskName1);
        taskManager.publishPackagedTaskName(taskName2);


        CompactStreamTask subTask1 = new CompactStreamTask(1L, 0, 10, 100, 0,
                "topic1", taskName1, 0, Map.of());
        CompactStreamTask subTask2 = new CompactStreamTask(2L, 0, 20, 200, 0,
                "topic2", taskName2, 0, Map.of());
        asyncOxiaClient.put(OxiaCompactTaskManager.buildSubTaskKey(subTask1), new byte[0],
                Set.of(PutOption.PartitionKey(String.valueOf(subTask1.getStreamId())))).get();
        asyncOxiaClient.put(OxiaCompactTaskManager.buildSubTaskKey(subTask2), new byte[0],
                Set.of(PutOption.PartitionKey(String.valueOf(subTask2.getStreamId())))).get();

        taskManager.deleteCompactTask(subTask1).get();
        taskManager.deleteCompactTask(subTask2).get();

        // Retrieve all tasks
        List<PackagedCompactStreamTask> allTasks = taskManager.getAllTasks().get();
        assertEquals(2, allTasks.size(), "Should retrieve two packaged tasks.");


        // Clean up
        taskManager.deletePackagedTaskName(taskName1).get();
        taskManager.deletePackagedTaskName(taskName2).get();
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testMergeTaskDataWithImmutableEmptyPackagedTaskNames() throws Exception {
        Method mergeTaskData = OxiaCompactTaskManager.class
                .getDeclaredMethod("mergeTaskData", Set.class, Map.class);
        mergeTaskData.setAccessible(true);

        List<PackagedCompactStreamTask> tasks = (List<PackagedCompactStreamTask>) mergeTaskData.invoke(
                taskManager,
                Set.of(),
                Map.of("task-1", List.of("/compact-stream-tasks/task-1/subtask-1")));

        assertNotNull(tasks);
        assertTrue(tasks.isEmpty(), "Orphan subtasks should be ignored without throwing");
    }

    @Test
    public void testOffsetLifecycle() throws Exception {
        long streamId = 123L;
        String name = "test-offset";


        // Test published offset
        taskManager.updatePublishedOffset(streamId, 1000L, 5000L);
        CompactedOffset publishedOffset = taskManager.getPublishedOffset(streamId);
        assertNotNull(publishedOffset, "Published offset should not be null.");
        assertEquals(1000L, publishedOffset.getOffset(), "Published offset value should match.");


        taskManager.updatePublishedOffset(name, streamId, 2000L);
        publishedOffset = taskManager.getPublishedOffset(name);
        assertNotNull(publishedOffset, "Published offset by name should not be null.");
        assertEquals(2000L, publishedOffset.getOffset(), "Published offset value by name should match.");
    }


    @Test
    public void testLocking() throws Exception {
        String taskName = "locking-task";

        // Test lock acquisition
        assertTrue(taskManager.tryLockTask(taskName), "Should be able to acquire the lock.");

        // Test unlock
        taskManager.unlockTask(taskName);

        // Test unlock and remove
        taskManager.unlockTaskAndRemoveLock(taskName);
    }

    @Test
    void testPrepareTaskPutAndGetWithoutName() throws Exception {

        PreparedCompactStreamTask originalTask = new PreparedCompactStreamTask();
        var streamId = 1L;
        originalTask.setStreamId(streamId);
        originalTask.setStartOffset(0);
        originalTask.setEndOffset(100);

        var keyName = String.format("prepared-task-%020d", streamId);

        // PublishCompactTaskRunner is using this format for the name
        taskManager.publishPreparedCompactTask(originalTask, Optional.empty());
        verify(asyncOxiaClient, times(1))
                .put(eq(keyName), any(),
                        eq(Set.of(PutOption.PartitionKey(String.valueOf(streamId)), PutOption.IfRecordDoesNotExist)));

        var task = taskManager.getPreparedStreamTask(streamId);
        verify(asyncOxiaClient, times(1))
                .get(eq(keyName), eq(Set.of(GetOption.PartitionKey(String.valueOf(streamId)))));
        assertEquals(originalTask, task);

        originalTask.setStatus(CompactStreamTask.COMPACTED);
        taskManager.updatePreparedCompactTask(originalTask, Optional.empty());
        verify(asyncOxiaClient, times(1))
                .put(eq(keyName), any(), eq(Set.of(PutOption.PartitionKey(String.valueOf(streamId)))));

        task = taskManager.getPreparedStreamTask(streamId);
        assertEquals(originalTask, task);

        taskManager.deletePreparedCompactTask(streamId);
        verify(asyncOxiaClient, times(1))
                .delete(eq(keyName), eq(Set.of(DeleteOption.PartitionKey(String.valueOf(streamId)))));

        task = taskManager.getPreparedStreamTask(streamId);
        assertNull(task);
    }

    @Test
    void testPrepareTaskPutAndGetWithName() throws Exception {

        PreparedCompactStreamTask originalTask = new PreparedCompactStreamTask();
        var streamId = 1L;
        originalTask.setStreamId(streamId);
        originalTask.setStartOffset(0);
        originalTask.setEndOffset(100);
        String taskName = "test-task";

        var keyName = String.format("prepared-task-%s", taskName);

        // PublishCompactTaskRunner is using this format for the name
        taskManager.publishPreparedCompactTask(originalTask, Optional.of(taskName));

        verify(asyncOxiaClient, times(1))
                .put(eq(keyName), any(), eq(Set.of(PutOption.PartitionKey(taskName), PutOption.IfRecordDoesNotExist)));

        var task = taskManager.getPreparedStreamTask(taskName);
        verify(asyncOxiaClient, times(1))
                .get(eq(keyName), eq(Set.of(GetOption.PartitionKey(taskName))));
        assertEquals(originalTask, task);

        originalTask.setStatus(CompactStreamTask.COMPACTED);
        taskManager.updatePreparedCompactTask(originalTask, Optional.of(taskName));
        verify(asyncOxiaClient, times(1))
                .put(eq(keyName), any(), eq(Set.of(PutOption.PartitionKey(taskName))));

        task = taskManager.getPreparedStreamTask(taskName);
        assertEquals(originalTask, task);

        taskManager.deletePreparedCompactTask(taskName);
        verify(asyncOxiaClient, times(1))
                .delete(eq(keyName), eq(Set.of(DeleteOption.PartitionKey(taskName))));
        task = taskManager.getPreparedStreamTask(taskName);
        assertNull(task);
    }

    @Test
    public void testGetAllTasksWithList() throws Exception {
        // Test that getAllTasks() works with the new list-based implementation

        // Create multiple packaged tasks
        String taskName1 = generateTaskName();
        String taskName2 = generateTaskName();
        String taskName3 = generateTaskName();

        taskManager.publishPackagedTaskName(taskName1);
        taskManager.publishPackagedTaskName(taskName2);
        taskManager.publishPackagedTaskName(taskName3);

        // Create subtasks for each package
        CompactStreamTask subTask1 = new CompactStreamTask(1L, 0, 100, 1000, 0,
                "topic1", taskName1, CompactStreamTask.INIT, Map.of());
        CompactStreamTask subTask2 = new CompactStreamTask(2L, 0, 200, 2000, 0,
                "topic2", taskName2, CompactStreamTask.INIT, Map.of());
        CompactStreamTask subTask3 = new CompactStreamTask(3L, 0, 300, 3000, 0,
                "topic3", taskName3, CompactStreamTask.INIT, Map.of());

        taskManager.publishCompactTask(subTask1);
        taskManager.publishCompactTask(subTask2);
        taskManager.publishCompactTask(subTask3);

        // Test getAllTasks
        List<PackagedCompactStreamTask> allTasks = taskManager.getAllTasks().get();
        assertEquals(3, allTasks.size(), "Should retrieve all three packaged tasks");

        // Verify task names are present
        Set<String> taskNames = allTasks.stream()
                .map(PackagedCompactStreamTask::getTaskName)
                .collect(java.util.stream.Collectors.toSet());
        assertTrue(taskNames.contains(taskName1));
        assertTrue(taskNames.contains(taskName2));
        assertTrue(taskNames.contains(taskName3));

        // Cleanup
        taskManager.deleteCompactTask(subTask1).get();
        taskManager.deleteCompactTask(subTask2).get();
        taskManager.deleteCompactTask(subTask3).get();
        taskManager.deletePackagedTaskName(taskName1).get();
        taskManager.deletePackagedTaskName(taskName2).get();
        taskManager.deletePackagedTaskName(taskName3).get();
    }

    @Test
    public void testGetAllDLQTasksWithList() throws Exception {
        // Test that getAllDLQTasks() works with the new list-based implementation

        // Create DLQ tasks
        String dlqTaskName1 = generateTaskName();
        String dlqTaskName2 = generateTaskName();

        taskManager.publishDLQPackagedTaskName(dlqTaskName1);
        taskManager.publishDLQPackagedTaskName(dlqTaskName2);

        // Create DLQ subtasks
        CompactStreamTask dlqTask1 = new CompactStreamTask(11L, 0, 50, 500, 0,
                "dlq-topic1", dlqTaskName1, 0, Map.of());
        dlqTask1.setTaskQueueType(CompactStreamTask.TaskQueueType.DLQ);

        CompactStreamTask dlqTask2 = new CompactStreamTask(12L, 0, 60, 600, 0,
                "dlq-topic2", dlqTaskName2, 0, Map.of());
        dlqTask2.setTaskQueueType(CompactStreamTask.TaskQueueType.DLQ);

        taskManager.publishCompactTask(dlqTask1);
        taskManager.publishCompactTask(dlqTask2);

        // Test getAllDLQTasks
        List<PackagedCompactStreamTask> dlqTasks = taskManager.getAllDLQTasks().get();
        assertEquals(2, dlqTasks.size(), "Should retrieve all DLQ tasks");

        // Verify DLQ task names
        Set<String> dlqTaskNames = dlqTasks.stream()
                .map(PackagedCompactStreamTask::getTaskName)
                .collect(java.util.stream.Collectors.toSet());
        assertTrue(dlqTaskNames.contains(dlqTaskName1));
        assertTrue(dlqTaskNames.contains(dlqTaskName2));

        // Cleanup
        taskManager.deleteCompactTask(dlqTask1).get();
        taskManager.deleteCompactTask(dlqTask2).get();
        taskManager.deleteDLQPackagedTaskName(dlqTaskName1);
        taskManager.deleteDLQPackagedTaskName(dlqTaskName2);
    }

    @Test
    public void testGetAllTasksEmptyResult() throws Exception {
        // Test edge case: no tasks exist
        List<PackagedCompactStreamTask> allTasks = taskManager.getAllTasks().get();
        assertTrue(allTasks.isEmpty(), "Should return empty list when no tasks exist");

        List<PackagedCompactStreamTask> dlqTasks = taskManager.getAllDLQTasks().get();
        assertTrue(dlqTasks.isEmpty(), "Should return empty list when no DLQ tasks exist");
    }

    @Test
    public void testGetAllTasksWithListErrorHandling() throws Exception {
        // Create a custom OxiaCompactTaskManager with a mocked client for error simulation
        AsyncOxiaClient errorClient = mock(AsyncOxiaClient.class);
        when(errorClient.list(anyString(), anyString()))
                .thenReturn(CompletableFuture.failedFuture(new RuntimeException("Oxia list error")));

        OxiaCompactTaskManager errorTaskManager = new OxiaCompactTaskManager(errorClient, null);

        // Test that getAllTasks handles errors gracefully
        List<PackagedCompactStreamTask> allTasks = errorTaskManager.getAllTasks().get();
        assertTrue(allTasks.isEmpty(), "Should return empty list on error");

        // Test that getAllDLQTasks handles errors gracefully
        List<PackagedCompactStreamTask> dlqTasks = errorTaskManager.getAllDLQTasks().get();
        assertTrue(dlqTasks.isEmpty(), "Should return empty list on error");
    }

    @Test
    public void testGetAllTasksWithNestedStructure() throws Exception {
        // Test complex nested structure with multiple levels

        // Create tasks with subtasks at different levels
        String taskNameA = generateTaskName();
        String taskNameB = generateTaskName();

        taskManager.publishPackagedTaskName(taskNameA);
        taskManager.publishPackagedTaskName(taskNameB);

        // Create multiple subtasks for each package
        for (int i = 0; i < 5; i++) {
            CompactStreamTask subTaskA = new CompactStreamTask(100L + i, i * 10, (i + 1) * 10, 1000, 0,
                    "topic-a-" + i, taskNameA, CompactStreamTask.INIT, Map.of());
            CompactStreamTask subTaskB = new CompactStreamTask(200L + i, i * 20, (i + 1) * 20, 2000, 0,
                    "topic-b-" + i, taskNameB, CompactStreamTask.INIT, Map.of());

            taskManager.publishCompactTask(subTaskA);
            taskManager.publishCompactTask(subTaskB);
        }

        // Verify all tasks are retrieved
        List<PackagedCompactStreamTask> allTasks = taskManager.getAllTasks().get();
        assertEquals(2, allTasks.size(), "Should retrieve both packaged tasks");

        // Verify each package has the correct number of subtasks
        for (PackagedCompactStreamTask packagedTask : allTasks) {
            assertEquals(5, packagedTask.getSubTasks().size(),
                    "Each packaged task should have 5 subtasks");
        }

        // Cleanup
        for (int i = 0; i < 5; i++) {
            CompactStreamTask subTaskA = new CompactStreamTask(100L + i, i * 10, (i + 1) * 10, 1000, 0,
                    "topic-a-" + i, taskNameA, CompactStreamTask.INIT, Map.of());
            CompactStreamTask subTaskB = new CompactStreamTask(200L + i, i * 20, (i + 1) * 20, 2000, 0,
                    "topic-b-" + i, taskNameB, CompactStreamTask.INIT, Map.of());

            taskManager.deleteCompactTask(subTaskA).get();
            taskManager.deleteCompactTask(subTaskB).get();
        }
        taskManager.deletePackagedTaskName(taskNameA).get();
        taskManager.deletePackagedTaskName(taskNameB).get();
    }

    @Test
    public void testMoveTaskToDLQWithList() throws Exception {
        // Test moving tasks to DLQ with the new list-based implementation
        String taskName = generateTaskName();
        String taskName1 = generateTaskName();
        Set<String> taskNames = Set.of(taskName, taskName1);
        taskManager.publishPackagedTaskName(taskName);

        // Create tasks to move to DLQ
        CompactStreamTask task1 = new CompactStreamTask(301L, 0, 100, 1000, 0,
                "topic-move-1", taskName, CompactStreamTask.COMPACTED, Map.of());
        CompactStreamTask task2 = new CompactStreamTask(302L, 0, 200, 2000, 0,
                "topic-move-2", taskName1, CompactStreamTask.COMPACTED, Map.of());

        taskManager.publishCompactTask(task1);
        taskManager.publishCompactTask(task2);

        // Move tasks to DLQ
        taskManager.moveTaskToDLQ(List.of(task1, task2));

        // Verify tasks are in DLQ
        List<PackagedCompactStreamTask> dlqTasks = taskManager.getAllDLQTasks().get();
        assertEquals(2, dlqTasks.size(), "Should have one DLQ packaged task");
        assertTrue(taskNames.contains(dlqTasks.get(0).getTaskName()));
        assertTrue(taskNames.contains(dlqTasks.get(1).getTaskName()));

        taskManager.deletePackagedTaskName(taskName).get();
        taskManager.deletePackagedTaskName(taskName1).get();

        // Verify tasks are removed from normal queue
        List<PackagedCompactStreamTask> normalTasks = taskManager.getAllTasks().get();
        assertTrue(normalTasks.stream().noneMatch(t -> t.getTaskName().equals(taskName)),
                "Task should be removed from normal queue");
        assertTrue(normalTasks.stream().noneMatch(t -> t.getTaskName().equals(taskName1)),
                "Task should be removed from normal queue");
        // Cleanup
        task1.setTaskQueueType(CompactStreamTask.TaskQueueType.DLQ);
        task2.setTaskQueueType(CompactStreamTask.TaskQueueType.DLQ);
        taskManager.deleteCompactTask(task1).get();
        taskManager.deleteCompactTask(task2).get();
        taskManager.deleteDLQPackagedTaskName(taskName);
        taskManager.deleteDLQPackagedTaskName(taskName1);
    }
}
