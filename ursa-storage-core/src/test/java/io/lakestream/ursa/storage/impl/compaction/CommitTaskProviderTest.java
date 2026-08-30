/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.storage.impl.compaction;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import io.lakestream.ursa.compaction.CompactTaskManager;
import io.lakestream.ursa.compaction.metrics.CompactionMetrics;
import io.lakestream.ursa.compaction.task.CompactStreamTask;
import io.lakestream.ursa.compaction.task.PackagedCompactStreamTask;
import io.lakestream.ursa.storage.impl.StorageConfig;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentSkipListSet;
import org.junit.jupiter.api.Test;

public class CommitTaskProviderTest {

    private void mockTaskManager(CompactTaskManager manager, Map<String, List<String>> tasks,
                                 Map<String, CompactStreamTask> subTasks) throws Exception {
        List<PackagedCompactStreamTask> allTasks = tasks.entrySet().stream()
            .map(mapEntry -> {
                var t = new PackagedCompactStreamTask();
                t.setTaskName(mapEntry.getKey());
                t.setSubTasks(mapEntry.getValue());
                return t;
            }).toList();
        doReturn(CompletableFuture.completedFuture(allTasks)).when(manager).getAllTasks();
        doAnswer(invocation -> {
            String taskName = invocation.getArgument(0);
            return CompletableFuture.completedFuture(subTasks.get(taskName));
        }).when(manager).getCompactStreamTask(anyString());
        doAnswer(invocation -> {
            String taskName = invocation.getArgument(0);
            tasks.remove(taskName);
            return CompletableFuture.completedFuture(true);
        }).when(manager).deletePackagedTaskNameIfEmpty(anyString());
    }

    @Test
    public void testOneStreamOrder() throws Exception {
        CompactTaskManager manager = mock(CompactTaskManager.class);
        Map<String, List<String>> tasks = new HashMap<>();
        Map<String, CompactStreamTask> subTasks = new HashMap<>();

        // Task 1
        var sub1Key = "1-0-999";
        var sub1Value = new CompactStreamTask(1, 0, 999, 0, 0, "topic-1", "task-1", CompactStreamTask.INIT, Collections.emptyMap());
        subTasks.put(sub1Key, sub1Value);

        // Task 2
        var sub2Key = "2-0-999";
        var sub2Value = new CompactStreamTask(2, 0, 999, 0, 0, "topic-1", "task-2", CompactStreamTask.COMPACTED, Collections.emptyMap());
        subTasks.put(sub2Key, sub2Value);

        // Task 3
        var sub3Key = "3-0-999";
        var sub3Value = new CompactStreamTask(3, 0, 999, 0, 0, "topic-1", "task-3", CompactStreamTask.COMPACTED, Collections.emptyMap());
        subTasks.put(sub3Key, sub3Value);

        // Task 4
        var sub4Key = "3-1000-1999";
        var sub4Value = new CompactStreamTask(3, 1000, 1999, 0, 0, "topic-1", "task-4", CompactStreamTask.COMPACTED, Collections.emptyMap());
        subTasks.put(sub4Key, sub4Value);

        tasks.put(UUID.randomUUID().toString(), List.of(sub4Key));
        tasks.put(UUID.randomUUID().toString(), List.of(sub3Key));
        tasks.put(UUID.randomUUID().toString(), List.of(sub2Key));
        tasks.put(UUID.randomUUID().toString(), List.of(sub1Key));

        Collections.shuffle(new ArrayList<>(tasks.entrySet()));

        mockTaskManager(manager, tasks, subTasks);

        StorageConfig config = new StorageConfig();
        config.setRefreshLocalTaskIntervalInSeconds(0);
        var commitTaskProvider = new CommitTaskProvider(config, manager, CompactionMetrics.NOOP);
        var taskMap = commitTaskProvider.getTask();

        assertEquals(1, commitTaskProvider.tasks.keySet().size());
        assertEquals(3, commitTaskProvider.tasks.get("topic-1").size());
        assertTrue(taskMap.isEmpty());

        // change the first task to COMPACTED
        subTasks.get(sub1Key).setStatus(CompactStreamTask.COMPACTED);
        taskMap = commitTaskProvider.getTask();
        assertEquals(1, commitTaskProvider.tasks.keySet().size());
        assertEquals(0, commitTaskProvider.tasks.get("topic-1").size());
        assertEquals(4, taskMap.get("topic-1").size());

        var result = taskMap.get("topic-1");

        for (int i = 0; i < result.size() - 1; i++) {
            var task = result.get(i);
            assertEquals(i + 1, task.getStreamId());
        }

        assertEquals(1, result.get(0).getStreamId());
        assertEquals(0, result.get(0).getStartOffset());

        assertEquals(2, result.get(1).getStreamId());
        assertEquals(0, result.get(1).getStartOffset());

        assertEquals(3, result.get(2).getStreamId());
        assertEquals(0, result.get(2).getStartOffset());

        assertEquals(3, result.get(3).getStreamId());
        assertEquals(1000, result.get(3).getStartOffset());

    }

    @Test
    public void testMultipleStreamOrder() throws Exception {
        CompactTaskManager manager = mock(CompactTaskManager.class);
        Map<String, List<String>> tasks = new HashMap<>();
        Map<String, CompactStreamTask> subTasks = new HashMap<>();

        // Task 1
        var sub1Key = "1-0-999";
        var sub1Value = new CompactStreamTask(1, 0, 999, 0, 0, "topic-1", "task-1", CompactStreamTask.INIT, Collections.emptyMap());
        subTasks.put(sub1Key, sub1Value);

        // Task 2
        var sub2Key = "2-0-999";
        var sub2Value = new CompactStreamTask(2, 0, 999, 0, 0, "topic-1", "task-2", CompactStreamTask.COMPACTED, Collections.emptyMap());
        subTasks.put(sub2Key, sub2Value);

        // Task 3
        var sub3Key = "3-0-999";
        var sub3Value = new CompactStreamTask(3, 0, 999, 0, 0, "topic-2", "task-3", CompactStreamTask.INIT, Collections.emptyMap());
        subTasks.put(sub3Key, sub3Value);

        // Task 4
        var sub4Key = "3-1000-1999";
        var sub4Value = new CompactStreamTask(3, 1000, 1999, 0, 0, "topic-2", "task-4", CompactStreamTask.COMPACTED, Collections.emptyMap());
        subTasks.put(sub4Key, sub4Value);

        tasks.put(UUID.randomUUID().toString(), List.of(sub4Key));
        tasks.put(UUID.randomUUID().toString(), List.of(sub3Key));
        tasks.put(UUID.randomUUID().toString(), List.of(sub2Key));
        tasks.put(UUID.randomUUID().toString(), List.of(sub1Key));

        mockTaskManager(manager, tasks, subTasks);

        StorageConfig config = new StorageConfig();
        config.setRefreshLocalTaskIntervalInSeconds(0);
        var commitTaskProvider = new CommitTaskProvider(config, manager, CompactionMetrics.NOOP);
        var tasksMap = commitTaskProvider.getTask();

        assertEquals(2, commitTaskProvider.tasks.keySet().size());
        assertEquals(1, commitTaskProvider.tasks.get("topic-1").size());
        assertEquals(1, commitTaskProvider.tasks.get("topic-2").size());
        assertTrue(tasksMap.isEmpty());


        // make topic-2 compacted
        subTasks.get(sub3Key).setStatus(CompactStreamTask.COMPACTED);
        tasksMap = commitTaskProvider.getTask();
        assertEquals(2, commitTaskProvider.tasks.keySet().size());
        assertEquals(1, commitTaskProvider.tasks.get("topic-1").size());
        assertEquals(0, commitTaskProvider.tasks.get("topic-2").size());


        var t2PartitionedName = "topic-2";
        var task = tasksMap.get(t2PartitionedName).get(0);
        assertEquals("topic-2", task.getTopic());
        assertEquals(CompactStreamTask.COMPACTED, task.getStatus());
        assertEquals(3, task.getStreamId());
        assertEquals(0, task.getStartOffset());

        task = tasksMap.get(t2PartitionedName).get(1);
        assertEquals("topic-2", task.getTopic());
        assertEquals(CompactStreamTask.COMPACTED, task.getStatus());
        assertEquals(3, task.getStreamId());
        assertEquals(1000, task.getStartOffset());

        // get again to trigger the deletion of the topic-2 tasks
        subTasks.remove(sub3Key);
        subTasks.remove(sub4Key);
        commitTaskProvider.getTask();
        assertEquals(1, commitTaskProvider.tasks.keySet().size());
        assertEquals(1, commitTaskProvider.tasks.get("topic-1").size());
        assertFalse(commitTaskProvider.tasks.containsKey("topic-2"));
        var existingTasks = tasks.values().stream().flatMap(Collection::stream).toList();
        assertEquals(2, existingTasks.size());
        assertEquals(List.of(sub1Key, sub2Key), existingTasks.stream().sorted().toList());
        verify(manager, times(2)).deletePackagedTaskNameIfEmpty(anyString());
    }

    private List<CompactStreamTask> generateTaskForTopic(String topic, long streamId, int numbers) {
        List<CompactStreamTask> tasks = new ArrayList<>();
        for (int i = 0; i < numbers; i++) {
            CompactStreamTask task = new CompactStreamTask();
            task.setTopic(topic);
            task.setStreamId(streamId);
            task.setStartOffset(i * 100);
            task.setEndOffset((i + 1) * 100 - 1);
            task.setStatus(CompactStreamTask.COMPACTED);
            tasks.add(task);
        }
        return tasks;
    }

    @Test
    void testGetTasksByPartitionedTopicName() {
        CommitTaskProvider provider = new CommitTaskProvider(new StorageConfig(), null, CompactionMetrics.NOOP);
        Map<String, ConcurrentSkipListSet<CompactStreamTask>> source = new HashMap<>();

        String t1p0 = "topic-1-partition-0";
        List<CompactStreamTask> t1p0Tasks = generateTaskForTopic(t1p0, 1, 3);
        String t1p1 = "topic-1-partition-1";
        List<CompactStreamTask> t1p1Tasks = generateTaskForTopic(t1p1, 2, 3);
        String t2p0 = "topic-2-partition-0";
        List<CompactStreamTask> t2p0Tasks = generateTaskForTopic(t2p0, 3, 1);
        source.put(t1p0, new ConcurrentSkipListSet<>(t1p0Tasks));
        source.put(t1p1, new ConcurrentSkipListSet<>(t1p1Tasks));
        source.put(t2p0, new ConcurrentSkipListSet<>(t2p0Tasks));

        Map<String, List<CompactStreamTask>> result = provider.getExecuteTasks(source);
        assertEquals(2, result.size());

        var t1PartitionedName = "topic-1";
        var t2PartitionedName = "topic-2";
        assertTrue(result.containsKey(t1PartitionedName));
        assertTrue(result.containsKey(t2PartitionedName));
        assertEquals(6, result.get(t1PartitionedName).size());
        assertEquals(1, result.get(t2PartitionedName).size());

        // verify the order of topic-1 tasks, the tasks should be ordered in the one partition topic
        // it can disordered between partitions
        var t1Tasks = result.get(t1PartitionedName);
        var firstTask = t1Tasks.get(0);
        if (firstTask.getStreamId() == 1) {
            var t1p0TasksResult = t1Tasks.subList(0, 3);
            assertEquals(t1p0TasksResult, t1p0Tasks);
            var t1p1TasksResult = t1Tasks.subList(3, 6);
            assertEquals(t1p1TasksResult, t1p1Tasks);
        } else {
            var t1p1TasksResult = t1Tasks.subList(0, 3);
            assertEquals(t1p1TasksResult, t1p1Tasks);
            var t1p0TasksResult = t1Tasks.subList(3, 6);
            assertEquals(t1p0TasksResult, t1p0Tasks);
        }

        // verify the topic-2 task
        var t2Tasks = result.get(t2PartitionedName);
        assertEquals(t2p0Tasks, t2Tasks);
    }

    @Test
    void testPartitionedTopicNameNormalizesStreamPartitions() {
        assertEquals("topic",
                CommitTaskProvider.partitionedTopicName("topic"));
        assertEquals("org/analytics/orders",
                CommitTaskProvider.partitionedTopicName("org/analytics/orders-partition-0"));
        assertEquals("streams/org/analytics/orders",
                CommitTaskProvider.partitionedTopicName("streams/org/analytics/orders-partition-17"));
    }

    @Test
    void testPartitionedTopicNameDoesNotStripNonCanonicalPartitionSuffixes() {
        List<String> localNames = List.of(
                "topic-partition-00",
                "topic-partition-01",
                "topic-partition-2147483648",
                "topic-partition-999999999999999999999999999999",
                "topic-partition--1",
                "topic-partition-+1",
                "topic-partition-",
                "topic-partition-not-a-number");

        for (String localName : localNames) {
            assertEquals(localName,
                    CommitTaskProvider.partitionedTopicName(localName));
        }
    }

    @Test
    void testPartitionedTopicNameRejectsBlankNames() {
        assertThrows(IllegalArgumentException.class,
                () -> CommitTaskProvider.partitionedTopicName(null));
        assertThrows(IllegalArgumentException.class,
                () -> CommitTaskProvider.partitionedTopicName(" "));
    }
}
