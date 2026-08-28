/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.storage.admin;

import com.google.gson.ExclusionStrategy;
import com.google.gson.FieldAttributes;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import io.lakestream.ursa.compact.CompactionScheduler;
import io.lakestream.ursa.compaction.OxiaCompactTaskManager;
import io.lakestream.ursa.compaction.task.CompactStreamTask;
import io.lakestream.ursa.lakehouse.compact.CompactedTaskRunner;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.ParentCommand;

@Command(name = "commit-tasks", description = "Manually commit tasks")
public class ManuallyCommitTasks implements Callable<Integer> {

    @ParentCommand
    private Admin parent;

    @Option(names = {"-t", "--topic"}, description = "Topic to filter tasks by", defaultValue = "")
    private String topic;

    @Option(names = {"-n", "--number-of-tasks"}, description = "Number of tasks to retrieve", defaultValue = "100")
    private int numberOfTasks;

    @Option(names = {"-p", "--partitions"},
        description = "Number of partitions of the topic when topic name is partitioned topic", defaultValue = "0")
    private int partitions;

    @Option(names = {"-dr", "--dry-run"}, description = "Dry run mode, do not execute the commit operation",
        defaultValue = "false")
    private boolean dryRun = false;

    @Option(names = {"-ef", "--exclude-field"}, description = "Fields to exclude from output")
    private List<String> excludeField = Collections.emptyList();

    @Option(names = {"-if", "--include-field"}, description = "Fields to include from output")
    private List<String> includeField = Collections.emptyList();

    @Override
    public Integer call() throws Exception {
        var config = Admin.getStorageConfig(parent.getConfigFile());
        CompactionScheduler compactionScheduler = new CompactionScheduler(config);
        try {
            var runner = compactionScheduler.getCommitRunner();
            if (!(runner instanceof CompactedTaskRunner commitRunner)) {
                throw new IllegalStateException("Unsupported commit runner type: " + runner.getClass().getName());
            }
            var tasks = getDlQTasks((OxiaCompactTaskManager) compactionScheduler.getCompactTaskManager(),
                numberOfTasks, topic, partitions);
            var partitionedTopicName = StreamNames.baseName(topic);

            Gson gson = new GsonBuilder().setPrettyPrinting()
                .setExclusionStrategies(new ExclusionStrategy() {
                    @Override
                    public boolean shouldSkipField(FieldAttributes f) {
                        if (!includeField.isEmpty()) {
                            return !includeField.contains(f.getName());
                        }
                        return excludeField.contains(f.getName());
                    }

                    @Override
                    public boolean shouldSkipClass(Class<?> clazz) {
                        return false;
                    }
                })
                .create();

            System.out.println("Tasks");
            System.out.println(gson.toJson(tasks));

            if (dryRun) {
                System.out.println(
                    String.format("[Dry run] commit %s tasks for the topic %s %n", tasks.size(), partitionedTopicName));
            } else {
                System.out.println("Run commit tasks for topic " + partitionedTopicName);
                commitRunner.runTask(partitionedTopicName, tasks);
            }
        } finally {
            compactionScheduler.close();
        }
        return 0;
    }

    private List<CompactStreamTask> getDlQTasks(OxiaCompactTaskManager taskManager, int numbers, String topic,
                                                int partitions)
        throws Exception {

        return taskManager.getFirstNTasksInDLQ(numbers)
            .thenApply(result -> {
                if (result.isEmpty()) {
                    System.out.println("No compaction tasks found in the DLQ");
                    return Collections.<CompactStreamTask>emptyList();
                } else {
                    List<String> topics = new ArrayList<>();
                    if (partitions > 0) {
                        for (int i = 0; i < partitions; i++) {
                            topics.add(StreamNames.partitionName(topic, i));
                        }
                    } else {
                        topics.add(StreamNames.normalize(topic));
                    }
                    return result.entrySet().stream()
                        .filter(entry -> topics.contains(entry.getKey()))
                        .flatMap(entry -> entry.getValue().stream())
                        .toList();
                }
            }).get();
    }
}
