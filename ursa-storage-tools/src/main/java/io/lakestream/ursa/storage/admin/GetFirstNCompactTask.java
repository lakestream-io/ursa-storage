/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.storage.admin;

import com.google.gson.ExclusionStrategy;
import com.google.gson.FieldAttributes;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import io.lakestream.ursa.compaction.OxiaCompactTaskManager;
import io.lakestream.ursa.compaction.task.CompactStreamTask;
import io.oxia.client.api.OxiaClientBuilder;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.stream.Collectors;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

/**
 * Command to get first N compact tasks.
 * Examples:
 *  bin/ursa admin  get-first-n-compact-tasks -o oxia-service:6648
 *      -ns ursa-storage -n 2 -t public/default/test-partition-0 -ef dataFiles
 *
 * Get 10 partitions task info of a topic:
 *
 * bin/ursa admin  get-first-n-compact-tasks -o oxia-service:6648
 *      -ns ursa-storage -n 2 -t public/default/example -p 10 -ef dataFiles
 *
 */
@Command(name = "get-first-n-compact-tasks", description = "Get first N compact tasks by topic")
public class GetFirstNCompactTask implements Callable<Integer> {

    @Option(names = {"-n", "--number-of-tasks"}, description = "Number of tasks to retrieve", defaultValue = "100")
    private int numberOfTasks;

    @Option(names = {"-o", "--oxia-server-addr"}, description = "Oxia server address", required = true)
    private String oxiaServerAddr;

    @Option(names = {"-ns", "--namespace"}, description = "Oxia namespace", defaultValue = "default")
    private String oxiaNamespace;

    @Option(names = {"-ef", "--exclude-field"}, description = "Fields to exclude from output")
    private List<String> excludeField = Collections.emptyList();

    @Option(names = {"-t", "--topic"}, description = "Topic to filter tasks by", defaultValue = "")
    private String topic;

    @Option(names = {"-p", "--partitions"},
        description = "Number of partitions of the topic when topic name is partitioned topic", defaultValue = "0")
    private int partitions;

    @Option(names = {"-d", "--delete"}, description = "Delete the retrieved tasks", defaultValue = "false")
    private boolean delete = false;

    @Option(names = {"-dr", "--dry-run"}, description = "Dry run mode, do not delete tasks", defaultValue = "false")
    private boolean dryRun = false;

    @Override
    public Integer call() throws Exception {
        try (var oxiaClient = OxiaClientBuilder.create(oxiaServerAddr)
            .namespace(oxiaNamespace).asyncClient().get()) {

            OxiaCompactTaskManager taskManager = new OxiaCompactTaskManager(oxiaClient);
            Gson gson = new GsonBuilder().setPrettyPrinting()
                .setExclusionStrategies(new ExclusionStrategy() {
                    @Override
                    public boolean shouldSkipField(FieldAttributes f) {
                        return excludeField.contains(f.getName());
                    }

                    @Override
                    public boolean shouldSkipClass(Class<?> clazz) {
                        return false;
                    }
                })
                .create();
            taskManager.getFirstNTasksOfTopic(numberOfTasks)
                .thenApply(result -> {
                    if (result.isEmpty()) {
                        System.out.println("No compact tasks found.");
                        return new HashMap<String, ConcurrentSkipListSet<CompactStreamTask>>();
                    } else {
                        if (topic.isBlank()) {
                            return result;
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
                                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
                        }
                    }
                })
                .thenCompose(tasks -> {
                    System.out.println(gson.toJson(tasks));
                    if (delete) {
                        var allTasks = tasks.values().stream()
                            .flatMap(ConcurrentSkipListSet::stream)
                            .toList();
                        if (allTasks.isEmpty()) {
                            System.out.println("No tasks to delete.");
                            return CompletableFuture.completedFuture(null);
                        } else {
                            return deleteTasksAsync(taskManager, allTasks);
                        }
                    }
                    return  CompletableFuture.completedFuture(null);
                }).join();
            return 0;
        } catch (Exception e) {
            System.err.println("Error getting first N compact tasks: " + e.getMessage());
            return 1;
        }
    }

    public CompletableFuture<Void> deleteTasksAsync(OxiaCompactTaskManager taskManager, List<CompactStreamTask> tasks) {
        System.out.println("Are you sure you want to delete these tasks? (y/n)");
        List<CompletableFuture<Void>> deletions = new ArrayList<>();
        var input = System.console().readLine();
        if (input.equalsIgnoreCase("y")) {
            for (CompactStreamTask task : tasks) {
                System.out.println("Deleting task: " + task.getTaskName());
                if (dryRun) {
                    System.out.println("[Dry run] Task deleted: " + task.getTaskName());
                } else {
                    deletions.add(taskManager.deleteCompactTask(task)
                        .thenRun(() -> System.out.println("Task deleted: " + task.getTaskName()))
                        .exceptionally(ex -> {
                            System.err.println("Failed to delete task " + task.getTaskName() + ": " + ex.getMessage());
                            return null;
                        }));
                }
            }
        } else {
            System.out.println("Aborting delete.");
        }
        return  CompletableFuture.allOf(deletions.toArray(new CompletableFuture[0]));
    }
}
