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
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

/**
 * Command to get compact tasks.
 *
 * Example:
 *  bin/ursa admin get-compact-tasks -ns default -n 1 -o localhost:6648 -ef stats,writeResult
 */
@Command(name = "get-compact-tasks", description = "Get compact tasks from Oxia")
public class GetCompactTasks implements Callable<Integer> {

    @Option(names = {"-i", "--interactive"}, description = "Run in interactive mode", defaultValue = "false")
    private boolean interactive;

    @Option(names = {"-n", "--number-of-tasks"}, description = "Number of tasks to retrieve", defaultValue = "100")
    private int numberOfTasks;

    @Option(names = {"-o", "--oxia-server-addr"}, description = "Oxia server address", required = true)
    private String oxiaServerAddr;

    @Option(names = {"-ns", "--namespace"}, description = "Oxia namespace", defaultValue = "default")
    private String oxiaNamespace;

    @Option(names = {"-ef", "--exclude-field"}, description = "Fields to exclude from output", split = ",")
    private List<String> excludeField = Collections.emptyList();

    @Option(names = {"-tn", "--task-name"}, description = "Filter by task name", defaultValue = "")
    private String taskName;

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
            if (taskName != null && !taskName.isBlank()) {
                var task = taskManager.getCompactStreamTask(taskName).get();
                if (task == null) {
                    System.out.println("No compact task found with name: " + taskName);
                } else {
                    System.out.println("Found compact task:");
                    System.out.println(gson.toJson(task));
                }
            }
            taskManager.rangeScanAllTasks(numberOfTasks)
                .thenAccept(result -> {
                    if (result.isEmpty()) {
                        System.out.println("No compact tasks found.");
                    } else {
                        System.out.println("Found " + result.size() + " compact tasks:");
                        for (CompactStreamTask task : result) {
                            System.out.println(gson.toJson(task));
                        }
                    }
                }).join();
            return 0;
        } catch (Exception e) {
            System.err.println("Error getting compact tasks: " + e.getMessage());
            return 1;
        }
    }
}
