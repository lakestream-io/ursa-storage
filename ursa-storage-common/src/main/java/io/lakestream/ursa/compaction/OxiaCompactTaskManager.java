/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.compaction;

import io.lakestream.ursa.compaction.task.CompactOffsetSerde;
import io.lakestream.ursa.compaction.task.CompactStreamTask;
import io.lakestream.ursa.compaction.task.CompactStreamTaskSerde;
import io.lakestream.ursa.compaction.task.CompactedOffset;
import io.lakestream.ursa.compaction.task.PackagedCompactStreamTask;
import io.lakestream.ursa.compaction.task.PreparedCompactStreamTask;
import io.lakestream.ursa.compaction.task.PreparedCompactStreamTaskSerde;
import io.lakestream.ursa.utils.lock.AsyncLock;
import io.lakestream.ursa.utils.lock.LockManager;
import io.lakestream.ursa.utils.lock.exception.LockException;
import io.oxia.client.api.AsyncOxiaClient;
import io.oxia.client.api.GetResult;
import io.oxia.client.api.RangeScanConsumer;
import io.oxia.client.api.exceptions.KeyAlreadyExistsException;
import io.oxia.client.api.exceptions.UnexpectedVersionIdException;
import io.oxia.client.api.options.DeleteOption;
import io.oxia.client.api.options.GetOption;
import io.oxia.client.api.options.PutOption;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class OxiaCompactTaskManager implements CompactTaskManager {
    private static final String TASK_PREFIX = "/compact-stream-tasks";
    private static final String TASK_PREFIX_DLQ = TASK_PREFIX + "-dlq";

    private static final String TASK_LOCK_PREFIX = "/task-locks/";

    private final AsyncOxiaClient oxiaClient;
    private final LockManager lockManager;

    public OxiaCompactTaskManager(AsyncOxiaClient oxiaClient, LockManager lockManager) {
        this.oxiaClient = oxiaClient;
        this.lockManager = lockManager;
    }


    public OxiaCompactTaskManager(AsyncOxiaClient oxiaClient) {
        this.oxiaClient = oxiaClient;
        this.lockManager = null;
    }

    public CompletableFuture<Map<String, ConcurrentSkipListSet<CompactStreamTask>>> getFirstNTasksInDLQ(int n) {
        return getFirstNTasksByPath(TASK_PREFIX_DLQ, n);
    }

    public CompletableFuture<Map<String, ConcurrentSkipListSet<CompactStreamTask>>> getFirstNTasksOfTopic(int n) {
        return getFirstNTasksByPath(TASK_PREFIX, n);
    }

    public CompletableFuture<Map<String, ConcurrentSkipListSet<CompactStreamTask>>> getFirstNTasksByPath(
        String basePath, int n) {

        String startKey = String.format("%s/%s/", basePath, "00000000-0000-0000-0000-000000000000");
        String endKey = String.format("%s/%s/", basePath, "fffffffff-ffff-ffff-ffff-ffffffffffff");

        CompletableFuture<Map<String, ConcurrentSkipListSet<CompactStreamTask>>> future = new CompletableFuture<>();
        Map<String, ConcurrentSkipListSet<CompactStreamTask>> results = new ConcurrentHashMap<>();
        oxiaClient.rangeScan(startKey, endKey, new RangeScanConsumer() {
            @Override
            public boolean onNext(GetResult getResult) {
                if (getResult != null) {
                    try {
                        var task = CompactStreamTaskSerde.INSTANCE.deserialize(getResult.value());
                        if (task != null) {
                            var topic = task.getTopic();
                            var list = results.computeIfAbsent(topic, k -> new ConcurrentSkipListSet<>());
                            list.add(task);
                            if (list.size() == n) {
                                list.pollLast();
                            }
                        }
                    } catch (Throwable e) {
                        log.error("Failed to deserialize compact stream task: {}", getResult.key(), e);
                    }
                }
                return true;
            }

            @Override
            public void onError(Throwable throwable) {
                future.completeExceptionally(throwable);
            }

            @Override
            public void onCompleted() {
                future.complete(results);
            }
        });
        return future;
    }

    public CompletableFuture<List<CompactStreamTask>> getAllDLQTasks(int numbers) {
        return rangeScanAllTasks(TASK_PREFIX_DLQ, numbers);
    }

    public CompletableFuture<List<CompactStreamTask>> rangeScanAllTasks(int numbers) {
        return rangeScanAllTasks(TASK_PREFIX, numbers);
    }

    public CompletableFuture<List<CompactStreamTask>> rangeScanAllTasks(String path, int numbers) {
        String startKey = String.format("%s/%s/", path, "00000000-0000-0000-0000-000000000000");
        String endKey = String.format("%s/%s/", path, "fffffffff-ffff-ffff-ffff-ffffffffffff");

        CompletableFuture<List<GetResult>> future = new CompletableFuture<>();
        ArrayList<GetResult> values = new ArrayList<>();
        AtomicInteger count = new AtomicInteger(numbers);
        oxiaClient.rangeScan(startKey, endKey, new RangeScanConsumer() {
            @Override
            public boolean onNext(GetResult getResult) {
                if (count.decrementAndGet() < 0) {
                    onCompleted();
                    return false;
                }
                values.add(getResult);
                return true;
            }

            @Override
            public void onError(Throwable throwable) {
                future.completeExceptionally(throwable);
            }

            @Override
            public void onCompleted() {
                future.complete(values);
            }
        });

        return future.thenApply(results ->
            results.stream().map(gr -> {
                try {
                    return CompactStreamTaskSerde.INSTANCE.deserialize(gr.value());
                } catch (Throwable throwable) {

                    log.error("Failed to deserialize compact stream task key: {}, value: {}", gr.key(),
                        Base64.getEncoder().encodeToString(gr.value()), throwable);
                    return null;
                }
            }).toList()
        );
    }

    @Override
    public CompletableFuture<List<PackagedCompactStreamTask>> getAllTasks() {
        return getAllTasksInternal(TASK_PREFIX);
    }

    @Override
    public CompletableFuture<List<PackagedCompactStreamTask>> getAllDLQTasks() {
        return getAllTasksInternal(TASK_PREFIX_DLQ);
    }

    private CompletableFuture<List<PackagedCompactStreamTask>> getAllTasksInternal(String prefix) {
        String packagedStart = prefix + "/";
        String packagedEnd = prefix + "//";
        String startKey = prefix + "/00000000-0000-0000-0000-000000000000/";
        String endKey = prefix + "/ffffffff-ffff-ffff-ffff-ffffffffffff/";

        // Fetch packaged tasks and subtasks in parallel
        CompletableFuture<Set<String>> packagedFuture = oxiaClient.list(packagedStart, packagedEnd)
                .thenApply(this::extractPackagedTaskNames);

        CompletableFuture<Map<String, List<String>>> subtaskFuture = oxiaClient.list(startKey, endKey)
                .thenApply(this::groupSubtasksByTaskName);

        // Combine results and build final task list
        return packagedFuture.thenCombine(subtaskFuture, this::mergeTaskData)
                .exceptionally(ex -> {
                    log.error("Failed to list tasks under prefix {}", prefix, ex);
                    return List.of();
                });
    }

    private Set<String> extractPackagedTaskNames(List<String> packagedPaths) {
        if (packagedPaths == null || packagedPaths.isEmpty()) {
            return Set.of();
        }

        return packagedPaths.stream()
                .map(path -> path.split("/", 4)) // Limit splits for efficiency
                .filter(parts -> parts.length >= 3)
                .map(parts -> parts[2])
                .collect(Collectors.toSet());
    }

    private Map<String, List<String>> groupSubtasksByTaskName(List<String> taskNodes) {
        if (taskNodes == null || taskNodes.isEmpty()) {
            return Map.of();
        }

        Map<String, List<String>> taskMap = new HashMap<>();

        for (String node : taskNodes) {
            String[] parts = node.split("/", 5); // Limit splits
            if (parts.length < 4) {
                log.warn("Invalid task name node: {}", node);
                continue;
            }

            String taskName = parts[2];
            taskMap.computeIfAbsent(taskName, k -> new ArrayList<>()).add(node);
        }

        return taskMap;
    }

    private List<PackagedCompactStreamTask> mergeTaskData(
            Set<String> packagedTaskNames,
            Map<String, List<String>> taskMap) {

        if (packagedTaskNames.isEmpty() && taskMap.isEmpty()) {
            return List.of();
        }

        Set<String> remainingPackagedTaskNames = new HashSet<>(packagedTaskNames);
        List<PackagedCompactStreamTask> tasks = new ArrayList<>(remainingPackagedTaskNames.size());

        // Add tasks that have subtasks
        taskMap.forEach((taskName, subtasks) -> {
            if (remainingPackagedTaskNames.remove(taskName)) {
                tasks.add(new PackagedCompactStreamTask(taskName, subtasks));
            }
        });

        // Add remaining packaged tasks without subtasks
        remainingPackagedTaskNames.forEach(taskName ->
                tasks.add(new PackagedCompactStreamTask(taskName, List.of()))
        );

        return tasks;
    }

    @Override
    public void publishPackagedTaskName(String taskName) throws ExecutionException, InterruptedException {
        String taskNameKey = buildPackageTaskKey(taskName);
        oxiaClient.put(taskNameKey, new byte[0]).get();
        log.debug("Publish packaged task name {}", taskNameKey);
    }

    @Override
    public void publishDLQPackagedTaskName(String taskName) throws ExecutionException, InterruptedException {
        String taskNameKey = buildDLQPackageTaskKey(taskName);
        oxiaClient.put(taskNameKey, new byte[0]).get();
        log.debug("Publish DLQ packaged task name {}", taskNameKey);
    }

    private String getPackagedTaskStatus(String taskName) throws ExecutionException, InterruptedException {
        String taskNameKey = buildPackageTaskKey(taskName);
        GetResult getResult = oxiaClient.get(taskNameKey).get();
        if (getResult != null) {
            return new String(getResult.value(), StandardCharsets.UTF_8);
        }
        return null;
    }

    @Override
    public CompletableFuture<Boolean> deletePackagedTaskName(String taskName) {
        String taskNameKey = buildPackageTaskKey(taskName);
        log.debug("Delete packaged task name {}", taskNameKey);
        return oxiaClient.delete(taskNameKey);
    }

    @Override
    public void deleteDLQPackagedTaskName(String taskName) throws ExecutionException, InterruptedException {
        String taskNameKey = buildDLQPackageTaskKey(taskName);
        oxiaClient.delete(taskNameKey).get();
        log.debug("Delete DLQ packaged task name {}", taskNameKey);
    }

    public void moveTaskToDLQ(List<CompactStreamTask> tasks) {
        try {
            List<CompletableFuture<Void>> futures = new ArrayList<>();
            for (CompactStreamTask task : tasks) {
                if (task.getStatus() == CompactStreamTask.INIT
                    || task.getStatus() == CompactStreamTask.COMMITTED
                    || task.getTaskQueueType() == CompactStreamTask.TaskQueueType.DLQ) {
                    continue;
                }
                // add task to DLQ
                task.setTaskQueueType(CompactStreamTask.TaskQueueType.DLQ);
                publishCompactTask(task);
                publishDLQPackagedTaskName(task.getTaskName());
                // delete the task from normal task queue
                task.setTaskQueueType(CompactStreamTask.TaskQueueType.NORMAL);
                futures.add(deleteCompactTask(task));
            }
            CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).get();
            futures.clear();
        } catch (Throwable e) {
            log.error("Failed to add compact task to DLQ, tasks: {} ", tasks, e);
        }
    }

    @Override
    public void publishCompactTask(CompactStreamTask compactStreamTask)
            throws IOException, ExecutionException, InterruptedException {
        String subTask = buildSubTaskKey(compactStreamTask);
        byte[] data = CompactStreamTaskSerde.INSTANCE.serialize(compactStreamTask);
        oxiaClient.put(subTask, data,
                Set.of(PutOption.PartitionKey(String.valueOf(compactStreamTask.getStreamId())),
                        PutOption.IfRecordDoesNotExist)).get();
        log.debug("Publish CompactStream task {}", subTask);
    }

    @Override
    public boolean publishCompactTaskIfAbsent(CompactStreamTask compactStreamTask)
            throws IOException, ExecutionException, InterruptedException {
        try {
            publishCompactTask(compactStreamTask);
            return true;
        } catch (ExecutionException error) {
            if (error.getCause() instanceof KeyAlreadyExistsException) {
                return false;
            }
            throw error;
        }
    }

    public CompletableFuture<Void> updateCompactTask(CompactStreamTask compactStreamTask) {
        try {
            String subTask = buildSubTaskKey(compactStreamTask);
            byte[] data = CompactStreamTaskSerde.INSTANCE.serialize(compactStreamTask);
            return oxiaClient.put(
                    subTask,
                    data,
                    Set.of(PutOption.PartitionKey(String.valueOf(compactStreamTask.getStreamId())))
                )
                .thenAccept(result -> {
                    log.debug("Update CompactStream task {} status: {}", subTask, compactStreamTask.getStatus());
                })
                .exceptionally(ex -> {
                    log.error("Failed to update CompactStream task {}", subTask, ex);
                    throw new CompletionException(ex);
                });
        } catch (IOException e) {
            return CompletableFuture.failedFuture(e);
        }
    }

    @Override
    public CompletableFuture<CompactStreamTask> getCompactStreamTask(String subTask) {
        long streamId = parseStreamId(subTask);
        return oxiaClient
                .get(subTask, Set.of(GetOption.PartitionKey(String.valueOf(streamId))))
                .thenApplyAsync(getResult -> { // Use fork-join pool for deserialization
                    if (getResult == null) {
                        return null;
                    }
                    try {
                        return CompactStreamTaskSerde.INSTANCE.deserialize(getResult.value());
                    } catch (IOException | ClassNotFoundException e) {
                        throw new CompletionException(e);
                    }
                });
    }

    @Override
    public CompletableFuture<Void> deleteCompactTask(CompactStreamTask compactStreamTask) {
        String subTask = buildSubTaskKey(compactStreamTask);
        return oxiaClient.delete(subTask,
                Set.of(DeleteOption.PartitionKey(String.valueOf(compactStreamTask.getStreamId()))))
            .thenAccept(result -> log.debug("Delete CompactStream task: {}", subTask))
            .exceptionally(ex -> {
                log.error("Failed to delete CompactStream task {}", subTask, ex);
                throw new CompletionException(ex);
            });
    }

    private long parseStreamId(String subTask) {
        String[] split = subTask.split("/");
        String lastTxt = split[split.length - 1];
        return Long.parseLong(lastTxt.split("-")[0]);
    }

    @Override
    public PreparedCompactStreamTask getPreparedStreamTask(long streamId)
            throws ExecutionException, InterruptedException, IOException {
        String key = buildPreparedTaskKey(streamId);
        GetResult getResult =
                oxiaClient.get(key, Set.of(GetOption.PartitionKey(String.valueOf(streamId)))).get();
        if (getResult == null) {
            return null;
        }
        return PreparedCompactStreamTaskSerde.INSTANCE.deserialize(getResult.value());
    }

    @Override
    public PreparedCompactStreamTask getPreparedStreamTask(String name)
        throws ExecutionException, InterruptedException, IOException {
        String key = buildPreparedTaskKey(name);
        GetResult getResult =
            oxiaClient.get(key, Set.of(GetOption.PartitionKey(name))).get();
        if (getResult == null) {
            return null;
        }
        return PreparedCompactStreamTaskSerde.INSTANCE.deserialize(getResult.value());
    }

    @Override
    public void publishPreparedCompactTask(PreparedCompactStreamTask preparedTask, Optional<String> name)
            throws IOException, ExecutionException, InterruptedException {

        long streamId = preparedTask.getStreamId();
        String key = name.map(OxiaCompactTaskManager::buildPreparedTaskKey)
            .orElseGet(() -> buildPreparedTaskKey(streamId));
        String partitionKey = name.orElseGet(() -> String.valueOf(streamId));
        byte[] serialize = PreparedCompactStreamTaskSerde.INSTANCE.serialize(preparedTask);
        oxiaClient.put(key, serialize,
                Set.of(PutOption.PartitionKey(partitionKey), PutOption.IfRecordDoesNotExist)).get();
    }

    @Override
    public void updatePreparedCompactTask(PreparedCompactStreamTask preparedTask, Optional<String> name)
            throws IOException, ExecutionException, InterruptedException {
        long streamId = preparedTask.getStreamId();
        String key = name.map(OxiaCompactTaskManager::buildPreparedTaskKey)
            .orElseGet(() -> buildPreparedTaskKey(streamId));
        String partitionKey = name.orElseGet(() -> String.valueOf(streamId));
        byte[] serialize = PreparedCompactStreamTaskSerde.INSTANCE.serialize(preparedTask);
        oxiaClient.put(key, serialize,
                Set.of(PutOption.PartitionKey(partitionKey))).get();
    }

    @Override
    public void deletePreparedCompactTask(long streamId) throws ExecutionException, InterruptedException {
        String key = buildPreparedTaskKey(streamId);
        oxiaClient.delete(key, Set.of(DeleteOption.PartitionKey(String.valueOf(streamId)))).get();
    }

    @Override
    public void deletePreparedCompactTask(String name) throws ExecutionException, InterruptedException {
        String key = buildPreparedTaskKey(name);
        oxiaClient.delete(key, Set.of(DeleteOption.PartitionKey(name))).get();
    }

    @Override
    public CompactedOffset getPublishedOffset(long streamId)
            throws ExecutionException, InterruptedException, IOException {
        GetResult getResult = oxiaClient.get(publishedOffsetKey(streamId),
                Set.of(GetOption.PartitionKey(String.valueOf(streamId)))).get();
        if (getResult == null) {
            return null;
        }
        return CompactOffsetSerde.INSTANCE.deserialize(getResult.value());
    }

    @Override
    public CompactedOffset getPublishedOffset(String name)
        throws ExecutionException, InterruptedException, IOException {
        GetResult getResult = oxiaClient.get(publishedOffsetKey(name),
            Set.of(GetOption.PartitionKey(name))).get();
        if (getResult == null) {
            return null;
        }
        return CompactOffsetSerde.INSTANCE.deserialize(getResult.value());
    }

    @Override
    public void updatePublishedOffset(long streamId, long offset, long cumulativeSize)
            throws IOException, ExecutionException, InterruptedException {
        CompactedOffset compactedOffset = new CompactedOffset(streamId, offset, cumulativeSize);
        byte[] data = CompactOffsetSerde.INSTANCE.serialize(compactedOffset);
        String key = publishedOffsetKey(streamId);
        oxiaClient.put(key, data, Set.of(PutOption.PartitionKey(String.valueOf(streamId)))).get();
        log.debug("Update compacted {} offset {}", key, compactedOffset);
    }

    @Override
    public void updatePublishedOffset(String name, long streamId, long offset)
        throws IOException, ExecutionException, InterruptedException {
        CompactedOffset compactedOffset = new CompactedOffset(streamId, offset, 0);
        byte[] data = CompactOffsetSerde.INSTANCE.serialize(compactedOffset);
        String key = publishedOffsetKey(name);
        oxiaClient.put(key, data, Set.of(PutOption.PartitionKey(name))).get();
        log.debug("Update compacted {} offset with offset: {}:{}", key, streamId, offset);
    }

    @Override
    public Optional<PublicationLease> tryAcquirePublicationLease(String name, long streamId)
            throws ExecutionException, InterruptedException {
        String ownerId = UUID.randomUUID().toString();
        String key = publicationLeaseKey(name);
        byte[] value = publicationLeaseValue(streamId, ownerId);
        try {
            var result = oxiaClient.put(key, value,
                    Set.of(PutOption.PartitionKey(name), PutOption.IfRecordDoesNotExist,
                            PutOption.AsEphemeralRecord)).get();
            return Optional.of(new PublicationLease(name, streamId, ownerId,
                    result.version().versionId()));
        } catch (ExecutionException error) {
            if (error.getCause() instanceof KeyAlreadyExistsException) {
                return Optional.empty();
            }
            throw error;
        }
    }

    @Override
    public boolean validatePublicationLease(PublicationLease lease)
            throws ExecutionException, InterruptedException {
        GetResult result = oxiaClient.get(publicationLeaseKey(lease.name()),
                Set.of(GetOption.PartitionKey(lease.name()))).get();
        return result != null
                && result.version().versionId() == lease.revision()
                && Arrays.equals(result.value(), publicationLeaseValue(lease.streamId(), lease.ownerId()));
    }

    @Override
    public boolean releasePublicationLease(PublicationLease lease)
            throws ExecutionException, InterruptedException {
        try {
            return oxiaClient.delete(publicationLeaseKey(lease.name()),
                    Set.of(DeleteOption.PartitionKey(lease.name()),
                            DeleteOption.IfVersionIdEquals(lease.revision()))).get();
        } catch (ExecutionException error) {
            if (error.getCause() instanceof UnexpectedVersionIdException) {
                return false;
            }
            throw error;
        }
    }

    @Override
    public PublishedOffsetClaim claimPublishedOffset(PublicationLease lease)
            throws IOException, ExecutionException, InterruptedException {
        String key = publishedOffsetKey(lease.name());
        if (!validatePublicationLease(lease)) {
            throw fenced(lease, null);
        }
        GetResult current = oxiaClient.get(key, Set.of(GetOption.PartitionKey(lease.name()))).get();
        if (!validatePublicationLease(lease)) {
            throw fenced(lease, null);
        }
        CompactedOffset currentOffset = current == null
                ? null : CompactOffsetSerde.INSTANCE.deserialize(current.value());
        CompactedOffset claimedOffset = currentOffset != null && currentOffset.getId() == lease.streamId()
                ? currentOffset : new CompactedOffset(lease.streamId(), -1L, 0L);
        Set<PutOption> options = current == null
                ? Set.of(PutOption.PartitionKey(lease.name()), PutOption.IfRecordDoesNotExist)
                : Set.of(PutOption.PartitionKey(lease.name()),
                        PutOption.IfVersionIdEquals(current.version().versionId()));
        try {
            var result = oxiaClient.put(key, CompactOffsetSerde.INSTANCE.serialize(claimedOffset), options).get();
            return new PublishedOffsetClaim(claimedOffset, result.version().versionId());
        } catch (ExecutionException error) {
            if (error.getCause() instanceof KeyAlreadyExistsException
                    || error.getCause() instanceof UnexpectedVersionIdException) {
                throw fenced(lease, error.getCause());
            }
            throw error;
        }
    }

    @Override
    public PublishedOffsetClaim compareAndSetPublishedOffset(
            PublicationLease lease,
            PublishedOffsetClaim expected,
            CompactedOffset updated)
            throws IOException, ExecutionException, InterruptedException {
        if (expected.offset().getId() != lease.streamId() || updated.getId() != lease.streamId()) {
            throw new IllegalArgumentException("The published-offset cursor must belong to the leased stream");
        }
        if (updated.getOffset() < expected.offset().getOffset()) {
            throw new IllegalArgumentException("The published-offset cursor cannot move backwards");
        }
        if (!validatePublicationLease(lease)) {
            throw fenced(lease, null);
        }
        try {
            var result = oxiaClient.put(publishedOffsetKey(lease.name()),
                    CompactOffsetSerde.INSTANCE.serialize(updated),
                    Set.of(PutOption.PartitionKey(lease.name()),
                            PutOption.IfVersionIdEquals(expected.revision()))).get();
            return new PublishedOffsetClaim(updated, result.version().versionId());
        } catch (ExecutionException error) {
            if (error.getCause() instanceof UnexpectedVersionIdException
                    || error.getCause() instanceof KeyAlreadyExistsException) {
                throw fenced(lease, error.getCause());
            }
            throw error;
        }
    }

    @Override
    public Optional<PreparedTaskClaim> getPreparedTaskClaim(String name)
            throws IOException, ExecutionException, InterruptedException {
        String key = buildPreparedTaskKey(name);
        GetResult result = oxiaClient.get(key, Set.of(GetOption.PartitionKey(name))).get();
        if (result == null) {
            return Optional.empty();
        }
        return Optional.of(new PreparedTaskClaim(
                PreparedCompactStreamTaskSerde.INSTANCE.deserialize(result.value()),
                result.version().versionId()));
    }

    @Override
    public Optional<PreparedTaskClaim> tryCreatePreparedTaskClaim(
            PreparedCompactStreamTask preparedTask,
            String name)
            throws IOException, ExecutionException, InterruptedException {
        String key = buildPreparedTaskKey(name);
        byte[] value = PreparedCompactStreamTaskSerde.INSTANCE.serialize(preparedTask);
        try {
            var result = oxiaClient.put(key, value,
                    Set.of(PutOption.PartitionKey(name), PutOption.IfRecordDoesNotExist)).get();
            return Optional.of(new PreparedTaskClaim(preparedTask, result.version().versionId()));
        } catch (ExecutionException error) {
            if (error.getCause() instanceof KeyAlreadyExistsException) {
                return Optional.empty();
            }
            throw error;
        }
    }

    @Override
    public boolean deletePreparedTaskClaim(String name, PreparedTaskClaim claim)
            throws ExecutionException, InterruptedException {
        try {
            return oxiaClient.delete(buildPreparedTaskKey(name),
                    Set.of(DeleteOption.PartitionKey(name),
                            DeleteOption.IfVersionIdEquals(claim.revision()))).get();
        } catch (ExecutionException error) {
            if (error.getCause() instanceof UnexpectedVersionIdException) {
                return false;
            }
            throw error;
        }
    }

    private static String publishedOffsetKey(long streamId) {
        return String.format("compact-offset-%020d", streamId);
    }

    private static String publishedOffsetKey(String name) {
        return String.format("compact-offset-%s", name);
    }

    private static String publicationLeaseKey(String name) {
        String encodedName = Base64.getUrlEncoder().withoutPadding()
                .encodeToString(name.getBytes(StandardCharsets.UTF_8));
        return "publication-lease-" + encodedName;
    }

    private static byte[] publicationLeaseValue(long streamId, String ownerId) {
        return (streamId + "\n" + ownerId).getBytes(StandardCharsets.UTF_8);
    }

    private static PublicationFencedException fenced(PublicationLease lease, Throwable cause) {
        String message = "Publication lease for " + lease.name() + " and stream " + lease.streamId()
                + " is no longer current";
        return cause == null
                ? new PublicationFencedException(message)
                : new PublicationFencedException(message, cause);
    }

    private static String committedOffsetKey(long streamId) {
        return String.format("committed-offset-%020d", streamId);
    }

    private static String committedOffsetKey(String name) {
        return String.format("committed-offset-%s", name);
    }

    private static String buildPreparedTaskKey(long streamId) {
        return String.format("prepared-task-%020d", streamId);
    }

    private static String buildPreparedTaskKey(String name) {
        return String.format("prepared-task-%s", name);
    }

    private static String buildPackageTaskKey(String taskName) {
        return TASK_PREFIX + "/" + taskName;
    }

    private static String buildDLQPackageTaskKey(String taskName) {
        return TASK_PREFIX_DLQ + "/" + taskName;
    }

    public static String buildSubTaskKey(CompactStreamTask compactStream) {
        if (compactStream.getTaskQueueType() == CompactStreamTask.TaskQueueType.DLQ) {
            return buildDLQSubTaskKey(compactStream);
        }
        return buildNormalSubTaskKey(compactStream);
    }

    public static String buildDLQSubTaskKey(CompactStreamTask compactStream) {
        return buildDLQPackageTaskKey(compactStream.getTaskName()) + "/" + compactStream.getStreamId() + "-"
                + compactStream.getStartOffset() + "-" + compactStream.getEndOffset();
    }

    public static String buildNormalSubTaskKey(CompactStreamTask compactStream) {
        return buildPackageTaskKey(compactStream.getTaskName()) + "/" + compactStream.getStreamId() + "-"
                + compactStream.getStartOffset() + "-" + compactStream.getEndOffset();
    }

    private String buildLockKey(String taskName) {
        return TASK_LOCK_PREFIX + taskName;
    }

    @Override
    public boolean tryLockTask(String taskName) {
        if (lockManager == null) {
            throw new IllegalStateException("LockManager is not initialized.");
        }

        String lockKey = buildLockKey(taskName);
        AsyncLock lock = lockManager.getThreadSimpleLock(lockKey);
        try {
            lock.tryLock().get();
            return true;
        } catch (InterruptedException e) {
            // Restore the interrupted status
            Thread.currentThread().interrupt();
            log.warn("Thread was interrupted while trying to acquire lock for task {}", taskName, e);
            return false;
        } catch (ExecutionException e) {
            // Log the underlying cause of the failure
            log.warn("Failed to acquire lock for task {}, exception: {}", taskName, e.getMessage());
            if (e.getCause() instanceof LockException.LockBusyException) {
                lockManager.removeLock(lockKey);
            }
            return false;
        }
    }

    @Override
    public void unlockTask(String taskName) throws IllegalStateException, InterruptedException, ExecutionException {
        if (lockManager == null) {
            throw new IllegalStateException("LockManager is not initialized.");
        }

        AsyncLock lock = lockManager.getThreadSimpleLock(buildLockKey(taskName));
        lock.unlock().get();
    }

    @Override
    public void unlockTaskAndRemoveLock(String taskName)
        throws IllegalStateException, InterruptedException, ExecutionException {
        if (lockManager == null) {
            throw new IllegalStateException("LockManager is not initialized.");
        }

        String lockKey = buildLockKey(taskName);
        AsyncLock lock = lockManager.getThreadSimpleLock(lockKey);
        try {
            lock.unlock().get();
        } finally {
            lockManager.removeLock(lockKey);
        }
    }

}
