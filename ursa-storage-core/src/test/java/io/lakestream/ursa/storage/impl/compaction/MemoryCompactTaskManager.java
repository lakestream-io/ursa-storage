/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.storage.impl.compaction;

import io.lakestream.ursa.compaction.CompactTaskManager;
import io.lakestream.ursa.compaction.PublicationFencedException;
import io.lakestream.ursa.compaction.task.CompactStreamTask;
import io.lakestream.ursa.compaction.task.CompactedOffset;
import io.lakestream.ursa.compaction.task.PackagedCompactStreamTask;
import io.lakestream.ursa.compaction.task.PreparedCompactStreamTask;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class MemoryCompactTaskManager implements CompactTaskManager {

    private final Map<String, PackagedCompactStreamTask> packagedTasks = new HashMap<>();
    private final Map<String, PackagedCompactStreamTask> dLQPackagedTasks = new HashMap<>();
    private final Map<String, CompactStreamTask> compactTasks = new HashMap<>();
    private final Map<String, PreparedCompactStreamTask> preparedTasks = new HashMap<>();
    private final Map<Object, CompactedOffset> publishedOffsets = new HashMap<>();
    private final Map<Object, CompactedOffset> committedOffsets = new HashMap<>();
    private final Set<String> taskLocks = new HashSet<>();
    private final Map<String, PublicationLease> publicationLeases = new HashMap<>();
    private final Map<String, Long> publishedOffsetVersions = new HashMap<>();
    private final Map<String, Long> preparedTaskVersions = new HashMap<>();
    private long nextRevision = 1L;

    public CompletableFuture<List<PackagedCompactStreamTask>> getAllTasks() {
        var tasks = packagedTasks.values().stream()
                .map(task -> {
                    var clone = new PackagedCompactStreamTask(task.getTaskName(), task.getSubTasks()); // if needed
                    var sub = compactTasks.get(clone.getTaskName());
                    if (sub != null) {
                        clone.getSubTasks().add(sub.getTaskName());
                    }
                    return clone;
                })
                .collect(Collectors.toList());

        return CompletableFuture.completedFuture(tasks);
    }

    @Override
    public CompletableFuture<List<PackagedCompactStreamTask>> getAllDLQTasks() {
        return CompletableFuture.completedFuture(new ArrayList<>(dLQPackagedTasks.values()));
    }

    @Override
    public void publishPackagedTaskName(String taskName) {
        packagedTasks.put(taskName, new PackagedCompactStreamTask(taskName, new ArrayList<>()));
    }

    @Override
    public void publishDLQPackagedTaskName(String taskName) {
        dLQPackagedTasks.put(taskName, new PackagedCompactStreamTask(taskName, new ArrayList<>()));
    }

    @Override
    public CompletableFuture<Boolean> deletePackagedTaskName(String taskName) {
        return CompletableFuture.completedFuture(packagedTasks.remove(taskName) != null);
    }

    @Override
    public synchronized CompletableFuture<Boolean> deletePackagedTaskNameIfEmpty(String taskName) {
        if (compactTasks.containsKey(taskName)) {
            return CompletableFuture.completedFuture(false);
        }
        return CompletableFuture.completedFuture(packagedTasks.remove(taskName) != null);
    }

    @Override
    public void deleteDLQPackagedTaskName(String taskName) {
        dLQPackagedTasks.remove(taskName);
    }

    @Override
    public void publishCompactTask(CompactStreamTask task) {
        compactTasks.put(task.getTaskName(), task);
    }

    @Override
    public synchronized boolean publishCompactTaskIfAbsent(CompactStreamTask task) {
        if (compactTasks.containsKey(task.getTaskName())) {
            return false;
        }
        compactTasks.put(task.getTaskName(), task);
        return true;
    }

    @Override
    public CompletableFuture<Void> updateCompactTask(CompactStreamTask task) {
        compactTasks.put(task.getTaskName(), task);
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletableFuture<CompactStreamTask> getCompactStreamTask(String subTask) {
        CompletableFuture<CompactStreamTask> future = new CompletableFuture<>();
        CompactStreamTask task = compactTasks.get(subTask);
        if (task == null) {
            future.completeExceptionally(new ClassNotFoundException("No task found with subtask name: " + subTask));
        } else {
            future.complete(task);
        }
        return future;
    }

    @Override
    public CompletableFuture<Void> deleteCompactTask(CompactStreamTask task) {
        compactTasks.remove(task.getTaskName());
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public synchronized PreparedCompactStreamTask getPreparedStreamTask(String name) throws IOException {
        return preparedTasks.get(name);
    }

    @Override
    public PreparedCompactStreamTask getPreparedStreamTask(long streamId) throws IOException {
        return preparedTasks.get(String.valueOf(streamId));
    }


    @Override
    public synchronized void publishPreparedCompactTask(PreparedCompactStreamTask task, Optional<String> name) {
        String key = name.orElseGet(() -> String.valueOf(task.getStreamId()));
        preparedTasks.put(key, task);
        preparedTaskVersions.put(key, nextRevision++);
    }

    @Override
    public synchronized void updatePreparedCompactTask(PreparedCompactStreamTask task, Optional<String> name) {
        String key = name.orElseGet(() -> String.valueOf(task.getStreamId()));
        preparedTasks.put(key, task);
        preparedTaskVersions.put(key, nextRevision++);
    }

    @Override
    public synchronized void deletePreparedCompactTask(String name) {
        preparedTasks.remove(name);
        preparedTaskVersions.remove(name);
    }

    @Override
    public void deletePreparedCompactTask(long streamId) {
        preparedTasks.remove(String.valueOf(streamId));
    }

    @Override
    public CompactedOffset getPublishedOffset(long streamId) {
        return publishedOffsets.get(streamId);
    }

    @Override
    public CompactedOffset getPublishedOffset(String name) {
        return publishedOffsets.get(name);
    }

    @Override
    public void updatePublishedOffset(long streamId, long offset, long cumulativeSize) {
        publishedOffsets.put(streamId, new CompactedOffset(streamId, offset, cumulativeSize));
    }

    @Override
    public synchronized void updatePublishedOffset(String name, long streamId, long offset) {
        publishedOffsets.put(name, new CompactedOffset(streamId, offset, 0)); // cumulativeSize is 0 by default
        publishedOffsetVersions.put(name, nextRevision++);
    }

    @Override
    public synchronized Optional<PublicationLease> tryAcquirePublicationLease(String name, long streamId) {
        if (publicationLeases.containsKey(name)) {
            return Optional.empty();
        }
        PublicationLease lease = new PublicationLease(
                name, streamId, UUID.randomUUID().toString(), nextRevision++);
        publicationLeases.put(name, lease);
        return Optional.of(lease);
    }

    @Override
    public synchronized boolean validatePublicationLease(PublicationLease lease) {
        return lease.equals(publicationLeases.get(lease.name()));
    }

    @Override
    public synchronized boolean releasePublicationLease(PublicationLease lease) {
        return publicationLeases.remove(lease.name(), lease);
    }

    @Override
    public synchronized PublishedOffsetClaim claimPublishedOffset(PublicationLease lease) {
        ensureCurrentLease(lease);
        CompactedOffset current = publishedOffsets.get(lease.name());
        CompactedOffset claimed = current != null && current.getId() == lease.streamId()
                ? current : new CompactedOffset(lease.streamId(), -1L, 0L);
        long revision = nextRevision++;
        publishedOffsets.put(lease.name(), claimed);
        publishedOffsetVersions.put(lease.name(), revision);
        return new PublishedOffsetClaim(claimed, revision);
    }

    @Override
    public synchronized PublishedOffsetClaim compareAndSetPublishedOffset(
            PublicationLease lease,
            PublishedOffsetClaim expected,
            CompactedOffset updated) {
        ensureCurrentLease(lease);
        Long currentRevision = publishedOffsetVersions.get(lease.name());
        if (currentRevision == null || currentRevision != expected.revision()) {
            throw new PublicationFencedException("Published-offset revision is no longer current");
        }
        if (expected.offset().getId() != lease.streamId() || updated.getId() != lease.streamId()) {
            throw new IllegalArgumentException("The published-offset cursor must belong to the leased stream");
        }
        if (updated.getOffset() < expected.offset().getOffset()) {
            throw new IllegalArgumentException("The published-offset cursor cannot move backwards");
        }
        long revision = nextRevision++;
        publishedOffsets.put(lease.name(), updated);
        publishedOffsetVersions.put(lease.name(), revision);
        return new PublishedOffsetClaim(updated, revision);
    }

    @Override
    public synchronized Optional<PreparedTaskClaim> getPreparedTaskClaim(String name) {
        PreparedCompactStreamTask task = preparedTasks.get(name);
        if (task == null) {
            return Optional.empty();
        }
        return Optional.of(new PreparedTaskClaim(task, preparedTaskVersions.get(name)));
    }

    @Override
    public synchronized Optional<PreparedTaskClaim> tryCreatePreparedTaskClaim(
            PreparedCompactStreamTask task,
            String name) {
        if (preparedTasks.containsKey(name)) {
            return Optional.empty();
        }
        long revision = nextRevision++;
        preparedTasks.put(name, task);
        preparedTaskVersions.put(name, revision);
        return Optional.of(new PreparedTaskClaim(task, revision));
    }

    @Override
    public synchronized boolean deletePreparedTaskClaim(String name, PreparedTaskClaim claim) {
        Long currentRevision = preparedTaskVersions.get(name);
        if (currentRevision == null || currentRevision != claim.revision()) {
            return false;
        }
        preparedTaskVersions.remove(name);
        preparedTasks.remove(name);
        return true;
    }

    private void ensureCurrentLease(PublicationLease lease) {
        if (!validatePublicationLease(lease)) {
            throw new PublicationFencedException("Publication lease is no longer current");
        }
    }

    @Override
    public boolean tryLockTask(String taskName) {
        return taskLocks.add(taskName);
    }

    @Override
    public void unlockTask(String taskName) throws InterruptedException, ExecutionException, IllegalStateException {
        taskLocks.remove(taskName);
    }

    @Override
    public void unlockTaskAndRemoveLock(String taskName)
        throws InterruptedException, ExecutionException, IllegalStateException {
        taskLocks.remove(taskName);
    }

    @Override
    public void moveTaskToDLQ(List<CompactStreamTask> tasks) {
        // do nothing
    }
}
