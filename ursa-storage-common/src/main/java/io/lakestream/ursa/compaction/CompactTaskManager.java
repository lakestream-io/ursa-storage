/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.compaction;

import io.lakestream.ursa.compaction.task.CompactStreamTask;
import io.lakestream.ursa.compaction.task.CompactedOffset;
import io.lakestream.ursa.compaction.task.PackagedCompactStreamTask;
import io.lakestream.ursa.compaction.task.PreparedCompactStreamTask;
import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

public interface CompactTaskManager {

    /**
     * Opaque ownership token for a named compaction-task publisher.
     *
     * <p>The revision is interpreted only by the task-manager implementation. Callers must pass the
     * complete token back unchanged and must never attempt to refresh a fenced lease.
     */
    record PublicationLease(String name, long streamId, String ownerId, long revision) {
        public PublicationLease {
            Objects.requireNonNull(name, "name");
            Objects.requireNonNull(ownerId, "ownerId");
        }
    }

    /** A published-offset value together with its opaque storage revision. */
    record PublishedOffsetClaim(CompactedOffset offset, long revision) {
        public PublishedOffsetClaim {
            Objects.requireNonNull(offset, "offset");
        }
    }

    /** A prepared task together with its opaque storage revision. */
    record PreparedTaskClaim(PreparedCompactStreamTask task, long revision) {
        public PreparedTaskClaim {
            Objects.requireNonNull(task, "task");
        }
    }

    CompletableFuture<List<PackagedCompactStreamTask>> getAllTasks();

    CompletableFuture<List<PackagedCompactStreamTask>> getAllDLQTasks();

    void publishPackagedTaskName(String taskName) throws ExecutionException, InterruptedException;

    void publishDLQPackagedTaskName(String taskName) throws ExecutionException, InterruptedException;

    CompletableFuture<Boolean> deletePackagedTaskName(String taskName);

    /**
     * Deletes a package marker only when a fresh storage read confirms that it has no subtasks.
     *
     * <p>The check and conditional marker deletion must be safe against a publisher concurrently
     * making a package visible.
     */
    CompletableFuture<Boolean> deletePackagedTaskNameIfEmpty(String taskName);

    void deleteDLQPackagedTaskName(String taskName) throws ExecutionException, InterruptedException;

    void publishCompactTask(CompactStreamTask compactStreamTask)
            throws IOException, ExecutionException, InterruptedException;

    CompletableFuture<Void> updateCompactTask(CompactStreamTask compactStreamTask);

    CompletableFuture<CompactStreamTask> getCompactStreamTask(String subTask);

    CompletableFuture<Void> deleteCompactTask(CompactStreamTask compactStreamTask);

    PreparedCompactStreamTask getPreparedStreamTask(long streamId)
            throws ExecutionException, InterruptedException, IOException;

    PreparedCompactStreamTask getPreparedStreamTask(String key)
        throws ExecutionException, InterruptedException, IOException;

    void publishPreparedCompactTask(PreparedCompactStreamTask preparedTask, Optional<String> key)
            throws IOException, ExecutionException, InterruptedException;

    void updatePreparedCompactTask(PreparedCompactStreamTask preparedTask, Optional<String> key)
            throws IOException, ExecutionException, InterruptedException;

    void deletePreparedCompactTask(long streamId) throws ExecutionException, InterruptedException;
    void deletePreparedCompactTask(String key) throws ExecutionException, InterruptedException;

    CompactedOffset getPublishedOffset(long streamId)
            throws ExecutionException, InterruptedException, IOException;

    CompactedOffset getPublishedOffset(String name)
        throws ExecutionException, InterruptedException, IOException;

    /** Records the last logical offset included in a published task for a stream. */
    void updatePublishedOffset(long streamId, long offset, long cumulativeSize)
            throws IOException, ExecutionException, InterruptedException;

    /**
     * Records the last logical offset and cumulative byte size included in a published task under a
     * named key.
     */
    void updatePublishedOffset(String name, long streamId, long offset, long cumulativeSize)
            throws IOException, ExecutionException, InterruptedException;

    /**
     * Attempts to acquire the long-lived publication lease for a named stream.
     *
     * <p>A lease is held for the lifetime of one publisher, rather than for one publication tick.
     * This prevents an obsolete stream incarnation from reacquiring the lock between ticks.
     */
    Optional<PublicationLease> tryAcquirePublicationLease(String name, long streamId)
            throws ExecutionException, InterruptedException;

    /** Returns whether the exact lease is still the current owner. */
    boolean validatePublicationLease(PublicationLease lease)
            throws ExecutionException, InterruptedException;

    /** Conditionally releases the exact lease without affecting a successor lease. */
    boolean releasePublicationLease(PublicationLease lease)
            throws ExecutionException, InterruptedException;

    /**
     * Claims the named published-offset cursor for this publisher session.
     *
     * <p>Claiming the same stream preserves its offset while changing the cursor revision. Claiming
     * a different stream starts that stream incarnation at offset {@code -1}. The returned revision
     * fences cursor writes from previous publisher sessions.
     */
    PublishedOffsetClaim claimPublishedOffset(PublicationLease lease)
            throws IOException, ExecutionException, InterruptedException;

    /** Advances a named cursor only if {@code expected} is still its exact storage revision. */
    PublishedOffsetClaim compareAndSetPublishedOffset(
            PublicationLease lease,
            PublishedOffsetClaim expected,
            CompactedOffset updated)
            throws IOException, ExecutionException, InterruptedException;

    /** Returns the current prepared task and revision for a named publication key, if any. */
    Optional<PreparedTaskClaim> getPreparedTaskClaim(String name)
            throws IOException, ExecutionException, InterruptedException;

    /** Creates a prepared-task claim only when no claim exists. */
    Optional<PreparedTaskClaim> tryCreatePreparedTaskClaim(
            PreparedCompactStreamTask preparedTask,
            String name)
            throws IOException, ExecutionException, InterruptedException;

    /** Conditionally deletes the exact prepared-task claim. */
    boolean deletePreparedTaskClaim(String name, PreparedTaskClaim claim)
            throws ExecutionException, InterruptedException;

    /**
     * Stages a compact subtask if it is absent.
     *
     * @return {@code true} when this call created the subtask, or {@code false} when the same key
     *         was already staged
     */
    boolean publishCompactTaskIfAbsent(CompactStreamTask compactStreamTask)
            throws IOException, ExecutionException, InterruptedException;

    boolean tryLockTask(String taskName);

    void unlockTask(String taskName) throws InterruptedException, ExecutionException, IllegalStateException;

    void unlockTaskAndRemoveLock(String taskName)
        throws InterruptedException, ExecutionException, IllegalStateException;

    void moveTaskToDLQ(List<CompactStreamTask> tasks);
}
