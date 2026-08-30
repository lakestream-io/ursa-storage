/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.compaction;

import io.lakestream.ursa.compaction.metrics.CompactionMetrics;
import io.lakestream.ursa.compaction.task.CompactStreamTask;
import io.lakestream.ursa.compaction.task.CompactedOffset;
import io.lakestream.ursa.compaction.task.OffsetRange;
import io.lakestream.ursa.compaction.task.PreparedCompactStreamTask;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.oxia.client.api.exceptions.KeyAlreadyExistsException;
import java.io.IOException;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class CompactionManager {

    public enum PublicationResult {
        NO_TASK,
        PUBLISHED
    }

    @FunctionalInterface
    public interface PublicationTaskFactory {
        Optional<PreparedCompactStreamTask> create(PublicationCursor cursor) throws Exception;
    }

    /** Immutable cursor presented to a task factory while its publication lease is held. */
    public record PublicationCursor(long streamId, long offset, long cumulativeSize) {

        public PublicationCursor {
            if (streamId < 0) {
                throw new IllegalArgumentException("streamId must be non-negative");
            }
            if (offset < -1) {
                throw new IllegalArgumentException("offset must be at least -1");
            }
            if (cumulativeSize < 0) {
                throw new IllegalArgumentException("cumulativeSize must be non-negative");
            }
        }
    }

    private final CompactTaskManager taskManager;
    private final CompactionMetrics metrics;
    private final Set<CompactTaskManager.PublicationLease> pendingPublicationLeaseReleases =
            ConcurrentHashMap.newKeySet();

    public CompactionManager(CompactTaskManager taskManager) {
        this.taskManager = taskManager;
        this.metrics = CompactionMetrics.NOOP;
    }

    public CompactionManager(CompactTaskManager taskManager, CompactionMetrics compactionMetrics) {
        this.taskManager = taskManager;
        this.metrics = compactionMetrics;
    }

    /**
     * Attempts to open a long-lived, fenced publication session for one named stream incarnation.
     *
     * <p>The caller must retain the returned session for the lifetime of its publisher and close it
     * when that publisher loses ownership. A fenced session is terminal and must not be reused or
     * automatically reacquired by the same publisher instance.
     */
    public Optional<PublicationSession> tryOpenPublicationSession(String topicName, long streamId)
            throws Exception {
        retryPendingPublicationLeaseRelease(topicName);
        Optional<CompactTaskManager.PublicationLease> lease =
                taskManager.tryAcquirePublicationLease(topicName, streamId);
        if (lease.isEmpty()) {
            return Optional.empty();
        }
        try {
            CompactTaskManager.PublishedOffsetClaim cursor =
                    taskManager.claimPublishedOffset(lease.get());
            return Optional.of(new PublicationSession(lease.get(), cursor));
        } catch (Throwable error) {
            CompactTaskManager.PublicationLease acquiredLease = lease.get();
            pendingPublicationLeaseReleases.add(acquiredLease);
            try {
                retryPendingPublicationLeaseRelease(topicName);
            } catch (Throwable releaseError) {
                if (releaseError instanceof InterruptedException) {
                    Thread.currentThread().interrupt();
                }
                error.addSuppressed(releaseError);
            }
            throw error;
        }
    }

    /**
     * Retries every publication-lease release left unsettled by session acquisition or close.
     *
     * <p>This is intentionally independent of a later open attempt. A publisher can lose
     * leadership anywhere in its session lifecycle, so its owner must be able to drain the exact
     * failed lease even when it will never open that topic again.
     */
    public void retryPendingPublicationLeaseReleases() throws Exception {
        Throwable firstFailure = null;
        for (CompactTaskManager.PublicationLease pending : pendingPublicationLeaseReleases) {
            try {
                settlePendingPublicationLeaseRelease(pending);
            } catch (Throwable error) {
                if (error instanceof InterruptedException) {
                    Thread.currentThread().interrupt();
                }
                if (firstFailure == null) {
                    firstFailure = error;
                } else {
                    firstFailure.addSuppressed(error);
                }
            }
        }
        if (firstFailure != null) {
            if (firstFailure instanceof Exception exception) {
                throw exception;
            }
            if (firstFailure instanceof Error error) {
                throw error;
            }
            throw new RuntimeException(firstFailure);
        }
    }

    /** Returns whether an acquired lease still needs a confirmed release. */
    public boolean hasPendingPublicationLeaseReleases() {
        return !pendingPublicationLeaseReleases.isEmpty();
    }

    private void retryPendingPublicationLeaseRelease(String topicName) throws Exception {
        for (CompactTaskManager.PublicationLease pending : pendingPublicationLeaseReleases) {
            if (!topicName.equals(pending.name())) {
                continue;
            }
            settlePendingPublicationLeaseRelease(pending);
        }
    }

    private void settlePendingPublicationLeaseRelease(
            CompactTaskManager.PublicationLease pending) throws Exception {
        try {
            // Both true (released by this call) and false (already absent or superseded) settle the
            // exact lease. Only an exception leaves the handle pending for a later retry.
            taskManager.releasePublicationLease(pending);
            pendingPublicationLeaseReleases.remove(pending);
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw interrupted;
        }
    }

    /**
     * A long-lived publication owner that serializes recovery, cursor selection and task publication.
     */
    public final class PublicationSession implements AutoCloseable {

        private static final int MAX_PREPARED_CLAIM_RETRIES = 3;

        private final CompactTaskManager.PublicationLease lease;
        private final ReentrantReadWriteLock lifecycleLock = new ReentrantReadWriteLock();
        private final ReentrantLock publicationLock = new ReentrantLock();
        private CompactTaskManager.PublishedOffsetClaim cursor;
        private volatile boolean closed;
        private volatile boolean fenced;
        private volatile boolean leaseReleaseSettled;

        private PublicationSession(
                CompactTaskManager.PublicationLease lease,
                CompactTaskManager.PublishedOffsetClaim cursor) {
            this.lease = lease;
            this.cursor = cursor;
        }

        public String topicName() {
            return lease.name();
        }

        public long streamId() {
            return lease.streamId();
        }

        /**
         * Recovers any durable publication intent, reads the resulting cursor, builds the next task
         * and publishes it without releasing the publication lease between those phases.
         */
        public PublicationResult publishNext(PublicationTaskFactory taskFactory) throws Exception {
            lifecycleLock.readLock().lock();
            publicationLock.lock();
            try {
                ensureCurrentLease();
                for (int attempt = 0; attempt < MAX_PREPARED_CLAIM_RETRIES; attempt++) {
                    recoverPreparedTask();
                    PublicationCursor publicationCursor = publicationCursor();
                    Optional<PreparedCompactStreamTask> nextTask = taskFactory.create(publicationCursor);
                    ensureCurrentLease();
                    if (nextTask.isEmpty()) {
                        return PublicationResult.NO_TASK;
                    }
                    PreparedCompactStreamTask task = nextTask.get();
                    validateNextTask(task, publicationCursor);
                    Optional<CompactTaskManager.PreparedTaskClaim> preparedClaim =
                            taskManager.tryCreatePreparedTaskClaim(task, lease.name());
                    ensureCurrentLease();
                    if (preparedClaim.isEmpty()) {
                        continue;
                    }
                    publishPreparedTask(preparedClaim.get());
                    return PublicationResult.PUBLISHED;
                }
                throw new IllegalStateException("Failed to claim the prepared task for " + lease.name());
            } catch (PublicationFencedException error) {
                fenced = true;
                throw error;
            } finally {
                publicationLock.unlock();
                lifecycleLock.readLock().unlock();
            }
        }

        public boolean isClosed() {
            return closed;
        }

        public boolean isFenced() {
            return fenced;
        }

        /**
         * Immediately and permanently fences this local publisher without waiting for an in-flight
         * publication or performing any remote operation.
         *
         * <p>The owner-loss path should invoke this method synchronously before scheduling
         * {@link #close()} to release the remote lease.
         */
        public void fence() {
            fenced = true;
        }

        private void ensureCurrentLease() throws ExecutionException, InterruptedException {
            if (closed || fenced) {
                throw fencedException();
            }
            boolean current = taskManager.validatePublicationLease(lease);
            // Re-read the local state after the remote validation. Ownership loss can fence the
            // session while that validation is in flight.
            if (closed || fenced || !current) {
                fence();
                throw fencedException();
            }
        }

        private PublicationFencedException fencedException() {
            return new PublicationFencedException(
                    "Publication session for " + lease.name() + " is fenced");
        }

        private void recoverPreparedTask() throws Exception {
            Optional<CompactTaskManager.PreparedTaskClaim> prepared =
                    taskManager.getPreparedTaskClaim(lease.name());
            ensureCurrentLease();
            if (prepared.isEmpty()) {
                return;
            }
            CompactTaskManager.PreparedTaskClaim claim = prepared.get();
            PreparedCompactStreamTask task = claim.task();
            if (task.getStreamId() != lease.streamId()) {
                log.warn("Discarding prepared task {} for obsolete stream {} while stream {} owns {}",
                        task.getTaskName(), task.getStreamId(), lease.streamId(), lease.name());
                taskManager.deletePreparedTaskClaim(lease.name(), claim);
                ensureCurrentLease();
                return;
            }

            long targetOffset = OffsetRange.lastIncludedOffset(task.getStartOffset(), task.getEndOffset());
            PublicationCursor currentCursor = publicationCursor();
            long currentOffset = currentCursor.offset();
            if (currentOffset == task.getStartOffset() - 1) {
                validateNextTask(task, currentCursor);
                publishPreparedTask(claim);
                return;
            }
            if (currentOffset == targetOffset) {
                validateRecoveredCommittedTask(task, currentCursor);
                makeTaskVisible(task, claim);
                return;
            }
            if (currentOffset > targetOffset) {
                log.warn("Discarding obsolete prepared task {} ending at {} because cursor for {} is already {}",
                        task.getTaskName(), targetOffset, lease.name(), currentOffset);
                taskManager.deletePreparedTaskClaim(lease.name(), claim);
                ensureCurrentLease();
                return;
            }
            throw new IllegalStateException("Prepared task " + task.getTaskName() + " has range ["
                    + task.getStartOffset() + ", " + task.getEndOffset() + ") but cursor for "
                    + lease.name() + " is " + currentOffset);
        }

        private PublicationCursor publicationCursor() {
            CompactedOffset publishedOffset = cursor.offset();
            if (publishedOffset.getId() != lease.streamId()) {
                fence();
                throw fencedException();
            }
            return new PublicationCursor(
                    publishedOffset.getId(),
                    publishedOffset.getOffset(),
                    publishedOffset.getCumulativeSize());
        }

        private void validateRecoveredCommittedTask(
                PreparedCompactStreamTask task, PublicationCursor publishedCursor) {
            validateTaskIdentity(task);
            if (task.getTotalSize() <= 0
                    || task.getCumulativeSize() < task.getTotalSize()
                    || task.getCumulativeSize() != publishedCursor.cumulativeSize()) {
                throw new IllegalStateException(
                        "Prepared task cumulative size does not match the committed cursor");
            }
        }

        private void publishPreparedTask(CompactTaskManager.PreparedTaskClaim preparedClaim) throws Exception {
            PreparedCompactStreamTask task = preparedClaim.task();
            ensureCurrentLease();
            taskManager.publishCompactTaskIfAbsent(task.toCompactStreamTask());
            ensureCurrentLease();
            long targetOffset = OffsetRange.lastIncludedOffset(task.getStartOffset(), task.getEndOffset());
            CompactedOffset updated = new CompactedOffset(
                    lease.streamId(), targetOffset, task.getCumulativeSize());
            cursor = taskManager.compareAndSetPublishedOffset(lease, cursor, updated);
            ensureCurrentLease();
            makeTaskVisible(task, preparedClaim);
            ensureCurrentLease();
            metrics.getLatestPublishedOffset().set(targetOffset,
                    Attributes.of(AttributeKey.stringKey("topic"), lease.name()));
        }

        private void makeTaskVisible(
                PreparedCompactStreamTask task,
                CompactTaskManager.PreparedTaskClaim preparedClaim) throws Exception {
            // The package marker is the worker-visible commit point. It is written only after the
            // durable cursor covers this range, either through this publication or crash recovery.
            ensureCurrentLease();
            taskManager.publishCompactTaskIfAbsent(task.toCompactStreamTask());
            ensureCurrentLease();
            taskManager.publishPackagedTaskName(task.getTaskName());
            ensureCurrentLease();
            if (!taskManager.deletePreparedTaskClaim(lease.name(), preparedClaim)) {
                ensureCurrentLease();
                log.info("Prepared task {} was already recovered by another owner", task.getTaskName());
            }
            ensureCurrentLease();
        }

        private void validateNextTask(PreparedCompactStreamTask task, PublicationCursor publishedCursor) {
            validateTaskIdentity(task);
            if (task.getStartOffset() != Math.addExact(publishedCursor.offset(), 1L)) {
                throw new IllegalArgumentException("Prepared task must start immediately after the published cursor");
            }
            long expectedTotalSize = Math.subtractExact(
                    task.getCumulativeSize(), publishedCursor.cumulativeSize());
            if (expectedTotalSize <= 0 || task.getTotalSize() != expectedTotalSize) {
                throw new IllegalArgumentException(
                        "Prepared task total size must equal its cumulative-size advance");
            }
            OffsetRange.lastIncludedOffset(task.getStartOffset(), task.getEndOffset());
        }

        private void validateTaskIdentity(PreparedCompactStreamTask task) {
            if (!lease.name().equals(task.getTopic())) {
                throw new IllegalArgumentException("Prepared task topic does not match the publication session");
            }
            if (task.getStreamId() != lease.streamId()) {
                throw new IllegalArgumentException("Prepared task stream does not match the publication session");
            }
        }

        @Override
        public void close() throws Exception {
            // Fencing is deliberately outside the write lock so an ownership-loss callback can
            // synchronously stop further publication before close waits for in-flight work.
            fence();
            lifecycleLock.writeLock().lock();
            try {
                if (leaseReleaseSettled) {
                    return;
                }
                closed = true;
                // A false CAS result means this exact lease is already gone or superseded, which
                // is a settled release. An exception is different: keep the release retryable on
                // a subsequent close() or manager-level retry while the local session remains
                // permanently fenced.
                try {
                    taskManager.releasePublicationLease(lease);
                } catch (Exception | Error releaseFailure) {
                    pendingPublicationLeaseReleases.add(lease);
                    if (releaseFailure instanceof InterruptedException) {
                        Thread.currentThread().interrupt();
                    }
                    throw releaseFailure;
                }
                pendingPublicationLeaseReleases.remove(lease);
                leaseReleaseSettled = true;
            } finally {
                lifecycleLock.writeLock().unlock();
            }
        }
    }

    public void recoverPreparedTasks(String topicName) throws Exception {
        recoverPreparedTasks(topicName, OptionalLong.empty());
    }

    /**
     * Recovers a prepared task only when it belongs to {@code expectedStreamId}.
     * A task left behind by an older incarnation of the named stream is deleted without being published.
     */
    public void recoverPreparedTasks(String topicName, long expectedStreamId) throws Exception {
        recoverPreparedTasks(topicName, OptionalLong.of(expectedStreamId));
    }

    private void recoverPreparedTasks(String topicName, OptionalLong expectedStreamId) throws Exception {
        PreparedCompactStreamTask preparedCompactStreamTask =
            taskManager.getPreparedStreamTask(topicName);
        if (preparedCompactStreamTask != null) {
            if (expectedStreamId.isPresent()
                    && preparedCompactStreamTask.getStreamId() != expectedStreamId.getAsLong()) {
                log.warn("Deleting stale prepared task for stream {} because its stream id {} does not match {}",
                        topicName, preparedCompactStreamTask.getStreamId(), expectedStreamId.getAsLong());
                taskManager.deletePreparedCompactTask(topicName);
                return;
            }
            int status = preparedCompactStreamTask.getStatus();
            if (status != PreparedCompactStreamTask.INIT
                    && status != PreparedCompactStreamTask.PUSHED_TASK) {
                return;
            }
            long publishedOffset = OffsetRange.lastIncludedOffset(
                    preparedCompactStreamTask.getStartOffset(), preparedCompactStreamTask.getEndOffset());
            if (status == PreparedCompactStreamTask.INIT) {
                CompactStreamTask compactStreamTask = preparedCompactStreamTask.toCompactStreamTask();
                try {
                    taskManager.publishCompactTask(compactStreamTask);
                } catch (ExecutionException e) {
                    if (e.getCause() instanceof KeyAlreadyExistsException) {
                        log.info("The task {} already pushed, ignore it.", compactStreamTask);
                    } else {
                        throw e;
                    }
                }
                taskManager.publishPackagedTaskName(preparedCompactStreamTask.getTaskName());
                preparedCompactStreamTask.setStatus(PreparedCompactStreamTask.PUSHED_TASK);
                taskManager.updatePreparedCompactTask(preparedCompactStreamTask, Optional.of(topicName));
                taskManager.updatePublishedOffset(topicName, preparedCompactStreamTask.getStreamId(),
                    publishedOffset, preparedCompactStreamTask.getCumulativeSize());
                taskManager.deletePreparedCompactTask(topicName);
            }
            if (status == PreparedCompactStreamTask.PUSHED_TASK) {
                taskManager.updatePublishedOffset(topicName, preparedCompactStreamTask.getStreamId(),
                    publishedOffset, preparedCompactStreamTask.getCumulativeSize());
                taskManager.deletePreparedCompactTask(topicName);
            }
        }
    }

    public void publishTask(PreparedCompactStreamTask task) throws Exception {
        var topicName = task.getTopic();
        long publishedOffset = OffsetRange.lastIncludedOffset(task.getStartOffset(), task.getEndOffset());
        taskManager.publishPreparedCompactTask(task, Optional.of(task.getTopic()));
        taskManager.publishCompactTask(task.toCompactStreamTask());
        log.info("Published compact task {}", task);
        taskManager.publishPackagedTaskName(task.getTaskName());
        task.setStatus(PreparedCompactStreamTask.PUSHED_TASK);
        taskManager.updatePreparedCompactTask(task, Optional.of(topicName));
        taskManager.updatePublishedOffset(
                topicName, task.getStreamId(), publishedOffset, task.getCumulativeSize());
        taskManager.deletePreparedCompactTask(topicName);

        // Record the last published offset so integrations can detect compaction lag.
        metrics.getLatestPublishedOffset().set(publishedOffset,
            Attributes.of(AttributeKey.stringKey("topic"), task.getTopic()));
    }

    public long lastPublishedOffset(String topicName) throws IOException, ExecutionException, InterruptedException {
        var publishedOffset = taskManager.getPublishedOffset(topicName);
        if (publishedOffset != null) {
            return publishedOffset.getOffset();
        }
        return -1;
    }

    /**
     * Returns the last published offset for the expected stream incarnation, or {@code -1} when none exists.
     */
    public long lastPublishedOffset(String topicName, long expectedStreamId)
            throws IOException, ExecutionException, InterruptedException {
        var publishedOffset = taskManager.getPublishedOffset(topicName);
        if (publishedOffset == null || publishedOffset.getId() != expectedStreamId) {
            return -1;
        }
        return publishedOffset.getOffset();
    }
}
