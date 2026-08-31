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
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class CompactionManager {

    static final long DEFAULT_PUBLICATION_LEASE_RELEASE_TIMEOUT_MILLIS =
            TimeUnit.SECONDS.toMillis(30);

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
            validatePublicationCursorCoordinates(streamId, offset, cumulativeSize);
        }
    }

    private static void validatePublicationCursorCoordinates(
            long streamId, long offset, long cumulativeSize) {
        if (streamId < 0) {
            throw new IllegalArgumentException("streamId must be non-negative");
        }
        if (offset < -1) {
            throw new IllegalArgumentException("offset must be at least -1");
        }
        if (cumulativeSize < 0) {
            throw new IllegalArgumentException("cumulativeSize must be non-negative");
        }
        if (offset == -1 && cumulativeSize != 0) {
            throw new IllegalArgumentException(
                    "cumulativeSize must be 0 when offset is -1");
        }
        if (offset >= 0 && cumulativeSize == 0) {
            throw new IllegalArgumentException(
                    "cumulativeSize must be positive when offset is non-negative");
        }
    }

    private final CompactTaskManager taskManager;
    private final CompactionMetrics metrics;
    private final long publicationLeaseReleaseTimeoutMillis;
    private final Set<CompactTaskManager.PublicationLease> pendingPublicationLeaseReleases =
            ConcurrentHashMap.newKeySet();
    // Guarded by this CompactionManager instance. At most one tracked remote delete is active for
    // an exact lease until its bounded attempt expires, preventing every scan from accumulating a
    // duplicate Oxia request while still allowing a permanently stalled request to be retried.
    private final Map<CompactTaskManager.PublicationLease, CompletableFuture<Void>>
            publicationLeaseReleaseAttempts = new HashMap<>();

    public CompactionManager(CompactTaskManager taskManager) {
        this(taskManager, CompactionMetrics.NOOP);
    }

    public CompactionManager(CompactTaskManager taskManager, CompactionMetrics compactionMetrics) {
        this(taskManager, compactionMetrics, DEFAULT_PUBLICATION_LEASE_RELEASE_TIMEOUT_MILLIS);
    }

    CompactionManager(
            CompactTaskManager taskManager,
            CompactionMetrics compactionMetrics,
            long publicationLeaseReleaseTimeoutMillis) {
        this.taskManager = taskManager;
        this.metrics = compactionMetrics;
        if (publicationLeaseReleaseTimeoutMillis <= 0) {
            throw new IllegalArgumentException(
                    "publicationLeaseReleaseTimeoutMillis must be positive");
        }
        this.publicationLeaseReleaseTimeoutMillis = publicationLeaseReleaseTimeoutMillis;
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
        if (retryPendingPublicationLeaseReleaseAsync(topicName)) {
            return Optional.empty();
        }
        Optional<CompactTaskManager.PublicationLease> lease =
                taskManager.tryAcquirePublicationLease(topicName, streamId);
        if (lease.isEmpty()) {
            return Optional.empty();
        }
        try {
            CompactTaskManager.PublicationLease acquiredLease = lease.get();
            try {
                taskManager.repairLegacyPublishedOffset(acquiredLease);
            } catch (IOException malformedCursor) {
                throw new PublicationRecoveryException(
                        "Published-offset recovery metadata for " + topicName
                                + " cannot be decoded safely",
                        malformedCursor);
            }
            CompactTaskManager.PublishedOffsetClaim cursor;
            try {
                cursor = taskManager.claimPublishedOffset(acquiredLease);
            } catch (IOException malformedCursor) {
                throw new PublicationRecoveryException(
                        "Published-offset cursor for " + topicName
                                + " cannot be decoded safely",
                        malformedCursor);
            }
            validateClaimedCursor(acquiredLease, cursor);
            return Optional.of(new PublicationSession(acquiredLease, cursor));
        } catch (Throwable error) {
            CompactTaskManager.PublicationLease acquiredLease = lease.get();
            beginPublicationLeaseReleaseAsync(acquiredLease).whenComplete((ignored, releaseError) -> {
                Throwable cause = unwrapCompletionFailure(releaseError);
                if (cause != null && cause != error) {
                    error.addSuppressed(cause);
                }
            });
            throw error;
        }
    }

    private static void validateClaimedCursor(
            CompactTaskManager.PublicationLease lease,
            CompactTaskManager.PublishedOffsetClaim cursor) {
        CompactedOffset offset = cursor.offset();
        if (offset.getId() != lease.streamId()) {
            throw new PublicationFencedException(
                    "Published-offset cursor for " + lease.name() + " belongs to stream "
                            + offset.getId() + " instead of leased stream " + lease.streamId());
        }
        if (offset.getOffset() >= 0 && offset.getCumulativeSize() == 0) {
            throw new LegacyPublishedOffsetException(
                    lease.name(), lease.streamId(), offset.getOffset(),
                    "the task-manager claim returned an unrepaired cursor");
        }
        try {
            validatePublicationCursorCoordinates(
                    offset.getId(), offset.getOffset(), offset.getCumulativeSize());
        } catch (IllegalArgumentException invalidCoordinates) {
            throw new PublicationRecoveryException(
                    "Published-offset cursor for " + lease.name()
                            + " has invalid durable coordinates",
                    invalidCoordinates);
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
                awaitPublicationLeaseRelease(retryPublicationLeaseReleaseAsync(pending));
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

    /** Initiates every pending release without waiting for any remote metadata-store request. */
    public void retryPendingPublicationLeaseReleasesAsync() {
        for (CompactTaskManager.PublicationLease pending : pendingPublicationLeaseReleases) {
            retryPublicationLeaseReleaseAsync(pending);
        }
    }

    /** Returns whether an acquired lease still needs a confirmed release. */
    public boolean hasPendingPublicationLeaseReleases() {
        return !pendingPublicationLeaseReleases.isEmpty();
    }

    private boolean retryPendingPublicationLeaseReleaseAsync(String topicName) {
        boolean pendingReleaseFound = false;
        for (CompactTaskManager.PublicationLease pending : pendingPublicationLeaseReleases) {
            if (!topicName.equals(pending.name())) {
                continue;
            }
            pendingReleaseFound = true;
            retryPublicationLeaseReleaseAsync(pending);
        }
        return pendingReleaseFound;
    }

    private synchronized CompletableFuture<Void> beginPublicationLeaseReleaseAsync(
            CompactTaskManager.PublicationLease lease) {
        return publicationLeaseReleaseAsync(lease, false);
    }

    /**
     * Continues an unsettled release, or confirms atomically that another retry already settled it.
     *
     * <p>The pending check must share the manager monitor with the in-flight lookup. Otherwise a
     * successful manager-level retry can remove both entries between a session's pending check and
     * its call to begin a release, causing that session to issue a redundant conditional delete.
     */
    private synchronized CompletableFuture<Void> retryPublicationLeaseReleaseAsync(
            CompactTaskManager.PublicationLease lease) {
        return publicationLeaseReleaseAsync(lease, true);
    }

    /** Caller must hold this {@link CompactionManager}'s monitor. */
    private CompletableFuture<Void> publicationLeaseReleaseAsync(
            CompactTaskManager.PublicationLease lease, boolean retryOnly) {
        CompletableFuture<Void> inFlight = publicationLeaseReleaseAttempts.get(lease);
        if (inFlight != null) {
            return inFlight;
        }
        if (retryOnly && !pendingPublicationLeaseReleases.contains(lease)) {
            return CompletableFuture.completedFuture(null);
        }
        pendingPublicationLeaseReleases.add(lease);
        CompletableFuture<Boolean> remoteRelease;
        try {
            remoteRelease = taskManager.releasePublicationLeaseAsync(lease);
            if (remoteRelease == null) {
                throw new IllegalStateException(
                        "Task manager returned null while releasing publication lease "
                                + lease.name());
            }
        } catch (Exception | Error failure) {
            remoteRelease = CompletableFuture.failedFuture(failure);
        }
        CompletableFuture<Void> attempt = new CompletableFuture<>();
        publicationLeaseReleaseAttempts.put(lease, attempt);
        CompletableFuture<Boolean> sourceRelease = remoteRelease;
        remoteRelease.thenApply(released -> released)
                .orTimeout(publicationLeaseReleaseTimeoutMillis, TimeUnit.MILLISECONDS)
                .whenComplete((ignored, failure) -> {
                    Throwable cause = unwrapCompletionFailure(failure);
                    if (cause instanceof TimeoutException) {
                        sourceRelease.cancel(false);
                    }
                    completePublicationLeaseRelease(lease, attempt, cause);
                });
        return attempt;
    }

    private void completePublicationLeaseRelease(
            CompactTaskManager.PublicationLease lease,
            CompletableFuture<Void> attempt,
            Throwable failure) {
        Throwable cause = unwrapCompletionFailure(failure);
        synchronized (this) {
            if (cause == null) {
                pendingPublicationLeaseReleases.remove(lease);
            }
            publicationLeaseReleaseAttempts.remove(lease, attempt);
        }
        if (cause == null) {
            attempt.complete(null);
        } else {
            recordPublicationLeaseReleaseFailureBestEffort(lease, cause);
            attempt.completeExceptionally(cause);
        }
    }

    private void recordPublicationLeaseReleaseFailureBestEffort(
            CompactTaskManager.PublicationLease lease, Throwable failure) {
        try {
            log.warn("Failed to release compaction publication lease for {} and stream {}; "
                            + "the exact lease remains pending for retry",
                    lease.name(), lease.streamId(), failure);
        } catch (Exception | Error observabilityFailure) {
            if (failure != observabilityFailure) {
                failure.addSuppressed(observabilityFailure);
            }
        }
        try {
            metrics.getPublishTaskFailedCount().increment();
        } catch (Exception | Error observabilityFailure) {
            if (failure != observabilityFailure) {
                failure.addSuppressed(observabilityFailure);
            }
        }
    }

    private static void awaitPublicationLeaseRelease(CompletableFuture<Void> release)
            throws Exception {
        try {
            release.get();
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw interrupted;
        } catch (ExecutionException failure) {
            Throwable cause = unwrapCompletionFailure(failure.getCause());
            if (cause instanceof InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                throw interrupted;
            }
            if (cause instanceof Exception exception) {
                throw exception;
            }
            if (cause instanceof Error error) {
                throw error;
            }
            throw new RuntimeException(cause);
        }
    }

    private static Throwable unwrapCompletionFailure(Throwable failure) {
        Throwable cause = failure;
        while (cause instanceof CompletionException && cause.getCause() != null) {
            cause = cause.getCause();
        }
        return cause;
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
        private boolean leaseReleaseAttempted;
        // Guarded by this PublicationSession instance. Remote release completes independently of
        // the lifecycle lock so one stalled Oxia request cannot block cleanup of other sessions.
        private CompletableFuture<Void> leaseReleaseInFlight;

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

        /**
         * Fences this publisher and immediately starts releasing its remote lease, even while an
         * in-flight publication still holds the lifecycle read lock.
         *
         * <p>This is the supervisor path for a timed-out or fatally failed publication. Waiting for
         * the read lock here would let an interrupt-ignoring callable retain the Oxia lease forever
         * and prevent a healthy peer from taking over. The local {@code closed}/{@code fenced}
         * checks stop that callable at its next stage boundary; prepared-task revisions and the
         * published-cursor CAS fence any single metadata operation that was already in flight when
         * the lease was released.
         */
        public CompletableFuture<Void> fenceAndReleaseLeaseAsync() {
            fence();
            closed = true;
            return beginLeaseReleaseAsync();
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
            Optional<CompactTaskManager.PreparedTaskClaim> prepared;
            try {
                prepared = taskManager.getPreparedTaskClaim(lease.name());
            } catch (IOException malformedTask) {
                throw new PublicationRecoveryException(
                        "Prepared publication metadata for " + lease.name()
                                + " cannot be decoded safely",
                        malformedTask);
            }
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

            long targetOffset;
            try {
                targetOffset = OffsetRange.lastIncludedOffset(
                        task.getStartOffset(), task.getEndOffset());
            } catch (RuntimeException invalidRange) {
                throw recoveryFailure(task, "has an invalid offset range", invalidRange);
            }
            PublicationCursor currentCursor = publicationCursor();
            long currentOffset = currentCursor.offset();
            if (currentOffset == task.getStartOffset() - 1) {
                try {
                    validateNextTask(task, currentCursor);
                } catch (RuntimeException invalidTask) {
                    throw recoveryFailure(
                            task, "does not advance the persisted cursor consistently", invalidTask);
                }
                publishPreparedTask(claim);
                return;
            }
            if (currentOffset == targetOffset) {
                try {
                    validateRecoveredCommittedTask(task, currentCursor);
                } catch (RuntimeException invalidTask) {
                    throw recoveryFailure(
                            task, "does not match the already-committed cursor", invalidTask);
                }
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
            throw recoveryFailure(task, "has range [" + task.getStartOffset() + ", "
                    + task.getEndOffset() + ") but the persisted cursor is " + currentOffset);
        }

        private PublicationRecoveryException recoveryFailure(
                PreparedCompactStreamTask task, String reason) {
            return recoveryFailure(task, reason, null);
        }

        private PublicationRecoveryException recoveryFailure(
                PreparedCompactStreamTask task, String reason, Throwable cause) {
            String message = "Prepared task " + task.getTaskName() + " for " + lease.name()
                    + " cannot be recovered safely because it " + reason;
            return cause == null
                    ? new PublicationRecoveryException(message)
                    : new PublicationRecoveryException(message, cause);
        }

        private PublicationCursor publicationCursor() {
            CompactedOffset publishedOffset = cursor.offset();
            if (publishedOffset.getId() != lease.streamId()) {
                fence();
                throw fencedException();
            }
            if (publishedOffset.getOffset() >= 0
                    && publishedOffset.getCumulativeSize() == 0) {
                throw new LegacyPublishedOffsetException(
                        lease.name(), lease.streamId(), publishedOffset.getOffset(),
                        "the active publication session observed an unrepaired cursor");
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
            if (task.getStatus() != PreparedCompactStreamTask.INIT
                    && task.getStatus() != PreparedCompactStreamTask.PUSHED_TASK) {
                throw new IllegalArgumentException("Prepared task has an unknown publication status");
            }
            String taskName = task.getTaskName();
            if (taskName == null || taskName.isBlank() || taskName.indexOf('/') >= 0) {
                throw new IllegalArgumentException(
                        "Prepared task name must be a non-blank single key segment");
            }
        }

        @Override
        public void close() throws Exception {
            // Fencing is deliberately outside the write lock so an ownership-loss callback can
            // synchronously stop further publication before close waits for in-flight work.
            fence();
            CompletableFuture<Void> release;
            lifecycleLock.writeLock().lock();
            try {
                closed = true;
                release = beginLeaseReleaseAsync();
            } finally {
                lifecycleLock.writeLock().unlock();
            }
            awaitLeaseRelease(release);
        }

        /**
         * Attempts to release this session without waiting for an in-flight publication.
         *
         * <p>The session is fenced even when the lifecycle lock is currently held by
         * {@link #publishNext(PublicationTaskFactory)}. A {@code false} result means that the
         * caller must retry after the in-flight stage observes the fence and returns. This lets a
         * supervisor release unrelated publication leases without one stuck publisher blocking
         * the entire cleanup pass.
         */
        public boolean tryClose() throws Exception {
            Optional<CompletableFuture<Void>> release = tryCloseAsync();
            if (release.isEmpty()) {
                return false;
            }
            awaitLeaseRelease(release.orElseThrow());
            return true;
        }

        /**
         * Starts a non-blocking lease release when no publication currently holds the lifecycle
         * read lock.
         *
         * @return an empty result while publication is in flight, otherwise the independently
         *         completing release attempt
         */
        public Optional<CompletableFuture<Void>> tryCloseAsync() {
            fence();
            if (!lifecycleLock.writeLock().tryLock()) {
                return Optional.empty();
            }
            try {
                closed = true;
                return Optional.of(beginLeaseReleaseAsync());
            } finally {
                lifecycleLock.writeLock().unlock();
            }
        }

        private synchronized CompletableFuture<Void> beginLeaseReleaseAsync() {
            if (leaseReleaseSettled) {
                return CompletableFuture.completedFuture(null);
            }
            if (leaseReleaseInFlight != null) {
                return leaseReleaseInFlight;
            }
            // A false CAS result means this exact lease is already gone or superseded, which
            // is a settled release. An exception is different: keep the release retryable on
            // a subsequent close() or manager-level retry while the local session remains
            // permanently fenced.
            CompletableFuture<Void> remoteRelease = leaseReleaseAttempted
                    ? CompactionManager.this.retryPublicationLeaseReleaseAsync(lease)
                    : CompactionManager.this.beginPublicationLeaseReleaseAsync(lease);
            leaseReleaseAttempted = true;
            CompletableFuture<Void> releaseAttempt = new CompletableFuture<>();
            leaseReleaseInFlight = releaseAttempt;
            remoteRelease.whenComplete(
                    (ignored, failure) -> completeLeaseRelease(releaseAttempt, failure));
            return releaseAttempt;
        }

        private void completeLeaseRelease(
                CompletableFuture<Void> releaseAttempt, Throwable failure) {
            Throwable cause = unwrapCompletionFailure(failure);
            synchronized (this) {
                if (cause == null) {
                    leaseReleaseSettled = true;
                }
                if (leaseReleaseInFlight == releaseAttempt) {
                    leaseReleaseInFlight = null;
                }
            }
            if (cause == null) {
                releaseAttempt.complete(null);
            } else {
                releaseAttempt.completeExceptionally(cause);
            }
        }

        private void awaitLeaseRelease(CompletableFuture<Void> release) throws Exception {
            try {
                release.get();
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                throw interrupted;
            } catch (ExecutionException failure) {
                Throwable cause = unwrapCompletionFailure(failure.getCause());
                if (cause instanceof InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                    throw interrupted;
                }
                if (cause instanceof Exception exception) {
                    throw exception;
                }
                if (cause instanceof Error error) {
                    throw error;
                }
                throw new RuntimeException(cause);
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
