/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.lakehouse.compact;

import io.lakestream.api.LifecycleState;
import io.lakestream.api.Log;
import io.lakestream.api.LogEntryHeader;
import io.lakestream.api.LogId;
import io.lakestream.api.LogOffset;
import io.lakestream.api.Namespace;
import io.lakestream.api.Stream;
import io.lakestream.api.StreamCatalog;
import io.lakestream.api.StreamIdentifier;
import io.lakestream.ursa.compaction.CompactTaskManager;
import io.lakestream.ursa.compaction.CompactionManager;
import io.lakestream.ursa.compaction.DynamicConfigs;
import io.lakestream.ursa.compaction.PublicationFencedException;
import io.lakestream.ursa.compaction.PublicationRecoveryException;
import io.lakestream.ursa.compaction.metrics.CompactionMetrics;
import io.lakestream.ursa.compaction.task.PreparedCompactStreamTask;
import io.lakestream.ursa.lakehouse.utils.TopicNames;
import io.lakestream.ursa.storage.impl.StorageConfig;
import io.lakestream.ursa.storage.impl.compaction.StartStopRunner;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.LongSupplier;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;

/**
 * Publishes compaction tasks by discovering streams through {@link StreamCatalog}.
 *
 * <p>The catalog is the source of truth for both native streams and externally registered streams.
 * A long-lived fenced {@link CompactionManager.PublicationSession} is held for every physical log
 * incarnation. Stream deletion, partition removal, or replacement by a different physical log ID
 * removes and synchronously fences the old local session before a replacement lease is acquired.
 * Publication identifies readable log ranges and does not resolve Kafka schemas; schema resolution
 * and its terminal-failure policy belong to the downstream materialization worker.
 */
@Slf4j
public final class PublishCompactTaskRunner implements Runnable, StartStopRunner {

    static final String ENTRY_FORMAT_PROPERTY = "entryFormat";
    static final long MIN_PUBLICATION_RECOVERY_BACKOFF_MILLIS = TimeUnit.SECONDS.toMillis(30);
    static final long MIN_PUBLICATION_TASK_TIMEOUT_MILLIS = TimeUnit.SECONDS.toMillis(30);
    private static final int MAX_FAIR_PUBLICATION_QUANTUM = 100;
    private static final AttributeKey<String> TOPIC_ATTRIBUTE = AttributeKey.stringKey("topic");

    private final StreamCatalog streamCatalog;
    private final CompactionManager compactionManager;
    private final ExecutorService scanExecutor;
    private final ScheduledExecutorService publicationControlExecutor;
    private final ExecutorService publicationWorkerExecutor;
    private final PublicationCoordinator publicationCoordinator;
    private final long scanIntervalMillis;
    private final int checkMessageStepLength;
    private final int maxTasksPerPublisherPerScan;
    private final long compactedFileSizeLimit;
    private final long tailVisibilityMillis;
    private final Properties baseProperties;
    private final Set<String> excludedNamespaces;
    private final Set<String> excludedStreams;
    private final CompactionMetrics compactionMetrics;
    private final LongSupplier currentTimeMillis;
    private final long publicationRecoveryBackoffMillis;
    private final long publicationTaskTimeoutMillis;
    private final Map<String, PartitionPublisher> publishers = new ConcurrentHashMap<>();
    private final Map<String, Long> tailWaitStartedAtMillis = new ConcurrentHashMap<>();
    private final Map<String, Long> unavailablePublicationLeases = new ConcurrentHashMap<>();
    private final Map<String, PublicationQuarantine> publicationQuarantines = new ConcurrentHashMap<>();
    private final AtomicBoolean pendingLeaseReleaseRetryScheduled = new AtomicBoolean();

    private volatile boolean stopped;
    private volatile boolean backlogRemaining;
    private volatile boolean publicationLeaseUnavailable;
    private volatile Future<?> scanFuture;
    private volatile ScheduledFuture<?> nextScanFuture;
    // Guarded by this. Invalidates publication sessions acquired outside the monitor when a fatal
    // scan reset happens before they can be installed in publishers.
    private long publisherEpoch;

    public PublishCompactTaskRunner(StreamCatalog streamCatalog,
                                    CompactTaskManager compactTaskManager,
                                    ExecutorService scanExecutor,
                                    ScheduledExecutorService publicationControlExecutor,
                                    ExecutorService publicationWorkerExecutor,
                                    StorageConfig storageConfig,
                                    CompactionMetrics compactionMetrics) {
        this(streamCatalog,
                new CompactionManager(compactTaskManager, compactionMetrics),
                scanExecutor,
                publicationControlExecutor,
                publicationWorkerExecutor,
                storageConfig,
                compactionMetrics,
                new PublicationCoordinator(storageConfig.getPublishThreadNum()));
    }

    PublishCompactTaskRunner(StreamCatalog streamCatalog,
                             CompactionManager compactionManager,
                             ExecutorService scanExecutor,
                             ScheduledExecutorService publicationControlExecutor,
                             ExecutorService publicationWorkerExecutor,
                             StorageConfig storageConfig,
                             CompactionMetrics compactionMetrics,
                             PublicationCoordinator publicationCoordinator) {
        this(streamCatalog,
                compactionManager,
                scanExecutor,
                publicationControlExecutor,
                publicationWorkerExecutor,
                storageConfig,
                compactionMetrics,
                publicationCoordinator,
                System::currentTimeMillis);
    }

    PublishCompactTaskRunner(StreamCatalog streamCatalog,
                             CompactionManager compactionManager,
                             ExecutorService scanExecutor,
                             ScheduledExecutorService publicationControlExecutor,
                             ExecutorService publicationWorkerExecutor,
                             StorageConfig storageConfig,
                             CompactionMetrics compactionMetrics,
                             PublicationCoordinator publicationCoordinator,
                             LongSupplier currentTimeMillis) {
        this(streamCatalog,
                compactionManager,
                scanExecutor,
                publicationControlExecutor,
                publicationWorkerExecutor,
                storageConfig,
                compactionMetrics,
                publicationCoordinator,
                currentTimeMillis,
                defaultPublicationTaskTimeoutMillis(storageConfig));
    }

    PublishCompactTaskRunner(StreamCatalog streamCatalog,
                             CompactionManager compactionManager,
                             ExecutorService scanExecutor,
                             ScheduledExecutorService publicationControlExecutor,
                             ExecutorService publicationWorkerExecutor,
                             StorageConfig storageConfig,
                             CompactionMetrics compactionMetrics,
                             PublicationCoordinator publicationCoordinator,
                             LongSupplier currentTimeMillis,
                             long publicationTaskTimeoutMillis) {
        this.streamCatalog = Objects.requireNonNull(streamCatalog, "streamCatalog");
        this.compactionManager = Objects.requireNonNull(compactionManager, "compactionManager");
        this.scanExecutor = Objects.requireNonNull(scanExecutor, "scanExecutor");
        this.publicationControlExecutor =
                Objects.requireNonNull(publicationControlExecutor, "publicationControlExecutor");
        this.publicationWorkerExecutor =
                Objects.requireNonNull(publicationWorkerExecutor, "publicationWorkerExecutor");
        this.publicationCoordinator =
                Objects.requireNonNull(publicationCoordinator, "publicationCoordinator");
        this.baseProperties = Objects.requireNonNull(storageConfig, "storageConfig").getProperties();
        this.compactionMetrics = Objects.requireNonNull(compactionMetrics, "compactionMetrics");
        this.currentTimeMillis = Objects.requireNonNull(currentTimeMillis, "currentTimeMillis");
        this.scanIntervalMillis = TimeUnit.SECONDS.toMillis(
                Math.max(1L, storageConfig.getRefreshLocalTaskIntervalInSeconds()));
        this.publicationRecoveryBackoffMillis = Math.max(
                MIN_PUBLICATION_RECOVERY_BACKOFF_MILLIS, scanIntervalMillis);
        if (publicationTaskTimeoutMillis <= 0) {
            throw new IllegalArgumentException("publicationTaskTimeoutMillis must be positive");
        }
        this.publicationTaskTimeoutMillis = publicationTaskTimeoutMillis;
        this.checkMessageStepLength = Math.max(1, storageConfig.getCheckCompactMessageStepLength());
        this.maxTasksPerPublisherPerScan = Math.max(1,
                Math.min(MAX_FAIR_PUBLICATION_QUANTUM, storageConfig.getPublishThreadPendingTasks()));
        this.compactedFileSizeLimit = storageConfig.getCompactedFileSizeLimit();
        this.tailVisibilityMillis = TimeUnit.SECONDS.toMillis(
                Math.max(0L, storageConfig.getTailCompactDataVisibilityIntervalInSeconds()));
        this.excludedNamespaces = Set.copyOf(storageConfig.getBlackNamespaceOfCompact());
        this.excludedStreams = storageConfig.getBlackTopicOfCompact().stream()
                .map(PublishCompactTaskRunner::parseExcludedStream)
                .flatMap(Optional::stream)
                .collect(Collectors.toUnmodifiableSet());
    }

    private static long defaultPublicationTaskTimeoutMillis(StorageConfig storageConfig) {
        return Math.max(
                MIN_PUBLICATION_TASK_TIMEOUT_MILLIS,
                TimeUnit.SECONDS.toMillis(Math.max(
                        1L, storageConfig.getRefreshLocalTaskIntervalInSeconds())));
    }

    private static Optional<String> parseExcludedStream(String configuredName) {
        try {
            return Optional.of(TopicNames.partitionedTopicName(configuredName));
        } catch (RuntimeException invalidName) {
            log.warn("Ignoring invalid blackTopicOfCompact entry '{}'", configuredName, invalidName);
            return Optional.empty();
        }
    }

    @Override
    public void start() {
        RuntimeException submissionFailure = null;
        Error fatalSubmissionFailure = null;
        synchronized (this) {
            if (stopped || (scanFuture != null && !scanFuture.isDone())) {
                return;
            }
            try {
                scanFuture = scanExecutor.submit(this);
            } catch (RuntimeException failure) {
                submissionFailure = failure;
            } catch (Error failure) {
                fatalSubmissionFailure = failure;
            }
        }
        if (fatalSubmissionFailure != null) {
            superviseRunnerFailure(
                    fatalSubmissionFailure, "submit the next compaction publication scan");
            throw fatalSubmissionFailure;
        }
        if (submissionFailure != null) {
            superviseRunnerFailure(
                    submissionFailure, "submit the next compaction publication scan");
            throw submissionFailure;
        }
    }

    @Override
    public void run() {
        try {
            try {
                scanCatalogOnce();
                while (!stopped && backlogRemaining) {
                    drainPublisherBacklogOnce();
                }
            } catch (Exception error) {
                recordActivePublisherCount();
                if (!stopped) {
                    log.warn("Failed to discover streams and publish compaction tasks", error);
                    compactionMetrics.getPublishTaskFailedCount().increment();
                }
            }
        } catch (Error fatal) {
            superviseRunnerFailure(fatal, "run the compaction publication scan");
            throw fatal;
        } finally {
            scheduleNextScan();
        }
    }

    private void scheduleNextScan() {
        RuntimeException schedulingFailure = null;
        Error fatalSchedulingFailure = null;
        synchronized (this) {
            if (stopped) {
                return;
            }
            long nextDelayMillis = publicationLeaseUnavailable
                    ? Math.min(1000L, scanIntervalMillis) : scanIntervalMillis;
            try {
                nextScanFuture = publicationControlExecutor.schedule(
                        this::start, nextDelayMillis, TimeUnit.MILLISECONDS);
            } catch (RuntimeException failure) {
                schedulingFailure = failure;
            } catch (Error failure) {
                fatalSchedulingFailure = failure;
            }
        }
        if (fatalSchedulingFailure != null) {
            superviseRunnerFailure(
                    fatalSchedulingFailure, "schedule the next compaction publication scan");
            throw fatalSchedulingFailure;
        }
        if (schedulingFailure != null) {
            superviseRunnerFailure(
                    schedulingFailure, "schedule the next compaction publication scan");
            throw schedulingFailure;
        }
    }

    /**
     * Fences and releases every local publisher before a scan submission, execution, or scheduling
     * failure escapes.
     *
     * <p>{@link ExecutorService#submit(Runnable)} captures failures in its returned future, so a
     * rethrow alone is not supervision: without this cleanup, the runner could silently stop while
     * retaining all of its publication leases. Releasing the leases lets a peer take over even when
     * the local control loop cannot schedule another scan.
     */
    private void superviseRunnerFailure(Throwable failure, String operation) {
        Map<String, PartitionPublisher> sessions;
        synchronized (this) {
            sessions = fenceLocalPublishers();
        }
        closeFencedPublishers(sessions, failure);
        recordRunnerFailureBestEffort(failure, operation);
    }

    /** Caller must hold this runner's monitor. */
    private Map<String, PartitionPublisher> fenceLocalPublishers() {
        publisherEpoch++;
        Map<String, PartitionPublisher> sessions = Map.copyOf(publishers);
        sessions.values().forEach(PartitionPublisher::fence);
        publishers.clear();
        tailWaitStartedAtMillis.clear();
        unavailablePublicationLeases.clear();
        backlogRemaining = false;
        return sessions;
    }

    private void closeFencedPublishers(
            Map<String, PartitionPublisher> sessions, Throwable failure) {
        for (PartitionPublisher publisher : sessions.values()) {
            try {
                tryClosePublisher(publisher);
            } catch (Exception | Error closeFailure) {
                if (closeFailure != failure) {
                    failure.addSuppressed(closeFailure);
                }
                // tryClosePublisher already retained the exact lease and scheduled both manager-
                // level and session-level release retries. Continue so one bad release cannot keep
                // any other publisher owned by this runner alive.
            }
        }
    }

    private void recordRunnerFailureBestEffort(Throwable failure, String operation) {
        try {
            log.error("Failed to {}; all local publishers were fenced and their leases were released",
                    operation, failure);
        } catch (Exception | Error observabilityFailure) {
            addSuppressed(failure, observabilityFailure);
        }
        try {
            compactionMetrics.getPublishTaskFailedCount().increment();
        } catch (Exception | Error observabilityFailure) {
            addSuppressed(failure, observabilityFailure);
        }
        try {
            recordActivePublisherCount();
        } catch (Exception | Error observabilityFailure) {
            addSuppressed(failure, observabilityFailure);
        }
    }

    /** Performs one complete catalog scan. Visible for deterministic unit tests. */
    void scanCatalogOnce() throws Exception {
        backlogRemaining = false;
        publicationLeaseUnavailable = false;
        retryPendingLeaseReleases();
        Map<String, PartitionIdentity> discovered = discoverPartitions();
        unavailablePublicationLeases.keySet().retainAll(discovered.keySet());
        clearObsoletePublicationQuarantines(discovered);
        if (stopped) {
            return;
        }

        // Fence removed or replaced physical logs before attempting to acquire their replacements.
        for (Map.Entry<String, PartitionPublisher> entry : List.copyOf(publishers.entrySet())) {
            PartitionIdentity current = discovered.get(entry.getKey());
            if (current == null || current.logId().id() != entry.getValue().streamId()) {
                if (publishers.remove(entry.getKey(), entry.getValue())) {
                    tailWaitStartedAtMillis.remove(entry.getKey());
                    closePublisher(entry.getValue());
                }
            }
        }

        publishDiscovered(discovered.values());
        recordActivePublisherCount();
    }

    private void recordActivePublisherCount() {
        long logicalStreamCount = publishers.values().stream()
                .map(PartitionPublisher::identity)
                .map(PartitionIdentity::stream)
                .distinct()
                .count();
        compactionMetrics.getOngoingCompactionTopicCount().set(logicalStreamCount);
    }

    /** Drains another fair quantum without repeating namespace and stream discovery. */
    private void drainPublisherBacklogOnce() throws Exception {
        backlogRemaining = false;
        retryPendingLeaseReleases();
        publishExisting(List.copyOf(publishers.values()));
        recordActivePublisherCount();
    }

    private void publishDiscovered(Iterable<PartitionIdentity> discovered) throws Exception {
        long scanPublisherEpoch = currentPublisherEpoch();
        List<PublicationRequest> requests = new ArrayList<>();
        for (PartitionIdentity identity : discovered) {
            if (stopped) {
                break;
            }
            if (publicationBackoffActive(identity)) {
                continue;
            }
            requests.add(new PublicationRequest(
                    identity,
                    scanPublisherEpoch,
                    attempt -> publishIdentity(identity, scanPublisherEpoch, attempt)));
        }
        publishInWindows(requests, "catalog scan");
    }

    private void publishExisting(List<PartitionPublisher> existing) throws Exception {
        long scanPublisherEpoch = currentPublisherEpoch();
        List<PublicationRequest> requests = new ArrayList<>();
        for (PartitionPublisher publisher : existing) {
            if (stopped) {
                break;
            }
            requests.add(new PublicationRequest(
                    publisher.identity(),
                    scanPublisherEpoch,
                    attempt -> publishAvailable(publisher, scanPublisherEpoch, attempt)));
        }
        publishInWindows(requests, "publisher backlog");
    }

    private void publishInWindows(List<PublicationRequest> requests, String scope) throws Exception {
        int nextRequest = 0;
        while (!stopped && nextRequest < requests.size()) {
            BlockingQueue<PublicationCompletion> completions = new LinkedBlockingQueue<>();
            List<PublicationAttempt> batch = new ArrayList<>();
            boolean waitForCapacity = false;
            while (!stopped && nextRequest < requests.size()) {
                PublicationRequest request = requests.get(nextRequest);
                if (publicationBackoffActive(request.identity())) {
                    nextRequest++;
                    continue;
                }
                PublicationAttempt attempt = new PublicationAttempt(
                        request.identity(), request.publisherEpoch(), scope, completions);
                RegistrationStatus registration = publicationCoordinator.tryRegister(attempt);
                if (registration == RegistrationStatus.DUPLICATE) {
                    nextRequest++;
                    continue;
                }
                if (registration != RegistrationStatus.REGISTERED) {
                    waitForCapacity = registration == RegistrationStatus.WINDOW_FULL;
                    break;
                }
                nextRequest++;
                try {
                    Future<?> workerFuture = publicationWorkerExecutor.submit(
                            () -> executePublicationAttempt(attempt, request.action()));
                    setPublicationWorkerFuture(attempt, workerFuture);
                    batch.add(attempt);
                } catch (Throwable schedulingFailure) {
                    publicationCoordinator.submissionFailed(attempt);
                    if (schedulingFailure instanceof Error fatal) {
                        detachPublicationObservers(batch);
                        throw fatal;
                    }
                    if (!stopped) {
                        recordPartitionFailure(
                                request.identity().taskTopic(),
                                "schedule task publication",
                                schedulingFailure);
                    }
                }
            }
            if (batch.isEmpty()) {
                if (waitForCapacity || publicationCoordinator.isStickyLimitReached()) {
                    return;
                }
                continue;
            }
            awaitPublication(completions, batch);
        }
    }

    private void executePublicationAttempt(
            PublicationAttempt attempt, PublicationAction publicationAction) {
        Throwable failure = null;
        try {
            if (startPublicationDeadline(attempt)) {
                publicationAction.run(attempt);
            }
        } catch (RuntimeException | Error error) {
            failure = error;
        } finally {
            finishPublicationAttempt(attempt, failure);
        }
    }

    private boolean startPublicationDeadline(PublicationAttempt attempt) {
        ScheduledFuture<?> timeoutFuture = publicationControlExecutor.schedule(
                () -> timeoutPublicationAttempt(attempt),
                publicationTaskTimeoutMillis,
                TimeUnit.MILLISECONDS);
        synchronized (attempt) {
            attempt.timeoutFuture = timeoutFuture;
            if (attempt.workerFinished || attempt.timedOut) {
                timeoutFuture.cancel(false);
            }
            return !attempt.workerFinished && !attempt.timedOut;
        }
    }

    private void setPublicationWorkerFuture(PublicationAttempt attempt, Future<?> workerFuture) {
        boolean cancel;
        synchronized (attempt) {
            attempt.workerFuture = workerFuture;
            cancel = attempt.timedOut;
        }
        if (cancel) {
            workerFuture.cancel(true);
        }
    }

    private void publishIdentity(
            PartitionIdentity identity,
            long scanPublisherEpoch,
            PublicationAttempt attempt) {
        try {
            if (!isCurrentPublisherEpoch(scanPublisherEpoch)
                    || !isPublicationAttemptActive(attempt)
                    || publicationBackoffActive(identity)) {
                return;
            }
            PartitionPublisher publisher = publishers.get(identity.taskTopic());
            if (publisher == null) {
                publisher = openPublisher(identity, scanPublisherEpoch, attempt);
                if (publisher == null) {
                    return;
                }
            }
            publishAvailable(publisher, scanPublisherEpoch, attempt);
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
        } catch (PublicationRecoveryException recoveryFailure) {
            if (isCurrentPublisherEpoch(scanPublisherEpoch)) {
                PublicationQuarantine quarantine = enterPublicationQuarantine(identity);
                recordPublicationQuarantineBestEffort(quarantine, recoveryFailure);
            }
        } catch (Exception error) {
            if (isCurrentPublisherEpoch(scanPublisherEpoch)) {
                recordPartitionFailure(identity.taskTopic(), "open task publisher", error);
            }
        }
    }

    private void awaitPublication(
            BlockingQueue<PublicationCompletion> completedPublications,
            List<PublicationAttempt> publicationAttempts) throws Exception {
        try {
            for (int completed = 0; completed < publicationAttempts.size(); completed++) {
                PublicationCompletion completion = completedPublications.take();
                if (completion.timedOut() || completion.failure() == null) {
                    continue;
                }
                Throwable cause = unwrapCompletionFailure(completion.failure());
                if (cause instanceof Error fatal) {
                    throw fatal;
                }
                recordPartitionFailure(
                        completion.attempt().identity.taskTopic(),
                        "complete task publication",
                        cause);
            }
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            detachPublicationObservers(publicationAttempts);
            throw interrupted;
        } catch (RuntimeException | Error failure) {
            detachPublicationObservers(publicationAttempts);
            throw failure;
        }
    }

    private void timeoutPublicationAttempt(PublicationAttempt attempt) {
        boolean reachedStickyLimit;
        synchronized (attempt) {
            if (attempt.workerFinished || attempt.timedOut) {
                return;
            }
            attempt.timedOut = true;
            reachedStickyLimit = publicationCoordinator.timedOut(attempt);
        }
        RuntimeException timeout = new RuntimeException(
                "Timed out after " + publicationTaskTimeoutMillis
                        + "ms while running " + attempt.scope + " publication of "
                        + attempt.identity.taskTopic());
        try {
            quarantineTimedOutPublication(attempt.identity, timeout);
        } finally {
            attempt.completions.offer(new PublicationCompletion(attempt, null, true));
            Future<?> workerFuture;
            synchronized (attempt) {
                workerFuture = attempt.workerFuture;
            }
            if (workerFuture != null) {
                workerFuture.cancel(true);
            }
            if (reachedStickyLimit) {
                recordStickyPublicationLimitBestEffort(timeout);
            }
        }
    }

    private void quarantineTimedOutPublication(
            PartitionIdentity identity, RuntimeException timeout) {
        PublicationQuarantine quarantine;
        PartitionPublisher timedOutPublisher = null;
        synchronized (this) {
            quarantine = enterPublicationQuarantine(identity);
            PartitionPublisher current = publishers.get(identity.taskTopic());
            if (current != null
                    && current.identity().logId().id() == identity.logId().id()
                    && publishers.remove(identity.taskTopic(), current)) {
                current.fence();
                timedOutPublisher = current;
                tailWaitStartedAtMillis.remove(identity.taskTopic());
            }
        }
        try {
            if (timedOutPublisher != null) {
                tryClosePublisher(timedOutPublisher);
            }
        } finally {
            recordPublicationTimeoutBestEffort(quarantine, timeout);
        }
    }

    private void finishPublicationAttempt(PublicationAttempt attempt, Throwable failure) {
        boolean timedOut;
        boolean observerDetached;
        ScheduledFuture<?> timeoutFuture;
        synchronized (attempt) {
            if (attempt.workerFinished) {
                return;
            }
            attempt.workerFinished = true;
            timedOut = attempt.timedOut;
            observerDetached = attempt.observerDetached;
            timeoutFuture = attempt.timeoutFuture;
            publicationCoordinator.finished(attempt, timedOut);
        }
        if (timeoutFuture != null) {
            timeoutFuture.cancel(false);
        }
        if (!timedOut && !observerDetached) {
            attempt.completions.offer(new PublicationCompletion(attempt, failure, false));
        } else if (failure instanceof Error fatal) {
            if (!superviseLateFatalPublicationIfCurrent(attempt.publisherEpoch, fatal)) {
                recordLateFatalPublicationBestEffort(attempt.identity, fatal);
            }
        }
    }

    private boolean superviseLateFatalPublicationIfCurrent(
            long expectedPublisherEpoch, Error fatal) {
        Map<String, PartitionPublisher> sessions;
        synchronized (this) {
            if (stopped || publisherEpoch != expectedPublisherEpoch) {
                return false;
            }
            sessions = fenceLocalPublishers();
        }
        closeFencedPublishers(sessions, fatal);
        recordRunnerFailureBestEffort(
                fatal, "complete a detached compaction publication in the current publisher epoch");
        return true;
    }

    private void recordLateFatalPublicationBestEffort(
            PartitionIdentity identity, Error fatal) {
        try {
            log.error("A detached or timed-out compaction publication for {} (physical log {}) "
                            + "failed fatally after its publisher epoch was fenced",
                    identity.taskTopic(), identity.logId().id(), fatal);
        } catch (Exception | Error observabilityFailure) {
            addSuppressed(fatal, observabilityFailure);
        }
        try {
            compactionMetrics.getPublishTaskFailedCount().increment();
        } catch (Exception | Error observabilityFailure) {
            addSuppressed(fatal, observabilityFailure);
        }
    }

    private static void detachPublicationObservers(List<PublicationAttempt> publicationAttempts) {
        for (PublicationAttempt attempt : publicationAttempts) {
            Future<?> workerFuture = null;
            synchronized (attempt) {
                if (!attempt.workerFinished && !attempt.timedOut) {
                    attempt.observerDetached = true;
                    workerFuture = attempt.workerFuture;
                }
            }
            if (workerFuture != null) {
                workerFuture.cancel(true);
            }
        }
    }

    private static boolean isPublicationAttemptActive(PublicationAttempt attempt) {
        synchronized (attempt) {
            return !attempt.workerFinished && !attempt.timedOut;
        }
    }

    private Map<String, PartitionIdentity> discoverPartitions() throws Exception {
        Map<String, PartitionIdentity> discovered = new LinkedHashMap<>();
        for (Namespace namespace : streamCatalog.listNamespaces().get()) {
            if (excludedNamespaces.contains(namespace.name())) {
                continue;
            }
            List<StreamIdentifier> identifiers;
            try {
                identifiers = streamCatalog.listStreams(namespace.name()).get();
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                throw interrupted;
            } catch (Exception error) {
                recordPartitionFailure(namespace.name(), "list namespace streams", error);
                retainExistingNamespace(namespace.name(), discovered);
                continue;
            }
            for (StreamIdentifier identifier : identifiers) {
                if (excludedStreams.contains(identifier.fullName())) {
                    continue;
                }
                try (Stream stream = streamCatalog.loadStream(identifier).get()) {
                    if (stream.state() == LifecycleState.ACTIVE || stream.state() == LifecycleState.SEALED) {
                        List<LogId> logIds = stream.layout().logIds().get();
                        for (int partition = 0; partition < logIds.size(); partition++) {
                            LogId logId = logIds.get(partition);
                            if (logId.id() < 0) {
                                continue;
                            }
                            String taskTopic = taskTopic(identifier, partition);
                            PartitionIdentity previous = discovered.putIfAbsent(taskTopic,
                                    new PartitionIdentity(identifier, partition, logId));
                            if (previous != null) {
                                recordPartitionFailure(taskTopic, "discover physical log",
                                        new IllegalStateException(
                                                "Catalog returned duplicate compaction identity " + taskTopic));
                            }
                        }
                    }
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                    throw interrupted;
                } catch (Exception error) {
                    recordPartitionFailure(identifier.fullName(), "discover stream partitions", error);
                    retainExistingStream(identifier, discovered);
                }
            }
        }
        return discovered;
    }

    private PartitionPublisher openPublisher(
            PartitionIdentity identity,
            long scanPublisherEpoch,
            PublicationAttempt attempt) throws Exception {
        if (!isCurrentPublisherEpoch(scanPublisherEpoch)
                || !isPublicationAttemptActive(attempt)) {
            return null;
        }
        Optional<CompactionManager.PublicationSession> session;
        try {
            session = compactionManager.tryOpenPublicationSession(
                    identity.taskTopic(), identity.logId().id());
        } catch (Exception | Error error) {
            schedulePendingLeaseReleaseRetry();
            throw error;
        }
        if (session.isEmpty()) {
            if (!isCurrentPublisherEpoch(scanPublisherEpoch)
                    || !isPublicationAttemptActive(attempt)) {
                return null;
            }
            publicationLeaseUnavailable = true;
            Long previousUnavailableStream = unavailablePublicationLeases.put(
                    identity.taskTopic(), identity.logId().id());
            if (previousUnavailableStream == null
                    || previousUnavailableStream != identity.logId().id()) {
                log.warn("Compaction publication lease for {} (physical log {}) is held by another owner; "
                                + "publication will be retried",
                        identity.taskTopic(), identity.logId().id());
                compactionMetrics.getPublicationLeaseUnavailableCount().increment(
                        Attributes.of(TOPIC_ATTRIBUTE, identity.taskTopic()));
            }
            return null;
        }
        unavailablePublicationLeases.remove(identity.taskTopic(), identity.logId().id());

        PartitionPublisher candidate = new PartitionPublisher(identity, session.orElseThrow());
        PartitionPublisher selected = null;
        synchronized (this) {
            if (!stopped
                    && publisherEpoch == scanPublisherEpoch
                    && !publicationBackoffActive(identity)
                    && isPublicationAttemptActive(attempt)) {
                PartitionPublisher raced = publishers.putIfAbsent(identity.taskTopic(), candidate);
                selected = raced == null ? candidate : raced;
            }
        }
        if (selected != candidate) {
            // stop(), fatal reset and session installation share the same monitor. If the scan
            // generation changed while the remote lease was being acquired, close the late session
            // instead of letting it escape the fencing snapshot.
            closePublisher(candidate);
        }
        return selected;
    }

    private void publishAvailable(
            PartitionPublisher publisher,
            long scanPublisherEpoch,
            PublicationAttempt attempt) {
        try {
            for (int published = 0; published < maxTasksPerPublisherPerScan; published++) {
                if (!isCurrentPublisherEpoch(scanPublisherEpoch)
                        || !isPublicationAttemptActive(attempt)
                        || publishers.get(publisher.identity().taskTopic()) != publisher
                        || publishNext(publisher) != CompactionManager.PublicationResult.PUBLISHED) {
                    return;
                }
            }
            backlogRemaining = true;
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
        } catch (PublicationRecoveryException recoveryFailure) {
            if (!isCurrentPublisherEpoch(scanPublisherEpoch)) {
                return;
            }
            PublicationQuarantine quarantine = enterPublicationQuarantine(publisher.identity());
            try {
                if (publishers.remove(publisher.identity().taskTopic(), publisher)) {
                    tailWaitStartedAtMillis.remove(publisher.identity().taskTopic());
                    closePublisher(publisher);
                }
            } finally {
                recordPublicationQuarantineBestEffort(quarantine, recoveryFailure);
            }
        } catch (Exception error) {
            if (isCurrentPublisherEpoch(scanPublisherEpoch)) {
                recordPartitionFailure(publisher.identity().taskTopic(), "publish compaction task", error);
            }
        }
    }

    private synchronized long currentPublisherEpoch() {
        return publisherEpoch;
    }

    private synchronized boolean isCurrentPublisherEpoch(long expectedPublisherEpoch) {
        return !stopped && publisherEpoch == expectedPublisherEpoch;
    }

    private PublicationQuarantine enterPublicationQuarantine(PartitionIdentity identity) {
        long now = currentTimeMillis.getAsLong();
        AtomicBoolean enteredQuarantine = new AtomicBoolean();
        PublicationQuarantine quarantine = publicationQuarantines.compute(
                identity.taskTopic(), (ignored, previous) -> {
                    if (previous != null
                            && previous.identity().logId().id() == identity.logId().id()
                            && previous.retryAfterMillis() > now) {
                        return previous;
                    }
                    enteredQuarantine.set(true);
                    return new PublicationQuarantine(
                            identity, saturatedAdd(now, publicationRecoveryBackoffMillis));
                });
        return enteredQuarantine.get() ? quarantine : null;
    }

    private void recordPublicationQuarantineBestEffort(
            PublicationQuarantine quarantine, RuntimeException error) {
        if (quarantine == null) {
            return;
        }
        PartitionIdentity identity = quarantine.identity();
        try {
            log.error("Quarantining compaction publication {} for physical log {} until {} because its "
                            + "durable publication metadata cannot be recovered safely; publication "
                            + "will retry after the backoff",
                    identity.taskTopic(), identity.logId().id(), quarantine.retryAfterMillis(), error);
        } catch (Exception | Error observabilityFailure) {
            addSuppressed(error, observabilityFailure);
        }
        try {
            compactionMetrics.getPublishTaskFailedCount().increment();
        } catch (Exception | Error observabilityFailure) {
            addSuppressed(error, observabilityFailure);
        }
    }

    private void recordPublicationTimeoutBestEffort(
            PublicationQuarantine quarantine, RuntimeException timeout) {
        if (quarantine == null) {
            return;
        }
        PartitionIdentity identity = quarantine.identity();
        try {
            log.error("Quarantining compaction publication {} for physical log {} until {} because "
                            + "the started publication exceeded its deadline; the local publisher "
                            + "was fenced and publication will retry after the backoff",
                    identity.taskTopic(), identity.logId().id(), quarantine.retryAfterMillis(), timeout);
        } catch (Exception | Error observabilityFailure) {
            addSuppressed(timeout, observabilityFailure);
        }
        try {
            compactionMetrics.getPublishTaskFailedCount().increment();
        } catch (Exception | Error observabilityFailure) {
            addSuppressed(timeout, observabilityFailure);
        }
    }

    private void recordStickyPublicationLimitBestEffort(RuntimeException timeout) {
        try {
            log.error("Compaction publication reached the sticky timed-out attempt limit {} "
                            + "(normal parallelism {}); new publication will remain suspended until "
                            + "at least one non-terminating callable returns",
                    publicationCoordinator.maxStickyAttempts(),
                    publicationCoordinator.normalParallelism(),
                    timeout);
        } catch (Exception | Error observabilityFailure) {
            addSuppressed(timeout, observabilityFailure);
        }
        try {
            compactionMetrics.getPublishTaskFailedCount().increment();
        } catch (Exception | Error observabilityFailure) {
            addSuppressed(timeout, observabilityFailure);
        }
    }

    private boolean publicationBackoffActive(PartitionIdentity identity) {
        while (true) {
            PublicationQuarantine quarantine = publicationQuarantines.get(identity.taskTopic());
            if (quarantine == null) {
                return false;
            }
            if (quarantine.identity().logId().id() != identity.logId().id()
                    || currentTimeMillis.getAsLong() >= quarantine.retryAfterMillis()) {
                if (publicationQuarantines.remove(identity.taskTopic(), quarantine)) {
                    return false;
                }
                continue;
            }
            return true;
        }
    }

    private void clearObsoletePublicationQuarantines(
            Map<String, PartitionIdentity> discovered) {
        publicationQuarantines.entrySet().removeIf(entry -> {
            PartitionIdentity current = discovered.get(entry.getKey());
            return current == null
                    || current.logId().id() != entry.getValue().identity().logId().id();
        });
    }

    private static long saturatedAdd(long value, long increment) {
        try {
            return Math.addExact(value, increment);
        } catch (ArithmeticException overflow) {
            return Long.MAX_VALUE;
        }
    }

    private CompactionManager.PublicationResult publishNext(PartitionPublisher publisher) throws Exception {
        try {
            TaskSnapshot[] task = new TaskSnapshot[1];
            CompactionManager.PublicationResult result = publisher.session().publishNext(cursor -> {
                TaskSnapshot snapshot = createTask(publisher.identity(), cursor);
                task[0] = snapshot;
                return snapshot == null ? Optional.empty() : Optional.of(snapshot.task());
            });
            if (result == CompactionManager.PublicationResult.PUBLISHED && task[0] != null) {
                long latestOffset = task[0].latestOffset();
                Attributes attributes = Attributes.of(TOPIC_ATTRIBUTE, publisher.identity().taskTopic());
                compactionMetrics.getLatestMessageOffset().set(latestOffset, attributes);
                compactionMetrics.getCompactionLag().set(
                        latestOffset - task[0].publishedOffset(), attributes);
                compactionMetrics.getPublishedTaskBytes().set(task[0].task().getTotalSize());
            }
            return result;
        } catch (PublicationFencedException fenced) {
            if (publishers.remove(publisher.identity().taskTopic(), publisher)) {
                tailWaitStartedAtMillis.remove(publisher.identity().taskTopic());
                closePublisher(publisher);
            }
            log.info("Compaction task publisher for {} was fenced", publisher.identity().taskTopic());
            return CompactionManager.PublicationResult.NO_TASK;
        }
    }

    private TaskSnapshot createTask(
            PartitionIdentity expected,
            CompactionManager.PublicationCursor publishedCursor) throws Exception {
        if (publishedCursor.streamId() != expected.logId().id()) {
            throw new PublicationFencedException(
                    "Published cursor for " + expected.taskTopic() + " belongs to a different physical log");
        }
        try (Stream stream = streamCatalog.loadStream(expected.stream()).get()) {
            List<LogId> logIds = stream.layout().logIds().get();
            if (expected.partition() >= logIds.size()
                    || logIds.get(expected.partition()).id() != expected.logId().id()) {
                throw new PublicationFencedException("Physical log for " + expected.taskTopic() + " changed");
            }
            try (Log logHandle = stream.getLog(expected.logId())) {
                LogOffset last = logHandle.getLastOffset().get();
                if (LogOffset.NOT_FOUND.equals(last)) {
                    return null;
                }
                long endOffset = Math.addExact(last.offset(), last.numberOfRecords());
                long startOffset = Math.addExact(publishedCursor.offset(), 1L);
                if (startOffset >= endOffset) {
                    return null;
                }

                long startCumulativeSize = publishedCursor.cumulativeSize();
                LogOffset selectedEnd = selectTaskEnd(
                        logHandle, expected.taskTopic(), startOffset, startCumulativeSize, last);
                if (selectedEnd == null) {
                    return null;
                }
                long selectedEndOffset = Math.addExact(
                        selectedEnd.offset(), selectedEnd.numberOfRecords());
                long totalSize = Math.subtractExact(selectedEnd.cumulativeSize(), startCumulativeSize);
                Map<String, String> taskProperties = resolveTaskProperties(stream.properties());
                PreparedCompactStreamTask task = new PreparedCompactStreamTask(
                        expected.logId().id(),
                        startOffset,
                        selectedEndOffset,
                        totalSize,
                        selectedEnd.cumulativeSize(),
                        PreparedCompactStreamTask.INIT,
                        UUID.randomUUID().toString(),
                        expected.taskTopic(),
                        taskProperties);
                return new TaskSnapshot(
                        task,
                        Math.subtractExact(endOffset, 1L),
                        Math.subtractExact(selectedEndOffset, 1L));
            }
        }
    }

    /**
     * Preserves the existing file-size and tail-visibility batching policy while reading only
     * through the catalog-owned {@link Log} API.
     */
    private LogOffset selectTaskEnd(Log logHandle,
                                    String taskTopic,
                                    long startOffset,
                                    long startCumulativeSize,
                                    LogOffset last) throws Exception {
        LogOffset selected = last;
        if (compactedFileSizeLimit > 0) {
            long lastEndOffset = Math.addExact(last.offset(), last.numberOfRecords());
            long probeOffset = Math.addExact(startOffset, checkMessageStepLength - 1L);
            while (probeOffset < lastEndOffset) {
                LogEntryHeader header = logHandle.getEntryMetadata(probeOffset).get();
                if (header != null && header.offset() >= 0
                        && header.cumulativeSize() - startCumulativeSize >= compactedFileSizeLimit) {
                    selected = new LogOffset(
                            header.offset(),
                            header.numberOfRecords(),
                            header.timestamp(),
                            header.entrySize(),
                            header.cumulativeSize());
                    break;
                }
                probeOffset = Math.addExact(probeOffset, checkMessageStepLength);
            }
        }

        long selectedSize = Math.subtractExact(selected.cumulativeSize(), startCumulativeSize);
        if (selectedSize < 0) {
            throw new PublicationRecoveryException(
                    "Persisted cumulative-size cursor for " + taskTopic + " is "
                            + startCumulativeSize + " bytes, beyond the selected log range at "
                            + selected.cumulativeSize() + " bytes");
        }
        if (selectedSize == 0) {
            tailWaitStartedAtMillis.remove(taskTopic);
            return null;
        }
        boolean reachedFileSizeLimit = compactedFileSizeLimit > 0
                && selectedSize >= compactedFileSizeLimit;
        if (!reachedFileSizeLimit && tailVisibilityMillis > 0) {
            LogEntryHeader first = logHandle.getEntryMetadata(startOffset).get();
            long firstTimestamp = first == null || first.offset() < 0 ? last.timestamp() : first.timestamp();
            long now = System.currentTimeMillis();
            if (firstTimestamp > 0) {
                tailWaitStartedAtMillis.remove(taskTopic);
                if (now - firstTimestamp < tailVisibilityMillis) {
                    return null;
                }
            } else {
                long waitStarted = tailWaitStartedAtMillis.computeIfAbsent(taskTopic, ignored -> now);
                if (now - waitStarted < tailVisibilityMillis) {
                    return null;
                }
                tailWaitStartedAtMillis.remove(taskTopic);
            }
        } else {
            tailWaitStartedAtMillis.remove(taskTopic);
        }
        return selected;
    }

    private Map<String, String> resolveTaskProperties(Map<String, String> streamProperties) {
        Map<String, String> taskProperties = new HashMap<>();
        if (streamProperties != null) {
            taskProperties.putAll(streamProperties);
        }
        String clusterName = baseProperties.getProperty("clusterName");
        DynamicConfigs dynamicConfigs = clusterName == null || clusterName.isBlank()
                ? DynamicConfigs.of(baseProperties)
                : new DynamicConfigs(clusterName, baseProperties);
        dynamicConfigs.overrideWith(taskProperties);
        taskProperties.putAll(dynamicConfigs.toTaskProperties());
        taskProperties.putIfAbsent(ENTRY_FORMAT_PROPERTY,
                baseProperties.getProperty(ENTRY_FORMAT_PROPERTY,
                        baseProperties.getProperty("dataSourceForCompaction", "URSA")));
        return Map.copyOf(taskProperties);
    }

    static String taskTopic(StreamIdentifier identifier, int partition) {
        return identifier.fullName() + "-partition-" + partition;
    }

    /**
     * Fences every local publisher before returning, then schedules remote publication-lease release.
     * If scheduling is unavailable, release is attempted inline. Callers that need to transfer
     * ownership immediately must wait for the lease store to confirm the release.
     */
    @Override
    public void stop() {
        Map<String, PartitionPublisher> sessions;
        synchronized (this) {
            if (stopped) {
                return;
            }
            stopped = true;
            publisherEpoch++;
            if (nextScanFuture != null) {
                nextScanFuture.cancel(false);
            }
            if (scanFuture != null) {
                scanFuture.cancel(false);
            }
            sessions = Map.copyOf(publishers);
            // Fence every local owner synchronously before scheduling remote lease release.
            sessions.values().forEach(PartitionPublisher::fence);
            publishers.clear();
            tailWaitStartedAtMillis.clear();
            unavailablePublicationLeases.clear();
            publicationQuarantines.clear();
        }
        sessions.values().forEach(this::closePublisherAfterStop);
        schedulePendingLeaseReleaseRetry();
        recordActivePublisherCount();
    }

    private void closePublisherAfterStop(PartitionPublisher publisher) {
        // Initiation is non-blocking. Starting every release inline prevents a busy or stopped
        // single-thread publish executor from serializing unrelated lease cleanup.
        tryClosePublisher(publisher);
    }

    private void closePublisher(PartitionPublisher publisher) {
        publisher.fence();
        tryClosePublisher(publisher);
    }

    private void tryClosePublisher(PartitionPublisher publisher) {
        try {
            publisher.session().fenceAndReleaseLeaseAsync().whenComplete((ignored, failure) -> {
                if (failure == null) {
                    publisher.closeRetryScheduled().set(false);
                    return;
                }
                handleCloseFailure(publisher, unwrapCompletionFailure(failure));
            });
        } catch (Exception | Error error) {
            handleCloseFailure(publisher, error);
        }
    }

    private void handleCloseFailure(PartitionPublisher publisher, Throwable error) {
        try {
            schedulePendingLeaseReleaseRetry();
        } catch (Exception | Error retryFailure) {
            addSuppressed(error, retryFailure);
        }
        try {
            scheduleCloseRetry(publisher);
        } catch (Exception | Error retryFailure) {
            addSuppressed(error, retryFailure);
        }
    }

    private void scheduleCloseRetry(PartitionPublisher publisher) {
        if (!publisher.closeRetryScheduled().compareAndSet(false, true)) {
            return;
        }
        try {
            publicationControlExecutor.schedule(() -> {
                publisher.closeRetryScheduled().set(false);
                tryClosePublisher(publisher);
            }, Math.min(1000L, scanIntervalMillis), TimeUnit.MILLISECONDS);
        } catch (RejectedExecutionException rejected) {
            publisher.closeRetryScheduled().set(false);
            log.warn("Failed to schedule publication-session close retry for {}",
                    publisher.identity().taskTopic(), rejected);
        }
    }

    private void retryPendingLeaseReleases() {
        if (!compactionManager.hasPendingPublicationLeaseReleases()) {
            return;
        }
        compactionManager.retryPendingPublicationLeaseReleasesAsync();
        schedulePendingLeaseReleaseRetry();
    }

    private static void addSuppressed(Throwable primary, Throwable secondary) {
        if (primary != secondary) {
            primary.addSuppressed(secondary);
        }
    }

    private static Throwable unwrapCompletionFailure(Throwable failure) {
        Throwable cause = failure;
        while ((cause instanceof CompletionException || cause instanceof ExecutionException)
                && cause.getCause() != null) {
            cause = cause.getCause();
        }
        return cause;
    }

    private void schedulePendingLeaseReleaseRetry() {
        if (!compactionManager.hasPendingPublicationLeaseReleases()
                || !pendingLeaseReleaseRetryScheduled.compareAndSet(false, true)) {
            return;
        }
        try {
            publicationControlExecutor.schedule(() -> {
                pendingLeaseReleaseRetryScheduled.set(false);
                retryPendingLeaseReleases();
                if (compactionManager.hasPendingPublicationLeaseReleases()) {
                    schedulePendingLeaseReleaseRetry();
                }
            }, Math.min(1000L, scanIntervalMillis), TimeUnit.MILLISECONDS);
        } catch (RejectedExecutionException rejected) {
            pendingLeaseReleaseRetryScheduled.set(false);
            log.warn("Failed to schedule an incomplete publication-lease release retry", rejected);
        }
    }

    private void recordPartitionFailure(String name, String operation, Throwable error) {
        log.warn("Failed to {} for {}", operation, name, error);
        compactionMetrics.getPublishTaskFailedCount().increment();
    }

    private void retainExistingNamespace(
            String namespace, Map<String, PartitionIdentity> discovered) {
        publishers.values().stream()
                .map(PartitionPublisher::identity)
                .filter(identity -> identity.stream().namespace().equals(namespace))
                .forEach(identity -> discovered.putIfAbsent(identity.taskTopic(), identity));
        publicationQuarantines.values().stream()
                .map(PublicationQuarantine::identity)
                .filter(identity -> identity.stream().namespace().equals(namespace))
                .forEach(identity -> discovered.putIfAbsent(identity.taskTopic(), identity));
    }

    private void retainExistingStream(
            StreamIdentifier stream, Map<String, PartitionIdentity> discovered) {
        publishers.values().stream()
                .map(PartitionPublisher::identity)
                .filter(identity -> identity.stream().equals(stream))
                .forEach(identity -> discovered.putIfAbsent(identity.taskTopic(), identity));
        publicationQuarantines.values().stream()
                .map(PublicationQuarantine::identity)
                .filter(identity -> identity.stream().equals(stream))
                .forEach(identity -> discovered.putIfAbsent(identity.taskTopic(), identity));
    }

    int sessionCount() {
        return publishers.size();
    }

    @FunctionalInterface
    private interface PublicationAction {
        void run(PublicationAttempt attempt);
    }

    private enum RegistrationStatus {
        REGISTERED,
        DUPLICATE,
        WINDOW_FULL,
        STICKY_LIMIT
    }

    private record PublicationRequest(
            PartitionIdentity identity,
            long publisherEpoch,
            PublicationAction action) {
    }

    private record PublicationCompletion(
            PublicationAttempt attempt,
            Throwable failure,
            boolean timedOut) {
    }

    private record PublicationKey(String taskTopic, long streamId) {
    }

    private static final class PublicationAttempt {
        private final PartitionIdentity identity;
        private final PublicationKey key;
        private final long publisherEpoch;
        private final String scope;
        private final BlockingQueue<PublicationCompletion> completions;
        private Future<?> workerFuture;
        private ScheduledFuture<?> timeoutFuture;
        private boolean timedOut;
        private boolean workerFinished;
        private boolean observerDetached;

        private PublicationAttempt(
                PartitionIdentity identity,
                long publisherEpoch,
                String scope,
                BlockingQueue<PublicationCompletion> completions) {
            this.identity = identity;
            this.key = new PublicationKey(identity.taskTopic(), identity.logId().id());
            this.publisherEpoch = publisherEpoch;
            this.scope = scope;
            this.completions = completions;
        }
    }

    /**
     * Shares admission and sticky in-flight ownership across publisher runner recreation.
     *
     * <p>Normal publication is limited to the configured parallelism. A started attempt that
     * exceeds its deadline becomes sticky until its callable really returns, but releases its
     * normal slot so a healthy identity can make progress. At most two full worker windows may be
     * sticky; reaching that explicit bound stops new publication without blocking control tasks.
     */
    static final class PublicationCoordinator {
        private final int normalParallelism;
        private final int maxStickyAttempts;
        private final Map<PublicationKey, PublicationAttempt> inFlight = new HashMap<>();
        private int activeAttempts;
        private int stickyAttempts;
        private boolean stickyLimitReported;

        PublicationCoordinator(int configuredParallelism) {
            normalParallelism = Math.max(1, configuredParallelism);
            maxStickyAttempts = (int) Math.min(
                    Integer.MAX_VALUE, Math.multiplyExact((long) normalParallelism, 2L));
        }

        synchronized RegistrationStatus tryRegister(PublicationAttempt attempt) {
            if (inFlight.containsKey(attempt.key)) {
                return RegistrationStatus.DUPLICATE;
            }
            if (activeAttempts >= normalParallelism) {
                return RegistrationStatus.WINDOW_FULL;
            }
            if (activeAttempts + stickyAttempts >= maxStickyAttempts) {
                return stickyAttempts >= maxStickyAttempts
                        ? RegistrationStatus.STICKY_LIMIT
                        : RegistrationStatus.WINDOW_FULL;
            }
            inFlight.put(attempt.key, attempt);
            activeAttempts++;
            return RegistrationStatus.REGISTERED;
        }

        synchronized void submissionFailed(PublicationAttempt attempt) {
            if (inFlight.remove(attempt.key, attempt)) {
                activeAttempts--;
            }
        }

        synchronized boolean timedOut(PublicationAttempt attempt) {
            if (inFlight.get(attempt.key) != attempt) {
                return false;
            }
            activeAttempts--;
            stickyAttempts++;
            if (stickyAttempts >= maxStickyAttempts && !stickyLimitReported) {
                stickyLimitReported = true;
                return true;
            }
            return false;
        }

        synchronized void finished(PublicationAttempt attempt, boolean timedOut) {
            if (!inFlight.remove(attempt.key, attempt)) {
                return;
            }
            if (timedOut) {
                stickyAttempts--;
                if (stickyAttempts < maxStickyAttempts) {
                    stickyLimitReported = false;
                }
            } else {
                activeAttempts--;
            }
        }

        synchronized boolean isStickyLimitReached() {
            return stickyAttempts >= maxStickyAttempts;
        }

        int normalParallelism() {
            return normalParallelism;
        }

        int maxStickyAttempts() {
            return maxStickyAttempts;
        }
    }

    private record PartitionIdentity(StreamIdentifier stream, int partition, LogId logId) {

        private PartitionIdentity {
            Objects.requireNonNull(stream, "stream");
            Objects.requireNonNull(logId, "logId");
            if (partition < 0) {
                throw new IllegalArgumentException("partition must be non-negative");
            }
        }

        private String taskTopic() {
            return PublishCompactTaskRunner.taskTopic(stream, partition);
        }
    }

    private record TaskSnapshot(PreparedCompactStreamTask task, long latestOffset, long publishedOffset) {
    }

    private record PublicationQuarantine(PartitionIdentity identity, long retryAfterMillis) {
    }

    private static final class PartitionPublisher {
        private final PartitionIdentity identity;
        private final CompactionManager.PublicationSession session;
        private final AtomicBoolean closeRetryScheduled = new AtomicBoolean();

        private PartitionPublisher(PartitionIdentity identity,
                                   CompactionManager.PublicationSession session) {
            this.identity = identity;
            this.session = session;
        }

        private PartitionIdentity identity() {
            return identity;
        }

        private long streamId() {
            return session.streamId();
        }

        private CompactionManager.PublicationSession session() {
            return session;
        }

        private AtomicBoolean closeRetryScheduled() {
            return closeRetryScheduled;
        }

        private void fence() {
            session.fence();
        }
    }
}
