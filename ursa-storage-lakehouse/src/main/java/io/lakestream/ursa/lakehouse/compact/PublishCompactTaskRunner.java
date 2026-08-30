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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
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
    private static final int MAX_FAIR_PUBLICATION_QUANTUM = 100;
    private static final AttributeKey<String> TOPIC_ATTRIBUTE = AttributeKey.stringKey("topic");

    private final StreamCatalog streamCatalog;
    private final CompactionManager compactionManager;
    private final ExecutorService scanExecutor;
    private final ScheduledExecutorService publishExecutor;
    private final long scanIntervalMillis;
    private final int checkMessageStepLength;
    private final int maxTasksPerPublisherPerScan;
    private final long compactedFileSizeLimit;
    private final long tailVisibilityMillis;
    private final Properties baseProperties;
    private final Set<String> excludedNamespaces;
    private final Set<String> excludedStreams;
    private final CompactionMetrics compactionMetrics;
    private final Map<String, PartitionPublisher> publishers = new ConcurrentHashMap<>();
    private final Map<String, Long> tailWaitStartedAtMillis = new ConcurrentHashMap<>();
    private final Map<String, Long> unavailablePublicationLeases = new ConcurrentHashMap<>();
    private final AtomicBoolean pendingLeaseReleaseRetryScheduled = new AtomicBoolean();

    private volatile boolean stopped;
    private volatile boolean backlogRemaining;
    private volatile boolean publicationLeaseUnavailable;
    private volatile Future<?> scanFuture;
    private volatile ScheduledFuture<?> nextScanFuture;

    public PublishCompactTaskRunner(StreamCatalog streamCatalog,
                                    CompactTaskManager compactTaskManager,
                                    ExecutorService scanExecutor,
                                    ScheduledExecutorService publishExecutor,
                                    StorageConfig storageConfig,
                                    CompactionMetrics compactionMetrics) {
        this(streamCatalog,
                new CompactionManager(compactTaskManager, compactionMetrics),
                scanExecutor,
                publishExecutor,
                storageConfig,
                compactionMetrics);
    }

    PublishCompactTaskRunner(StreamCatalog streamCatalog,
                             CompactionManager compactionManager,
                             ExecutorService scanExecutor,
                             ScheduledExecutorService publishExecutor,
                             StorageConfig storageConfig,
                             CompactionMetrics compactionMetrics) {
        this.streamCatalog = Objects.requireNonNull(streamCatalog, "streamCatalog");
        this.compactionManager = Objects.requireNonNull(compactionManager, "compactionManager");
        this.scanExecutor = Objects.requireNonNull(scanExecutor, "scanExecutor");
        this.publishExecutor = Objects.requireNonNull(publishExecutor, "publishExecutor");
        this.baseProperties = Objects.requireNonNull(storageConfig, "storageConfig").getProperties();
        this.compactionMetrics = Objects.requireNonNull(compactionMetrics, "compactionMetrics");
        this.scanIntervalMillis = TimeUnit.SECONDS.toMillis(
                Math.max(1L, storageConfig.getRefreshLocalTaskIntervalInSeconds()));
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
        synchronized (this) {
            if (stopped || (scanFuture != null && !scanFuture.isDone())) {
                return;
            }
            scanFuture = scanExecutor.submit(this);
        }
    }

    @Override
    public void run() {
        boolean scheduleNextScan = true;
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
        } catch (Error fatal) {
            scheduleNextScan = false;
            throw fatal;
        } finally {
            synchronized (this) {
                if (!stopped && scheduleNextScan) {
                    long nextDelayMillis = publicationLeaseUnavailable
                            ? Math.min(1000L, scanIntervalMillis) : scanIntervalMillis;
                    nextScanFuture = publishExecutor.schedule(
                            this::start, nextDelayMillis, TimeUnit.MILLISECONDS);
                }
            }
        }
    }

    /** Performs one complete catalog scan. Visible for deterministic unit tests. */
    void scanCatalogOnce() throws Exception {
        backlogRemaining = false;
        publicationLeaseUnavailable = false;
        retryPendingLeaseReleases();
        Map<String, PartitionIdentity> discovered = discoverPartitions();
        unavailablePublicationLeases.keySet().retainAll(discovered.keySet());
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
        List<Future<?>> publicationFutures = new ArrayList<>();
        for (PartitionIdentity identity : discovered) {
            if (stopped) {
                break;
            }
            try {
                publicationFutures.add(publishExecutor.submit(() -> publishIdentity(identity)));
            } catch (RejectedExecutionException rejected) {
                if (!stopped) {
                    recordPartitionFailure(identity.taskTopic(), "schedule task publication", rejected);
                }
            }
        }
        awaitPublication(publicationFutures, "catalog scan");
    }

    private void publishExisting(List<PartitionPublisher> existing) throws Exception {
        List<Future<?>> publicationFutures = new ArrayList<>();
        for (PartitionPublisher publisher : existing) {
            if (stopped) {
                break;
            }
            try {
                publicationFutures.add(publishExecutor.submit(() -> publishAvailable(publisher)));
            } catch (RejectedExecutionException rejected) {
                if (!stopped) {
                    recordPartitionFailure(
                            publisher.identity().taskTopic(), "schedule backlog publication", rejected);
                }
            }
        }
        awaitPublication(publicationFutures, "publisher backlog");
    }

    private void publishIdentity(PartitionIdentity identity) {
        try {
            PartitionPublisher publisher = publishers.get(identity.taskTopic());
            if (publisher == null) {
                publisher = openPublisher(identity);
                if (publisher == null) {
                    return;
                }
            }
            publishAvailable(publisher);
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
        } catch (Exception error) {
            recordPartitionFailure(identity.taskTopic(), "open task publisher", error);
        }
    }

    private void awaitPublication(List<Future<?>> publicationFutures, String scope) throws Exception {
        for (Future<?> publicationFuture : publicationFutures) {
            try {
                publicationFuture.get();
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                throw interrupted;
            } catch (ExecutionException error) {
                if (error.getCause() instanceof Error fatal) {
                    throw fatal;
                }
                recordPartitionFailure(scope, "complete task publication", error.getCause());
            }
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

    private PartitionPublisher openPublisher(PartitionIdentity identity) throws Exception {
        Optional<CompactionManager.PublicationSession> session;
        try {
            session = compactionManager.tryOpenPublicationSession(
                    identity.taskTopic(), identity.logId().id());
        } catch (Exception | Error error) {
            schedulePendingLeaseReleaseRetry();
            throw error;
        }
        if (session.isEmpty()) {
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
            if (!stopped) {
                PartitionPublisher raced = publishers.putIfAbsent(identity.taskTopic(), candidate);
                selected = raced == null ? candidate : raced;
            }
        }
        if (selected != candidate) {
            // stop() and session installation share the same monitor. If leadership was lost while
            // the remote lease was being acquired, this closes the late session instead of letting
            // it escape the stop-time fencing snapshot.
            closePublisher(candidate);
        }
        return selected;
    }

    private void publishAvailable(PartitionPublisher publisher) {
        try {
            for (int published = 0; published < maxTasksPerPublisherPerScan; published++) {
                if (stopped || publishers.get(publisher.identity().taskTopic()) != publisher
                        || publishNext(publisher) != CompactionManager.PublicationResult.PUBLISHED) {
                    return;
                }
            }
            backlogRemaining = true;
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
        } catch (Exception error) {
            recordPartitionFailure(publisher.identity().taskTopic(), "publish compaction task", error);
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
            throw new IllegalStateException(
                    "Selected compaction range ends before the persisted cumulative-size cursor");
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
        }
        sessions.values().forEach(this::closePublisherAfterStop);
        schedulePendingLeaseReleaseRetry();
        recordActivePublisherCount();
    }

    private void closePublisherAfterStop(PartitionPublisher publisher) {
        try {
            publishExecutor.execute(() -> tryClosePublisher(publisher));
        } catch (RejectedExecutionException rejected) {
            log.warn("Failed to schedule publication-session close for {}; closing inline",
                    publisher.identity().taskTopic(), rejected);
            tryClosePublisher(publisher);
        }
    }

    private void closePublisher(PartitionPublisher publisher) {
        publisher.fence();
        tryClosePublisher(publisher);
    }

    private void tryClosePublisher(PartitionPublisher publisher) {
        try {
            publisher.session().close();
            publisher.closeRetryScheduled().set(false);
        } catch (Exception | Error error) {
            if (error instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            log.warn("Failed to close compaction publication session for {}",
                    publisher.identity().taskTopic(), error);
            schedulePendingLeaseReleaseRetry();
            scheduleCloseRetry(publisher);
            if (error instanceof Error fatal) {
                throw fatal;
            }
        }
    }

    private void scheduleCloseRetry(PartitionPublisher publisher) {
        if (!publisher.closeRetryScheduled().compareAndSet(false, true)) {
            return;
        }
        try {
            publishExecutor.schedule(() -> {
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
        try {
            compactionManager.retryPendingPublicationLeaseReleases();
        } catch (Exception error) {
            if (error instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            log.warn("Failed to release an unsettled compaction publication lease", error);
            schedulePendingLeaseReleaseRetry();
        }
    }

    private void schedulePendingLeaseReleaseRetry() {
        if (!compactionManager.hasPendingPublicationLeaseReleases()
                || !pendingLeaseReleaseRetryScheduled.compareAndSet(false, true)) {
            return;
        }
        try {
            publishExecutor.schedule(() -> {
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
    }

    private void retainExistingStream(
            StreamIdentifier stream, Map<String, PartitionIdentity> discovered) {
        publishers.values().stream()
                .map(PartitionPublisher::identity)
                .filter(identity -> identity.stream().equals(stream))
                .forEach(identity -> discovered.putIfAbsent(identity.taskTopic(), identity));
    }

    int sessionCount() {
        return publishers.size();
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
