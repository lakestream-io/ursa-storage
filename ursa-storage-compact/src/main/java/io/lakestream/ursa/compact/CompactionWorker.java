/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.compact;

import io.lakestream.api.LogId;
import io.lakestream.api.Stream;
import io.lakestream.api.StreamCatalog;
import io.lakestream.api.StreamIdentifier;
import io.lakestream.api.exception.NoSuchStreamException;
import io.lakestream.api.exception.PartitionLifecycleFencedException;
import io.lakestream.api.exception.StreamPermanentlyDeletedException;
import io.lakestream.api.materialization.ResolvedMaterialization;
import io.lakestream.api.materialization.TableCatalog;
import io.lakestream.ursa.compaction.CompactTaskManager;
import io.lakestream.ursa.compaction.metrics.CompactionMetrics;
import io.lakestream.ursa.compaction.task.CompactStreamTask;
import io.lakestream.ursa.compaction.task.PackagedCompactStreamTask;
import io.lakestream.ursa.exception.ExceptionCode;
import io.lakestream.ursa.exception.ExceptionWithCode;
import io.lakestream.ursa.exception.RuntimeExceptionWithCode;
import io.lakestream.ursa.materialization.MaterializationException;
import io.lakestream.ursa.materialization.MaterializationService;
import io.lakestream.ursa.materialization.MaterializationTask;
import io.lakestream.ursa.storage.impl.StorageConfig;
import io.lakestream.ursa.storage.impl.compaction.CompactionService;
import io.lakestream.ursa.storage.impl.compaction.CompactionTaskProviderV2;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;

/**
 * Per-thread compaction worker invoked by {@code CompactionScheduler}.
 *
 * <p>T10 collapses the lakehouse-coupled control flow into a sink-neutral path:
 * <ul>
 *   <li>The internal WAL → Compacted Object compaction is still dispatched through
 *       {@link CompactionService#compactStream(CompactStreamTask)}.</li>
 *   <li>If the stream resolves an
 *       {@link io.lakestream.api.materialization.ResolvedMaterialization}
 *       policy, the worker hands the task to
 *       {@link MaterializationService#materialize(MaterializationTask)}.</li>
 *   <li>On {@link MaterializationException} the worker reads the carried
 *       {@link ExceptionCode} (T5 polish) and uses the same retry / quarantine
 *       logic previously gated on lakehouse exception subclasses. Non-retryable
 *       codes also call {@link MaterializationService#invalidate(StreamIdentifier)}
 *       so the sink can drop cached writer state.</li>
 * </ul>
 *
 * <p>This class no longer imports any integration-package class — verified by
 * the grep gate in T10.
 */
@Slf4j
public class CompactionWorker implements Runnable {

    private static final Pattern PARTITION_SUFFIX = Pattern.compile("-partition-(\\d+)$");

    private final CompactTaskManager compactTaskManager;
    private final CompactionService compactionService;
    @Nullable
    private final MaterializationService materializationService;
    @Nullable
    private final StreamCatalog streamCatalog;
    private final CompactionTaskProviderV2 compactionTaskProvider;
    private final long retryableTaskQuarantineInMs;
    private final long nonRetryableTaskQuarantineInMs;
    private final long waitForAvailableTaskIntervalInMs;
    private final CompactionMetrics compactionMetrics;
    private final Set<String> blackTopicOfCompact;
    private final StorageConfig config;

    /**
     * Legacy three-arg constructor preserved for the existing
     * {@link CompactionWorkerTest} suite. New call sites supply the
     * materialization service + stream catalog via
     * {@link #CompactionWorker(CompactTaskManager, CompactionService,
     * MaterializationService, StreamCatalog, CompactionTaskProviderV2,
     * StorageConfig, CompactionMetrics)}.
     */
    public CompactionWorker(CompactTaskManager compactTaskManager, CompactionService compactionService,
                            CompactionTaskProviderV2 compactionTaskProvider, StorageConfig config,
                            CompactionMetrics compactionMetrics) {
        this(compactTaskManager, compactionService, null, null,
                compactionTaskProvider, config, compactionMetrics);
    }

    @SuppressWarnings("ParameterNumber")
    public CompactionWorker(CompactTaskManager compactTaskManager, CompactionService compactionService,
                            @Nullable MaterializationService materializationService,
                            @Nullable StreamCatalog streamCatalog,
                            CompactionTaskProviderV2 compactionTaskProvider, StorageConfig config,
                            CompactionMetrics compactionMetrics) {
        this.compactTaskManager = compactTaskManager;
        this.compactionService = compactionService;
        this.materializationService = materializationService;
        this.streamCatalog = streamCatalog;
        this.config = config;
        this.compactionTaskProvider = compactionTaskProvider;
        this.compactionMetrics = compactionMetrics;
        this.retryableTaskQuarantineInMs = TimeUnit.SECONDS.toMillis(config.getRetryableQuarantineInSeconds());
        this.nonRetryableTaskQuarantineInMs = TimeUnit.SECONDS.toMillis(config.getNonRetryableQuarantineInSeconds());
        this.waitForAvailableTaskIntervalInMs =
                TimeUnit.SECONDS.toMillis(config.getRefreshLocalTaskIntervalInSeconds());
        this.blackTopicOfCompact = config.getBlackTopicOfCompact()
                .stream()
                .map(CompactionWorker::parseBlacklistedStream)
                .flatMap(Optional::stream)
                .collect(Collectors.toSet());
    }

    private static Optional<String> parseBlacklistedStream(String configuredName) {
        try {
            return Optional.of(partitionedStreamName(configuredName));
        } catch (RuntimeException invalidName) {
            log.warn("Ignoring invalid blackTopicOfCompact entry '{}'", configuredName, invalidName);
            return Optional.empty();
        }
    }

    @Override
    public void run() {
        log.info("Start compact runner thread {}...", Thread.currentThread().getName());
        while (true) {
            PackagedCompactStreamTask compactionTask = null;
            String failedTopicName = null;
            CompactStreamTask failedCompactTask = null;
            try {
                compactionTask = compactionTaskProvider.getTask();
                if (compactionTask == null) {
                    if (log.isDebugEnabled()) {
                        log.debug("No available tasks, wait for {}ms", waitForAvailableTaskIntervalInMs);
                    }
                    Thread.sleep(waitForAvailableTaskIntervalInMs);
                    continue;
                }

                String taskName = compactionTask.getTaskName();
                List<CompactStreamTask> validCompactTasks = new ArrayList<>();
                long currentTime = System.currentTimeMillis();
                for (String subTask : compactionTask.getSubTasks()) {
                    CompactStreamTask compactStreamTask = null;
                    try {
                        compactStreamTask = compactTaskManager.getCompactStreamTask(subTask).get();
                    } catch (InterruptedException interrupted) {
                        Thread.currentThread().interrupt();
                        throw interrupted;
                    } catch (Exception e) {
                        log.warn("Failed to get compact stream task {} for task {}",
                                subTask, taskName, e);
                        continue;
                    }

                    if (compactStreamTask == null) {
                        continue;
                    }

                    if (isBlacklistedTopic(compactStreamTask.getTopic())) {
                        if (log.isDebugEnabled()) {
                            log.debug("Skip blacklisted topic {} for task {}",
                                    compactStreamTask.getTopic(), taskName);
                        }
                        continue;
                    }

                    Long quarantinedUntil =
                            compactionTaskProvider.getQuarantinedTopic(compactStreamTask.getTopic());
                    if (compactStreamTask.getStatus() == CompactStreamTask.INIT
                            && (quarantinedUntil == null || quarantinedUntil < currentTime)) {
                        // remove the quarantined topic if it is expired
                        if (quarantinedUntil != null) {
                            compactionTaskProvider.removeQuarantinedTopic(compactStreamTask.getTopic());
                        }
                        validCompactTasks.add(compactStreamTask);
                    }
                }
                if (validCompactTasks.isEmpty()) {
                    long quarantineUntil = System.currentTimeMillis() + retryableTaskQuarantineInMs;
                    if (log.isDebugEnabled()) {
                        log.debug("Quarantine task {} for {}ms until {} due to the task is invalid.",
                            taskName, retryableTaskQuarantineInMs, quarantineUntil);
                    }
                    compactionTaskProvider.quarantineTask(quarantineUntil, taskName);
                    continue;
                }
                if (!compactTaskManager.tryLockTask(taskName)) {
                    long quarantineUntil = System.currentTimeMillis() + retryableTaskQuarantineInMs;
                    if (log.isDebugEnabled()) {
                        log.debug("Quarantine task {} for {}ms until {} due to lock task failed.",
                            taskName, retryableTaskQuarantineInMs, quarantineUntil);
                    }
                    compactionTaskProvider.quarantineTask(quarantineUntil, taskName);
                    continue;
                }

                try {
                    for (CompactStreamTask validCompactTask : validCompactTasks) {
                        try {
                            if (config != null && config.isMaterializationEnabled()) {
                                // Primary path: dispatch through the sink-neutral materialization SPI
                                // (unified SBT + SDT). The legacy internal-compaction call below is the
                                // flag-controlled fallback only.
                                maybeMaterialize(validCompactTask);
                            } else {
                                // Fallback path (materializationEnabled=false): the legacy internal
                                // WAL -> Compacted Object compaction. Deprecated in favour of the
                                // materialization SPI; retained so deployments can roll back.
                                compactionService.compactStream(validCompactTask);
                            }
                        } catch (MaterializationException me) {
                            failedTopicName = validCompactTask.getTopic();
                            failedCompactTask = validCompactTask;
                            // Sink-neutral failure path: invalidate cached state when the code is
                            // non-retryable so the sink can drop writer state. The outer
                            // ExceptionWithCode handling still applies because
                            // MaterializationException extends RuntimeExceptionWithCode.
                            ExceptionCode code = me.getExceptionCode();
                            if (!isPureRetryCode(code) && materializationService != null) {
                                try {
                                    materializationService.invalidate(
                                            toStreamIdentifier(validCompactTask.getTopic()));
                                } catch (Throwable invalidationFailure) {
                                    log.warn("Failed to invalidate stream {} after materialization failure",
                                            validCompactTask.getTopic(), invalidationFailure);
                                }
                            }
                            throw me;
                        } catch (Throwable e) {
                            failedTopicName = validCompactTask.getTopic();
                            failedCompactTask = validCompactTask;
                            throw e;
                        }
                    }
                } finally {
                    compactTaskManager.unlockTaskAndRemoveLock(taskName);
                }
            } catch (Throwable e) {
                // Workers are submitted once as long-lived Runnables. Letting an Error escape would
                // permanently remove one worker from the pool, so supervise every task failure here.
                // A MaterializationException that happens to carry an Error still follows its explicit
                // ExceptionCode; a directly thrown Error falls back to the non-retryable policy below.
                if (e instanceof InterruptedException) {
                    log.warn("[{}] Compact runner thread interrupted", compactionTask, e);
                    break;
                }

                recordTaskFailureBestEffort(compactionTask, e);

                boolean failureHandlingFailed = false;
                try {
                    // Per-code quarantine: immediate retry for transient source read/throttle
                    // failures, shorter retryable quarantine for exhausted/input-client failures
                    // (which can recur on the same topic and would otherwise hot-loop the worker),
                    // and the existing nonRetryable quarantine for everything else.
                    ExceptionCode code = exceptionCode(e);
                    if (failedCompactTask != null && isTerminalCode(code)) {
                        if (deleteTerminalTask(failedCompactTask)) {
                            continue;
                        }
                    }
                    if (failedTopicName != null && !isPureRetryCode(code)) {
                        long quarantineMs = isRetryableQuarantineCode(code)
                                ? retryableTaskQuarantineInMs : nonRetryableTaskQuarantineInMs;
                        long currentTime = System.currentTimeMillis();
                        log.info("Quarantine topic {} for {}ms until {} due to the task failed (code={}).",
                            failedTopicName, quarantineMs, currentTime + quarantineMs, code);
                        compactionTaskProvider.quarantineTopic(
                                failedTopicName, currentTime + quarantineMs);
                    }
                    if (!isPureRetryCode(code) && code != ExceptionCode.NO_MORE_RECORDS) {
                        compactionMetrics.getFailedCompactTaskCount().increment();
                    }
                } catch (InterruptedException handlingInterrupted) {
                    Thread.currentThread().interrupt();
                    log.warn("Compact runner interrupted while handling task failure",
                            handlingInterrupted);
                    break;
                } catch (Throwable handlingFailure) {
                    failureHandlingFailed = true;
                    if (handlingFailure != e) {
                        e.addSuppressed(handlingFailure);
                    }
                    try {
                        log.error("Failed to apply compaction failure policy; keeping worker supervised",
                                handlingFailure);
                    } catch (Throwable observabilityFailure) {
                        if (observabilityFailure != e && observabilityFailure != handlingFailure) {
                            e.addSuppressed(observabilityFailure);
                        }
                    }
                }

                // Before a subtask identifies its topic there is nothing durable to quarantine.
                // Back off the long-lived worker so a persistent provider/linkage Error cannot turn
                // every worker into a CPU and log storm. Also back off when quarantine/metrics fail.
                if ((failedTopicName == null || failureHandlingFailed)
                        && !pauseAfterUnscopedFailure()) {
                    break;
                }
            }
        }
    }

    private static void recordTaskFailureBestEffort(
            PackagedCompactStreamTask compactionTask, Throwable failure) {
        try {
            if (failure instanceof Error) {
                log.error("[{}] Fatal error during compaction; keeping worker supervised",
                        compactionTask, failure);
            } else {
                log.warn("[{}] During compact error", compactionTask, failure);
            }
        } catch (Throwable observabilityFailure) {
            if (observabilityFailure != failure) {
                failure.addSuppressed(observabilityFailure);
            }
        }
    }

    private boolean pauseAfterUnscopedFailure() {
        try {
            Thread.sleep(Math.max(1L, waitForAvailableTaskIntervalInMs));
            return true;
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            log.warn("Compact runner interrupted during supervised failure backoff", interrupted);
            return false;
        }
    }

    /**
     * Resolves the stream's effective materialization policy, reads the task's WAL entries, and
     * dispatches them to the {@link MaterializationService}.
     *
     * <p>Throws a {@link MaterializationException} when materialization is disabled, not wired
     * (legacy constructor / catalog absent), the topic is not a materializable stream, or the
     * stream resolves no policy. Failures that would otherwise silently drop the offset range
     * (stream load failure, WAL read failure) are escalated as a {@link MaterializationException}
     * so the worker's quarantine/retry path re-runs the task rather than advancing past
     * unmaterialized data.
     */
    private void maybeMaterialize(CompactStreamTask task) {
        if (config != null && !config.isMaterializationEnabled()) {
            log.warn("Materialization disabled by config. skipping materialization for stream {} (task {})",
                    task.getTopic(), task.getTaskName());
            throw new MaterializationException(ExceptionCode.INTERNAL_ERROR, "Materialization disabled");
        }
        if (materializationService == null || streamCatalog == null) {
            log.warn("Materialization service or stream catalog not available. "
                    + "skipping materialization for stream {} (task {})", task.getTopic(), task.getTaskName());
            throw new MaterializationException(ExceptionCode.INTERNAL_ERROR,
                "Materialization service or stream catalog not available");
        }
        StreamIdentifier id;
        try {
            id = toStreamIdentifier(task.getTopic());
        } catch (RuntimeException re) {
            // A topic that is not a parseable stream identifier is not materializable; skip.
            throw new MaterializationException(ExceptionCode.INTERNAL_ERROR,
                "Skipping materialization for unparsable topic " + task.getTopic(), re);
        }
        Map<String, String> props = task.getProperties() == null ? Map.of() : task.getProperties();
        Stream stream = resolveMaterializationStream(task, id, props);
        boolean ownershipTransferred = false;
        Throwable materializationFailure = null;
        try {
            Optional<ResolvedMaterialization> resolved = stream.effectiveMaterialization();
            if (resolved.isEmpty()) {
                // Back-compat: no stream/namespace/cluster policy resolved. Deployments that drive
                // materialization through compaction task properties (legacy DynamicConfigs + catalog
                // config) carry the config on the task, so fall back to resolving from the task properties.
                // TODO: This is current task property resolution, which is deprecated in favor of the
                //  stream/namespace/cluster policy. Remove this fallback once all deployments have migrated
                //  to the new policy resolution.
                resolved = materializationService.resolveFromTaskProperties(id, task.getTopic(), props)
                        .map(this::withRegisteredCatalog);
            }
            if (resolved.isEmpty()) {
                log.warn("Stream {} has no effective materialization policy and no materialization task "
                        + "properties; skipping materialization for task {}", task.getTopic(), task.getTaskName());
                throw new MaterializationException(ExceptionCode.INTERNAL_ERROR,
                    "No effective materialization policy for stream " + id.fullName());
            }
            // Hand ownership of the opened Stream handle to the materialization service so the
            // factory can build sink-specific materializers. Until registration succeeds, this
            // method remains responsible for closing the handle on every exit path.
            materializationService.registerActiveStream(id, stream);
            ownershipTransferred = true;
            // The service reads source entries for the offset range itself (Ursa or Kafka
            // via the source-aware reader factory) and decodes + writes them. The compaction module does
            // not read or carry entries, keeping it free of integration-package reader types.
            MaterializationTask mt = new MaterializationTask(
                    id,
                    resolved.get(),
                    task.getTopic(),
                    task.getStreamId(),
                    task.getStartOffset(),
                    task.getEndOffset(),
                    task);
            // The Lakehouse service records write results onto the task and persists it as COMPACTED
            // for the group-commit runner; ClickHouse commits inline. The returned CommitResult is not
            // needed by the worker today (metrics are recorded sink-side).
            materializationService.materialize(mt);
        } catch (RuntimeException | Error failure) {
            materializationFailure = failure;
            throw failure;
        } finally {
            if (!ownershipTransferred) {
                try {
                    stream.close();
                } catch (Exception closeFailure) {
                    if (materializationFailure != null) {
                        materializationFailure.addSuppressed(closeFailure);
                    } else {
                        throw new MaterializationException(ExceptionCode.INTERNAL_ERROR,
                                "Failed to close unregistered stream " + id.fullName(), closeFailure);
                    }
                }
            }
        }
    }

    private Stream resolveMaterializationStream(
            CompactStreamTask task,
            StreamIdentifier id,
            Map<String, String> properties) {
        int partitionIndex = partitionIndexOf(task.getTopic());
        Stream existing;
        try {
            existing = streamCatalog.loadStream(id).join();
        } catch (RuntimeException failure) {
            if (hasCause(failure, StreamPermanentlyDeletedException.class)) {
                throw catalogFailure(
                    "Stream " + id.fullName() + " was permanently deleted", failure);
            }
            if (!hasCause(failure, NoSuchStreamException.class)) {
                throw catalogFailure(
                    "Failed to load stream " + id.fullName() + " for materialization", failure);
            }
            return registerExternalAndLoad(task, id, partitionIndex, properties);
        }

        List<LogId> logIds;
        try {
            logIds = existing.layout().logIds().join();
        } catch (RuntimeException failure) {
            closeBeforeFallback(existing, failure);
            throw catalogFailure(
                "Failed to read stream layout for " + id.fullName(), failure);
        }
        if (partitionIndex >= logIds.size() || logIds.get(partitionIndex).id() < 0) {
            closeBeforeFallback(existing, null);
            return registerExternalAndLoad(task, id, partitionIndex, properties);
        }
        try {
            verifyTaskLogIdentity(task, id, partitionIndex, logIds.get(partitionIndex));
        } catch (RuntimeException failure) {
            closeBeforeFallback(existing, failure);
            throw catalogFailure(
                "Stream " + id.fullName() + " does not match the compaction task", failure);
        }
        return existing;
    }

    private Stream registerExternalAndLoad(
            CompactStreamTask task,
            StreamIdentifier id,
            int partitionIndex,
            Map<String, String> properties) {
        try {
            streamCatalog.registerExternalPartition(
                id, partitionIndex, task.getStreamId(), properties).join();
        } catch (RuntimeException failure) {
            throw catalogFailure(
                "Failed to register stream " + id.fullName() + " for materialization", failure);
        }

        Stream registered;
        try {
            registered = streamCatalog.loadStream(id).join();
        } catch (RuntimeException failure) {
            throw catalogFailure(
                "Failed to load registered stream " + id.fullName() + " for materialization", failure);
        }
        try {
            List<LogId> logIds = registered.layout().logIds().join();
            if (partitionIndex >= logIds.size()) {
                throw new PartitionLifecycleFencedException(
                    id, partitionIndex, "registered stream layout does not contain the task partition");
            }
            verifyTaskLogIdentity(task, id, partitionIndex, logIds.get(partitionIndex));
            return registered;
        } catch (RuntimeException failure) {
            closeBeforeFallback(registered, failure);
            throw catalogFailure(
                "Registered stream " + id.fullName() + " does not match the compaction task", failure);
        }
    }

    private static void verifyTaskLogIdentity(
            CompactStreamTask task,
            StreamIdentifier id,
            int partitionIndex,
            LogId actual) {
        if (actual.id() != task.getStreamId()) {
            log.warn("Reject stale compaction task {} for stream {} partition {}: task logId {} "
                    + "differs from catalog logId {}; terminal cleanup will delete the task "
                    + "without changing the catalog registration",
                task.getTaskName(), id.fullName(), partitionIndex, task.getStreamId(), actual.id());
            throw new PartitionLifecycleFencedException(
                id,
                partitionIndex,
                "stale compaction task " + task.getTaskName() + " references physical log "
                    + task.getStreamId() + " but the catalog owns " + actual.id());
        }
    }

    private static void closeBeforeFallback(Stream stream, @Nullable Throwable failure) {
        try {
            stream.close();
        } catch (Exception closeFailure) {
            if (failure != null) {
                failure.addSuppressed(closeFailure);
            } else {
                throw new MaterializationException(
                    ExceptionCode.INTERNAL_ERROR,
                    "Failed to close stream before external registration fallback",
                    closeFailure);
            }
        }
    }

    /**
     * Prefers the operator-registered {@link TableCatalog} over the one the task-property fallback
     * synthesizes from flat connection properties. Catalog definitions are loaded once at startup from
     * the compaction-service properties ({@code *.catalog.<name>.*}) and registered in the
     * {@link StreamCatalog}; task properties carry only the catalog name. So when a catalog with the
     * resolved name is registered, that registration is the source of truth for type / connection /
     * properties — the task-derived table identifier and effective policy are kept. Falls back to the
     * synthesized catalog when nothing is registered under that name (or no catalog is available).
     */
    private ResolvedMaterialization withRegisteredCatalog(ResolvedMaterialization resolved) {
        if (streamCatalog == null) {
            return resolved;
        }
        String name = resolved.catalog().name();
        try {
            TableCatalog registered = streamCatalog.getTableCatalog(name).join();
            if (registered != null) {
                return new ResolvedMaterialization(registered, resolved.tableIdentifier(),
                        resolved.effectivePolicy());
            }
            if (log.isDebugEnabled()) {
                log.debug("No table catalog '{}' registered in the StreamCatalog; using the catalog "
                    + "synthesized from task properties for stream {}", name, resolved.tableIdentifier());
            }
        } catch (RuntimeException e) {
            log.warn("Failed to load registered table catalog '{}'; using the catalog synthesized from "
                    + "task properties", name, e);
        }
        return resolved;
    }

    private static boolean isPureRetryCode(ExceptionCode code) {
        return code == ExceptionCode.SOURCE_READ_ERROR
            || code == ExceptionCode.SOURCE_THROTTLED;
    }

    private static boolean isRetryableQuarantineCode(ExceptionCode code) {
        return code == ExceptionCode.NO_MORE_RECORDS
            || code == ExceptionCode.SOURCE_CLIENT_ERROR;
    }

    private static MaterializationException catalogFailure(
            String message, RuntimeException failure) {
        ExceptionCode code = hasCause(failure, StreamPermanentlyDeletedException.class)
                || hasCause(failure, PartitionLifecycleFencedException.class)
            ? ExceptionCode.NO_SUCH_STREAM
            : ExceptionCode.INTERNAL_ERROR;
        return new MaterializationException(code, message, failure);
    }

    private static boolean hasCause(Throwable failure, Class<? extends Throwable> type) {
        Throwable current = failure;
        while (current != null) {
            if (type.isInstance(current)) {
                return true;
            }
            Throwable cause = current.getCause();
            if (cause == current) {
                break;
            }
            current = cause;
        }
        return false;
    }

    private static ExceptionCode exceptionCode(Throwable error) {
        Throwable current = error;
        while (current != null) {
            if (current instanceof ExceptionWithCode exceptionWithCode) {
                return exceptionWithCode.getExceptionCode();
            }
            if (current instanceof RuntimeExceptionWithCode runtimeExceptionWithCode) {
                return runtimeExceptionWithCode.getRealException().getExceptionCode();
            }
            current = current.getCause();
        }
        return null;
    }

    private static boolean isTerminalCode(ExceptionCode code) {
        return code == ExceptionCode.NO_SUCH_LOG
                || code == ExceptionCode.NO_SUCH_STREAM
                || code == ExceptionCode.NO_SUCH_ENTRIES
                || code == ExceptionCode.NO_SUCH_OFFSET
                || code == ExceptionCode.COMPACTION_NO_WRITE_RESULT;
    }

    boolean deleteTerminalTask(CompactStreamTask task) throws InterruptedException {
        log.info("Delete terminal materialization task {} because its source returned {}",
                task.getTaskName(), task.getTopic());
        try {
            compactTaskManager.deleteCompactTask(task).get();
            return true;
        } catch (InterruptedException deleteInterrupted) {
            Thread.currentThread().interrupt();
            throw deleteInterrupted;
        } catch (Exception deleteFailure) {
            log.warn("Failed to delete terminal materialization task {}; it will be retried",
                    task.getTaskName(), deleteFailure);
            return false;
        }
    }

    /**
     * Maps a canonical {@code namespace/name-partition-N} log name to a stream identifier.
     * The partition suffix is stripped so lookup matches the per-stream catalog entry.
     */
    static StreamIdentifier toStreamIdentifier(String topic) {
        CanonicalName name = CanonicalName.parse(topic);
        return StreamIdentifier.of(name.namespace(), name.baseName());
    }

    /** Partition index for the topic, or 0 for a non-partitioned topic. */
    static int partitionIndexOf(String topic) {
        return CanonicalName.parse(topic).partition();
    }

    private boolean isBlacklistedTopic(String topic) {
        if (blackTopicOfCompact.isEmpty()) {
            return false;
        }
        String base = partitionedStreamName(topic);
        return blackTopicOfCompact.contains(base);
    }

    private static String partitionedStreamName(String topic) {
        CanonicalName name = CanonicalName.parse(topic);
        return name.namespace() + "/" + name.baseName();
    }

    private record CanonicalName(String namespace, String localName, String baseName, int partition) {

        private static CanonicalName parse(String value) {
            if (value == null || value.isBlank() || value.contains("://")) {
                throw new IllegalArgumentException("Invalid stream name: " + value);
            }
            String canonical = value.strip();
            String namespace;
            String localName;
            int separator = canonical.lastIndexOf('/');
            if (separator < 0) {
                namespace = "default";
                localName = canonical;
            } else {
                namespace = canonical.substring(0, separator);
                localName = canonical.substring(separator + 1);
            }
            if (namespace.isBlank() || localName.isBlank()) {
                throw new IllegalArgumentException("Invalid stream name: " + value);
            }
            Matcher matcher = PARTITION_SUFFIX.matcher(localName);
            if (!matcher.find()) {
                return new CanonicalName(namespace, localName, localName, 0);
            }
            return new CanonicalName(namespace, localName, localName.substring(0, matcher.start()),
                    Integer.parseInt(matcher.group(1)));
        }
    }
}
