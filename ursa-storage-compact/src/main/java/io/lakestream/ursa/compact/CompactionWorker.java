/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.compact;

import io.lakestream.api.LogId;
import io.lakestream.api.StreamCatalog;
import io.lakestream.api.StreamIdentifier;
import io.lakestream.api.StreamMetadata;
import io.lakestream.api.exception.NoSuchStreamException;
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
                .map(CompactionWorker::partitionedStreamName)
                .collect(Collectors.toSet());
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
                            // Sink-neutral failure path: invalidate cached state when the code is
                            // non-retryable so the sink can drop writer state. The outer
                            // ExceptionWithCode handling still applies because
                            // MaterializationException extends RuntimeExceptionWithCode.
                            ExceptionCode code = me.getExceptionCode();
                            if (!isPureRetryCode(code) && materializationService != null) {
                                try {
                                    materializationService.invalidate(
                                            toStreamIdentifier(validCompactTask.getTopic()));
                                } catch (RuntimeException invalidationFailure) {
                                    log.warn("Failed to invalidate stream {} after materialization failure",
                                            validCompactTask.getTopic(), invalidationFailure);
                                }
                            }
                            failedTopicName = validCompactTask.getTopic();
                            failedCompactTask = validCompactTask;
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
                // To avoid the thread being interrupted by the throwable, we need to catch the throwable here.
                if (e instanceof InterruptedException) {
                    log.warn("[{}] Compact runner thread interrupted", compactionTask, e);
                    break;
                }

                log.warn("[{}] During compact error", compactionTask, e);

                // Per-code quarantine: immediate retry for transient source read/throttle failures,
                // shorter retryable quarantine for exhausted/input-client failures (which can recur on
                // the same topic and would otherwise hot-loop the worker), and the existing nonRetryable
                // quarantine for everything else.
                ExceptionCode code = exceptionCode(e);
                if (failedCompactTask != null && isTerminalCode(code)) {
                    try {
                        if (deleteTerminalTask(failedCompactTask)) {
                            continue;
                        }
                    } catch (InterruptedException deleteInterrupted) {
                        Thread.currentThread().interrupt();
                        log.warn("Compact runner interrupted while deleting terminal task {}",
                                failedCompactTask.getTaskName(), deleteInterrupted);
                        break;
                    }
                }
                if (failedTopicName != null && !isPureRetryCode(code)) {
                    long quarantineMs = isRetryableQuarantineCode(code)
                            ? retryableTaskQuarantineInMs : nonRetryableTaskQuarantineInMs;
                    long currentTime = System.currentTimeMillis();
                    log.info("Quarantine topic {} for {}ms until {} due to the task failed (code={}).",
                        failedTopicName, quarantineMs, currentTime + quarantineMs, code);
                    compactionTaskProvider.quarantineTopic(failedTopicName, currentTime + quarantineMs);
                }
                if (!isPureRetryCode(code) && code != ExceptionCode.NO_MORE_RECORDS) {
                    compactionMetrics.getFailedCompactTaskCount().increment();
                }
            }
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
        StreamMetadata streamMetadata;
        try {
            streamMetadata = streamCatalog.loadStream(id).join();
        } catch (RuntimeException re) {
            // Do not silently skip: a transient load failure would drop the range. Retry.
            throw catalogFailure(
                "Failed to load stream " + id.fullName() + " for materialization", re);
        }
        LogId taskLogId = LogId.of(task.getStreamId());
        try {
            if (!streamMetadata.layout().logIds().join().contains(taskLogId)) {
                throw new MaterializationException(ExceptionCode.NO_SUCH_LOG,
                        "Compaction task log " + taskLogId.id() + " does not belong to stream "
                                + id.fullName());
            }
        } catch (MaterializationException e) {
            throw e;
        } catch (RuntimeException re) {
            throw catalogFailure(
                    "Failed to validate log " + taskLogId.id() + " for stream " + id.fullName(), re);
        }
        Optional<ResolvedMaterialization> resolved;
        try {
            resolved = streamCatalog.resolveMaterialization(id).join();
        } catch (RuntimeException re) {
            throw catalogFailure(
                    "Failed to resolve materialization for stream " + id.fullName(), re);
        }
        if (resolved.isEmpty()) {
            // Back-compat: no stream/namespace/cluster policy resolved. Deployments that drive
            // materialization through compaction task properties (legacy DynamicConfigs + catalog
            // config) carry the config on the task, so fall back to resolving from the task properties.
            // TODO: Remove this fallback once all deployments have migrated to catalog policies.
            resolved = materializationService.resolveFromTaskProperties(id, task.getTopic(), props)
                    .map(this::withRegisteredCatalog);
        }
        if (resolved.isEmpty()) {
            log.warn("Stream {} has no effective materialization policy and no materialization task "
                    + "properties; skipping materialization for task {}", task.getTopic(), task.getTaskName());
            throw new MaterializationException(ExceptionCode.INTERNAL_ERROR,
                "No effective materialization policy for stream " + id.fullName());
        }
        // The service reads source entries for the offset range through StorageApi and decodes
        // and writes them. StreamMetadata is immutable and carries no closeable data-plane handle.
        MaterializationTask mt = new MaterializationTask(
                streamMetadata,
                resolved.get(),
                task.getTopic(),
                task.getStreamId(),
                task.getStartOffset(),
                task.getEndOffset(),
                task);
        materializationService.materialize(mt);
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
        ExceptionCode code = hasCause(failure, NoSuchStreamException.class)
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
            String[] parts = value.strip().split("/", -1);
            String namespace;
            String localName;
            if (parts.length == 1) {
                namespace = "default";
                localName = parts[0];
            } else if (parts.length == 2) {
                namespace = parts[0];
                localName = parts[1];
            } else {
                throw new IllegalArgumentException("Invalid stream name: " + value);
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
