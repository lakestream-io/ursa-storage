/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.lakehouse.cleaner;

import com.google.common.annotations.VisibleForTesting;
import io.lakestream.api.EntryIndex;
import io.lakestream.api.Position;
import io.lakestream.api.materialization.ResolvedMaterialization;
import io.lakestream.api.materialization.TableCatalogType;
import io.lakestream.api.materialization.TableMode;
import io.lakestream.ursa.lakehouse.LakehouseCommitter;
import io.lakestream.ursa.lakehouse.LakehouseConfiguration;
import io.lakestream.ursa.lakehouse.exception.LakehouseException;
import io.lakestream.ursa.lakehouse.v2.LakehouseWriterFactory;
import io.lakestream.ursa.lakehouse.writer.ParquetFileStat;
import io.lakestream.ursa.storage.FileStorage;
import io.lakestream.ursa.storage.StorageApi;
import io.lakestream.ursa.storage.impl.StorageConfig;
import io.lakestream.ursa.storage.impl.compaction.StartStopRunner;
import io.netty.util.concurrent.DefaultThreadFactory;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;

/**
 * Handles the actual deletion of compacted data (parquet files) for a given topic partition.
 * <p>
 * This class is responsible for:
 * <ul>
 *   <li>Splitting large cleanup tasks into manageable sub-tasks, each handling up to 1000 of files.</li>
 *   <li>For each sub-task:
 *     <ul>
 *       <li>Committing file deletions to the Lakehouse metadata (for managed tables, e.g., Iceberg/DeltaLake).</li>
 *       <li>Deleting the physical parquet files from cloud storage in batch.</li>
 *       <li>Removing the corresponding Oxia index entries to ensure logical consistency.</li>
 *     </ul>
 *   </li>
 *   <li>Ensuring all operations are idempotent and safe to retry in case of failures.</li>
 *   <li>Executing cleanup operations asynchronously using a dedicated thread pool.</li>
 * </ul>
 * <p>
 * This handler is invoked by {@link AsyncCompactedDataCleaner} and is a key part of the LIP-145 retention and cleanup
 * mechanism, ensuring that storage is reclaimed efficiently and safely.
 *
 * @see AsyncCompactedDataCleaner
 */
@Slf4j
public class CompactedDataCleanupHandler implements StartStopRunner {
    private final StorageConfig config;
    private final StorageApi storage;
    private final FileStorage fileStorage;
    private final ExecutorService executor;
    private final Function<String, Optional<ResolvedMaterialization>> materializationLookup;
    private static final int MAX_FILES_PER_TASK = 1000;
    private static final long INFLIGHT_DRAIN_TIMEOUT_SECS = 60L;

    // Tracks the in-flight cleanup chains so stop() can wait for them to finish before shutting
    // down the executor. Without this, async callbacks (e.g. Oxia/gRPC) that complete after the
    // executor terminates fail with RejectedExecutionException when they try to dispatch the next
    // stage onto the now-terminated executor.
    private final Set<CompletableFuture<Void>> inflightCleanups = ConcurrentHashMap.newKeySet();
    private final Object stateLock = new Object();

    private volatile boolean isCancel = false;

    /**
     * Constructs a new CompactedDataCleanupHandler.
     *
     * @param config      The storage configuration, including thread pool and file path settings.
     * @param storage     The storage API for reading indexes and updating stream metadata.
     * @param fileStorage The file storage API for deleting files from cloud storage.
     */
    public CompactedDataCleanupHandler(StorageConfig config, StorageApi storage, FileStorage fileStorage) {
        this(config, storage, fileStorage, null);
    }

    /**
     * Constructs a cleanup handler that can resolve per-stream catalog policies.
     *
     * @param config                storage and legacy lakehouse configuration
     * @param storage               storage API used to read and delete indexes
     * @param fileStorage           compacted-object storage
     * @param materializationLookup current resolved materialization by canonical stream/log name,
     *                              or {@code null} for legacy config-only deployments
     */
    public CompactedDataCleanupHandler(
            StorageConfig config,
            StorageApi storage,
            FileStorage fileStorage,
            Function<String, Optional<ResolvedMaterialization>> materializationLookup) {
        this.config = config;
        this.storage = storage;
        this.fileStorage = fileStorage;
        this.materializationLookup = materializationLookup;
        this.executor = Executors.newFixedThreadPool(config.getCompactedDataCleanupThreadNum(),
                new DefaultThreadFactory("ursa-compacted-data-cleanup"));
    }

    record SubTask(TopicCleanupTask parentTask, List<ParquetFileStat> parquetFiles, long endOffset) {
    }

    /**
     * Returns the list of LakehouseCommitter instances for the given topic.
     * Used to commit file deletions to the Lakehouse metadata (for managed tables).
     *
     * @param task The cleanup task.
     * @return The list of committers for the topic.
     */
    @VisibleForTesting
    List<LakehouseCommitter> getLakeHouseCommitters(TopicCleanupTask task) {
        return getLakehouseConfiguration(task)
                .map(lakehouseConfig -> LakehouseCommitter.get(
                        lakehouseConfig, task.getCompactionTopic()))
                .orElseGet(Collections::emptyList);
    }

    /**
     * Resolves the same managed-table configuration used by the writer and committer. A catalog
     * policy wins over deployment-wide legacy settings, so an explicit managed table identifier
     * cannot make cleanup target the incarnation-scoped storage name by mistake.
     */
    @VisibleForTesting
    Optional<LakehouseConfiguration> getLakehouseConfiguration(TopicCleanupTask task) {
        if (materializationLookup != null) {
            Optional<ResolvedMaterialization> resolved = materializationLookup.apply(task.getCompactionTopic());
            if (resolved.isPresent()) {
                ResolvedMaterialization materialization = resolved.get();
                TableMode mode = materialization.effectivePolicy().table()
                        .flatMap(table -> table.mode())
                        .orElse(TableMode.MANAGED);
                if (mode != TableMode.MANAGED) {
                    // External/custom tables do not contain the managed Compacted Objects.
                    return Optional.empty();
                }
                TableCatalogType catalogType = materialization.catalog().type();
                String prefix;
                switch (catalogType) {
                    case ICEBERG -> prefix = "iceberg";
                    case DELTA, DELTA_UC -> prefix = "delta";
                    case CLICKHOUSE, NONE -> {
                        return Optional.empty();
                    }
                    default -> throw new IllegalArgumentException(
                            "Unsupported managed table catalog type: " + catalogType);
                }
                return Optional.of(LakehouseWriterFactory.buildConfiguration(
                        materialization.catalog(), materialization.effectivePolicy(), prefix, Map.of()));
            }
        }

        LakehouseConfiguration legacyConfig = new LakehouseConfiguration(config.getProperties());
        if (legacyConfig.getStreamTableMode() == LakehouseConfiguration.StreamTableMode.EXTERNAL) {
            // For external tables, we don't need to commit deletions to the Lakehouse.
            return Optional.empty();
        }
        return Optional.of(legacyConfig);
    }

    /**
     * Splits a large cleanup task into smaller sub-tasks, each handling up to a configured number (1000) of files.
     * Only compacted (parquet) files before the mark-deleted offset are included.
     *
     * @param task The parent cleanup task.
     * @return A future containing a queue of sub-tasks to be processed.
     */
    @VisibleForTesting
    CompletableFuture<Queue<SubTask>> splitTasks(TopicCleanupTask task) {
        return storage.readIndexes(task.streamId(), 0L, task.markDeletedOffset(), true)
                .thenApplyAsync(indexes -> {
                    var compactionTopic = task.getCompactionTopic();
                    var subTasks = new LinkedList<SubTask>();
                    if (indexes.isEmpty()) {
                        return subTasks;
                    }
                    var parquetFiles = new ArrayList<ParquetFileStat>();
                    long endOffset = 0L;
                    int totalFiles = 0;
                    for (var index : indexes) {
                        if (index.indexType() != EntryIndex.IndexType.COMPACT) {
                            if (log.isDebugEnabled()) {
                                log.debug("Skipping non-compact index {}, stream-id: {}", index.position(),
                                        task.streamId());
                            }
                            continue;
                        }
                        if (index.position().fileType() != Position.FileType.PARQUET) {
                            if (log.isDebugEnabled()) {
                                log.debug("Skipping non-parquet file type index {}, stream-id: {}",
                                        index.position(), task.streamId());
                            }
                            continue;
                        }
                        var file = index.position().file();
                        if (file.size() <= 0) {
                            log.warn(
                                    "Found empty parquet file: {}, stream-id: {}, index: {}. This may be from the "
                                            + "older version of Ursa. Skip this entry index.",
                                    file.location(), task.streamId(), index);
                            continue;
                        }
                        // The format for the `fileFullPath` looks like
                        // "{compactionPrefix}/public/default/topic/__partition=0/part-xxx.parquet"
                        // The `fileFullPath` is used to delete the file from the Cloud Storage.
                        String fileFullPath = getFileFullPath(compactionTopic, file.location());
                        final Map<String, String> tags = Map.of(
                                "totalMessage", String.valueOf(index.header().numberOfMessages())
                        );
                        var stats = new ParquetFileStat(file.location(), fileFullPath, file.size(), null,
                                Collections.emptyMap(), tags);
                        parquetFiles.add(stats);
                        totalFiles++;
                        endOffset = index.header().offset() + index.header().numberOfMessages();
                        if (parquetFiles.size() >= MAX_FILES_PER_TASK) {
                            subTasks.add(new SubTask(task, parquetFiles, endOffset));
                            parquetFiles = new ArrayList<>();
                        }
                    }
                    if (!parquetFiles.isEmpty()) {
                        subTasks.add(new SubTask(task, parquetFiles, endOffset));
                    }
                    log.info("Split task for topic: {}, total sub-tasks: {}, total files: {}",
                            task.topic(), subTasks.size(), totalFiles);
                    return subTasks;
                }, executor);
    }

    private String getFileFullPath(String topic, String location) {
        return String.format("%s/%s/%s", config.getCompactionPrefix(), topic, location);
    }

    /**
     * Initiates the cleanup process for a given topic partition.
     * <p>
     * The process includes:
     * <ul>
     *   <li>Splitting the cleanup into sub-tasks if there are many files.</li>
     *   <li>For each sub-task, deleting files from the Lakehouse and cloud storage, and updating the Oxia index.</li>
     * </ul>
     *
     * @param task The cleanup task containing topic metadata and the mark-deleted offset.
     * @return A CompletableFuture that completes when the cleanup is finished.
     */
    public CompletableFuture<Void> cleanup(TopicCleanupTask task) {
        final CompletableFuture<Void> future;
        // Lock against stop() so the future is registered before stop() snapshots the in-flight
        // set. Otherwise stop() could observe an empty set, shut down the executor, and the chain
        // we are about to start would fail when its Oxia callback tries to dispatch to it.
        synchronized (stateLock) {
            if (isCancel) {
                log.warn("Compacted data cleanup handler is cancelled, skipping cleanup for topic: {}", task.topic());
                return CompletableFuture.completedFuture(null);
            }
            future = splitTasks(task).thenComposeAsync(this::cleanupSubTask, executor);
            inflightCleanups.add(future);
        }
        future.whenComplete((__, ___) -> inflightCleanups.remove(future));
        return future;
    }

    /**
     * Processes each sub-task in the queue sequentially:
     * <ul>
     *   <li>Commits file deletions to the Lakehouse (if applicable).</li>
     *   <li>Deletes files from cloud storage in batch.</li>
     *   <li>Deletes the corresponding Oxia index entries.</li>
     * </ul>
     * Continues until all sub-tasks are processed.
     *
     * @param subTaskQueue The queue of sub-tasks to process.
     * @return A CompletableFuture that completes when all sub-tasks are finished.
     */
    private CompletableFuture<Void> cleanupSubTask(Queue<SubTask> subTaskQueue) {
        if (isCancel) {
            log.info("Compacted data cleanup handler is cancelled, aborting remaining sub-tasks");
            return CompletableFuture.completedFuture(null);
        }
        final var subTask = subTaskQueue.poll();
        if (subTask == null) {
            return CompletableFuture.completedFuture(null);
        }
        final var task = subTask.parentTask();
        var parquetFiles = subTask.parquetFiles;
        if (parquetFiles.isEmpty()) {
            return CompletableFuture.completedFuture(null);
        }

        var lakeHouseCommitters = getLakeHouseCommitters(task);
        for (var committer : lakeHouseCommitters) {
            try {
                committer.delete(parquetFiles);
                log.info("Committed deletion of compacted files for topic: {}, mark deleted offset: {}",
                        task.topic(), task.markDeletedOffset());
            } catch (LakehouseException e) {
                // Duplicate deletion is allowed, it won't throw an exception.
                return CompletableFuture.failedFuture(e);
            }
        }

        List<String> files = parquetFiles.stream()
                .map(ParquetFileStat::getFileFullPath)
                .collect(Collectors.toList());

        // The deleteAsync should be handled by the executor thread pool because there is a sync operation
        // `rateLimiter.acquire()` in it.
        return this.fileStorage.deleteAsync(files)
                .thenCompose(__ -> {
                    log.info("Deleted parquet files for topic: {}, end offset: {}, files count: {}, deleted files: {}",
                            task.topic(), subTask.endOffset(), files.size(), files);
                    final var streamId = task.streamId();
                    return storage.withStreamWriteLease(streamId, ignoredLease ->
                        storage.hardTrimStream(streamId, subTask.endOffset()))
                            .thenAccept(___ -> log.info(
                                    "Deleted stream head for topic: {}, end offset: {}, stream-id: {}",
                                    task.topic(), subTask.endOffset(), streamId));
                })
                .thenComposeAsync(__ -> {
                    if (!subTaskQueue.isEmpty()) {
                        return cleanupSubTask(subTaskQueue);
                    }
                    return CompletableFuture.completedFuture(null);
                }, executor);
    }

    /**
     * No-op start hook. The cleanup handler has no scan loop; cleanup is
     * driven by {@link #cleanup(TopicCleanupTask)} calls from
     * {@link AsyncCompactedDataCleaner}.
     */
    @Override
    public void start() {
        // No background loop to start; cleanup is driven by AsyncCompactedDataCleaner.
    }

    /**
     * Stops the cleanup handler and shuts down the executor service.
     * Waits for ongoing tasks to complete or times out after a short period.
     *
     * <p>The {@link StartStopRunner#stop()} contract is no-throws; if the
     * shutdown wait is interrupted the interrupt status is restored and the
     * method returns.
     */
    @Override
    public void stop() {
        final List<CompletableFuture<Void>> pending;
        synchronized (stateLock) {
            if (isCancel) {
                log.warn("Compacted data cleanup handler is already cancelled");
                return;
            }
            isCancel = true;
            // Snapshot under the lock so any cleanup() racing with us has either already added
            // its future (we will wait for it) or will observe isCancel=true and return early.
            pending = new ArrayList<>(inflightCleanups);
        }

        if (!pending.isEmpty()) {
            log.info("Waiting for {} in-flight cleanup(s) to complete before shutting down executor",
                    pending.size());
            try {
                CompletableFuture.allOf(pending.toArray(new CompletableFuture[0]))
                        .get(INFLIGHT_DRAIN_TIMEOUT_SECS, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                log.warn("Interrupted while waiting for in-flight cleanups; proceeding with shutdown");
                Thread.currentThread().interrupt();
            } catch (TimeoutException e) {
                log.warn("Timed out after {}s waiting for in-flight cleanups; proceeding with shutdown",
                        INFLIGHT_DRAIN_TIMEOUT_SECS);
            } catch (ExecutionException e) {
                // Individual cleanup failures are tolerated during shutdown; the next leader will retry.
                log.info("One or more in-flight cleanups failed during shutdown drain: {}",
                        e.getCause() == null ? e.toString() : e.getCause().toString());
            }
        }

        executor.shutdown();
        try {
            if (!executor.awaitTermination(60, TimeUnit.SECONDS)) {
                executor.shutdownNow();
            }
        } catch (InterruptedException e) {
            log.warn("Compacted data cleanup handler shutdown interrupted", e);
            executor.shutdownNow();
            Thread.currentThread().interrupt();
        }
        log.info("Compacted data cleanup handler stopped.");
    }
}
