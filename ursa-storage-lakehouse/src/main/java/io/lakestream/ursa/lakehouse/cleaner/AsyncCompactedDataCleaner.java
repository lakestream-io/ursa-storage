/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.lakehouse.cleaner;

import com.google.common.base.Strings;
import io.lakestream.api.EntryIndex;
import io.lakestream.ursa.storage.StorageApi;
import io.lakestream.ursa.storage.impl.StorageConfig;
import io.lakestream.ursa.storage.impl.compaction.StartStopRunner;
import io.netty.util.concurrent.DefaultThreadFactory;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;

/**
 * Periodically scans all topics and schedules asynchronous cleanup tasks for compacted data (parquet files)
 * that are eligible for deletion according to each topic's mark-deleted offset.
 * <p>
 * This cleaner is responsible for orchestrating the background deletion of compacted data in Ursa's Lakehouse engine.
 * It works in conjunction with {@link CompactedDataCleanupHandler}, which performs the actual deletion logic.
 * <p>
 * The cleaner runs at a configurable interval, discovers all topics with eligible data for cleanup, and submits
 * cleanup tasks to {@link CompactedDataCleanupHandler}, ensuring that the number of concurrent cleanup operations is
 * bounded.
 * <p>
 * Key responsibilities:
 * <ul>
 *   <li>Periodically trigger cleanup based on the configured interval.</li>
 *   <li>For each topic, determine if there is compacted data (parquet files) before the mark-deleted offset that can
 *   be deleted.</li>
 *   <li>Submit eligible cleanup tasks to the handler, respecting concurrency limits.</li>
 *   <li>Log progress and handle errors gracefully, ensuring the system remains robust.</li>
 * </ul>
 * <p>
 * This class is a core part of the LIP-145 retention and cleanup design, ensuring that cloud storage is efficiently
 * reclaimed and that retention policies are enforced at the storage layer.
 *
 * @see CompactedDataCleanupHandler
 */
@Slf4j
public class AsyncCompactedDataCleaner implements Runnable, StartStopRunner {
    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor(
            new DefaultThreadFactory("ursa-compact-async-cleaner"));
    private final StorageApi storage;
    private final CompactedDataCleanupHandler cleanupHandler;
    private final Semaphore semaphore;
    private final long intervalSec;

    private volatile boolean isCancel = false;

    /**
     * Constructs a new AsyncCompactedDataCleaner.
     *
     * @param config          The storage configuration containing cleanup intervals and concurrency settings.
     * @param storage         The storage API for accessing topic metadata and mark-deleted offsets.
     * @param cleanupHandler  The handler responsible for executing the actual cleanup logic for each topic.
     */
    public AsyncCompactedDataCleaner(StorageConfig config, StorageApi storage,
                                     CompactedDataCleanupHandler cleanupHandler) {
        this.storage = storage;
        this.cleanupHandler = cleanupHandler;
        this.intervalSec = config.getCompactedDataCleanupJobIntervalInSecs();
        semaphore = new Semaphore(config.getCompactedDataCleanupPendingTasks());
        log.info("Starting AsyncCompactedDataCleaner with interval: {} seconds", intervalSec);
        scheduler.scheduleWithFixedDelay(this, intervalSec, intervalSec, TimeUnit.SECONDS);
    }

    private Queue<TopicCleanupTask> getAllTopicCleanupTasks()
            throws ExecutionException, InterruptedException, TimeoutException {
        final var topicTaskQueue = new ConcurrentLinkedQueue<TopicCleanupTask>();
        final var markDeletedOffsetMap = storage.getMarkDeletedOffsetMap().get();
        if (markDeletedOffsetMap.isEmpty()) {
            return topicTaskQueue;
        }
        final var streamToTopicMap = storage.listStreamsWithProperties().get();
        var futures = streamToTopicMap.entrySet().stream()
                .map(entry -> {
                    var streamId = entry.getKey();
                    if (Strings.isNullOrEmpty(entry.getValue().key())) {
                        return CompletableFuture.completedFuture(null);
                    }
                    var topic = entry.getValue().key();
                    var markDeletedOffset = markDeletedOffsetMap.get(entry.getKey().toString());
                    if (markDeletedOffset == null) {
                        return CompletableFuture.completedFuture(null); // Skip topics with markDeletedOffset
                    }
                    return storage.getFirstEntry(entry.getKey(), true)
                            .thenAccept(entryIndex -> {
                                if (EntryIndex.NOT_FOUND.equals(entryIndex)) {
                                    // No entries found, skip this topic
                                    return;
                                }
                                if (entryIndex.header().offset() + entryIndex.header().numberOfMessages()
                                        > markDeletedOffset) {
                                    return;
                                }
                                topicTaskQueue.add(new TopicCleanupTask(topic, streamId, markDeletedOffset));
                            }).exceptionally(e -> {
                                log.warn("Failed to get first entry for topic: {}. SKip this topic", topic, e);
                                return null;
                            });
                }).collect(Collectors.toSet());
        CompletableFuture.allOf(futures.toArray(CompletableFuture[]::new)).get(30, TimeUnit.MINUTES);
        return topicTaskQueue;
    }

    /**
     * The main execution logic for the cleaner.
     * <p>
     * Scans all topics, determines which have eligible compacted data for cleanup, and submits cleanup tasks.
     * Handles concurrency, error logging, and concurrent task management using a semaphore.
     */
    @Override
    public void run() {
        if (isCancel) {
            log.warn("Async compacted data cleaner is cancelled, skipping this run");
            return;
        }
        try {
            log.info("Start checking topics for async compacted data cleanup. interval: {} seconds",
                    intervalSec);
            final var topicTaskQueue = getAllTopicCleanupTasks();
            if (topicTaskQueue.isEmpty()) {
                log.info("No topics found for async compacted data cleanup");
                return;
            }
            log.info("Found some topics for async compacted data cleanup: topic-count: {}", topicTaskQueue.size());
            for (var task : topicTaskQueue) {
                if (log.isDebugEnabled()) {
                    log.debug("Prepare topic cleanup task for topic: {}, mark deleted offset: {}",
                            task.topic(), task.markDeletedOffset());
                }

                semaphore.acquire();

                if (log.isDebugEnabled()) {
                    log.debug("Start to clean up topic: {}, mark deleted offset: {}",
                            task.topic(), task.markDeletedOffset());
                }

                cleanupHandler.cleanup(task)
                        .whenComplete((__, throwable) -> {
                            semaphore.release();
                            if (throwable != null) {
                                var reason = unwrapCompletionException(throwable);
                                log.warn("Failed to clean up topic: {}, mark deleted offset: {}",
                                        task.topic(), task.markDeletedOffset(), reason);
                            } else {
                                log.info("Successfully cleaned up topic: {}, mark deleted offset: {}",
                                        task.topic(), task.markDeletedOffset());
                            }
                        });
            }
            log.info("Finished cleaning up topics");
        } catch (InterruptedException e) {
            log.warn("Async compacted data cleaner interrupted", e);
            Thread.currentThread().interrupt();
        } catch (Throwable e) {
            log.error("Error during checking topics for async compacted data cleaner", e);
        }
    }

    private static Throwable unwrapCompletionException(Throwable error) {
        Throwable current = error;
        while ((current instanceof CompletionException || current instanceof ExecutionException)
                && current.getCause() != null) {
            current = current.getCause();
        }
        return current;
    }

    /**
     * No-op start hook. The cleaner self-schedules in its constructor; this
     * method exists to satisfy the {@link StartStopRunner} lifecycle.
     */
    @Override
    public void start() {
        // The scheduler is already kicked off in the constructor.
    }

    /**
     * Stops the background cleaner and shuts down the scheduler.
     * Waits for ongoing tasks to complete or times out after a short period.
     *
     * <p>The {@link StartStopRunner#stop()} contract is no-throws; if a thread
     * is interrupted while awaiting shutdown the interrupt status is restored
     * and the method returns.
     */
    @Override
    public void stop() {
        if (isCancel) {
            log.warn("Async compacted data cleaner is already cancelled");
            return;
        }
        isCancel = true;
        scheduler.shutdown();
        try {
            if (!scheduler.awaitTermination(10, TimeUnit.SECONDS)) {
                scheduler.shutdownNow();
            }
        } catch (InterruptedException e) {
            log.warn("Async compacted data cleaner shutdown interrupted", e);
            scheduler.shutdownNow();
            Thread.currentThread().interrupt();
        }
        cleanupHandler.stop();
        log.info("Async compacted data cleaner stopped");
    }
}
