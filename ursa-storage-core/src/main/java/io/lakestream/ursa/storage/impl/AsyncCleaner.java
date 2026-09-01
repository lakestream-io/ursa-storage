/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.storage.impl;

import com.google.common.annotations.VisibleForTesting;
import io.lakestream.api.Position;
import io.lakestream.ursa.storage.FileStorage;
import io.lakestream.ursa.storage.IDGeneratorWithDate;
import io.lakestream.ursa.storage.StorageApi;
import io.lakestream.ursa.storage.WalStorage;
import io.netty.util.concurrent.DefaultThreadFactory;
import io.oxia.client.api.options.PutOption;
import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class AsyncCleaner implements Runnable {

    private static final String deleteMarkerPath  = "ursa-wal-delete-marker";
    private static final String lockPath = "ursa-wal-cleanup-lock";
    private static final byte[] lockValue = new byte[0];
    private static final long INITIALIZATION_RETRY_DELAY_MILLIS = 1_000L;
    private static final long STOP_TIMEOUT_SECONDS = 10L;

    enum LifecycleState {
        NOT_REQUESTED,
        WAITING_FOR_DATA_PLANE,
        INITIALIZATION_SCHEDULED,
        INITIALIZING,
        RUNNING,
        STOPPED
    }

    private final StorageConfig config;
    private final StorageApi storageApi;
    private final WalStorage walStorage;
    private final ScheduledExecutorService scheduledService;
    private final long initializationRetryDelayMillis;
    private FileStorage fileStorage;
    private boolean startRequested;
    private LifecycleState lifecycleState = LifecycleState.NOT_REQUESTED;
    private ScheduledFuture<?> initializationFuture;
    private ScheduledFuture<?> cleanupFuture;

    public AsyncCleaner(StorageApi storageApi, WalStorage walStorage, StorageConfig config) {
        this(storageApi, walStorage, config,
            Executors.newSingleThreadScheduledExecutor(
                new DefaultThreadFactory("ursa-wal-async-cleaner")),
            INITIALIZATION_RETRY_DELAY_MILLIS);
    }

    @VisibleForTesting
    AsyncCleaner(StorageApi storageApi, WalStorage walStorage, StorageConfig config,
                 ScheduledExecutorService scheduledService,
                 long initializationRetryDelayMillis) {
        if (initializationRetryDelayMillis < 0) {
            throw new IllegalArgumentException("initializationRetryDelayMillis must not be negative");
        }
        this.config = config;
        this.storageApi = storageApi;
        this.walStorage = walStorage;
        this.scheduledService = scheduledService;
        this.initializationRetryDelayMillis = initializationRetryDelayMillis;
    }

    @VisibleForTesting
    AsyncCleaner() {
        this.config = null;
        this.storageApi = null;
        this.walStorage = null;
        this.fileStorage = null;
        this.scheduledService = Executors.newSingleThreadScheduledExecutor(
            new DefaultThreadFactory("ursa-storage-async-cleaner"));
        this.initializationRetryDelayMillis = INITIALIZATION_RETRY_DELAY_MILLIS;
    }

    public synchronized void startCleanupTask() {
        if (lifecycleState == LifecycleState.STOPPED) {
            return;
        }
        startRequested = true;
        if (lifecycleState == LifecycleState.INITIALIZATION_SCHEDULED
                || lifecycleState == LifecycleState.INITIALIZING
                || lifecycleState == LifecycleState.RUNNING) {
            return;
        }
        FileStorage initializedFileStorage =
            walStorage == null ? null : walStorage.getFileStorage();
        if (initializedFileStorage == null) {
            lifecycleState = LifecycleState.WAITING_FOR_DATA_PLANE;
            log.info("WAL data plane is not initialized; defer starting the cleanup task.");
            return;
        }
        fileStorage = initializedFileStorage;
        scheduleInitializationLocked(0L);
    }

    synchronized void onDataPlaneAvailable(FileStorage initializedFileStorage) {
        if (initializedFileStorage == null
                || lifecycleState == LifecycleState.STOPPED
                || !startRequested
                || lifecycleState == LifecycleState.INITIALIZATION_SCHEDULED
                || lifecycleState == LifecycleState.INITIALIZING
                || lifecycleState == LifecycleState.RUNNING) {
            return;
        }
        fileStorage = initializedFileStorage;
        scheduleInitializationLocked(0L);
    }

    private void scheduleInitializationLocked(long delayMillis) {
        if (lifecycleState == LifecycleState.STOPPED || fileStorage == null) {
            return;
        }
        try {
            lifecycleState = LifecycleState.INITIALIZATION_SCHEDULED;
            initializationFuture = scheduledService.schedule(
                this::initializeCleanupTask, delayMillis, TimeUnit.MILLISECONDS);
        } catch (RejectedExecutionException failure) {
            if (lifecycleState != LifecycleState.STOPPED) {
                lifecycleState = LifecycleState.WAITING_FOR_DATA_PLANE;
                log.error("Failed to schedule WAL cleanup initialization.", failure);
            }
        }
    }

    private void initializeCleanupTask() {
        synchronized (this) {
            initializationFuture = null;
            if (lifecycleState == LifecycleState.STOPPED
                    || !startRequested
                    || fileStorage == null) {
                return;
            }
            lifecycleState = LifecycleState.INITIALIZING;
        }

        Throwable failure = null;
        try {
            initializeDeleteMarker();
        } catch (Throwable initializationFailure) {
            failure = initializationFailure;
        }

        if (failure != null) {
            handleInitializationFailure(failure);
            return;
        }

        synchronized (this) {
            if (lifecycleState == LifecycleState.STOPPED || fileStorage == null) {
                return;
            }
            try {
                int interval = config.getCleanupJobIntervalInHours();
                cleanupFuture = scheduledService.scheduleAtFixedRate(
                    this, interval, interval, TimeUnit.HOURS);
                lifecycleState = LifecycleState.RUNNING;
                log.info("Started the WAL cleanup task.");
            } catch (Throwable schedulingFailure) {
                handleInitializationFailure(schedulingFailure);
            }
        }
    }

    private void initializeDeleteMarker() throws Exception {
        String lastDeletedPosition = getLastDeletedPosition().get();
        if (lastDeletedPosition != null) {
            return;
        }
        String dummyDatePrefix = IDGeneratorWithDate.getDummyDatePrefix();
        updateLastDeletedPosition(dummyDatePrefix).get();
        log.info("Init dummy date prefix: {} to Oxia. Usually, "
                + "it means this is a new cluster or the first time start the cleanup service.",
            dummyDatePrefix);
    }

    private synchronized void handleInitializationFailure(Throwable failure) {
        if (lifecycleState == LifecycleState.STOPPED) {
            return;
        }
        Throwable cause = unwrapExecutionFailure(failure);
        log.warn("Failed to initialize the WAL cleanup service; retrying in {} ms.",
            initializationRetryDelayMillis, cause);
        scheduleInitializationLocked(initializationRetryDelayMillis);
    }

    private static Throwable unwrapExecutionFailure(Throwable failure) {
        if (failure instanceof ExecutionException && failure.getCause() != null) {
            return failure.getCause();
        }
        return failure;
    }

    @VisibleForTesting
    synchronized LifecycleState lifecycleState() {
        return lifecycleState;
    }

    @VisibleForTesting
    synchronized FileStorage configuredFileStorage() {
        return fileStorage;
    }

    @Override
    public void run() {
        cleanup();
    }

    @VisibleForTesting
    void cleanup() {
        FileStorage initializedFileStorage;
        synchronized (this) {
            if (lifecycleState == LifecycleState.STOPPED) {
                return;
            }
            initializedFileStorage = fileStorage != null
                ? fileStorage : walStorage == null ? null : walStorage.getFileStorage();
        }
        if (initializedFileStorage == null) {
            log.info("No backend file storage configured, skip the cleanup task.");
            return;
        }
        log.info("Start to clean up the WAL log files.");
        // lock() uses IfRecordDoesNotExist, so it throws when another node already holds the lock.
        // Only release the lock if this node actually took it - unlocking unconditionally in the
        // finally block would delete the holder's record and break mutual exclusion.
        boolean lockAcquired = false;
        try {
            lock();
            lockAcquired = true;
            Set<Long> streams = storageApi.listStreams().get();
            log.info("Start to calculate the next delete position from {} streams.", streams.size());
            String nextDelete = null;
            for (Long stream : streams) {
                Position firstUnCompacted = storageApi.getFirstUnCompactedPosition(stream).get();
                if (!Position.NOT_FOUND.equals(firstUnCompacted)) {
                    if (nextDelete == null) {
                        nextDelete = firstUnCompacted.location();
                    } else {
                        LocalDateTime exist = IDGeneratorWithDate.getDatePrefix(nextDelete);
                        LocalDateTime newOne = IDGeneratorWithDate.getDatePrefix(firstUnCompacted.location());
                        if (newOne.isBefore(exist)) {
                            nextDelete = firstUnCompacted.location();
                        }
                    }
                }
            }
            if (nextDelete != null) {
                log.info("The next delete position {} is found from {} streams.", nextDelete, streams.size());
                String lastDeleted = getLastDeletedPosition().get();
                if (lastDeleted == null) {
                    log.error("The clean up task will not continue due to null last deleted position found from Oxia."
                            + "Maybe the metadata has been deleted accidentally!"
                            + "Please check the path {} in Oxia.", deleteMarkerPath);
                    return;
                } else {
                    log.info("Start to perform the WAL log clean up from {} to {}.", lastDeleted, nextDelete);
                }
                initializedFileStorage.deleteWithDatePrefixes(
                    getPrefixes(lastDeleted, nextDelete)).get();
                log.info("Applied the S3 object lifecycle change for prefix between {} and {}.",
                        lastDeleted, nextDelete);
                updateLastDeletedPosition(nextDelete).get();
                log.info("The WAL cleanup is done and the new mark last deleted positiion {} to Oxia.",
                        nextDelete);
            } else {
                log.info("There is no next delete position found from {} streams.", streams.size());
            }

        } catch (Exception e) {
            log.error("Failed to process the WAL cleanup.", e);
        } finally {
            if (lockAcquired) {
                unlock();
            }
        }
    }

    private CompletableFuture<Void> updateLastDeletedPosition(String location) {
        return storageApi.getStorageOxiaClient()
                .put(deleteMarkerPath, location.getBytes(StandardCharsets.UTF_8))
                .thenApply(v -> null);
    }


    CompletableFuture<String> getLastDeletedPosition() {
        return storageApi.getStorageOxiaClient()
            .get(deleteMarkerPath)
            .thenApply(gr -> {
                if (gr != null) {
                    return new String(gr.value(), StandardCharsets.UTF_8);
                } else {
                    return null;
                }
            });

    }

    Set<String> getPrefixes(String lastDeletedLocation, String nextDeletedLocation) {
        LocalDateTime lastDeletedPrefix = IDGeneratorWithDate.getDatePrefix(lastDeletedLocation);
        LocalDateTime nextDeletedPrefix = IDGeneratorWithDate.getDatePrefix(nextDeletedLocation);

        long hours = ChronoUnit.HOURS.between(lastDeletedPrefix, nextDeletedPrefix);
        Set<String> result = new HashSet<>(Math.toIntExact(hours));

        LocalDateTime time = lastDeletedPrefix;
        while (time.isBefore(nextDeletedPrefix)) {
            result.add(IDGeneratorWithDate.getDatePrefix(time));
            time = time.plusHours(1);
        }
        return result;
    }

    void lock() throws Exception {
        CompletableFuture<?> acquisition = storageApi.getStorageOxiaClient().put(
            lockPath, lockValue,
            Set.of(PutOption.AsEphemeralRecord, PutOption.IfRecordDoesNotExist));
        try {
            acquisition.get();
        } catch (InterruptedException failure) {
            superviseLateLockAcquisition(acquisition);
            Thread.currentThread().interrupt();
            throw failure;
        }
    }

    private void superviseLateLockAcquisition(CompletableFuture<?> acquisition) {
        acquisition.whenComplete((ignored, acquireFailure) -> {
            if (acquireFailure != null) {
                return;
            }
            final CompletableFuture<Boolean> release;
            try {
                release = storageApi.getStorageOxiaClient().delete(lockPath);
            } catch (RuntimeException | Error releaseFailure) {
                log.error("Failed to release the WAL cleanup lock after interrupted acquisition.",
                    releaseFailure);
                return;
            }
            release.whenComplete((deleted, releaseFailure) -> {
                if (releaseFailure != null) {
                    log.error("Failed to release the WAL cleanup lock after interrupted "
                        + "acquisition.", releaseFailure);
                }
            });
        });
    }

    void unlock() {
        try {
            storageApi.getStorageOxiaClient().delete(lockPath).get();
        } catch (Exception e) {
            //  todo: figure out more detailed exception to handle them differently.
            //        such as the lock is not exist, or the lock is not belong to the current process.
            log.warn("Failed to unlock the cleanup service.", e);
        }
    }

    public void stop() {
        ScheduledFuture<?> initializationToCancel;
        ScheduledFuture<?> cleanupToCancel;
        synchronized (this) {
            if (lifecycleState == LifecycleState.STOPPED) {
                return;
            }
            lifecycleState = LifecycleState.STOPPED;
            startRequested = false;
            fileStorage = null;
            initializationToCancel = initializationFuture;
            cleanupToCancel = cleanupFuture;
            initializationFuture = null;
            cleanupFuture = null;
        }
        if (initializationToCancel != null) {
            initializationToCancel.cancel(true);
        }
        if (cleanupToCancel != null) {
            cleanupToCancel.cancel(true);
        }
        scheduledService.shutdownNow();
        try {
            if (!scheduledService.awaitTermination(STOP_TIMEOUT_SECONDS, TimeUnit.SECONDS)) {
                log.warn("Timed out waiting for the WAL cleanup service to stop.");
            }
        } catch (InterruptedException failure) {
            Thread.currentThread().interrupt();
            log.warn("Interrupted while waiting for the WAL cleanup service to stop.", failure);
        }
    }
}
