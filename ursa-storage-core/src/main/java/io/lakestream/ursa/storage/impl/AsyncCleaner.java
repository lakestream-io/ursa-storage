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
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class AsyncCleaner implements Runnable {

    private static final String deleteMarkerPath  = "ursa-wal-delete-marker";
    private static final String lockPath = "ursa-wal-cleanup-lock";
    private static final byte[] lockValue = new byte[0];

    private final StorageConfig config;
    private final StorageApi storageApi;
    private final FileStorage fileStorage;
    private final ScheduledExecutorService scheduledService;

    public AsyncCleaner(StorageApi storageApi, WalStorage walStorage, StorageConfig config) {
        this.config = config;
        this.storageApi = storageApi;
        if (walStorage != null) {
            this.fileStorage = walStorage.getFileStorage();
        } else {
            this.fileStorage = null;
        }
        this.scheduledService = Executors.newSingleThreadScheduledExecutor(
            new DefaultThreadFactory("ursa-wal-async-cleaner"));
    }

    @VisibleForTesting
    AsyncCleaner() {
        this.config = null;
        this.storageApi = null;
        this.fileStorage = null;
        this.scheduledService = Executors.newSingleThreadScheduledExecutor(
            new DefaultThreadFactory("ursa-storage-async-cleaner"));
    }

    public void startCleanupTask() throws Exception {
        if (this.fileStorage == null) {
            log.info("No backend file storage configured, skip the cleanup task.");
            return;
        }
        String lastDeletedPosition = getLastDeletedPosition().get();
        if (lastDeletedPosition == null) {
            String dummyDatePrefix = IDGeneratorWithDate.getDummyDatePrefix();
            updateLastDeletedPosition(dummyDatePrefix).get();
            log.info("Init dummy date prefix: {} to Oxia. Usually, "
                    + "it means this is a new cluster or the first time start the cleanup service.", dummyDatePrefix);
        }
        int interval = config.getCleanupJobIntervalInHours();
        scheduledService.scheduleAtFixedRate(this, interval, interval, TimeUnit.HOURS);
    }

    @Override
    public void run() {
        cleanup();
    }

    @VisibleForTesting
    void cleanup() {
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
                fileStorage.deleteWithDatePrefixes(getPrefixes(lastDeleted, nextDelete)).get();
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
        storageApi.getStorageOxiaClient().put(lockPath, lockValue,
            Set.of(PutOption.AsEphemeralRecord, PutOption.IfRecordDoesNotExist)).get();
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
        scheduledService.shutdown();
    }
}
