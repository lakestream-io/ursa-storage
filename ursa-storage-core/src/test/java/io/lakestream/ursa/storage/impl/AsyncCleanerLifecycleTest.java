/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.storage.impl;

import static io.lakestream.ursa.storage.impl.AsyncCleaner.LifecycleState.INITIALIZATION_SCHEDULED;
import static io.lakestream.ursa.storage.impl.AsyncCleaner.LifecycleState.RUNNING;
import static io.lakestream.ursa.storage.impl.AsyncCleaner.LifecycleState.STOPPED;
import static io.lakestream.ursa.storage.impl.AsyncCleaner.LifecycleState.WAITING_FOR_DATA_PLANE;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.lakestream.ursa.storage.FileStorage;
import io.lakestream.ursa.storage.StorageApi;
import io.lakestream.ursa.storage.WalStorage;
import io.oxia.client.api.AsyncOxiaClient;
import io.oxia.client.api.GetResult;
import io.oxia.client.api.PutResult;
import io.oxia.client.api.options.PutOption;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class AsyncCleanerLifecycleTest {

    @Test
    void requestBeforeDataPlaneDefersAndFailedInitializationRetries() throws Exception {
        StorageApi storageApi = mock(StorageApi.class);
        AsyncOxiaClient oxiaClient = mock(AsyncOxiaClient.class);
        when(storageApi.getStorageOxiaClient()).thenReturn(oxiaClient);
        WalStorage walStorage = mock(WalStorage.class);
        when(walStorage.getFileStorage()).thenReturn(null);
        FileStorage fileStorage = mock(FileStorage.class);
        CountDownLatch retryEntered = new CountDownLatch(1);
        AtomicInteger markerReads = new AtomicInteger();
        when(oxiaClient.get("ursa-wal-delete-marker")).thenAnswer(invocation -> {
            if (markerReads.incrementAndGet() == 1) {
                return CompletableFuture.failedFuture(
                    new IllegalStateException("injected marker read failure"));
            }
            retryEntered.countDown();
            return CompletableFuture.completedFuture(null);
        });
        when(oxiaClient.put(anyString(), any(byte[].class)))
            .thenReturn(CompletableFuture.completedFuture(mock(PutResult.class)));
        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
        AsyncCleaner cleaner = new AsyncCleaner(
            storageApi, walStorage, config(), scheduler, 0L);

        try {
            cleaner.startCleanupTask();
            assertEquals(WAITING_FOR_DATA_PLANE, cleaner.lifecycleState());
            verify(oxiaClient, never()).get(anyString());

            cleaner.onDataPlaneAvailable(fileStorage);
            assertTrue(retryEntered.await(10, TimeUnit.SECONDS));
            await().atMost(10, TimeUnit.SECONDS)
                .until(() -> cleaner.lifecycleState() == RUNNING);

            assertEquals(2, markerReads.get());
            assertSame(fileStorage, cleaner.configuredFileStorage());
        } finally {
            cleaner.stop();
        }
    }

    @Test
    void stopCancelsInitializationAndRejectsFutureStartsAndCandidates() throws Exception {
        StorageApi storageApi = mock(StorageApi.class);
        AsyncOxiaClient oxiaClient = mock(AsyncOxiaClient.class);
        when(storageApi.getStorageOxiaClient()).thenReturn(oxiaClient);
        WalStorage walStorage = mock(WalStorage.class);
        FileStorage fileStorage = mock(FileStorage.class);
        when(walStorage.getFileStorage()).thenReturn(fileStorage);
        CompletableFuture<GetResult> markerRead = new CompletableFuture<>();
        CountDownLatch initializationEntered = new CountDownLatch(1);
        when(oxiaClient.get("ursa-wal-delete-marker")).thenAnswer(invocation -> {
            initializationEntered.countDown();
            return markerRead;
        });
        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
        AsyncCleaner cleaner = new AsyncCleaner(
            storageApi, walStorage, config(), scheduler, 0L);

        cleaner.startCleanupTask();
        assertTrue(initializationEntered.await(10, TimeUnit.SECONDS));
        cleaner.stop();

        assertEquals(STOPPED, cleaner.lifecycleState());
        assertNull(cleaner.configuredFileStorage());
        assertTrue(scheduler.isTerminated());
        cleaner.startCleanupTask();
        cleaner.onDataPlaneAvailable(mock(FileStorage.class));
        markerRead.complete(null);

        verify(oxiaClient, times(1)).get("ursa-wal-delete-marker");
        verify(oxiaClient, never()).put(anyString(), any(byte[].class));
    }

    @Test
    void stopCancelsRetryScheduledByFailedInitialization() {
        StorageApi storageApi = mock(StorageApi.class);
        AsyncOxiaClient oxiaClient = mock(AsyncOxiaClient.class);
        when(storageApi.getStorageOxiaClient()).thenReturn(oxiaClient);
        WalStorage walStorage = mock(WalStorage.class);
        FileStorage fileStorage = mock(FileStorage.class);
        when(walStorage.getFileStorage()).thenReturn(fileStorage);
        AtomicInteger markerReads = new AtomicInteger();
        when(oxiaClient.get("ursa-wal-delete-marker")).thenAnswer(invocation -> {
            markerReads.incrementAndGet();
            return CompletableFuture.failedFuture(
                new IllegalStateException("injected marker read failure"));
        });
        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
        AsyncCleaner cleaner = new AsyncCleaner(
            storageApi, walStorage, config(), scheduler, TimeUnit.MINUTES.toMillis(1));

        cleaner.startCleanupTask();
        await().atMost(10, TimeUnit.SECONDS).until(() ->
            markerReads.get() == 1
                && cleaner.lifecycleState() == INITIALIZATION_SCHEDULED);
        cleaner.stop();

        assertEquals(STOPPED, cleaner.lifecycleState());
        assertNull(cleaner.configuredFileStorage());
        assertTrue(scheduler.isTerminated());
        assertEquals(1, markerReads.get());
        verify(oxiaClient, times(1)).get("ursa-wal-delete-marker");
    }

    @Test
    void stopDuringLockAcquisitionReleasesALockThatCompletesLate() throws Exception {
        StorageApi storageApi = mock(StorageApi.class);
        AsyncOxiaClient oxiaClient = mock(AsyncOxiaClient.class);
        when(storageApi.getStorageOxiaClient()).thenReturn(oxiaClient);
        WalStorage walStorage = mock(WalStorage.class);
        when(walStorage.getFileStorage()).thenReturn(mock(FileStorage.class));
        CompletableFuture<PutResult> lockAcquisition = new CompletableFuture<>();
        CountDownLatch acquisitionEntered = new CountDownLatch(1);
        when(oxiaClient.put(eq("ursa-wal-cleanup-lock"), any(byte[].class), eq(Set.of(
                PutOption.AsEphemeralRecord, PutOption.IfRecordDoesNotExist))))
            .thenAnswer(invocation -> {
                acquisitionEntered.countDown();
                return lockAcquisition;
            });
        when(oxiaClient.delete("ursa-wal-cleanup-lock"))
            .thenReturn(CompletableFuture.completedFuture(true));
        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
        AsyncCleaner cleaner = new AsyncCleaner(
            storageApi, walStorage, config(), scheduler, 0L);

        scheduler.submit(cleaner);
        assertTrue(acquisitionEntered.await(10, TimeUnit.SECONDS));
        cleaner.stop();
        lockAcquisition.complete(mock(PutResult.class));

        verify(oxiaClient, times(1)).delete("ursa-wal-cleanup-lock");
        assertTrue(scheduler.isTerminated());
    }

    private static StorageConfig config() {
        return StorageConfig.builder()
            .cleanupJobIntervalInHours(1)
            .build();
    }
}
