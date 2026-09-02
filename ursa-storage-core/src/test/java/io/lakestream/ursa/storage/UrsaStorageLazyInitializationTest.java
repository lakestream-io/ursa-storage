/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.storage;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTimeout;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import io.lakestream.ursa.storage.StorageApi.StreamIdAllocation;
import io.lakestream.ursa.storage.StorageApi.StreamIdMappingFence;
import io.lakestream.ursa.storage.StorageApi.StreamIdMappingOwner;
import io.lakestream.ursa.storage.impl.StorageConfig;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.util.ReferenceCountUtil;
import io.opentelemetry.api.OpenTelemetry;
import io.oxia.client.api.AsyncOxiaClient;
import io.oxia.client.api.GetResult;
import io.oxia.client.api.PutResult;
import io.oxia.client.api.Version;
import java.io.IOException;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;
import org.mockito.InOrder;

class UrsaStorageLazyInitializationTest {

    @Test
    void metadataFacadeAndCloseDoNotInitializeDataPlane() throws Exception {
        AsyncOxiaClient oxiaClient = mock(AsyncOxiaClient.class);
        UrsaStorage.DataPlaneFactory dataPlaneFactory =
            mock(UrsaStorage.DataPlaneFactory.class);

        UrsaStorage storage = new UrsaStorage(
            config("LOCAL"), OpenTelemetry.noop(), oxiaClient, dataPlaneFactory);

        assertSame(oxiaClient, storage.getDefaultStorageApi().getStorageOxiaClient());
        storage.getDefaultStorageApi().supportsFencedStreamIdMappings();
        storage.getDefaultStorageApi().supportsDurableStreamWriteFencing();
        storage.close();

        verifyNoInteractions(dataPlaneFactory);
    }

    @Test
    void cleanupRequestDoesNotInitializeDataPlane() throws Exception {
        AsyncOxiaClient oxiaClient = mock(AsyncOxiaClient.class);
        UrsaStorage.DataPlaneFactory dataPlaneFactory =
            mock(UrsaStorage.DataPlaneFactory.class);
        UrsaStorage storage = new UrsaStorage(
            config("S3"), OpenTelemetry.noop(), oxiaClient, dataPlaneFactory);

        storage.getDefaultStorageApi().startWALCleanupService();

        verifyNoInteractions(dataPlaneFactory);
        storage.close();
        verifyNoInteractions(dataPlaneFactory);
    }

    @Test
    void requestedCleanupStartsAfterDataPlaneInitialization() throws Exception {
        AsyncOxiaClient oxiaClient = metadataOxiaClient();
        FileStorage fileStorage = mock(FileStorage.class);
        WalStorage walStorage = mock(WalStorage.class);
        when(walStorage.close()).thenReturn(CompletableFuture.completedFuture(null));
        UrsaStorage.DataPlaneFactory dataPlaneFactory =
            mock(UrsaStorage.DataPlaneFactory.class);
        when(dataPlaneFactory.create(any(), any(), any(), any(), any()))
            .thenReturn(new UrsaStorage.DataPlane(fileStorage, walStorage));
        UrsaStorage storage = new UrsaStorage(
            config("S3"), OpenTelemetry.noop(), oxiaClient, dataPlaneFactory);

        storage.getDefaultStorageApi().startWALCleanupService();
        verifyNoInteractions(dataPlaneFactory);
        assertSame(fileStorage, storage.getFileStorage());

        verify(oxiaClient, timeout(10_000)).get("ursa-wal-delete-marker");
        storage.close();
    }

    @Test
    void catalogLifecycleMetadataOperationsDoNotInitializeDataPlane() throws Exception {
        AsyncOxiaClient oxiaClient = metadataOxiaClient();
        UrsaStorage.DataPlaneFactory dataPlaneFactory =
            mock(UrsaStorage.DataPlaneFactory.class);
        UrsaStorage storage = new UrsaStorage(
            config("S3"), OpenTelemetry.noop(), oxiaClient, dataPlaneFactory);
        StorageApi storageApi = storage.getDefaultStorageApi();
        StreamIdMappingOwner owner =
            new StreamIdMappingOwner("incarnation-1", "owner-1", 1L);
        String firstKey = "lakestream-native/public/default/topic/partition-0";
        String secondKey = "lakestream-native/public/default/topic/partition-1";

        // StreamCatalogService requests cleanup during bootstrap. The request must remain
        // metadata-only until a reader or writer actually initializes the WAL.
        storageApi.startWALCleanupService();
        StreamIdAllocation first = storageApi.allocateStreamId(
            firstKey, owner, Optional.empty()).join();
        assertEquals(first.streamId(), storageApi.getStreamIdByKey(firstKey).join());

        storageApi.bindStreamIdMapping(
            secondKey, 777L, owner, Optional.empty()).join();
        assertEquals(777L, storageApi.getStreamIdByKey(secondKey).join());
        assertEquals(Set.of(first.streamId(), 777L), storageApi.listStreams().join());

        StreamIdMappingFence firstFence = storageApi.fenceStreamIdMapping(
            firstKey, first.streamId(), owner).join().orElseThrow();
        assertEquals(first.streamId(), firstFence.streamId());
        storageApi.fenceAndDrainStreamWrites(
            first.streamId(), Duration.ZERO).join();
        storageApi.deleteStream(first.streamId(), Duration.ZERO).join();
        assertEquals(Set.of(777L), storageApi.listStreams().join());

        StreamIdMappingFence secondFence = storageApi.fenceStreamIdMapping(
            secondKey, 777L, owner).join().orElseThrow();
        assertEquals(777L, secondFence.streamId());
        storageApi.deleteStream(777L, Duration.ZERO).join();
        assertTrue(storageApi.listStreams().join().isEmpty());

        verifyNoInteractions(dataPlaneFactory);
        storage.close();
        verifyNoInteractions(dataPlaneFactory);
    }

    @Test
    void explicitFileStorageAccessInitializesDataPlaneExactlyOnce() throws Exception {
        AsyncOxiaClient oxiaClient = mock(AsyncOxiaClient.class);
        FileStorage fileStorage = mock(FileStorage.class);
        WalStorage walStorage = mock(WalStorage.class);
        when(walStorage.close()).thenReturn(CompletableFuture.completedFuture(null));
        UrsaStorage.DataPlaneFactory dataPlaneFactory =
            mock(UrsaStorage.DataPlaneFactory.class);
        when(dataPlaneFactory.create(any(), any(), any(), any(), any()))
            .thenReturn(new UrsaStorage.DataPlane(fileStorage, walStorage));
        UrsaStorage storage = new UrsaStorage(
            config("LOCAL"), OpenTelemetry.noop(), oxiaClient, dataPlaneFactory);

        assertSame(fileStorage, storage.getFileStorage());
        assertSame(fileStorage, storage.getFileStorage());
        assertSame(fileStorage, storage.getDefaultWalStorage().getFileStorage());

        verify(dataPlaneFactory, times(1)).create(any(), any(), any(), any(), any());
        storage.close();
        verify(walStorage).close();
        verify(fileStorage).close();
    }

    @Test
    void concurrentFirstAccessInitializesOneDataPlane() throws Exception {
        AsyncOxiaClient oxiaClient = mock(AsyncOxiaClient.class);
        FileStorage fileStorage = mock(FileStorage.class);
        WalStorage walStorage = mock(WalStorage.class);
        when(walStorage.close()).thenReturn(CompletableFuture.completedFuture(null));
        AtomicInteger createCount = new AtomicInteger();
        CountDownLatch factoryEntered = new CountDownLatch(1);
        CountDownLatch allowFactoryToFinish = new CountDownLatch(1);
        UrsaStorage.DataPlaneFactory dataPlaneFactory =
            (config, provider, client, format, stateManager) -> {
                createCount.incrementAndGet();
                factoryEntered.countDown();
                if (!allowFactoryToFinish.await(10, TimeUnit.SECONDS)) {
                    throw new IllegalStateException("Timed out waiting to finish data-plane creation");
                }
                return new UrsaStorage.DataPlane(fileStorage, walStorage);
            };
        UrsaStorage storage = new UrsaStorage(
            config("LOCAL"), OpenTelemetry.noop(), oxiaClient, dataPlaneFactory);

        CompletableFuture<FileStorage> first =
            CompletableFuture.supplyAsync(storage::getFileStorage);
        factoryEntered.await(10, TimeUnit.SECONDS);
        CompletableFuture<FileStorage> second =
            CompletableFuture.supplyAsync(storage::getFileStorage);
        allowFactoryToFinish.countDown();

        assertSame(fileStorage, first.get(10, TimeUnit.SECONDS));
        assertSame(fileStorage, second.get(10, TimeUnit.SECONDS));
        assertEquals(1, createCount.get());
        storage.close();
    }

    @Test
    void persistentDataPlaneDoesNotStartCleanupWithoutAnExplicitRequest() throws Exception {
        AsyncOxiaClient oxiaClient = mock(AsyncOxiaClient.class);
        FileStorage fileStorage = mock(FileStorage.class);
        WalStorage walStorage = mock(WalStorage.class);
        when(walStorage.close()).thenReturn(CompletableFuture.completedFuture(null));
        UrsaStorage.DataPlaneFactory dataPlaneFactory =
            mock(UrsaStorage.DataPlaneFactory.class);
        when(dataPlaneFactory.create(any(), any(), any(), any(), any()))
            .thenReturn(new UrsaStorage.DataPlane(fileStorage, walStorage));
        UrsaStorage storage = new UrsaStorage(
            config("S3"), OpenTelemetry.noop(), oxiaClient, dataPlaneFactory);

        assertSame(fileStorage, storage.getFileStorage());

        verifyNoInteractions(oxiaClient);
        storage.close();
    }

    @Test
    void failedFirstInitializationCanBeRetried() throws Exception {
        AsyncOxiaClient oxiaClient = mock(AsyncOxiaClient.class);
        FileStorage fileStorage = mock(FileStorage.class);
        WalStorage walStorage = mock(WalStorage.class);
        when(walStorage.close()).thenReturn(CompletableFuture.completedFuture(null));
        UrsaStorage.DataPlaneFactory dataPlaneFactory =
            mock(UrsaStorage.DataPlaneFactory.class);
        when(dataPlaneFactory.create(any(), any(), any(), any(), any()))
            .thenThrow(new IllegalStateException("first initialization failed"))
            .thenReturn(new UrsaStorage.DataPlane(fileStorage, walStorage));
        UrsaStorage storage = new UrsaStorage(
            config("LOCAL"), OpenTelemetry.noop(), oxiaClient, dataPlaneFactory);

        assertThrows(IllegalStateException.class, storage::getFileStorage);
        assertSame(fileStorage, storage.getFileStorage());

        verify(dataPlaneFactory, times(2)).create(any(), any(), any(), any(), any());
        storage.close();
        verify(walStorage).close();
        verify(fileStorage).close();
    }

    @Test
    void failedLazyPutInitializationPreservesCallerBufferOwnership() throws Exception {
        AsyncOxiaClient oxiaClient = mock(AsyncOxiaClient.class);
        IllegalStateException initializationFailure =
            new IllegalStateException("data-plane initialization failed");
        UrsaStorage.DataPlaneFactory dataPlaneFactory =
            mock(UrsaStorage.DataPlaneFactory.class);
        when(dataPlaneFactory.create(any(), any(), any(), any(), any()))
            .thenThrow(initializationFailure);
        UrsaStorage storage = new UrsaStorage(
            config("LOCAL"), OpenTelemetry.noop(), oxiaClient, dataPlaneFactory);
        WalStorage lazyWal = storage.getDefaultWalStorage();
        ByteBuf appendPayload = Unpooled.wrappedBuffer(new byte[] {1});
        ByteBuf offsetPayload = Unpooled.wrappedBuffer(new byte[] {2});
        ByteBuf singlePayload = Unpooled.wrappedBuffer(new byte[] {3});

        ExecutionException appendFailure = assertThrows(ExecutionException.class,
            () -> lazyWal.put(1L, 1, appendPayload).get());
        ExecutionException offsetFailure = assertThrows(ExecutionException.class,
            () -> lazyWal.put(2L, 1, 0L, 1L, offsetPayload).get());
        ExecutionException singleFailure = assertThrows(ExecutionException.class,
            () -> lazyWal.put(3L, singlePayload).get());

        assertSame(initializationFailure, appendFailure.getCause());
        assertSame(initializationFailure, offsetFailure.getCause());
        assertSame(initializationFailure, singleFailure.getCause());
        assertEquals(1, appendPayload.refCnt());
        assertEquals(1, offsetPayload.refCnt());
        assertEquals(1, singlePayload.refCnt());

        assertTrue(appendPayload.release());
        assertTrue(offsetPayload.release());
        assertTrue(singlePayload.release());
        assertEquals(0, appendPayload.refCnt());
        assertEquals(0, offsetPayload.refCnt());
        assertEquals(0, singlePayload.refCnt());
        storage.close();
        verify(dataPlaneFactory, times(3)).create(any(), any(), any(), any(), any());
    }

    @Test
    void lazyWalCloseWaitsForConcurrentInitializationAndPreventsLaterUse()
            throws Exception {
        AsyncOxiaClient oxiaClient = mock(AsyncOxiaClient.class);
        FileStorage fileStorage = mock(FileStorage.class);
        WalStorage walStorage = mock(WalStorage.class);
        AddResult addResult = new AddResult(null, null, false);
        when(walStorage.put(anyLong(), any(ByteBuf.class)))
            .thenReturn(CompletableFuture.completedFuture(addResult));
        when(walStorage.close()).thenReturn(CompletableFuture.completedFuture(null));
        CountDownLatch factoryEntered = new CountDownLatch(1);
        CountDownLatch allowFactoryToFinish = new CountDownLatch(1);
        UrsaStorage.DataPlaneFactory dataPlaneFactory =
            (config, provider, client, format, stateManager) -> {
                factoryEntered.countDown();
                if (!allowFactoryToFinish.await(10, TimeUnit.SECONDS)) {
                    throw new IllegalStateException("Timed out waiting for data-plane creation");
                }
                return new UrsaStorage.DataPlane(fileStorage, walStorage);
            };
        UrsaStorage storage = new UrsaStorage(
            config("LOCAL"), OpenTelemetry.noop(), oxiaClient, dataPlaneFactory);
        WalStorage lazyWal = storage.getDefaultWalStorage();
        ByteBuf firstPayload = Unpooled.wrappedBuffer(new byte[] {1});

        CompletableFuture<AddResult> firstWrite = CompletableFuture
            .supplyAsync(() -> lazyWal.put(1L, firstPayload))
            .thenCompose(future -> future);
        assertTrue(factoryEntered.await(10, TimeUnit.SECONDS));
        CompletableFuture<Void> close = CompletableFuture
            .supplyAsync(lazyWal::close)
            .thenCompose(future -> future);
        assertFalse(close.isDone());

        allowFactoryToFinish.countDown();
        assertSame(addResult, firstWrite.get(10, TimeUnit.SECONDS));
        close.get(10, TimeUnit.SECONDS);
        verify(walStorage).close();
        assertTrue(firstPayload.release());

        ByteBuf rejectedPayload = Unpooled.wrappedBuffer(new byte[] {2});
        ExecutionException rejected = assertThrows(ExecutionException.class,
            () -> lazyWal.put(2L, rejectedPayload).get());
        assertTrue(rejected.getCause() instanceof IllegalStateException);
        assertEquals(1, rejectedPayload.refCnt());
        assertTrue(rejectedPayload.release());
    }

    @Test
    void runtimeCloseDrainsPendingWalOperationBeforeClosingDataPlane() throws Exception {
        AsyncOxiaClient oxiaClient = mock(AsyncOxiaClient.class);
        FileStorage fileStorage = mock(FileStorage.class);
        WalStorage walStorage = mock(WalStorage.class);
        CompletableFuture<AddResult> pendingWrite = new CompletableFuture<>();
        when(walStorage.put(anyLong(), any(ByteBuf.class))).thenReturn(pendingWrite);
        when(walStorage.close()).thenReturn(CompletableFuture.completedFuture(null));
        UrsaStorage.DataPlaneFactory dataPlaneFactory =
            mock(UrsaStorage.DataPlaneFactory.class);
        when(dataPlaneFactory.create(any(), any(), any(), any(), any()))
            .thenReturn(new UrsaStorage.DataPlane(fileStorage, walStorage));
        UrsaStorage storage = new UrsaStorage(
            config("LOCAL"), OpenTelemetry.noop(), oxiaClient, dataPlaneFactory);
        ByteBuf payload = Unpooled.buffer(1).writeByte(1);

        CompletableFuture<AddResult> write = storage.getDefaultWalStorage().put(1L, payload);
        CompletableFuture<Void> close = CompletableFuture.runAsync(() -> {
            try {
                storage.close();
            } catch (Exception failure) {
                throw new RuntimeException(failure);
            }
        });

        assertFalse(close.isDone());
        verify(walStorage, never()).close();
        verify(fileStorage, never()).close();

        AddResult result = new AddResult(null, null, false);
        pendingWrite.complete(result);

        assertSame(result, write.get(10, TimeUnit.SECONDS));
        assertTrue(payload.release());
        close.get(10, TimeUnit.SECONDS);
        verify(walStorage).close();
        verify(fileStorage).close();
    }

    @Test
    void runtimeCloseRetriesWalFailureBeforeClosingOwnedDependencies() throws Exception {
        AsyncOxiaClient oxiaClient = mock(AsyncOxiaClient.class);
        FileStorage fileStorage = mock(FileStorage.class);
        WalStorage walStorage = mock(WalStorage.class);
        IllegalStateException firstWalCloseFailure =
            new IllegalStateException("injected WAL close failure");
        when(walStorage.close())
            .thenReturn(CompletableFuture.failedFuture(firstWalCloseFailure))
            .thenReturn(CompletableFuture.completedFuture(null));
        UrsaStorage.DataPlaneFactory dataPlaneFactory =
            mock(UrsaStorage.DataPlaneFactory.class);
        when(dataPlaneFactory.create(any(), any(), any(), any(), any()))
            .thenReturn(new UrsaStorage.DataPlane(fileStorage, walStorage));
        UrsaStorage storage = new UrsaStorage(
            config("LOCAL"), OpenTelemetry.noop(), oxiaClient, dataPlaneFactory,
            Duration.ofSeconds(1), true);

        assertSame(fileStorage, storage.getFileStorage());

        assertSame(firstWalCloseFailure,
            assertThrows(IllegalStateException.class, storage::close));
        verify(fileStorage, never()).close();
        verify(oxiaClient, never()).close();

        storage.close();

        InOrder closeOrder = inOrder(walStorage, fileStorage, oxiaClient);
        closeOrder.verify(walStorage, times(2)).close();
        closeOrder.verify(fileStorage).close();
        closeOrder.verify(oxiaClient).close();
    }

    @Test
    void runtimeCloseTimesOutWithoutReleasingDependenciesAndCanFinishLater()
            throws Exception {
        AsyncOxiaClient oxiaClient = mock(AsyncOxiaClient.class);
        FileStorage fileStorage = mock(FileStorage.class);
        WalStorage walStorage = mock(WalStorage.class);
        CompletableFuture<AddResult> pendingWrite = new CompletableFuture<>();
        when(walStorage.put(anyLong(), any(ByteBuf.class))).thenReturn(pendingWrite);
        when(walStorage.close()).thenReturn(CompletableFuture.completedFuture(null));
        UrsaStorage.DataPlaneFactory dataPlaneFactory =
            mock(UrsaStorage.DataPlaneFactory.class);
        when(dataPlaneFactory.create(any(), any(), any(), any(), any()))
            .thenReturn(new UrsaStorage.DataPlane(fileStorage, walStorage));
        UrsaStorage storage = new UrsaStorage(
            config("LOCAL"), OpenTelemetry.noop(), oxiaClient, dataPlaneFactory,
            Duration.ofMillis(100), true);
        ByteBuf payload = Unpooled.buffer(1).writeByte(1);
        CompletableFuture<AddResult> write =
            storage.getDefaultWalStorage().put(1L, payload);

        IOException closeTimeout = assertTimeout(Duration.ofSeconds(2),
            () -> assertThrows(IOException.class, storage::close));

        assertTrue(closeTimeout.getCause() instanceof TimeoutException);
        verify(walStorage, never()).close();
        verify(fileStorage, never()).close();
        verify(oxiaClient, never()).close();

        AddResult result = new AddResult(null, null, false);
        pendingWrite.complete(result);
        assertSame(result, write.get(10, TimeUnit.SECONDS));
        assertTrue(payload.release());

        storage.close();

        InOrder closeOrder = inOrder(walStorage, fileStorage, oxiaClient);
        closeOrder.verify(walStorage).close();
        closeOrder.verify(fileStorage).close();
        closeOrder.verify(oxiaClient).close();
    }

    @Test
    void failedDataPlaneInitializationCleanupWaitIsBoundedAndPreservesFileStorage()
            throws Exception {
        FileStorage fileStorage = mock(FileStorage.class);
        WalStorage walStorage = mock(WalStorage.class);
        CompletableFuture<Void> pendingWalClose = new CompletableFuture<>();
        when(walStorage.close()).thenReturn(pendingWalClose);
        IllegalStateException initializationFailure =
            new IllegalStateException("injected initialization failure");

        ExecutorService cleanupExecutor = Executors.newSingleThreadExecutor();
        try {
            assertTimeout(Duration.ofSeconds(2), () ->
                UrsaStorage.closeDataPlaneAfterFailure(
                    fileStorage, walStorage, initializationFailure,
                    Duration.ofMillis(100), cleanupExecutor));

            verify(walStorage).close();
            verify(fileStorage, never()).close();
            assertEquals(1, initializationFailure.getSuppressed().length);
            assertTrue(initializationFailure.getSuppressed()[0] instanceof TimeoutException);

            pendingWalClose.complete(null);

            verify(fileStorage, timeout(5000)).close();
            InOrder closeOrder = inOrder(walStorage, fileStorage);
            closeOrder.verify(walStorage).close();
            closeOrder.verify(fileStorage).close();
        } finally {
            cleanupExecutor.shutdownNow();
        }
    }

    @Test
    void failedInitializationCleanupRetriesSynchronousWalCloseFailure()
            throws Exception {
        FileStorage fileStorage = mock(FileStorage.class);
        WalStorage walStorage = mock(WalStorage.class);
        IllegalStateException closeFailure =
            new IllegalStateException("injected synchronous close failure");
        when(walStorage.close())
            .thenThrow(closeFailure)
            .thenReturn(CompletableFuture.completedFuture(null));
        IllegalStateException initializationFailure =
            new IllegalStateException("injected initialization failure");

        ExecutorService cleanupExecutor = Executors.newSingleThreadExecutor();
        try {
            assertTimeout(Duration.ofSeconds(2), () ->
                UrsaStorage.closeDataPlaneAfterFailure(
                    fileStorage, walStorage, initializationFailure,
                    Duration.ofSeconds(1), cleanupExecutor));

            InOrder closeOrder = inOrder(walStorage, fileStorage);
            closeOrder.verify(walStorage, times(2)).close();
            closeOrder.verify(fileStorage).close();
            assertEquals(1, initializationFailure.getSuppressed().length);
            assertSame(closeFailure, initializationFailure.getSuppressed()[0]);
        } finally {
            cleanupExecutor.shutdownNow();
        }
    }

    @Test
    void failedInitializationCleanupRetriesExceptionalWalCloseFuture()
            throws Exception {
        FileStorage fileStorage = mock(FileStorage.class);
        WalStorage walStorage = mock(WalStorage.class);
        IOException closeFailure = new IOException("injected asynchronous close failure");
        when(walStorage.close())
            .thenReturn(CompletableFuture.failedFuture(closeFailure))
            .thenReturn(CompletableFuture.completedFuture(null));
        IllegalStateException initializationFailure =
            new IllegalStateException("injected initialization failure");

        ExecutorService cleanupExecutor = Executors.newSingleThreadExecutor();
        try {
            assertTimeout(Duration.ofSeconds(2), () ->
                UrsaStorage.closeDataPlaneAfterFailure(
                    fileStorage, walStorage, initializationFailure,
                    Duration.ofSeconds(1), cleanupExecutor));

            InOrder closeOrder = inOrder(walStorage, fileStorage);
            closeOrder.verify(walStorage, times(2)).close();
            closeOrder.verify(fileStorage).close();
            assertEquals(1, initializationFailure.getSuppressed().length);
            assertSame(closeFailure, initializationFailure.getSuppressed()[0]);
        } finally {
            cleanupExecutor.shutdownNow();
        }
    }

    @Test
    void failedDataPlaneCleanupStopsRetryingOnceStorageCloseShutsDownExecutor()
            throws Exception {
        AsyncOxiaClient oxiaClient = mock(AsyncOxiaClient.class);
        UrsaStorage.DataPlaneFactory dataPlaneFactory =
            mock(UrsaStorage.DataPlaneFactory.class);
        UrsaStorage storage = new UrsaStorage(
            config("LOCAL"), OpenTelemetry.noop(), oxiaClient, dataPlaneFactory);

        FileStorage fileStorage = mock(FileStorage.class);
        WalStorage walStorage = mock(WalStorage.class);
        IllegalStateException closeFailure =
            new IllegalStateException("perpetually failing WAL close");
        when(walStorage.close()).thenThrow(closeFailure);
        IllegalStateException initializationFailure =
            new IllegalStateException("injected initialization failure");

        // Start a failed-data-plane cleanup directly against the storage's own executor, using
        // the package-private test hooks (the cleanup never reaches this executor through
        // dataPlaneFactory, since that is mocked). The WAL close fails on every attempt, so a
        // retry is scheduled via the executor before the storage is closed.
        CompletableFuture<Void> cleanupCompletion =
            UrsaStorage.startFailedDataPlaneCleanupForTesting(
                fileStorage, walStorage, initializationFailure,
                storage.failedDataPlaneCloseExecutorForTesting());

        verify(walStorage, timeout(5000).atLeastOnce()).close();
        assertFalse(cleanupCompletion.isDone());

        storage.close();

        // Before the fix this future never completed: the scheduled retry kept hitting
        // RejectedExecutionException from the now-shut-down executor and rescheduling itself
        // forever. Bounding the wait turns "would hang forever" into a clear test failure.
        ExecutionException rejected = assertThrows(ExecutionException.class,
            () -> cleanupCompletion.get(5, TimeUnit.SECONDS));
        assertInstanceOf(RejectedExecutionException.class, rejected.getCause());
        // Nothing else will close the data plane once the retry chain gives up, so the give-up
        // branch closes both storages itself before reporting the rejection.
        verify(fileStorage).close();
    }

    @Test
    void closingLazyWalBeforeInitializationRejectsLaterFileStorageAccess()
            throws Exception {
        AsyncOxiaClient oxiaClient = mock(AsyncOxiaClient.class);
        UrsaStorage.DataPlaneFactory dataPlaneFactory =
            mock(UrsaStorage.DataPlaneFactory.class);
        UrsaStorage storage = new UrsaStorage(
            config("LOCAL"), OpenTelemetry.noop(), oxiaClient, dataPlaneFactory);

        storage.getDefaultWalStorage().close().join();

        assertThrows(IllegalStateException.class, storage::getFileStorage);
        verifyNoInteractions(dataPlaneFactory);
        storage.close();
        verifyNoInteractions(dataPlaneFactory);
    }

    @Test
    void failedRequestedCleanupDoesNotBlockDataPlaneOrAppendAndRetries() throws Exception {
        AsyncOxiaClient oxiaClient = metadataOxiaClient();
        CompletableFuture<GetResult> firstMarkerRead = new CompletableFuture<>();
        CountDownLatch firstCleanerEntered = new CountDownLatch(1);
        CountDownLatch retryMarkerWritten = new CountDownLatch(1);
        AtomicInteger markerReadCount = new AtomicInteger();
        when(oxiaClient.get("ursa-wal-delete-marker")).thenAnswer(invocation -> {
            if (markerReadCount.incrementAndGet() == 1) {
                firstCleanerEntered.countDown();
                return firstMarkerRead;
            }
            return CompletableFuture.completedFuture(null);
        });
        when(oxiaClient.put(eq("ursa-wal-delete-marker"), any(byte[].class)))
            .thenAnswer(invocation -> {
                retryMarkerWritten.countDown();
                return CompletableFuture.completedFuture(mock(PutResult.class));
            });

        FileStorage fileStorage = mock(FileStorage.class);
        WalStorage walStorage = mock(WalStorage.class);
        when(walStorage.close()).thenReturn(CompletableFuture.completedFuture(null));
        AddResult addResult = new AddResult(null, null, false);
        when(walStorage.put(anyLong(), any(ByteBuf.class))).thenAnswer(invocation -> {
            return CompletableFuture.completedFuture(addResult);
        });
        UrsaStorage.DataPlaneFactory dataPlaneFactory =
            mock(UrsaStorage.DataPlaneFactory.class);
        when(dataPlaneFactory.create(any(), any(), any(), any(), any()))
            .thenReturn(new UrsaStorage.DataPlane(fileStorage, walStorage));
        UrsaStorage storage = new UrsaStorage(
            config("S3"), OpenTelemetry.noop(), oxiaClient, dataPlaneFactory);

        storage.getDefaultStorageApi().startWALCleanupService();
        CompletableFuture<FileStorage> dataPlaneAccess =
            CompletableFuture.supplyAsync(storage::getFileStorage);
        assertSame(fileStorage, dataPlaneAccess.get(10, TimeUnit.SECONDS));
        assertTrue(firstCleanerEntered.await(10, TimeUnit.SECONDS));

        ByteBuf entry = Unpooled.wrappedBuffer(new byte[] {1, 2, 3});
        assertSame(addResult,
            storage.getDefaultWalStorage().put(42L, entry).get(10, TimeUnit.SECONDS));
        assertEquals(1, entry.refCnt());
        ReferenceCountUtil.safeRelease(entry);
        assertEquals(0, entry.refCnt());

        firstMarkerRead.completeExceptionally(
            new IllegalStateException("injected cleanup initialization failure"));
        assertTrue(retryMarkerWritten.await(10, TimeUnit.SECONDS));
        assertEquals(2, markerReadCount.get());
        verify(dataPlaneFactory, times(1)).create(any(), any(), any(), any(), any());

        storage.close();
        verify(walStorage).close();
        verify(fileStorage).close();
    }

    @Test
    void closeCancelsBlockedCleanupInitializationAndPreventsRestart() throws Exception {
        AsyncOxiaClient oxiaClient = metadataOxiaClient();
        CompletableFuture<GetResult> markerRead = new CompletableFuture<>();
        CountDownLatch cleanerEntered = new CountDownLatch(1);
        when(oxiaClient.get("ursa-wal-delete-marker")).thenAnswer(invocation -> {
            cleanerEntered.countDown();
            return markerRead;
        });
        FileStorage fileStorage = mock(FileStorage.class);
        WalStorage walStorage = mock(WalStorage.class);
        when(walStorage.close()).thenReturn(CompletableFuture.completedFuture(null));
        UrsaStorage.DataPlaneFactory dataPlaneFactory =
            mock(UrsaStorage.DataPlaneFactory.class);
        when(dataPlaneFactory.create(any(), any(), any(), any(), any()))
            .thenReturn(new UrsaStorage.DataPlane(fileStorage, walStorage));
        UrsaStorage storage = new UrsaStorage(
            config("S3"), OpenTelemetry.noop(), oxiaClient, dataPlaneFactory);

        storage.getDefaultStorageApi().startWALCleanupService();
        assertSame(fileStorage, storage.getFileStorage());
        assertTrue(cleanerEntered.await(10, TimeUnit.SECONDS));

        storage.close();
        storage.getDefaultStorageApi().startWALCleanupService();
        markerRead.complete(null);

        verify(oxiaClient, times(1)).get("ursa-wal-delete-marker");
        verify(oxiaClient, never()).put(eq("ursa-wal-delete-marker"), any(byte[].class));
        verify(walStorage).close();
        verify(fileStorage).close();
    }

    @Test
    void closingUninitializedWalDoesNotInitializeDataPlane() throws Exception {
        AsyncOxiaClient oxiaClient = mock(AsyncOxiaClient.class);
        UrsaStorage.DataPlaneFactory dataPlaneFactory =
            mock(UrsaStorage.DataPlaneFactory.class);
        UrsaStorage storage = new UrsaStorage(
            config("LOCAL"), OpenTelemetry.noop(), oxiaClient, dataPlaneFactory);

        storage.getDefaultWalStorage().close().get(10, TimeUnit.SECONDS);

        verify(dataPlaneFactory, never()).create(any(), any(), any(), any(), any());
        storage.close();
        verifyNoInteractions(dataPlaneFactory);
    }

    private static StorageConfig config(String backendStorageType) {
        Properties properties = new Properties();
        properties.setProperty("backendStorageType", backendStorageType);
        return StorageConfig.fromProperties(properties);
    }

    @SuppressWarnings("unchecked")
    private static AsyncOxiaClient metadataOxiaClient() {
        AsyncOxiaClient client = mock(AsyncOxiaClient.class);
        Map<String, StoredValue> values = new HashMap<>();
        AtomicLong nextVersion = new AtomicLong(100L);

        when(client.get(anyString())).thenAnswer(invocation ->
            CompletableFuture.completedFuture(getResult(
                invocation.getArgument(0, String.class), values)));
        when(client.get(anyString(), any(Set.class))).thenAnswer(invocation ->
            CompletableFuture.completedFuture(getResult(
                invocation.getArgument(0, String.class), values)));
        when(client.put(anyString(), any(byte[].class))).thenAnswer(invocation ->
            putResult(invocation.getArgument(0, String.class),
                invocation.getArgument(1, byte[].class), values, nextVersion));
        when(client.put(anyString(), any(byte[].class), any(Set.class))).thenAnswer(invocation ->
            putResult(invocation.getArgument(0, String.class),
                invocation.getArgument(1, byte[].class), values, nextVersion));
        when(client.delete(anyString())).thenAnswer(invocation ->
            CompletableFuture.completedFuture(
                values.remove(invocation.getArgument(0, String.class)) != null));
        when(client.delete(anyString(), any(Set.class))).thenAnswer(invocation ->
            CompletableFuture.completedFuture(
                values.remove(invocation.getArgument(0, String.class)) != null));
        when(client.deleteRange(anyString(), anyString(), any(Set.class)))
            .thenReturn(CompletableFuture.completedFuture(null));
        when(client.list(anyString(), anyString())).thenAnswer(invocation -> {
            String start = invocation.getArgument(0, String.class);
            List<String> keys = values.keySet().stream()
                .filter(key -> key.startsWith(start))
                .sorted()
                .toList();
            return CompletableFuture.completedFuture(keys);
        });
        when(client.list(anyString(), anyString(), any(Set.class)))
            .thenReturn(CompletableFuture.completedFuture(List.of()));
        return client;
    }

    private static CompletableFuture<PutResult> putResult(
            String key, byte[] value, Map<String, StoredValue> values,
            AtomicLong nextVersion) {
        long versionId = nextVersion.incrementAndGet();
        Version version = version(versionId);
        values.put(key, new StoredValue(value.clone(), version));
        return CompletableFuture.completedFuture(new PutResult(key, version));
    }

    private static GetResult getResult(
            String key, Map<String, StoredValue> values) {
        StoredValue value = values.get(key);
        return value == null ? null
            : new GetResult(key, value.value().clone(), value.version());
    }

    private static Version version(long versionId) {
        return new Version(
            versionId, 0, 0, 0, Optional.empty(), Optional.empty());
    }

    private record StoredValue(byte[] value, Version version) {
    }
}
