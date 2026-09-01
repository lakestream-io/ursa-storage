/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.storage.impl;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.startsWith;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.lakestream.api.EntryHeader;
import io.lakestream.api.LogState;
import io.lakestream.api.exception.LogFencedException;
import io.lakestream.ursa.metrics.InstrumentProvider;
import io.lakestream.ursa.storage.AddResult;
import io.lakestream.ursa.storage.StorageApi.StreamWriteLease;
import io.lakestream.ursa.storage.StorageApi.StreamWriteLeaseDrainTimeoutException;
import io.lakestream.ursa.storage.WalStorage;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.oxia.client.api.AsyncOxiaClient;
import io.oxia.client.api.GetResult;
import io.oxia.client.api.PutResult;
import io.oxia.client.api.exceptions.KeyAlreadyExistsException;
import io.oxia.client.api.options.DeleteOption;
import io.oxia.client.api.options.DeleteRangeOption;
import io.oxia.client.api.options.GetOption;
import io.oxia.client.api.options.ListOption;
import io.oxia.client.api.options.PutOption;
import java.time.Duration;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.InOrder;

class PersistStorageApiWriteLeaseTest {

    private static final long STREAM_ID = 42L;

    private AsyncOxiaClient oxiaClient;
    private WalStorage walStorage;
    private PersistStorageApi storageApi;
    private StreamStateManagerImpl stateManager;

    @BeforeEach
    void setUp() {
        oxiaClient = mock(AsyncOxiaClient.class);
        walStorage = mock(WalStorage.class);
        stateManager = new StreamStateManagerImpl();
        StorageConfig config = StorageConfig.builder().backendStorageType("local").build();
        storageApi = new PersistStorageApi(
            config,
            oxiaClient,
            walStorage,
            InstrumentProvider.NOOP,
            new StorageFormat(config),
            stateManager);
    }

    @Test
    void acquiredLeaseBlocksPhysicalDeletion() {
        String leasePrefix = PersistStorageApi.streamWriteLeasePrefix(STREAM_ID);
        when(oxiaClient.put(startsWith(leasePrefix), any(byte[].class), anyPutOptions()))
            .thenReturn(CompletableFuture.completedFuture(mock(PutResult.class)));
        when(oxiaClient.get(eq(PersistStorageApi.streamWriteFenceKey(STREAM_ID)), anyGetOptions()))
            .thenReturn(CompletableFuture.completedFuture(null));
        StreamWriteLease lease = storageApi.acquireStreamWriteLease(STREAM_ID).join();
        InOrder acquisitionOrder = inOrder(oxiaClient);
        acquisitionOrder.verify(oxiaClient).put(
            startsWith(leasePrefix), any(byte[].class), leaseCreateOptions());
        acquisitionOrder.verify(oxiaClient).get(
            eq(PersistStorageApi.streamWriteFenceKey(STREAM_ID)), anyGetOptions());

        when(oxiaClient.put(
                eq(PersistStorageApi.streamWriteFenceKey(STREAM_ID)),
                any(byte[].class), anyPutOptions()))
            .thenReturn(CompletableFuture.completedFuture(mock(PutResult.class)));
        when(oxiaClient.list(eq(leasePrefix), eq(leasePrefix + "\uffff"), anyListOptions()))
            .thenReturn(CompletableFuture.completedFuture(List.of(leasePrefix + "open-handle")));

        CompletionException failure = assertThrows(
            CompletionException.class,
            () -> storageApi.deleteStream(STREAM_ID, Duration.ZERO).join());

        StreamWriteLeaseDrainTimeoutException timeout =
            (StreamWriteLeaseDrainTimeoutException) failure.getCause();
        assertEquals(STREAM_ID, timeout.streamId());
        assertEquals(1, timeout.activeLeaseCount());
        assertEquals(LogState.FENCED, stateManager.getState(STREAM_ID));
        verify(oxiaClient, never()).deleteRange(anyString(), anyString(), anyDeleteRangeOptions());
        verify(oxiaClient, never()).delete(eq(StorageFormat.STREAM_REGISTER_PATH + "/" + STREAM_ID));

        when(oxiaClient.delete(startsWith(leasePrefix), anyDeleteOptions()))
            .thenReturn(CompletableFuture.completedFuture(true));
        lease.closeAsync().join();
    }

    @Test
    void fenceWinningBeforeLeaseReadRejectsAndCleansLease() {
        String leasePrefix = PersistStorageApi.streamWriteLeasePrefix(STREAM_ID);
        CompletableFuture<PutResult> leaseCreated = new CompletableFuture<>();
        when(oxiaClient.put(startsWith(leasePrefix), any(byte[].class), anyPutOptions()))
            .thenReturn(leaseCreated);
        when(oxiaClient.put(
                eq(PersistStorageApi.streamWriteFenceKey(STREAM_ID)),
                any(byte[].class), anyPutOptions()))
            .thenReturn(CompletableFuture.completedFuture(mock(PutResult.class)));
        when(oxiaClient.get(eq(PersistStorageApi.streamWriteFenceKey(STREAM_ID)), anyGetOptions()))
            .thenReturn(CompletableFuture.completedFuture(mock(GetResult.class)));
        when(oxiaClient.delete(startsWith(leasePrefix), anyDeleteOptions()))
            .thenReturn(
                CompletableFuture.failedFuture(new IllegalStateException("temporary failure")),
                CompletableFuture.completedFuture(true));

        CompletableFuture<StreamWriteLease> acquire =
            storageApi.acquireStreamWriteLease(STREAM_ID);
        storageApi.fenceStreamWrites(STREAM_ID).join();
        leaseCreated.complete(mock(PutResult.class));

        CompletionException failure = assertThrows(CompletionException.class, acquire::join);
        assertTrue(failure.getCause() instanceof LogFencedException);
        verify(oxiaClient, times(2)).delete(startsWith(leasePrefix), anyDeleteOptions());
        assertEquals(LogState.FENCED, stateManager.getState(STREAM_ID));
    }

    @Test
    void fenceAndLeaseCloseAreIdempotent() {
        String fenceKey = PersistStorageApi.streamWriteFenceKey(STREAM_ID);
        String leasePrefix = PersistStorageApi.streamWriteLeasePrefix(STREAM_ID);
        when(oxiaClient.put(startsWith(leasePrefix), any(byte[].class), anyPutOptions()))
            .thenReturn(CompletableFuture.completedFuture(mock(PutResult.class)));
        when(oxiaClient.get(eq(fenceKey), anyGetOptions()))
            .thenReturn(CompletableFuture.completedFuture(null));
        CompletableFuture<Boolean> released = new CompletableFuture<>();
        when(oxiaClient.delete(startsWith(leasePrefix), anyDeleteOptions()))
            .thenReturn(released);
        StreamWriteLease lease = storageApi.acquireStreamWriteLease(STREAM_ID).join();

        CompletableFuture<Void> firstClose = lease.closeAsync();
        CompletableFuture<Void> secondClose = lease.closeAsync();
        assertSame(firstClose, secondClose);
        assertFalse(firstClose.isDone());
        firstClose.orTimeout(1, TimeUnit.MILLISECONDS);
        assertThrows(TimeoutException.class,
            () -> firstClose.get(50, TimeUnit.MILLISECONDS));
        assertFalse(firstClose.isDone());
        released.complete(true);
        firstClose.join();
        verify(oxiaClient, times(1)).delete(startsWith(leasePrefix), anyDeleteOptions());

        when(oxiaClient.put(eq(fenceKey), any(byte[].class), anyPutOptions()))
            .thenReturn(
                CompletableFuture.completedFuture(mock(PutResult.class)),
                CompletableFuture.failedFuture(new KeyAlreadyExistsException(fenceKey)));
        storageApi.fenceStreamWrites(STREAM_ID).join();
        storageApi.fenceStreamWrites(STREAM_ID).join();

        assertEquals(LogState.FENCED, stateManager.getState(STREAM_ID));
        verify(oxiaClient, times(2)).put(eq(fenceKey), any(byte[].class), anyPutOptions());
    }

    @Test
    void failedLeaseCloseRetriesInternallyAndSharesCompletion() {
        String leasePrefix = PersistStorageApi.streamWriteLeasePrefix(STREAM_ID);
        when(oxiaClient.put(startsWith(leasePrefix), any(byte[].class), anyPutOptions()))
            .thenReturn(CompletableFuture.completedFuture(mock(PutResult.class)));
        when(oxiaClient.get(eq(PersistStorageApi.streamWriteFenceKey(STREAM_ID)), anyGetOptions()))
            .thenReturn(CompletableFuture.completedFuture(null));
        when(oxiaClient.delete(startsWith(leasePrefix), anyDeleteOptions()))
            .thenReturn(
                CompletableFuture.failedFuture(new IllegalStateException("temporary failure")),
                CompletableFuture.completedFuture(true));
        StreamWriteLease lease = storageApi.acquireStreamWriteLease(STREAM_ID).join();

        CompletableFuture<Void> firstClose = lease.closeAsync();
        CompletableFuture<Void> secondClose = lease.closeAsync();
        assertSame(firstClose, secondClose);
        assertFalse(firstClose.cancel(false));
        firstClose.join();
        assertSame(firstClose, lease.closeAsync());

        verify(oxiaClient, times(2)).delete(startsWith(leasePrefix), anyDeleteOptions());
    }

    @Test
    void timeoutNeverPurgesIndexesOrRegistration() {
        String leasePrefix = PersistStorageApi.streamWriteLeasePrefix(STREAM_ID);
        when(oxiaClient.put(
                eq(PersistStorageApi.streamWriteFenceKey(STREAM_ID)),
                any(byte[].class), anyPutOptions()))
            .thenReturn(CompletableFuture.completedFuture(mock(PutResult.class)));
        when(oxiaClient.list(eq(leasePrefix), eq(leasePrefix + "\uffff"), anyListOptions()))
            .thenReturn(CompletableFuture.completedFuture(List.of(leasePrefix + "stuck")));

        CompletionException failure = assertThrows(
            CompletionException.class,
            () -> storageApi.deleteStream(STREAM_ID, Duration.ZERO).join());

        assertTrue(failure.getCause() instanceof StreamWriteLeaseDrainTimeoutException);
        verify(oxiaClient, never()).deleteRange(anyString(), anyString(), anyDeleteRangeOptions());
        verify(oxiaClient, never()).delete(eq(StorageFormat.STREAM_REGISTER_PATH + "/" + STREAM_ID));
    }

    @Test
    void stalledLeaseInventoryHonorsDeadlineWithoutPurgingOrCancelingTheRead() {
        String leasePrefix = PersistStorageApi.streamWriteLeasePrefix(STREAM_ID);
        when(oxiaClient.put(
                eq(PersistStorageApi.streamWriteFenceKey(STREAM_ID)),
                any(byte[].class), anyPutOptions()))
            .thenReturn(CompletableFuture.completedFuture(mock(PutResult.class)));
        CompletableFuture<List<String>> stalledInventory = new CompletableFuture<>();
        when(oxiaClient.list(eq(leasePrefix), eq(leasePrefix + "\uffff"), anyListOptions()))
            .thenReturn(stalledInventory);

        CompletionException failure = assertThrows(CompletionException.class,
            () -> storageApi.deleteStream(STREAM_ID, Duration.ofMillis(25)).join());

        StreamWriteLeaseDrainTimeoutException timeout =
            (StreamWriteLeaseDrainTimeoutException) failure.getCause();
        assertEquals(
            StreamWriteLeaseDrainTimeoutException.UNKNOWN_ACTIVE_LEASE_COUNT,
            timeout.activeLeaseCount());
        assertFalse(stalledInventory.isDone());
        verify(oxiaClient, never()).deleteRange(
            anyString(), anyString(), anyDeleteRangeOptions());
        verify(oxiaClient, never()).delete(
            eq(StorageFormat.STREAM_REGISTER_PATH + "/" + STREAM_ID));

        stalledInventory.complete(List.of());
    }

    @Test
    void deletionCanResumeAfterTheBlockingLeaseIsReleased() {
        String fenceKey = PersistStorageApi.streamWriteFenceKey(STREAM_ID);
        String leasePrefix = PersistStorageApi.streamWriteLeasePrefix(STREAM_ID);
        when(oxiaClient.put(startsWith(leasePrefix), any(byte[].class), anyPutOptions()))
            .thenReturn(CompletableFuture.completedFuture(mock(PutResult.class)));
        when(oxiaClient.get(eq(fenceKey), anyGetOptions()))
            .thenReturn(CompletableFuture.completedFuture(null));
        when(oxiaClient.put(eq(fenceKey), any(byte[].class), anyPutOptions()))
            .thenReturn(
                CompletableFuture.completedFuture(mock(PutResult.class)),
                CompletableFuture.failedFuture(new KeyAlreadyExistsException(fenceKey)));
        when(oxiaClient.list(eq(leasePrefix), eq(leasePrefix + "\uffff"), anyListOptions()))
            .thenReturn(
                CompletableFuture.completedFuture(List.of(leasePrefix + "open-handle")),
                CompletableFuture.completedFuture(List.of()));
        when(oxiaClient.delete(startsWith(leasePrefix), anyDeleteOptions()))
            .thenReturn(CompletableFuture.completedFuture(true));
        when(oxiaClient.deleteRange(anyString(), anyString(), anyDeleteRangeOptions()))
            .thenReturn(CompletableFuture.completedFuture(null));
        when(oxiaClient.delete(StorageFormat.STREAM_REGISTER_PATH + "/" + STREAM_ID))
            .thenReturn(CompletableFuture.completedFuture(true));

        StreamWriteLease lease = storageApi.acquireStreamWriteLease(STREAM_ID).join();
        CompletionException firstDelete = assertThrows(
            CompletionException.class,
            () -> storageApi.deleteStream(STREAM_ID, Duration.ZERO).join());
        assertTrue(firstDelete.getCause() instanceof StreamWriteLeaseDrainTimeoutException);

        lease.closeAsync().join();
        storageApi.deleteStream(STREAM_ID, Duration.ZERO).join();

        verify(oxiaClient).deleteRange(anyString(), anyString(), anyDeleteRangeOptions());
        verify(oxiaClient).delete(StorageFormat.STREAM_REGISTER_PATH + "/" + STREAM_ID);
        assertEquals(LogState.FENCED, stateManager.getState(STREAM_ID));
    }

    @Test
    void mutationWithoutLocalDurableLeaseFailsBeforeWalWrite() {
        CompletionException failure = assertThrows(CompletionException.class, () ->
            storageApi.append(STREAM_ID, 1, Unpooled.EMPTY_BUFFER).join());

        assertTrue(failure.getCause() instanceof IllegalStateException);
        verify(walStorage, never()).put(
            eq(STREAM_ID), eq(1), eq(-1L), eq(-1L), any(ByteBuf.class));
    }

    @Test
    void appendRejectsCancellationUntilCallerBufferIsNoLongerInUse() {
        String leasePrefix = PersistStorageApi.streamWriteLeasePrefix(STREAM_ID);
        when(oxiaClient.put(startsWith(leasePrefix), any(byte[].class), anyPutOptions()))
            .thenReturn(CompletableFuture.completedFuture(mock(PutResult.class)));
        when(oxiaClient.get(
                eq(PersistStorageApi.streamWriteFenceKey(STREAM_ID)), anyGetOptions()))
            .thenReturn(CompletableFuture.completedFuture(null));
        when(oxiaClient.delete(startsWith(leasePrefix), anyDeleteOptions()))
            .thenReturn(CompletableFuture.completedFuture(true));
        CompletableFuture<AddResult> pendingMutation = new CompletableFuture<>();
        when(walStorage.put(
                eq(STREAM_ID), eq(1), eq(-1L), eq(-1L), any(ByteBuf.class)))
            .thenReturn(pendingMutation);
        StreamWriteLease lease = storageApi.acquireStreamWriteLease(STREAM_ID).join();

        ByteBuf payload = Unpooled.buffer(1).writeByte(7);
        CompletableFuture<AddResult> append = storageApi.append(STREAM_ID, 1, payload);
        assertFalse(append.cancel(false));
        append.orTimeout(1, TimeUnit.MILLISECONDS);
        assertThrows(TimeoutException.class,
            () -> append.get(50, TimeUnit.MILLISECONDS));
        assertFalse(append.isDone());
        assertEquals(1, payload.refCnt());
        CompletableFuture<Void> close = lease.closeAsync();

        assertFalse(close.isDone());
        verify(oxiaClient, never()).delete(startsWith(leasePrefix), anyDeleteOptions());

        AddResult stored = mock(AddResult.class);
        when(stored.header()).thenReturn(mock(EntryHeader.class));
        pendingMutation.complete(stored);

        assertSame(stored, append.join());
        assertTrue(payload.release());
        close.join();
        verify(oxiaClient).delete(startsWith(leasePrefix), anyDeleteOptions());
    }

    @SuppressWarnings("unchecked")
    private static Set<PutOption> anyPutOptions() {
        return any(Set.class);
    }

    @SuppressWarnings("unchecked")
    private static Set<PutOption> leaseCreateOptions() {
        return argThat(options -> options != null
            && options.contains(PutOption.AsEphemeralRecord)
            && options.contains(PutOption.IfRecordDoesNotExist)
            && options.contains(PutOption.PartitionKey(String.valueOf(STREAM_ID))));
    }

    @SuppressWarnings("unchecked")
    private static Set<GetOption> anyGetOptions() {
        return any(Set.class);
    }

    @SuppressWarnings("unchecked")
    private static Set<DeleteOption> anyDeleteOptions() {
        return any(Set.class);
    }

    @SuppressWarnings("unchecked")
    private static Set<DeleteRangeOption> anyDeleteRangeOptions() {
        return any(Set.class);
    }

    @SuppressWarnings("unchecked")
    private static Set<ListOption> anyListOptions() {
        return any(Set.class);
    }
}
