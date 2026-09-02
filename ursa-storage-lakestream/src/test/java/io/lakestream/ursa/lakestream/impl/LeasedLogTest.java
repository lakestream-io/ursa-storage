/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.lakestream.impl;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.lakestream.api.Log;
import io.lakestream.api.LogCursor;
import io.lakestream.api.LogEntry;
import io.lakestream.api.LogEntryHeader;
import io.lakestream.api.LogId;
import io.lakestream.ursa.storage.StorageApi.StreamWriteLease;
import io.netty.buffer.Unpooled;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.mockito.InOrder;

class LeasedLogTest {

    @Test
    void closeDrainsAcceptedAppendBeforeDelegateAndLease() throws Exception {
        Log delegate = mock(Log.class);
        StreamWriteLease lease = lease(17L);
        CompletableFuture<LogEntryHeader> pendingAppend = new CompletableFuture<>();
        when(delegate.id()).thenReturn(LogId.of(17L));
        when(delegate.append(1, Unpooled.EMPTY_BUFFER)).thenReturn(pendingAppend);
        when(lease.closeAsync()).thenReturn(CompletableFuture.completedFuture(null));
        LeasedLog log = new LeasedLog(delegate, lease);

        CompletableFuture<LogEntryHeader> append = log.append(1, Unpooled.EMPTY_BUFFER);
        CompletableFuture<Void> close = CompletableFuture.runAsync(() -> {
            try {
                log.close();
            } catch (Exception failure) {
                throw new RuntimeException(failure);
            }
        });

        assertFalse(close.isDone());
        verify(delegate, never()).close();
        verify(lease, never()).closeAsync();

        LogEntryHeader header = mock(LogEntryHeader.class);
        pendingAppend.complete(header);

        assertSame(header, append.get(10, TimeUnit.SECONDS));
        close.get(10, TimeUnit.SECONDS);
        verify(delegate).close();
        verify(lease).closeAsync();

        ExecutionException rejected = assertThrows(ExecutionException.class,
            () -> log.append(1, Unpooled.EMPTY_BUFFER).get());
        assertEquals(IllegalStateException.class, rejected.getCause().getClass());
        verify(delegate, times(1)).append(1, Unpooled.EMPTY_BUFFER);
    }

    @Test
    void appendRejectsCancellationUntilCallerBufferIsNoLongerInUse()
            throws Exception {
        Log delegate = mock(Log.class);
        StreamWriteLease lease = lease(21L);
        CompletableFuture<LogEntryHeader> pendingAppend = new CompletableFuture<>();
        when(delegate.id()).thenReturn(LogId.of(21L));
        var payload = Unpooled.buffer(1).writeByte(9);
        when(delegate.append(1, payload)).thenReturn(pendingAppend);
        when(lease.closeAsync()).thenReturn(CompletableFuture.completedFuture(null));
        LeasedLog log = new LeasedLog(delegate, lease);

        CompletableFuture<LogEntryHeader> callerFuture = log.append(1, payload);
        assertFalse(callerFuture.cancel(false));
        assertFalse(callerFuture.isDone());
        assertEquals(1, payload.refCnt());
        CompletableFuture<Void> close = CompletableFuture.runAsync(() -> {
            try {
                log.close();
            } catch (Exception failure) {
                throw new RuntimeException(failure);
            }
        });

        assertFalse(close.isDone());
        verify(delegate, never()).close();
        verify(lease, never()).closeAsync();

        LogEntryHeader header = mock(LogEntryHeader.class);
        pendingAppend.complete(header);

        assertSame(header, callerFuture.get(10, TimeUnit.SECONDS));
        assertTrue(payload.release());
        close.get(10, TimeUnit.SECONDS);
        verify(delegate).close();
        verify(lease).closeAsync();
    }

    @Test
    void closeTimesOutOnCanceledPendingReadAndRetriesAfterEntryCleanup()
            throws Exception {
        Log delegate = mock(Log.class);
        StreamWriteLease lease = lease(33L);
        LogEntry entry = mock(LogEntry.class);
        CompletableFuture<List<LogEntry>> pendingRead = new CompletableFuture<>();
        when(delegate.id()).thenReturn(LogId.of(33L));
        when(delegate.readEntries(0L, 1, 1024L)).thenReturn(pendingRead);
        when(lease.closeAsync()).thenReturn(CompletableFuture.completedFuture(null));
        LeasedLog log = new LeasedLog(delegate, lease, 25L);

        CompletableFuture<List<LogEntry>> read = log.readEntries(0L, 1, 1024L);
        assertTrue(read.cancel(false));

        IOException timeout = assertThrows(IOException.class, log::close);

        assertTrue(timeout.getMessage().contains("accepted log operations"));
        verify(entry, never()).close();
        verify(delegate, never()).close();
        verify(lease, never()).closeAsync();

        pendingRead.complete(List.of(entry));
        assertTrue(read.isCancelled());
        log.close();

        InOrder closeOrder = inOrder(entry, delegate, lease);
        closeOrder.verify(entry).close();
        closeOrder.verify(delegate).close();
        closeOrder.verify(lease).closeAsync();
    }

    @Test
    void closeDrainsAcceptedTrimBeforeReleasingLease() throws Exception {
        Log delegate = mock(Log.class);
        StreamWriteLease lease = lease(22L);
        CompletableFuture<Long> pendingTrim = new CompletableFuture<>();
        when(delegate.id()).thenReturn(LogId.of(22L));
        when(delegate.softTrim(9L)).thenReturn(pendingTrim);
        when(lease.closeAsync()).thenReturn(CompletableFuture.completedFuture(null));
        LeasedLog log = new LeasedLog(delegate, lease);

        CompletableFuture<Long> trim = log.softTrim(9L);
        CompletableFuture<Void> close = CompletableFuture.runAsync(() -> {
            try {
                log.close();
            } catch (Exception failure) {
                throw new RuntimeException(failure);
            }
        });

        assertFalse(close.isDone());
        pendingTrim.complete(10L);

        assertEquals(10L, trim.get(10, TimeUnit.SECONDS));
        close.get(10, TimeUnit.SECONDS);
        verify(delegate).close();
        verify(lease).closeAsync();
    }

    @Test
    void synchronousAppendFailureDoesNotBlockClose() throws Exception {
        Log delegate = mock(Log.class);
        StreamWriteLease lease = lease(18L);
        RuntimeException appendFailure = new RuntimeException("append failed");
        when(delegate.id()).thenReturn(LogId.of(18L));
        when(delegate.append(1, Unpooled.EMPTY_BUFFER)).thenThrow(appendFailure);
        when(lease.closeAsync()).thenReturn(CompletableFuture.completedFuture(null));
        LeasedLog log = new LeasedLog(delegate, lease);

        assertSame(appendFailure,
            assertThrows(RuntimeException.class,
                () -> log.append(1, Unpooled.EMPTY_BUFFER)));

        log.close();
        verify(delegate).close();
        verify(lease).closeAsync();
    }

    @Test
    void delegateCloseFailureRetainsLeaseUntilRetrySucceeds() throws Exception {
        Log delegate = mock(Log.class);
        StreamWriteLease lease = lease(19L);
        IOException delegateFailure = new IOException("delegate close failed");
        when(delegate.id()).thenReturn(LogId.of(19L));
        doThrow(delegateFailure).doNothing().when(delegate).close();
        when(lease.closeAsync()).thenReturn(CompletableFuture.completedFuture(null));
        LeasedLog log = new LeasedLog(delegate, lease);

        assertSame(delegateFailure, assertThrows(IOException.class, log::close));
        verify(lease, never()).closeAsync();

        log.close();
        log.close();

        verify(delegate, times(2)).close();
        verify(lease, times(1)).closeAsync();
    }

    @Test
    void transientLeaseReleaseFailureCanBeRetried() throws Exception {
        Log delegate = mock(Log.class);
        StreamWriteLease lease = lease(20L);
        RuntimeException leaseFailure = new RuntimeException("temporary Oxia failure");
        when(delegate.id()).thenReturn(LogId.of(20L));
        when(lease.closeAsync())
            .thenReturn(CompletableFuture.failedFuture(leaseFailure))
            .thenReturn(CompletableFuture.completedFuture(null));
        LeasedLog log = new LeasedLog(delegate, lease);

        assertSame(leaseFailure, assertThrows(RuntimeException.class, log::close));
        log.close();
        log.close();

        verify(delegate, times(1)).close();
        verify(lease, times(2)).closeAsync();
    }

    @Test
    void cursorCannotExposeUnfencedDelegateLog() throws Exception {
        Log delegate = mock(Log.class);
        LogCursor delegateCursor = mock(LogCursor.class);
        StreamWriteLease lease = lease(23L);
        when(delegate.id()).thenReturn(LogId.of(23L));
        when(delegate.openEphemeralCursor("reader", 0L))
            .thenReturn(CompletableFuture.completedFuture(delegateCursor));
        when(delegateCursor.log()).thenReturn(delegate);
        when(lease.closeAsync()).thenReturn(CompletableFuture.completedFuture(null));
        LeasedLog log = new LeasedLog(delegate, lease);

        LogCursor cursor = log.openEphemeralCursor("reader", 0L).get();

        assertSame(log, cursor.log());
        cursor.close();
        log.close();
        ExecutionException rejected = assertThrows(ExecutionException.class,
            () -> cursor.log().append(1, Unpooled.EMPTY_BUFFER).get());
        assertEquals(IllegalStateException.class, rejected.getCause().getClass());
        verify(delegate, never()).append(1, Unpooled.EMPTY_BUFFER);
    }

    @Test
    void closeWaitsForPendingCursorReadAndOpenCursorHandleBeforeRetry()
            throws Exception {
        Log delegate = mock(Log.class);
        LogCursor delegateCursor = mock(LogCursor.class);
        StreamWriteLease lease = lease(34L);
        LogEntry entry = mock(LogEntry.class);
        CompletableFuture<List<LogEntry>> pendingRead = new CompletableFuture<>();
        when(delegate.id()).thenReturn(LogId.of(34L));
        when(delegate.openEphemeralCursor("reader", 0L))
            .thenReturn(CompletableFuture.completedFuture(delegateCursor));
        when(delegateCursor.readEntries(1, 1024L)).thenReturn(pendingRead);
        when(lease.closeAsync()).thenReturn(CompletableFuture.completedFuture(null));
        LeasedLog log = new LeasedLog(delegate, lease, 25L);
        LogCursor cursor = log.openEphemeralCursor("reader", 0L).get();

        CompletableFuture<List<LogEntry>> read = cursor.readEntries(1, 1024L);

        IOException pendingReadTimeout = assertThrows(IOException.class, log::close);
        assertTrue(pendingReadTimeout.getMessage().contains("accepted log operations"));
        verify(delegate, never()).close();
        verify(lease, never()).closeAsync();

        pendingRead.complete(List.of(entry));
        assertSame(entry, read.get(5, TimeUnit.SECONDS).get(0));
        entry.close();

        IOException openCursorTimeout = assertThrows(IOException.class, log::close);
        assertTrue(openCursorTimeout.getMessage().contains("accepted log operations and cursors"));
        verify(delegateCursor, never()).close();
        verify(delegate, never()).close();
        verify(lease, never()).closeAsync();

        cursor.close();
        log.close();

        InOrder closeOrder = inOrder(entry, delegateCursor, delegate, lease);
        closeOrder.verify(entry).close();
        closeOrder.verify(delegateCursor).close();
        closeOrder.verify(delegate).close();
        closeOrder.verify(lease).closeAsync();
    }

    @Test
    void closeDrainsCursorPersistenceAndRejectsLaterCursorMutations() throws Exception {
        Log delegate = mock(Log.class);
        LogCursor delegateCursor = mock(LogCursor.class);
        StreamWriteLease lease = lease(27L);
        CompletableFuture<Void> pendingPersistence = new CompletableFuture<>();
        when(delegate.id()).thenReturn(LogId.of(27L));
        when(delegate.openEphemeralCursor("reader", 0L))
            .thenReturn(CompletableFuture.completedFuture(delegateCursor));
        when(delegateCursor.persistState()).thenReturn(pendingPersistence);
        when(lease.closeAsync()).thenReturn(CompletableFuture.completedFuture(null));
        LeasedLog log = new LeasedLog(delegate, lease);
        LogCursor cursor = log.openEphemeralCursor("reader", 0L).get();

        CompletableFuture<Void> persistence = cursor.persistState();
        CompletableFuture<Void> close = CompletableFuture.runAsync(() -> {
            try {
                log.close();
            } catch (Exception failure) {
                throw new RuntimeException(failure);
            }
        });

        assertFalse(close.isDone());
        verify(delegate, never()).close();
        verify(lease, never()).closeAsync();
        pendingPersistence.complete(null);
        persistence.get(5, TimeUnit.SECONDS);
        assertFalse(close.isDone());
        verify(delegate, never()).close();
        cursor.close();
        close.get(5, TimeUnit.SECONDS);

        ExecutionException rejected = assertThrows(ExecutionException.class,
            () -> cursor.markDelete(1L, Map.of()).get());
        assertEquals(IllegalStateException.class, rejected.getCause().getClass());
        verify(delegateCursor, never()).markDelete(1L, Map.of());
    }

    @Test
    void failedCursorCloseKeepsHandleActiveAndCanBeRetriedOnce() throws Exception {
        Log delegate = mock(Log.class);
        LogCursor delegateCursor = mock(LogCursor.class);
        StreamWriteLease lease = lease(35L);
        IOException cursorCloseFailure = new IOException("cursor close failed");
        when(delegate.id()).thenReturn(LogId.of(35L));
        when(delegate.openEphemeralCursor("reader", 0L))
            .thenReturn(CompletableFuture.completedFuture(delegateCursor));
        doThrow(cursorCloseFailure).doNothing().when(delegateCursor).close();
        when(lease.closeAsync()).thenReturn(CompletableFuture.completedFuture(null));
        LeasedLog log = new LeasedLog(delegate, lease, 25L);
        LogCursor cursor = log.openEphemeralCursor("reader", 0L).get();

        assertSame(cursorCloseFailure, assertThrows(IOException.class, cursor::close));
        IOException timeout = assertThrows(IOException.class, log::close);
        assertTrue(timeout.getMessage().contains("accepted log operations and cursors"));
        verify(delegate, never()).close();
        verify(lease, never()).closeAsync();

        cursor.close();
        cursor.close();
        log.close();

        verify(delegateCursor, times(2)).close();
        verify(delegate).close();
        verify(lease).closeAsync();
    }

    @Test
    void eventualCloseRetriesWithoutDroppingTheLeaseGuard() throws Exception {
        Log delegate = mock(Log.class);
        StreamWriteLease lease = lease(28L);
        when(delegate.id()).thenReturn(LogId.of(28L));
        doThrow(new IOException("transient close failure"))
            .doNothing().when(delegate).close();
        when(lease.closeAsync()).thenReturn(CompletableFuture.completedFuture(null));
        LeasedLog log = new LeasedLog(delegate, lease);

        log.closeEventually(Runnable::run).get(5, TimeUnit.SECONDS);

        verify(delegate, times(2)).close();
        verify(lease).closeAsync();
    }

    @Test
    void closeAsyncRetriesUntilDelegateAndLeaseAreClosed() throws Exception {
        Log delegate = mock(Log.class);
        StreamWriteLease lease = lease(41L);
        when(delegate.id()).thenReturn(LogId.of(41L));
        doThrow(new IOException("first close fails")).doNothing().when(delegate).close();
        when(lease.closeAsync()).thenReturn(CompletableFuture.completedFuture(null));
        LeasedLog log = new LeasedLog(delegate, lease, Runnable::run, () -> { });

        CompletableFuture<Void> closed = log.closeAsync();

        closed.get(10, TimeUnit.SECONDS);
        verify(delegate, times(2)).close();
        verify(lease, times(1)).closeAsync();
        assertSame(closed, log.closeAsync(), "closeAsync is idempotent");
    }

    @Test
    void failedOpenCleanupDoesNotReadThrowingDelegateIdAndRetriesClose()
            throws Exception {
        Log delegate = mock(Log.class);
        StreamWriteLease lease = lease(36L);
        when(delegate.id()).thenThrow(new IllegalStateException("invalid delegate identity"));
        doThrow(new IOException("transient close failure"))
            .doNothing().when(delegate).close();
        when(lease.closeAsync()).thenReturn(CompletableFuture.completedFuture(null));

        LeasedLog cleanup = LeasedLog.forFailedOpen(delegate, lease, Runnable::run);
        cleanup.closeEventually(Runnable::run).get(5, TimeUnit.SECONDS);

        InOrder closeOrder = inOrder(delegate, lease);
        closeOrder.verify(delegate, times(2)).close();
        closeOrder.verify(lease).closeAsync();
    }

    @Test
    void failedOpenCleanupDoesNotReadNullDelegateId() throws Exception {
        Log delegate = mock(Log.class);
        StreamWriteLease lease = lease(37L);
        when(delegate.id()).thenReturn(null);
        when(lease.closeAsync()).thenReturn(CompletableFuture.completedFuture(null));

        LeasedLog cleanup = LeasedLog.forFailedOpen(delegate, lease, Runnable::run);
        cleanup.closeEventually(Runnable::run).get(5, TimeUnit.SECONDS);

        InOrder closeOrder = inOrder(delegate, lease);
        closeOrder.verify(delegate).close();
        closeOrder.verify(lease).closeAsync();
    }

    @Test
    void eventualLeaseReleaseRetriesAFailedCloseFuture() throws Exception {
        StreamWriteLease lease = lease(29L);
        when(lease.closeAsync())
            .thenReturn(CompletableFuture.failedFuture(
                new IOException("transient release failure")))
            .thenReturn(CompletableFuture.completedFuture(null));

        LeasedLog.releaseLeaseEventually(lease, Runnable::run)
            .get(5, TimeUnit.SECONDS);

        verify(lease, times(2)).closeAsync();
    }

    @Test
    void closeTimesOutWithoutReleasingLeaseWhileAcceptedOperationIsPending()
            throws Exception {
        Log delegate = mock(Log.class);
        StreamWriteLease lease = lease(24L);
        CompletableFuture<LogEntryHeader> pendingAppend = new CompletableFuture<>();
        when(delegate.id()).thenReturn(LogId.of(24L));
        when(delegate.append(1, Unpooled.EMPTY_BUFFER)).thenReturn(pendingAppend);
        when(lease.closeAsync()).thenReturn(CompletableFuture.completedFuture(null));
        LeasedLog log = new LeasedLog(delegate, lease, 25L);
        log.append(1, Unpooled.EMPTY_BUFFER);

        IOException timeout = assertThrows(IOException.class, log::close);

        assertTrue(timeout.getMessage().contains("accepted log operations"));
        verify(delegate, never()).close();
        verify(lease, never()).closeAsync();

        pendingAppend.complete(mock(LogEntryHeader.class));
        log.close();
        verify(delegate).close();
        verify(lease).closeAsync();
    }

    @Test
    void closeTimesOutOnBlockedDelegateAndContinuesSameAttemptOnRetry()
            throws Exception {
        Log delegate = mock(Log.class);
        StreamWriteLease lease = lease(25L);
        CountDownLatch delegateCloseStarted = new CountDownLatch(1);
        CountDownLatch allowDelegateClose = new CountDownLatch(1);
        CountDownLatch delegateCloseFinished = new CountDownLatch(1);
        when(delegate.id()).thenReturn(LogId.of(25L));
        doAnswer(ignored -> {
            delegateCloseStarted.countDown();
            allowDelegateClose.await();
            delegateCloseFinished.countDown();
            return null;
        }).when(delegate).close();
        when(lease.closeAsync()).thenReturn(CompletableFuture.completedFuture(null));
        LeasedLog log = new LeasedLog(delegate, lease, 25L);

        IOException timeout = assertThrows(IOException.class, log::close);

        assertTrue(timeout.getMessage().contains("delegate log"));
        assertTrue(delegateCloseStarted.await(5, TimeUnit.SECONDS));
        verify(lease, never()).closeAsync();

        allowDelegateClose.countDown();
        assertTrue(delegateCloseFinished.await(5, TimeUnit.SECONDS));
        log.close();
        verify(delegate, times(1)).close();
        verify(lease).closeAsync();
    }

    @Test
    void closeTimesOutOnLeaseReleaseAndContinuesSameAttemptOnRetry()
            throws Exception {
        Log delegate = mock(Log.class);
        StreamWriteLease lease = lease(26L);
        CompletableFuture<Void> pendingRelease = new CompletableFuture<>();
        when(delegate.id()).thenReturn(LogId.of(26L));
        when(lease.closeAsync()).thenReturn(pendingRelease);
        LeasedLog log = new LeasedLog(delegate, lease, 25L);

        IOException timeout = assertThrows(IOException.class, log::close);

        assertTrue(timeout.getMessage().contains("write lease"));
        verify(delegate).close();
        verify(lease).closeAsync();

        pendingRelease.complete(null);
        log.close();
        verify(delegate, times(1)).close();
        verify(lease, times(1)).closeAsync();
    }

    @Test
    void boundedDelegateCloseExecutorRejectsExcessWorkWithoutReleasingLease()
            throws Exception {
        AtomicInteger threadsCreated = new AtomicInteger();
        ExecutorService closeExecutor = new ThreadPoolExecutor(
            1, 1, 0L, TimeUnit.MILLISECONDS, new ArrayBlockingQueue<>(1), task -> {
                threadsCreated.incrementAndGet();
                Thread thread = new Thread(task, "bounded-log-close-test");
                thread.setDaemon(true);
                return thread;
            }, new ThreadPoolExecutor.AbortPolicy());
        CountDownLatch firstCloseStarted = new CountDownLatch(1);
        CountDownLatch allowFirstClose = new CountDownLatch(1);
        Log firstDelegate = mock(Log.class);
        Log secondDelegate = mock(Log.class);
        Log thirdDelegate = mock(Log.class);
        StreamWriteLease firstLease = lease(30L);
        StreamWriteLease secondLease = lease(31L);
        StreamWriteLease thirdLease = lease(32L);
        when(firstDelegate.id()).thenReturn(LogId.of(30L));
        when(secondDelegate.id()).thenReturn(LogId.of(31L));
        when(thirdDelegate.id()).thenReturn(LogId.of(32L));
        doAnswer(ignored -> {
            firstCloseStarted.countDown();
            allowFirstClose.await();
            return null;
        }).when(firstDelegate).close();
        when(firstLease.closeAsync()).thenReturn(CompletableFuture.completedFuture(null));
        when(secondLease.closeAsync()).thenReturn(CompletableFuture.completedFuture(null));
        when(thirdLease.closeAsync()).thenReturn(CompletableFuture.completedFuture(null));
        LeasedLog first = new LeasedLog(firstDelegate, firstLease, 25L, closeExecutor);
        LeasedLog second = new LeasedLog(secondDelegate, secondLease, 25L, closeExecutor);
        LeasedLog third = new LeasedLog(thirdDelegate, thirdLease, 25L, closeExecutor);

        try {
            assertThrows(IOException.class, first::close);
            assertTrue(firstCloseStarted.await(5, TimeUnit.SECONDS));
            assertThrows(IOException.class, second::close);
            assertThrows(RejectedExecutionException.class, third::close);

            assertEquals(1, threadsCreated.get());
            verify(firstLease, never()).closeAsync();
            verify(secondLease, never()).closeAsync();
            verify(thirdLease, never()).closeAsync();

            allowFirstClose.countDown();
            first.close();
            second.close();
            third.close();

            verify(firstLease).closeAsync();
            verify(secondLease).closeAsync();
            verify(thirdLease).closeAsync();
        } finally {
            allowFirstClose.countDown();
            closeExecutor.shutdownNow();
        }
    }

    private static StreamWriteLease lease(long streamId) {
        StreamWriteLease lease = mock(StreamWriteLease.class);
        when(lease.streamId()).thenReturn(streamId);
        return lease;
    }
}
