/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.compaction;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import io.lakestream.ursa.compaction.metrics.CompactionMetrics;
import io.lakestream.ursa.compaction.task.CompactedOffset;
import io.lakestream.ursa.compaction.task.PreparedCompactStreamTask;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.metrics.LongGauge;
import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.InOrder;

public class CompactionManagerTest {

    private static final String TOPIC = "test-topic";
    // Distinct values so an argument swap between streamId / endOffset / cumulativeSize is detectable.
    private static final long STREAM_ID = 42L;
    private static final long START_OFFSET = 0L;
    private static final long END_OFFSET = 100L;
    private static final long LAST_INCLUDED_OFFSET = END_OFFSET - 1;
    private static final long TOTAL_SIZE = 4096L;
    private static final long CUMULATIVE_SIZE = 5000L;

    private CompactTaskManager taskManager;
    private CompactionManager compactionManager;
    private ExecutorService executor;

    @BeforeEach
    public void setup() {
        taskManager = mock(CompactTaskManager.class);
        compactionManager = new CompactionManager(taskManager);
        executor = Executors.newCachedThreadPool();
    }

    @AfterEach
    public void cleanup() {
        executor.shutdownNow();
    }

    private PreparedCompactStreamTask newTask(int status) {
        return new PreparedCompactStreamTask(STREAM_ID, START_OFFSET, END_OFFSET, TOTAL_SIZE, CUMULATIVE_SIZE,
                status, "test-task", TOPIC, Collections.emptyMap());
    }

    @Test
    public void testRecoverInitTaskUsesTopicKeyAndStreamIdOffset() throws Exception {
        PreparedCompactStreamTask task = newTask(PreparedCompactStreamTask.INIT);
        when(taskManager.getPreparedStreamTask(TOPIC)).thenReturn(task);

        compactionManager.recoverPreparedTasks(TOPIC);

        // Bug 1: the prepared task update must be keyed by the topic, not Optional.empty().
        verify(taskManager).updatePreparedCompactTask(eq(task), eq(Optional.of(TOPIC)));

        // The task end is exclusive; the published offset is the last offset included in the task.
        verify(taskManager).updatePublishedOffset(eq(TOPIC), eq(STREAM_ID), eq(LAST_INCLUDED_OFFSET));
        verify(taskManager).deletePreparedCompactTask(TOPIC);
    }

    @Test
    public void testRecoverPushedTaskUsesStreamIdOffset() throws Exception {
        PreparedCompactStreamTask task = newTask(PreparedCompactStreamTask.PUSHED_TASK);
        when(taskManager.getPreparedStreamTask(TOPIC)).thenReturn(task);

        compactionManager.recoverPreparedTasks(TOPIC);

        verify(taskManager).updatePublishedOffset(eq(TOPIC), eq(STREAM_ID), eq(LAST_INCLUDED_OFFSET));
        verify(taskManager).deletePreparedCompactTask(TOPIC);
        // The PUSHED_TASK branch does not re-publish or re-update the prepared task.
        verify(taskManager, never()).updatePreparedCompactTask(any(), any());
        verify(taskManager, never()).publishCompactTask(any());
    }

    @Test
    public void testRecoverNoPreparedTaskIsNoop() throws Exception {
        when(taskManager.getPreparedStreamTask(TOPIC)).thenReturn(null);

        compactionManager.recoverPreparedTasks(TOPIC);

        verify(taskManager).getPreparedStreamTask(TOPIC);
        verify(taskManager, never()).updatePublishedOffset(eq(TOPIC), anyLong(), anyLong());
        verify(taskManager, never()).deletePreparedCompactTask(TOPIC);
    }

    @Test
    public void testPublishTaskRecordsLastIncludedOffsetAndMetric() throws Exception {
        PreparedCompactStreamTask task = newTask(PreparedCompactStreamTask.INIT);
        CompactionMetrics metrics = mock(CompactionMetrics.class);
        LongGauge latestPublishedOffset = mock(LongGauge.class);
        when(metrics.getLatestPublishedOffset()).thenReturn(latestPublishedOffset);
        compactionManager = new CompactionManager(taskManager, metrics);

        compactionManager.publishTask(task);

        verify(taskManager).publishPreparedCompactTask(eq(task), eq(Optional.of(TOPIC)));
        verify(taskManager).updatePreparedCompactTask(eq(task), eq(Optional.of(TOPIC)));
        verify(taskManager).updatePublishedOffset(eq(TOPIC), eq(STREAM_ID), eq(LAST_INCLUDED_OFFSET));
        verify(taskManager).deletePreparedCompactTask(TOPIC);
        verify(latestPublishedOffset).set(LAST_INCLUDED_OFFSET,
                Attributes.of(AttributeKey.stringKey("topic"), TOPIC));
    }

    @Test
    public void testPublishTaskRejectsEmptyRangeBeforePersisting() {
        PreparedCompactStreamTask task = newTask(PreparedCompactStreamTask.INIT);
        task.setStartOffset(END_OFFSET);

        IllegalArgumentException error = assertThrows(
                IllegalArgumentException.class, () -> compactionManager.publishTask(task));

        assertEquals("Invalid offset range [100, 100): startOffset must be non-negative "
                + "and endOffset must be greater than startOffset", error.getMessage());
        verifyNoInteractions(taskManager);
    }

    @Test
    public void testRecoverPreparedTaskRejectsReversedRangeWithoutDeletingIt() throws Exception {
        PreparedCompactStreamTask task = newTask(PreparedCompactStreamTask.PUSHED_TASK);
        task.setStartOffset(END_OFFSET + 1);
        when(taskManager.getPreparedStreamTask(TOPIC)).thenReturn(task);

        assertThrows(IllegalArgumentException.class, () -> compactionManager.recoverPreparedTasks(TOPIC));

        verify(taskManager).getPreparedStreamTask(TOPIC);
        verifyNoMoreInteractions(taskManager);
    }

    @Test
    public void testRecoverPreparedTaskDeletesMismatchedStreamIncarnation() throws Exception {
        PreparedCompactStreamTask task = newTask(PreparedCompactStreamTask.INIT);
        when(taskManager.getPreparedStreamTask(TOPIC)).thenReturn(task);

        compactionManager.recoverPreparedTasks(TOPIC, STREAM_ID + 1);

        verify(taskManager).getPreparedStreamTask(TOPIC);
        verify(taskManager).deletePreparedCompactTask(TOPIC);
        verify(taskManager, never()).publishCompactTask(any());
        verify(taskManager, never()).updatePublishedOffset(eq(TOPIC), anyLong(), anyLong());
        verifyNoMoreInteractions(taskManager);
    }

    @Test
    public void testLastPublishedOffset() throws Exception {
        when(taskManager.getPublishedOffset(TOPIC)).thenReturn(null);
        assertEquals(-1L, compactionManager.lastPublishedOffset(TOPIC));

        CompactedOffset offset = mock(CompactedOffset.class);
        when(offset.getOffset()).thenReturn(777L);
        when(taskManager.getPublishedOffset(TOPIC)).thenReturn(offset);
        assertEquals(777L, compactionManager.lastPublishedOffset(TOPIC));
    }

    @Test
    public void testLastPublishedOffsetChecksStreamIncarnation() throws Exception {
        CompactedOffset offset = new CompactedOffset(STREAM_ID, 777L, 0L);
        when(taskManager.getPublishedOffset(TOPIC)).thenReturn(offset);

        assertEquals(777L, compactionManager.lastPublishedOffset(TOPIC, STREAM_ID));
        assertEquals(-1L, compactionManager.lastPublishedOffset(TOPIC, STREAM_ID + 1));
    }

    @Test
    public void testPublicationSessionMakesTaskVisibleOnlyAfterCursorCas() throws Exception {
        CompactTaskManager.PublicationLease lease = lease();
        CompactTaskManager.PublishedOffsetClaim initialCursor = cursor(-1L, 11L);
        CompactTaskManager.PublishedOffsetClaim advancedCursor = cursor(LAST_INCLUDED_OFFSET, 12L);
        PreparedCompactStreamTask task = newTask(PreparedCompactStreamTask.INIT);
        CompactTaskManager.PreparedTaskClaim prepared =
                new CompactTaskManager.PreparedTaskClaim(task, 21L);
        when(taskManager.tryAcquirePublicationLease(TOPIC, STREAM_ID)).thenReturn(Optional.of(lease));
        when(taskManager.claimPublishedOffset(lease)).thenReturn(initialCursor);
        when(taskManager.validatePublicationLease(lease)).thenReturn(true);
        when(taskManager.getPreparedTaskClaim(TOPIC)).thenReturn(Optional.empty());
        when(taskManager.tryCreatePreparedTaskClaim(task, TOPIC)).thenReturn(Optional.of(prepared));
        when(taskManager.publishCompactTaskIfAbsent(any())).thenReturn(true, false);
        when(taskManager.compareAndSetPublishedOffset(
                eq(lease), eq(initialCursor), any(CompactedOffset.class))).thenReturn(advancedCursor);
        when(taskManager.deletePreparedTaskClaim(TOPIC, prepared)).thenReturn(true);
        when(taskManager.releasePublicationLease(lease)).thenReturn(true);

        CompactionManager.PublicationSession session =
                compactionManager.tryOpenPublicationSession(TOPIC, STREAM_ID).orElseThrow();
        assertEquals(CompactionManager.PublicationResult.PUBLISHED,
                session.publishNext(ignored -> Optional.of(task)));
        session.close();

        InOrder order = inOrder(taskManager);
        order.verify(taskManager).publishCompactTaskIfAbsent(any());
        order.verify(taskManager).compareAndSetPublishedOffset(
                eq(lease), eq(initialCursor), any(CompactedOffset.class));
        order.verify(taskManager).publishCompactTaskIfAbsent(any());
        order.verify(taskManager).publishPackagedTaskName(task.getTaskName());
        order.verify(taskManager).deletePreparedTaskClaim(TOPIC, prepared);
    }

    @Test
    public void testPublicationSessionPermanentlyFencesOnCursorConflict() throws Exception {
        CompactTaskManager.PublicationLease lease = lease();
        CompactTaskManager.PublishedOffsetClaim initialCursor = cursor(-1L, 11L);
        PreparedCompactStreamTask task = newTask(PreparedCompactStreamTask.INIT);
        CompactTaskManager.PreparedTaskClaim prepared =
                new CompactTaskManager.PreparedTaskClaim(task, 21L);
        when(taskManager.tryAcquirePublicationLease(TOPIC, STREAM_ID)).thenReturn(Optional.of(lease));
        when(taskManager.claimPublishedOffset(lease)).thenReturn(initialCursor);
        when(taskManager.validatePublicationLease(lease)).thenReturn(true);
        when(taskManager.getPreparedTaskClaim(TOPIC)).thenReturn(Optional.empty());
        when(taskManager.tryCreatePreparedTaskClaim(task, TOPIC)).thenReturn(Optional.of(prepared));
        when(taskManager.publishCompactTaskIfAbsent(any())).thenReturn(true);
        when(taskManager.compareAndSetPublishedOffset(
                eq(lease), eq(initialCursor), any(CompactedOffset.class)))
                .thenThrow(new PublicationFencedException("cursor changed"));
        when(taskManager.releasePublicationLease(lease)).thenReturn(false);

        CompactionManager.PublicationSession session =
                compactionManager.tryOpenPublicationSession(TOPIC, STREAM_ID).orElseThrow();

        assertThrows(PublicationFencedException.class,
                () -> session.publishNext(ignored -> Optional.of(task)));
        assertTrue(session.isFenced());
        assertThrows(PublicationFencedException.class,
                () -> session.publishNext(ignored -> Optional.of(task)));
        verify(taskManager, never()).publishPackagedTaskName(any());
        verify(taskManager, never()).deletePreparedTaskClaim(TOPIC, prepared);
        verify(taskManager, never()).deleteCompactTask(any());
        session.close();
    }

    @Test
    public void testOldSessionCannotPublishMarkerAfterSuccessorClaimsLease() throws Exception {
        CompactTaskManager.PublicationLease lease = lease();
        CompactTaskManager.PublishedOffsetClaim initialCursor = cursor(-1L, 11L);
        CompactTaskManager.PublishedOffsetClaim advancedCursor = cursor(LAST_INCLUDED_OFFSET, 12L);
        PreparedCompactStreamTask task = newTask(PreparedCompactStreamTask.INIT);
        CompactTaskManager.PreparedTaskClaim prepared =
                new CompactTaskManager.PreparedTaskClaim(task, 21L);
        when(taskManager.tryAcquirePublicationLease(TOPIC, STREAM_ID)).thenReturn(Optional.of(lease));
        when(taskManager.claimPublishedOffset(lease)).thenReturn(initialCursor);
        // The session is current when the tick begins, then loses its lease after cursor commit and
        // before the package marker would make the task visible.
        when(taskManager.validatePublicationLease(lease))
                .thenReturn(true, true, true, true, true, true, true, true, false);
        when(taskManager.getPreparedTaskClaim(TOPIC)).thenReturn(Optional.empty());
        when(taskManager.tryCreatePreparedTaskClaim(task, TOPIC)).thenReturn(Optional.of(prepared));
        when(taskManager.publishCompactTaskIfAbsent(any())).thenReturn(true, false);
        when(taskManager.compareAndSetPublishedOffset(
                eq(lease), eq(initialCursor), any(CompactedOffset.class))).thenReturn(advancedCursor);
        when(taskManager.releasePublicationLease(lease)).thenReturn(false);

        CompactionManager.PublicationSession session =
                compactionManager.tryOpenPublicationSession(TOPIC, STREAM_ID).orElseThrow();

        assertThrows(PublicationFencedException.class,
                () -> session.publishNext(ignored -> Optional.of(task)));
        assertTrue(session.isFenced());
        verify(taskManager).compareAndSetPublishedOffset(
                eq(lease), eq(initialCursor), any(CompactedOffset.class));
        verify(taskManager, never()).publishPackagedTaskName(any());
        verify(taskManager, never()).deletePreparedTaskClaim(TOPIC, prepared);
        session.close();
    }

    @Test
    public void testPublicationSessionDoesNotRecoverObsoleteStreamPreparedTask() throws Exception {
        CompactTaskManager.PublicationLease lease = lease();
        PreparedCompactStreamTask obsolete = newTask(PreparedCompactStreamTask.INIT);
        obsolete.setStreamId(STREAM_ID - 1);
        CompactTaskManager.PreparedTaskClaim obsoleteClaim =
                new CompactTaskManager.PreparedTaskClaim(obsolete, 20L);
        when(taskManager.tryAcquirePublicationLease(TOPIC, STREAM_ID)).thenReturn(Optional.of(lease));
        when(taskManager.claimPublishedOffset(lease)).thenReturn(cursor(-1L, 11L));
        when(taskManager.validatePublicationLease(lease)).thenReturn(true);
        when(taskManager.getPreparedTaskClaim(TOPIC)).thenReturn(Optional.of(obsoleteClaim));
        when(taskManager.deletePreparedTaskClaim(TOPIC, obsoleteClaim)).thenReturn(true);
        when(taskManager.releasePublicationLease(lease)).thenReturn(true);

        CompactionManager.PublicationSession session =
                compactionManager.tryOpenPublicationSession(TOPIC, STREAM_ID).orElseThrow();
        assertEquals(CompactionManager.PublicationResult.NO_TASK,
                session.publishNext(ignored -> Optional.empty()));
        session.close();

        verify(taskManager).deletePreparedTaskClaim(TOPIC, obsoleteClaim);
        verify(taskManager, never()).publishCompactTaskIfAbsent(any());
        verify(taskManager, never()).compareAndSetPublishedOffset(any(), any(), any());
        verify(taskManager, never()).publishPackagedTaskName(any());
    }

    @Test
    public void testPublicationSessionFenceIsNonBlockingAndCloseWaitsForFactory() throws Exception {
        CompactTaskManager.PublicationLease lease = lease();
        when(taskManager.tryAcquirePublicationLease(TOPIC, STREAM_ID)).thenReturn(Optional.of(lease));
        when(taskManager.claimPublishedOffset(lease)).thenReturn(cursor(-1L, 11L));
        when(taskManager.validatePublicationLease(lease)).thenReturn(true);
        when(taskManager.getPreparedTaskClaim(TOPIC)).thenReturn(Optional.empty());
        when(taskManager.releasePublicationLease(lease)).thenReturn(true);
        CompactionManager.PublicationSession session =
                compactionManager.tryOpenPublicationSession(TOPIC, STREAM_ID).orElseThrow();
        CountDownLatch factoryEntered = new CountDownLatch(1);
        CountDownLatch allowFactoryReturn = new CountDownLatch(1);
        CountDownLatch closeStarted = new CountDownLatch(1);

        CompletableFuture<CompactionManager.PublicationResult> publishFuture = new CompletableFuture<>();
        executor.execute(() -> {
            try {
                publishFuture.complete(session.publishNext(ignored -> {
                    factoryEntered.countDown();
                    assertTrue(allowFactoryReturn.await(5, TimeUnit.SECONDS));
                    return Optional.of(newTask(PreparedCompactStreamTask.INIT));
                }));
            } catch (Throwable error) {
                publishFuture.completeExceptionally(error);
            }
        });
        assertTrue(factoryEntered.await(5, TimeUnit.SECONDS));

        assertTimeoutPreemptively(Duration.ofSeconds(1), session::fence);
        assertTrue(session.isFenced());
        CompletableFuture<Void> closeFuture = CompletableFuture.runAsync(() -> {
            closeStarted.countDown();
            try {
                session.close();
            } catch (Exception error) {
                throw new RuntimeException(error);
            }
        }, executor);
        assertTrue(closeStarted.await(5, TimeUnit.SECONDS));
        assertFalse(closeFuture.isDone());
        verify(taskManager, never()).releasePublicationLease(lease);

        allowFactoryReturn.countDown();
        ExecutionException publishError = assertThrows(
                ExecutionException.class, () -> publishFuture.get(5, TimeUnit.SECONDS));
        assertTrue(publishError.getCause() instanceof PublicationFencedException);
        closeFuture.get(5, TimeUnit.SECONDS);
        verify(taskManager).releasePublicationLease(lease);
        verify(taskManager, never()).tryCreatePreparedTaskClaim(any(), any());
        verify(taskManager, never()).publishCompactTaskIfAbsent(any());
        verify(taskManager, never()).compareAndSetPublishedOffset(any(), any(), any());
        verify(taskManager, never()).publishPackagedTaskName(any());
    }

    @Test
    public void testPublicationSessionFenceStopsAfterInFlightStage() throws Exception {
        CompactTaskManager.PublicationLease lease = lease();
        CompactTaskManager.PublishedOffsetClaim initialCursor = cursor(-1L, 11L);
        PreparedCompactStreamTask task = newTask(PreparedCompactStreamTask.INIT);
        CompactTaskManager.PreparedTaskClaim prepared =
                new CompactTaskManager.PreparedTaskClaim(task, 21L);
        when(taskManager.tryAcquirePublicationLease(TOPIC, STREAM_ID)).thenReturn(Optional.of(lease));
        when(taskManager.claimPublishedOffset(lease)).thenReturn(initialCursor);
        when(taskManager.validatePublicationLease(lease)).thenReturn(true);
        when(taskManager.getPreparedTaskClaim(TOPIC)).thenReturn(Optional.empty());
        when(taskManager.tryCreatePreparedTaskClaim(task, TOPIC)).thenReturn(Optional.of(prepared));
        when(taskManager.releasePublicationLease(lease)).thenReturn(true);
        CountDownLatch stageEntered = new CountDownLatch(1);
        CountDownLatch allowStageReturn = new CountDownLatch(1);
        when(taskManager.publishCompactTaskIfAbsent(any())).thenAnswer(ignored -> {
            stageEntered.countDown();
            assertTrue(allowStageReturn.await(5, TimeUnit.SECONDS));
            return true;
        });
        CompactionManager.PublicationSession session =
                compactionManager.tryOpenPublicationSession(TOPIC, STREAM_ID).orElseThrow();

        CompletableFuture<CompactionManager.PublicationResult> publishFuture = new CompletableFuture<>();
        executor.execute(() -> {
            try {
                publishFuture.complete(session.publishNext(ignored -> Optional.of(task)));
            } catch (Throwable error) {
                publishFuture.completeExceptionally(error);
            }
        });
        assertTrue(stageEntered.await(5, TimeUnit.SECONDS));

        assertTimeoutPreemptively(Duration.ofSeconds(1), session::fence);
        assertTrue(session.isFenced());
        CompletableFuture<Void> closeFuture = CompletableFuture.runAsync(() -> {
            try {
                session.close();
            } catch (Exception error) {
                throw new RuntimeException(error);
            }
        }, executor);
        assertFalse(closeFuture.isDone());
        verify(taskManager, never()).releasePublicationLease(lease);

        allowStageReturn.countDown();
        ExecutionException publishError = assertThrows(
                ExecutionException.class, () -> publishFuture.get(5, TimeUnit.SECONDS));
        assertTrue(publishError.getCause() instanceof PublicationFencedException);
        closeFuture.get(5, TimeUnit.SECONDS);
        verify(taskManager).releasePublicationLease(lease);
        verify(taskManager, never()).compareAndSetPublishedOffset(any(), any(), any());
        verify(taskManager, never()).publishPackagedTaskName(any());
        verify(taskManager, never()).deletePreparedTaskClaim(TOPIC, prepared);
    }

    @Test
    public void testPublicationSessionCloseRetriesTransientLeaseReleaseFailure() throws Exception {
        CompactTaskManager.PublicationLease lease = lease();
        when(taskManager.tryAcquirePublicationLease(TOPIC, STREAM_ID)).thenReturn(Optional.of(lease));
        when(taskManager.claimPublishedOffset(lease)).thenReturn(cursor(-1L, 11L));
        when(taskManager.releasePublicationLease(lease))
                .thenThrow(new ExecutionException(new IOException("temporary Oxia failure")))
                .thenReturn(true);
        CompactionManager.PublicationSession session =
                compactionManager.tryOpenPublicationSession(TOPIC, STREAM_ID).orElseThrow();

        assertThrows(ExecutionException.class, session::close);
        assertTrue(session.isClosed());
        assertTrue(session.isFenced());

        session.close();
        session.close();

        verify(taskManager, times(2)).releasePublicationLease(lease);
    }

    @Test
    public void testPublicationSessionCloseRestoresInterruptAndKeepsReleaseRetryable() throws Exception {
        CompactTaskManager.PublicationLease lease = lease();
        when(taskManager.tryAcquirePublicationLease(TOPIC, STREAM_ID)).thenReturn(Optional.of(lease));
        when(taskManager.claimPublishedOffset(lease)).thenReturn(cursor(-1L, 11L));
        when(taskManager.releasePublicationLease(lease))
                .thenThrow(new InterruptedException("lease release interrupted"))
                .thenReturn(true);
        CompactionManager.PublicationSession session =
                compactionManager.tryOpenPublicationSession(TOPIC, STREAM_ID).orElseThrow();

        try {
            assertThrows(InterruptedException.class, session::close);
            assertTrue(Thread.currentThread().isInterrupted());
            assertTrue(session.isClosed());
            assertTrue(session.isFenced());
        } finally {
            Thread.interrupted();
        }

        session.close();
        session.close();

        verify(taskManager, times(2)).releasePublicationLease(lease);
    }

    @Test
    public void testClaimFailureRetainsLeaseForReleaseRecoveryBeforeReacquire() throws Exception {
        CompactTaskManager.PublicationLease failedLease = lease();
        CompactTaskManager.PublicationLease recoveredLease =
                new CompactTaskManager.PublicationLease(TOPIC, STREAM_ID, "successor", 8L);
        ExecutionException claimFailure =
                new ExecutionException(new IOException("temporary cursor claim failure"));
        ExecutionException releaseFailure =
                new ExecutionException(new IOException("temporary lease release failure"));
        when(taskManager.tryAcquirePublicationLease(TOPIC, STREAM_ID))
                .thenReturn(Optional.of(failedLease), Optional.of(recoveredLease));
        when(taskManager.claimPublishedOffset(failedLease)).thenThrow(claimFailure);
        when(taskManager.releasePublicationLease(failedLease))
                .thenThrow(releaseFailure)
                .thenReturn(true);
        when(taskManager.claimPublishedOffset(recoveredLease)).thenReturn(cursor(-1L, 12L));
        when(taskManager.releasePublicationLease(recoveredLease)).thenReturn(true);

        ExecutionException openFailure = assertThrows(
                ExecutionException.class,
                () -> compactionManager.tryOpenPublicationSession(TOPIC, STREAM_ID));
        assertEquals(claimFailure, openFailure);
        assertEquals(1, openFailure.getSuppressed().length);
        assertEquals(releaseFailure, openFailure.getSuppressed()[0]);

        CompactionManager.PublicationSession recovered =
                compactionManager.tryOpenPublicationSession(TOPIC, STREAM_ID).orElseThrow();
        recovered.close();

        InOrder order = inOrder(taskManager);
        order.verify(taskManager).tryAcquirePublicationLease(TOPIC, STREAM_ID);
        order.verify(taskManager).claimPublishedOffset(failedLease);
        order.verify(taskManager, times(2)).releasePublicationLease(failedLease);
        order.verify(taskManager).tryAcquirePublicationLease(TOPIC, STREAM_ID);
        order.verify(taskManager).claimPublishedOffset(recoveredLease);
        order.verify(taskManager).releasePublicationLease(recoveredLease);
    }

    private static CompactTaskManager.PublicationLease lease() {
        return new CompactTaskManager.PublicationLease(TOPIC, STREAM_ID, "owner", 7L);
    }

    private static CompactTaskManager.PublishedOffsetClaim cursor(long offset, long revision) {
        return new CompactTaskManager.PublishedOffsetClaim(
                new CompactedOffset(STREAM_ID, offset, 0L), revision);
    }
}
