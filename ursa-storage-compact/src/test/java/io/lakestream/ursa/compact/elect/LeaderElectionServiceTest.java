/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.compact.elect;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.lakestream.ursa.metrics.InstrumentProvider;
import io.oxia.client.api.AsyncOxiaClient;
import io.oxia.client.api.GetResult;
import io.oxia.client.api.PutResult;
import io.oxia.client.api.Version;
import io.oxia.client.api.options.DeleteOption;
import io.oxia.client.api.options.PutOption;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

class LeaderElectionServiceTest {

    private static final String HOSTNAME = "compactor-a";
    private static final String ELECTION_ROOT = "/test/compact/leader";
    private static final Version OUR_VERSION = version(17);
    private static final Version OTHER_VERSION = version(23);

    private AsyncOxiaClient client;
    private ScheduledExecutorService executor;
    private ScheduledFuture<?> refreshFuture;

    @BeforeEach
    void setUp() {
        client = mock(AsyncOxiaClient.class);
        executor = mock(ScheduledExecutorService.class);
        refreshFuture = mock(ScheduledFuture.class);
        doReturn(refreshFuture).when(executor).scheduleWithFixedDelay(
                any(Runnable.class), eq(2L), eq(2L), eq(TimeUnit.SECONDS));
        when(client.delete(eq(ELECTION_ROOT), any())).thenReturn(CompletableFuture.completedFuture(null));
    }

    @Test
    void listenerErrorDuringInitialAcquireReleasesClaimBeforeRetrying() {
        when(client.get(ELECTION_ROOT)).thenReturn(
                CompletableFuture.completedFuture(null),
                CompletableFuture.completedFuture(null));
        when(client.put(eq(ELECTION_ROOT), any(byte[].class), eq(acquireOptions())))
                .thenReturn(
                        CompletableFuture.completedFuture(new PutResult(ELECTION_ROOT, OUR_VERSION)),
                        CompletableFuture.completedFuture(new PutResult(ELECTION_ROOT, version(29))));
        List<Boolean> transitions = new ArrayList<>();
        AtomicInteger leaderAttempts = new AtomicInteger();
        LeaderElectionService service = newService(leading -> {
            transitions.add(leading);
            if (leading && leaderAttempts.getAndIncrement() == 0) {
                throw new AssertionError("leader runner startup failed");
            }
        });

        assertDoesNotThrow(service::start);

        assertFalse(service.isLeader());
        assertEquals(List.of(true, false), transitions);
        ArgumentCaptor<Set<DeleteOption>> releasedClaim = ArgumentCaptor.forClass(Set.class);
        verify(client).delete(eq(ELECTION_ROOT), releasedClaim.capture());
        assertEquals(Set.of(DeleteOption.IfVersionIdEquals(OUR_VERSION.versionId())),
                releasedClaim.getValue());
        Runnable scheduledRefresh = captureScheduledRefresh();

        assertDoesNotThrow(scheduledRefresh::run);

        assertTrue(service.isLeader());
        assertEquals(List.of(true, false, true), transitions);
        service.close();
    }

    @Test
    void failedRelinquishIsRetriedWithoutRestartingLeaderRunners() {
        GetResult ourLeader = new GetResult(
                ELECTION_ROOT, HOSTNAME.getBytes(StandardCharsets.UTF_8), OUR_VERSION);
        when(client.get(ELECTION_ROOT)).thenReturn(
                CompletableFuture.completedFuture(null),
                CompletableFuture.completedFuture(ourLeader),
                CompletableFuture.completedFuture(null));
        when(client.put(eq(ELECTION_ROOT), any(byte[].class), eq(acquireOptions())))
                .thenReturn(
                        CompletableFuture.completedFuture(new PutResult(ELECTION_ROOT, OUR_VERSION)),
                        CompletableFuture.completedFuture(new PutResult(ELECTION_ROOT, version(29))));
        when(client.delete(eq(ELECTION_ROOT), any()))
                .thenReturn(
                        CompletableFuture.failedFuture(new RuntimeException("Oxia unavailable")),
                        CompletableFuture.completedFuture(null),
                        CompletableFuture.completedFuture(null));
        List<Boolean> transitions = new ArrayList<>();
        AtomicInteger leaderAttempts = new AtomicInteger();
        LeaderElectionService service = newService(leading -> {
            transitions.add(leading);
            if (leading && leaderAttempts.getAndIncrement() == 0) {
                throw new AssertionError("leader runner startup failed");
            }
        });

        service.start();
        Runnable scheduledRefresh = captureScheduledRefresh();

        assertFalse(service.isLeader());
        assertEquals(List.of(true, false), transitions);
        scheduledRefresh.run();

        assertFalse(service.isLeader());
        assertEquals(List.of(true, false), transitions);
        verify(client, times(2)).delete(eq(ELECTION_ROOT), eq(Set.of(
                DeleteOption.IfVersionIdEquals(OUR_VERSION.versionId()))));

        scheduledRefresh.run();

        assertTrue(service.isLeader());
        assertEquals(List.of(true, false, true), transitions);
        service.close();
    }

    @Test
    void supersedingLeaderSettlesFailedRelinquishWithoutDeletingSuccessor() {
        GetResult otherLeader = new GetResult(
                ELECTION_ROOT, "compactor-b".getBytes(StandardCharsets.UTF_8), OTHER_VERSION);
        when(client.get(ELECTION_ROOT)).thenReturn(
                CompletableFuture.completedFuture(null),
                CompletableFuture.completedFuture(otherLeader));
        when(client.put(eq(ELECTION_ROOT), any(byte[].class), eq(acquireOptions())))
                .thenReturn(CompletableFuture.completedFuture(
                        new PutResult(ELECTION_ROOT, OUR_VERSION)));
        when(client.delete(eq(ELECTION_ROOT), any()))
                .thenReturn(CompletableFuture.failedFuture(new RuntimeException("Oxia unavailable")));
        List<Boolean> transitions = new ArrayList<>();
        LeaderElectionService service = newService(leading -> {
            transitions.add(leading);
            if (leading) {
                throw new AssertionError("leader runner startup failed");
            }
        });

        service.start();
        Runnable scheduledRefresh = captureScheduledRefresh();
        scheduledRefresh.run();

        assertFalse(service.isLeader());
        assertEquals(List.of(true, false), transitions);
        assertEquals("compactor-b", service.getCurrentLeader().orElseThrow().getHostname());
        verify(client).delete(eq(ELECTION_ROOT), eq(Set.of(
                DeleteOption.IfVersionIdEquals(OUR_VERSION.versionId()))));

        service.close();

        verify(client, never()).delete(eq(ELECTION_ROOT), eq(Set.of(
                DeleteOption.IfVersionIdEquals(OTHER_VERSION.versionId()))));
    }

    @Test
    void closeReleasesAcquiredRecordAfterLeaderListenerError() {
        stubAcquireLeadership();
        LeaderElectionService service = newService(leading -> {
            if (leading) {
                throw new AssertionError("leader runner startup failed");
            }
        });
        service.start();

        assertFalse(service.isLeader());
        assertDoesNotThrow(service::close);

        ArgumentCaptor<Set<DeleteOption>> options = ArgumentCaptor.forClass(Set.class);
        verify(client).delete(eq(ELECTION_ROOT), options.capture());
        assertEquals(Set.of(DeleteOption.IfVersionIdEquals(OUR_VERSION.versionId())), options.getValue());
    }

    @Test
    void closeCompletesFollowerCallbackBeforeReleasingElectionRecord() {
        stubAcquireLeadership();
        AtomicBoolean followerCallbackCompleted = new AtomicBoolean();
        AtomicBoolean deleteObservedCompletedCallback = new AtomicBoolean();
        when(client.delete(eq(ELECTION_ROOT), any())).thenAnswer(invocation -> {
            deleteObservedCompletedCallback.set(followerCallbackCompleted.get());
            return CompletableFuture.completedFuture(null);
        });
        LeaderElectionService service = newService(leading -> {
            if (!leading) {
                followerCallbackCompleted.set(true);
            }
        });
        service.start();

        service.close();

        assertTrue(followerCallbackCompleted.get());
        assertTrue(deleteObservedCompletedCallback.get());
    }

    @Test
    void listenerErrorOnLeadershipLossIsReconciledWhenLeadershipIsReacquired() {
        GetResult otherLeader = new GetResult(
                ELECTION_ROOT, "compactor-b".getBytes(StandardCharsets.UTF_8), OTHER_VERSION);
        when(client.get(ELECTION_ROOT)).thenReturn(
                CompletableFuture.completedFuture(null),
                CompletableFuture.completedFuture(otherLeader),
                CompletableFuture.completedFuture(null));
        when(client.put(eq(ELECTION_ROOT), any(byte[].class), eq(acquireOptions())))
                .thenReturn(
                        CompletableFuture.completedFuture(new PutResult(ELECTION_ROOT, OUR_VERSION)),
                        CompletableFuture.completedFuture(new PutResult(ELECTION_ROOT, version(29))));
        List<Boolean> transitions = new ArrayList<>();
        AtomicInteger followerAttempts = new AtomicInteger();
        LeaderElectionService service = newService(leading -> {
            transitions.add(leading);
            if (!leading && followerAttempts.getAndIncrement() == 0) {
                throw new AssertionError("follower runner shutdown failed");
            }
        });
        service.start();
        Runnable scheduledRefresh = captureScheduledRefresh();

        assertDoesNotThrow(scheduledRefresh::run);

        assertFalse(service.isLeader());
        assertEquals(List.of(true, false), transitions);
        assertEquals("compactor-b", service.getCurrentLeader().orElseThrow().getHostname());

        assertDoesNotThrow(scheduledRefresh::run);

        assertTrue(service.isLeader());
        assertEquals(List.of(true, false, true), transitions);
        service.close();
        verify(client).delete(eq(ELECTION_ROOT), any());
    }

    @Test
    void lateRefreshCannotPromoteAfterClose() throws Exception {
        GetResult ourLeader = new GetResult(
                ELECTION_ROOT, HOSTNAME.getBytes(StandardCharsets.UTF_8), OUR_VERSION);
        CompletableFuture<GetResult> blockedRead = new CompletableFuture<>();
        CountDownLatch refreshStarted = new CountDownLatch(1);
        when(client.get(ELECTION_ROOT)).thenReturn(CompletableFuture.completedFuture(null))
                .thenAnswer(invocation -> {
                    refreshStarted.countDown();
                    return blockedRead;
                });
        when(client.put(eq(ELECTION_ROOT), any(byte[].class), eq(acquireOptions())))
                .thenReturn(CompletableFuture.completedFuture(new PutResult(ELECTION_ROOT, OUR_VERSION)));
        List<Boolean> transitions = new ArrayList<>();
        LeaderElectionService service = newService(transitions::add);
        service.start();
        Runnable scheduledRefresh = captureScheduledRefresh();
        CompletableFuture<Void> inFlightRefresh = CompletableFuture.runAsync(scheduledRefresh);
        assertTrue(refreshStarted.await(5, TimeUnit.SECONDS));

        service.close();
        blockedRead.complete(ourLeader);
        inFlightRefresh.get(5, TimeUnit.SECONDS);

        assertFalse(service.isLeader());
        assertEquals(List.of(true, false), transitions);
        verify(executor).shutdownNow();
    }

    @Test
    void blockedInitialReadCannotAcquireLeadershipAfterClose() throws Exception {
        CompletableFuture<GetResult> blockedRead = new CompletableFuture<>();
        CountDownLatch refreshStarted = new CountDownLatch(1);
        when(client.get(ELECTION_ROOT)).thenAnswer(invocation -> {
            refreshStarted.countDown();
            return blockedRead;
        });
        LeaderElectionService service = newService(leading -> { });
        CompletableFuture<Void> start = CompletableFuture.runAsync(service::start);
        assertTrue(refreshStarted.await(5, TimeUnit.SECONDS));

        service.close();
        blockedRead.complete(null);
        start.get(5, TimeUnit.SECONDS);

        assertFalse(service.isLeader());
        verify(client, never()).put(eq(ELECTION_ROOT), any(byte[].class), eq(acquireOptions()));
        verify(executor, never()).scheduleWithFixedDelay(
                any(Runnable.class), eq(2L), eq(2L), eq(TimeUnit.SECONDS));
    }

    @Test
    void directRefreshErrorDoesNotPreventSchedulingOrRecovery() {
        AssertionError readFailure = new AssertionError("Oxia client linkage failure");
        when(client.get(ELECTION_ROOT))
                .thenThrow(readFailure)
                .thenReturn(CompletableFuture.completedFuture(null));
        when(client.put(eq(ELECTION_ROOT), any(byte[].class), eq(acquireOptions())))
                .thenReturn(CompletableFuture.completedFuture(new PutResult(ELECTION_ROOT, OUR_VERSION)));
        LeaderElectionService service = newService(leading -> { });

        assertDoesNotThrow(service::start);
        assertFalse(service.isLeader());
        Runnable scheduledRefresh = captureScheduledRefresh();

        assertDoesNotThrow(scheduledRefresh::run);
        assertTrue(service.isLeader());
        service.close();
    }

    @Test
    void refreshFailureDoesNotEscapeOrStopFutureRefreshes() {
        RuntimeException readFailure = new RuntimeException("Oxia unavailable");
        when(client.get(ELECTION_ROOT)).thenReturn(
                CompletableFuture.completedFuture(null),
                CompletableFuture.failedFuture(readFailure),
                CompletableFuture.completedFuture(null));
        when(client.put(eq(ELECTION_ROOT), any(byte[].class), eq(acquireOptions())))
                .thenReturn(CompletableFuture.completedFuture(new PutResult(ELECTION_ROOT, OUR_VERSION)));
        LeaderElectionService service = newService(leading -> { });
        service.start();
        Runnable scheduledRefresh = captureScheduledRefresh();

        assertDoesNotThrow(scheduledRefresh::run);
        assertFalse(service.isLeader());
        assertDoesNotThrow(scheduledRefresh::run);
        assertTrue(service.isLeader());
        service.close();
    }

    @Test
    void closeAlwaysCancelsAndShutsDownWhenDeleteAndListenerFail() {
        stubAcquireLeadership();
        when(client.delete(eq(ELECTION_ROOT), any()))
                .thenReturn(CompletableFuture.failedFuture(new RuntimeException("delete failed")));
        List<Boolean> transitions = new ArrayList<>();
        LeaderElectionService service = newService(leading -> {
            transitions.add(leading);
            if (!leading) {
                throw new AssertionError("follower runner shutdown failed");
            }
        });
        service.start();

        assertDoesNotThrow(service::close);

        assertFalse(service.isLeader());
        assertEquals(List.of(true, false), transitions);
        verify(refreshFuture).cancel(true);
        verify(executor).shutdownNow();
    }

    @Test
    void closeDeletesOnlyTheVersionItAcquired() {
        stubAcquireLeadership();
        LeaderElectionService service = newService(leading -> { });
        service.start();

        service.close();

        ArgumentCaptor<Set<DeleteOption>> options = ArgumentCaptor.forClass(Set.class);
        verify(client).delete(eq(ELECTION_ROOT), options.capture());
        assertEquals(Set.of(DeleteOption.IfVersionIdEquals(OUR_VERSION.versionId())), options.getValue());
    }

    private void stubAcquireLeadership() {
        when(client.get(ELECTION_ROOT)).thenReturn(CompletableFuture.completedFuture(null));
        when(client.put(eq(ELECTION_ROOT), any(byte[].class), eq(acquireOptions())))
                .thenReturn(CompletableFuture.completedFuture(new PutResult(ELECTION_ROOT, OUR_VERSION)));
    }

    private LeaderElectionService newService(Consumer<Boolean> listener) {
        return new LeaderElectionService(
                client, HOSTNAME, ELECTION_ROOT, listener, InstrumentProvider.NOOP, executor);
    }

    private Runnable captureScheduledRefresh() {
        ArgumentCaptor<Runnable> task = ArgumentCaptor.forClass(Runnable.class);
        verify(executor).scheduleWithFixedDelay(
                task.capture(), eq(2L), eq(2L), eq(TimeUnit.SECONDS));
        return task.getValue();
    }

    private static Set<PutOption> acquireOptions() {
        return Set.of(PutOption.AsEphemeralRecord, PutOption.IfRecordDoesNotExist);
    }

    private static Version version(long versionId) {
        return new Version(versionId, 0, 0, 0, Optional.empty(), Optional.empty());
    }
}
