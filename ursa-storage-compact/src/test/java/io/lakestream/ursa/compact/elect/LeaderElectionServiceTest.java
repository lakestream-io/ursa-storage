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
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
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
    void listenerFailureDuringInitialAcquireDoesNotPreventRefreshScheduling() {
        stubAcquireLeadership();
        List<Boolean> transitions = new ArrayList<>();
        LeaderElectionService service = newService(failingListener(transitions));

        assertDoesNotThrow(service::start);

        assertTrue(service.isLeader());
        assertEquals(List.of(true), transitions);
        verify(executor).scheduleWithFixedDelay(
                any(Runnable.class), eq(2L), eq(2L), eq(TimeUnit.SECONDS));
        service.close();
    }

    @Test
    void listenerFailureOnLeadershipLossDoesNotEscapeScheduledRefresh() {
        GetResult otherLeader = new GetResult(
                ELECTION_ROOT, "compactor-b".getBytes(StandardCharsets.UTF_8), OTHER_VERSION);
        when(client.get(ELECTION_ROOT)).thenReturn(
                CompletableFuture.completedFuture(null),
                CompletableFuture.completedFuture(otherLeader));
        when(client.put(eq(ELECTION_ROOT), any(byte[].class), eq(acquireOptions())))
                .thenReturn(CompletableFuture.completedFuture(new PutResult(ELECTION_ROOT, OUR_VERSION)));
        List<Boolean> transitions = new ArrayList<>();
        LeaderElectionService service = newService(failingListener(transitions));
        service.start();
        Runnable scheduledRefresh = captureScheduledRefresh();

        assertDoesNotThrow(scheduledRefresh::run);

        assertFalse(service.isLeader());
        assertEquals(List.of(true, false), transitions);
        assertEquals("compactor-b", service.getCurrentLeader().orElseThrow().getHostname());
        service.close();
        verify(client, never()).delete(eq(ELECTION_ROOT), any());
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
        LeaderElectionService service = newService(failingListener(transitions));
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

    private static Consumer<Boolean> failingListener(List<Boolean> transitions) {
        return leading -> {
            transitions.add(leading);
            throw new IllegalStateException("listener failed");
        };
    }

    private static Set<PutOption> acquireOptions() {
        return Set.of(PutOption.AsEphemeralRecord, PutOption.IfRecordDoesNotExist);
    }

    private static Version version(long versionId) {
        return new Version(versionId, 0, 0, 0, Optional.empty(), Optional.empty());
    }
}
