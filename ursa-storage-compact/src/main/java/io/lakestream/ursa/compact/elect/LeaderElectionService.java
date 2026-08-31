/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.compact.elect;

import io.lakestream.ursa.metrics.InstrumentProvider;
import io.lakestream.ursa.metrics.Unit;
import io.opentelemetry.api.metrics.ObservableLongMeasurement;
import io.oxia.client.api.AsyncOxiaClient;
import io.oxia.client.api.GetResult;
import io.oxia.client.api.exceptions.KeyAlreadyExistsException;
import io.oxia.client.api.exceptions.UnexpectedVersionIdException;
import io.oxia.client.api.options.DeleteOption;
import io.oxia.client.api.options.PutOption;
import java.nio.charset.StandardCharsets;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import lombok.extern.slf4j.Slf4j;

/** Cluster leader election backed directly by an ephemeral Oxia record. */
@Slf4j
public class LeaderElectionService implements AutoCloseable {

    private static final String ELECTION_ROOT = "/compact/leader";
    private static final long REFRESH_INTERVAL_SECONDS = 2;

    private final AsyncOxiaClient client;
    private final String hostname;
    private final String electionRoot;
    private final Consumer<Boolean> listener;
    private final ScheduledExecutorService executor;
    private final AtomicBoolean leader = new AtomicBoolean();
    private final AtomicBoolean closed = new AtomicBoolean();
    // Guarded by updateLeadership. Tracks the state successfully applied by the listener, which
    // can lag the observed Oxia ownership when runner startup or shutdown fails. A failed callback
    // can have partial side effects, so listenerStateKnown forces the next refresh to reconcile it.
    private boolean listenerLeadership;
    private boolean listenerStateKnown = true;
    private volatile Optional<CompactLeader> currentLeader = Optional.empty();
    private volatile long versionId = -1;
    private volatile long relinquishingVersionId = -1;
    private volatile ScheduledFuture<?> refreshFuture;

    public LeaderElectionService(AsyncOxiaClient client,
                                 String hostname,
                                 Consumer<Boolean> listener,
                                 InstrumentProvider provider) {
        this(client, hostname, ELECTION_ROOT, listener, provider);
    }

    public LeaderElectionService(AsyncOxiaClient client, String hostname, String electionRoot,
                                 Consumer<Boolean> listener, InstrumentProvider provider) {
        this(client, hostname, electionRoot, listener, provider,
                Executors.newSingleThreadScheduledExecutor(runnable -> {
                    Thread thread = new Thread(runnable, "compact-leader-election");
                    thread.setDaemon(true);
                    return thread;
                }));
    }

    LeaderElectionService(AsyncOxiaClient client, String hostname, String electionRoot,
                          Consumer<Boolean> listener, InstrumentProvider provider,
                          ScheduledExecutorService executor) {
        this.client = client;
        this.hostname = hostname;
        this.electionRoot = electionRoot;
        this.listener = listener;
        this.executor = executor;
        provider.getMeter()
                .gaugeBuilder("compaction_cluster_leaders_total")
                .setDescription("Total number of leaders in the compaction service cluster")
                .setUnit(Unit.None.toString())
                .ofLongs()
                .buildWithCallback(this::recordLeaderCount);
    }

    private void recordLeaderCount(ObservableLongMeasurement measurement) {
        measurement.record(isLeader() ? 1L : 0L);
    }

    public void start() {
        refreshLeadership();
        if (closed.get()) {
            return;
        }
        refreshFuture = executor.scheduleWithFixedDelay(
                this::refreshLeadership,
                REFRESH_INTERVAL_SECONDS,
                REFRESH_INTERVAL_SECONDS,
                TimeUnit.SECONDS);
    }

    private void refreshLeadership() {
        if (closed.get()) {
            return;
        }
        try {
            GetResult existing = client.get(electionRoot).join();
            if (closed.get()) {
                return;
            }
            if (retryPendingLeadershipRelinquish(existing)) {
                return;
            }
            if (isOurRecord(existing)) {
                if (!updateLeadership(true, Optional.of(new CompactLeader(hostname)))) {
                    relinquishFailedLeadership(versionId);
                }
                return;
            }
            if (existing != null) {
                updateLeadership(false, decodeLeader(existing));
                return;
            }
            tryAcquire();
        } catch (Throwable failure) {
            recordRefreshFailure(failure);
            if (!closed.get()) {
                updateLeadership(false, Optional.empty());
            }
        }
    }

    private boolean isOurRecord(GetResult result) {
        return result != null && versionId >= 0 && result.version().versionId() == versionId;
    }

    private void tryAcquire() {
        try {
            if (closed.get()) {
                return;
            }
            var result = client.put(
                    electionRoot,
                    hostname.getBytes(StandardCharsets.UTF_8),
                    Set.of(PutOption.AsEphemeralRecord, PutOption.IfRecordDoesNotExist)).join();
            versionId = result.version().versionId();
            if (closed.get()) {
                markLeadershipRelinquishing(versionId);
                tryReleaseElectionRecord(versionId);
                return;
            }
            if (!updateLeadership(true, Optional.of(new CompactLeader(hostname)))) {
                relinquishFailedLeadership(versionId);
            }
        } catch (RuntimeException e) {
            Throwable cause = unwrap(e);
            if (!(cause instanceof KeyAlreadyExistsException)) {
                log.warn("Failed to acquire compaction leadership", cause);
            }
            updateLeadership(false, readCurrentLeader().join());
        }
    }

    private synchronized boolean updateLeadership(
            boolean leading, Optional<CompactLeader> observedLeader) {
        if (leading && closed.get()) {
            leader.set(false);
            return false;
        }
        currentLeader = observedLeader;
        leader.set(leading);
        if (listenerStateKnown && listenerLeadership == leading) {
            return true;
        }
        try {
            listener.accept(leading);
            listenerLeadership = leading;
            listenerStateKnown = true;
            return true;
        } catch (Throwable failure) {
            // Starting leader-only runners is part of becoming locally usable as leader. If it
            // fails, keep the successfully-applied listener state unchanged and report this node
            // as a follower until a later refresh retries the same observed Oxia ownership.
            if (leading) {
                leader.set(false);
            }
            listenerStateKnown = false;
            try {
                log.error("Compaction leadership listener failed while transitioning to {}; "
                                + "the transition will be retried on the next refresh",
                        leading ? "leader" : "follower", failure);
            } catch (Throwable observabilityFailure) {
                addSuppressed(failure, observabilityFailure);
            }
            return false;
        }
    }

    /**
     * Gives up an Oxia claim whose leader-only runners could not be started.
     *
     * <p>Keeping the ephemeral record while only retrying the local listener would make this
     * unhealthy process the permanent cluster leader and prevent a healthy peer from taking over.
     * Reconcile any partial listener side effects first, then delete only the exact version this
     * process acquired.
     */
    private void relinquishFailedLeadership(long failedVersionId) {
        leader.set(false);
        markLeadershipRelinquishing(failedVersionId);
        if (updateLeadership(false, Optional.empty())) {
            tryReleaseElectionRecord(failedVersionId);
        }
    }

    /**
     * Retries an exact-version release without promoting the locally unusable owner again.
     *
     * <p>A missing key or a different version proves that the pending claim is already gone. When
     * the same version remains, follower reconciliation must succeed before its record is exposed
     * to a peer; otherwise partially stopped leader work could overlap its successor.
     */
    private boolean retryPendingLeadershipRelinquish(GetResult existing) {
        long pendingVersionId = relinquishingVersionId;
        if (pendingVersionId < 0) {
            return false;
        }
        if (existing == null || existing.version().versionId() != pendingVersionId) {
            clearLeadershipClaim(pendingVersionId);
            if (existing != null) {
                updateLeadership(false, decodeLeader(existing));
            }
            return true;
        }
        leader.set(false);
        if (updateLeadership(false, Optional.empty())) {
            tryReleaseElectionRecord(pendingVersionId);
        }
        return true;
    }

    private synchronized void markLeadershipRelinquishing(long recordVersionId) {
        if (recordVersionId >= 0 && versionId == recordVersionId) {
            relinquishingVersionId = recordVersionId;
        }
    }

    private synchronized void clearLeadershipClaim(long recordVersionId) {
        if (relinquishingVersionId == recordVersionId) {
            relinquishingVersionId = -1L;
        }
        if (versionId == recordVersionId) {
            versionId = -1L;
        }
    }

    private void recordRefreshFailure(Throwable failure) {
        try {
            log.warn("Failed to refresh compaction leadership", unwrap(failure));
        } catch (Throwable observabilityFailure) {
            addSuppressed(failure, observabilityFailure);
        }
    }

    private boolean tryReleaseElectionRecord(long recordVersionId) {
        try {
            client.delete(electionRoot,
                    Set.of(DeleteOption.IfVersionIdEquals(recordVersionId))).join();
            clearLeadershipClaim(recordVersionId);
            return true;
        } catch (Throwable failure) {
            Throwable cause = unwrap(failure);
            if (cause instanceof UnexpectedVersionIdException) {
                clearLeadershipClaim(recordVersionId);
                return true;
            }
            try {
                log.warn("Failed to release compaction leadership record version {}; "
                                + "the exact claim remains pending for retry",
                        recordVersionId, cause);
            } catch (Throwable observabilityFailure) {
                addSuppressed(failure, observabilityFailure);
            }
            return false;
        }
    }

    private static void addSuppressed(Throwable primary, Throwable secondary) {
        if (primary != secondary) {
            primary.addSuppressed(secondary);
        }
    }

    private static Optional<CompactLeader> decodeLeader(GetResult result) {
        if (result == null) {
            return Optional.empty();
        }
        return Optional.of(new CompactLeader(new String(result.value(), StandardCharsets.UTF_8)));
    }

    private static Throwable unwrap(Throwable error) {
        Throwable current = error;
        while (current instanceof CompletionException && current.getCause() != null) {
            current = current.getCause();
        }
        return current;
    }

    @Override
    public void close() {
        if (!closed.compareAndSet(false, true)) {
            return;
        }
        try {
            if (refreshFuture != null) {
                try {
                    refreshFuture.cancel(true);
                } catch (Throwable cancelFailure) {
                    recordRefreshFailure(cancelFailure);
                }
            }
        } finally {
            boolean followerReady = false;
            try {
                // Stop this node's leader-only work before making the election record available
                // to a successor. If the callback cannot prove that local work stopped, retain the
                // claim until the scheduler shuts down the Oxia session instead of allowing an
                // overlapping successor.
                followerReady = updateLeadership(false, Optional.empty());
            } finally {
                try {
                    // A failed listener startup deliberately reports this node as a follower, but
                    // the ephemeral record acquired immediately before the callback still belongs
                    // to this service. Delete it only after follower reconciliation has completed;
                    // a stale version cannot delete a successor's record.
                    long recordVersionId = relinquishingVersionId >= 0
                            ? relinquishingVersionId : versionId;
                    if (followerReady && recordVersionId >= 0) {
                        markLeadershipRelinquishing(recordVersionId);
                        tryReleaseElectionRecord(recordVersionId);
                    } else if (!followerReady && recordVersionId >= 0) {
                        log.warn("Retaining compaction leadership record version {} because "
                                        + "leader-only work did not stop cleanly; closing the Oxia "
                                        + "session will provide the final release",
                                recordVersionId);
                    }
                } finally {
                    executor.shutdownNow();
                }
            }
        }
    }

    public CompletableFuture<Optional<CompactLeader>> readCurrentLeader() {
        return client.get(electionRoot).thenApply(LeaderElectionService::decodeLeader);
    }

    public Optional<CompactLeader> getCurrentLeader() {
        return currentLeader;
    }

    public boolean isLeader() {
        return leader.get();
    }
}
