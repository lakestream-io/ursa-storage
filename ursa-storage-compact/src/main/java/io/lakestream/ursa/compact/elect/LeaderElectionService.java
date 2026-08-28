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
    private volatile Optional<CompactLeader> currentLeader = Optional.empty();
    private volatile long versionId = -1;
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
            if (isOurRecord(existing)) {
                updateLeadership(true, Optional.of(new CompactLeader(hostname)));
                return;
            }
            if (existing != null) {
                updateLeadership(false, decodeLeader(existing));
                return;
            }
            tryAcquire();
        } catch (RuntimeException e) {
            log.warn("Failed to refresh compaction leadership", unwrap(e));
            updateLeadership(false, Optional.empty());
        }
    }

    private boolean isOurRecord(GetResult result) {
        return result != null && versionId >= 0 && result.version().versionId() == versionId;
    }

    private void tryAcquire() {
        try {
            var result = client.put(
                    electionRoot,
                    hostname.getBytes(StandardCharsets.UTF_8),
                    Set.of(PutOption.AsEphemeralRecord, PutOption.IfRecordDoesNotExist)).join();
            versionId = result.version().versionId();
            updateLeadership(true, Optional.of(new CompactLeader(hostname)));
        } catch (RuntimeException e) {
            Throwable cause = unwrap(e);
            if (!(cause instanceof KeyAlreadyExistsException)) {
                log.warn("Failed to acquire compaction leadership", cause);
            }
            updateLeadership(false, readCurrentLeader().join());
        }
    }

    private void updateLeadership(boolean leading, Optional<CompactLeader> observedLeader) {
        currentLeader = observedLeader;
        boolean changed = leader.getAndSet(leading) != leading;
        if (changed) {
            try {
                listener.accept(leading);
            } catch (RuntimeException e) {
                log.error("Compaction leadership listener failed while transitioning to {}",
                        leading ? "leader" : "follower", e);
            }
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
                refreshFuture.cancel(true);
            }
            if (leader.get() && versionId >= 0) {
                client.delete(electionRoot, Set.of(DeleteOption.IfVersionIdEquals(versionId))).join();
            }
        } catch (RuntimeException e) {
            log.debug("Leadership record was already released", unwrap(e));
        } finally {
            updateLeadership(false, Optional.empty());
            executor.shutdownNow();
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
