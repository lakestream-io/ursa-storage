/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.lakestream.impl;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class BoundedParallelTest {

    @Test
    void runsAtMostLimitBodiesConcurrentlyAndCompletesAll() throws Exception {
        AtomicInteger active = new AtomicInteger();
        AtomicInteger maxActive = new AtomicInteger();
        List<CompletableFuture<Void>> gates = new CopyOnWriteArrayList<>();
        CompletableFuture<Void> all = BoundedParallel.forEach(10, 3, i -> {
            maxActive.accumulateAndGet(active.incrementAndGet(), Math::max);
            CompletableFuture<Void> gate = new CompletableFuture<>();
            gates.add(gate);
            return gate.whenComplete((v, e) -> active.decrementAndGet());
        });
        assertEquals(3, gates.size());
        // Drain by index rather than gates.forEach: CopyOnWriteArrayList iterates a snapshot,
        // so it would miss the gates that each completion launches.
        for (int index = 0; index < 10; index++) {
            gates.get(index).complete(null);
        }
        all.get(10, TimeUnit.SECONDS);
        assertEquals(10, gates.size());
        assertEquals(3, maxActive.get());
    }

    @Test
    void failureOfOneBodyFailsTheWholeRunAfterOthersFinish() {
        AtomicInteger invocations = new AtomicInteger();
        AtomicInteger completions = new AtomicInteger();
        CompletableFuture<Void> failingGate = new CompletableFuture<>();
        CompletableFuture<Void> straggler = new CompletableFuture<>();
        CompletableFuture<Void> all = BoundedParallel.forEach(3, 3, i -> {
            invocations.incrementAndGet();
            CompletableFuture<Void> body = switch (i) {
                case 1 -> failingGate;
                case 2 -> straggler;
                default -> CompletableFuture.completedFuture(null);
            };
            return body.whenComplete((value, failure) -> completions.incrementAndGet());
        });
        // Every index runs because none of them had failed when the launcher reached it.
        assertEquals(3, invocations.get());
        assertFalse(all.isDone());

        failingGate.completeExceptionally(new IllegalStateException("boom"));
        // The failure is recorded, but the run must not settle while a body is still in flight,
        // and no further body may be launched.
        assertFalse(all.isDone());
        assertEquals(3, invocations.get());

        straggler.complete(null);
        ExecutionException failure = assertThrows(ExecutionException.class, () -> all.get(10, TimeUnit.SECONDS));
        assertEquals("boom", failure.getCause().getMessage());
        assertEquals(3, invocations.get());
        // Every launched body ran to completion before the failure propagated.
        assertEquals(3, completions.get());
    }

    @Test
    void aSynchronousFailureStopsTheRunBeforeTheNextBodyStarts() {
        AtomicInteger invocations = new AtomicInteger();
        CompletableFuture<Void> all = BoundedParallel.forEach(3, 3, i -> {
            invocations.incrementAndGet();
            return CompletableFuture.failedFuture(new IllegalStateException("boom"));
        });

        // Index 0 fails before the launcher reaches index 1, so nothing else is attempted.
        assertEquals(1, invocations.get());
        ExecutionException failure = assertThrows(ExecutionException.class, () -> all.get(10, TimeUnit.SECONDS));
        assertEquals("boom", failure.getCause().getMessage());
    }

    @Test
    void firstFailureStopsLaunchingBeyondTheInFlightWindow() {
        List<Integer> started = new CopyOnWriteArrayList<>();
        List<CompletableFuture<Void>> gates = new CopyOnWriteArrayList<>();
        CompletableFuture<Void> all = BoundedParallel.forEach(6, 2, i -> {
            started.add(i);
            CompletableFuture<Void> gate = new CompletableFuture<>();
            gates.add(gate);
            return gate;
        });
        assertEquals(List.of(0, 1), started);

        gates.get(0).completeExceptionally(new IllegalStateException("boom"));
        // Index 2 is never launched, and the run still waits for the body in flight.
        assertEquals(List.of(0, 1), started);
        assertFalse(all.isDone());

        gates.get(1).complete(null);
        ExecutionException failure = assertThrows(ExecutionException.class, () -> all.get(10, TimeUnit.SECONDS));
        assertEquals("boom", failure.getCause().getMessage());
        assertEquals(List.of(0, 1), started);
    }

    @Test
    void largeSynchronousRunDoesNotOverflowTheStack() throws Exception {
        AtomicInteger invocations = new AtomicInteger();
        CompletableFuture<Void> all = BoundedParallel.forEach(200_000, 4, i -> {
            invocations.incrementAndGet();
            return CompletableFuture.completedFuture(null);
        });
        all.get(10, TimeUnit.SECONDS);
        assertEquals(200_000, invocations.get());
    }
}
