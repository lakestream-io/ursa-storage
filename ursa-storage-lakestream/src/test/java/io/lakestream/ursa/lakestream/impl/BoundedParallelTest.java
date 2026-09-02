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
        CompletableFuture<Void> failingGate = new CompletableFuture<>();
        CompletableFuture<Void> all = BoundedParallel.forEach(3, 3, i -> {
            invocations.incrementAndGet();
            return i == 1 ? failingGate : CompletableFuture.completedFuture(null);
        });
        // Every index runs even though one of them is destined to fail.
        assertEquals(3, invocations.get());
        // The run stays pending while the failing body is still in flight.
        assertFalse(all.isDone());
        failingGate.completeExceptionally(new IllegalStateException("boom"));
        ExecutionException failure = assertThrows(ExecutionException.class, () -> all.get(10, TimeUnit.SECONDS));
        assertEquals("boom", failure.getCause().getMessage());
        assertEquals(3, invocations.get());
    }

    @Test
    void failureIsReportedOnlyAfterASlowerBodySettles() {
        AtomicInteger invocations = new AtomicInteger();
        CompletableFuture<Void> straggler = new CompletableFuture<>();
        CompletableFuture<Void> all = BoundedParallel.forEach(3, 3, i -> {
            invocations.incrementAndGet();
            if (i == 0) {
                return CompletableFuture.failedFuture(new IllegalStateException("boom"));
            }
            return i == 1 ? straggler : CompletableFuture.completedFuture(null);
        });
        assertEquals(3, invocations.get());
        // The failure is already recorded, but the run must not settle while work is in flight.
        assertFalse(all.isDone());
        straggler.complete(null);
        ExecutionException failure = assertThrows(ExecutionException.class, () -> all.get(10, TimeUnit.SECONDS));
        assertEquals("boom", failure.getCause().getMessage());
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
