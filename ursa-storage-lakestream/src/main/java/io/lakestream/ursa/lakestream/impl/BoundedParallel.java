/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.lakestream.impl;

import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.IntFunction;

/** Runs index-addressed asynchronous bodies with a concurrency limit. */
final class BoundedParallel {
    private BoundedParallel() {
    }

    static CompletableFuture<Void> forEach(
            int count, int limit, IntFunction<CompletableFuture<Void>> body) {
        if (count <= 0) {
            return CompletableFuture.completedFuture(null);
        }
        if (limit <= 0) {
            throw new IllegalArgumentException("limit must be positive");
        }
        Run run = new Run(count, body);
        for (int i = 0; i < Math.min(limit, count); i++) {
            run.requestLaunch();
        }
        return run.result;
    }

    private static Throwable unwrap(Throwable failure) {
        return failure instanceof CompletionException && failure.getCause() != null
            ? failure.getCause() : failure;
    }

    /**
     * One {@link #forEach} invocation. Launches are driven by a trampoline so that bodies which
     * settle synchronously do not grow the stack: a completion that wants to start the next index
     * only records the request, and the thread already inside the drain loop performs it.
     */
    private static final class Run {

        private final int count;
        private final IntFunction<CompletableFuture<Void>> body;
        private final CompletableFuture<Void> result = new CompletableFuture<>();
        private final AtomicInteger next = new AtomicInteger();
        private final AtomicInteger remaining;
        private final AtomicInteger pendingLaunches = new AtomicInteger();
        private final AtomicReference<Throwable> firstFailure = new AtomicReference<>();

        private Run(int count, IntFunction<CompletableFuture<Void>> body) {
            this.count = count;
            this.body = body;
            this.remaining = new AtomicInteger(count);
        }

        /**
         * Asks for one more index to be launched. The first caller becomes the drainer and keeps
         * launching until every queued request has been served; later callers hand their request
         * to that drainer and return immediately.
         */
        private void requestLaunch() {
            if (pendingLaunches.incrementAndGet() > 1) {
                return;
            }
            do {
                launchOne();
            } while (pendingLaunches.decrementAndGet() > 0);
        }

        private void launchOne() {
            int index = next.getAndIncrement();
            if (index >= count) {
                return;
            }
            CompletableFuture<Void> step;
            try {
                step = Objects.requireNonNull(body.apply(index), "body returned null");
            } catch (Throwable failure) {
                step = CompletableFuture.failedFuture(failure);
            }
            step.whenComplete((ignored, failure) -> {
                if (failure != null) {
                    firstFailure.compareAndSet(null, unwrap(failure));
                }
                if (remaining.decrementAndGet() == 0) {
                    Throwable recorded = firstFailure.get();
                    if (recorded == null) {
                        result.complete(null);
                    } else {
                        result.completeExceptionally(recorded);
                    }
                    return;
                }
                requestLaunch();
            });
        }
    }
}
