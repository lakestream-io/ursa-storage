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
        CompletableFuture<Void> result = new CompletableFuture<>();
        AtomicInteger next = new AtomicInteger();
        AtomicInteger remaining = new AtomicInteger(count);
        AtomicReference<Throwable> firstFailure = new AtomicReference<>();
        Runnable[] launch = new Runnable[1];
        launch[0] = () -> {
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
                launch[0].run();
            });
        };
        for (int i = 0; i < Math.min(limit, count); i++) {
            launch[0].run();
        }
        return result;
    }

    private static Throwable unwrap(Throwable failure) {
        return failure instanceof CompletionException && failure.getCause() != null
            ? failure.getCause() : failure;
    }
}
