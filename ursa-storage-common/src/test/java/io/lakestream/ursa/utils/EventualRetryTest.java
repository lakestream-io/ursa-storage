/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.utils;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class EventualRetryTest {

    @Test
    void retriesUntilTheWorkFinallySucceeds() throws Exception {
        AtomicInteger attempts = new AtomicInteger();
        CompletableFuture<Void> result = new CompletableFuture<>();

        EventualRetry.start(Runnable::run, "closing something", () -> {
            if (attempts.incrementAndGet() < 3) {
                throw new IllegalStateException("not yet");
            }
            return null;
        }, result);

        result.get(30, TimeUnit.SECONDS);
        assertEquals(3, attempts.get());
    }

    @Test
    void aFailedFutureFromTheWorkIsRetriedLikeAThrownFailure() throws Exception {
        AtomicInteger attempts = new AtomicInteger();
        CompletableFuture<Void> result = new CompletableFuture<>();

        EventualRetry.start(Runnable::run, "closing something", () ->
            attempts.incrementAndGet() < 2
                ? CompletableFuture.failedFuture(new IllegalStateException("not yet"))
                : CompletableFuture.completedFuture(null), result);

        result.get(30, TimeUnit.SECONDS);
        assertEquals(2, attempts.get());
    }

    @Test
    void backoffDoublesPerAttemptAndIsCappedAtTenSeconds() {
        assertEquals(List.of(100L, 200L, 400L, 800L, 1600L, 3200L, 6400L),
            List.of(
                EventualRetry.retryDelayMillis(0), EventualRetry.retryDelayMillis(1),
                EventualRetry.retryDelayMillis(2), EventualRetry.retryDelayMillis(3),
                EventualRetry.retryDelayMillis(4), EventualRetry.retryDelayMillis(5),
                EventualRetry.retryDelayMillis(6)));
        // Past the cap the shift must neither overflow nor exceed ten seconds.
        assertEquals(10_000L, EventualRetry.retryDelayMillis(7));
        assertEquals(10_000L, EventualRetry.retryDelayMillis(Integer.MAX_VALUE));
        assertEquals(100L, EventualRetry.retryDelayMillis(-1));
    }

    @Test
    void givesUpOnceItsExecutorIsShutDown() {
        ExecutorService executor = Executors.newSingleThreadExecutor();
        executor.shutdown();
        AtomicInteger attempts = new AtomicInteger();
        CompletableFuture<Void> result = new CompletableFuture<>();

        // A rejection from a shut-down executor can never succeed on retry. Bounding the wait
        // turns "would retry forever" into a clear failure.
        EventualRetry.start(executor, "closing something", () -> {
            attempts.incrementAndGet();
            return null;
        }, result);

        ExecutionException rejected = assertThrows(ExecutionException.class,
            () -> result.get(5, TimeUnit.SECONDS));
        assertInstanceOf(RejectedExecutionException.class, rejected.getCause());
        assertEquals(0, attempts.get());
    }

    @Test
    void theGiveUpHookOwnsTheResultWhenOneIsSupplied() {
        ExecutorService executor = Executors.newSingleThreadExecutor();
        executor.shutdown();
        AtomicInteger recordedFailures = new AtomicInteger();
        CompletableFuture<Void> result = new CompletableFuture<>();

        EventualRetry.start(executor, "closing something", () -> null, result,
            failure -> recordedFailures.incrementAndGet(),
            rejection -> result.complete(null));

        assertTrue(result.isDone());
        assertEquals(null, result.join());
        assertEquals(1, recordedFailures.get());
    }
}
