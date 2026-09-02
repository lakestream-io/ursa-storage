/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.utils;

import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import lombok.extern.slf4j.Slf4j;

/**
 * Runs cleanup work that has to happen eventually, retrying until it does.
 *
 * <p>Releasing a write lease, closing a delegate, unwinding a half-built data plane: whoever asked
 * for that work has usually already failed and moved on, so nobody is left to retry it by hand.
 * Each attempt runs on the caller's executor, and a failed attempt is retried after a delay that
 * starts at 100 ms, doubles per attempt, and is capped at ten seconds.
 *
 * <p>The one failure that is not retried is a rejection from an executor that is already shut down.
 * No later attempt could ever run, so retrying would spin forever against a permanently rejecting
 * executor; the chain gives up and reports the rejection instead.
 */
@Slf4j
public final class EventualRetry {

    private static final long INITIAL_RETRY_MILLIS = 100L;
    private static final long MAX_RETRY_MILLIS = TimeUnit.SECONDS.toMillis(10);
    /** Any larger shift would overflow; the delay is capped long before it gets here. */
    private static final int MAX_BACKOFF_SHIFT = 30;

    /** One attempt at the work, run on the retry executor. */
    @FunctionalInterface
    public interface Work {

        /**
         * Runs one attempt.
         *
         * @return a future completing when the attempt does, or {@code null} if it already has
         * @throws Exception if the attempt failed outright
         */
        CompletableFuture<Void> run() throws Exception;
    }

    private final Executor executor;
    private final String description;
    private final Work work;
    private final CompletableFuture<Void> result;
    private final Consumer<Throwable> onAttemptFailure;
    private final Consumer<Throwable> onGiveUp;

    private EventualRetry(
            Executor executor, String description, Work work, CompletableFuture<Void> result,
            Consumer<Throwable> onAttemptFailure, Consumer<Throwable> onGiveUp) {
        this.executor = Objects.requireNonNull(executor, "executor");
        this.description = Objects.requireNonNull(description, "description");
        this.work = Objects.requireNonNull(work, "work");
        this.result = Objects.requireNonNull(result, "result");
        this.onAttemptFailure = Objects.requireNonNull(onAttemptFailure, "onAttemptFailure");
        this.onGiveUp = onGiveUp;
    }

    /**
     * Retries {@code work} on {@code executor} until it succeeds, then completes {@code result}.
     *
     * @param description what the work is, for log messages (e.g. "closing log 7")
     */
    public static void start(
            Executor executor, String description, Work work, CompletableFuture<Void> result) {
        start(executor, description, work, result, failure -> { }, null);
    }

    /**
     * As {@link #start(Executor, String, Work, CompletableFuture)}, for callers that have to note
     * every failed attempt or unwind state of their own when the chain gives up.
     *
     * @param onAttemptFailure called with every attempt failure, before the retry decision
     * @param onGiveUp called instead of the default give-up once the executor is shut down, and
     *     responsible for completing {@code result}; {@code null} keeps the default, which fails
     *     {@code result} with the rejection
     */
    public static void start(
            Executor executor, String description, Work work, CompletableFuture<Void> result,
            Consumer<Throwable> onAttemptFailure, Consumer<Throwable> onGiveUp) {
        new EventualRetry(executor, description, work, result, onAttemptFailure, onGiveUp)
            .attempt(0);
    }

    private void attempt(int retryAttempt) {
        try {
            executor.execute(() -> {
                final CompletableFuture<Void> outcome;
                try {
                    outcome = work.run();
                } catch (Throwable failure) {
                    scheduleRetry(retryAttempt, failure);
                    return;
                }
                if (outcome == null) {
                    result.complete(null);
                    return;
                }
                outcome.whenComplete((ignored, failure) -> {
                    if (failure == null) {
                        result.complete(null);
                    } else {
                        scheduleRetry(
                            retryAttempt, FutureUtils.unwrapCompletionException(failure));
                    }
                });
            });
        } catch (Throwable failure) {
            scheduleRetry(retryAttempt, failure);
        }
    }

    private void scheduleRetry(int retryAttempt, Throwable failure) {
        onAttemptFailure.accept(failure);
        if (isShutDownRejection(failure)) {
            if (onGiveUp != null) {
                onGiveUp.accept(failure);
                return;
            }
            log.warn("Giving up on {}: its retry executor is shut down", description, failure);
            result.completeExceptionally(failure);
            return;
        }
        int nextAttempt = retryAttempt == Integer.MAX_VALUE
            ? Integer.MAX_VALUE : retryAttempt + 1;
        long delayMillis = retryDelayMillis(retryAttempt);
        log.warn("Failed while {}; retrying in {} ms (attempt {})",
            description, delayMillis, nextAttempt, failure);
        try {
            CompletableFuture.delayedExecutor(delayMillis, TimeUnit.MILLISECONDS)
                .execute(() -> attempt(nextAttempt));
        } catch (Throwable schedulingFailure) {
            failure.addSuppressed(schedulingFailure);
            result.completeExceptionally(failure);
        }
    }

    private boolean isShutDownRejection(Throwable failure) {
        return failure instanceof RejectedExecutionException
            && executor instanceof ExecutorService service
            && service.isShutdown();
    }

    static long retryDelayMillis(int retryAttempt) {
        int shift = Math.min(Math.max(retryAttempt, 0), MAX_BACKOFF_SHIFT);
        return Math.min(INITIAL_RETRY_MILLIS << shift, MAX_RETRY_MILLIS);
    }
}
