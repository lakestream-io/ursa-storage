/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.lakehouse.utils.lock;

import static io.lakestream.ursa.lakehouse.utils.RunnableUtils.safeExecute;
import static io.lakestream.ursa.lakehouse.utils.RunnableUtils.safeRun;
import static io.lakestream.ursa.utils.lock.AsyncLock.LockStatus.ACQUIRED;
import static io.lakestream.ursa.utils.lock.AsyncLock.LockStatus.ACQUIRING;
import static io.lakestream.ursa.utils.lock.AsyncLock.LockStatus.INIT;
import static io.lakestream.ursa.utils.lock.AsyncLock.LockStatus.RELEASED;
import static io.lakestream.ursa.utils.lock.AsyncLock.LockStatus.RELEASING;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.concurrent.CompletableFuture.failedFuture;
import static java.util.concurrent.CompletableFuture.runAsync;

import com.google.common.base.Throwables;
import io.grpc.netty.shaded.io.netty.util.internal.PlatformDependent;
import io.grpc.netty.shaded.io.netty.util.internal.shaded.org.jctools.queues.MessagePassingQueue;
import io.lakestream.ursa.utils.lock.AsyncLock;
import io.lakestream.ursa.utils.lock.NotificationReceiver;
import io.lakestream.ursa.utils.lock.OptionAutoRevalidate;
import io.lakestream.ursa.utils.lock.exception.LockException;
import io.oxia.client.api.AsyncOxiaClient;
import io.oxia.client.api.Notification;
import io.oxia.client.api.exceptions.KeyAlreadyExistsException;
import io.oxia.client.api.exceptions.UnexpectedVersionIdException;
import io.oxia.client.api.options.DeleteOption;
import io.oxia.client.api.options.PutOption;
import io.oxia.client.api.options.defs.OptionVersionId;
import io.oxia.client.util.Backoff;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import javax.annotation.concurrent.ThreadSafe;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@ThreadSafe
final class SharedSimpleLock implements AsyncLock, NotificationReceiver {

    public static final Class<? extends Throwable>[] DEFAULT_RETRYABLE_EXCEPTIONS =
            new Class[] {LockException.LockBusyException.class};
    private static final byte[] DEFAULT_VALUE = new byte[0];
    private static final AtomicReferenceFieldUpdater<SharedSimpleLock, LockStatus> STATE_UPDATER =
            AtomicReferenceFieldUpdater.newUpdater(SharedSimpleLock.class, LockStatus.class, "state");

    private final AsyncOxiaClient client;
    private final String key;
    private final Backoff backoff;
    private final Set<String> retryableExceptions = new TreeSet<>();
    private final ScheduledExecutorService taskExecutor;
    private volatile ScheduledFuture<?> autoRevalidateFuture;

    @SafeVarargs
    SharedSimpleLock(
            AsyncOxiaClient client,
            String key,
            ScheduledExecutorService executorService,
            Backoff backoff,
            OptionAutoRevalidate optionAutoRevalidate,
            Class<? extends Throwable>... retryableExceptions) {
        this.client = client;
        this.key = key;
        this.state = INIT;
        this.backoff = backoff;
        this.taskExecutor = executorService;
        for (Class<? extends Throwable> retryableException : retryableExceptions) {
            this.retryableExceptions.add(retryableException.getName());
        }
        if (optionAutoRevalidate.enabled()) {
            autoRevalidateFuture = taskExecutor.scheduleWithFixedDelay(
                    () -> safeRun(log, () -> {
                        maybeCleanupRevalidation();
                        notifyStateChanged(null);
                    }),
                    optionAutoRevalidate.initDelay(),
                    optionAutoRevalidate.delay(),
                    optionAutoRevalidate.unit());
        }
    }

    private volatile LockStatus state;
    private volatile long versionId;

    @SuppressWarnings("OptionalUsedAsFieldOrParameterType")
    private volatile Optional<Long> sessionId;

    private static Throwable unwrapCompletionException(Throwable error) {
        Throwable current = error;
        while (current instanceof CompletionException && current.getCause() != null) {
            current = current.getCause();
        }
        return current;
    }

    private static CompletionException wrapToCompletionException(Throwable error) {
        return error instanceof CompletionException completionException
                ? completionException
                : new CompletionException(error);
    }

    @Override
    public LockStatus getStatus() {
        return STATE_UPDATER.get(this);
    }

    @Override
    public CompletableFuture<Void> lock() {
        return lock(ForkJoinPool.commonPool());
    }

    @Override
    public CompletableFuture<Void> tryLock() {
        return tryLock(ForkJoinPool.commonPool());
    }

    @Override
    public CompletableFuture<Void> unlock() {
        return unlock(ForkJoinPool.commonPool());
    }

    @Override
    public CompletableFuture<Void> lock(ExecutorService callbackService) {
        final CompletableFuture<Void> f = new CompletableFuture<>();
        spinLock(callbackService, taskExecutor, f);
        return f;
    }

    private void spinLock(
            ExecutorService callbackService,
            ScheduledExecutorService taskService,
            CompletableFuture<Void> callback) {
        tryLock(callbackService)
                .whenComplete(
                        (r, err) -> {
                            if (err != null) {
                                final Throwable rc = unwrapCompletionException(err);
                                if (retryableExceptions.contains(rc.getClass().getName())) {
                                    final long ndm = backoff.nextDelayMillis();
                                    if (log.isDebugEnabled()) {
                                        log.debug(
                                                "Acquiring Lock failed, retrying... after {} million seconds."
                                                + " key={} session={}",
                                                ndm,
                                                key,
                                                sessionId);
                                    }
                                    // retry later
                                    taskService.schedule(
                                            () -> spinLock(callbackService, taskService, callback),
                                            ndm,
                                            TimeUnit.MILLISECONDS);
                                } else {
                                    callback.completeExceptionally(err);
                                }
                            } else {
                                callback.complete(null);
                            }
                        });
    }

    @Override
    public CompletableFuture<Void> tryLock(ExecutorService callbackService) {
        while (true) {
            final LockStatus status = STATE_UPDATER.get(this);
            if (status == INIT) {
                // put the future here to ensure wait status MUST initialize future
                if (!STATE_UPDATER.compareAndSet(this, INIT, ACQUIRING)) {
                    continue;
                }
                return tryLock1(OptionVersionId.KEY_NOT_EXISTS)
                        // don't forget switch thread context
                        .thenAcceptAsync(__ -> {}, callbackService);
            } else if (status == ACQUIRED) {
                return runAsync(
                        () -> {
                            // switch to callback thread here
                            throw wrapToCompletionException(new LockException
                                    .IllegalLockStatusException(INIT, ACQUIRED));
                        },
                        callbackService);
            } else if (status == ACQUIRING) {
                return runAsync(
                        () -> {
                            // switch to callback thread here
                            throw wrapToCompletionException(new LockException
                                    .IllegalLockStatusException(INIT, ACQUIRING));
                        },
                        callbackService);
            } else if (status == RELEASING) {
                return runAsync(
                        () -> {
                            // switch to callback thread here
                            throw wrapToCompletionException(new LockException
                                    .IllegalLockStatusException(INIT, RELEASING));
                        },
                        callbackService);
            } else if (status == RELEASED) {
                STATE_UPDATER.set(this, INIT);
            } else {
                return runAsync(
                        () -> {
                            // switch to callback thread here
                            throw wrapToCompletionException(new LockException
                                    .UnknownLockStatusException(status));
                        },
                        callbackService);
            }
        }
    }

    private CompletableFuture<Void> tryLock1(long version) {
        final PutOption versionOption =
                version == OptionVersionId.KEY_NOT_EXISTS
                        ? PutOption.IfRecordDoesNotExist
                        : PutOption.IfVersionIdEquals(versionId);
        return client
                .put(key, DEFAULT_VALUE, Set.of(PutOption.AsEphemeralRecord, versionOption))
                .thenAccept(
                        result -> {
                            SharedSimpleLock.this.versionId = result.version().versionId();
                            SharedSimpleLock.this.sessionId = result.version().sessionId();
                            if (log.isDebugEnabled()) {
                                log.debug("Acquired Lock. key={} session={}", key, sessionId);
                            }
                            STATE_UPDATER.set(this, ACQUIRED);
                        })
                .exceptionally(
                        ex -> {
                            final Throwable rc = unwrapCompletionException(ex);
                            final LockException lockE;
                            if (rc instanceof UnexpectedVersionIdException
                                    || rc instanceof KeyAlreadyExistsException) {
                                lockE = new LockException.LockBusyException();
                            } else {
                                lockE = LockException.wrap(ex);
                            }
                            // ensure status rollback after exceptional future complete
                            STATE_UPDATER.set(this, RELEASED);
                            throw wrapToCompletionException(lockE);
                        });
    }

    @Override
    public CompletableFuture<Void> unlock(ExecutorService callbackService) {
        final CompletableFuture<Void> f = new CompletableFuture<>();
        tryUnlock(callbackService)
                .whenComplete((r, err) -> {
                    if (err == null) {
                        f.complete(null);
                        return;
                    }
                    spinUnlock(callbackService, f, err);
                });
        return f;
    }

    private void spinUnlock(ExecutorService callbackService,
                            CompletableFuture<Void> callback,
                            Throwable error) {
        final Throwable rc = unwrapCompletionException(error);
        final long ndm = backoff.nextDelayMillis();
        log.warn(
                "Unlock failed, retrying... after {} milliseconds."
                        + " key={} session={}",
                ndm,
                key,
                sessionId, Throwables.getRootCause(rc));
        // retry later
        taskExecutor.schedule(
                () -> {
                    safeRun(log, () -> unlock0(callbackService)
                            .whenComplete((r, err) -> {
                                        if (err == null) {
                                            callback.complete(null);
                                            return;
                                        }
                                        spinUnlock(callbackService, callback, err);
                                    }
                            ));
                },
                ndm,
                TimeUnit.MILLISECONDS);

    }

    private CompletableFuture<Void> tryUnlock(ExecutorService executorService) {
        while (true) {
            switch (STATE_UPDATER.get(this)) {
                case INIT -> {
                    return runAsync(
                            () -> {
                                throw wrapToCompletionException(new LockException
                                        .IllegalLockStatusException(ACQUIRED, INIT));
                            },
                            executorService);
                }
                case ACQUIRING -> {
                    if (log.isDebugEnabled()) {
                        log.debug("busy wait for acquiring. it should be happened very rare.");
                    }
                    return runAsync(
                            () -> {
                                throw wrapToCompletionException(new LockException
                                        .IllegalLockStatusException(ACQUIRED, ACQUIRING));
                            },
                            executorService);
                }
                case ACQUIRED -> {
                    if (!STATE_UPDATER.compareAndSet(this, ACQUIRED, RELEASING)) {
                        if (log.isDebugEnabled()) {
                            log.debug("busy wait. expect: acquired, actual: {}", STATE_UPDATER.get(this));
                        }
                        try {
                            waitForAWhile();
                        } catch (Throwable ex) {
                            return failedFuture(ex);
                        }
                        continue;
                    }
                    if (log.isDebugEnabled()) {
                        log.debug(
                                "Releasing Lock by unlock." + " key={} session={} step={}", key, sessionId, ACQUIRED);
                    }
                    return unlock0(executorService);
                }
                case RELEASING -> {
                    return runAsync(
                            () -> {
                                throw wrapToCompletionException(new LockException
                                        .IllegalLockStatusException(ACQUIRED, RELEASING));
                            },
                            executorService);
                }
                case RELEASED -> {
                    return completedFuture(null);
                }
                default -> {
                    return runAsync(
                            () -> {
                                throw wrapToCompletionException(new LockException.UnknownLockStatusException(state));
                            });
                }
            }
        }
    }


    private static void waitForAWhile() {
        try {
            Thread.sleep(1);
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            throw wrapToCompletionException(LockException.wrap(ex));
        }
    }

    private void maybeCleanupRevalidation() {
        if (STATE_UPDATER.get(SharedSimpleLock.this) == RELEASED && autoRevalidateFuture != null) {
            autoRevalidateFuture.cancel(false);
            autoRevalidateFuture = null;
        }
    }

    private CompletableFuture<Void> unlock0(ExecutorService executorService) {
        return client
                .delete(key, Set.of(DeleteOption.IfVersionIdEquals(versionId)))
                .thenAcceptAsync(
                        result -> {
                            if (log.isDebugEnabled()) {
                                log.debug(
                                        "Released Lock by unlock. key={} session={}", key, sessionId);
                            }
                            SharedSimpleLock.this.versionId = OptionVersionId.KEY_NOT_EXISTS;
                            SharedSimpleLock.this.sessionId = Optional.empty();
                            STATE_UPDATER.set(this, RELEASED);
                        },
                        executorService)
                .exceptionallyAsync(
                        ex -> {
                            final var rc = unwrapCompletionException(ex);
                            if (rc instanceof UnexpectedVersionIdException) {
                                // (1) the lock has been grant by others
                                if (log.isDebugEnabled()) {
                                    log.debug(
                                            "Released Lock by session lost when unlock. key={} session={} ", key,
                                            sessionId);
                                }
                                STATE_UPDATER.set(this, RELEASED);
                                return null;
                            }
                            if (log.isDebugEnabled()) {
                                log.debug(
                                        "unknown issue. key={} session={} ", key, sessionId, rc);
                            }
                            // todo: better error handling
                            throw new CompletionException(rc);
                        },
                        executorService).whenComplete((__, e) -> maybeCleanupRevalidation());
    }

    private void revalidate() {
        // reset
        final List<Notification> notifications = new ArrayList<>();
        revalidateQueue.drain(notifications::add);
        final long currentVersionId = versionId;
        final boolean validSignal =
                notifications.stream()
                        .anyMatch(
                                notification -> {
                                    if (notification instanceof Notification.KeyCreated no
                                            && no.version() <= currentVersionId) {
                                        return false;
                                    }
                                    return !(notification instanceof Notification.KeyModified no)
                                            || no.version() > currentVersionId;
                                });
        if (!validSignal) {
            STATE_UPDATER.set(this, ACQUIRED);
            return;
        }

        if (log.isDebugEnabled()) {
            log.debug(
                    "Acquiring Lock by revalidation. key={} session={}", key, sessionId);
        }
        tryLock1(currentVersionId)
                .thenAccept(
                        __ -> {
                            if (log.isDebugEnabled()) {
                                /* serial revalidation */
                                log.debug("Acquired Lock by revalidation. key={} session={}", key, sessionId);
                            }
                        })
                .exceptionally(
                        ex -> {
                            if (log.isDebugEnabled()) {
                                log.debug(
                                        "Released Lock by revalidation. key={} session={}", key,
                                        sessionId, Throwables.getRootCause(ex));
                            }
                            return null;
                        });
    }

    @SuppressWarnings("unchecked")
    private final MessagePassingQueue<Notification> revalidateQueue =
            (MessagePassingQueue<Notification>) PlatformDependent.newMpscQueue();

    public void notifyStateChanged(Notification notification) {
        switch (STATE_UPDATER.get(this)) {
            case INIT, RELEASING, RELEASED -> {
                // no-op
            }
            case ACQUIRING -> {
                if (notification == null) {
                    return;
                }
                revalidateQueue.offer(notification);
            }
            case ACQUIRED -> {
                revalidateQueue.offer(
                        Objects.requireNonNullElseGet(
                                notification,
                                // mock a notification here to trigger the revalidation
                                () -> new Notification.KeyDeleted(key)));
                if (!STATE_UPDATER.compareAndSet(this, ACQUIRED, ACQUIRING)) {
                    return;
                }
                safeExecute(log, taskExecutor, this::revalidate);
            }
        }
    }

    @Override
    public void close() {
        if (autoRevalidateFuture != null) {
            autoRevalidateFuture.cancel(false);
            autoRevalidateFuture = null;
        }
    }
}
