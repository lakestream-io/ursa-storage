/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.lakehouse.utils.lock;

import static io.lakestream.ursa.lakehouse.utils.lock.SharedSimpleLock.DEFAULT_RETRYABLE_EXCEPTIONS;

import com.google.common.annotations.VisibleForTesting;
import io.lakestream.ursa.utils.lock.AsyncLock;
import io.lakestream.ursa.utils.lock.LockManager;
import io.lakestream.ursa.utils.lock.NotificationReceiver;
import io.lakestream.ursa.utils.lock.OptionAutoRevalidate;
import io.lakestream.ursa.utils.lock.OptionBackoff;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.metrics.Meter;
import io.opentelemetry.api.metrics.ObservableLongGauge;
import io.oxia.client.api.AsyncOxiaClient;
import io.oxia.client.api.Notification;
import io.oxia.client.metrics.Unit;
import io.oxia.client.util.Backoff;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import lombok.Getter;


final class LockManagerImpl implements LockManager, Consumer<Notification> {
    private final AsyncOxiaClient client;
    @Getter
    @VisibleForTesting
    private final Map<String, AsyncLock> locks;
    private final ScheduledExecutorService executor;
    private final OptionAutoRevalidate optionAutoRevalidate;
    private final ObservableLongGauge gaugeOxiaLocksStatus;

    LockManagerImpl(
            AsyncOxiaClient client,
            Meter meter,
            ScheduledExecutorService scheduledExecutorService,
            OptionAutoRevalidate optionAutoRevalidate) {
        this.client = client;
        this.locks = new ConcurrentHashMap<>();
        this.executor = scheduledExecutorService;
        this.optionAutoRevalidate = optionAutoRevalidate;
        // register self as the notification receiver
        client.notifications(this);
        gaugeOxiaLocksStatus =
                meter
                        .gaugeBuilder("oxia.locks.status")
                        .setDescription("Current lock status")
                        .setUnit(Unit.Events.toString())
                        .ofLongs()
                        .buildWithCallback(
                                (ob) -> {
                                    final Set<Map.Entry<String, AsyncLock>> entries = locks.entrySet();
                                    for (Map.Entry<String, AsyncLock> entry : entries) {
                                        ob.record(
                                                1,
                                                Attributes.builder()
                                                        .put("oxia.lock.key", entry.getKey())
                                                        .put("oxia.lock.status", entry.getValue().getStatus().name())
                                                        .build());
                                    }
                                });
    }

    @Override
    public AsyncLock getSharedLock(String key, OptionBackoff optionBackoff) {
        return locks.computeIfAbsent(
                key,
                (k) ->
                        new SharedSimpleLock(
                                client,
                                key,
                                executor,
                                new Backoff(
                                        optionBackoff.initDelay(),
                                        optionBackoff.initDelayUnit(),
                                        optionBackoff.maxDelay(),
                                        optionBackoff.maxDelayUnit()),
                                optionAutoRevalidate,
                                DEFAULT_RETRYABLE_EXCEPTIONS));
    }

    @Override
    public AsyncLock getThreadSimpleLock(String key, OptionBackoff optionBackoff) {
        return locks.computeIfAbsent(
                key,
                (k) ->
                        new ThreadSimpleLock(
                                client,
                                key,
                                executor,
                                new Backoff(
                                        optionBackoff.initDelay(),
                                        optionBackoff.initDelayUnit(),
                                        optionBackoff.maxDelay(),
                                        optionBackoff.maxDelayUnit()),
                                optionAutoRevalidate,
                                DEFAULT_RETRYABLE_EXCEPTIONS));
    }

    @Override
    public void removeLock(String key) {
        var lock = locks.remove(key);
        if (lock != null) {
            lock.close();
        }
    }

    @Override
    public void accept(Notification notification) {
        final var lock = locks.get(notification.key());
        if (lock == null) {
            return;
        }
        if (lock instanceof NotificationReceiver receiver) {
            receiver.notifyStateChanged(notification);
        }
    }

    @Override
    public void close() {
        if (executor != null) {
            executor.shutdown();
            try {
                if (!executor.awaitTermination(800, TimeUnit.MILLISECONDS)) {
                    executor.shutdownNow();
                }
            } catch (InterruptedException e) {
                executor.shutdownNow();
            }
        }
        gaugeOxiaLocksStatus.close();
        locks.clear();
    }
}
