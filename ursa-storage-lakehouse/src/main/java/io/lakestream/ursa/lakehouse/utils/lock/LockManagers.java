/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.lakehouse.utils.lock;

import io.grpc.netty.shaded.io.netty.util.concurrent.DefaultThreadFactory;
import io.lakestream.ursa.utils.lock.LockManager;
import io.lakestream.ursa.utils.lock.OptionAutoRevalidate;
import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.OpenTelemetry;
import io.oxia.client.api.AsyncOxiaClient;
import java.util.Objects;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import lombok.experimental.UtilityClass;

@UtilityClass
public final class LockManagers {

    /**
     * Creates a LockManager with a default single-thread ScheduledExecutorService and default
     * OptionAutoRevalidate.
     *
     * @param client the AsyncOxiaClient to be used by the LockManager
     * @return a new LockManager instance
     */
    public static LockManager createLockManager(AsyncOxiaClient client) {
        Objects.requireNonNull(client);
        final OpenTelemetry openTelemetry = GlobalOpenTelemetry.get();
        final var meter = openTelemetry.getMeter("io.github.oxia-db.lock");
        return new LockManagerImpl(
                client,
                meter,
                Executors.newSingleThreadScheduledExecutor(new DefaultThreadFactory("oxia-lock-manager")),
                OptionAutoRevalidate.DEFAULT);
    }

    /**
     * Creates a LockManager with a custom ScheduledExecutorService and OptionAutoRevalidate.
     *
     * @param client the AsyncOxiaClient to be used by the LockManager
     * @param service the ScheduledExecutorService to be used
     * @param optionAutoRevalidate the OptionAutoRevalidate setting to be used
     * @return a new LockManager instance
     */
    public static LockManager createLockManager(
            AsyncOxiaClient client,
            OpenTelemetry openTelemetry,
            ScheduledExecutorService service,
            OptionAutoRevalidate optionAutoRevalidate) {
        Objects.requireNonNull(client);
        final var meter = openTelemetry.getMeter("io.github.oxia-db.lock");
        return new LockManagerImpl(client, meter, service, optionAutoRevalidate);
    }
}
