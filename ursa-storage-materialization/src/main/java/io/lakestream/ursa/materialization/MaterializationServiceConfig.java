/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.materialization;

import java.time.Duration;
import java.util.Map;
import java.util.Objects;

/**
 * Operator-side knobs the orchestrator passes to {@link MaterializationService}.
 *
 * <p>Intentionally minimal while still letting the orchestrator pass through
 * per-deployment sizing limits.
 *
 * @param workerPoolSize           number of worker threads the service may use
 *                                 (must be {@code > 0})
 * @param walReadRateLimitWindow   sliding-window size for WAL read rate limiting
 * @param walReadRateLimitBytes    rate limit (in bytes) per window
 *                                 (must be {@code > 0})
 * @param additionalProperties     escape hatch for deployment-specific knobs
 *                                 not yet promoted to typed fields
 */
public record MaterializationServiceConfig(
        int workerPoolSize,
        Duration walReadRateLimitWindow,
        long walReadRateLimitBytes,
        Map<String, String> additionalProperties) {

    /**
     * Canonical constructor: validates positive sizes and non-null fields, and
     * defensively copies {@code additionalProperties}.
     */
    public MaterializationServiceConfig {
        if (workerPoolSize <= 0) {
            throw new IllegalArgumentException("workerPoolSize must be > 0");
        }
        Objects.requireNonNull(walReadRateLimitWindow, "walReadRateLimitWindow");
        if (walReadRateLimitBytes <= 0) {
            throw new IllegalArgumentException("walReadRateLimitBytes must be > 0");
        }
        Objects.requireNonNull(additionalProperties, "additionalProperties");
        additionalProperties = Map.copyOf(additionalProperties);
    }

    /**
     * Sensible defaults: 8 workers, 50 MiB/s WAL read rate limit.
     *
     * <p>Suitable for tests and bootstrap; production deployments should override
     * these values via the config bootstrap planned for T7.
     */
    public static MaterializationServiceConfig defaults() {
        return new MaterializationServiceConfig(
                8,
                Duration.ofSeconds(1),
                52_428_800L,
                Map.of());
    }
}
