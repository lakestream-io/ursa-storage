/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.utils;

import java.time.Duration;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Simple exponential backoff utility for retry logic.
 * The implementation is self-contained so callers do not need an external retry utility.
 */
public class RetryBackoff {

    private final long initialDelayMs;
    private final long maxBackoffMs;
    private long currentDelayMs;

    public RetryBackoff(Duration initialDelay, Duration maxBackoff) {
        this.initialDelayMs = initialDelay.toMillis();
        this.maxBackoffMs = maxBackoff.toMillis();
        this.currentDelayMs = initialDelayMs;
    }

    /**
     * Returns the next backoff duration and advances the internal state.
     * Uses exponential backoff with jitter, capped at maxBackoff.
     */
    public Duration next() {
        long delay = currentDelayMs;
        // Add small jitter (up to 10% of delay)
        if (delay > 0) {
            delay += ThreadLocalRandom.current().nextLong(Math.max(1, delay / 10));
        }
        delay = Math.min(delay, maxBackoffMs);
        // Double for next call
        currentDelayMs = Math.min(currentDelayMs * 2, maxBackoffMs);
        return Duration.ofMillis(delay);
    }

    public void reset() {
        this.currentDelayMs = initialDelayMs;
    }
}
