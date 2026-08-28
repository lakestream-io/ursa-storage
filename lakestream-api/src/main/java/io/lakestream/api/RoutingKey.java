/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.api;

import java.util.OptionalInt;

/**
 * Context for routing a write to the correct log within a stream.
 *
 * <p>Currently supports index-based routing only. Key-based routing
 * (murmur2 hash, range lookup) will be added in a future version.
 *
 * @param indexHint explicit log index for the write; empty for round-robin
 */
public record RoutingKey(OptionalInt indexHint) {

    /**
     * Routes to a specific log by ordinal index.
     *
     * @param index the zero-based log index
     * @return a routing key targeting the given index
     */
    public static RoutingKey ofIndex(int index) {
        return new RoutingKey(OptionalInt.of(index));
    }

    /**
     * Routes using round-robin strategy across available logs.
     *
     * @return a routing key for round-robin distribution
     */
    public static RoutingKey roundRobin() {
        return new RoutingKey(OptionalInt.empty());
    }
}
