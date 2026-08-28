/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.materialization;

import java.util.concurrent.CompletableFuture;

/**
 * Forwards records that could not be materialized to a dead-letter sink.
 *
 * <p>The framework hands the handler ownership of the record's
 * {@link FailureRecord#payload()} {@code ByteBuf}; implementations must release
 * it after the future completes (success or failure).
 */
public interface FailureMessageHandler extends AutoCloseable {

    /**
     * Sends {@code record} to the dead-letter sink. The returned future
     * completes when the record has been durably accepted, or fails when the
     * record cannot be forwarded.
     *
     * @param record the failed record to forward
     * @return a future that completes when the record has been delivered
     */
    CompletableFuture<Void> sendFailureMessage(FailureRecord record);

    /** Default no-op close; sinks with resources should override. */
    @Override
    default void close() {
    }

    /**
     * Returns a no-op handler that completes every send immediately. Useful for
     * unit tests and stub deployments where DLQ delivery is not required.
     */
    static FailureMessageHandler noop() {
        return record -> CompletableFuture.completedFuture(null);
    }
}
