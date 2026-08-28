/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.materialization;

import io.lakestream.api.StreamIdentifier;
import io.lakestream.api.materialization.TableCatalogType;
import io.netty.buffer.ByteBuf;
import java.util.Objects;
import java.util.Optional;

/**
 * A single failed record forwarded to a dead-letter sink.
 *
 * <p>The {@code payload} is a Netty {@link ByteBuf}; ownership transfers to
 * the {@link FailureMessageHandler} when accepted, which is responsible for
 * releasing the buffer once the message has been delivered or fails terminally.
 *
 * @param stream      the originating stream
 * @param catalogType the catalog type that rejected the record
 * @param dlqTopic    optional DLQ topic override (resolved by the handler when empty)
 * @param reason      human-readable failure reason
 * @param payload     raw payload of the failed record (ownership transferred)
 */
public record FailureRecord(
        StreamIdentifier stream,
        TableCatalogType catalogType,
        Optional<String> dlqTopic,
        String reason,
        ByteBuf payload) {

    /** Canonical constructor: validates non-null fields. */
    public FailureRecord {
        Objects.requireNonNull(stream, "stream");
        Objects.requireNonNull(catalogType, "catalogType");
        Objects.requireNonNull(dlqTopic, "dlqTopic cannot be null; use Optional.empty()");
        Objects.requireNonNull(reason, "reason");
        Objects.requireNonNull(payload, "payload");
    }
}
