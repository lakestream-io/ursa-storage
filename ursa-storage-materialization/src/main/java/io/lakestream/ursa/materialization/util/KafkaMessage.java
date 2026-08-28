/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.materialization.util;

import java.util.List;

/**
 * A Kafka record decoded from a native {@code MemoryRecords} entry payload.
 *
 * @param offset Kafka record offset
 * @param timestamp Kafka record timestamp, or {@code -1} when the record has no timestamp
 * @param key record key, or {@code null}
 * @param value record value, or {@code null} for a tombstone
 * @param headers Kafka record headers in wire order, including duplicate keys and null values
 */
public record KafkaMessage(long offset, long timestamp, byte[] key, byte[] value, List<KafkaHeader> headers) {

    public KafkaMessage {
        headers = headers == null ? List.of() : List.copyOf(headers);
    }

    public KafkaMessage(long offset, byte[] key, byte[] value) {
        this(offset, -1L, key, value, List.of());
    }

    public byte[] getData() {
        return value;
    }

    /** One Kafka header. The byte value may be {@code null}. */
    public record KafkaHeader(String key, byte[] value) {
    }
}
