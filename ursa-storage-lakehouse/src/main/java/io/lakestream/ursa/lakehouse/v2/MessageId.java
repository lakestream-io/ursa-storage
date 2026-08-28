/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.lakehouse.v2;

public record MessageId(long streamId, long offset) implements Comparable<MessageId> {
    @Override
    public int compareTo(MessageId other) {
        int cmp = Long.compare(this.streamId, other.streamId);
        if (cmp != 0) {
            return cmp;
        }
        return Long.compare(this.offset, other.offset);
    }

    @Override
    public String toString() {
        return streamId + ":" + offset;
    }

    public static MessageId fromString(String value) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException("MessageId string is null or blank");
        }

        int idx = value.indexOf(':');
        if (idx < 0 || idx != value.lastIndexOf(':')) {
            throw new IllegalArgumentException(
                    "Invalid MessageId format, expected '<streamId>:<offset>', got: " + value
            );
        }

        try {
            long streamId = Long.parseLong(value.substring(0, idx));
            long offset = Long.parseLong(value.substring(idx + 1));
            return new MessageId(streamId, offset);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(
                    "Invalid number in MessageId string: " + value, e);
        }
    }
}
