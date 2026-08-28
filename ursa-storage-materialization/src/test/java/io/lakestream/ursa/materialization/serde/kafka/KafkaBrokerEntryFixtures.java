/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.materialization.serde.kafka;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.nio.ByteBuffer;
import org.apache.kafka.common.record.MemoryRecords;

final class KafkaBrokerEntryFixtures {

    static final long RECORD_TIMESTAMP = 1_700_000_000_000L;

    private KafkaBrokerEntryFixtures() {
    }

    static ByteBuf rawEntry(MemoryRecords... recordSets) {
        int size = 0;
        for (MemoryRecords records : recordSets) {
            size += records.sizeInBytes();
        }
        ByteBuf result = Unpooled.buffer(size, size);
        boolean success = false;
        try {
            for (MemoryRecords records : recordSets) {
                ByteBuffer buffer = records.buffer().duplicate();
                result.writeBytes(buffer);
            }
            success = true;
            return result;
        } finally {
            if (!success) {
                result.release();
            }
        }
    }
}
