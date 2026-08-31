/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.clickhouse;

import io.lakestream.api.EntryHeader;
import io.lakestream.ursa.materialization.serde.GenericEntry;
import io.lakestream.ursa.storage.Entry;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.SimpleRecord;

/** Test fixture for native Kafka {@link MemoryRecords} storage entries. */
final class MemoryRecordsEntries {

    private MemoryRecordsEntries() {
    }

    static GenericEntry of(String value) {
        MemoryRecords records = MemoryRecords.withRecords(
                0L,
                Compression.NONE,
                new SimpleRecord(
                        1_700_000_000_000L,
                        "key".getBytes(StandardCharsets.UTF_8),
                        value.getBytes(StandardCharsets.UTF_8)));
        return entry(records, 0L, 1);
    }

    static GenericEntry batch(String first, String second, String third) {
        MemoryRecords records = MemoryRecords.withRecords(
                0L,
                Compression.NONE,
                new SimpleRecord(1_700_000_000_000L, bytes(first)),
                new SimpleRecord(1_700_000_000_001L, bytes(second)),
                new SimpleRecord(1_700_000_000_002L, bytes(third)));
        return entry(records, 40L, 3);
    }

    private static GenericEntry entry(MemoryRecords records, long offset, int recordCount) {
        ByteBuffer recordsBuffer = records.buffer().duplicate();
        ByteBuf payload = Unpooled.buffer(recordsBuffer.remaining());
        payload.writeBytes(recordsBuffer);
        EntryHeader header = new EntryHeader(offset, recordCount, 1_700_000_000_100L,
                payload.readableBytes(), payload.readableBytes());
        return new GenericEntry(new Entry(header, payload));
    }

    private static byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }
}
