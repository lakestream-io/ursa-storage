/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.materialization.serde.kafka;

import io.lakestream.ursa.materialization.util.KafkaMessage;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.nio.ByteBuffer;
import java.util.List;
import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.SimpleRecord;

/** Creates native Kafka {@link MemoryRecords} payloads for Ursa storage entries. */
public final class KafkaMemoryRecords {

    private KafkaMemoryRecords() {
    }

    /**
     * Encodes one record using producer-style offsets. Lakestream assigns the durable entry offset,
     * so the batch embedded in the payload always starts at zero.
     */
    public static ByteBuf encode(KafkaMessage message) {
        List<KafkaMessage.KafkaHeader> sourceHeaders = message.headers();
        Header[] headers = new Header[sourceHeaders.size()];
        for (int i = 0; i < sourceHeaders.size(); i++) {
            KafkaMessage.KafkaHeader header = sourceHeaders.get(i);
            headers[i] = new RecordHeader(header.key(), header.value());
        }
        MemoryRecords records = MemoryRecords.withRecords(
                0L,
                Compression.NONE,
                new SimpleRecord(message.timestamp(), message.key(), message.value(), headers));
        ByteBuffer recordsBuffer = records.buffer().duplicate();
        ByteBuf payload = Unpooled.buffer(recordsBuffer.remaining(), recordsBuffer.remaining());
        try {
            payload.writeBytes(recordsBuffer);
            return payload;
        } catch (Throwable failure) {
            payload.release();
            throw failure;
        }
    }
}
