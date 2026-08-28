/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.materialization.serde.kafka;

import io.lakestream.ursa.materialization.util.KafkaMessage;
import io.lakestream.ursa.materialization.util.KafkaMessage.KafkaHeader;
import io.netty.buffer.ByteBuf;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.Record;
import org.apache.kafka.common.record.RecordBatch;

/** Decodes native Kafka {@link MemoryRecords} stored directly as an Ursa entry payload. */
public final class KafkaStorageEntryDecoder {

    private static final int MAX_INITIAL_MESSAGE_CAPACITY = 1024;

    private KafkaStorageEntryDecoder() {
    }

    /**
     * Decodes all data records without changing or retaining {@code entryPayload}.
     *
     * <p>Producer batches arrive with a zero base offset. Lakestream assigns the durable base offset
     * to the containing entry, so record offsets are derived from {@code entryBaseOffset} and record
     * order rather than trusting the offsets embedded in individual producer batches.
     */
    public static List<KafkaMessage> decode(
            ByteBuf entryPayload, long entryBaseOffset, int expectedRecordCount) {
        if (expectedRecordCount <= 0) {
            throw new IllegalArgumentException(
                    "Kafka storage entry must contain at least one record, but expected "
                            + expectedRecordCount);
        }
        int recordsSize = entryPayload.readableBytes();
        if (recordsSize == 0) {
            throw new IllegalArgumentException("Kafka storage entry contains no MemoryRecords payload");
        }
        ByteBuffer recordsBuffer = entryPayload.nioBuffer(entryPayload.readerIndex(), recordsSize);
        MemoryRecords memoryRecords = MemoryRecords.readableRecords(recordsBuffer);
        List<KafkaMessage> messages = new ArrayList<>(
                Math.min(expectedRecordCount, MAX_INITIAL_MESSAGE_CAPACITY));
        int recordIndex = 0;

        for (RecordBatch batch : memoryRecords.batches()) {
            batch.ensureValid();
            if (batch.isControlBatch()) {
                throw new IllegalArgumentException("Kafka control batches cannot be materialized as table rows");
            }
            if (batch.isTransactional()) {
                throw new IllegalArgumentException("Kafka transactional batches cannot be materialized as table rows");
            }

            boolean validateV2Offsets = batch.magic() >= RecordBatch.MAGIC_VALUE_V2;
            if (validateV2Offsets && batch.baseOffset() != 0L) {
                throw new IllegalArgumentException(
                        "Kafka producer record batch must have base offset 0, but was " + batch.baseOffset());
            }

            int batchRecordCount = 0;
            long expectedRecordOffset = batch.baseOffset();
            for (Record record : batch) {
                record.ensureValid();
                if (validateV2Offsets) {
                    if (record.offset() != expectedRecordOffset) {
                        throw new IllegalArgumentException(
                                "Kafka record batch contains a non-sequential offset: expected="
                                        + expectedRecordOffset + ", actual=" + record.offset());
                    }
                    expectedRecordOffset = Math.addExact(expectedRecordOffset, 1L);
                }
                long offset = Math.addExact(entryBaseOffset, recordIndex);
                messages.add(new KafkaMessage(
                        offset,
                        record.timestamp(),
                        copy(record.key()),
                        copy(record.value()),
                        copy(record.headers())));
                batchRecordCount = Math.addExact(batchRecordCount, 1);
                recordIndex = Math.addExact(recordIndex, 1);
            }

            if (batchRecordCount <= 0) {
                throw new IllegalArgumentException("Kafka MemoryRecords contains an empty record batch");
            }
            Integer declaredRecordCount = batch.countOrNull();
            if (declaredRecordCount != null && declaredRecordCount != batchRecordCount) {
                throw new IllegalArgumentException(
                        "Kafka record batch count does not match its contents: decoded="
                                + batchRecordCount + ", declared=" + declaredRecordCount);
            }
            if (validateV2Offsets) {
                long recordCountFromOffsets = batch.lastOffset() - batch.baseOffset() + 1L;
                if (recordCountFromOffsets != batchRecordCount) {
                    throw new IllegalArgumentException(
                            "Kafka record batch offset range does not match its contents: range="
                                    + recordCountFromOffsets + ", decoded=" + batchRecordCount);
                }
            }
        }

        if (memoryRecords.validBytes() != recordsSize) {
            throw new IllegalArgumentException(
                    "Kafka MemoryRecords contains trailing or incomplete bytes: valid="
                            + memoryRecords.validBytes() + ", payload=" + recordsSize);
        }
        if (recordIndex != expectedRecordCount) {
            throw new IllegalArgumentException(
                    "Kafka record count does not match the storage header: decoded="
                            + recordIndex + ", expected=" + expectedRecordCount);
        }
        return messages;
    }

    private static byte[] copy(ByteBuffer source) {
        if (source == null) {
            return null;
        }
        ByteBuffer duplicate = source.duplicate();
        byte[] result = new byte[duplicate.remaining()];
        duplicate.get(result);
        return result;
    }

    private static List<KafkaHeader> copy(Header[] headers) {
        if (headers == null || headers.length == 0) {
            return List.of();
        }
        List<KafkaHeader> result = new ArrayList<>(headers.length);
        for (Header header : headers) {
            byte[] value = header.value();
            result.add(new KafkaHeader(header.key(), value == null ? null : value.clone()));
        }
        return result;
    }
}
