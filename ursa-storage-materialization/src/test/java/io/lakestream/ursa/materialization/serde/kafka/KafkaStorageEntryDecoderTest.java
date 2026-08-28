/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.materialization.serde.kafka;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.lakestream.ursa.materialization.util.KafkaMessage;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.errors.CorruptRecordException;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.record.ControlRecordType;
import org.apache.kafka.common.record.DefaultRecordBatch;
import org.apache.kafka.common.record.EndTransactionMarker;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.MemoryRecordsBuilder;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.record.SimpleRecord;
import org.apache.kafka.common.record.TimestampType;
import org.junit.jupiter.api.Test;

class KafkaStorageEntryDecoderTest {

    @Test
    void decodesRawMemoryRecordsAcrossCompressedBatchesWithoutTakingOwnership() {
        Header[] firstHeaders = {
            new RecordHeader("trace", new byte[] {1, 2}),
            new RecordHeader("trace", null)
        };
        MemoryRecords compressedBatch = MemoryRecords.withRecords(
                0L,
                Compression.gzip().build(),
                new SimpleRecord(KafkaBrokerEntryFixtures.RECORD_TIMESTAMP,
                        bytes("key-1"), bytes("value-1"), firstHeaders),
                new SimpleRecord(KafkaBrokerEntryFixtures.RECORD_TIMESTAMP,
                        null, bytes("value-2"), new Header[0]));
        MemoryRecords secondBatch = MemoryRecords.withRecords(
                0L,
                Compression.NONE,
                new SimpleRecord(KafkaBrokerEntryFixtures.RECORD_TIMESTAMP,
                        bytes("key-3"), null, new Header[] {
                            new RecordHeader("binary", new byte[] {(byte) 0xff, 0})
                        }));
        ByteBuf rawEntry = KafkaBrokerEntryFixtures.rawEntry(compressedBatch, secondBatch);
        ByteBuf withPrefix = Unpooled.buffer(rawEntry.readableBytes() + 3);
        withPrefix.writeZero(3).writeBytes(rawEntry);
        withPrefix.readerIndex(3);
        rawEntry.release();

        try {
            int readerIndex = withPrefix.readerIndex();
            int refCount = withPrefix.refCnt();
            var messages = KafkaStorageEntryDecoder.decode(withPrefix, 41L, 3);

            assertThat(messages).hasSize(3);
            assertThat(messages).extracting(KafkaMessage::offset).containsExactly(41L, 42L, 43L);
            assertThat(messages).extracting(KafkaMessage::timestamp)
                    .containsOnly(KafkaBrokerEntryFixtures.RECORD_TIMESTAMP);
            assertThat(messages.get(0).key()).isEqualTo(bytes("key-1"));
            assertThat(messages.get(0).value()).isEqualTo(bytes("value-1"));
            assertThat(messages.get(0).headers()).hasSize(2);
            assertThat(messages.get(0).headers().get(0).key()).isEqualTo("trace");
            assertThat(messages.get(0).headers().get(0).value()).containsExactly(1, 2);
            assertThat(messages.get(0).headers().get(1).key()).isEqualTo("trace");
            assertThat(messages.get(0).headers().get(1).value()).isNull();
            assertThat(messages.get(1).key()).isNull();
            assertThat(messages.get(2).value()).isNull();
            assertThat(messages.get(2).headers().get(0).value())
                    .containsExactly((byte) 0xff, (byte) 0);
            assertThat(withPrefix.readerIndex()).isEqualTo(readerIndex);
            assertThat(withPrefix.refCnt()).isEqualTo(refCount);
        } finally {
            withPrefix.release();
        }
    }

    @Test
    void rejectsExpectedRecordCountThatViolatesTheStorageContract() {
        MemoryRecords records = MemoryRecords.withRecords(
                Compression.NONE,
                new SimpleRecord(KafkaBrokerEntryFixtures.RECORD_TIMESTAMP, bytes("one")));
        ByteBuf payload = KafkaBrokerEntryFixtures.rawEntry(records);
        try {
            assertThatThrownBy(() -> KafkaStorageEntryDecoder.decode(payload, 7L, 0))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("expected 0");
            assertThatThrownBy(() -> KafkaStorageEntryDecoder.decode(payload, 7L, -1))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("expected -1");
            assertThat(payload.refCnt()).isOne();
        } finally {
            payload.release();
        }
    }

    @Test
    void capsInitialAllocationForUntrustedExpectedRecordCount() {
        MemoryRecords records = MemoryRecords.withRecords(
                Compression.NONE,
                new SimpleRecord(KafkaBrokerEntryFixtures.RECORD_TIMESTAMP, bytes("one")));
        ByteBuf payload = KafkaBrokerEntryFixtures.rawEntry(records);
        try {
            int readerIndex = payload.readerIndex();
            assertThatThrownBy(() -> KafkaStorageEntryDecoder.decode(payload, 7L, Integer.MAX_VALUE))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("decoded=1, expected=" + Integer.MAX_VALUE);
            assertThat(payload.readerIndex()).isEqualTo(readerIndex);
            assertThat(payload.refCnt()).isOne();
        } finally {
            payload.release();
        }
    }

    @Test
    void rejectsEmptyMemoryRecordsPayloadWithoutChangingOwnership() {
        ByteBuf payload = Unpooled.buffer(0, 0);
        try {
            assertThatThrownBy(() -> KafkaStorageEntryDecoder.decode(payload, 0, 1))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("no MemoryRecords payload");
            assertThat(payload.refCnt()).isOne();
        } finally {
            payload.release();
        }
    }

    @Test
    void rejectsStorageHeaderRecordCountMismatch() {
        MemoryRecords records = MemoryRecords.withRecords(
                Compression.NONE,
                new SimpleRecord(KafkaBrokerEntryFixtures.RECORD_TIMESTAMP, bytes("one")),
                new SimpleRecord(KafkaBrokerEntryFixtures.RECORD_TIMESTAMP, bytes("two")),
                new SimpleRecord(KafkaBrokerEntryFixtures.RECORD_TIMESTAMP, bytes("three")));
        ByteBuf payload = KafkaBrokerEntryFixtures.rawEntry(records);
        try {
            assertThatThrownBy(() -> KafkaStorageEntryDecoder.decode(payload, 0, 2))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("decoded=3, expected=2");
            assertThat(payload.refCnt()).isOne();
        } finally {
            payload.release();
        }
    }

    @Test
    void rejectsTrailingBytesWithoutChangingOwnership() {
        MemoryRecords records = MemoryRecords.withRecords(
                Compression.NONE,
                new SimpleRecord(KafkaBrokerEntryFixtures.RECORD_TIMESTAMP, bytes("one")));
        ByteBuf recordsPayload = KafkaBrokerEntryFixtures.rawEntry(records);
        ByteBuf payload = Unpooled.buffer(recordsPayload.readableBytes() + 1);
        payload.writeBytes(recordsPayload, recordsPayload.readerIndex(), recordsPayload.readableBytes());
        payload.writeByte(1);
        recordsPayload.release();
        try {
            int readerIndex = payload.readerIndex();
            assertThatThrownBy(() -> KafkaStorageEntryDecoder.decode(payload, 0, 1))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("trailing or incomplete bytes");
            assertThat(payload.readerIndex()).isEqualTo(readerIndex);
            assertThat(payload.refCnt()).isOne();
        } finally {
            payload.release();
        }
    }

    @Test
    void rejectsAnyPrefixBeforeMemoryRecordsWithoutChangingOwnership() {
        MemoryRecords records = MemoryRecords.withRecords(
                Compression.NONE,
                new SimpleRecord(KafkaBrokerEntryFixtures.RECORD_TIMESTAMP, bytes("one")));
        ByteBuf rawEntry = KafkaBrokerEntryFixtures.rawEntry(records);
        ByteBuf payload = Unpooled.buffer(Integer.BYTES + rawEntry.readableBytes());
        payload.writeInt(0);
        payload.writeBytes(rawEntry, rawEntry.readerIndex(), rawEntry.readableBytes());
        rawEntry.release();
        try {
            int readerIndex = payload.readerIndex();
            assertThatThrownBy(() -> KafkaStorageEntryDecoder.decode(payload, 0, 1))
                    .isInstanceOf(RuntimeException.class);
            assertThat(payload.readerIndex()).isEqualTo(readerIndex);
            assertThat(payload.refCnt()).isOne();
        } finally {
            payload.release();
        }
    }

    @Test
    void rejectsCorruptBatchCrcWithoutChangingOwnership() {
        MemoryRecords records = MemoryRecords.withRecords(
                Compression.NONE,
                new SimpleRecord(KafkaBrokerEntryFixtures.RECORD_TIMESTAMP, bytes("one")));
        ByteBuf payload = KafkaBrokerEntryFixtures.rawEntry(records);
        payload.setByte(payload.writerIndex() - 1, payload.getByte(payload.writerIndex() - 1) ^ 1);
        try {
            int readerIndex = payload.readerIndex();
            assertThatThrownBy(() -> KafkaStorageEntryDecoder.decode(payload, 0, 1))
                    .isInstanceOf(CorruptRecordException.class);
            assertThat(payload.readerIndex()).isEqualTo(readerIndex);
            assertThat(payload.refCnt()).isOne();
        } finally {
            payload.release();
        }
    }

    @Test
    void rejectsControlBatchWithoutChangingOwnership() {
        MemoryRecords records = MemoryRecords.withEndTransactionMarker(
                0L,
                KafkaBrokerEntryFixtures.RECORD_TIMESTAMP,
                RecordBatch.NO_PARTITION_LEADER_EPOCH,
                11L,
                (short) 2,
                new EndTransactionMarker(ControlRecordType.COMMIT, 3));
        ByteBuf payload = KafkaBrokerEntryFixtures.rawEntry(records);
        try {
            int readerIndex = payload.readerIndex();
            assertThatThrownBy(() -> KafkaStorageEntryDecoder.decode(payload, 0, 1))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("control batches cannot be materialized");
            assertThat(payload.readerIndex()).isEqualTo(readerIndex);
            assertThat(payload.refCnt()).isOne();
        } finally {
            payload.release();
        }
    }

    @Test
    void rejectsNonZeroBaseOffsetAndTransactionalBatch() {
        MemoryRecords nonZeroBaseRecords = MemoryRecords.withRecords(
                5L,
                Compression.NONE,
                new SimpleRecord(KafkaBrokerEntryFixtures.RECORD_TIMESTAMP, bytes("one")));
        assertInvalid(nonZeroBaseRecords, 1, "base offset 0");

        MemoryRecords transactionalRecords = MemoryRecords.withTransactionalRecords(
                Compression.NONE,
                13L,
                (short) 2,
                0,
                new SimpleRecord(KafkaBrokerEntryFixtures.RECORD_TIMESTAMP, bytes("one")));
        assertInvalid(transactionalRecords, 1, "transactional batches cannot be materialized");
    }

    @Test
    void rejectsNonCanonicalV2Offsets() {
        assertInvalid(recordsWithOffsets(0L, 7L, 0L, 1L), 2, "offset range does not match");
        assertInvalid(recordsWithOffsets(0L, 1L, 0L, 2L), 2, "non-sequential offset");
    }

    @Test
    void rejectsEmptyBatchBeforeLaterDataBatch() {
        MemoryRecords validRecords = MemoryRecords.withRecords(
                Compression.NONE,
                new SimpleRecord(KafkaBrokerEntryFixtures.RECORD_TIMESTAMP, bytes("one")));
        ByteBuf payload = KafkaBrokerEntryFixtures.rawEntry(emptyBatch(), validRecords);
        try {
            assertThatThrownBy(() -> KafkaStorageEntryDecoder.decode(payload, 0L, 1))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("empty record batch");
            assertThat(payload.refCnt()).isOne();
        } finally {
            payload.release();
        }
    }

    private static void assertInvalid(MemoryRecords records, int expectedRecordCount, String message) {
        ByteBuf payload = KafkaBrokerEntryFixtures.rawEntry(records);
        try {
            assertThatThrownBy(() -> KafkaStorageEntryDecoder.decode(payload, 0L, expectedRecordCount))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining(message);
            assertThat(payload.refCnt()).isOne();
        } finally {
            payload.release();
        }
    }

    private static MemoryRecords recordsWithOffsets(long baseOffset, long lastOffset, long... recordOffsets) {
        ByteBuffer buffer = ByteBuffer.allocate(1024);
        MemoryRecordsBuilder builder = MemoryRecords.builder(
                buffer,
                RecordBatch.MAGIC_VALUE_V2,
                Compression.NONE,
                TimestampType.CREATE_TIME,
                baseOffset);
        for (long recordOffset : recordOffsets) {
            builder.appendWithOffset(
                    recordOffset,
                    new SimpleRecord(KafkaBrokerEntryFixtures.RECORD_TIMESTAMP, bytes("value")));
        }
        builder.overrideLastOffset(lastOffset);
        return builder.build();
    }

    private static MemoryRecords emptyBatch() {
        ByteBuffer buffer = ByteBuffer.allocate(DefaultRecordBatch.RECORD_BATCH_OVERHEAD);
        DefaultRecordBatch.writeEmptyHeader(
                buffer,
                RecordBatch.MAGIC_VALUE_V2,
                RecordBatch.NO_PRODUCER_ID,
                RecordBatch.NO_PRODUCER_EPOCH,
                RecordBatch.NO_SEQUENCE,
                0L,
                -1L,
                RecordBatch.NO_PARTITION_LEADER_EPOCH,
                TimestampType.CREATE_TIME,
                KafkaBrokerEntryFixtures.RECORD_TIMESTAMP,
                false,
                false);
        buffer.flip();
        return MemoryRecords.readableRecords(buffer);
    }

    private static byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }
}
