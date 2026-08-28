/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.test;

import static org.assertj.core.api.Assertions.assertThat;

import io.lakestream.api.EntryHeader;
import io.lakestream.ursa.lakehouse.v2.serde.kafka.parquet.KafkaEntryBatchedRawDataToParquetEncoder;
import io.lakestream.ursa.materialization.serde.GenericEntry;
import io.lakestream.ursa.materialization.serde.MaterializationRecord;
import io.lakestream.ursa.materialization.serde.ResultConsumer;
import io.lakestream.ursa.storage.Entry;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.Unpooled;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.Test;

class KafkaBatchedRawParquetFramingTest {

    @Test
    void preservesCompleteMemoryRecordsWithoutDecodingThem() {
        ByteBuf generatedPayload = KafkaLakehouseIngestionTest.rawMemoryRecords(
                "key".getBytes(StandardCharsets.UTF_8), "value".getBytes(StandardCharsets.UTF_8));
        byte[] backingArray = ByteBufUtil.getBytes(generatedPayload);
        generatedPayload.release();
        byte[] expectedRecords = backingArray.clone();
        ByteBuf payload = Unpooled.wrappedBuffer(backingArray);
        EntryHeader header = new EntryHeader(
                50L, 1, 1234L, payload.readableBytes(), payload.readableBytes());
        var result = new AtomicReference<MaterializationRecord<GenericRecord>>();

        new KafkaEntryBatchedRawDataToParquetEncoder().encode(
                "default/orders", new GenericEntry(new Entry(header, payload)), resultConsumer(result));

        Arrays.fill(backingArray, (byte) 0);
        ByteBuffer persistedPayload = (ByteBuffer) result.get().record().get("payload");
        byte[] actualRecords = new byte[persistedPayload.remaining()];
        persistedPayload.duplicate().get(actualRecords);
        assertThat(actualRecords).containsExactly(expectedRecords);
        assertThat(result.get().metadata().orElseThrow().getLakehouseEntryOffset().entryId())
                .isEqualTo(50L);
        assertThat(payload.refCnt()).isZero();
    }

    private static ResultConsumer<MaterializationRecord<GenericRecord>> resultConsumer(
            AtomicReference<MaterializationRecord<GenericRecord>> result) {
        return new ResultConsumer<>() {
            @Override
            public void onResult(MaterializationRecord<GenericRecord> record) {
                result.set(record);
            }

            @Override
            public void onErrorWithCtx(Object context, Throwable throwable) {
                throw new AssertionError(throwable);
            }
        };
    }
}
