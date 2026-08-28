/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.lakehouse.v2.serde.kafka.parquet;

import static io.lakestream.ursa.lakehouse.v2.serde.kafka.parquet.KafkaEntryToParquetEncoder.PRIMITIVE_RECORD_FIELD_NAME;
import static io.lakestream.ursa.lakehouse.v2.serde.kafka.parquet.KafkaEntryToParquetEncoder.PRIMITIVE_RECORD_NAME;

import io.lakestream.ursa.materialization.serde.EntryEncoder;
import io.lakestream.ursa.materialization.serde.GenericEntry;
import io.lakestream.ursa.materialization.serde.LakehouseEntryMetadata;
import io.lakestream.ursa.materialization.serde.MaterializationRecord;
import io.lakestream.ursa.materialization.serde.ResultConsumer;
import java.nio.ByteBuffer;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;

public class KafkaEntryBatchedRawDataToParquetEncoder implements EntryEncoder<GenericRecord> {

    private static final Schema SCHEMA = SchemaBuilder.record(PRIMITIVE_RECORD_NAME)
        .fields()
        .optionalBytes(PRIMITIVE_RECORD_FIELD_NAME)
        .endRecord();

    @Override
    public void encode(String topic, GenericEntry entry,
                       ResultConsumer<MaterializationRecord<GenericRecord>> consumer) {
        try {
            var header = entry.entry().header();
            int numberOfMessages = header.numberOfMessages();
            if (numberOfMessages <= 0) {
                throw new IllegalArgumentException("Kafka batch must contain at least one message");
            }
            long inclusiveEndOffset = Math.addExact(header.offset(), numberOfMessages - 1L);
            byte[] payloadCopy = new byte[entry.entry().payload().readableBytes()];
            entry.entry().payload().getBytes(entry.entry().payload().readerIndex(), payloadCopy);
            GenericRecord record = new GenericData.Record(SCHEMA);
            record.put(PRIMITIVE_RECORD_FIELD_NAME, ByteBuffer.wrap(payloadCopy));
            LakehouseEntryMetadata metadata = new LakehouseEntryMetadata(header, null);
            metadata.setLakehouseEntryOffset(inclusiveEndOffset, 0);
            metadata.setNumberOfMessagesInBatch(numberOfMessages);
            metadata.setNeedToPersistent(true);
            var lakehouseEntry = new MaterializationRecord<GenericRecord>(record, metadata);
            consumer.onResult(lakehouseEntry);
        } finally {
            entry.entry().payload().release();
        }
    }

}
