/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.lakehouse.v2.serde.kafka.parquet;

import static io.lakestream.ursa.lakehouse.v2.serde.kafka.parquet.KafkaEntryToParquetEncoder.PRIMITIVE_RECORD_FIELD_NAME;

import io.lakestream.ursa.materialization.serde.EntryDecoder;
import io.lakestream.ursa.materialization.serde.GenericEntry;
import io.lakestream.ursa.materialization.serde.MaterializationRecord;
import io.lakestream.ursa.materialization.serde.ResultConsumer;
import io.lakestream.ursa.materialization.serde.exception.DeserializationException;
import io.lakestream.ursa.storage.Entry;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.nio.ByteBuffer;
import java.util.Iterator;
import org.apache.avro.generic.GenericRecord;

public class KafkaEntryBatchedRawDataToParquetDecoder implements EntryDecoder<GenericRecord> {

    @Override
    public void decode(String topic, Iterator<MaterializationRecord<GenericRecord>> entry,
                       ResultConsumer<GenericEntry> consumer) {
        while (entry.hasNext()) {
            var lakehouseEntry = entry.next();
            if (lakehouseEntry == null) {
                return;
            }
            var genericRecord = lakehouseEntry.record();
            var payload = genericRecord.get(PRIMITIVE_RECORD_FIELD_NAME);
            if (lakehouseEntry.metadata().isEmpty()) {
                consumer.onErrorWithCtx(null, new DeserializationException("The entry metadata is empty"));
                continue;
            }
            ByteBuf payloadBuf;
            if (payload instanceof ByteBuffer buffer) {
                payloadBuf = Unpooled.wrappedBuffer(buffer);
            } else {
                payloadBuf = Unpooled.wrappedBuffer((byte[]) payload);
            }
            var eh = lakehouseEntry.metadata().get().getEntryHeader();
            Entry e = new Entry(eh, payloadBuf);
            GenericEntry genericEntry = new GenericEntry(e);
            consumer.onResult(genericEntry);
        }
    }
}
