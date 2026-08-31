/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.materialization.serde.kafka;

import static org.assertj.core.api.Assertions.assertThat;

import io.confluent.kafka.schemaregistry.client.SchemaMetadata;
import io.lakestream.api.EntryHeader;
import io.lakestream.ursa.exception.ExceptionWithCode;
import io.lakestream.ursa.materialization.serde.GenericEntry;
import io.lakestream.ursa.materialization.serde.LakehouseEntryMetadata;
import io.lakestream.ursa.materialization.serde.MaterializationRecord;
import io.lakestream.ursa.materialization.serde.ResultConsumer;
import io.lakestream.ursa.materialization.serde.SchemaKey;
import io.lakestream.ursa.materialization.util.KafkaMessage;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

class KafkaEntryDecoderTest {

    @Test
    void reconstructsOneNativeMemoryRecordsEntry() {
        EntryHeader sourceHeader = new EntryHeader(37L, 4, 1_700_000_000_321L, 123, 456);
        LakehouseEntryMetadata metadata = new LakehouseEntryMetadata(sourceHeader, 0L);
        MaterializationRecord<String> materialized = new MaterializationRecord<>("value", metadata);
        AtomicReference<GenericEntry> result = new AtomicReference<>();

        decoder().decode("events", List.of(materialized).iterator(), new ResultConsumer<>() {
            @Override
            public void onResult(GenericEntry entry) {
                result.set(entry);
            }

            @Override
            public void onErrorWithCtx(Object context, Throwable throwable) {
                throw new AssertionError(throwable);
            }
        });

        GenericEntry entry = result.get();
        try {
            assertThat(entry.entry().header().offset()).isEqualTo(37L);
            assertThat(entry.entry().header().numberOfMessages()).isOne();
            assertThat(entry.entry().header().entrySize())
                    .isEqualTo(entry.entry().payload().readableBytes());
            List<KafkaMessage> messages = KafkaStorageEntryDecoder.decode(
                    entry.entry().payload(), entry.entry().header().offset(), 1);
            assertThat(messages).singleElement().satisfies(message -> {
                assertThat(message.offset()).isEqualTo(37L);
                assertThat(message.timestamp()).isEqualTo(sourceHeader.writtenTimestamp());
                assertThat(message.value()).isEqualTo("value".getBytes(StandardCharsets.UTF_8));
            });
        } finally {
            entry.entry().payload().release();
        }
    }

    private static KafkaEntryDecoder<String> decoder() {
        return new KafkaEntryDecoder<>(null) {
            @Override
            protected ByteBuffer processLakehouseEntry(
                    String topic, MaterializationRecord<String> lakehouseEntry) {
                return ByteBuffer.wrap(lakehouseEntry.record().getBytes(StandardCharsets.UTF_8));
            }

            @Override
            protected ByteBuffer transform(
                    String object, SchemaMetadata schemaMetadata, SchemaKey schemaKey)
                    throws ExceptionWithCode {
                throw new AssertionError("processLakehouseEntry is overridden for this test");
            }
        };
    }
}
