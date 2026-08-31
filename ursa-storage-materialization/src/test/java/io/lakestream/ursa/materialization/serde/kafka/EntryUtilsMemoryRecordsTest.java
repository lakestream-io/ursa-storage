/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.materialization.serde.kafka;

import static org.assertj.core.api.Assertions.assertThat;

import io.lakestream.api.EntryHeader;
import io.lakestream.ursa.materialization.util.EntryUtils;
import io.lakestream.ursa.materialization.util.KafkaMessage;
import io.lakestream.ursa.storage.Entry;
import io.netty.buffer.ByteBuf;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.SimpleRecord;
import org.junit.jupiter.api.Test;

class EntryUtilsMemoryRecordsTest {

    @Test
    void visitsEveryRecordInMemoryRecords() throws Exception {
        MemoryRecords records = MemoryRecords.withRecords(
                Compression.NONE,
                new SimpleRecord(KafkaBrokerEntryFixtures.RECORD_TIMESTAMP, bytes("one")),
                new SimpleRecord(KafkaBrokerEntryFixtures.RECORD_TIMESTAMP, bytes("two")),
                new SimpleRecord(KafkaBrokerEntryFixtures.RECORD_TIMESTAMP, bytes("three")));
        ByteBuf payload = KafkaBrokerEntryFixtures.rawEntry(records);
        Entry entry = entry(payload, 9L, 3);
        List<KafkaMessage> messages = new ArrayList<>();
        try {
            EntryUtils.entryToKafkaMessage(entry, messages::add);

            assertThat(messages).extracting(KafkaMessage::offset).containsExactly(9L, 10L, 11L);
            assertThat(messages).extracting(message -> new String(message.value(), StandardCharsets.UTF_8))
                    .containsExactly("one", "two", "three");
            assertThat(payload.refCnt()).isOne();
        } finally {
            payload.release();
        }
    }

    @Test
    void preservesKeyAndValueFromOneRecordMemoryRecords() throws Exception {
        MemoryRecords records = MemoryRecords.withRecords(
                0L,
                Compression.NONE,
                new SimpleRecord(KafkaBrokerEntryFixtures.RECORD_TIMESTAMP, bytes("key"), bytes("value")));
        ByteBuf payload = KafkaBrokerEntryFixtures.rawEntry(records);
        Entry entry = entry(payload, 17L, 1);
        List<KafkaMessage> messages = new ArrayList<>();
        try {
            EntryUtils.entryToKafkaMessage(entry, messages::add);

            assertThat(messages).singleElement().satisfies(message -> {
                assertThat(message.offset()).isEqualTo(17L);
                assertThat(message.key()).isEqualTo(bytes("key"));
                assertThat(message.value()).isEqualTo(bytes("value"));
            });
            assertThat(payload.refCnt()).isOne();
        } finally {
            payload.release();
        }
    }

    private static Entry entry(ByteBuf payload, long offset, int records) {
        return new Entry(new EntryHeader(offset, records, 123L,
                payload.readableBytes(), payload.readableBytes()), payload);
    }

    private static byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }
}
