/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.materialization.serde.kafka;

import static org.assertj.core.api.Assertions.assertThat;

import io.lakestream.ursa.materialization.util.KafkaMessage;
import java.nio.charset.StandardCharsets;
import java.util.List;
import org.junit.jupiter.api.Test;

class KafkaMemoryRecordsTest {

    @Test
    void roundTripsOneNativeRecordWithHeadersAndTombstoneValue() {
        KafkaMessage source = new KafkaMessage(
                91L,
                1_700_000_000_123L,
                "key".getBytes(StandardCharsets.UTF_8),
                null,
                List.of(
                        new KafkaMessage.KafkaHeader("trace", new byte[] {1, 2}),
                        new KafkaMessage.KafkaHeader("trace", null)));
        var payload = KafkaMemoryRecords.encode(source);
        try {
            List<KafkaMessage> decoded = KafkaStorageEntryDecoder.decode(payload, source.offset(), 1);

            assertThat(decoded).singleElement().satisfies(message -> {
                assertThat(message.offset()).isEqualTo(source.offset());
                assertThat(message.timestamp()).isEqualTo(source.timestamp());
                assertThat(message.key()).isEqualTo(source.key());
                assertThat(message.value()).isNull();
                assertThat(message.headers()).hasSize(2);
                assertThat(message.headers().get(0).value()).containsExactly(1, 2);
                assertThat(message.headers().get(1).value()).isNull();
            });
            assertThat(payload.refCnt()).isOne();
        } finally {
            payload.release();
        }
    }
}
