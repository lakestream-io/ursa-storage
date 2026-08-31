/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.test;

import static org.assertj.core.api.Assertions.assertThat;

import io.lakestream.ursa.materialization.serde.kafka.KafkaMemoryRecords;
import io.lakestream.ursa.materialization.serde.kafka.KafkaStorageEntryDecoder;
import io.lakestream.ursa.materialization.util.KafkaMessage;
import io.netty.buffer.ByteBuf;
import java.nio.charset.StandardCharsets;
import java.util.List;

public final class KafkaBackendTestSupport {

    static final byte[] KEY = "customer-17".getBytes(StandardCharsets.UTF_8);
    static final byte[] VALUE = "created".getBytes(StandardCharsets.UTF_8);

    private KafkaBackendTestSupport() {
    }

    public static ByteBuf payload() {
        return KafkaMemoryRecords.encode(new KafkaMessage(
                0L, 1_700_000_000_000L, KEY, VALUE, List.of()));
    }

    public static void assertPayload(ByteBuf buffer) {
        List<KafkaMessage> messages = KafkaStorageEntryDecoder.decode(buffer.duplicate(), 0L, 1);
        assertThat(messages).singleElement().satisfies(message -> {
            assertThat(message.key()).isEqualTo(KEY);
            assertThat(message.value()).isEqualTo(VALUE);
        });
    }
}
