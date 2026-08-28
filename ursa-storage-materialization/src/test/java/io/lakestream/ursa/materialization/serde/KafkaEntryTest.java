/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.materialization.serde;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.netty.buffer.Unpooled;
import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.Test;

class KafkaEntryTest {

    @Test
    void roundTripsKeyAndValue() {
        KafkaEntry expected = new KafkaEntry(
                "key".getBytes(StandardCharsets.UTF_8),
                "value".getBytes(StandardCharsets.UTF_8));
        var buffer = expected.toByteBuf();
        try {
            KafkaEntry actual = KafkaEntry.fromByteBuf(buffer);
            assertThat(actual.key()).isEqualTo(expected.key());
            assertThat(actual.value()).isEqualTo(expected.value());
        } finally {
            buffer.release();
        }
    }

    @Test
    void preservesNullSeparatelyFromEmpty() {
        var nullBuffer = new KafkaEntry(null, null).toByteBuf();
        var emptyBuffer = new KafkaEntry(new byte[0], new byte[0]).toByteBuf();
        try {
            KafkaEntry nullEntry = KafkaEntry.fromByteBuf(nullBuffer);
            KafkaEntry emptyEntry = KafkaEntry.fromByteBuf(emptyBuffer);
            assertThat(nullEntry.key()).isNull();
            assertThat(nullEntry.value()).isNull();
            assertThat(emptyEntry.key()).isEmpty();
            assertThat(emptyEntry.value()).isEmpty();
        } finally {
            nullBuffer.release();
            emptyBuffer.release();
        }
    }

    @Test
    void rejectsLengthsOutsideTheFrame() {
        var buffer = Unpooled.buffer().writeInt(100).writeInt(0);
        try {
            assertThatThrownBy(() -> KafkaEntry.fromByteBuf(buffer))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("key length exceeds");
        } finally {
            buffer.release();
        }
    }
}
