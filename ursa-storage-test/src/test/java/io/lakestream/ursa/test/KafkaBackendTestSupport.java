/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.test;

import static org.assertj.core.api.Assertions.assertThat;

import io.lakestream.ursa.materialization.serde.KafkaEntry;
import io.netty.buffer.ByteBuf;
import java.nio.charset.StandardCharsets;

public final class KafkaBackendTestSupport {

    static final byte[] KEY = "customer-17".getBytes(StandardCharsets.UTF_8);
    static final byte[] VALUE = "created".getBytes(StandardCharsets.UTF_8);

    private KafkaBackendTestSupport() {
    }

    public static ByteBuf frame() {
        return new KafkaEntry(KEY, VALUE).toByteBuf();
    }

    public static void assertFrame(ByteBuf buffer) {
        KafkaEntry entry = KafkaEntry.fromByteBuf(buffer.duplicate());
        assertThat(entry.key()).isEqualTo(KEY);
        assertThat(entry.value()).isEqualTo(VALUE);
    }
}
