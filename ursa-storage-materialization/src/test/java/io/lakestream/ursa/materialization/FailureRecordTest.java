/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.materialization;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNullPointerException;

import io.lakestream.api.StreamIdentifier;
import io.lakestream.api.materialization.TableCatalogType;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.util.Optional;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class FailureRecordTest {

    private static final StreamIdentifier STREAM = StreamIdentifier.of("public/default", "events");

    private ByteBuf payload;

    @BeforeEach
    void setUp() {
        payload = Unpooled.wrappedBuffer(new byte[]{1, 2, 3});
    }

    @AfterEach
    void tearDown() {
        if (payload != null && payload.refCnt() > 0) {
            payload.release();
        }
    }

    @Test
    void rejectsNullStream() {
        assertThatNullPointerException()
                .isThrownBy(() -> new FailureRecord(
                        null, TableCatalogType.ICEBERG, Optional.empty(), "r", payload))
                .withMessageContaining("stream");
    }

    @Test
    void rejectsNullCatalogType() {
        assertThatNullPointerException()
                .isThrownBy(() -> new FailureRecord(
                        STREAM, null, Optional.empty(), "r", payload))
                .withMessageContaining("catalogType");
    }

    @Test
    void rejectsNullDlqTopic() {
        assertThatNullPointerException()
                .isThrownBy(() -> new FailureRecord(
                        STREAM, TableCatalogType.ICEBERG, null, "r", payload))
                .withMessageContaining("dlqTopic");
    }

    @Test
    void rejectsNullReason() {
        assertThatNullPointerException()
                .isThrownBy(() -> new FailureRecord(
                        STREAM, TableCatalogType.ICEBERG, Optional.empty(), null, payload))
                .withMessageContaining("reason");
    }

    @Test
    void rejectsNullPayload() {
        assertThatNullPointerException()
                .isThrownBy(() -> new FailureRecord(
                        STREAM, TableCatalogType.ICEBERG, Optional.empty(), "r", null))
                .withMessageContaining("payload");
    }

    @Test
    void preservesAllFields() {
        FailureRecord record = new FailureRecord(
                STREAM,
                TableCatalogType.CLICKHOUSE,
                Optional.of("dlq.events"),
                "parse failure",
                payload);

        assertThat(record.stream()).isEqualTo(STREAM);
        assertThat(record.catalogType()).isEqualTo(TableCatalogType.CLICKHOUSE);
        assertThat(record.dlqTopic()).contains("dlq.events");
        assertThat(record.reason()).isEqualTo("parse failure");
        assertThat(record.payload()).isSameAs(payload);
    }
}
