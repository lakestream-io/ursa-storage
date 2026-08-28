/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.clickhouse.serde;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

import io.lakestream.ursa.materialization.serde.EntrySerdeFactory;
import io.lakestream.ursa.materialization.serde.EntrySerdeFactory.SerdeType;
import io.lakestream.ursa.materialization.serde.kafka.KafkaSchemaService;
import org.junit.jupiter.api.Test;

/**
 * Verifies that the shared serde registry discovers the optional ClickHouse integration and
 * exposes the source-specific row encoders.
 */
public class ClickHouseSerdeRegistrationTest {

    @Test
    public void kafkaEncoderIsRegistered() {
        EntrySerdeFactory factory = new EntrySerdeFactory(mock(KafkaSchemaService.class));

        assertThat(factory.getEncoder(SerdeType.KAFKA_CLICKHOUSE))
                .isInstanceOf(KafkaEntryToClickHouseRowEncoder.class);
    }
}
