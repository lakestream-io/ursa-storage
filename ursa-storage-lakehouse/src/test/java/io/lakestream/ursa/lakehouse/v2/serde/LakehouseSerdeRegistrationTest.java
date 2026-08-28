/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.lakehouse.v2.serde;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

import io.lakestream.ursa.materialization.serde.EntrySerdeFactory;
import io.lakestream.ursa.materialization.serde.EntrySerdeFactory.SerdeType;
import io.lakestream.ursa.materialization.serde.kafka.KafkaSchemaService;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * Locks the wiring between {@link LakehouseSerdeRegistry} and {@link EntrySerdeFactory}.
 * Simply constructing an {@link EntrySerdeFactory} must be enough: the factory's static
 * initializer eagerly loads known registry classes (including
 * {@link LakehouseSerdeRegistry}), so every supported {@link SerdeType} resolves to a
 * non-null encoder/decoder without callers needing to invoke a separate bootstrap method.
 *
 * <p>This guards against a regression where the materialization module's pluggable provider
 * registry is left empty because nothing triggered the registry class load.
 */
@Tag("lakehouse")
public class LakehouseSerdeRegistrationTest {

    @Test
    public void rawKafkaProviderIsRegistered() {
        EntrySerdeFactory factory = new EntrySerdeFactory(mock(KafkaSchemaService.class));

        assertThat(factory.getEncoder(SerdeType.KAFKA_BATCHED_RAW_PARQUET)).isNotNull();
        assertThat(factory.getDecoder(SerdeType.KAFKA_BATCHED_RAW_PARQUET)).isNotNull();
    }

    @Test
    public void kafkaSinkProvidersWireUpWithKafkaSchemaService() {
        EntrySerdeFactory factory = new EntrySerdeFactory(mock(KafkaSchemaService.class));

        assertThat(factory.getEncoder(SerdeType.KAFKA_PARQUET)).isNotNull();
        assertThat(factory.getDecoder(SerdeType.KAFKA_PARQUET)).isNotNull();
        assertThat(factory.getEncoder(SerdeType.KAFKA_ICEBERG)).isNotNull();
        assertThat(factory.getEncoder(SerdeType.KAFKA_DELTA)).isNotNull();
    }
}
