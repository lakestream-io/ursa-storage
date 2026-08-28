/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.lakehouse.v2.serde;

import io.lakestream.ursa.lakehouse.v2.serde.delta.KafkaEntryToDeltaRecordEncoder;
import io.lakestream.ursa.lakehouse.v2.serde.iceberg.KafkaEntryToIcebergRecordEncoder;
import io.lakestream.ursa.lakehouse.v2.serde.kafka.parquet.KafkaEntryBatchedRawDataToParquetDecoder;
import io.lakestream.ursa.lakehouse.v2.serde.kafka.parquet.KafkaEntryBatchedRawDataToParquetEncoder;
import io.lakestream.ursa.lakehouse.v2.serde.kafka.parquet.KafkaEntryToParquetDecoder;
import io.lakestream.ursa.lakehouse.v2.serde.kafka.parquet.KafkaEntryToParquetEncoder;
import io.lakestream.ursa.materialization.serde.EntrySerdeFactory;
import io.lakestream.ursa.materialization.serde.EntrySerdeFactory.SerdeType;
import io.lakestream.ursa.materialization.serde.kafka.KafkaSchemaService;

/**
 * Registers the lakehouse-specific source/sink encoders and decoders with the generic
 * {@link EntrySerdeFactory} registry. The static initializer below runs when the class is first
 * loaded; {@link EntrySerdeFactory}'s own static initializer eagerly loads this class (via
 * {@code Class.forName}), so callers never need to invoke {@link #ensureRegistered()} explicitly.
 * The method is kept as a public no-op for back-compat and as an explicit "touch this class"
 * affordance.
 */
public final class LakehouseSerdeRegistry {

    private static volatile boolean initialized;

    private LakehouseSerdeRegistry() {
    }

    /**
     * No-op kept for back-compat. Class loading via {@link EntrySerdeFactory}'s static
     * initializer already triggers the registrations below; calling this method simply
     * guarantees the class has been touched.
     */
    public static synchronized void ensureRegistered() {
        if (initialized) {
            return;
        }
        EntrySerdeFactory.registerEncoderProvider(SerdeType.KAFKA_BATCHED_RAW_PARQUET,
                schemaService -> new KafkaEntryBatchedRawDataToParquetEncoder());
        EntrySerdeFactory.registerDecoderProvider(SerdeType.KAFKA_BATCHED_RAW_PARQUET,
                schemaService -> new KafkaEntryBatchedRawDataToParquetDecoder());

        // Kafka source encoders/decoders that require a KafkaSchemaService.
        EntrySerdeFactory.registerEncoderProvider(SerdeType.KAFKA_PARQUET,
                schemaService -> schemaService instanceof KafkaSchemaService
                        ? new KafkaEntryToParquetEncoder(schemaService) : null);
        EntrySerdeFactory.registerDecoderProvider(SerdeType.KAFKA_PARQUET,
                schemaService -> schemaService instanceof KafkaSchemaService
                        ? new KafkaEntryToParquetDecoder(schemaService) : null);
        EntrySerdeFactory.registerEncoderProvider(SerdeType.KAFKA_ICEBERG,
                schemaService -> schemaService instanceof KafkaSchemaService
                        ? new KafkaEntryToIcebergRecordEncoder(schemaService) : null);
        EntrySerdeFactory.registerEncoderProvider(SerdeType.KAFKA_DELTA,
                schemaService -> schemaService instanceof KafkaSchemaService
                        ? new KafkaEntryToDeltaRecordEncoder(schemaService) : null);

        initialized = true;
    }

    static {
        ensureRegistered();
    }
}
