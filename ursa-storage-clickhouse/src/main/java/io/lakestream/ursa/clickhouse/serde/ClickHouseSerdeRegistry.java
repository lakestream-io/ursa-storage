/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.clickhouse.serde;

import io.lakestream.ursa.materialization.serde.EntrySerdeFactory;
import io.lakestream.ursa.materialization.serde.EntrySerdeFactory.SerdeType;
import io.lakestream.ursa.materialization.serde.kafka.KafkaSchemaService;

/**
 * Registers the ClickHouse source encoders with the generic {@link EntrySerdeFactory}. The static
 * initializer runs when the class is first loaded; {@link EntrySerdeFactory}'s static initializer
 * eagerly loads this class via {@code Class.forName}, so callers never invoke
 * {@link #ensureRegistered()} explicitly. Each provider returns {@code null} when the supplied
 * {@code SchemaService} is not the matching source type, so the registry is inert for non-ClickHouse
 * deployments.
 */
public final class ClickHouseSerdeRegistry {

    private static volatile boolean initialized;

    private ClickHouseSerdeRegistry() {
    }

    /** No-op kept for back-compat / explicit "touch this class"; registration is idempotent. */
    public static synchronized void ensureRegistered() {
        if (initialized) {
            return;
        }
        EntrySerdeFactory.registerEncoderProvider(SerdeType.KAFKA_CLICKHOUSE,
                schemaService -> schemaService instanceof KafkaSchemaService
                        ? new KafkaEntryToClickHouseRowEncoder(schemaService) : null);
        initialized = true;
    }

    static {
        ensureRegistered();
    }
}
