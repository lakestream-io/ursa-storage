/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.lakehouse.reader;

import io.lakestream.ursa.lakehouse.LakehouseConfiguration;
import io.lakestream.ursa.lakehouse.LakehouseKafkaReaderV2;
import io.lakestream.ursa.lakehouse.v2.LakehouseFactory;
import io.lakestream.ursa.lakestream.reader.CompactedObjectReader;
import io.lakestream.ursa.lakestream.reader.CompactedObjectReaderFactory;
import io.lakestream.ursa.materialization.serde.kafka.KafkaSchemaService;
import io.lakestream.ursa.metrics.InstrumentProvider;
import java.util.Properties;

public class LakehouseReaderFactory implements CompactedObjectReaderFactory {

    private KafkaSchemaService kafkaSchemaService;
    private LakehouseFactory lakehouseFactory;

    public LakehouseReaderFactory() {}

    @Override
    public void initialize(Properties properties, InstrumentProvider provider) throws Exception {
        this.kafkaSchemaService = new KafkaSchemaService(KafkaSchemaRegistryClients.create(properties), false);
        LakehouseConfiguration lakehouseConfiguration = new LakehouseConfiguration(properties);
        lakehouseConfiguration.setAllowApproximateMatching();
        this.lakehouseFactory = new LakehouseFactory(lakehouseConfiguration, kafkaSchemaService, provider);
    }

    @Override
    public CompactedObjectReader open(String logName) {
        return new LakehouseKafkaReaderV2(logName, lakehouseFactory);
    }

    @Override
    public void close() {
        if (kafkaSchemaService != null) {
            try {
                kafkaSchemaService.close();
            } catch (Exception ignored) {

            }
        }
        if (lakehouseFactory != null) {
            try {
                lakehouseFactory.close();
            } catch (Exception ignored) {

            }
        }
    }
}
