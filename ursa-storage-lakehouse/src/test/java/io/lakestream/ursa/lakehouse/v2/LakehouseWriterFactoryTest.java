/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.lakehouse.v2;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.lakestream.api.Stream;
import io.lakestream.api.StreamIdentifier;
import io.lakestream.api.materialization.TableCatalog;
import io.lakestream.api.materialization.TableCatalogType;
import io.lakestream.api.materialization.TableConf;
import io.lakestream.api.materialization.TableMaterializationPolicy;
import io.lakestream.api.materialization.TableMode;
import io.lakestream.ursa.lakehouse.LakehouseConfiguration;
import io.lakestream.ursa.lakehouse.compact.KafkaEntryProcessFactory;
import io.lakestream.ursa.materialization.serde.EntryFormat;
import io.lakestream.ursa.materialization.serde.kafka.KafkaSchemaService;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag("lakehouse")
class LakehouseWriterFactoryTest {

    @Test
    void recreatedKafkaTopicsKeepDistinctDestinationsButShareLogicalSchemaTopic() {
        Stream oldStream = mock(Stream.class);
        Stream newStream = mock(Stream.class);
        when(oldStream.identifier()).thenReturn(StreamIdentifier.of("default", "orders-old-topic-id"));
        when(newStream.identifier()).thenReturn(StreamIdentifier.of("default", "orders-new-topic-id"));
        Map<String, String> taskProperties = Map.of(
                KafkaEntryProcessFactory.SOURCE_TOPIC_PROPERTY, "orders-partition-3",
                KafkaEntryProcessFactory.SOURCE_SCHEMA_TOPIC_PROPERTY, "orders");

        assertThat(LakehouseWriterFactory.destinationTopic(oldStream))
                .isEqualTo("default/orders-old-topic-id");
        assertThat(LakehouseWriterFactory.destinationTopic(newStream))
                .isEqualTo("default/orders-new-topic-id");
        String oldSchemaTopic = LakehouseWriterFactory.schemaTopic(oldStream, taskProperties);
        String newSchemaTopic = LakehouseWriterFactory.schemaTopic(newStream, taskProperties);
        assertThat(oldSchemaTopic).isEqualTo("orders");
        assertThat(newSchemaTopic).isEqualTo("orders");

        KafkaSchemaService schemaService = new KafkaSchemaService(mock(SchemaRegistryClient.class), false);
        assertThat(schemaService.getSubject(oldSchemaTopic)).isEqualTo("orders-value");
        assertThat(schemaService.getSubject(newSchemaTopic)).isEqualTo("orders-value");
    }

    @Test
    void legacyPartitionTaskDerivesUnpartitionedSchemaSubject() {
        Stream stream = mock(Stream.class);
        when(stream.identifier()).thenReturn(StreamIdentifier.of("default", "orders-partition-3"));
        Map<String, String> legacyProperties = Map.of(
                KafkaEntryProcessFactory.SOURCE_TOPIC_PROPERTY, "orders-partition-3");

        String schemaTopic = LakehouseWriterFactory.schemaTopic(stream, legacyProperties);

        assertThat(schemaTopic).isEqualTo("orders");
        KafkaSchemaService schemaService = new KafkaSchemaService(mock(SchemaRegistryClient.class), false);
        assertThat(schemaService.getSubject(schemaTopic)).isEqualTo("orders-value");
    }

    @Test
    void externalDeltaDltCanBeDisabledForKafkaFailureRetryPolicy() {
        TableCatalog catalog = new TableCatalog(
                "delta-catalog",
                TableCatalogType.DELTA,
                Map.of("warehouse", "/tmp/ursa-test-delta"),
                Map.of("directExternalStoragePath", "/tmp/ursa-test-delta"));
        Stream stream = mock(Stream.class);
        when(stream.identifier()).thenReturn(StreamIdentifier.of("default", "delta-topic"));

        var dltWriter = LakehouseWriterFactory.externalDltWriter(
                withMode(TableMode.EXTERNAL),
                catalog,
                stream,
                "delta",
                EntryFormat.KAFKA,
                Map.of(LakehouseConfiguration.DELTA_DLT_ENABLED, "false"));

        assertThat(dltWriter).isEmpty();
    }

    private static TableMaterializationPolicy withMode(TableMode mode) {
        TableConf tableConf = new TableConf(
                Optional.of(mode),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty());
        return new TableMaterializationPolicy(
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.of(tableConf),
                Map.of());
    }
}
