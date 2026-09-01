/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.lakehouse.v2;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.lakestream.api.StreamIdentifier;
import io.lakestream.api.StreamMetadata;
import io.lakestream.api.materialization.TableCatalog;
import io.lakestream.api.materialization.TableCatalogType;
import io.lakestream.api.materialization.TableConf;
import io.lakestream.api.materialization.TableMaterializationPolicy;
import io.lakestream.api.materialization.TableMode;
import io.lakestream.ursa.lakehouse.LakehouseConfiguration;
import io.lakestream.ursa.materialization.serde.kafka.KafkaSchemaService;
import io.lakestream.ursa.materialization.serde.kafka.KafkaSourceMetadata;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag("lakehouse")
class LakehouseWriterFactoryTest {

    @Test
    void partitionedStorageStreamDerivesUnpartitionedSchemaSubject() {
        StreamMetadata stream = mock(StreamMetadata.class);
        when(stream.identifier()).thenReturn(StreamIdentifier.of("default", "orders-partition-3"));

        String schemaTopic = LakehouseWriterFactory.schemaTopic(stream, Map.of());

        assertThat(schemaTopic).isEqualTo("orders");
        KafkaSchemaService schemaService = new KafkaSchemaService(mock(SchemaRegistryClient.class), false);
        assertThat(schemaService.getSubject(schemaTopic)).isEqualTo("orders-value");
    }

    @Test
    void uuidQualifiedStorageStreamUsesRegisteredKafkaTopicForSchemaLookup() {
        StreamMetadata stream = mock(StreamMetadata.class);
        when(stream.identifier()).thenReturn(StreamIdentifier.of(
                "default", "orders-topic-id-65WMNfybQpCDVulYOxMCTw"));

        String schemaTopic = LakehouseWriterFactory.schemaTopic(
                stream, Map.of(KafkaSourceMetadata.TOPIC_NAME_PROPERTY, "orders"));

        assertThat(schemaTopic).isEqualTo("orders");
    }

    @Test
    void externalDeltaDltCanBeDisabledByTaskPolicy() {
        TableCatalog catalog = new TableCatalog(
                "delta-catalog",
                TableCatalogType.DELTA,
                Map.of("warehouse", "/tmp/ursa-test-delta"),
                Map.of("directExternalStoragePath", "/tmp/ursa-test-delta"));
        StreamMetadata stream = mock(StreamMetadata.class);
        when(stream.identifier()).thenReturn(StreamIdentifier.of("default", "delta-topic"));

        var dltWriter = LakehouseWriterFactory.externalDltWriter(
                withMode(TableMode.EXTERNAL),
                catalog,
                stream,
                "delta",
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
