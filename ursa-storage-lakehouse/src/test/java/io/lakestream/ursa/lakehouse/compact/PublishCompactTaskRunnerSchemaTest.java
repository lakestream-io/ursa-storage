/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.lakehouse.compact;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.confluent.kafka.schemaregistry.client.SchemaMetadata;
import io.lakestream.api.EntryHeader;
import io.lakestream.api.EntryIndex;
import io.lakestream.api.Position;
import io.lakestream.ursa.compaction.CompactTaskManager;
import io.lakestream.ursa.compaction.metrics.CompactionMetrics;
import io.lakestream.ursa.lakehouse.schema.SchemaRegistry;
import io.lakestream.ursa.materialization.serde.kafka.KafkaSourceMetadata;
import io.lakestream.ursa.storage.StorageApi;
import io.lakestream.ursa.storage.impl.StorageConfig;
import io.lakestream.ursa.storage.impl.compaction.TopicManager;
import io.lakestream.ursa.storage.impl.compaction.TopicMetadata;
import io.lakestream.ursa.storage.impl.compaction.TopicProvider;
import io.opentelemetry.api.metrics.LongGauge;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Covers the schema pre-check of {@link PublishCompactTaskRunner}: the registry subject must be derived
 * from the logical Kafka topic carried in the stream properties rather than from the UUID-qualified
 * stream name (GitHub issue #13).
 */
class PublishCompactTaskRunnerSchemaTest {

    private static final String LOGICAL_TOPIC = "orders";
    private static final String STREAM =
            "default/orders-topic-id-PN--gt4BTtCOfFofVthDgw-partition-0";
    private static final String STREAM_LOCAL_NAME = "orders-topic-id-PN--gt4BTtCOfFofVthDgw";
    private static final long STREAM_ID = 7L;
    private static final SchemaMetadata AVRO = new SchemaMetadata(1, 1, "AVRO", null, "{}");

    private StorageApi storageApi;
    private CompactTaskManager taskManager;
    private SchemaRegistry schemaRegistry;
    private PublishCompactTaskRunner runner;

    @BeforeEach
    void setUp() throws Exception {
        storageApi = mock(StorageApi.class);
        taskManager = mock(CompactTaskManager.class);
        schemaRegistry = mock(SchemaRegistry.class);
        CompactionMetrics metrics = mock(CompactionMetrics.class);
        when(metrics.getLatestMessageOffset()).thenReturn(mock(LongGauge.class));
        when(metrics.getLatestPublishedOffset()).thenReturn(mock(LongGauge.class));
        when(metrics.getCompactionLag()).thenReturn(mock(LongGauge.class));
        when(metrics.getPublishedTaskBytes()).thenReturn(mock(LongGauge.class));
        runner = new PublishCompactTaskRunner(
                storageApi,
                taskManager,
                mock(TopicManager.class),
                mock(ExecutorService.class),
                mock(ScheduledExecutorService.class),
                mock(TopicProvider.class),
                new StorageConfig(),
                schemaRegistry,
                metrics);
        stubStreamWithData();
    }

    @Test
    void looksUpSchemaByLogicalNameProperty() throws Exception {
        when(schemaRegistry.fetchLatest(LOGICAL_TOPIC)).thenReturn(AVRO);

        runner.publishStreamCompactTask(STREAM, metadata(
                Map.of(KafkaSourceMetadata.LOGICAL_NAME_PROPERTY, LOGICAL_TOPIC)));

        verify(schemaRegistry).fetchLatest(LOGICAL_TOPIC);
        verify(schemaRegistry, never()).fetchLatest(STREAM);
        verify(schemaRegistry, never()).fetchLatest(STREAM_LOCAL_NAME);
        verify(taskManager).publishCompactTask(any());
    }

    @Test
    void looksUpSchemaByLegacyKafkaTopicNameProperty() throws Exception {
        when(schemaRegistry.fetchLatest(LOGICAL_TOPIC)).thenReturn(AVRO);

        runner.publishStreamCompactTask(STREAM, metadata(
                Map.of(KafkaSourceMetadata.TOPIC_NAME_PROPERTY, LOGICAL_TOPIC)));

        verify(schemaRegistry).fetchLatest(LOGICAL_TOPIC);
        verify(taskManager).publishCompactTask(any());
    }

    @Test
    void fallsBackToPartitionedLocalNameWithoutSourceMetadata() throws Exception {
        when(schemaRegistry.fetchLatest(anyString())).thenReturn(AVRO);

        runner.publishStreamCompactTask("default/plain-partition-3", metadata(Map.of()));

        verify(schemaRegistry).fetchLatest("plain");
    }

    @Test
    void cachesSupportedSchemaByLogicalTopicAcrossPartitions() throws Exception {
        when(schemaRegistry.fetchLatest(LOGICAL_TOPIC)).thenReturn(AVRO);
        Map<String, String> properties = Map.of(KafkaSourceMetadata.LOGICAL_NAME_PROPERTY, LOGICAL_TOPIC);

        runner.publishStreamCompactTask(STREAM, metadata(properties));
        runner.publishStreamCompactTask(
                "default/orders-topic-id-PN--gt4BTtCOfFofVthDgw-partition-1", metadata(properties));
        runner.publishStreamCompactTask(STREAM, metadata(properties));

        verify(schemaRegistry, times(1)).fetchLatest(LOGICAL_TOPIC);
        verify(taskManager, times(3)).publishCompactTask(any());
    }

    @Test
    void quarantinesStreamWhoseLogicalTopicHasUnsupportedSchema() throws Exception {
        when(schemaRegistry.fetchLatest(LOGICAL_TOPIC))
                .thenReturn(new SchemaMetadata(1, 1, "THRIFT", null, "{}"));

        runner.publishStreamCompactTask(STREAM, metadata(
                Map.of(KafkaSourceMetadata.LOGICAL_NAME_PROPERTY, LOGICAL_TOPIC)));

        verify(taskManager, never()).publishCompactTask(any());
        verify(taskManager, never()).updatePublishedOffset(anyLong(), anyLong(), anyLong());
        assertThat(runner.getFetchSchemaFailedQuarantineTime())
                .containsKey("default/orders-topic-id-PN--gt4BTtCOfFofVthDgw");
    }

    private void stubStreamWithData() throws Exception {
        EntryHeader header = new EntryHeader(0L, 10, 0L, 100, 100L);
        when(storageApi.getLastEntry(STREAM_ID))
                .thenReturn(CompletableFuture.completedFuture(EntryIndex.of(header, Position.NOT_FOUND, 1, 1)));
        when(storageApi.readEntryHeader(eq(STREAM_ID), anyLong()))
                .thenReturn(CompletableFuture.completedFuture(EntryHeader.NOT_FOUND));
        when(taskManager.getPreparedStreamTask(STREAM_ID)).thenReturn(null);
        when(taskManager.getPublishedOffset(STREAM_ID)).thenReturn(null);
    }

    private static TopicMetadata metadata(Map<String, String> properties) {
        return new TopicMetadata(STREAM, STREAM_ID, properties);
    }
}
