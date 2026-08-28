/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.lakehouse.compact;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.confluent.kafka.schemaregistry.client.SchemaMetadata;
import io.lakestream.api.EntryHeader;
import io.lakestream.api.EntryIndex;
import io.lakestream.api.Position;
import io.lakestream.ursa.compaction.CompactTaskManager;
import io.lakestream.ursa.compaction.metrics.CompactionMetrics;
import io.lakestream.ursa.compaction.task.CompactedOffset;
import io.lakestream.ursa.compaction.task.PreparedCompactStreamTask;
import io.lakestream.ursa.lakehouse.schema.SchemaRegistry;
import io.lakestream.ursa.storage.StorageApi;
import io.lakestream.ursa.storage.impl.StorageConfig;
import io.lakestream.ursa.storage.impl.compaction.TopicManager;
import io.lakestream.ursa.storage.impl.compaction.TopicMetadata;
import io.lakestream.ursa.storage.impl.compaction.TopicProvider;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.metrics.LongGauge;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.InOrder;

class PublishCompactTaskRunnerOffsetTest {

    private static final String TOPIC = "default/test-partition-0";
    private static final long STREAM_ID = 42L;
    private static final Attributes TOPIC_ATTRIBUTES =
            Attributes.of(AttributeKey.stringKey("topic"), TOPIC);

    private StorageApi storageApi;
    private CompactTaskManager taskManager;
    private CompactionMetrics metrics;
    private LongGauge latestMessageOffset;
    private LongGauge latestPublishedOffset;
    private LongGauge compactionLag;
    private LongGauge publishedTaskBytes;
    private PublishCompactTaskRunner runner;

    @BeforeEach
    void setUp() throws Exception {
        storageApi = mock(StorageApi.class);
        taskManager = mock(CompactTaskManager.class);
        metrics = mock(CompactionMetrics.class);
        latestMessageOffset = mock(LongGauge.class);
        latestPublishedOffset = mock(LongGauge.class);
        compactionLag = mock(LongGauge.class);
        publishedTaskBytes = mock(LongGauge.class);
        when(metrics.getLatestMessageOffset()).thenReturn(latestMessageOffset);
        when(metrics.getLatestPublishedOffset()).thenReturn(latestPublishedOffset);
        when(metrics.getCompactionLag()).thenReturn(compactionLag);
        when(metrics.getPublishedTaskBytes()).thenReturn(publishedTaskBytes);

        SchemaRegistry schemaRegistry = mock(SchemaRegistry.class);
        when(schemaRegistry.fetchLatest(TOPIC)).thenReturn(new SchemaMetadata(1, 1, "AVRO", null, "{}"));
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
    }

    @ParameterizedTest
    @ValueSource(ints = {PreparedCompactStreamTask.INIT, PreparedCompactStreamTask.PUSHED_TASK})
    void recoveryPublishesLastIncludedOffsetMetricsAfterCursorUpdate(int status) throws Exception {
        PreparedCompactStreamTask task = task(status, 0L, 10L);
        when(taskManager.getPreparedStreamTask(STREAM_ID)).thenReturn(task);
        when(taskManager.getPublishedOffset(STREAM_ID))
                .thenReturn(new CompactedOffset(STREAM_ID, 9L, 100L));
        stubLastEntry(new EntryHeader(0L, 10, 0L, 100, 100L));
        when(storageApi.readEntryHeader(eq(STREAM_ID), anyLong()))
                .thenReturn(CompletableFuture.completedFuture(EntryHeader.NOT_FOUND));

        runner.publishStreamCompactTask(TOPIC, topicMetadata());

        InOrder order = inOrder(taskManager, latestPublishedOffset, compactionLag);
        order.verify(taskManager).updatePublishedOffset(STREAM_ID, 9L, 100L);
        order.verify(latestPublishedOffset).set(9L, TOPIC_ATTRIBUTES);
        order.verify(compactionLag).set(0L, TOPIC_ATTRIBUTES);
        verify(latestMessageOffset).set(9L, TOPIC_ATTRIBUTES);
        verify(taskManager).deletePreparedCompactTask(STREAM_ID);
    }

    @Test
    void newTaskUpdatesPublishedMetricsOnlyAfterCursorUpdate() throws Exception {
        when(taskManager.getPreparedStreamTask(STREAM_ID)).thenReturn(null);
        when(taskManager.getPublishedOffset(STREAM_ID)).thenReturn(null);
        stubLastEntry(new EntryHeader(0L, 10, 0L, 100, 100L));
        when(storageApi.readEntryHeader(eq(STREAM_ID), anyLong()))
                .thenReturn(CompletableFuture.completedFuture(EntryHeader.NOT_FOUND));

        runner.publishStreamCompactTask(TOPIC, topicMetadata());

        InOrder order = inOrder(taskManager, latestPublishedOffset, compactionLag);
        order.verify(taskManager).updatePublishedOffset(STREAM_ID, 9L, 100L);
        order.verify(latestPublishedOffset).set(9L, TOPIC_ATTRIBUTES);
        order.verify(compactionLag).set(0L, TOPIC_ATTRIBUTES);
    }

    @Test
    void recoveryRejectsEmptyRangeBeforePublishingOrAdvancingCursor() throws Exception {
        when(taskManager.getPreparedStreamTask(STREAM_ID))
                .thenReturn(task(PreparedCompactStreamTask.INIT, 10L, 10L));

        assertThrows(IllegalArgumentException.class,
                () -> runner.publishStreamCompactTask(TOPIC, topicMetadata()));

        verify(taskManager, never()).publishCompactTask(any());
        verify(taskManager, never()).updatePublishedOffset(anyLong(), anyLong(), anyLong());
    }

    private void stubLastEntry(EntryHeader header) {
        EntryIndex entryIndex = EntryIndex.of(header, Position.NOT_FOUND, 1, 1);
        when(storageApi.getLastEntry(STREAM_ID)).thenReturn(CompletableFuture.completedFuture(entryIndex));
    }

    private static TopicMetadata topicMetadata() {
        return new TopicMetadata(TOPIC, STREAM_ID, Map.of());
    }

    private static PreparedCompactStreamTask task(int status, long startOffset, long endOffset) {
        return new PreparedCompactStreamTask(
                STREAM_ID,
                startOffset,
                endOffset,
                100L,
                100L,
                status,
                "task",
                TOPIC,
                Collections.emptyMap());
    }
}
