/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.lakehouse.compact;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.lakestream.ursa.compaction.CompactTaskManager;
import io.lakestream.ursa.compaction.metrics.CompactionMetrics;
import io.lakestream.ursa.compaction.task.CompactStreamTask;
import io.lakestream.ursa.exception.ExceptionCode;
import io.lakestream.ursa.lakehouse.v2.LakehouseFactory;
import io.lakestream.ursa.materialization.MaterializationException;
import io.lakestream.ursa.storage.impl.StorageConfig;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag("lakehouse")
class LakehouseCompactionWorkerTest {

    @Test
    void averageEntrySizeUsesHalfOpenRangeLength() {
        double updated = LakehouseCompactionWorker.updatedAverageEntrySize(
                1_024.0, 400L, 10L, 14L);

        // Four entries in [10, 14), so the observed average is 100 bytes.
        assertThat(updated).isEqualTo(1_024.0 * 0.9 + 100.0 * 0.1);
    }

    @Test
    void averageEntrySizeRejectsEmptyRange() {
        assertThatThrownBy(() -> LakehouseCompactionWorker.updatedAverageEntrySize(
                1_024.0, 0L, 10L, 10L))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("[10, 10)");
    }

    @Test
    void terminalSourceFailureRetiresTaskFromRuntimeExceptionCode() throws Exception {
        CompactStreamTask task = new CompactStreamTask();
        task.setTopic("default/orders-partition-0-topic-id");
        task.setTaskName("orders-incarnation-mismatch");
        task.setStreamId(17L);
        task.setStartOffset(0L);
        task.setEndOffset(10L);

        EntryProcessFactory mismatchedReaderFactory = new EntryProcessFactory() {
            @Override
            public IEntryReader createEntryReader(String topic, long streamId, long startOffset,
                                                  long endOffset, double avgEntrySize,
                                                  EntryReaderOptions options) {
                throw new MaterializationException(
                        ExceptionCode.NO_SUCH_LOG, "Source log no longer exists");
            }

            @Override
            public void close() {
            }
        };
        CompactTaskManager taskManager = mock(CompactTaskManager.class);
        when(taskManager.deleteCompactTask(task)).thenReturn(CompletableFuture.completedFuture(null));
        LakehouseCompactionWorker worker = new LakehouseCompactionWorker(
                mock(LakehouseFactory.class), mismatchedReaderFactory, taskManager,
                mock(CompactionMetrics.class), new StorageConfig());

        worker.doCompact(task);

        verify(taskManager).deleteCompactTask(task);
    }

    @Test
    void interruptedTerminalTaskDeletionPropagatesAndStopsWorkerInvocation() throws Exception {
        CompactStreamTask task = new CompactStreamTask();
        task.setTopic("default/orders-partition-0-topic-id");
        task.setTaskName("interrupted-incarnation-mismatch");
        task.setStreamId(17L);
        task.setStartOffset(0L);
        task.setEndOffset(10L);
        EntryProcessFactory mismatchedReaderFactory = new EntryProcessFactory() {
            @Override
            public IEntryReader createEntryReader(String topic, long streamId, long startOffset,
                                                  long endOffset, double avgEntrySize,
                                                  EntryReaderOptions options) {
                throw new MaterializationException(
                        ExceptionCode.NO_SUCH_LOG, "Source log no longer exists");
            }

            @Override
            public void close() {
            }
        };
        CompactTaskManager taskManager = mock(CompactTaskManager.class);
        CountDownLatch deleteStarted = new CountDownLatch(1);
        when(taskManager.deleteCompactTask(task)).thenAnswer(invocation -> {
            deleteStarted.countDown();
            return new CompletableFuture<Void>();
        });
        LakehouseCompactionWorker worker = new LakehouseCompactionWorker(
                mock(LakehouseFactory.class), mismatchedReaderFactory, taskManager,
                mock(CompactionMetrics.class), new StorageConfig());
        AtomicReference<Throwable> failure = new AtomicReference<>();
        Thread runner = new Thread(() -> {
            try {
                worker.doCompact(task);
            } catch (Throwable error) {
                failure.set(error);
            }
        });

        runner.start();
        assertThat(deleteStarted.await(10, TimeUnit.SECONDS)).isTrue();
        runner.interrupt();
        runner.join(10_000L);

        assertThat(runner.isAlive()).isFalse();
        assertThat(runner.isInterrupted()).isTrue();
        assertThat(failure.get()).isInstanceOf(InterruptedException.class);
    }
}
