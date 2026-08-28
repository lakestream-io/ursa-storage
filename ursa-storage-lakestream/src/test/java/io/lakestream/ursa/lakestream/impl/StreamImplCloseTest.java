/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.lakestream.impl;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import io.lakestream.api.LifecycleState;
import io.lakestream.api.LogId;
import io.lakestream.api.LogStateManager;
import io.lakestream.api.LogStorage;
import io.lakestream.api.Partitioning;
import io.lakestream.api.PartitioningStrategy;
import io.lakestream.api.SchemaConfig;
import io.lakestream.api.StreamConfig;
import io.lakestream.api.StreamIdentifier;
import io.lakestream.api.StreamLayout;
import io.lakestream.ursa.storage.impl.EntryIndexCache;
import java.io.IOException;
import java.lang.reflect.Field;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

class StreamImplCloseTest {

    @Test
    void closeAttemptsEveryResourceAndRetriesOnlyFailures() throws Exception {
        UnifiedStreamReader unifiedReader = mock(UnifiedStreamReader.class);
        doThrow(new IOException("reader close failed"))
                .doNothing()
                .when(unifiedReader).close();
        StreamImpl stream = new StreamImpl(
                StreamIdentifier.of("public/default", "close-test"),
                new StreamConfig(),
                new Partitioning(PartitioningStrategy.INDEXED, Map.of("numPartitions", "1")),
                new SchemaConfig(),
                Map.of(),
                LifecycleState.ACTIVE,
                mock(StreamLayout.class),
                mock(LogStorage.class),
                unifiedReader,
                mock(EntryIndexCache.class),
                mock(LogStateManager.class));
        LogImpl failedLog = mock(LogImpl.class);
        LogImpl closedLog = mock(LogImpl.class);
        doThrow(new IOException("log close failed"))
                .doNothing()
                .when(failedLog).close();
        logCache(stream).put(LogId.of(1L), failedLog);
        logCache(stream).put(LogId.of(2L), closedLog);

        assertThatThrownBy(stream::close)
                .isInstanceOf(IOException.class)
                .hasMessage("log close failed")
                .satisfies(error -> assertThatThrownBy(() -> {
                    throw error.getSuppressed()[0];
                }).isInstanceOf(IOException.class).hasMessage("reader close failed"));
        verify(closedLog).close();

        stream.close();

        verify(failedLog, times(2)).close();
        verify(closedLog).close();
        verify(unifiedReader, times(2)).close();
    }

    @Test
    void getLogIsRejectedAfterClose() throws Exception {
        StreamImpl stream = newStream(mock(UnifiedStreamReader.class));

        stream.close();

        assertThatThrownBy(() -> stream.getLog(LogId.of(1L)))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("is closed");
    }

    @Test
    void concurrentGetLogCannotEscapeAnInProgressClose() throws Exception {
        StreamImpl stream = newStream(mock(UnifiedStreamReader.class));
        LogImpl existingLog = mock(LogImpl.class);
        CountDownLatch closeEntered = new CountDownLatch(1);
        CountDownLatch allowClose = new CountDownLatch(1);
        doAnswer(invocation -> {
            closeEntered.countDown();
            if (!allowClose.await(10, TimeUnit.SECONDS)) {
                throw new AssertionError("Timed out waiting to finish stream close");
            }
            return null;
        }).when(existingLog).close();
        logCache(stream).put(LogId.of(1L), existingLog);

        CompletableFuture<Void> close = CompletableFuture.runAsync(() -> {
            try {
                stream.close();
            } catch (Exception error) {
                throw new AssertionError(error);
            }
        });
        assertThat(closeEntered.await(10, TimeUnit.SECONDS)).isTrue();
        CountDownLatch getLogAttempted = new CountDownLatch(1);
        CompletableFuture<?> getLog = CompletableFuture.supplyAsync(() -> {
            getLogAttempted.countDown();
            return stream.getLog(LogId.of(2L));
        });
        assertThat(getLogAttempted.await(10, TimeUnit.SECONDS)).isTrue();
        assertThat(getLog).isNotDone();

        allowClose.countDown();
        close.get(10, TimeUnit.SECONDS);
        assertThatThrownBy(() -> getLog.get(10, TimeUnit.SECONDS))
                .hasCauseInstanceOf(IllegalStateException.class)
                .hasRootCauseMessage("Stream public/default/concurrent-close-test is closed");
        assertThat(logCache(stream)).isEmpty();
    }

    private static StreamImpl newStream(UnifiedStreamReader unifiedReader) {
        return new StreamImpl(
                StreamIdentifier.of("public/default", "concurrent-close-test"),
                new StreamConfig(),
                new Partitioning(PartitioningStrategy.INDEXED, Map.of("numPartitions", "1")),
                new SchemaConfig(),
                Map.of(),
                LifecycleState.ACTIVE,
                mock(StreamLayout.class),
                mock(LogStorage.class),
                unifiedReader,
                mock(EntryIndexCache.class),
                mock(LogStateManager.class));
    }

    @SuppressWarnings("unchecked")
    private static Map<LogId, LogImpl> logCache(StreamImpl stream) throws Exception {
        Field field = StreamImpl.class.getDeclaredField("logCache");
        field.setAccessible(true);
        return (Map<LogId, LogImpl>) field.get(stream);
    }
}
