/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.lakestream.impl;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.lakestream.api.LogEntry;
import io.lakestream.api.LogId;
import io.lakestream.api.StreamReader;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.Test;

class CatalogOwnedStreamReaderTest {

    @Test
    void canceledReadStillDrainsAndReleasesLateEntriesBeforeClose() throws Exception {
        StreamReader delegate = mock(StreamReader.class);
        LogEntry entry = mock(LogEntry.class);
        CompletableFuture<StreamReader.ReadResult> source = new CompletableFuture<>();
        when(delegate.read(LogId.of(1L), 0L, 1, 1024L)).thenReturn(source);
        AtomicBoolean handleReleased = new AtomicBoolean();
        CatalogOwnedStreamReader reader = new CatalogOwnedStreamReader(
            delegate, Runnable::run, () -> handleReleased.set(true));

        CompletableFuture<StreamReader.ReadResult> exposed =
            reader.read(LogId.of(1L), 0L, 1, 1024L);
        assertTrue(exposed.cancel(false));
        CompletableFuture<Void> close = CompletableFuture.runAsync(() -> {
            try {
                reader.close();
            } catch (Exception failure) {
                throw new RuntimeException(failure);
            }
        });

        assertFalse(close.isDone());
        verify(delegate, never()).close();
        assertFalse(handleReleased.get());

        source.complete(new StreamReader.ReadResult(List.of(entry), 1L));
        close.get(5, TimeUnit.SECONDS);

        verify(entry).close();
        verify(delegate).close();
        assertTrue(handleReleased.get());
    }

    @Test
    void canceledReadCleansLateEntriesBeforeDelegateAndHandleAreReleased()
            throws Exception {
        StreamReader delegate = mock(StreamReader.class);
        LogEntry entry = mock(LogEntry.class);
        CountDownLatch entryCloseStarted = new CountDownLatch(1);
        CountDownLatch allowEntryClose = new CountDownLatch(1);
        doAnswer(ignored -> {
            entryCloseStarted.countDown();
            allowEntryClose.await();
            return null;
        }).when(entry).close();
        CompletableFuture<StreamReader.ReadResult> source = new CompletableFuture<>();
        when(delegate.read(LogId.of(3L), 0L, 1, 1024L)).thenReturn(source);
        AtomicBoolean handleReleased = new AtomicBoolean();
        CatalogOwnedStreamReader reader = new CatalogOwnedStreamReader(
            delegate, Runnable::run, () -> handleReleased.set(true));
        CompletableFuture<StreamReader.ReadResult> exposed =
            reader.read(LogId.of(3L), 0L, 1, 1024L);
        assertTrue(exposed.cancel(false));

        CompletableFuture<Void> sourceCompletion = CompletableFuture.runAsync(() ->
            source.complete(new StreamReader.ReadResult(List.of(entry), 1L)));
        assertTrue(entryCloseStarted.await(5, TimeUnit.SECONDS));
        CompletableFuture<Void> close = CompletableFuture.runAsync(() -> {
            try {
                reader.close();
            } catch (Exception failure) {
                throw new RuntimeException(failure);
            }
        });

        assertFalse(close.isDone());
        verify(delegate, never()).close();
        assertFalse(handleReleased.get());

        allowEntryClose.countDown();
        sourceCompletion.get(5, TimeUnit.SECONDS);
        close.get(5, TimeUnit.SECONDS);
        verify(delegate).close();
        assertTrue(handleReleased.get());
    }

    @Test
    void delegateCloseFailureRetainsCatalogHandleUntilRetry() throws Exception {
        StreamReader delegate = mock(StreamReader.class);
        IOException closeFailure = new IOException("reader close failed");
        doThrow(closeFailure).doNothing().when(delegate).close();
        AtomicBoolean handleReleased = new AtomicBoolean();
        CatalogOwnedStreamReader reader = new CatalogOwnedStreamReader(
            delegate, Runnable::run, () -> handleReleased.set(true));

        assertThrows(IOException.class, reader::close);
        assertFalse(handleReleased.get());

        reader.close();
        reader.close();

        verify(delegate, times(2)).close();
        assertTrue(handleReleased.get());
    }

    @Test
    void inlineReadCompletionCanCloseWithoutWaitingOnItself() throws Exception {
        StreamReader delegate = mock(StreamReader.class);
        CompletableFuture<StreamReader.ReadResult> source = new CompletableFuture<>();
        when(delegate.read(LogId.of(2L), 0L, 1, 1024L)).thenReturn(source);
        AtomicBoolean handleReleased = new AtomicBoolean();
        CatalogOwnedStreamReader reader = new CatalogOwnedStreamReader(
            delegate, Runnable::run, () -> handleReleased.set(true));

        CompletableFuture<Void> callback = reader.read(LogId.of(2L), 0L, 1, 1024L)
            .thenAccept(result -> {
                try {
                    reader.close();
                } catch (Exception failure) {
                    throw new CompletionException(failure);
                }
            });
        source.complete(new StreamReader.ReadResult(List.of(), 0L));

        callback.get(5, TimeUnit.SECONDS);
        verify(delegate).close();
        assertTrue(handleReleased.get());
    }

    @Test
    void blockedDelegateCloseTimesOutWithoutReleasingCatalogHandle() throws Exception {
        StreamReader delegate = mock(StreamReader.class);
        CountDownLatch closeStarted = new CountDownLatch(1);
        CountDownLatch allowClose = new CountDownLatch(1);
        CountDownLatch closeFinished = new CountDownLatch(1);
        doAnswer(ignored -> {
            closeStarted.countDown();
            allowClose.await();
            closeFinished.countDown();
            return null;
        }).when(delegate).close();
        AtomicBoolean handleReleased = new AtomicBoolean();
        ExecutorService delegateCloseExecutor = Executors.newSingleThreadExecutor();
        try {
            CatalogOwnedStreamReader reader = new CatalogOwnedStreamReader(
                delegate, delegateCloseExecutor, () -> handleReleased.set(true), 25L);

            IOException timeout = assertThrows(IOException.class, reader::close);

            assertTrue(timeout.getMessage().contains("delegate stream reader"));
            assertTrue(closeStarted.await(5, TimeUnit.SECONDS));
            assertFalse(handleReleased.get());

            allowClose.countDown();
            assertTrue(closeFinished.await(5, TimeUnit.SECONDS));
            reader.close();

            verify(delegate).close();
            assertTrue(handleReleased.get());
        } finally {
            allowClose.countDown();
            delegateCloseExecutor.shutdownNow();
        }
    }
}
