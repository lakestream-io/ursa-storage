/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.lakehouse.compact;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.lakestream.api.EntryHeader;
import io.lakestream.ursa.compaction.metrics.CompactionMetrics;
import io.lakestream.ursa.lakehouse.exception.LakehouseException;
import io.lakestream.ursa.storage.Entry;
import io.lakestream.ursa.storage.StorageApi;
import io.netty.buffer.Unpooled;
import java.lang.reflect.Proxy;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;
import org.junit.jupiter.api.Test;

class EntryReaderOwnershipTest {

    @Test
    void testReleasesEntriesOutsideRequestedRange() throws Exception {
        Entry beforeStart = newEntry(0, 10);
        Entry accepted = newEntry(10, 10);
        Entry atEnd = newEntry(20, 1);
        Entry afterEnd = newEntry(21, 1);
        StorageApi controlledStorage = controlledStorageApi((start, maxEntries) ->
                CompletableFuture.completedFuture(List.of(beforeStart, accepted, atEnd, afterEnd)));

        try (var reader = new EntryReader(controlledStorage, 11, 10, 20, 100, CompactionMetrics.NOOP)) {
            Entry returned = reader.readEntry();
            assertSame(accepted, returned);
            assertEquals(1, returned.payload().refCnt());
            assertNull(reader.readEntry());
            assertEquals(0, beforeStart.payload().refCnt());
            assertEquals(0, atEnd.payload().refCnt());
            assertEquals(0, afterEnd.payload().refCnt());
            returned.payload().release();
        }
    }

    @Test
    void testReadTransfersEntryOwnershipToCaller() throws Exception {
        Entry entry = newEntry(0, 1);
        StorageApi controlledStorage = controlledStorageApi((start, maxEntries) ->
                CompletableFuture.completedFuture(List.of(entry)));

        try (var reader = new EntryReader(controlledStorage, 12, 0, 1, 10, CompactionMetrics.NOOP)) {
            var genericEntry = reader.read();
            assertSame(entry, genericEntry.entry());
            assertEquals(1, genericEntry.entry().payload().refCnt());
            genericEntry.entry().payload().release();
            assertEquals(0, entry.payload().refCnt());
            assertNull(reader.read());
        }
    }

    @Test
    void testDiscontinuityReleasesUntransferredEntries() throws Exception {
        Entry accepted = newEntry(0, 10);
        Entry discontinuous = newEntry(20, 10);
        Entry trailing = newEntry(30, 10);
        StorageApi controlledStorage = controlledStorageApi((start, maxEntries) ->
                CompletableFuture.completedFuture(List.of(accepted, discontinuous, trailing)));

        try (var reader = new EntryReader(controlledStorage, 13, 0, 100, 100, CompactionMetrics.NOOP)) {
            Entry returned = reader.readEntry();
            assertSame(accepted, returned);
            returned.payload().release();

            LakehouseException exception = assertThrows(LakehouseException.class, reader::readEntry);
            assertInstanceOf(IllegalStateException.class, exception.getCause());
            assertEquals(0, discontinuous.payload().refCnt());
            assertEquals(0, trailing.payload().refCnt());
        }
    }

    @Test
    void testCloseReleasesEntriesFromLateReadCompletion() throws Exception {
        CompletableFuture<List<Entry>> pendingRead = new CompletableFuture<>();
        CountDownLatch readStarted = new CountDownLatch(1);
        Entry first = newEntry(0, 1);
        Entry second = newEntry(1, 1);
        StorageApi controlledStorage = controlledStorageApi((start, maxEntries) -> {
            readStarted.countDown();
            return pendingRead;
        });
        var reader = new EntryReader(controlledStorage, 14, 0, 2, 10, CompactionMetrics.NOOP);

        try {
            assertTrue(readStarted.await(5, TimeUnit.SECONDS));
            reader.close();
            pendingRead.complete(List.of(first, second));
            awaitCondition(() -> first.payload().refCnt() == 0 && second.payload().refCnt() == 0);
        } finally {
            reader.close();
        }
    }

    @Test
    void testQueueInterruptReleasesEntryWaitingForTransfer() throws Exception {
        Entry first = newEntry(0, 1);
        Entry second = newEntry(1, 1);
        Entry waiting = newEntry(2, 1);
        Map<Long, AtomicInteger> callsByOffset = new ConcurrentHashMap<>();
        StorageApi controlledStorage = controlledStorageApi((start, maxEntries) -> {
            int invocation = callsByOffset.computeIfAbsent(start, ignored -> new AtomicInteger())
                    .incrementAndGet();
            if (invocation == 1 && start > 0) {
                return CompletableFuture.completedFuture(List.of());
            }
            return CompletableFuture.completedFuture(switch ((int) start) {
                case 0 -> List.of(first);
                case 1 -> List.of(second);
                case 2 -> List.of(waiting);
                default -> List.of();
            });
        });
        AtomicReference<Thread> worker = new AtomicReference<>();
        ExecutorService executor = Executors.newSingleThreadExecutor(task -> {
            Thread thread = new Thread(task, "entry-reader-ownership-test");
            worker.set(thread);
            return thread;
        });
        var reader = new EntryReader(controlledStorage, 15, 0, 3, 1, CompactionMetrics.NOOP, executor);

        try {
            awaitCondition(() -> queuedEntryCount(reader) == 2
                    && callsByOffset.getOrDefault(2L, new AtomicInteger()).get() >= 2
                    && isWaitingToEnqueue(worker.get()));
            worker.get().interrupt();
            awaitCondition(() -> waiting.payload().refCnt() == 0);
        } finally {
            reader.close();
            executor.shutdownNow();
            assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
        }
        assertEquals(0, first.payload().refCnt());
        assertEquals(0, second.payload().refCnt());
        assertEquals(0, waiting.payload().refCnt());
    }

    private static Entry newEntry(long offset, int numberOfMessages) {
        var payload = Unpooled.buffer(1).writeByte(1);
        return Entry.of(new EntryHeader(offset, numberOfMessages, 0, payload.readableBytes(),
                offset + numberOfMessages), payload);
    }

    private static StorageApi controlledStorageApi(ReadEntries readEntries) {
        return (StorageApi) Proxy.newProxyInstance(
                StorageApi.class.getClassLoader(),
                new Class<?>[]{StorageApi.class},
                (proxy, method, args) -> {
                    if (method.getName().equals("readEntries") && method.getParameterCount() == 4) {
                        return readEntries.read((Long) args[1], (Integer) args[2]);
                    }
                    if (method.getName().equals("toString") && method.getParameterCount() == 0) {
                        return "ControlledStorageApi";
                    }
                    if (method.getName().equals("hashCode") && method.getParameterCount() == 0) {
                        return System.identityHashCode(proxy);
                    }
                    if (method.getName().equals("equals") && method.getParameterCount() == 1) {
                        return proxy == args[0];
                    }
                    throw new UnsupportedOperationException(method.toString());
                });
    }

    private static int queuedEntryCount(EntryReader reader) {
        try {
            var queueField = EntryReader.class.getDeclaredField("queue");
            queueField.setAccessible(true);
            return ((ArrayBlockingQueue<?>) queueField.get(reader)).size();
        } catch (ReflectiveOperationException e) {
            throw new AssertionError(e);
        }
    }

    private static boolean isWaitingToEnqueue(Thread thread) {
        if (thread == null) {
            return false;
        }
        for (StackTraceElement element : thread.getStackTrace()) {
            if (element.getClassName().equals(EntryReader.class.getName())
                    && element.getMethodName().equals("enqueueEntry")) {
                return true;
            }
        }
        return false;
    }

    private static void awaitCondition(BooleanSupplier condition) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (!condition.getAsBoolean() && System.nanoTime() < deadline) {
            Thread.sleep(10);
        }
        assertTrue(condition.getAsBoolean(), "Condition was not met before timeout");
    }

    @FunctionalInterface
    private interface ReadEntries {
        CompletableFuture<List<Entry>> read(long startOffset, int maxEntries);
    }
}
