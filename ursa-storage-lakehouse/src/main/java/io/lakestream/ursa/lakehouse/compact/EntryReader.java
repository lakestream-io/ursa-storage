/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.lakehouse.compact;

import io.lakestream.api.EntryHeader;
import io.lakestream.ursa.compaction.metrics.CompactionMetrics;
import io.lakestream.ursa.exception.DataSourceException;
import io.lakestream.ursa.exception.ExceptionCode;
import io.lakestream.ursa.lakehouse.exception.LakehouseException;
import io.lakestream.ursa.materialization.serde.GenericEntry;
import io.lakestream.ursa.storage.Entry;
import io.lakestream.ursa.storage.StorageApi;
import io.lakestream.ursa.storage.impl.exception.NoSuchEntriesException;
import io.lakestream.ursa.storage.impl.exception.NoSuchOffsetException;
import io.netty.util.ReferenceCountUtil;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class EntryReader implements IEntryReader {

    private final StorageApi storageApi;
    private final long streamId;
    private final ArrayBlockingQueue<Entry> queue;
    private final Object queueMonitor = new Object();
    private final CompletableFuture<Void> fillResult;
    private final CompactionMetrics metrics;
    private final Executor executor;
    private EntryHeader lastReadEntryHeader;
    private final int readMaxEntriesInOneBatch;
    private final long startOffset;
    private final long endOffset;
    private int readCount = 0;
    private volatile boolean closed;

    public EntryReader(StorageApi storageApi, long streamId, long startOffset, long endOffset,
                       int readMaxEntriesInOneBatch, CompactionMetrics metrics) {
        this(storageApi, streamId, startOffset, endOffset, readMaxEntriesInOneBatch, metrics,
                ForkJoinPool.commonPool());
    }

    EntryReader(StorageApi storageApi, long streamId, long startOffset, long endOffset,
                int readMaxEntriesInOneBatch, CompactionMetrics metrics, Executor executor) {
        this.storageApi = storageApi;
        this.streamId = streamId;
        this.startOffset = startOffset;
        this.endOffset = endOffset;
        this.readMaxEntriesInOneBatch = readMaxEntriesInOneBatch;
        this.queue = new ArrayBlockingQueue<>(readMaxEntriesInOneBatch * 2);
        this.fillResult = new CompletableFuture<>();
        this.metrics = metrics;
        this.executor = executor;
        fillEntries(startOffset, endOffset);
    }

    /**
     * Transfers ownership of the returned entry's payload reference to the caller.
     * The caller must release the payload after consuming the entry.
     */
    @Override
    public GenericEntry read() throws DataSourceException {
        try {
            var entry = readEntry();
            if (entry == null) {
                return null;
            }
            return new GenericEntry(entry);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        } catch (LakehouseException e) {
            if (e.getCause() instanceof NoSuchEntriesException) {
                throw new DataSourceException(ExceptionCode.NO_SUCH_ENTRIES, "Internal error", e);
            }
            if (e.getCause() instanceof NoSuchOffsetException) {
                throw new DataSourceException(ExceptionCode.NO_SUCH_OFFSET, "Internal error", e);
            }
            throw new DataSourceException(ExceptionCode.INTERNAL_ERROR, "Internal error", e);
        }
    }

    /**
     * Read an entry from the stream. It's a blocking method which will wait until the entry is available.
     *
     * @return an entry whose payload reference is owned by the caller, or {@code null} at the end of the range
     */
    public Entry readEntry() throws InterruptedException, LakehouseException {
        while (!queue.isEmpty() || !fillResult.isDone()) {
            Entry entry = queue.poll(100, TimeUnit.MILLISECONDS);
            if (entry != null) {
                signalQueueSpace();
                return entry;
            }
        }
        if (fillResult.isCompletedExceptionally()) {
            try {
                fillResult.get();
            } catch (ExecutionException e) {
                throw new LakehouseException(
                    "Failed to read entry from stream " + streamId + " " + startOffset + "-" + endOffset, e.getCause());
            }
        }
        return null;
    }

    void fillEntries(long readStart, long readEnd) {
        CompletableFuture.runAsync(() -> {
            var maxRead = Math.toIntExact(Math.min(readMaxEntriesInOneBatch, readEnd - readStart));
            if (fillResult.isDone()) {
                return;
            }
            if (maxRead <= 0) {
                if (readCount == 0) {
                    fillResult.completeExceptionally(
                        new NoSuchEntriesException(streamId, startOffset, endOffset));
                } else {
                    fillResult.complete(null);
                }
                return;
            }
            readEntries(streamId, readStart, maxRead)
                .whenCompleteAsync((entries, ex) -> {
                    if (ex != null) {
                        log.error("Failed to read entries from stream {} start offset: {} maxEntries: {}",
                            streamId, readStart, maxRead, ex);
                        fillResult.completeExceptionally(ex);
                        return;
                    }
                    if (closed) {
                        releaseEntries(entries, 0);
                        return;
                    }
                    if (entries.isEmpty()) {
                        if (readCount == 0) {
                            fillResult.completeExceptionally(
                                new NoSuchEntriesException(streamId, startOffset, endOffset));
                        } else {
                            fillResult.complete(null);
                        }
                        return;
                    }
                    processEntries(entries, readEnd);
                }, executor);
            // todo: improve the prefetch part to fill them into the read cache instead of read them out the
            //  to speed up the compaction
            var nextReadPos = readStart + maxRead;
            var nextReadMax = Math.toIntExact(Math.min(readMaxEntriesInOneBatch, readEnd - nextReadPos));
            if (nextReadMax == 0) {
                return;
            }
            readEntries(streamId, nextReadPos, nextReadMax)
                .thenAccept(entries -> releaseEntries(entries, 0));
        }, executor);

    }

    /**
     * Releases every entry returned by StorageApi that is not transferred to the queue.
     */
    private void processEntries(List<Entry> entries, long readEnd) {
        int nextOwnedEntry = 0;
        try {
            while (nextOwnedEntry < entries.size()) {
                Entry entry = entries.get(nextOwnedEntry);
                EntryHeader entryHeader = entry.header();
                // The task is 0-100000. The first read returns 0-10000, then the stream is truncated
                // to 20000. The second read starts at 20000, so retry the task from the real start.
                if (lastReadEntryHeader != null
                        && lastReadEntryHeader.offset() + lastReadEntryHeader.numberOfMessages()
                        != entryHeader.offset()) {
                    fillResult.completeExceptionally(
                            new IllegalStateException("The read entry is not continuous"));
                    return;
                }
                lastReadEntryHeader = entryHeader;
                if (entryHeader.offset() < startOffset) {
                    log.warn("The stream {} read entry offset {} is out of range {}-{}, ignore it.", streamId,
                            entryHeader.offset(), startOffset, endOffset);
                    releaseEntry(entry);
                    nextOwnedEntry++;
                    continue;
                }
                if (entryHeader.offset() >= endOffset) {
                    log.info("The stream {} read entry offset {} is out of range {}-{}. It may be mark deleted;"
                                    + " ignore it.", streamId, entryHeader.offset(), startOffset, endOffset);
                    releaseEntry(entry);
                    nextOwnedEntry++;
                    continue;
                }
                if (!enqueueEntry(entry)) {
                    return;
                }
                nextOwnedEntry++;
                readCount++;
            }
            EntryHeader lastEntryHeader = entries.get(entries.size() - 1).header();
            fillEntries(lastEntryHeader.offset() + lastEntryHeader.numberOfMessages(), readEnd);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.error("Interrupted while putting an entry into the queue", e);
            fillResult.completeExceptionally(e);
        } catch (RuntimeException | Error e) {
            fillResult.completeExceptionally(e);
        } finally {
            releaseEntries(entries, nextOwnedEntry);
        }
    }

    private boolean enqueueEntry(Entry entry) throws InterruptedException {
        synchronized (queueMonitor) {
            while (!closed) {
                if (queue.offer(entry)) {
                    return true;
                }
                queueMonitor.wait();
            }
            return false;
        }
    }

    private void signalQueueSpace() {
        synchronized (queueMonitor) {
            queueMonitor.notifyAll();
        }
    }

    private static void releaseEntries(List<Entry> entries, int fromIndex) {
        if (entries == null) {
            return;
        }
        for (int i = fromIndex; i < entries.size(); i++) {
            releaseEntry(entries.get(i));
        }
    }

    private static void releaseEntry(Entry entry) {
        if (entry != null) {
            ReferenceCountUtil.safeRelease(entry.payload());
        }
    }

    private CompletableFuture<List<Entry>> readEntries(long streamId, long startOffset, int maxEntries) {
        long start = System.nanoTime();
        return storageApi.readEntries(streamId, startOffset, maxEntries, -1)
            .whenComplete((res, ex) -> {
                if (ex != null) {
                    metrics.getReadMessagesFromWalLatency().recordFailure(System.nanoTime() - start);
                } else {
                    metrics.getReadMessagesFromWalLatency().recordSuccess(System.nanoTime() - start);
                }
            });
    }

    @Override
    public void close() {
        List<Entry> queuedEntries = new ArrayList<>();
        synchronized (queueMonitor) {
            if (closed) {
                return;
            }
            closed = true;
            queue.drainTo(queuedEntries);
            queueMonitor.notifyAll();
        }
        fillResult.completeExceptionally(new LakehouseException("Force shutdown the entry reader"));
        releaseEntries(queuedEntries, 0);
    }
}
