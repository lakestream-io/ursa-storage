/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.lakestream.impl;

import io.lakestream.api.Log;
import io.lakestream.api.LogId;
import io.lakestream.api.RoutingKey;
import io.lakestream.api.StreamLayout;
import io.lakestream.api.StreamWriter;
import io.lakestream.ursa.storage.OwnedResultFutures;
import io.lakestream.ursa.utils.FutureUtils;
import io.netty.buffer.ByteBuf;
import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * Default implementation of {@link StreamWriter}.
 *
 * <p>Resolves routing via {@link StreamLayout}, then delegates the actual
 * append to catalog-opened {@link Log} handles.
 *
 * <p>The writer owns every supplied log and closes them when it is closed. Catalogs must supply
 * leased logs so a stream writer cannot bypass durable deletion fencing.
 * Closing rejects new writes and drains every write accepted before closing before any owned log
 * is released. A timed-out or interrupted close leaves those logs open for a later close retry.
 */
public class StreamWriterImpl implements StreamWriter {

    private static final long DEFAULT_CLOSE_TIMEOUT_MILLIS = TimeUnit.SECONDS.toMillis(10);

    private final StreamLayout layout;
    private final Map<LogId, Log> logs;
    private final List<Log> ownedLogs;
    private final boolean[] releasedLogs;
    private final long closeTimeoutMillis;
    private boolean closed;
    private int activeWrites;

    public StreamWriterImpl(StreamLayout layout, List<Log> logs) {
        this(layout, logs, DEFAULT_CLOSE_TIMEOUT_MILLIS);
    }

    StreamWriterImpl(StreamLayout layout, List<Log> logs, long closeTimeoutMillis) {
        this.layout = Objects.requireNonNull(layout, "layout");
        this.ownedLogs = List.copyOf(logs);
        Map<LogId, Log> indexed = new LinkedHashMap<>();
        for (Log log : ownedLogs) {
            Log previous = indexed.put(Objects.requireNonNull(log, "log").id(), log);
            if (previous != null) {
                throw new IllegalArgumentException("Duplicate opened log " + log.id());
            }
        }
        this.logs = Map.copyOf(indexed);
        this.releasedLogs = new boolean[ownedLogs.size()];
        if (closeTimeoutMillis <= 0) {
            throw new IllegalArgumentException("closeTimeoutMillis must be positive");
        }
        this.closeTimeoutMillis = closeTimeoutMillis;
    }

    @Override
    public CompletableFuture<WriteResult> write(RoutingKey key, int numberOfRecords,
                                                 ByteBuf data) {
        synchronized (this) {
            if (closed) {
                return CompletableFuture.failedFuture(
                    new IllegalStateException("Stream writer is closed"));
            }
            activeWrites++;
        }

        CompletableFuture<WriteResult> write;
        try {
            write = layout.resolveForWrite(key)
                .thenCompose(logId -> {
                    Log log = logs.get(logId);
                    if (log == null) {
                        return CompletableFuture.failedFuture(new IllegalStateException(
                            "Committed layout resolved unopened log " + logId));
                    }
                    return log.append(numberOfRecords, data)
                        .thenApply(header -> new WriteResult(logId, header.offset()));
                });
        } catch (RuntimeException | Error failure) {
            writeFinished();
            throw failure;
        }

        CompletableFuture<WriteResult> trackedWrite = write.whenComplete(
            (ignored, ignoredError) -> writeFinished());
        return OwnedResultFutures.nonCancellableCompletion(trackedWrite);
    }

    private synchronized void writeFinished() {
        activeWrites--;
        if (activeWrites == 0) {
            notifyAll();
        }
    }

    private void awaitWritesDrained(long deadlineNanos) throws IOException {
        while (activeWrites > 0) {
            long remainingNanos = deadlineNanos - System.nanoTime();
            if (remainingNanos <= 0) {
                throw new IOException("Timed out after " + closeTimeoutMillis
                    + " ms waiting for accepted stream-writer operations to drain");
            }
            try {
                TimeUnit.NANOSECONDS.timedWait(this, remainingNanos);
            } catch (InterruptedException failure) {
                Thread.currentThread().interrupt();
                throw new IOException(
                    "Interrupted while waiting for accepted stream-writer operations to drain",
                    failure);
            }
        }
    }

    @Override
    public StreamLayout layout() {
        return layout;
    }

    @Override
    public synchronized void close() throws Exception {
        long deadlineNanos = System.nanoTime()
            + TimeUnit.MILLISECONDS.toNanos(closeTimeoutMillis);
        closed = true;
        awaitWritesDrained(deadlineNanos);
        Throwable failure = null;
        for (int i = ownedLogs.size() - 1; i >= 0; i--) {
            if (releasedLogs[i]) {
                continue;
            }
            try {
                ownedLogs.get(i).close();
                releasedLogs[i] = true;
            } catch (Exception | Error closeFailure) {
                Throwable unwrapped = FutureUtils.unwrapCompletionException(closeFailure);
                if (failure == null) {
                    failure = unwrapped;
                } else if (failure != unwrapped) {
                    failure.addSuppressed(unwrapped);
                }
            }
        }
        if (failure instanceof Error error) {
            throw error;
        }
        if (failure instanceof Exception exception) {
            throw exception;
        }
        if (failure != null) {
            throw new RuntimeException(failure);
        }
    }
}
