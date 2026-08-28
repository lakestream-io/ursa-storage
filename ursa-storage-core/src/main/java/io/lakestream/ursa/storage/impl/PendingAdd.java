/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.storage.impl;

import io.lakestream.api.exception.LogFencedException;
import io.lakestream.ursa.metrics.LatencyHistogram;
import io.lakestream.ursa.storage.AddResult;
import io.lakestream.ursa.storage.PersistCallback;
import io.netty.buffer.ByteBuf;
import java.util.concurrent.CompletableFuture;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class PendingAdd implements PersistCallback {
    private final LatencyHistogram putLatencyRef;

    long id;
    long indexId;
    int numberOfMessages;
    ByteBuf buf;

    CompletableFuture<AddResult> future;
    final long startTime;

    PendingAdd(long id, int numberOfMessages, ByteBuf buf,
               CompletableFuture<AddResult> future, LatencyHistogram putLatencyRef) {
        this.putLatencyRef = putLatencyRef;
        this.id = id;
        this.numberOfMessages = numberOfMessages;
        this.buf = buf.retain();
        this.future = future;
        this.startTime = System.nanoTime();
    }

    @Override
    public void onSuccess(AddResult addResult) {
        if (log.isDebugEnabled()) {
            log.debug("Persisted  {}:{} to location: {} successfully", id, indexId, addResult.position());
        }
        this.future.complete(addResult);
        release();
    }

    @Override
    public void onFailure(Throwable t) {
        if (t instanceof LogFencedException) {
            log.info("Cancel adding {}:{} to storage", id, indexId);
        } else {
            log.error("Failed to persist {}:{} to storage", id, indexId, t);
        }
        putLatencyRef.recordFailure(System.nanoTime() - startTime);
        this.future.completeExceptionally(t);
        release();
    }

    public long initialOffset() {
        return -1;
    }

    public long cumulativeSize() {
        return -1;
    }

    public boolean release() {
        return buf.release();
    }
}
