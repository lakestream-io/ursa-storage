/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.storage.impl;

import io.lakestream.ursa.metrics.LatencyHistogram;
import io.lakestream.ursa.storage.AddResult;
import io.netty.buffer.ByteBuf;
import java.util.concurrent.CompletableFuture;

public class PendingWrite extends PendingAdd{
    final long initialOffset;
    final long cumulativeSize;

    PendingWrite(long id, int numberOfMessages, long initialOffset, long cumulativeSize, ByteBuf buf,
                 CompletableFuture<AddResult> future,
                 LatencyHistogram putLatencyRef) {
        super(id, numberOfMessages, buf, future, putLatencyRef);
        this.initialOffset = initialOffset;
        this.cumulativeSize = cumulativeSize;
    }

    @Override
    public long initialOffset() {
        return initialOffset;
    }

    @Override
    public long cumulativeSize() {
        return cumulativeSize;
    }
}
