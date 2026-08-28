/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.lakestream.impl;

import io.lakestream.api.LogStorage;
import io.lakestream.api.RoutingKey;
import io.lakestream.api.StreamLayout;
import io.lakestream.api.StreamWriter;
import io.netty.buffer.ByteBuf;
import java.util.concurrent.CompletableFuture;

/**
 * Default implementation of {@link StreamWriter}.
 *
 * <p>Resolves routing via {@link StreamLayout}, then delegates the actual
 * append to {@link LogStorage}.
 */
public class StreamWriterImpl implements StreamWriter {

    private final StreamLayout layout;
    private final LogStorage logStorage;

    public StreamWriterImpl(StreamLayout layout, LogStorage logStorage) {
        this.layout = layout;
        this.logStorage = logStorage;
    }

    @Override
    public CompletableFuture<WriteResult> write(RoutingKey key, int numberOfRecords,
                                                 ByteBuf data) {
        return layout.resolveForWrite(key)
            .thenCompose(logId ->
                logStorage.append(logId, numberOfRecords, data)
                    .thenApply(header -> new WriteResult(logId, header.offset()))
            );
    }

    @Override
    public StreamLayout layout() {
        return layout;
    }

    @Override
    public void close() throws Exception {
        // LogStorage lifecycle managed externally
    }
}
