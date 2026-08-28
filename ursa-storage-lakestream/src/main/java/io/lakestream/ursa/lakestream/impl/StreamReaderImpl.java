/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.lakestream.impl;

import io.lakestream.api.LogEntry;
import io.lakestream.api.LogId;
import io.lakestream.api.LogStorage;
import io.lakestream.api.StreamLayout;
import io.lakestream.api.StreamReader;
import io.lakestream.ursa.storage.OwnedResultFutures;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * Default implementation of {@link StreamReader}.
 *
 * <p>Supports two modes:
 * <ul>
 *   <li><b>Simple mode</b> — constructed with {@link LogStorage} only; reads from
 *       RAW storage via the clean API. Suitable for basic usage.</li>
 *   <li><b>Unified mode</b> — constructed with {@link UnifiedStreamReader}; reads
 *       transparently handle both RAW and PARQUET data. Used when compacted
 *       object reading is available.</li>
 * </ul>
 */
public class StreamReaderImpl implements StreamReader {

    private final StreamLayout layout;
    private final LogStorage logStorage;
    private final UnifiedStreamReader unifiedReader;

    /**
     * Simple mode: reads via LogStorage only (RAW data).
     */
    public StreamReaderImpl(StreamLayout layout, LogStorage logStorage) {
        this.layout = layout;
        this.logStorage = logStorage;
        this.unifiedReader = null;
    }

    /**
     * Unified mode: reads via UnifiedStreamReader (handles PARQUET/RAW transparently).
     */
    public StreamReaderImpl(StreamLayout layout, UnifiedStreamReader unifiedReader) {
        this.layout = layout;
        this.logStorage = null;
        this.unifiedReader = unifiedReader;
    }

    @Override
    public CompletableFuture<ReadResult> read(LogId logId, long startOffset,
                                               int maxMessageCount, long maxSizeBytes) {
        CompletableFuture<ReadResult> read = unifiedReader != null
            ? readViaUnified(logId, startOffset, maxMessageCount, maxSizeBytes)
            : readViaLogStorage(logId, startOffset, maxMessageCount, maxSizeBytes);
        return OwnedResultFutures.transfer(read, result -> {
            if (result != null) {
                OwnedResultFutures.closeLogEntries(result.entries());
            }
        });
    }

    private CompletableFuture<ReadResult> readViaLogStorage(
            LogId logId, long startOffset, int maxMessageCount, long maxSizeBytes) {
        return logStorage.readEntries(logId, startOffset, maxMessageCount, maxSizeBytes)
            .thenApply(entries -> createReadResult(entries, startOffset));
    }

    private CompletableFuture<ReadResult> readViaUnified(
            LogId logId, long startOffset, int maxMessageCount, long maxSizeBytes) {
        return unifiedReader.readEntries(logId, startOffset, maxMessageCount, maxSizeBytes)
            .thenApply(result -> new ReadResult(result.entries(), result.nextOffset()));
    }

    private static ReadResult createReadResult(List<LogEntry> entries, long startOffset) {
        try {
            long nextOffset = startOffset;
            for (LogEntry entry : entries) {
                nextOffset = entry.offset() + entry.numberOfRecords();
            }
            return new ReadResult(entries, nextOffset);
        } catch (RuntimeException | Error metadataFailure) {
            for (LogEntry entry : entries) {
                if (entry == null) {
                    continue;
                }
                try {
                    entry.close();
                } catch (RuntimeException | Error cleanupFailure) {
                    metadataFailure.addSuppressed(cleanupFailure);
                }
            }
            throw metadataFailure;
        }
    }

    @Override
    public CompletableFuture<List<LogId>> logIds() {
        return layout.logIds();
    }

    @Override
    public StreamLayout layout() {
        return layout;
    }

    @Override
    public void close() throws Exception {
        if (unifiedReader != null) {
            unifiedReader.close();
        }
    }
}
