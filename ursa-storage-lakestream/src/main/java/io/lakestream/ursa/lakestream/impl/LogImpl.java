/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.lakestream.impl;

import io.lakestream.api.EntryIndex;
import io.lakestream.api.Log;
import io.lakestream.api.LogCursor;
import io.lakestream.api.LogEntry;
import io.lakestream.api.LogEntryHeader;
import io.lakestream.api.LogId;
import io.lakestream.api.LogOffset;
import io.lakestream.api.LogState;
import io.lakestream.api.LogStateManager;
import io.lakestream.api.LogStorage;
import io.lakestream.ursa.storage.OwnedResultFutures;
import io.lakestream.ursa.storage.impl.EntryIndexCache;
import io.netty.buffer.ByteBuf;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Predicate;

/**
 * Default implementation of {@link Log} — per-log managed operations.
 *
 * <p>Internally uses:
 * <ul>
 *   <li>{@link LogStorage} for append and basic read operations</li>
 *   <li>{@link UnifiedStreamReader} for transparent RAW/PARQUET reads (optional)</li>
 *   <li>{@link EntryIndexCache} for cached entry metadata queries</li>
 *   <li>{@link LogStateManager} for fencing</li>
 * </ul>
 */
public class LogImpl implements Log {

    private final LogId logId;
    private final LogStorage logStorage;
    private final UnifiedStreamReader unifiedReader;
    private final boolean ownsUnifiedReader;
    private final EntryIndexCache entryIndexCache;
    private final LogStateManager streamStateManager;
    private final AtomicBoolean closed = new AtomicBoolean();

    /**
     * Creates a log that borrows the supplied unified reader. Closing the log does not close the reader.
     */
    public LogImpl(LogId logId,
                   LogStorage logStorage,
                   UnifiedStreamReader unifiedReader,
                   EntryIndexCache entryIndexCache,
                   LogStateManager streamStateManager) {
        this(logId, logStorage, unifiedReader, entryIndexCache, streamStateManager, false);
    }

    /**
     * Creates a log with explicit unified-reader ownership.
     *
     * @param ownsUnifiedReader whether closing this log also closes {@code unifiedReader}
     */
    public LogImpl(LogId logId,
                   LogStorage logStorage,
                   UnifiedStreamReader unifiedReader,
                   EntryIndexCache entryIndexCache,
                   LogStateManager streamStateManager,
                   boolean ownsUnifiedReader) {
        this.logId = logId;
        this.logStorage = logStorage;
        this.unifiedReader = unifiedReader;
        this.ownsUnifiedReader = ownsUnifiedReader;
        this.entryIndexCache = entryIndexCache;
        this.streamStateManager = streamStateManager;
    }

    @Override
    public LogId id() {
        return logId;
    }

    @Override
    public CompletableFuture<LogEntryHeader> append(int numberOfRecords, ByteBuf data) {
        return logStorage.append(logId, numberOfRecords, data);
    }

    @Override
    public CompletableFuture<List<LogEntry>> readEntries(long startOffset, int maxMessageCount,
                                                          long maxSizeBytes) {
        return readEntries(startOffset, maxMessageCount, maxSizeBytes, false);
    }

    @Override
    public CompletableFuture<List<LogEntry>> readEntries(long startOffset, int maxMessageCount,
                                                          long maxSizeBytes, boolean includeTrimmed) {
        CompletableFuture<List<LogEntry>> read;
        if (unifiedReader != null) {
            read = unifiedReader.readEntries(logId, startOffset, maxMessageCount, maxSizeBytes)
                .thenApply(UnifiedStreamReader.ReadResult::entries);
        } else {
            read = logStorage.readEntries(logId, startOffset, maxMessageCount, maxSizeBytes);
        }
        return OwnedResultFutures.transferLogEntries(read);
    }

    @Override
    public LogStorage logStorage() {
        return logStorage;
    }

    @Override
    public CompletableFuture<LogEntry> readEntry(long offset) {
        CompletableFuture<LogEntry> read = readEntries(offset, 1, Long.MAX_VALUE, true)
            .thenApply(entries -> {
                if (entries.isEmpty()) {
                    return null;
                }
                LogEntry result = entries.get(0);
                try {
                    for (int i = 1; i < entries.size(); i++) {
                        LogEntry extra = entries.get(i);
                        if (extra != null) {
                            extra.close();
                        }
                    }
                    return result;
                } catch (RuntimeException | Error cleanupFailure) {
                    for (LogEntry entry : entries) {
                        if (entry == null) {
                            continue;
                        }
                        try {
                            entry.close();
                        } catch (RuntimeException | Error suppressed) {
                            cleanupFailure.addSuppressed(suppressed);
                        }
                    }
                    throw cleanupFailure;
                }
            });
        return OwnedResultFutures.transfer(read, entry -> {
            if (entry != null) {
                entry.close();
            }
        });
    }

    @Override
    public CompletableFuture<LogEntryHeader> getEntryMetadata(long offset) {
        if (entryIndexCache != null) {
            return entryIndexCache.searchEntryHeader(logId.id(), offset)
                .thenApply(header -> header);
        }
        return CompletableFuture.failedFuture(
            new UnsupportedOperationException("No EntryIndexCache configured"));
    }

    @Override
    public CompletableFuture<EntryIndex> getEntryIndex(long offset) {
        if (entryIndexCache != null) {
            return entryIndexCache.get(logId.id(), offset);
        }
        return CompletableFuture.failedFuture(
            new UnsupportedOperationException("No EntryIndexCache configured"));
    }

    @Override
    public CompletableFuture<List<EntryIndex>> readIndexRange(long startOffset, long endOffset) {
        return logStorage.readIndexRange(logId, startOffset, endOffset);
    }

    @Override
    public CompletableFuture<List<LogEntryHeader>> getEntryMetadataRange(long startOffset,
                                                                          long endOffset) {
        if (entryIndexCache == null) {
            return CompletableFuture.failedFuture(
                new UnsupportedOperationException("No EntryIndexCache configured"));
        }
        List<CompletableFuture<LogEntryHeader>> futures = new ArrayList<>();
        long offset = startOffset;
        while (offset < endOffset) {
            final long currentOffset = offset;
            futures.add(entryIndexCache.searchEntryHeader(logId.id(), currentOffset)
                .thenApply(header -> header));
            offset++;
        }
        return CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]))
            .thenApply(v -> futures.stream()
                .map(CompletableFuture::join)
                .toList());
    }

    @Override
    public CompletableFuture<LogOffset> getFirstOffset() {
        return logStorage.getFirstOffset(logId);
    }

    @Override
    public CompletableFuture<LogOffset> getFirstOffset(boolean includeTrimmed) {
        return logStorage.getFirstOffset(logId, includeTrimmed);
    }

    @Override
    public CompletableFuture<LogOffset> getLastOffset() {
        return logStorage.getLastOffset(logId);
    }

    @Override
    public CompletableFuture<Long> softTrim(long offsetIncluded) {
        return logStorage.softTrim(logId, offsetIncluded);
    }

    @Override
    public void cacheIndex(EntryIndex index) {
        if (entryIndexCache != null) {
            entryIndexCache.put(logId.id(), index);
        }
    }

    @Override
    public void invalidateCache() {
        if (entryIndexCache != null) {
            entryIndexCache.invalidateAll();
        }
    }

    @Override
    public void invalidateCache(long offset) {
        if (entryIndexCache != null) {
            entryIndexCache.invalidate(offset);
        }
    }

    @Override
    public long getMessageCount(long startOffset, long endOffset) {
        if (entryIndexCache != null) {
            return entryIndexCache.getMessageCount(logId.id(), startOffset).join();
        }
        return -1;
    }

    @Override
    public void fence() {
        if (streamStateManager != null) {
            streamStateManager.setState(logId.id(), LogState.FENCED);
        }
    }

    // --- Retention ---

    @Override
    public CompletableFuture<Long> computeRetentionTrimOffset(long maxOffset, long retentionMillis,
            long retentionSizeBytes) {
        CompletableFuture<Long> timeBased = computeTimeTrimOffset(maxOffset, retentionMillis);
        CompletableFuture<Long> sizeBased = computeSizeTrimOffset(maxOffset, retentionSizeBytes);

        return timeBased.thenCombine(sizeBased,
                (timeBasedOffset, sizeBasedOffset) -> {
                    long retentionOffset = Math.max(timeBasedOffset, sizeBasedOffset);
                    return Math.min(retentionOffset, maxOffset);
                });
    }

    private CompletableFuture<Long> computeTimeTrimOffset(long maxOffset, long retentionMillis) {
        if (retentionMillis == 0) {
            return CompletableFuture.completedFuture(maxOffset);
        } else if (retentionMillis < 0) {
            return CompletableFuture.completedFuture(-1L);
        }
        long now = System.currentTimeMillis();
        return getFirstOffset()
                .thenCompose(firstOffset -> BinarySearch.binarySearch(
                        (sid, off) -> getEntryIndex(off),
                        logId.id(), firstOffset.offset(), maxOffset,
                        header -> now - header.writtenTimestamp() > retentionMillis));
    }

    private CompletableFuture<Long> computeSizeTrimOffset(long maxOffset, long retentionSizeBytes) {
        if (retentionSizeBytes == 0) {
            return CompletableFuture.completedFuture(maxOffset);
        } else if (retentionSizeBytes < 0) {
            return CompletableFuture.completedFuture(-1L);
        }
        return getLastOffset()
                .thenCompose(lastOffset -> {
                    long cumulativeSize = lastOffset.cumulativeSize();
                    return getFirstOffset()
                            .thenCompose(firstOffset -> BinarySearch.binarySearch(
                                    (sid, off) -> getEntryIndex(off),
                                    logId.id(), firstOffset.offset(), maxOffset,
                                    header -> cumulativeSize - header.cumulativeSize()
                                            + header.entrySize() > retentionSizeBytes));
                });
    }

    // --- Search ---

    @Override
    public CompletableFuture<Long> binarySearchOffset(long min, long max,
            Predicate<LogEntryHeader> predicate) {
        return BinarySearch.binarySearch(
                (sid, off) -> getEntryIndex(off),
                logId.id(), min, max,
                header -> predicate.test(header));
    }

    @Override
    public CompletableFuture<LogCursor> openEphemeralCursor(String name, long initialOffset) {
        LogCursorImpl cursor = new LogCursorImpl(name, this, initialOffset, -1L);
        return CompletableFuture.completedFuture(cursor);
    }

    @Override
    public void close() throws Exception {
        if (ownsUnifiedReader && unifiedReader != null && closed.compareAndSet(false, true)) {
            unifiedReader.close();
        }
    }
}
