/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.lakestream.impl;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.RateLimiter;
import io.lakestream.api.EntryHeader;
import io.lakestream.api.EntryIndex;
import io.lakestream.api.Log;
import io.lakestream.api.LogCursor;
import io.lakestream.api.LogEntry;
import io.lakestream.api.LogEntryHeader;
import io.lakestream.api.Position;
import io.lakestream.ursa.lakestream.reader.CompactedObjectReader;
import io.lakestream.ursa.storage.Entry;
import io.lakestream.ursa.storage.OwnedResultFutures;
import io.lakestream.ursa.storage.impl.exception.EntryCacheClosedException;
import io.oxia.client.api.AsyncOxiaClient;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Predicate;
import javax.annotation.Nullable;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

/**
 * Rich implementation of {@link LogCursor} — per-log cursor with position tracking,
 * individual acknowledgment, mark-delete persistence, and index prefetching.
 *
 * <p>Delegates data operations to the underlying {@link Log} and maintains
 * read/mark-delete offsets. Composes {@link IndividualAcksTracker} for per-message
 * acknowledgment and {@link CursorStateStore} for persisting cursor state to Oxia.
 */
@Slf4j
public class LogCursorImpl implements LogCursor {

    private final String name;
    private final Log logDelegate;

    @Getter(AccessLevel.PACKAGE)
    private volatile long readOffset;
    @Getter(AccessLevel.PACKAGE)
    private volatile long markDeleteOffset;
    @Getter(AccessLevel.PACKAGE)
    private volatile long persistedMarkDeleteOffset;

    private Map<String, Long> markDeleteProperties;

    @Nullable private final Long cursorId;
    @Nullable private final AsyncOxiaClient oxia;
    private final long streamId;
    private final boolean durable;

    @Getter
    private final IndividualAcksTracker individualAcksTracker;

    private final ReadWriteLock lock = new ReentrantReadWriteLock();
    private Optional<RateLimiter> persistenceRateLimiter = Optional.empty();

    // --- Index prefetch state ---
    private final ConcurrentLinkedDeque<EntryIndex> preFetchedIndexes = new ConcurrentLinkedDeque<>();
    private volatile int preFetchedMessageCount = 0;
    private volatile CompletableFuture<Void> preFetchIndexesFuture = CompletableFuture.completedFuture(null);
    private static final int MAX_PREFETCHED_INDEXES = 10_000;
    private static final int PREFETCH_FACTOR = 5;
    private static final long SYNC_TIMEOUT_SECONDS = 30;
    private static final int MAX_PRE_FETCH_SIZE = 64 * 1024 * 1024;

    // PARQUET prefetch state
    private volatile CompletableFuture<Long> previousPrefetchEntryOffsetFuture;
    private EntryIndex prefetchedParquetIndex;
    private volatile CompletableFuture<Void> prefetchedParquetIndexFuture =
            CompletableFuture.completedFuture(null);
    // Bound on how far isCacheLifecycleFailure() walks a wrapped cause chain.
    private static final int MAX_CAUSE_CHAIN_DEPTH = 16;
    private static final int PARQUET_TOTAL_CACHE_COUNT = 5;
    private static final int TRIGGER_PARQUET_CACHE_ONE_ROUND = 5;
    private final AtomicInteger currentParquetCacheCount = new AtomicInteger(1);

    @Nullable private volatile CompactedObjectReader compactedObjectReader;

    // Adaptive sizing
    private int avgEntrySize = -1;
    private int avgMessageCountPerIndex = 1;

    // Pre-resolved next index
    @Getter
    private volatile CompletableFuture<EntryIndex> nextReadIndex =
            CompletableFuture.completedFuture(null);

    private volatile long previousReadOffset;
    private volatile long cachedLastOffset = -1;
    private long indexesCacheMessageRequest = MAX_PREFETCHED_INDEXES;
    private long indexesCacheBuildTimestamp = 0;
    private final long maxIndexesCacheBuildDelayInMillis;

    /**
     * Full constructor for durable cursors with persistence and individual acks.
     */
    public LogCursorImpl(String name, Log log, long streamId,
                         @Nullable Long cursorId, @Nullable AsyncOxiaClient oxia,
                         boolean durable, double throttleMarkDelete,
                         long maxIndexesCacheBuildDelayInMillis) {
        this.name = name;
        this.logDelegate = log;
        this.streamId = streamId;
        this.cursorId = cursorId;
        this.oxia = oxia;
        this.durable = durable;
        this.readOffset = 0;
        this.markDeleteOffset = -1L;
        this.persistedMarkDeleteOffset = -1L;
        this.markDeleteProperties = Collections.emptyMap();
        this.maxIndexesCacheBuildDelayInMillis = maxIndexesCacheBuildDelayInMillis;
        this.individualAcksTracker = new IndividualAcksTracker(oxia, cursorId != null ? cursorId : 0L, durable);
        if (throttleMarkDelete > 0) {
            persistenceRateLimiter = Optional.of(RateLimiter.create(throttleMarkDelete));
        }
    }

    /**
     * Simple constructor for non-durable cursors (in-memory only).
     */
    public LogCursorImpl(String name, Log log, long initialReadOffset, long initialMarkDeleteOffset) {
        this.name = name;
        this.logDelegate = log;
        this.streamId = 0;
        this.cursorId = null;
        this.oxia = null;
        this.durable = false;
        this.readOffset = initialReadOffset;
        this.markDeleteOffset = initialMarkDeleteOffset;
        this.persistedMarkDeleteOffset = initialMarkDeleteOffset;
        this.markDeleteProperties = Collections.emptyMap();
        this.maxIndexesCacheBuildDelayInMillis = 0;
        this.individualAcksTracker = new IndividualAcksTracker(null, 0L, false);
    }

    /**
     * Initializes cursor state from persisted storage (Oxia).
     * Call after constructing a durable cursor to load mark-delete position and individual acks.
     */
    public CompletableFuture<LogCursorImpl> initialize() {
        return individualAcksTracker.initialize()
                .thenCompose(__ -> {
                    if (oxia == null || cursorId == null) {
                        return CompletableFuture.completedFuture(null);
                    }
                    return CursorStateStore.readMarkDeletePosition(oxia, streamId, cursorId);
                })
                .thenApply(markDeleteRecord -> {
                    if (markDeleteRecord != null) {
                        this.markDeleteProperties = markDeleteRecord.properties();
                        this.markDeleteOffset = markDeleteRecord.offset();
                        this.persistedMarkDeleteOffset = markDeleteRecord.offset();
                        this.readOffset = markDeleteRecord.offset() + 1;
                        this.previousReadOffset = readOffset;
                        this.indexesCacheBuildTimestamp = 0;
                    }
                    return this;
                });
    }

    /**
     * Creates a new cursor with the given initial mark-delete offset and properties.
     */
    public CompletableFuture<LogCursorImpl> create(long initialMarkDeleteOffset,
                                                    Map<String, Long> markDeleteProperties) {
        if (oxia != null && cursorId != null) {
            return CursorStateStore.writeMarkDeletePosition(oxia, streamId, cursorId,
                            initialMarkDeleteOffset, markDeleteProperties)
                    .thenApply(__ -> {
                        this.markDeleteProperties = markDeleteProperties;
                        this.markDeleteOffset = initialMarkDeleteOffset;
                        this.persistedMarkDeleteOffset = initialMarkDeleteOffset;
                        this.readOffset = initialMarkDeleteOffset + 1;
                        this.previousReadOffset = readOffset;
                        this.indexesCacheBuildTimestamp = 0;
                        return this;
                    });
        }
        this.markDeleteProperties = markDeleteProperties;
        this.markDeleteOffset = initialMarkDeleteOffset;
        this.persistedMarkDeleteOffset = initialMarkDeleteOffset;
        this.readOffset = initialMarkDeleteOffset + 1;
        this.previousReadOffset = readOffset;
        return CompletableFuture.completedFuture(this);
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public Log log() {
        return logDelegate;
    }

    @Override
    public long readOffset() {
        return readOffset;
    }

    @Override
    public long markDeleteOffset() {
        return markDeleteOffset;
    }

    public void setCachedLastOffset(long offset) {
        this.cachedLastOffset = offset;
    }

    public void setReadOffset(long offset) {
        this.readOffset = offset;
        this.previousReadOffset = offset;
        this.indexesCacheBuildTimestamp = 0;
        this.preFetchedIndexes.clear();
        this.preFetchedMessageCount = 0;
    }

    @Override
    public CompletableFuture<List<LogEntry>> readEntries(int maxEntries, long maxSizeBytes) {
        // Refresh the cached upper bound before reading when it has not been initialized.
        if (cachedLastOffset < 0) {
            refreshLastOffset();
        }
        return OwnedResultFutures.transferLogEntries(
            internalReadEntries(maxEntries, Long.MAX_VALUE, maxSizeBytes, null, new ArrayList<>(), 0));
    }

    @Override
    public CompletableFuture<List<LogEntry>> readEntries(int maxEntries, long maxSizeBytes,
            Predicate<Long> skipCondition, long maxOffset) {
        // Pre-filter: merge skipCondition with isOffsetDeleted
        long scanMax = Math.min(maxOffset, readOffset + 10000);
        long[] filterResult = preFilterEntries(readOffset, scanMax, skipCondition);
        long startReadOffset = filterResult[0];
        if (startReadOffset == -1) {
            if (scanMax > this.readOffset) {
                this.readOffset = scanMax;
            }
            return CompletableFuture.completedFuture(List.of());
        }
        if (startReadOffset > readOffset) {
            this.readOffset = startReadOffset;
        }

        return OwnedResultFutures.transferLogEntries(
            internalReadEntries(maxEntries, maxOffset, maxSizeBytes, skipCondition, new ArrayList<>(), 0));
    }

    private CompletableFuture<List<LogEntry>> internalReadEntries(
            int maxEntries, long maxOffset, long maxSizeBytes,
            @Nullable Predicate<Long> skipCondition,
            List<LogEntry> result, long resultSizeIn) {
        // Use nextReadIndex to determine file type before starting prefetch
        boolean useRawPrefetch = true;
        if (nextReadIndex.isDone() && !nextReadIndex.isCompletedExceptionally()) {
            EntryIndex resolved = nextReadIndex.join();
            if (resolved != null && !resolved.position().isBinary()) {
                useRawPrefetch = false;
            }
        }

        int remainingEntries = maxEntries - result.size();
        long remainingSizeBytes = maxSizeBytes - resultSizeIn;

        // Wait for prefetch to complete before consuming the cached indexes.
        if (useRawPrefetch) {
            if (preFetchedIndexes.isEmpty()) {
                preFetchIndexesFuture.join();
            }
            triggerPrefetchIfReady(remainingEntries);
        }

        final boolean rawPrefetch = useRawPrefetch;
        final long currentResultSize = resultSizeIn;

        // Poll from prefetch cache synchronously, then read via WAL direct path.
        List<EntryIndex> indices = rawPrefetch
            ? pollRawFromPrefetchCache(remainingEntries, remainingSizeBytes, maxOffset)
            : List.of();

        final CompletableFuture<List<LogEntry>> readFuture;
        if (!indices.isEmpty()) {
            int messageBudget = sumNumberOfMessages(indices);
            // Trigger next prefetch for future reads
            EntryIndex lastPolled = indices.get(indices.size() - 1);
            long nextPrefetchFrom = lastPolled.header().offset()
                + lastPolled.header().numberOfMessages();
            if (preFetchedMessageCount < (long) messageBudget * PREFETCH_FACTOR) {
                long prefetchCount = Math.max(indexesCacheMessageRequest,
                    Math.max(MAX_PREFETCHED_INDEXES,
                        (readOffset - previousReadOffset) * PREFETCH_FACTOR));
                preFetchIndexesFuture = buildIndexesCache(nextPrefetchFrom, prefetchCount);
            }
            // Collect positions for WAL data prefetch
            List<Position> preFetchPositions = collectPrefetchPositions(maxSizeBytes);
            logDelegate.logStorage().preFetchEntries(logDelegate.id(), preFetchPositions);
            // Read from WAL directly using index (no Oxia lookup)
            readFuture = logDelegate.logStorage().readEntriesByIndex(logDelegate.id(), indices,
                readOffset, maxOffset, messageBudget, remainingSizeBytes,
                this::isOffsetDeleted, skipCondition);
        } else {
            long fallbackReadOffset = readOffset;
            readFuture = selectMessageCountForReadAsync(fallbackReadOffset, remainingEntries,
                    remainingSizeBytes, maxOffset)
                .thenCompose(messageBudget -> {
                    if (messageBudget <= 0) {
                        return CompletableFuture.completedFuture(List.of());
                    }
                    return logDelegate.readEntries(fallbackReadOffset, messageBudget, remainingSizeBytes);
                });
        }

        final long readOffsetBefore = this.readOffset;
        // internalReadEntries may block when running on Oxia internal threads, and readFuture is
        // completed by an Oxia thread as well. Use thenComposeAsync to avoid blocking Oxia's thread.
        return readFuture.thenComposeAsync(entries -> {
            int nextUnownedEntry = 0;
            long newResultSize = currentResultSize;
            try {
                lock.readLock().lock();
                try {
                    long newReadOffset = this.readOffset;
                    boolean isParquetRead = false;
                    while (nextUnownedEntry < entries.size()) {
                        LogEntry entry = entries.get(nextUnownedEntry);
                        long entryOffset = entry.offset();
                        int entrySize = entry.size();
                        if (result.size() >= maxEntries
                                || entryOffset >= maxOffset
                                || (!result.isEmpty()
                                    && exceedsSizeLimit(newResultSize, entrySize, maxSizeBytes))) {
                            break;
                        }
                        newReadOffset = entryOffset + entry.numberOfRecords();
                        if (isOffsetDeleted(entryOffset)
                                || (skipCondition != null && skipCondition.test(entryOffset))) {
                            entry.close();
                            nextUnownedEntry++;
                            continue;
                        }
                        result.add(entry);
                        nextUnownedEntry++;
                        newResultSize += entrySize;
                    }
                    if (newReadOffset > this.readOffset) {
                        this.previousReadOffset = this.readOffset;
                        this.readOffset = newReadOffset;
                        this.nextReadIndex = logDelegate.getEntryIndex(readOffset);
                        if (!entries.isEmpty()) {
                            isParquetRead = !rawPrefetch;
                        }
                    }
                    // Trigger PARQUET prefetch only if we read from PARQUET
                    if (isParquetRead) {
                        int readCount = Math.max(1,
                            avgEntrySize <= 0 ? maxEntries : (int) (maxSizeBytes / avgEntrySize));
                        triggerParquetPrefetch(readOffset, readCount, maxSizeBytes);
                    }
                } finally {
                    lock.readLock().unlock();
                }

                // Recursive top-up: fill quota if more entries available.
                // Also check that readOffset actually advanced to prevent infinite
                // loops when all returned entries are past maxOffset or filtered.
                if (result.size() < maxEntries && hasMoreEntries()
                        && (maxSizeBytes < 0 || newResultSize < maxSizeBytes)
                        && !entries.isEmpty()
                        && this.readOffset > readOffsetBefore) {
                    return internalReadEntries(maxEntries, maxOffset, maxSizeBytes,
                        skipCondition, result, newResultSize);
                }
                return CompletableFuture.completedFuture(result);
            } finally {
                closeEntries(entries, nextUnownedEntry);
            }
        }).exceptionally(ex -> {
            closeEntries(result, 0);
            if (isCacheLifecycleFailure(ex)) {
                // A WAL cache segment was closed underneath the read. That is transient and scoped to
                // one segment, so drop only this cursor's prefetch state: invalidating the log-wide
                // index cache here would force every other cursor on the log to re-read its indexes
                // because of a microsecond-scale miss.
                log.warn("Transient cache lifecycle error reading entries for cursor {}: {}. "
                    + "Clearing prefetch state.", name, ex.getMessage());
                clearPrefetchCache();
                throw new CompletionException(ex);
            }
            log.warn("Error reading entries for cursor {}: {}. Clearing cache.", name, ex.getMessage());
            // Invalidate shared index cache + all prefetch state
            logDelegate.invalidateCache();
            clearPrefetchCache();
            throw new CompletionException(ex);
        });
    }

    /**
     * Reports whether {@code failure} was caused by a WAL cache segment being closed or recycled,
     * rather than by a genuine index or storage problem.
     *
     * <p>Walks a bounded slice of the cause chain because the storage layer wraps the original cause
     * in {@link CompletionException} and its own exception types. Guards against self-referential and
     * cyclic causes.
     */
    private static boolean isCacheLifecycleFailure(Throwable failure) {
        Throwable cause = failure;
        for (int depth = 0; cause != null && depth < MAX_CAUSE_CHAIN_DEPTH; depth++) {
            if (cause instanceof EntryCacheClosedException) {
                return true;
            }
            Throwable next = cause.getCause();
            if (next == cause) {
                break;
            }
            cause = next;
        }
        return false;
    }

    @Override
    public CompletableFuture<LogEntry> readEntry(long offset) {
        CompletableFuture<LogEntry> read = logDelegate.readEntries(offset, 1, Long.MAX_VALUE, true)
                .thenApply(entries -> {
                    if (entries.isEmpty()) {
                        throw new IllegalArgumentException("No entry found at offset " + offset);
                    }
                    LogEntry result = entries.get(0);
                    closeEntries(entries, 1);
                    return result;
                });
        return OwnedResultFutures.transfer(read, entry -> {
            if (entry != null) {
                entry.close();
            }
        });
    }

    @Override
    public CompletableFuture<Void> markDelete(long offset, Map<String, Long> properties) {
        if (this.markDeleteOffset == offset
                && (properties.isEmpty() || this.markDeleteProperties.equals(properties))) {
            return CompletableFuture.completedFuture(null);
        }
        lock.writeLock().lock();
        try {
            this.markDeleteOffset = offset;
            if (readOffset <= offset) {
                readOffset = offset + 1;
                previousReadOffset = readOffset;
                indexesCacheBuildTimestamp = 0;
            }
            individualAcksTracker.trimToOffset(offset);
            markDeleteProperties = properties;
        } finally {
            lock.writeLock().unlock();
        }

        if (shouldPersistNow()) {
            return persistMarkDeleteOffset();
        }
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletableFuture<Void> seek(long offset) {
        setReadOffset(offset);
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletableFuture<LogEntryHeader> getEntryMetadata(long offset) {
        return logDelegate.getEntryMetadata(offset);
    }

    // --- Individual acknowledgment ---

    @Override
    public CompletableFuture<Void> individualDelete(long offset, int numberOfRecords) {
        lock.writeLock().lock();
        try {
            if (numberOfRecords <= 0) {
                individualAcksTracker.deleteOffset(offset);
            } else {
                for (long o = offset; o < offset + numberOfRecords; o++) {
                    individualAcksTracker.deleteOffset(o);
                }
            }
        } finally {
            lock.writeLock().unlock();
        }
        return CompletableFuture.completedFuture(null);
    }

    /**
     * Deletes individually acknowledged offsets from the tracker.
     * Each position may include an ack set for batch entries.
     *
     * @param offsets list of offset values to individually delete
     * @param ackSets parallel list of ack sets (null entries mean single-offset delete)
     */
    public void deleteOffsets(List<Long> offsets, List<long[]> ackSets) {
        lock.writeLock().lock();
        try {
            for (int i = 0; i < offsets.size(); i++) {
                long offset = offsets.get(i);
                long[] ackSet = ackSets != null ? ackSets.get(i) : null;
                individualAcksTracker.deleteOffset(offset, ackSet);
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public boolean isOffsetIndividuallyDeleted(long offset) {
        return individualAcksTracker.contains(offset);
    }

    @Override
    public long individualDeleteCount() {
        return individualAcksTracker.count();
    }

    @Override
    public long firstNonDeletedOffset() {
        return individualAcksTracker.firstNonDeletedOffset(markDeleteOffset);
    }

    /**
     * Returns true if the offset is deleted (either by mark-delete or individual ack).
     */
    public boolean isOffsetDeleted(long offset) {
        if (durable) {
            return offset <= markDeleteOffset || individualAcksTracker.contains(offset);
        }
        return individualAcksTracker.contains(offset);
    }

    // --- Persistence ---

    @Override
    public CompletableFuture<Void> persistState() {
        CompletableFuture<Void> flushMarkDelete = persistedMarkDeleteOffset != markDeleteOffset
                ? persistMarkDeleteOffset()
                : CompletableFuture.completedFuture(null);
        return CompletableFuture.allOf(flushMarkDelete, individualAcksTracker.flush());
    }

    @Override
    public long persistedMarkDeleteOffset() {
        return persistedMarkDeleteOffset;
    }

    @Override
    public Map<String, Long> properties() {
        return markDeleteProperties;
    }

    // --- Lifecycle ---

    @Override
    public boolean hasMoreEntries() {
        return readOffset < cachedLastOffset;
    }

    /**
     * Refreshes the cached last offset by querying the log.
     * Must be called from a thread that is safe for blocking Oxia calls.
     */
    public void refreshLastOffset() {
        try {
            var lastOffset = logDelegate.getLastOffset().get(SYNC_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            cachedLastOffset = lastOffset.offset() + lastOffset.numberOfRecords();
        } catch (Exception e) {
            // Keep the previous cached value
        }
    }

    @Override
    public long getNumberOfEntriesInBacklog() {
        try {
            var lastOffset = logDelegate.getLastOffset().get(SYNC_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            long lastOff = lastOffset.offset() + lastOffset.numberOfRecords();
            return lastOff - markDeleteOffset - 1 - individualAcksTracker.count();
        } catch (Exception e) {
            return 0;
        }
    }

    @Override
    public CompletableFuture<Void> deleteCursor() {
        CompletableFuture<Void> removeMarkDelete =
                (oxia != null)
                        ? CursorStateStore.removeMarkDeletePosition(oxia, streamId, cursorId)
                        : CompletableFuture.completedFuture(null);
        return removeMarkDelete
                .thenCompose(__ -> individualAcksTracker.remove());
    }

    @Override
    public void close() throws Exception {
        if (!durable) {
            clearPrefetchCache();
            return;
        }
        persistState().get(SYNC_TIMEOUT_SECONDS, TimeUnit.SECONDS);
    }

    // --- Ack set and batch helpers ---

    /**
     * Flushes individual acks tracker if persistence rate allows.
     */
    public CompletableFuture<Boolean> tryFlushIndividualAcks() {
        if (shouldPersistNow()) {
            return individualAcksTracker.flush().thenApply(__ -> true);
        }
        return CompletableFuture.completedFuture(false);
    }

    /**
     * Flushes individual acks tracker unconditionally.
     */
    public CompletableFuture<Void> flushIndividualAcks() {
        return individualAcksTracker.flush();
    }

    /**
     * Counts individually deleted entries in range.
     */
    public long countIndividualAcksInRange(long from, long to) {
        return individualAcksTracker.countFromRange(from, to);
    }

    // --- Index prefetch ---

    @VisibleForTesting
    CompletableFuture<Void> buildIndexesCache(long startOffset, long count) {
        if (preFetchedMessageCount <= MAX_PREFETCHED_INDEXES) {
            if (startOffset + count >= getLastOffsetValue()
                    && System.currentTimeMillis() - indexesCacheBuildTimestamp
                    < maxIndexesCacheBuildDelayInMillis) {
                return CompletableFuture.completedFuture(null);
            }

            return logDelegate.readIndexRange(startOffset, startOffset + count)
                    .thenCompose(indices -> {
                        indexesCacheBuildTimestamp = System.currentTimeMillis();
                        if (indices.isEmpty()) {
                            indexesCacheMessageRequest = count * 2;
                            return CompletableFuture.completedFuture(null);
                        }
                        for (var index : indices) {
                            preFetchedIndexes.add(index);
                            logDelegate.cacheIndex(index);
                            incrementPrefetchedMessageCount(index.header().numberOfMessages());
                        }
                        return CompletableFuture.completedFuture(null);
                    });
        }
        return CompletableFuture.completedFuture(null);
    }

    ConcurrentLinkedDeque<EntryIndex> getPrefetchedIndexes() {
        return preFetchedIndexes;
    }

    int getPrefetchedMessageCount() {
        return preFetchedMessageCount;
    }

    void decrementPrefetchedMessageCount(int amount) {
        preFetchedMessageCount = Math.max(0, preFetchedMessageCount - amount);
    }

    @VisibleForTesting
    void setPrefetchedMessageCount(int count) {
        this.preFetchedMessageCount = count;
    }

    @VisibleForTesting
    void setNextReadIndex(CompletableFuture<EntryIndex> future) {
        this.nextReadIndex = future;
    }

    private void triggerPrefetchIfReady(int maxEntries) {
        if (preFetchIndexesFuture.isDone()) {
            validateAndRefreshCache();
            if (preFetchedIndexes.isEmpty()) {
                long prefetchCount = Math.max(indexesCacheMessageRequest,
                    Math.max(MAX_PREFETCHED_INDEXES,
                        (readOffset - previousReadOffset) * PREFETCH_FACTOR));
                preFetchIndexesFuture = buildIndexesCache(readOffset, prefetchCount);
            }
        }
    }

    private void validateAndRefreshCache() {
        EntryIndex first = preFetchedIndexes.peekFirst();
        if (first != null) {
            long firstOffset = first.header().offset();
            if (readOffset < firstOffset
                    || readOffset > firstOffset + first.header().numberOfMessages() - 1) {
                preFetchedIndexes.clear();
                preFetchedMessageCount = 0;
            }
        }
    }

    private List<EntryIndex> pollRawFromPrefetchCache(int maxEntries, long maxSizeBytes,
                                                      long maxOffset) {
        List<EntryIndex> result = new ArrayList<>();
        long totalSize = 0;
        long messageCount = 0;
        while (!preFetchedIndexes.isEmpty() && result.size() < maxEntries) {
            EntryIndex next = preFetchedIndexes.peekFirst();
            if (next == null) {
                break;
            }
            // Skip entries before readOffset
            if (next.header().offset() + next.header().numberOfMessages() <= readOffset) {
                popPrefetchedIndex();
                continue;
            }
            // Stop at compacted entries and fall back to the UnifiedStreamReader path.
            if (!next.position().isBinary()) {
                popPrefetchedIndex();
                nextReadIndex = CompletableFuture.completedFuture(next);
                break;
            }
            EntryHeader eh = next.header();
            popPrefetchedIndex();
            // Skip individually deleted offsets (single-entry indexes only)
            if (next.entryCount() == 1 && isOffsetDeleted(eh.offset())) {
                continue;
            }
            if (eh.offset() >= maxOffset
                    || (!result.isEmpty()
                        && exceedsSizeLimit(totalSize, eh.entrySize(), maxSizeBytes))) {
                pushBackPrefetchedIndex(next);
                break;
            }
            result.add(next);
            totalSize += eh.entrySize();
            messageCount += eh.numberOfMessages();
        }
        // Update adaptive sizing
        if (messageCount > 0 && !result.isEmpty()) {
            avgMessageCountPerIndex = (int) Math.max(1, messageCount / result.size());
            indexesCacheMessageRequest = (long) avgMessageCountPerIndex * PREFETCH_FACTOR;
        }
        return result;
    }

    private EntryIndex popPrefetchedIndex() {
        EntryIndex index = preFetchedIndexes.pollFirst();
        if (index != null) {
            decrementPrefetchedMessageCount(index.header().numberOfMessages());
        }
        return index;
    }

    private void pushBackPrefetchedIndex(EntryIndex index) {
        preFetchedIndexes.addFirst(index);
        incrementPrefetchedMessageCount(index.header().numberOfMessages());
    }

    private void incrementPrefetchedMessageCount(int amount) {
        preFetchedMessageCount = (int) Math.min(Integer.MAX_VALUE,
            (long) preFetchedMessageCount + amount);
    }

    /**
     * Converts the cursor entry quota into the message quota expected by fallback
     * lower readers. It walks entry indexes from {@code startOffset}, selects at
     * most {@code maxEntries} entries while respecting byte and offset bounds, then
     * returns the sum of the selected entries' message counts. The first candidate
     * entry is always selected before the byte limit is considered, so small
     * {@code maxSizeBytes} values still produce a non-zero lower-reader message
     * budget when an entry is available.
     */
    private CompletableFuture<Integer> selectMessageCountForReadAsync(long startOffset, int maxEntries,
                                                                      long maxSizeBytes, long maxOffset) {
        if (maxEntries <= 0 || startOffset >= maxOffset) {
            return CompletableFuture.completedFuture(0);
        }

        CompletableFuture<Integer> result = new CompletableFuture<>();
        advanceMessageCountSelectionAsync(new MessageCountSelectionState(startOffset), maxEntries,
            maxSizeBytes, maxOffset, result);
        return result;
    }

    private void advanceMessageCountSelectionAsync(MessageCountSelectionState state, int maxEntries,
                                                   long maxSizeBytes, long maxOffset,
                                                   CompletableFuture<Integer> result) {
        if (result.isDone()) {
            return;
        }
        if (state.done || state.entryCount >= maxEntries || state.currentOffset >= maxOffset) {
            result.complete((int) state.messageCount);
            return;
        }

        resolveEntryIndexAsync(state.currentOffset).whenCompleteAsync((index, error) -> {
            if (error != null) {
                result.completeExceptionally(error);
                return;
            }
            selectMessageCount(state, index, maxSizeBytes, maxOffset);
            advanceMessageCountSelectionAsync(state, maxEntries, maxSizeBytes, maxOffset, result);
        });
    }

    private static void selectMessageCount(MessageCountSelectionState state, EntryIndex index,
                                           long maxSizeBytes, long maxOffset) {
        if (index == null || index == EntryIndex.NOT_FOUND
                || index.header() == EntryHeader.NOT_FOUND) {
            state.done = true;
            return;
        }
        EntryHeader header = entryHeaderForOffset(index, state.currentOffset);
        if (header == EntryHeader.NOT_FOUND || header.numberOfMessages() <= 0) {
            state.done = true;
            return;
        }
        long nextOffset = header.offset() + header.numberOfMessages();
        if (nextOffset <= state.currentOffset) {
            state.done = true;
            return;
        }
        long nextTotalSize = state.totalSize + header.entrySize();
        long nextMessageCount = state.messageCount + header.numberOfMessages();
        if (nextMessageCount >= Integer.MAX_VALUE) {
            state.messageCount = Integer.MAX_VALUE;
            state.done = true;
            return;
        }
        if (nextOffset > maxOffset || exceedsSizeLimit(nextTotalSize, 0, maxSizeBytes)) {
            state.messageCount = nextMessageCount;
            state.done = true;
            return;
        }
        state.currentOffset = nextOffset;
        state.entryCount++;
        state.totalSize = nextTotalSize;
        state.messageCount = nextMessageCount;
    }

    private static final class MessageCountSelectionState {
        private long currentOffset;
        private int entryCount;
        private long totalSize;
        private long messageCount;
        private boolean done;

        private MessageCountSelectionState(long currentOffset) {
            this.currentOffset = currentOffset;
        }
    }

    private CompletableFuture<EntryIndex> resolveEntryIndexAsync(long offset) {
        if (nextReadIndex.isDone() && !nextReadIndex.isCompletedExceptionally()) {
            EntryIndex resolved = nextReadIndex.getNow(EntryIndex.NOT_FOUND);
            if (resolved != null && resolved != EntryIndex.NOT_FOUND
                    && containsOffset(resolved, offset)) {
                return CompletableFuture.completedFuture(resolved);
            }
        }
        try {
            return logDelegate.getEntryIndex(offset);
        } catch (Exception e) {
            return CompletableFuture.failedFuture(e);
        }
    }

    private static boolean containsOffset(EntryIndex index, long offset) {
        EntryHeader header = index.header();
        return header != EntryHeader.NOT_FOUND
            && header.offset() <= offset
            && offset < header.offset() + header.numberOfMessages();
    }

    private static EntryHeader entryHeaderForOffset(EntryIndex index, long offset) {
        try {
            return index.searchEntryHeader(offset);
        } catch (RuntimeException e) {
            return index.header();
        }
    }

    private static int sumNumberOfMessages(List<EntryIndex> indexes) {
        long messageCount = 0;
        for (EntryIndex index : indexes) {
            messageCount += index.header().numberOfMessages();
            if (messageCount >= Integer.MAX_VALUE) {
                return Integer.MAX_VALUE;
            }
        }
        return (int) messageCount;
    }

    private static boolean exceedsSizeLimit(long currentSize, int additionalSize, long maxSizeBytes) {
        return maxSizeBytes >= 0 && currentSize + additionalSize > maxSizeBytes;
    }

    private void closeEntries(List<LogEntry> entries, int fromIndex) {
        for (int i = fromIndex; i < entries.size(); i++) {
            LogEntry entry = entries.get(i);
            if (entry == null) {
                continue;
            }
            try {
                entry.close();
            } catch (RuntimeException | Error e) {
                log.warn("Failed to close log entry at list index {} for cursor {}", i, name, e);
            }
        }
    }

    /**
     * Collects WAL file positions from the remaining prefetch cache for data pre-warming.
     */
    private List<Position> collectPrefetchPositions(long maxSizeBytes) {
        List<Position> positions = new ArrayList<>();
        long preFetchSize = 0;
        long maxPreFetchSize = Math.min(maxSizeBytes * PREFETCH_FACTOR, MAX_PRE_FETCH_SIZE);
        for (EntryIndex index : preFetchedIndexes) {
            positions.add(index.position());
            preFetchSize += index.header().entrySize();
            if (preFetchSize >= maxPreFetchSize) {
                break;
            }
        }
        return positions;
    }

    /**
     * Scans offset range to find first non-deleted/non-skipped offset, avoiding unnecessary reads.
     */
    private long[] preFilterEntries(long startOffset, long maxOffset,
                                     @Nullable Predicate<Long> skipCondition) {
        long firstValid = -1;
        long lastValid = -1;
        for (long offset = startOffset; offset < maxOffset; offset++) {
            boolean shouldSkip = isOffsetDeleted(offset)
                || (skipCondition != null && skipCondition.test(offset));
            if (shouldSkip) {
                if (firstValid != -1) {
                    break;
                }
            } else {
                if (firstValid == -1) {
                    firstValid = offset;
                }
                lastValid = offset;
            }
        }
        return new long[]{firstValid, firstValid == -1 ? 0 : (lastValid - firstValid + 1)};
    }

    private void clearPrefetchCache() {
        preFetchedIndexes.clear();
        preFetchedMessageCount = 0;
        preFetchIndexesFuture = CompletableFuture.completedFuture(null);
        nextReadIndex = CompletableFuture.completedFuture(null);
        previousPrefetchEntryOffsetFuture = null;
    }

    // --- PARQUET prefetch ---

    /**
     * Sets the compacted object reader used for PARQUET prefetch after cursor construction.
     */
    public void setCompactedObjectReader(@Nullable CompactedObjectReader reader) {
        this.compactedObjectReader = reader;
    }

    @VisibleForTesting
    @Nullable
    public CompactedObjectReader getCompactedObjectReader() {
        return compactedObjectReader;
    }

    /**
     * Triggers PARQUET data prefetch after a PARQUET read completes.
     * Chains async prefetches via previousPrefetchEntryOffsetFuture.
     */
    void triggerParquetPrefetch(long readOffset, int readCount, long maxSizeBytes) {
        CompactedObjectReader reader = compactedObjectReader;
        if (reader == null) {
            return;
        }
        synchronized (this) {
            if (previousPrefetchEntryOffsetFuture != null
                    && !previousPrefetchEntryOffsetFuture.isDone()
                    && !previousPrefetchEntryOffsetFuture.isCompletedExceptionally()) {
                return;
            }
            if (previousPrefetchEntryOffsetFuture == null
                    || previousPrefetchEntryOffsetFuture.isCompletedExceptionally()) {
                previousPrefetchEntryOffsetFuture = preFetchParquetCache(
                    readOffset, readCount, maxSizeBytes);
            } else {
                previousPrefetchEntryOffsetFuture = previousPrefetchEntryOffsetFuture
                    .thenCompose(previousOffset -> preFetchParquetCache(
                        previousOffset, readCount, maxSizeBytes))
                    .exceptionally(ex -> {
                        previousPrefetchEntryOffsetFuture = null;
                        return readOffset;
                    });
            }
        }
    }

    private CompletableFuture<Long> preFetchParquetCache(long readOffset, int toRead, long maxSizeBytes) {
        CompactedObjectReader reader = compactedObjectReader;
        if (reader == null) {
            return CompletableFuture.completedFuture(readOffset);
        }
        if (prefetchedParquetIndexFuture.isDone()) {
            if (prefetchedParquetIndex == null
                    || EntryHeader.NOT_FOUND == prefetchedParquetIndex.header()) {
                prefetchedParquetIndexFuture = updateParquetIndexCache(readOffset);
            } else {
                EntryHeader header = prefetchedParquetIndex.header();
                long beginOffset = header.offset();
                long endOffset = header.offset() + header.numberOfMessages();
                if (readOffset < beginOffset || readOffset >= endOffset) {
                    prefetchedParquetIndexFuture = updateParquetIndexCache(readOffset);
                }
            }
        }
        return prefetchedParquetIndexFuture.thenCompose(__ -> {
            if (prefetchedParquetIndex == null) {
                return CompletableFuture.completedFuture(readOffset);
            }
            // Skip v2 index entries
            try {
                if (reader.getManagedTableFileIndex(prefetchedParquetIndex).isPresent()) {
                    return CompletableFuture.completedFuture(readOffset);
                }
            } catch (IllegalArgumentException e) {
                return CompletableFuture.completedFuture(readOffset);
            }
            EntryHeader prefetchHeader = prefetchedParquetIndex.header();
            if (EntryHeader.NOT_FOUND == prefetchHeader) {
                return CompletableFuture.completedFuture(readOffset);
            }
            long beginOffset = prefetchHeader.offset();
            long endOffset = prefetchHeader.offset() + prefetchHeader.numberOfMessages();
            if (readOffset < beginOffset || readOffset >= endOffset) {
                return preFetchParquetCache(readOffset, toRead, maxSizeBytes);
            }
            Position prefetchPosition = prefetchedParquetIndex.position();
            if (prefetchPosition == null || prefetchPosition.fileType() != Position.FileType.PARQUET) {
                prefetchedParquetIndex = null;
                return CompletableFuture.completedFuture(readOffset);
            }
            CompletableFuture<Long> future = null;
            long baseOffset = readOffset;
            for (int i = 0; i < TRIGGER_PARQUET_CACHE_ONE_ROUND
                    && currentParquetCacheCount.get() < PARQUET_TOTAL_CACHE_COUNT
                    && reader.hasSpaceInCache(); i++) {
                if (baseOffset >= endOffset) {
                    future = preFetchParquetCache(baseOffset, toRead, maxSizeBytes);
                    break;
                }
                int readCount = baseOffset + toRead > endOffset
                    ? (int) (endOffset - baseOffset) : toRead;
                currentParquetCacheCount.incrementAndGet();
                future = reader.preFetchMessagesAsync(prefetchPosition.location(),
                        baseOffset, prefetchHeader.offset(), readCount, maxSizeBytes, maxSizeBytes)
                    .thenApply((Entry prefetched) -> {
                        currentParquetCacheCount.decrementAndGet();
                        return prefetched.header().offset() + prefetched.header().numberOfMessages();
                    });
                baseOffset += readCount;
            }
            if (future == null) {
                return CompletableFuture.completedFuture(readOffset);
            }
            return future;
        });
    }

    private CompletableFuture<Void> updateParquetIndexCache(long startReadOffset) {
        return logDelegate.getEntryIndex(startReadOffset)
            .thenAccept(index -> this.prefetchedParquetIndex = index);
    }

    /**
     * Returns the current mark-delete throttle rate.
     */
    public double getThrottleMarkDelete() {
        return persistenceRateLimiter.map(RateLimiter::getRate).orElse(0.0);
    }

    /**
     * Dynamically adjusts the mark-delete throttle rate.
     */
    public void setThrottleMarkDelete(double throttleMarkDelete) {
        if (throttleMarkDelete > 0.0) {
            if (persistenceRateLimiter.isEmpty()) {
                persistenceRateLimiter = Optional.of(RateLimiter.create(throttleMarkDelete));
            } else {
                persistenceRateLimiter.get().setRate(throttleMarkDelete);
            }
        } else {
            persistenceRateLimiter = Optional.empty();
        }
    }

    // --- Internal helpers ---

    private boolean shouldPersistNow() {
        return durable && cursorId != null && persistenceRateLimiter.map(RateLimiter::tryAcquire).orElse(true);
    }

    private CompletableFuture<Void> persistMarkDeleteOffset() {
        if (oxia == null) {
            return CompletableFuture.completedFuture(null);
        }
        long toPersistentOffset = this.markDeleteOffset;
        return CursorStateStore.writeMarkDeletePosition(oxia, streamId, cursorId,
                        toPersistentOffset, markDeleteProperties)
                .thenRun(() -> this.persistedMarkDeleteOffset = toPersistentOffset);
    }

    private long getLastOffsetValue() {
        return cachedLastOffset >= 0 ? cachedLastOffset : Long.MAX_VALUE;
    }
}
