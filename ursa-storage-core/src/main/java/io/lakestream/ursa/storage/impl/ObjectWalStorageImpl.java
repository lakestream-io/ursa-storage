/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.storage.impl;

import com.google.common.annotations.VisibleForTesting;
import io.grpc.netty.shaded.io.netty.util.concurrent.DefaultThreadFactory;
import io.lakestream.api.EntryIndex;
import io.lakestream.api.FileInfo;
import io.lakestream.api.LogStateManager;
import io.lakestream.api.Position;
import io.lakestream.ursa.metrics.InstrumentProvider;
import io.lakestream.ursa.storage.AddResult;
import io.lakestream.ursa.storage.Entry;
import io.lakestream.ursa.storage.EntryList;
import io.lakestream.ursa.storage.FileStorage;
import io.lakestream.ursa.storage.IDGenerator;
import io.lakestream.ursa.storage.OwnedResultFutures;
import io.lakestream.ursa.storage.WalStorage;
import io.lakestream.ursa.storage.WalStorageMetrics;
import io.lakestream.ursa.storage.impl.exception.IDGeneratorException;
import io.lakestream.ursa.storage.impl.exception.OperationRejectException;
import io.lakestream.ursa.storage.impl.exception.RetryableException;
import io.lakestream.ursa.storage.impl.exception.WalStorageException;
import io.lakestream.ursa.utils.FutureUtils;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.oxia.client.api.AsyncOxiaClient;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.atomic.AtomicLong;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.Pair;


@Slf4j
public class ObjectWalStorageImpl implements WalStorage {

    // internal used configurations and resources
    private final StorageConfig config;
    private final ByteBufAllocator allocator;
    private final FileStorage fileStorage;
    private final AsyncOxiaClient oxiaClient;
    private final StorageFormat storageFormat;
    private final IDGenerator idGenerator;

    private final long writeBufferFlushIntervalMs;

    // write and read process used resources
    private final BlockingQueue<PendingAdd> pendingAddRequests;
    private final AtomicLong pendingAddRequestsDataSize = new AtomicLong(0);
    private final long maxPendingAddRequestsInBytes;
    private boolean pauseProcessAddRequest = false;
    private long lastFlushTime = System.currentTimeMillis();
    private final WriteCache writeCache;
    private final BlockingQueue<Pair<PersistCache, CompletableFuture<String>>> flushResultQueue;
    private final ReadCache readCache;
    private final LogStateManager streamStateManager;

    private final Thread requestProcessThread;
    private final Thread resultProcessThread;
    private final ExecutorService callbackExecutor;

    private boolean initialized;
    private volatile boolean shutdown;

    private final WalStorageMetrics metrics;

    public ObjectWalStorageImpl(ByteBufAllocator allocator, FileStorage fileStorage, IDGenerator idGenerator,
                                StorageConfig storageConfig, InstrumentProvider instrumentProvider,
                                AsyncOxiaClient oxiaClient, StorageFormat storageFormat,
                                LogStateManager streamStateManager) {
        this.metrics = new WalStorageMetrics(WalStorageFactory.Type.SIMPLE.toString().toLowerCase(Locale.ROOT),
                instrumentProvider, storageConfig);
        this.config = storageConfig;
        this.allocator = allocator;
        this.fileStorage = fileStorage;
        this.oxiaClient = oxiaClient;
        this.storageFormat = storageFormat;
        this.idGenerator = idGenerator;
        this.writeBufferFlushIntervalMs = storageConfig.getWriteBufferFlushIntervalMs();
        this.requestProcessThread =
                new Thread(this::processRequestLoop, "ursa-storage-simple-wal-request-processor");
        this.resultProcessThread =
                new Thread(this::processResultLoop, "ursa-storage-simple-wal-result-processor");
        this.callbackExecutor = Executors.newSingleThreadExecutor(
                new DefaultThreadFactory("storage-simple-wal-callback-processor"));

        // the pending request queue
        this.pendingAddRequests = new LinkedBlockingDeque<>();
        this.maxPendingAddRequestsInBytes = config.getMaxPendingAddRequestsUsedBytes();
        metrics.buildPutEntryPendingGaugeWithCallback(pendingAddRequests);

        // write buffer related initialize
        this.writeCache = new WriteCache(allocator, config, instrumentProvider);
        this.flushResultQueue = new ArrayBlockingQueue<>(config.getWriteBufferSegment());
        metrics.buildWriteCacheFlushCallbackPendingGauge(flushResultQueue);

        // read cache related initialize
        this.readCache = new ReadCache(config, allocator, fileStorage, metrics);
        this.streamStateManager = streamStateManager;
    }


    @Override
    public void initialize() throws Exception {
        requestProcessThread.start();
        resultProcessThread.start();
        initialized = true;
    }

    private void processRequestLoop() {
        try {
            while (!shutdown && !Thread.currentThread().isInterrupted()) {
                processSingleRequest();
            }
        } finally {
            shutdown = true;
            clearPendingAddOps();
        }
    }

    protected void processSingleRequest() {
        try {
            if (pendingAddRequestsDataSize.get() >= config.getWriteBufferFlushSize()
                    || System.currentTimeMillis() - lastFlushTime >= writeBufferFlushIntervalMs) {
                pauseProcessAddRequest = false;
            }
            if (pauseProcessAddRequest || pendingAddRequestsDataSize.get() == 0) {
                Thread.sleep(10);
                return;
            }

            List<PendingAdd> pendingAdds = new ArrayList<>();
            pendingAddRequests.drainTo(pendingAdds);
            if (pendingAdds.isEmpty()) {
                return;
            }
            pauseProcessAddRequest = true;

            // Group pending adds by their id
            Map<Long, List<PendingAdd>> pendingAddMap = new HashMap<>();
            for (PendingAdd pendingAdd : pendingAdds) {
                pendingAddMap.computeIfAbsent(pendingAdd.id, k -> new ArrayList<>()).add(pendingAdd);
            }

            pendingAdds.clear();
            int totalStreams = 0;
            long totalSize = 0;

            for (Map.Entry<Long, List<PendingAdd>> entry : pendingAddMap.entrySet()) {
                List<PendingAdd> currentBatch = entry.getValue();
                long batchSize = currentBatch.stream()
                        .mapToLong(add -> add.buf.readableBytes())
                        .sum();

                // If this single batch exceeds buffer size, flush it separately
                if (batchSize >= config.getWriteBufferSize()) {
                    flushCurrentBatchIfNeeded(pendingAdds);
                    totalStreams = 0;
                    totalSize = 0;
                    processWriteBufferFlush(currentBatch);
                    continue;
                }

                // If adding this batch would exceed limits, flush current and start new batch
                if (totalStreams >= config.getWriteBufferMaxStreamIds()
                        || totalSize + batchSize > config.getWriteBufferSize()) {
                    flushCurrentBatchIfNeeded(pendingAdds);
                    totalStreams = 0;
                    totalSize = 0;
                }

                // Add batch to current collection
                pendingAdds.addAll(currentBatch);
                totalStreams++;
                totalSize += batchSize;
            }

            flushCurrentBatchIfNeeded(pendingAdds);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.error("Entry processor is interrupted, shutting down the processor", e);
        } catch (Exception e) {
            log.error("Failed to generate id, shutting down the processor", e);
            throw new RuntimeException(e);
        }
    }

    private void flushCurrentBatchIfNeeded(List<PendingAdd> batch) throws Exception {
        if (!batch.isEmpty()) {
            processWriteBufferFlush(batch);
            batch.clear();
        }
    }

    private void processWriteBufferFlush(List<PendingAdd> pendingAdds) throws Exception {
        for (int i = 0; i < pendingAdds.size(); i++) {
            PendingAdd pendingAdd = pendingAdds.get(i);
            this.pendingAddRequestsDataSize.addAndGet(-pendingAdd.buf.readableBytes());
            metrics.getPutEntryPendingLatency().recordSuccess(System.nanoTime() - pendingAdd.startTime);

            long startTime = System.nanoTime();
            long entryId;
            while (true) {
                PersistCache nextWriteCache = writeCache.peek();
                if (nextWriteCache == null) {
                    log.info("There is no available write buffer segment when "
                            + "retry processing pending add entries");
                    Thread.sleep(10);
                    continue;
                }
                entryId = nextWriteCache.put(pendingAdd);
                if (entryId == -1) {
                    if (!nextWriteCache.isEmpty()) {
                        writeCache.poll();
                        flush(nextWriteCache);
                    }
                    final int entrySize = pendingAdd.buf.readableBytes();
                    if (entrySize > config.getWriteBufferSize()) {
                        // Just create a new PersistCache and close the PersistCache after the entry is persisted
                        PersistCache newCache = PersistCacheFactory.create(allocator, entrySize,
                                config.getIndexSerializeFormatVersion());
                        entryId = newCache.put(pendingAdd);
                        pendingAdd.indexId = entryId;
                        flush(newCache);
                        break;
                    }
                } else {
                    pendingAdd.indexId = entryId;
                    if (i == pendingAdds.size() - 1) {
                        // If it's the last entry, flush the write cache
                        writeCache.poll();
                        flush(nextWriteCache);
                    }
                    break;
                }
            }
            metrics.getPutEntryToCacheLatency().recordSuccess(System.nanoTime() - startTime);
        }
    }

    private void flush(PersistCache cache) throws IDGeneratorException {
        lastFlushTime = System.currentTimeMillis();
        String logId = idGenerator.generate();

        var flushFuture = cache.persist(fileStorage, logId, storageFormat);
        try {
            flushResultQueue.put(Pair.of(cache, flushFuture));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private void processResultLoop() {
        while (true) {
            Pair<PersistCache, CompletableFuture<String>> flush = null;
            try {
                flush = flushResultQueue.take();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                log.error("result backoff sleep is interrupted", e);
                if (shutdown) {
                    break;
                }
            }

            if (flush == null) {
                continue;
            }

            PersistCache cache = flush.getLeft();
            var flushFuture = flush.getRight();
            String location = null;
            try {
                location = flushFuture.get();
            } catch (Exception e) {
                try {
                    cache.flushFailed(e);
                } catch (Exception ex) {
                    log.error("Failed to notify data persist failure", ex);
                }
                metrics.getWriteCacheFlushLatency()
                        .recordFailure(System.nanoTime() - cache.getFlushStartTime());
                returnOrCloseCache(cache, Optional.empty());
                continue;
            }

            String finalLocation = location;
            cache.index(oxiaClient, streamStateManager)
                    .whenCompleteAsync((indexResults, e) -> {
                        if (e != null) {
                            try {
                                cache.flushFailed(e);
                            } catch (Exception ex) {
                                log.error("Failed to notify flush failure", ex);
                            }
                            metrics.getWriteCacheFlushLatency()
                                    .recordFailure(System.nanoTime() - cache.getFlushStartTime());
                            returnOrCloseCache(cache, Optional.empty());
                            return;
                        }

                        boolean flushSucceeded = false;
                        boolean allIndexesSucceeded = false;
                        try {
                            cache.flushSucceed(finalLocation, indexResults);
                            metrics.getWriteCacheFlushLatency()
                                    .recordSuccess(System.nanoTime() - cache.getFlushStartTime());
                            flushSucceeded = true;
                            allIndexesSucceeded = indexResults.values().stream()
                                    .allMatch(r -> r instanceof StreamIndexResult.Success);
                        } catch (Exception ex) {
                            log.error("Failed to process flush results for location:{}", finalLocation, ex);
                            metrics.getWriteCacheFlushLatency()
                                    .recordFailure(System.nanoTime() - cache.getFlushStartTime());
                        } finally {
                            returnOrCloseCache(cache,
                                    flushSucceeded && allIndexesSucceeded
                                            ? Optional.ofNullable(finalLocation) : Optional.empty());
                        }
                    }, callbackExecutor);
        }
    }

    private void returnOrCloseCache(PersistCache cache, Optional<String> location) {
        try {
            if (cache.sizeInBytes() > config.getWriteBufferSize()) {
                cache.close();
            } else {
                writeCache.returnToCache(cache, location);
            }
        } catch (Exception e) {
            log.error("Failed to return or close cache segment", e);
            try {
                cache.close();
            } catch (Exception closeEx) {
                log.error("Failed to close cache segment after return failure", closeEx);
            }
        }
    }

    public void clearPendingAddOps() {
        List<PendingAdd> ops = new ArrayList<>();
        pendingAddRequests.drainTo(ops);
        for (PendingAdd op : ops) {
            op.release();
        }
        this.pendingAddRequestsDataSize.set(0);
    }

    @Override
    public CompletableFuture<AddResult> put(long id, ByteBuf buf) {
        return put(id, 1, buf);
    }

    @Override
    public CompletableFuture<AddResult> put(long id, int numberOfMessages, ByteBuf buf) {
        return put(id, numberOfMessages, -1, -1, buf);
    }

    @Override
    public CompletableFuture<AddResult> put(long id, int numberOfMessages, long initialOffset, long cumulativeSize,
                                            ByteBuf buf) {
        CompletableFuture<AddResult> future = new CompletableFuture<>();

        checkState().whenComplete((x, e) -> {
            if (e != null) {
                future.completeExceptionally(e);
                return;
            }
            PendingAdd pendingAdd;
            if (initialOffset != -1) {
                if (cumulativeSize != -1) {
                    pendingAdd = new PendingWrite(id, numberOfMessages, initialOffset, cumulativeSize, buf, future,
                            metrics.getPutEntryLatency());
                } else {
                    future.completeExceptionally(
                            new IllegalArgumentException("cumulativeSize must be set if initialOffset is set"));
                    return;
                }
            } else {
                pendingAdd = new PendingAdd(id, numberOfMessages, buf, future, metrics.getPutEntryLatency());
            }

            boolean added = addPendingRequest(pendingAdd);
            if (!added) {
                metrics.getRejectedPutEntryRequestCount().increment();
                pendingAdd.onFailure(new OperationRejectException("Pending add request queue is full. Please slow "
                        + "down the request rate or increase the maxPendingAddRequests, or increase more "
                        + "cloudStorageMaxConcurrencyRequest."));
            } else {
                metrics.getPutEntryRequestCount().increment();
            }
        });
        return OwnedResultFutures.nonCancellableCompletion(future);
    }


    @VisibleForTesting
    boolean addPendingRequest(PendingAdd pendingAdd) {
        long dataSize = pendingAdd.buf.readableBytes();
        while (true) {
            long current = pendingAddRequestsDataSize.get();
            if (current + dataSize > maxPendingAddRequestsInBytes) {
                return false;
            }
            if (pendingAddRequestsDataSize.compareAndSet(current, current + dataSize)) {
                if (!pendingAddRequests.offer(pendingAdd)) {
                    pendingAddRequestsDataSize.addAndGet(-dataSize);
                    return false;
                }
                return true;
            }
        }
    }

    @VisibleForTesting
    WriteCache getWriteCacheForTest() {
        return writeCache;
    }

    @VisibleForTesting
    AtomicLong getPendingAddRequestsDataSizeForTest() {
        return pendingAddRequestsDataSize;
    }

    @VisibleForTesting
    BlockingQueue<PendingAdd> getPendingAddRequestsForTest() {
        return pendingAddRequests;
    }


    private CompletableFuture<PersistCache> getPersistCache(long id, Position position) {
        if (log.isTraceEnabled()) {
            log.trace("Get entry for id: {}, indexId: {}, location: {}",
                    id, position.indexId(), position.location());
        }
        long start = System.nanoTime();
        CompletableFuture<PersistCache> cache = checkState().thenCompose(x -> {
            PersistCache write = writeCache.get(position.location());
            if (write == null) {
                metrics.getGetEntriesCacheMiss()
                    .increment(Attributes.of(AttributeKey.stringKey("type"), "write_cache"));
                return readCache.get(position.file(), 1);
            } else {
                return CompletableFuture.completedFuture(write);
            }
        });
        cache.whenComplete((__, e) -> {
            if (e == null) {
                metrics.getGetEntryDuration().recordSuccess(System.nanoTime() - start);
            } else {
                metrics.getGetEntryDuration().recordFailure(System.nanoTime() - start);
            }
        });
        return cache;
    }

    @Override
    public CompletableFuture<Entry> get(long id, EntryIndex index) {
        var position = index.position();
        return getPersistCache(id, position).thenCompose(c -> {
            ByteBuf payload = c.get(id, position.indexId());
            if (payload != null) {
                return CompletableFuture.completedFuture(new Entry(index.header(), payload));
            }
            return fileStorage
                    .getAsync(position.location()).thenApply(byteBuf -> {
                        PersistCache readFromStorage = PersistCacheFactory.deserialize(allocator, byteBuf,
                                config.getIndexSerializeFormatVersion());
                        ByteBuf result = readFromStorage.get(id, position.indexId());
                        readFromStorage.close();
                        return new Entry(index.header(), result);
                    });

        });
    }

    @Override
    public CompletableFuture<Entry> get(long id, long offset, EntryIndex compactedIndex) {
        return get(id, offset, compactedIndex, false);
    }

    private CompletableFuture<Entry> get(long id, long offset, EntryIndex compactedIndex, boolean hasRetried) {
        return getPersistCache(id, compactedIndex.position()).thenCompose(c -> {
            try {
                var entry = c.get(id, offset, compactedIndex);
                if (entry != null) {
                    return CompletableFuture.completedFuture(entry);
                }
            } catch (RetryableException e) {
                // Retry once to handle transient cache staleness in race condition with writeCache.
                // Only one retry needed since if data changed in PersistentCache, it's already removed from writeCache.
                if (!hasRetried) {
                    return get(id, offset, compactedIndex, true);
                } else {
                    return CompletableFuture.failedFuture(new WalStorageException(e.getMessage(), e));
                }
            }
            return fileStorage.getAsync(compactedIndex.position().location()).thenCompose(byteBuf -> {
                PersistCache readFromStorage = PersistCacheFactory.deserialize(allocator, byteBuf,
                        config.getIndexSerializeFormatVersion());
                try {
                    Entry entry2 = c.get(id, offset, compactedIndex);
                    readFromStorage.close();
                    return CompletableFuture.completedFuture(entry2);
                } catch (RetryableException e) {
                    return CompletableFuture.failedFuture(new WalStorageException(e.getMessage(), e));
                }
            });
        });
    }

    @Override
    public CompletableFuture<Void> get(List<EntryIndex> indices, EntryList entryList) {
        return get(indices, entryList, false);
    }

    private CompletableFuture<Void> get(List<EntryIndex> indices, EntryList entryList, boolean hasRetried) {
        long start = System.nanoTime();
        return checkState().thenCompose((x) -> {
            Map<String, CompletableFuture<PersistCache>> caches = new HashMap<>();
                Map<FileInfo, Integer> locations = new HashMap<>();
            for (var index : indices) {
                var position = index.position();
                locations.put(position.file(), locations.getOrDefault(position.file(), 0) + 1);
            }
            for (var e : locations.entrySet()) {
                var location = e.getKey();
                var posCount = e.getValue();
                PersistCache write = writeCache.get(location.location());
                if (write == null) {
                    metrics.getGetEntriesCacheMiss()
                            .add(posCount, Attributes.of(AttributeKey.stringKey("type"), "write_cache"));
                    caches.put(location.location(), readCache.get(location, posCount));
                } else {
                    caches.put(location.location(), CompletableFuture.completedFuture(write));
                }
            }
            CompletableFuture<Void> cachesFuture = FutureUtils.waitForAll(caches.values());
            cachesFuture.whenComplete((__, e) -> {
                if (e != null) {
                    metrics.getGetEntriesDuration().recordFailure(System.nanoTime() - start);
                }
            });
            return cachesFuture.thenCompose(__ -> {
                boolean hasNullValue = false;
                List<CompletableFuture<ByteBuf>> getFromStorageList = new ArrayList<>();
                for (int i = 0; i < indices.size(); i++) {
                    var index = indices.get(i);
                    var header = index.header();
                    var pos = index.position();
                    PersistCache c = caches.get(pos.location()).join();
                    boolean success = true;
                    if (storageFormat.isProtobufFormat()) {
                        try {
                            success = convertPersistCacheToEntryList(c, index, entryList);
                            if (!success) {
                                entryList.setRepeatEntryIndex(i);
                            }
                        } catch (RetryableException e) {
                            // Retry once to handle transient cache staleness in race condition with writeCache.
                            // Only one retry needed since if data changed in PersistentCache,
                            // it's already removed from writeCache.
                            if (!hasRetried) {
                                entryList.clear();
                                return get(indices, entryList, true);
                            } else {
                                return CompletableFuture.failedFuture(new WalStorageException(e.getMessage(), e));
                            }
                        }
                    } else {
                        var id = entryList.getStreamId();
                        if (entryList.shouldSkip(header)) {
                            continue;
                        }
                        if (entryList.isNotFull(header)) {
                            ByteBuf payload = c.get(id, pos.indexId());
                            if (payload == null) {
                                hasNullValue = true;
                                CompletableFuture<ByteBuf> entryFromStorage = fileStorage
                                        .getAsync(pos.location()).thenApply(byteBuf -> {
                                            PersistCache readFromStorage =
                                                    PersistCacheFactory.deserialize(allocator, byteBuf,
                                                            storageFormat.getIndexSerializeFormatVersion());
                                            ByteBuf result = readFromStorage.get(id, pos.indexId());
                                            readFromStorage.close();
                                            return result;
                                        });
                                getFromStorageList.add(entryFromStorage);
                                entryList.add(Entry.of(header, null));
                            } else {
                                entryList.add(Entry.of(header, payload));
                            }
                        } else {
                            success = false;
                        }
                    }
                    if (!success) {
                        break;
                    }
                }

                if (!hasNullValue) {
                    metrics.getGetEntriesDuration().recordSuccess(System.nanoTime() - start);
                    return CompletableFuture.completedFuture(null);
                } else {
                    CompletableFuture<Void> storagePromise = CompletableFuture.allOf(
                        getFromStorageList.toArray(new CompletableFuture[0]));
                    return storagePromise.whenComplete((ignoredStorageResult, readFailure) -> {
                        if (readFailure != null) {
                            releaseCompletedBuffers(getFromStorageList, 0, readFailure);
                            clearEntryListAfterFailure(entryList, readFailure);
                        }
                    }).thenApply(v -> installStorageReadResults(
                        entryList, getFromStorageList, start));
                }
            });
        });
    }

    private Void installStorageReadResults(
            EntryList entryList, List<CompletableFuture<ByteBuf>> storageReads, long startNanos) {
        int nextStorageRead = 0;
        try {
            for (int i = 0; i < entryList.size(); i++) {
                Entry entry = entryList.get(i);
                if (entry.payload() == null) {
                    ByteBuf payload = storageReads.get(nextStorageRead).join();
                    entryList.set(i, new Entry(entry.header(), payload));
                    nextStorageRead++;
                }
            }
            metrics.getGetEntriesDuration().recordSuccess(System.nanoTime() - startNanos);
            return null;
        } catch (RuntimeException | Error mappingFailure) {
            releaseCompletedBuffers(storageReads, nextStorageRead, mappingFailure);
            clearEntryListAfterFailure(entryList, mappingFailure);
            throw mappingFailure;
        }
    }

    @VisibleForTesting
    static void releaseCompletedBuffers(
            List<CompletableFuture<ByteBuf>> reads, int fromIndex, Throwable failure) {
        for (int i = fromIndex; i < reads.size(); i++) {
            CompletableFuture<ByteBuf> read = reads.get(i);
            if (!read.isDone() || read.isCompletedExceptionally() || read.isCancelled()) {
                continue;
            }
            try {
                ByteBuf payload = read.join();
                if (payload != null) {
                    payload.release();
                }
            } catch (RuntimeException | Error cleanupFailure) {
                failure.addSuppressed(cleanupFailure);
            }
        }
    }

    @VisibleForTesting
    static void clearEntryListAfterFailure(EntryList entryList, Throwable failure) {
        try {
            entryList.clear();
        } catch (RuntimeException | Error cleanupFailure) {
            failure.addSuppressed(cleanupFailure);
        }
    }

    protected boolean convertPersistCacheToEntryList(PersistCache cache, EntryIndex index,
             EntryList entryList) throws RetryableException {
        return cache.copy(index, entryList);
    }

    @Override
    public void preFetch(long id, List<Position> positions) {
        if (!initialized || shutdown) {
            return;
        }

        Set<FileInfo> locations = new HashSet<>();
        for (Position position : positions) {
            PersistCache write = writeCache.get(position.file().location());
            if (write == null) {
                // Only prefetch if not in write cache
                locations.add(position.file());
            }
        }

        readCache.tryEvict();
        readCache.load(id, locations);
    }

    @Override
    public CompletableFuture<Void> delete(long id, List<Position> positions) {
        return checkState().thenRun(() -> {
            for (Position position : positions) {
                try {
                    fileStorage.delete(position.location());
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            }
        });
    }

    private CompletableFuture<Void> checkState() {
        if (!initialized) {
            return CompletableFuture.failedFuture(WalStorageException.NOT_INITIALIZED_EXCEPTION);
        }
        if (shutdown) {
            return CompletableFuture.failedFuture(WalStorageException.SHUTDOWN_EXCEPTION);
        }
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public FileStorage getFileStorage() {
        return fileStorage;
    }

    long getPendingReadRequests() {
        return readCache.getPendingRead().get();
    }

    @Override
    public CompletableFuture<Void> close() {
        shutdown = true;
        try {
            readCache.close();
        } catch (Exception e) {
            log.error("Failed to close read cache", e);
        }
        try {
            writeCache.close();
        } catch (Exception e) {
            log.error("Failed to close write cache", e);
        }
        try {
            requestProcessThread.interrupt();
        } catch (Exception e) {
            log.error("Failed to interrupt thread", e);
        }
        try {
            resultProcessThread.interrupt();
        } catch (Exception e) {
            log.error("Failed to interrupt callback thread", e);
        }
        return CompletableFuture.completedFuture(null);
    }
}
