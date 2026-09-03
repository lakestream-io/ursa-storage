/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.storage.impl;

import static io.lakestream.api.Position.FileType.RAW;
import static io.lakestream.ursa.storage.impl.StorageConfig.DEFAULT_INDEX_SERIALIZE_FORMAT_VERSION;
import static io.lakestream.ursa.storage.proto.IndexType.COMPACT;
import static io.lakestream.ursa.storage.proto.IndexType.NORMAL;
import static io.lakestream.ursa.utils.cache.SerDesUtils.readVarInt;
import static io.lakestream.ursa.utils.cache.SerDesUtils.writeVarInt;

import com.google.common.annotations.VisibleForTesting;
import io.lakestream.api.EntryHeader;
import io.lakestream.api.EntryIndex;
import io.lakestream.api.FileInfo;
import io.lakestream.api.LogState;
import io.lakestream.api.LogStateManager;
import io.lakestream.api.Position;
import io.lakestream.ursa.storage.AddResult;
import io.lakestream.ursa.storage.Entry;
import io.lakestream.ursa.storage.EntryList;
import io.lakestream.ursa.storage.FileStorage;
import io.lakestream.ursa.storage.Key;
import io.lakestream.ursa.storage.PersistCallback;
import io.lakestream.ursa.storage.impl.exception.EntryCacheClosedException;
import io.lakestream.ursa.storage.impl.exception.RetryableException;
import io.lakestream.ursa.utils.lock.SingleThreadVerifier;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.CompositeByteBuf;
import io.oxia.client.api.AsyncOxiaClient;
import io.oxia.client.api.options.PutOption;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import org.apache.commons.lang3.mutable.MutableInt;
import org.apache.commons.lang3.mutable.MutableLong;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EntryCache implements PersistCache {

    @AllArgsConstructor
    @NoArgsConstructor
    @Getter
    static class Stat {
        protected long sizeInBytes;
        protected int entryCount;
        protected int messageCount;
        protected long initialOffset = -1;
        protected long cumulativeSize = -1;

        public void add(int sizeInBytes, int messageCount) {
            this.entryCount++;
            this.sizeInBytes += sizeInBytes;
            this.messageCount += messageCount;
        }

        public void set(long sizeInBytes, int entryCount, int messageCount) {
            this.sizeInBytes = sizeInBytes;
            this.entryCount = entryCount;
            this.messageCount = messageCount;
        }

        public void clear() {
            sizeInBytes = 0;
            entryCount = 0;
            messageCount = 0;
            initialOffset = -1;
            cumulativeSize = -1;
        }
    }

    private record Index(int offset, int sizeInBytes, int messageCount) {
    }


    @AllArgsConstructor
    private static class Value {
        Stat stat;
        List<Index> indices;
        List<PersistCallback> callbacks;

        public static Value empty() {
            return new Value(new Stat(), new ArrayList<>(), new ArrayList<>());
        }
    }

    private volatile boolean closed = false;
    // Segment lease state. Readers take a lease through tryRetain()/release() so the cache that owns
    // this segment cannot release cacheBuffer while a read is still in flight. Deliberately lock-free
    // (one AtomicInteger plus one volatile): a lease may be taken while the owning cache holds its own
    // lock, and taking this segment's lock there would add a new lock-order edge.
    private final AtomicInteger leases = new AtomicInteger(0);
    // Set by close(): no further lease is granted and the last releaser performs the teardown.
    private volatile boolean retired = false;
    private final ReadWriteLock lock = new ReentrantReadWriteLock();
    @Getter
    private final Map<Long, Value> index;
    private final ByteBuf cacheBuffer;
    @Getter
    private final boolean readonly;
    private final long maxCacheSize;
    private final Stat stat = new Stat();
    private final ByteBufAllocator allocator;
    private final Map<Long, Map<Long, Pair<EntryHeader, Integer>>> entryHeaders = new ConcurrentHashMap<>();

    private final List<IndexRequest> indexRequests = new ArrayList<>();
    private long flushStartTime;

    // readonly stats
    private LongAdder readCount;
    private long createdTimestamp;
    private volatile long lastReadTimestamp;
    // Maps streamId to the starting offset for entries stored in this cache.
    // Used to validate read operations and ensure offset consistency when copying entries.
    // Set during flush operations and checked during copy() to prevent stale cache reads.
    private final Map<Long, Long> startOffsets = new ConcurrentHashMap<>();

    // Currently we rely on the fact that put() and persist() are called in the same thread, this option is added to
    // verify it in tests.
    private final SingleThreadVerifier singleThreadVerifier = new SingleThreadVerifier();

    public EntryCache(ByteBufAllocator allocator, int maxCacheSize) {
        this.readonly = false;
        this.allocator = allocator;
        this.maxCacheSize = maxCacheSize;
        this.cacheBuffer = allocator.buffer(maxCacheSize, maxCacheSize);
        this.createdTimestamp = System.currentTimeMillis();
        this.lastReadTimestamp = createdTimestamp;
        this.index = new HashMap<>();
    }

    private void validateState() {
        if (closed) {
            throw new EntryCacheClosedException("already closed");
        }
    }


    @Override
    public boolean tryRetain() {
        if (retired) {
            return false;
        }
        leases.incrementAndGet();
        if (retired) {
            // Lost the race with close(): back the lease out and report a miss. Both the retired store
            // and the lease increment are sequentially consistent, so close() either observes this
            // lease and defers, or this check observes the retirement.
            release();
            return false;
        }
        return true;
    }

    @Override
    public void release() {
        int remaining = leases.decrementAndGet();
        if (remaining < 0) {
            leases.incrementAndGet();
            throw new IllegalStateException("release() without a matching tryRetain()");
        }
        if (remaining == 0 && retired) {
            doClose();
        }
    }

    @VisibleForTesting
    int leaseCount() {
        return leases.get();
    }

    @Override
    public void close() {
        // Retire first so no new lease can be granted, then let whoever drops the last lease do the
        // teardown. With nothing leased that is this caller, so close() stays synchronous as before.
        retired = true;
        if (leases.get() == 0) {
            doClose();
        }
    }

    private void doClose() {
        lock.writeLock().lock();
        try {
            if (closed) {
                return;
            }
            closed = true;
            doClear();
            cacheBuffer.release();
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public long put(PendingAdd pendingAdd) {
        if (readonly) {
            return -1;
        }
        singleThreadVerifier.run("put");
        long streamId = pendingAdd.id;
        int messageCount = pendingAdd.numberOfMessages;
        long initialOffset = pendingAdd.initialOffset();
        long cumulativeSize = pendingAdd.cumulativeSize();
        ByteBuf entry = pendingAdd.buf;
        lock.writeLock().lock();
        try {
            validateState();
            int size = entry.readableBytes();
            int offset = (int) stat.sizeInBytes;
            if ((offset + size) > maxCacheSize) {
                // Cache is full
                return -1;
            }

            var value = this.index.computeIfAbsent(streamId, k -> Value.empty());
            if (initialOffset != -1) {
                if (value.stat.initialOffset != -1) {
                    if (initialOffset != value.stat.initialOffset + value.stat.messageCount) {
                        pendingAdd.onFailure(new IllegalArgumentException(
                                String.format("Invalid initial offset %d, expected %d", initialOffset,
                                        value.stat.initialOffset + value.stat.messageCount)));
                        return -1;
                    }
                } else {
                    value.stat.initialOffset = initialOffset;
                }
                value.stat.cumulativeSize = cumulativeSize;
            }

            cacheBuffer.setBytes(offset, entry, entry.readerIndex(), size);
            stat.sizeInBytes += size;
            stat.entryCount++;
            stat.messageCount += messageCount;

            value.indices.add(new Index(offset, size, messageCount));
            var entryId = value.stat.entryCount;
            value.stat.add(size, messageCount);
            if (value.callbacks == null) {
                value.callbacks = new ArrayList<>();
            }
            value.callbacks.add(pendingAdd);
            return entryId;
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public boolean copy(EntryIndex compactedIndex, EntryList entryList) throws RetryableException {
        lock.readLock().lock();
        try {
            validateState();
            var streamId = entryList.getStreamId();
            if (!readonly) {
                if (!startOffsets.containsKey(streamId)) {
                    log.error("[{}] Entry cache is not available for read due to flushed first offset is not set",
                            streamId);
                    throw new RetryableException(
                            "Entry cache is not available for read due to flushed first offset is not set");
                } else if (compactedIndex.getFirstEntryHeader().offset() != startOffsets.get(streamId)) {
                    log.error("[{}] Read offset mismatch, first read offset: {}, start offset: {}", streamId,
                            compactedIndex.getFirstEntryHeader().offset(), startOffsets.get(streamId));
                    throw new RetryableException("Read offset mismatch");
                }
            }

            Map<Long, Pair<EntryHeader, Integer>> entryHeaderMap = null;
            boolean newEntryHeaderMap = false;

            final MutableInt entryId;
            final MutableLong offset;
            final MutableLong cumulativeSize;
            var value = this.index.get(streamId);

            if (value == null) {
                throw new RetryableException(
                        "index is not found by streamId:" + streamId + ", compactedIndex:" + compactedIndex.toString());
            }
            var stat = value.stat;
            final var streamEntryCount = stat.entryCount;
            var compactedHeader = compactedIndex.header();
            var startOffset = entryList.getStartOffset();
            entryHeaderMap = entryHeaders.get(streamId);
            var startHeader = entryHeaderMap != null ? entryHeaderMap.get(startOffset) : null;
            var entryIndices = value.indices;
            if (startHeader != null) {
                entryId = new MutableInt(startHeader.getRight());
                offset = new MutableLong(startHeader.getLeft().offset());
                cumulativeSize = new MutableLong(startHeader.getLeft().cumulativeSize()
                        - startHeader.getLeft().entrySize());
            } else {
                entryId = new MutableInt(0);
                offset = new MutableLong(compactedHeader.offset());
                cumulativeSize = new MutableLong(compactedHeader.cumulativeSize()
                        - compactedHeader.entrySize());
                if (entryHeaderMap == null) {
                    newEntryHeaderMap = true;
                    entryHeaderMap = new HashMap<>();
                }
            }


            boolean success = true;
            for (; entryId.longValue() < streamEntryCount; entryId.add(1)) {
                var index = entryIndices.get(entryId.intValue());
                int entrySize = index.sizeInBytes;
                int numberOfMessages = index.messageCount;
                int payloadOffset = index.offset;

                cumulativeSize.add(index.sizeInBytes);
                var entryHeaderPair = entryHeaderMap
                        .computeIfAbsent(offset.longValue(), kk -> Pair.of(
                                new EntryHeader(offset.longValue(), numberOfMessages,
                                        compactedHeader.writtenTimestamp(),
                                        entrySize, cumulativeSize.longValue()),
                                entryId.intValue()));

                offset.add(numberOfMessages);

                var eh = entryHeaderPair.getLeft();

                if (!entryList.shouldSkip(eh)) {
                    if (entryList.isNotFull(eh)) {
                        var payload = doGet(payloadOffset, entrySize);
                        if (payload == null) {
                            throw new IllegalStateException(
                                    "payload is not found at " + payloadOffset + " " + index + " " + streamId);
                        }
                        entryList.add(Entry.of(eh, payload));
                    } else {
                        success = false;
                        break;
                    }
                }
            }

            if (success && startHeader == null && newEntryHeaderMap) {
                var finalEntryHeaderMap = entryHeaderMap;
                entryHeaders.computeIfAbsent(streamId, k -> finalEntryHeaderMap);
            }
            return success;
        } catch (Exception e) {
            entryList.clear();
            throw e;
        } finally {
            lock.readLock().unlock();
        }
    }

    private ByteBuf doGetByEntryId(long streamId, long entryId) {
        var result = this.index.get(streamId);
        if (result == null) {
            return null;
        }
        var index = result.indices.get((int) entryId);
        if (index == null) {
            return null;
        }
        return doGet(index.offset, index.sizeInBytes);
    }

    @Override
    public ByteBuf get(long streamId, long entryId) {
        lock.readLock().lock();
        try {
            validateState();
            return doGetByEntryId(streamId, entryId);
        } finally {
            lock.readLock().unlock();
        }
    }


    private Entry doGetByEntryHeader(long streamId, long offset) {
        var entryHeaderMap = entryHeaders.get(streamId);
        var entryHeader = entryHeaderMap != null ? entryHeaderMap.get(offset) : null;
        if (entryHeader != null) {
            var index = this.index.get(streamId).indices.get(entryHeader.getRight());
            if (index == null) {
                throw new IllegalStateException(
                        "The index is not found for " + streamId + ":" + offset + ":" + entryHeader);
            }
            int localOffset = index.offset;
            int size = index.sizeInBytes;
            var payload = doGet(localOffset, size);
            if (payload == null) {
                throw new IllegalStateException(
                        "The payload is not found from " + streamId + " " + offset + " " + entryHeader);
            }
            return new Entry(entryHeader.getLeft(), payload);
        }
        return null;
    }

    private Entry doGetByCopy(long streamId, long offset, EntryIndex compactedHeader) throws RetryableException {
        var entryList = new EntryList(streamId, offset, 1, Long.MAX_VALUE);
        copy(compactedHeader, entryList);
        if (entryList.isEmpty()) {
            return null;
        }

        var entry = entryList.get(0);
        var header = entry.header();
        //3 [4,5] 6
        if (offset < header.offset()
                || (header.offset() + header.numberOfMessages() <= offset)) {
            throw new IllegalStateException(
                    "Invalid result entry offset from " + streamId + ":" + offset + " found: "
                            + entry.header() + " compactedHeader:" + compactedHeader);
        }
        return entry;
    }

    @Override
    public Entry get(long streamId, long offset, EntryIndex compactedHeader) throws RetryableException {
        lock.readLock().lock();
        try {
            validateState();
            Entry result = doGetByEntryHeader(streamId, offset);

            if (result == null) {
                result = doGetByCopy(streamId, offset, compactedHeader);
            }
            return result;
        } finally {
            lock.readLock().unlock();
        }
    }

    private ByteBuf doGet(int offset, int size) {
        if (offset + size > maxCacheSize) {
            return null;
        }

        ByteBuf entry = allocator.buffer(size, size);
        entry.writeBytes(cacheBuffer, offset, size);
        if (readonly) {
            readCount.increment();
            lastReadTimestamp = System.currentTimeMillis();
        }
        return entry;
    }

    @Override
    public void clear() {
        lock.writeLock().lock();
        try {
            validateState();
            doClear();
        } finally {
            lock.writeLock().unlock();
        }
    }

    private void doClear() {
        stat.clear();
        index.clear();

        if (entryHeaders != null) {
            entryHeaders.clear();
        }

        if (indexRequests != null) {
            indexRequests.clear();
        }

        if (readonly && readCount != null) {
            readCount.reset();
        }
        createdTimestamp = System.currentTimeMillis();
        lastReadTimestamp = createdTimestamp;
        startOffsets.clear();
    }

    @Override
    public long sizeInBytes() {
        lock.readLock().lock();
        try {
            validateState();
            return stat.sizeInBytes;
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public int entryCount() {
        lock.readLock().lock();
        try {
            validateState();
            return stat.entryCount;
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public boolean isEmpty() {
        lock.readLock().lock();
        try {
            validateState();
            return stat.entryCount == 0;
        } finally {
            lock.readLock().unlock();
        }
    }

    private static final Logger log = LoggerFactory.getLogger(EntryCache.class);

    // serialize the whole writeCaceh object to a ByteBuf
    static EntryCache deserialize(ByteBufAllocator allocator, ByteBuf data) {
        return new EntryCache(allocator, data);
    }

    void rebuildIndex(ByteBuf buf) {
        int version = buf.readInt();
        long totalSize = 0;
        int totalCount = 0;
        int totalNumberOfMessages = 0;
        while (buf.readableBytes() > 0) {
            long streamId = buf.readLong();
            int count = readVarInt(buf);
            int size = 0;
            int numberOfMessages = 0;

            List<Index> indexList = new ArrayList<>();
            for (int i = 0; i < count; i++) {
                int offset = readVarInt(buf);
                int sizeInBytes = readVarInt(buf);
                int messageCount = readVarInt(buf);
                Index index = new Index(offset, sizeInBytes, messageCount);
                indexList.add(index);
                size += sizeInBytes;
                numberOfMessages += messageCount;
            }
            totalSize += size;
            totalCount += count;
            totalNumberOfMessages += numberOfMessages;
            index.put(streamId, new Value(new Stat(size, count, numberOfMessages, -1, -1), indexList, null));
        }
        stat.set(totalSize, totalCount, totalNumberOfMessages);
    }

    private EntryCache(ByteBufAllocator allocator, ByteBuf data) {
        this.readonly = true;
        this.allocator = allocator;

        int dataReadIdx = 0;
        int indexLength = data.readInt();
        dataReadIdx += 4;
        index = new HashMap<>();
        rebuildIndex(data.slice(4, indexLength));
        dataReadIdx += indexLength;
        data.readerIndex(dataReadIdx);
        this.maxCacheSize = data.readableBytes();
        this.cacheBuffer = data.retainedSlice(dataReadIdx, data.readableBytes());
        data.release();
        this.readCount = new LongAdder();
        this.createdTimestamp = System.currentTimeMillis();
        this.lastReadTimestamp = createdTimestamp;
    }

    private void markFlushStartTime() {
        flushStartTime = System.nanoTime();
    }

    @Override
    public CompletableFuture<String> persist(FileStorage storage, String location, StorageFormat format) {
        singleThreadVerifier.run("persist");
        return CompletableFuture.supplyAsync(() -> {
            lock.readLock().lock();
            try {
                validateState();
                ByteBuf content = serialize(location, format);
                markFlushStartTime();
                return content;
            } finally {
                lock.readLock().unlock();
            }
        }).thenCompose(content ->
                // let the storage put finish
                storage.putAsync(content, location)
                        .thenApply(__ -> location)
                        .whenComplete((__, e) -> {
                            content.release();
                        })
        );
    }

    @Override
    public void flushFailed(Throwable e) {
        lock.readLock().lock();
        try {
            log.error("Failed to persist write cache", e);
            index.values().forEach(value -> fail(value, e));
        } finally {
            lock.readLock().unlock();
        }
    }

    private static void fail(Value value, Throwable throwable) {
        if (value.callbacks != null) {
            for (var cb : value.callbacks) {
                try {
                    cb.onFailure(throwable);
                } catch (Exception e1) {
                    log.warn("Notify callback onFailure failed.", e1);
                }
            }
        }
    }

    @Override
    public void flushSucceed(String location, Map<Long, StreamIndexResult> indexResults) {
        lock.readLock().lock();
        try {
            for (var etr : index.entrySet()) {
                long streamId = etr.getKey();
                var value = etr.getValue();
                try {
                    var indexResult = indexResults.get(streamId);
                    if (indexResult == null) {
                        log.warn("Notify callback failed on streamStats:{}, indexResults:{}",
                                index.keySet(),
                                indexResults.keySet());
                        fail(value, new IllegalStateException(
                                "Index result missing for stream " + streamId));
                        continue;
                    }
                    if (indexResult instanceof StreamIndexResult.Fenced) {
                        fail(value, LogStateUtil.toException(LogState.FENCED, streamId).orElseThrow());
                    } else if (indexResult instanceof StreamIndexResult.Failed f) {
                        log.error("Index write failed for stream {}", streamId, f.cause());
                        fail(value, f.cause());
                    } else if (indexResult instanceof StreamIndexResult.Success success) {
                        notifySuccess(streamId, value, success, location);
                    }
                } catch (Exception ex) {
                    log.warn("Failed to process index result for stream {}, location:{}",
                            streamId, location, ex);
                    fail(value, ex);
                }
            }
        } finally {
            lock.readLock().unlock();
        }
    }

    private void notifySuccess(long streamId, Value value, StreamIndexResult.Success success,
                               String location) {
        final var putCompactedOffsetResult = success.putResult();
        io.lakestream.ursa.storage.Key k =
                io.lakestream.ursa.storage.Key.parse(putCompactedOffsetResult.key());
        var streamStat = value.stat;
        var callbacks = value.callbacks;
        var indices = value.indices;

        long offset = k.offset() - streamStat.messageCount;
        startOffsets.put(streamId, offset);
        long cumulativeSize = k.cumulativeSize() - streamStat.sizeInBytes;
        long createdTimeStamp = putCompactedOffsetResult.version().createdTimestamp();

        if (log.isDebugEnabled()) {
            log.debug(
                    "compacted key:{}, offset:{}, cumulativeSize:{}, putCompactedOffsetResult:{}, location:{}, "
                            + "entryCount:{}, messageCount:{}",
                    putCompactedOffsetResult.key(),
                    offset, cumulativeSize, putCompactedOffsetResult.version()
                            .createdTimestamp(), location, streamStat.entryCount,
                    streamStat.messageCount);
        }

        int len = indices.size();
        for (int entryId = 0; entryId < len; entryId++) {
            var index = indices.get(entryId);
            if (index == null) {
                log.error("index is not found for stream:{} entryId:{}, failing remaining callbacks",
                        streamId, entryId);
                var ex = new IllegalStateException("index is not found for :" + streamId);
                failRemaining(callbacks, entryId, len, ex);
                return;
            }
            int numberOfMessages = index.messageCount;
            int size = index.sizeInBytes;
            cumulativeSize += size;
            try {
                callbacks.get(entryId).onSuccess(new AddResult(
                        new EntryHeader(offset, numberOfMessages, createdTimeStamp, size,
                                cumulativeSize), null,
                        entryId == len - 1));
            } catch (Exception e) {
                log.warn("Notify callback onSuccess failed on entry:{}:{} for location:{}",
                        entryId, location, e);
            }
            offset += numberOfMessages;
        }
    }

    private static void failRemaining(List<PersistCallback> callbacks, int fromIndex,
                                       int toIndex, Throwable cause) {
        for (int i = fromIndex; i < toIndex; i++) {
            try {
                callbacks.get(i).onFailure(cause);
            } catch (Exception e) {
                log.warn("Notify callback onFailure failed for entry:{}", i, e);
            }
        }
    }

    @Override
    public ByteBuf serialize(String location, StorageFormat format) {
        // read.locked at the upper level, persist func
        validateState();
        CompositeByteBuf composite = allocator.compositeBuffer(3);
        // Append the index
        ByteBuf index = persistIndex(location, format);
        ByteBuf indexLength = allocator.buffer(4);
        indexLength.writeInt(index.readableBytes());
        composite.addComponent(true, indexLength.slice(0, indexLength.readableBytes()));
        composite.addComponent(true, index.slice(0, index.readableBytes()));

        // Add the actual cache data
        composite.addComponent(true, cacheBuffer.retainedSlice(0,
                (int) stat.sizeInBytes));

        return composite;
    }

    @Override
    public long getReadCount() {
        validateState();
        if (readCount == null) {
            return 0;
        }
        return readCount.sum();
    }

    @Override
    public long getReadDurationInMillis() {
        validateState();
        return lastReadTimestamp - createdTimestamp;
    }

    @Override
    public long getFlushStartTime() {
        return flushStartTime;
    }

    @Override
    public long getLastReadTimestamp() {
        validateState();
        return lastReadTimestamp;
    }

    // Method to serialize a HashMap into a ByteBuf
    private ByteBuf persistIndex(String location,
                                 StorageFormat format) {

        int streamIdCount = index.size();
        var fileType = RAW;
        var indexType = streamIdCount == 1 ? COMPACT : NORMAL;
        var fileSize = stat.sizeInBytes;

        ByteBuf buf = allocator.buffer();
        buf.writeInt(DEFAULT_INDEX_SERIALIZE_FORMAT_VERSION);
        for (var e : index.entrySet()) {
            long streamId = e.getKey();
            var value = e.getValue();
            buf.writeLong(streamId);
            var indices = value.indices;
            int len = indices.size();
            writeVarInt(buf, len);

            var stat = value.stat;
            long numberOfMessages = stat.getMessageCount();
            long size = stat.getSizeInBytes();
            int count = stat.entryCount;
            var offsets = count > 1 ? new int[count] : null;
            int offset = 0;
            for (int i = 0; i < len; i++) {
                var index = indices.get(i);
                writeVarInt(buf, index.offset);
                writeVarInt(buf, index.sizeInBytes);
                writeVarInt(buf, index.messageCount);
                if (offsets != null) {
                    offset += index.messageCount;
                    offsets[i] = offset;
                }
            }
            final EntryCache.IndexRequest request;
            var indexValue = new io.lakestream.ursa.storage.Value(numberOfMessages, size, count, indexType,
                    new Position(new FileInfo(location, fileSize), -1, fileType),
                    Optional.ofNullable(offsets)).toBytes(format.getIndexSerializeFormatVersion());
            if (stat.initialOffset == -1) {
                request = new IndexRequest(
                        streamId,
                        format.getStreamIdKey(streamId),
                        indexValue,
                        Set.of(PutOption.PartitionKey(String.valueOf(streamId)),
                                PutOption.SequenceKeysDeltas(
                                        List.of(numberOfMessages, size))));
            } else {
                request = new IndexRequest(
                        streamId,
                        new Key(streamId, stat.initialOffset + numberOfMessages, stat.cumulativeSize).toString(),
                        indexValue,
                        Set.of(PutOption.PartitionKey(String.valueOf(streamId))));
            }
            indexRequests.add(request);
        }
        return buf;
    }


    record IndexRequest(long streamId, String key, byte[] value, Set<PutOption> options) {
    }

    @Override
    public CompletableFuture<Map<Long, StreamIndexResult>> index(AsyncOxiaClient oxiaClient,
        LogStateManager streamStateManager) {
        lock.readLock().lock();
        try {
            validateState();
            List<CompletableFuture<Void>> futures = new ArrayList<>();
            Map<Long, StreamIndexResult> result = new ConcurrentHashMap<>();
            for (var request : indexRequests) {
                if (streamStateManager.getState(request.streamId) == LogState.FENCED) {
                    result.put(request.streamId, new StreamIndexResult.Fenced());
                } else {
                    futures.add(oxiaClient.put(request.key, request.value, request.options)
                            .handle((putResult, ex) -> {
                                if (ex != null) {
                                    result.put(request.streamId, new StreamIndexResult.Failed(ex));
                                } else {
                                    result.put(request.streamId, new StreamIndexResult.Success(putResult));
                                }
                                return null;
                            }));
                }
            }
            return CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]))
                    .thenApply(__ -> result);
        } finally {
            lock.readLock().unlock();
        }
    }

    @VisibleForTesting
    void setStartOffsets(long streamId, long startOffset) {
        this.startOffsets.put(streamId, startOffset);
    }
}
