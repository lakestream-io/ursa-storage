/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.storage.impl;

import io.lakestream.api.EntryIndex;
import io.lakestream.api.FileInfo;
import io.lakestream.api.LogStateManager;
import io.lakestream.api.Position;
import io.lakestream.ursa.storage.AddResult;
import io.lakestream.ursa.storage.Entry;
import io.lakestream.ursa.storage.EntryList;
import io.lakestream.ursa.storage.FileStorage;
import io.lakestream.ursa.storage.PersistCallback;
import io.lakestream.ursa.utils.lock.SingleThreadVerifier;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.Unpooled;
import io.oxia.client.api.AsyncOxiaClient;
import it.unimi.dsi.fastutil.ints.IntIntPair;
import it.unimi.dsi.fastutil.longs.LongLongPair;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import javax.annotation.Nullable;
import lombok.Getter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EntryCacheV0 implements PersistCache {

    private final ReadWriteLock lock = new ReentrantReadWriteLock();
    private final Map<LongLongPair, IntIntPair> index = new HashMap<>();

    private final ByteBuf cacheBuffer;
    @Getter
    private final boolean readonly;
    private final int maxCacheSize;
    private final AtomicLong cacheSize = new AtomicLong(0);

    private int sizeInBytes = 0; // the sizeInBytes of bytes that have been written so far
    private int entryCount = 0; // the count of entry buffers that have been written so far
    private final ByteBufAllocator allocator;
    private final List<PersistCallback> pendingCallbacks = new ArrayList<>();
    @Getter
    private volatile long flushStartTime;

    // readonly stats
    private LongAdder readCount;
    private volatile long createdTimestamp;
    @Getter
    private volatile long lastReadTimestamp;
    // Currently we rely on the fact that put() and persist() are called in the same thread, this option is added to
    // verify it in tests.
    private final SingleThreadVerifier singleThreadVerifier = new SingleThreadVerifier();

    public EntryCacheV0(ByteBufAllocator allocator, int maxCacheSize) {
        this.readonly = false;
        this.allocator = allocator;
        this.maxCacheSize = maxCacheSize;
        this.cacheBuffer = Unpooled.directBuffer(maxCacheSize, maxCacheSize);
        this.createdTimestamp = System.currentTimeMillis();
        this.lastReadTimestamp = createdTimestamp;
    }

    @Override
    public void close() {
        lock.writeLock().lock();
        try {
            unsafeClose();
        } finally {
            lock.writeLock().unlock();
        }
    }

    private void unsafeClose() {
        unsafeClear();
        cacheBuffer.release();
    }

    @Override
    public long put(PendingAdd pendingAdd) {
        if (readonly) {
            return -1;
        }
        singleThreadVerifier.run("put");
        lock.writeLock().lock();
        try {
            return unsafePut(pendingAdd);
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public boolean copy(EntryIndex compactedHeader, EntryList entryList) {
        throw new UnsupportedOperationException();
    }

    private long unsafePut(PendingAdd pendingAdd) {
        long streamId = pendingAdd.id;
        ByteBuf entry = pendingAdd.buf;
        int size = entry.readableBytes();
        if (sizeInBytes + size > maxCacheSize) {
            // Cache is full
            return -1;
        }

        cacheBuffer.setBytes(sizeInBytes, entry, entry.readerIndex(), size);
        index.put(LongLongPair.of(streamId, entryCount), IntIntPair.of(sizeInBytes, size));
        cacheSize.addAndGet(size);
        pendingCallbacks.add(pendingAdd);

        final var entryId = entryCount;
        sizeInBytes += size;
        entryCount++;
        return entryId;
    }

    @Nullable
    public ByteBuf get(long streamId, long entryId) {
        lock.readLock().lock();
        try {
            return unsafeGet(streamId, entryId);
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public Entry get(long streamId, long offset, EntryIndex compactedHeader) {
        throw new UnsupportedOperationException();
    }

    @Nullable
    private ByteBuf unsafeGet(long streamId, long entryId) {
        final var result = index.get(LongLongPair.of(streamId, entryId));
        if (result == null) {
            return null;
        }

        int offset = result.leftInt();
        int size = result.rightInt();
        ByteBuf entry = allocator.buffer(size, size);
        entry.writeBytes(cacheBuffer, offset, size);
        if (readonly) {
            readCount.increment();
            lastReadTimestamp = System.currentTimeMillis();
        }

        return entry;
    }

    public void clear() {
        lock.writeLock().lock();
        try {
            unsafeClear();
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public long sizeInBytes() {
        lock.readLock().lock();
        try {
            return sizeInBytes;
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public int entryCount() {
        lock.readLock().lock();
        try {
            return entryCount;
        } finally {
            lock.readLock().unlock();
        }
    }

    private void unsafeClear() {
        cacheSize.set(0L);
        sizeInBytes = 0;
        entryCount = 0;
        index.clear();
        pendingCallbacks.clear();
        if (readonly && readCount != null) {
            readCount.reset();
        }
        createdTimestamp = System.currentTimeMillis();
        lastReadTimestamp = createdTimestamp;
    }

    public boolean isEmpty() {
        return cacheSize.get() == 0L;
    }

    private static final Logger log = LoggerFactory.getLogger(PersistCache.class);

    // serialize the whole writeCaceh object to a ByteBuf
    static EntryCacheV0 deserialize(ByteBufAllocator allocator, ByteBuf data) {
        return new EntryCacheV0(allocator, data);
    }

    private EntryCacheV0(ByteBufAllocator allocator, ByteBuf data) {
        this.readonly = true;
        this.allocator = allocator;

        int dataReadIdx = 0;
        long indexLength = data.readLong();
        dataReadIdx += 8;
        rebuildIndex(data.slice(8, (int) indexLength));
        dataReadIdx += indexLength;
        data.readerIndex(dataReadIdx);
        this.maxCacheSize = data.readableBytes();
        this.cacheBuffer = data.retainedSlice(dataReadIdx, data.readableBytes());
        data.release();
        this.readCount = new LongAdder();
        this.createdTimestamp = System.currentTimeMillis();
        this.lastReadTimestamp = createdTimestamp;
    }

    private void rebuildIndex(ByteBuf data) {
        while (data.readableBytes() >= 8/* long */ * 4) {
            long streamId = data.readLong();
            long entryId = data.readLong();
            int offset = (int) data.readLong();
            int length = (int) data.readLong();
            index.put(LongLongPair.of(streamId, entryId), IntIntPair.of(offset, length));
            entryCount++;
        }
    }

    private void markFlushStartTime() {
        flushStartTime = System.nanoTime();
    }

    @Override
    public CompletableFuture<String> persist(FileStorage storage, String location, StorageFormat format) {
        singleThreadVerifier.run("persist");
        ByteBuf content = serialize(location, format);
        markFlushStartTime();
        CompletableFuture<String> future = storage.putAsync(content, location).thenApply(__ -> location);
        future.whenComplete((v, e) -> {
            content.release();
        });
        return future;
    }

    @Override
    public CompletableFuture<Map<Long, StreamIndexResult>> index(AsyncOxiaClient oxiaClient,
                                                                            LogStateManager streamStateManager) {
        return CompletableFuture.completedFuture(Collections.emptyMap());
    }

    public void flushFailed(Throwable e) {
        log.error("Failed to persist write cache", e);
        final List<PersistCallback> callbacks;
        lock.readLock().lock();
        try {
            callbacks = new ArrayList<>(pendingCallbacks);
        } finally {
            lock.readLock().unlock();
        }
        callbacks.forEach(callback -> {
            try {
                callback.onFailure(e);
            } catch (Exception e1) {
                log.warn("Notify callback onFailure failed.", e1);
            }
        });
    }

    public void flushSucceed(String location, Map<Long, StreamIndexResult> indexResults) {
        final List<PersistCallback> callbacks;
        lock.readLock().lock();
        try {
            callbacks = new ArrayList<>(pendingCallbacks);
        } finally {
            lock.readLock().unlock();
        }
        callbacks.forEach(callback -> {
            try {
                callback.onSuccess(new AddResult(
                        null,
                        new Position(new FileInfo(location, sizeInBytes), ((PendingAdd) callback).indexId,
                                Position.FileType.RAW), true));
            } catch (Exception e) {
                log.warn("Notify callback onSuccess failed.", e);
            }
        });
    }

    public ByteBuf serialize(String location, StorageFormat format) {
        lock.readLock().lock();
        try {
            return unsafeSerialize();
        } finally {
            lock.readLock().unlock();
        }
    }

    private ByteBuf unsafeSerialize() {
        if (log.isTraceEnabled()) {
            log.trace("Serializing write cache, cache count: {}", entryCount);
        }
        CompositeByteBuf composite = allocator.compositeBuffer(3);

        // Append the index
        ByteBuf index = persistIndex();
        ByteBuf indexLength = allocator.buffer(8);
        indexLength.writeLong(index.readableBytes());
        composite.addComponent(true, indexLength.slice(0, indexLength.readableBytes()));
        composite.addComponent(true, index.slice(0, index.readableBytes()));

        // Add the actual cache data
        composite.addComponent(true, cacheBuffer.retainedSlice(0, sizeInBytes));

        return composite;
    }

    private ByteBuf persistIndex() {
        int size = index.size() * 8/* long */ * 4;
        ByteBuf indexBuffer = allocator.buffer(size);
        index.forEach((pair1, pair2) -> {
            final var streamId = pair1.leftLong();
            final var entryId = pair1.rightLong();
            final var offset = pair2.leftInt();
            final var length = pair2.rightInt();
            // NOTE: write long for each field here to keep backward compatibility
            indexBuffer.writeLong(streamId);
            indexBuffer.writeLong(entryId);
            indexBuffer.writeLong(offset);
            indexBuffer.writeLong(length);
        });
        return indexBuffer;
    }

    public long getReadCount() {
        if (readCount == null) {
            return 0;
        }
        return readCount.sum();
    }

    public long getReadDurationInMillis() {
        return lastReadTimestamp - createdTimestamp;
    }
}
