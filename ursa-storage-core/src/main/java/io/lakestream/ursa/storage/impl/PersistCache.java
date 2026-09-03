/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.storage.impl;

import io.lakestream.api.EntryIndex;
import io.lakestream.api.LogStateManager;
import io.lakestream.ursa.storage.Entry;
import io.lakestream.ursa.storage.EntryList;
import io.lakestream.ursa.storage.FileStorage;
import io.lakestream.ursa.storage.impl.exception.RetryableException;
import io.netty.buffer.ByteBuf;
import io.oxia.client.api.AsyncOxiaClient;
import java.io.Closeable;
import java.io.Serializable;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

public interface PersistCache extends Serializable, Closeable {
    void close();

    /**
     * Acquires a read lease so the owning cache cannot tear this segment down while the lease is held.
     *
     * <p>A {@code false} result means the segment is retired — evicted, recycled or closed — and its
     * backing buffer is about to be released. Callers must treat that as an ordinary cache miss and
     * satisfy the read from storage instead; it is never an error.
     *
     * <p><b>Lease discipline.</b> A lease is acquired and released inside a single synchronous block
     * and must never be held across an asynchronous boundary, so a retired segment stays pinned for
     * at most one {@code copy()}/{@code get()} call rather than for the duration of a remote read.
     * Every successful {@code tryRetain()} needs exactly one matching {@link #release()}, normally in
     * a {@code finally}.
     *
     * @return true when the lease was granted, false when the segment is retired.
     */
    boolean tryRetain();

    /**
     * Releases a lease taken by {@link #tryRetain()}. The last release of a retired segment performs
     * the close that was deferred while readers were still using it.
     *
     * <p>Must not be called while holding the owning cache's lock: the deferred close takes this
     * segment's write lock, and the established lock order is cache lock before segment lock.
     */
    void release();

    long put(PendingAdd pendingAdd);

    boolean copy(EntryIndex compactedIndex, EntryList entryList) throws RetryableException;

    ByteBuf get(long streamId, long entryId);

    Entry get(long streamId, long offset, EntryIndex compactedIndex) throws RetryableException;

    void clear();

    long sizeInBytes();

    int entryCount();

    boolean isEmpty();

    boolean isReadonly();

    CompletableFuture<String> persist(FileStorage storage, String location, StorageFormat format);

    CompletableFuture<Map<Long, StreamIndexResult>> index(AsyncOxiaClient oxiaClient,
                                                         LogStateManager streamStateManager);

    void flushSucceed(String location, Map<Long, StreamIndexResult> indexResults);

    void flushFailed(Throwable e);

    ByteBuf serialize(String location, StorageFormat format);

    long getReadCount();

    long getReadDurationInMillis();

    long getFlushStartTime();

    long getLastReadTimestamp();
}
