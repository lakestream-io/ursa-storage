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
