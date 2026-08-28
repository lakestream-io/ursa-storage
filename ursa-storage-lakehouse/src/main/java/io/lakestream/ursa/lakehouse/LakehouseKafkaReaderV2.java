/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.lakehouse;

import io.lakestream.api.EntryIndex;
import io.lakestream.ursa.compaction.common.ManagedTableFileIndex;
import io.lakestream.ursa.lakehouse.compact.ObjectPool;
import io.lakestream.ursa.lakehouse.utils.TopicNames;
import io.lakestream.ursa.lakehouse.v2.LakehouseFactory;
import io.lakestream.ursa.lakehouse.v2.LakehouseReader;
import io.lakestream.ursa.lakestream.reader.CompactedObjectReader;
import io.lakestream.ursa.materialization.serde.GenericEntry;
import io.lakestream.ursa.storage.Entry;
import io.lakestream.ursa.storage.OwnedResultFutures;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class LakehouseKafkaReaderV2 implements CompactedObjectReader {

    private final String topic;
    private final LakehouseFactory factory;

    public LakehouseKafkaReaderV2(String logName, LakehouseFactory factory) {
        this.topic = TopicNames.canonical(logName);
        this.factory = factory;
    }

    @Override
    public CompletableFuture<ReadResult> readMessagesWithEntryIndexAsync(EntryIndex entryIndex, long startOffset,
                                                                    long baseOffset, long maxNumOfMessages,
                                                                    long maxSize) {
        var extraMetadata = entryIndex.extraData().orElse(new HashMap<>());
        var fileIndexString = extraMetadata.get(ManagedTableFileIndex.NAME);
        if (fileIndexString == null) {
            return CompletableFuture.failedFuture(
                new IOException("The required file index is not exists in the index extra metadata, this is "
                                + "not a valid index for v2 lakehouse reader"));
        }

        var managedTableFileIndex = ManagedTableFileIndex.deserializeFromString(fileIndexString);
        try {
            var filePath = managedTableFileIndex.get(startOffset);
            return readMessagesAsync(filePath, startOffset, baseOffset, maxNumOfMessages, maxSize);
        } catch (IllegalArgumentException illegalArgumentException) {
            return CompletableFuture.failedFuture(illegalArgumentException);
        }
    }

    @Override
    public CompletableFuture<ReadResult> readMessagesAsync(String path, long startOffset, long baseOffset,
                                                      long maxNumOfMessages, long maxSize) {
        final int maxMessages;
        try {
            maxMessages = Math.toIntExact(maxNumOfMessages);
        } catch (RuntimeException runtimeException) {
            return CompletableFuture.failedFuture(runtimeException);
        }

        final ObjectPool.PooledObject<LakehouseReader> pooledReader;
        try {
            pooledReader = factory.getPooledReader(topic);
        } catch (RuntimeException | Error acquisitionFailure) {
            return CompletableFuture.failedFuture(acquisitionFailure);
        }

        try {
            var reader = pooledReader.getInstance();
            CompletableFuture<ReadResult> read = reader.readAsync(
                    path, startOffset, baseOffset, maxMessages, maxSize)
                .thenApply(genericEntries -> {
                    List<Entry> entries = unwrapEntries(genericEntries);
                    var entryList = Entry.toLogEntries(entries);
                    return new ReadResult(true, entryList);
                }).whenComplete((result, error) ->
                    releasePooledReader(pooledReader, result, error));
            return OwnedResultFutures.transfer(read, result -> {
                if (result != null) {
                    OwnedResultFutures.closeLogEntries(result.entries());
                }
            });
        } catch (RuntimeException | Error readFailure) {
            releasePooledReaderAfterFailure(pooledReader, readFailure);
            return CompletableFuture.failedFuture(readFailure);
        }
    }

    private void releasePooledReader(ObjectPool.PooledObject<LakehouseReader> pooledReader,
                                     ReadResult result, Throwable error) {
        try {
            factory.releasePooledReader(topic, pooledReader);
        } catch (RuntimeException | Error releaseFailure) {
            if (result != null) {
                try {
                    OwnedResultFutures.closeLogEntries(result.entries());
                } catch (RuntimeException | Error cleanupFailure) {
                    releaseFailure.addSuppressed(cleanupFailure);
                }
            }
            if (error != null) {
                error.addSuppressed(releaseFailure);
            } else {
                throw releaseFailure;
            }
        }
    }

    private void releasePooledReaderAfterFailure(
            ObjectPool.PooledObject<LakehouseReader> pooledReader, Throwable readFailure) {
        try {
            factory.releasePooledReader(topic, pooledReader);
        } catch (RuntimeException | Error releaseFailure) {
            readFailure.addSuppressed(releaseFailure);
        }
    }

    private static List<Entry> unwrapEntries(List<GenericEntry> genericEntries) {
        if (genericEntries == null) {
            throw new IllegalArgumentException("Reader result must not be null");
        }
        try {
            List<Entry> entries = new ArrayList<>(genericEntries.size());
            for (GenericEntry genericEntry : genericEntries) {
                if (genericEntry == null || genericEntry.entry() == null) {
                    throw new IllegalArgumentException("Reader result must not contain null entries");
                }
                entries.add(genericEntry.entry());
            }
            return entries;
        } catch (RuntimeException | Error mappingFailure) {
            for (GenericEntry genericEntry : genericEntries) {
                if (genericEntry == null || genericEntry.entry() == null
                        || genericEntry.entry().payload() == null) {
                    continue;
                }
                try {
                    genericEntry.entry().payload().release();
                } catch (RuntimeException | Error cleanupFailure) {
                    mappingFailure.addSuppressed(cleanupFailure);
                }
            }
            throw mappingFailure;
        }
    }

    @Override
    public boolean hasSpaceInCache() {
        return false;
    }

    @Override
    public CompletableFuture<Entry> preFetchMessagesAsync(String path, long startOffset, long baseOffset,
                                                          long maxNumOfMessages, long maxSize, long estimatedSize) {
        throw new UnsupportedOperationException("prefetch in the lakehouse v2 is not supported");
    }

    @Override
    public void close() {
    }
}
