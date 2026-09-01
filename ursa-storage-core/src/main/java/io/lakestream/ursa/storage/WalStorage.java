/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.storage;

import io.lakestream.api.EntryIndex;
import io.lakestream.api.Position;
import io.netty.buffer.ByteBuf;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * WalStorage interface defines operations for a Write-Ahead Log (WAL) storage system.
 * This interface provides methods for initializing, writing, reading, and managing data entries
 * in a distributed write-ahead log.
 */
public interface WalStorage {

    /**
     * Initializes the WAL storage system.
     * This method should be called before any other operations to set up necessary resources,
     * connections, or state required for the WAL to function.
     *
     * @throws Exception if initialization fails due to connection issues, resource allocation problems,
     *                   or any other initialization-related errors.
     */
    void initialize() throws Exception;

    /**
     * Writes a data entry to the WAL.
     * This method asynchronously appends the given data to the WAL, associating it with the provided ID.
     *
     * @param id               A unique identifier for the entry, typically representing a stream or partition.
     * @param numberOfMessages number of messages in the buf
     * @param buf              ByteBuf containing the data to be written. The caller retains
     *                         ownership of its reference until the returned future completes and
     *                         must then release that reference exactly once. An implementation may
     *                         retain and release its own reference for asynchronous work.
     * @return CompletableFuture that resolves to the Position of the written entry. This Position
     * can be used for subsequent read or delete operations.
     */
    CompletableFuture<AddResult> put(long id, int numberOfMessages, ByteBuf buf);

    /**
     * Writes a data entry to the WAL with the specified initial offset.
     * This method asynchronously appends the given data to the WAL, associating it with the provided ID.
     *
     * @param id               A unique identifier for the entry, typically representing a stream or partition.
     * @param numberOfMessages number of messages in the buf
     * @param initialOffset    The initial offset of the entry.
     * @param cumulativeSize   The cumulative size for the stream
     * @param buf              ByteBuf containing the data to be written. The caller retains
     *                         ownership of its reference until the returned future completes and
     *                         must then release that reference exactly once. An implementation may
     *                         retain and release its own reference for asynchronous work.
     * @return CompletableFuture that resolves to the Position of the written entry. This Position
     * can be used for subsequent read or delete operations.
     */
    default CompletableFuture<AddResult> put(long id, int numberOfMessages, long initialOffset, long cumulativeSize,
                                             ByteBuf buf) {
        throw new UnsupportedOperationException();
    }

    /**
     * Writes a data entry to the WAL.
     * This method asynchronously appends the given data to the WAL, associating it with the provided ID.
     *
     * @param id  A unique identifier for the entry, typically representing a stream or partition.
     * @param buf ByteBuf containing the data to be written. The caller retains ownership of its
     *            reference until the returned future completes and must then release that
     *            reference exactly once. An implementation may retain and release its own
     *            reference for asynchronous work.
     * @return CompletableFuture that resolves to the Position of the written entry. This Position
     * can be used for subsequent read or delete operations.
     */
    CompletableFuture<AddResult> put(long id, ByteBuf buf);

    /**
     * Retrieves a single data entry from the WAL.
     * This method asynchronously fetches the entry associated with the given ID and position.
     *
     * @param id             The unique identifier of the stream or partition.
     * @param offset         The offset to get Entry in the compacted WAL.
     * @param compactedIndex The compacted index.
     * @return CompletableFuture that resolves to the Entry containing the retrieved data and header.
     * The caller is responsible for releasing the ByteBuf in Entry after use.
     */
    CompletableFuture<Entry> get(long id, long offset, EntryIndex compactedIndex);


    /**
     * @deprecated This method is no longer supported after index compaction feature enabled
     * Use {@link #get(long id, long startOffset, EntryIndex compactedIndex)} instead.
     *
     * Retrieves a single data entry from the WAL.
     * This method asynchronously fetches the entry associated with the given ID and position.
     *
     * @param id             The unique identifier of the stream or partition.
     * @param index The specific Position of the entry to retrieve, as returned by the put operation.
     * @return CompletableFuture that resolves to the ByteBuf containing the retrieved data.
     * The caller is responsible for releasing the ByteBuf after use.
     */
    @Deprecated
    CompletableFuture<Entry> get(long id, EntryIndex index);

    /**
     * Retrieves multiple data entries from the WAL in a batch operation.
     * This method asynchronously fetches multiple entries associated with the given ID and positions.
     *
     * @param indices List of Positions for the entries to retrieve.
     * @param entryList containing entry read conditions and output entries
     * @return a CompletableFuture
     */
    CompletableFuture<Void> get(List<EntryIndex> indices, EntryList entryList);


    /**
     * Preemptively loads data entries into the read cache without returning them.
     * This method triggers the loading of specified entries into the cache to reduce latency
     * for subsequent read operations.
     *
     * @param id        The unique identifier of the stream or partition.
     * @param positions List of Positions for the entries to prefetch.
     */
    void preFetch(long id, List<Position> positions);

    /**
     * Deletes multiple entries from the WAL.
     * This method asynchronously removes the specified entries from the WAL.
     *
     * @param id        The unique identifier of the stream or partition.
     * @param positions List of Positions for the entries to delete.
     * @return CompletableFuture that completes when the deletion is finished. If the operation
     *         fails, the future will be completed exceptionally with the cause of the failure.
     */
    CompletableFuture<Void> delete(long id, List<Position> positions);

    /**
     * Retrieves the underlying file storage system used by the WAL.
     *
     * @return FileStorage instance used by the WAL.
     */
    default FileStorage getFileStorage() {
        throw new UnsupportedOperationException("Not implemented");
    }

    /**
     * Closes the WAL storage system.
     * This method should be called when the WAL is no longer needed to release resources,
     * close connections, and perform any necessary cleanup operations.
     *
     * @return CompletableFuture that completes when the closing process is finished. If the operation
     *         fails, the future will be completed exceptionally with the cause of the failure.
     */
    CompletableFuture<Void> close();
}
