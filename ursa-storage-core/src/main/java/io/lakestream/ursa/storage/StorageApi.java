/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.storage;

import io.lakestream.api.EntryHeader;
import io.lakestream.api.EntryIndex;
import io.lakestream.api.LogState;
import io.lakestream.api.LogStateManager;
import io.lakestream.api.Position;
import io.netty.buffer.ByteBuf;
import io.oxia.client.api.AsyncOxiaClient;
import java.io.Closeable;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

/**
 * StorageApi is the high-level interface for the Ursa stream storage engine.
 * It provides a comprehensive set of methods for managing and interacting with data streams,
 * offering a simplified abstraction layer for clients to perform operations such as
 * appending, reading, and managing entries within streams.
 *
 * <p>Key features and responsibilities of the StorageApi:
 * <ul>
 *   <li>Stream Management: Creating, deleting, and truncating streams</li>
 *   <li>Data Operations: Appending data to streams and reading entries</li>
 *   <li>Metadata Handling: Managing and retrieving stream and entry metadata</li>
 *   <li>Index Management: Compacting and reading entry indexes</li>
 * </ul>
 *
 * <p>This interface works in conjunction with other components of the storage system:
 *
 * <ol>
 *   <li><strong>WalStorage:</strong> StorageApi implementations typically use WalStorage for actual
 *   data persistence. When append operations are called, the data is first written
 *   to the Write-Ahead Log via WalStorage before being considered durable.</li>
 *
 *   <li><strong>FileStorage:</strong> While not directly used by StorageApi, the underlying WalStorage
 *   implementation utilizes FileStorage for persistent storage of data.</li>
 *
 *   <li><strong>Metadata Management:</strong> StorageApi is responsible for managing stream metadata,
 *   often using a separate metadata store (e.g., AsyncOxiaClient in PersistStorageApi)
 *   to keep track of stream information and entry positions.</li>
 * </ol>
 *
 * <p>The asynchronous nature of the methods (returning CompletableFuture) allows for
 * non-blocking operations, enhancing the overall performance and scalability of the
 * storage system.
 *
 * <p>Implementations of this interface should handle the coordination between these
 * different components, ensuring:
 * <ul>
 *   <li>Data consistency across all operations</li>
 *   <li>Durability of appended data</li>
 *   <li>Efficient read and write operations</li>
 *   <li>Proper management of stream metadata and indexes</li>
 * </ul>
 *
 * <p>This interface is designed to be the primary point of interaction for clients
 * of the Ursa streaming engine, providing a robust and flexible API for stream
 * storage operations.
 */
public interface StorageApi extends Closeable {

    /**
     * Generates a new stream ID.
     * @return
     */
    default CompletableFuture<Long> generateStreamId() {
        return generateStreamId(Optional.empty());
    }

    /**
     * Generates a new stream ID with the specified key. The key will binding to the stream ID.
     *
     * @param key The key of the stream
     * @return A CompletableFuture that resolves to the generated stream ID
     */
    CompletableFuture<Long> generateStreamId(Optional<String> key);

    /**
     * Retrieves the stream ID associated with the specified key.
     *
     * @param key The key of the stream
     * @return A CompletableFuture that resolves to the stream ID associated with the key.
     */
    CompletableFuture<Long> getStreamIdByKey(String key);

    /**
     * Deletes only the keyed stream-ID mapping.
     *
     * <p>This operation deliberately does not delete stream data or registration metadata. It is
     * intended for callers that must remove the mapping only after data and higher-level catalog
     * metadata have been deleted successfully.
     *
     * @param key the key whose mapping should be deleted
     * @return a future that completes when the mapping has been deleted
     */
    CompletableFuture<Void> deleteStreamIdMapping(String key);

    /**
     * Retrieves a map of all stream IDs and their associated keys.
     *
     * <p>The returned map contains mappings from stream IDs to their corresponding {@link StreamProperties}.
     * The key in the map is the stream ID (Long), and the value is a {@link StreamProperties} containing
     * the key associated with that stream ID.
     *
     * <p>If a stream ID does not have an associated key, the {@link StreamProperties#key()} will be
     * {@code null}, which represents an empty key for that stream.
     *
     * <p>If there are no streams in the storage, an empty map will be returned.
     *
     * @return A CompletableFuture that resolves to a map where each key is a stream ID (Long)
     *         and each value is a {@link StreamProperties} containing the key (String) associated with
     *         that stream ID, or a {@link StreamProperties} with a {@code null} key if the stream ID
     *         has no associated key.
     */
    CompletableFuture<Map<Long, StreamProperties>> listStreamsWithProperties();

    /**
     * Retrieves the header of the first entry in the specified stream.
     *
     * @param streamId The ID of the stream
     * @return A CompletableFuture that resolves to the EntryIndex of the first entry
     */
    CompletableFuture<EntryIndex> getFirstEntry(long streamId);

    /**
     * Retrieves the header of the first entry in the specified stream, optionally including trimmed entries.
     *
     * @param streamId The ID of the stream
     * @param includeTrimmed Whether to include trimmed entries in the result
     * @return A CompletableFuture that resolves to the EntryIndex of the first entry
     */
    CompletableFuture<EntryIndex> getFirstEntry(long streamId, boolean includeTrimmed);

    /**
     * Retrieves the header of the last entry in the specified stream.
     *
     * @param streamId The ID of the stream
     * @return A CompletableFuture that resolves to the EntryIndex of the last entry
     */
    CompletableFuture<EntryIndex> getLastEntry(long streamId);

    /**
     * Reads the header of an entry at a specific offset in the stream.
     * @param streamId The ID of the stream
     * @param offset The offset of the entry in the stream
     * @return A CompletableFuture that resolves to the EntryHeader at the specified offset
     */
    CompletableFuture<EntryHeader> readEntryHeader(long streamId, long offset);

    /**
     * Appends data to the specified stream.
     *
     * @param streamId         The ID of the stream
     * @param numberOfMessages The number of messages in the data
     * @param data             The ByteBuf containing the data to append
     * @return A CompletableFuture that resolves to the EntryHeader of the appended entry
     */
    CompletableFuture<AddResult> append(long streamId, int numberOfMessages, ByteBuf data);

    /**
     * Appends data to the specified stream with the specified initial offset.
     *
     * @param streamId         The ID of the stream
     * @param numberOfMessages The number of messages in the data
     * @param initialOffset    The initial offset of the entry
     * @param cumulativeSize   The specified cumulative size
     * @param data             The ByteBuf containing the data to append
     * @return A CompletableFuture that resolves to the EntryHeader of the appended entry
     */
    CompletableFuture<AddResult> write(long streamId, int numberOfMessages, long initialOffset, long cumulativeSize,
                                       ByteBuf data);

    /**
     * @deprecated This method is no longer supported after index compaction feature enabled
     * Use {@link #read(long id, long offset, EntryIndex index)} instead.
     *
     * Reads an entry from the specified stream at a given offset.
     * @param streamId The ID of the stream
     * @param offset The offset of the entry to read
     * @return A CompletableFuture that resolves to the Entry at the specified offset
     */
    @Deprecated
    CompletableFuture<Entry> read(long streamId, long offset);

    /**
     *
     * Reads an entry from the specified stream at a given offset in the given index.
     * @param streamId The ID of the stream
     * @param offset The offset of the entry to read
     * @param index The compacted index where the entry should be located in its compacted offset range.
     * @return A CompletableFuture that resolves to the Entry at the specified offset
     */
    CompletableFuture<Entry> read(long streamId, long offset, EntryIndex index);

    /**
     * Reads an entry from the specified stream at a given offset in the given index,
     * with an option to include trimmed entries.
     *
     * <p>The {@code includeTrimmed} parameter
     * controls whether entries that have been soft-trimmed (marked as deleted)
     * should be accessible for reading.
     *
     * <p>When {@code includeTrimmed} is set to {@code false}, only active entries
     * can be read. When set to {@code true}, both active and trimmed entries
     * can be accessed, which is useful for recovery operations or administrative tasks.
     *
     * @param streamId       The ID of the stream
     * @param offset         The offset of the entry to read
     * @param index          The compacted index where the entry should be located in its compacted offset range
     * @param includeTrimmed Whether to allow reading of trimmed (soft-deleted) entries
     * @return A CompletableFuture that resolves to the Entry at the specified offset
     */
    CompletableFuture<Entry> read(long streamId, long offset, EntryIndex index, boolean includeTrimmed);

    /**
     * Reads multiple entries from the specified stream, starting at a given offset.
     * @param streamId The ID of the stream
     * @param startOffset The offset to start reading from
     * @param maxMessageCount The maximum number of messages to read
     * @param maxSize The maximum total size of entries to read
     * @return A CompletableFuture that resolves to a List of Entries
     */
    CompletableFuture<List<Entry>> readEntries(long streamId, long startOffset, int maxMessageCount, int maxSize);

    /**
     * Reads multiple entries from the specified stream based on a list of entry positions.
     *
     * This method allows for efficient batch reading of entries from a stream using
     * pre-fetched entry headers and positions. It's particularly useful when you have
     * already obtained the headers and positions of entries (e.g., from a previous
     * operation) and want to retrieve the full entries in a single operation.
     *
     * @param indices   A list of index, each containing an EntryHeader and its corresponding Position
     *                  in the stream. This allows for precise and efficient retrieval of specific entries.
     * @param entryList The entry list read conditions and entry output list.
     * @return A CompletableFuture that resolves to a List of Entry objects corresponding
     * to the provided positions. The entries in the returned list will be in the
     * same order as the input positions.
     */
    CompletableFuture<List<Entry>> readEntries(List<EntryIndex> indices, EntryList entryList);

    /**
     * Proactively fetches entries into the storage cache for future read operations.
     *
     * <p>This method provides a performance optimization by pre-loading entries into
     * the cache before they are actually needed. It is particularly useful when the
     * application knows in advance which entries will be accessed, allowing for
     * reduced latency on subsequent read operations.
     *
     * <p>The operation is asynchronous and non-blocking. Any failures during
     * prefetching are handled silently and won't affect the main operations.
     *
     * @param streamId The ID of the stream containing the entries to prefetch
     * @param positions A list of Position objects indicating the locations of entries
     *                 to prefetch into the cache
     */
    void preFetchEntries(long streamId, List<Position> positions);

    /**
     * Trim the stream by marking all entries up to and including the specified offset as deleted.
     * This operation marks the entries as deleted rather than physically removing them.
     *
     * @param streamId       The ID of the stream
     * @param offsetIncluded The offset up to which (inclusive) entries should be removed
     * @return the future of the first entry's offset after the truncation is done
     */
    CompletableFuture<Long> softTrimStream(long streamId, long offsetIncluded);

    /**
     * Trim the stream by deleting all entries in the specified stream up to and including the given offset.
     * This operation is a hard trim, meaning it physically removes the entries from the stream.
     *
     * <p>This method removes the data entries in the range from the beginning of the stream
     * up to and including {@code offsetIncluded}. Unlike {@link #softTrimStream(long, long)},
     * this method does not update the mark-deleted offset or return the next available offset.
     * It simply deletes the specified range of entries from the Oxia.
     *
     * <p>The operation is asynchronous and returns a CompletableFuture that completes
     * when the deletion is finished.
     *
     * @param streamId       The ID of the stream from which to delete entries
     * @param offsetExcluded The offset up to which (exclusive) entries should be deleted
     * @return A CompletableFuture that completes when the entries have been deleted
     */
    CompletableFuture<Void> hardTrimStream(long streamId, long offsetExcluded);

    /**
     * Deletes the specified stream.
     * @param streamId The ID of the stream to delete
     * @return A CompletableFuture that completes when the stream is deleted
     */
    default CompletableFuture<Void> deleteStream(long streamId) {
        return deleteStream(streamId, Optional.empty());
    }

    /**
     * Deletes the specified stream with the specified key.
     *
     * @param streamId The ID of the stream to delete
     * @param key The key of the stream. If the key is present, the stream will be deleted only if the key matches.
     * @return A CompletableFuture that completes when the stream is deleted
     */
    CompletableFuture<Void> deleteStream(long streamId, Optional<String> key);

    /**
     * Compact the oxia indexes. If there are three oxia keys: 0-0-10, 0-10-20, 0-20-30. We input params
     * {stream:0, startOffset:0, endOffset: 20, endCumulativeSize:30, value: newValue}, it will update
     * the key:0-20-30 value to newValue, and then delete keys: 0-0-10, 0-10-20.
     *
     * @param streamId The ID of the stream to delete
     * @param startOffset The start offset for compact index.
     * @param endOffset The end offset for compact index.
     * @param endCumulativeSize The new compacted key's endCumulativeSize
     * @param value The new value.
     */
    CompletableFuture<Void> compactEntryIndex(long streamId, long startOffset, long endOffset, long endCumulativeSize,
                                              Value value);

    /**
     * Get the first un-compacted position of the stream.
     * @param streamId The ID of the stream
     * @return A CompletableFuture that resolves to the first un-compacted position
     */
    default CompletableFuture<Position> getFirstUnCompactedPosition(long streamId) {
        return CompletableFuture.failedFuture(new UnsupportedOperationException("Not implemented"));
    }

    /**
     * Retrieves a map of all stream IDs and their corresponding mark-deleted offsets.
     *
     * <p>The mark-deleted offset for a stream indicates the highest offset up to and excluding which
     * all entries have been logically deleted
     * This information is useful for understanding the current truncation state of each stream and for
     * managing storage cleanup or retention policies.
     *
     * <p>The returned map's keys are string representations of stream IDs, and the values are the
     * corresponding mark-deleted offsets for each stream.
     *
     * @return A CompletableFuture that resolves to a map where each key is a stream ID (as a String)
     * and each value is the mark-deleted offset (as a Long) for that stream.
     */
    CompletableFuture<Map<String, Long>> getMarkDeletedOffsetMap();

    /**
     * List all streams.
     *
     * @return A CompletableFuture that resolves to a Set of stream IDs
     */
    default CompletableFuture<Set<Long>> listStreams() {
        return CompletableFuture.failedFuture(new UnsupportedOperationException());
    }

    /**
     * Get the underlying oxia client for the stream storage.
     *
     * @return The underlying oxia client
     */
    AsyncOxiaClient getStorageOxiaClient();

    /**
     * Starts the Write-Ahead Log (WAL) cleanup service.
     *
     * This method initializes and begins the process of cleaning up old or unnecessary
     * entries in the Write-Ahead Log. The cleanup service helps to manage storage space
     * and improve overall system performance by removing outdated log entries.
     *
     * @throws Exception if there's an error starting the WAL cleanup service
     */
    void startWALCleanupService() throws Exception;

    /**
     * Reads the entry header and its position for a specific entry in the stream.
     *
     * This method retrieves both the EntryHeader and the Position of an entry
     * at the specified offset in the given stream. This can be useful for
     * operations that require both metadata about the entry (from the header)
     * and its exact location in storage.
     *
     * @param streamId The ID of the stream containing the entry
     * @param offset The offset of the entry within the stream
     * @return A CompletableFuture that resolves to a EntryIndex containing the EntryHeader
     *         and Position of the specified entry.
     */
    CompletableFuture<EntryIndex> readEntryIndex(long streamId, long offset);

    /**
     * Reads multiple entry indexes from the specified stream within a given offset range.
     *
     * This method retrieves a list of entry headers and their corresponding positions
     * for entries in the specified stream, starting from the given start offset up to
     * (but not including) the end offset. This can be useful for efficiently fetching
     * metadata about a range of entries without reading their full content.
     *
     * @param streamId The ID of the stream from which to read the indexes
     * @param startOffset The offset of the first entry to include in the results
     * @param endOffset The offset of the first entry to exclude from the results
     * @return A CompletableFuture that resolves to a List of EntryIndex, each containing
     *         an EntryHeader and its corresponding Position for entries within the
     *         specified range. The list is ordered by increasing offset.
     */
    default CompletableFuture<List<EntryIndex>> readIndexes(long streamId, long startOffset, long endOffset) {
        return readIndexes(streamId, startOffset, endOffset, false);
    }

    /**
     * Reads multiple entry indexes from the specified stream within a given offset range,
     * with an option to include trimmed entries.
     *
     * <p>This method retrieves a list of entry headers and their corresponding positions
     * for entries in the specified stream, starting from the given start offset up to
     * (but not including) the end offset. This can be useful for efficiently fetching
     * metadata about a range of entries without reading their full content.
     *
     * <p>The {@code includeTrimmed} parameter controls whether entries that have been
     * soft-trimmed (marked as deleted) should be included in the results. When set to
     * {@code false}, only active entries will be returned. When set to {@code true},
     * both active and trimmed entries will be included.
     *
     * @param streamId The ID of the stream from which to read the indexes
     * @param startOffset The offset of the first entry to include in the results
     * @param endOffset The offset of the first entry to exclude from the results
     * @param includeTrimmed Whether to include trimmed (soft-deleted) entries in the results
     * @return A CompletableFuture that resolves to a List of EntryIndex, each containing
     *         an EntryHeader and its corresponding Position for entries within the
     *         specified range. The list is ordered by increasing offset.
     */
    CompletableFuture<List<EntryIndex>> readIndexes(long streamId, long startOffset, long endOffset,
                                                    boolean includeTrimmed);

    /**
     * Get the log state manager, which manages the state of all streams generated by this storage.
     * When an existing stream id is retrieved from {@link this#generateStreamId(Optional)}, the stream's state will be
     * reset as {@link LogState#NORMAL} automatically.
     */
    LogStateManager getStreamStateManager();
}
