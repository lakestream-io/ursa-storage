/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.storage;

import io.lakestream.api.EntryHeader;
import io.lakestream.api.EntryIndex;
import io.lakestream.api.LogStateManager;
import io.lakestream.api.Position;
import io.netty.buffer.ByteBuf;
import io.oxia.client.api.AsyncOxiaClient;
import java.io.Closeable;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

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
 * <p>Catalog implementations that clean up keyed stream allocations require an atomic, durable
 * mapping fence. Implementations that do not advertise
 * {@link #supportsFencedStreamIdMappings()} remain usable for operations that do not encounter
 * retired allocations, but destructive lifecycle operations fail with
 * {@link UnsupportedOperationException}. A conditional delete alone cannot close the window in
 * which an allocator started before deletion publishes after the key becomes absent. The catalog
 * must also not emulate fencing with {@link #getStreamIdByKey(String)} followed by
 * {@link #deleteStreamIdMapping(String)} because a concurrent replacement between those calls
 * would be deleted without ownership fencing.
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
     * A durable-storage write lease held by one opened writable log handle.
     *
     * <p>The backing record is ephemeral and therefore also disappears when the Oxia client
     * session expires. Callers must nevertheless close the lease explicitly after all writes
     * issued by the log handle have drained. Closing is idempotent. Implementations must retain
     * ownership of a started release and retry transient metadata-store failures internally; a
     * caller dropping or attempting to cancel the returned future must not abandon the ephemeral
     * lease cleanup.
     */
    interface StreamWriteLease extends AutoCloseable {

        /** Returns the numeric stream ID protected by this lease. */
        long streamId();

        /**
         * Releases this lease asynchronously.
         *
         * <p>Concurrent calls share one in-flight completion. Transient release failures are
         * retried internally with bounded backoff until the ephemeral lease is removed. Caller
         * cancellation must not stop that cleanup.
         */
        CompletableFuture<Void> closeAsync();

        /** Releases this lease and waits for Oxia to acknowledge the deletion. */
        @Override
        default void close() {
            closeAsync().join();
        }
    }

    /**
     * Reports that physical deletion could not drain the opened writable log handles before its
     * deadline.
     *
     * <p>The durable write fence remains installed after this exception. No stream indexes or
     * registration metadata have been purged, so a lifecycle reconciler can retry deletion later.
     */
    final class StreamWriteLeaseDrainTimeoutException extends IllegalStateException {

        /** The active count is unknown because the lease inventory read itself timed out. */
        public static final int UNKNOWN_ACTIVE_LEASE_COUNT = -1;

        private final long streamId;
        private final int activeLeaseCount;

        public StreamWriteLeaseDrainTimeoutException(long streamId, int activeLeaseCount,
                                                      Duration timeout) {
            super(timeoutMessage(streamId, activeLeaseCount, timeout));
            if (activeLeaseCount < UNKNOWN_ACTIVE_LEASE_COUNT) {
                throw new IllegalArgumentException(
                    "activeLeaseCount must be non-negative or UNKNOWN_ACTIVE_LEASE_COUNT");
            }
            this.streamId = streamId;
            this.activeLeaseCount = activeLeaseCount;
        }

        private static String timeoutMessage(
                long streamId, int activeLeaseCount, Duration timeout) {
            String leaseDescription = activeLeaseCount == UNKNOWN_ACTIVE_LEASE_COUNT
                ? "the active write-lease inventory"
                : activeLeaseCount + " active write lease(s)";
            return "Timed out after " + timeout + " waiting for " + leaseDescription
                + " on stream " + streamId;
        }

        public long streamId() {
            return streamId;
        }

        public int activeLeaseCount() {
            return activeLeaseCount;
        }
    }

    /**
     * Generates a globally unique stream ID that is never reused by this storage instance.
     *
     * @return a future resolving to a fresh stream ID
     */
    default CompletableFuture<Long> generateStreamId() {
        return generateStreamId(Optional.empty());
    }

    /**
     * Generates or resolves a stream ID bound to the specified key.
     *
     * <p>Every newly generated numeric ID must be globally unique and must never be reused, even
     * after its keyed mapping and physical log have been retired. Lifecycle cleanup relies on this
     * invariant to ensure an old cleanup journal cannot delete a later log with the same ID.
     *
     * @param key The key of the stream
     * @return A CompletableFuture that resolves to the generated stream ID
     */
    CompletableFuture<Long> generateStreamId(Optional<String> key);

    /**
     * Allocates a stream ID and reports whether this invocation created its keyed mapping.
     *
     * <p>The default is deliberately conservative for existing implementations: without an
     * atomic allocation result, callers must treat the returned mapping as reused and must not
     * destructively compensate it after a higher-level operation is rejected.
     *
     * @param key the optional key to bind to the stream ID
     * @return the allocated stream ID and keyed-mapping provenance
     */
    default CompletableFuture<StreamIdAllocation> allocateStreamId(Optional<String> key) {
        return generateStreamId(key).thenApply(streamId ->
            new StreamIdAllocation(streamId, false));
    }

    /** Result of a stream-ID allocation. */
    record StreamIdAllocation(long streamId, boolean createdKeyedMapping) {
    }

    /**
     * Durable identity of the catalog lifecycle that owns a keyed stream-ID mapping.
     *
     * <p>The incarnation prevents a generation counter that restarts for a recreated logical
     * stream from being confused with an older lifecycle. The token distinguishes competing
     * owners within one incarnation, while the generation orders ownership changes recorded by
     * the catalog.
     */
    record StreamIdMappingOwner(
            String incarnationId, String ownerToken, long ownerGeneration) {

        private static final String LEGACY_IDENTITY = "__legacy__";

        public StreamIdMappingOwner {
            Objects.requireNonNull(incarnationId, "incarnationId");
            Objects.requireNonNull(ownerToken, "ownerToken");
            if (incarnationId.isBlank()) {
                throw new IllegalArgumentException("incarnationId must not be blank");
            }
            if (ownerToken.isBlank()) {
                throw new IllegalArgumentException("ownerToken must not be blank");
            }
            if (ownerGeneration < 0) {
                throw new IllegalArgumentException("ownerGeneration must be non-negative");
            }
        }

        /** Returns the synthetic owner used when reading the legacy numeric mapping format. */
        public static StreamIdMappingOwner legacy() {
            return new StreamIdMappingOwner(LEGACY_IDENTITY, LEGACY_IDENTITY, 0);
        }

        /** Returns whether this is the synthetic owner of a legacy numeric mapping. */
        public boolean isLegacy() {
            return incarnationId.equals(LEGACY_IDENTITY)
                && ownerToken.equals(LEGACY_IDENTITY)
                && ownerGeneration == 0;
        }
    }

    /**
     * Descriptor of a durable keyed-mapping tombstone.
     *
     * <p>A caller must retain this descriptor in its cleanup journal and acknowledge the exact
     * descriptor before a later owner can replace the tombstone. A stream ID of {@code -1}
     * represents a fence installed while the mapping was absent.
     */
    record StreamIdMappingFence(long streamId, StreamIdMappingOwner owner) {

        public StreamIdMappingFence {
            if (streamId < -1) {
                throw new IllegalArgumentException("streamId must be -1 or non-negative");
            }
            Objects.requireNonNull(owner, "owner");
        }
    }

    /** Descriptor of an active keyed stream-ID mapping observed by an atomic fence attempt. */
    record ActiveStreamIdMapping(long streamId, StreamIdMappingOwner owner) {

        public ActiveStreamIdMapping {
            if (streamId < 0) {
                throw new IllegalArgumentException("streamId must be non-negative");
            }
            Objects.requireNonNull(owner, "owner");
        }
    }

    /**
     * Atomic outcome of a lifecycle-aware keyed-mapping fence.
     *
     * <p>{@link Fenced} proves that the key contains the returned durable tombstone. {@link
     * PreservedActive} reports the complete active mapping that was deliberately left unchanged
     * because it did not match the expected lifecycle.
     */
    interface StreamIdMappingFenceResult {

        /** A durable tombstone was installed or was already present. */
        record Fenced(StreamIdMappingFence fence) implements StreamIdMappingFenceResult {

            public Fenced {
                Objects.requireNonNull(fence, "fence");
            }
        }

        /** A non-matching active mapping was observed and preserved. */
        record PreservedActive(ActiveStreamIdMapping mapping)
                implements StreamIdMappingFenceResult {

            public PreservedActive {
                Objects.requireNonNull(mapping, "mapping");
            }
        }
    }

    /**
     * Reports an active keyed mapping that cannot be allocated or bound by another lifecycle.
     */
    final class StreamIdMappingConflictException extends IllegalStateException {

        private final String key;
        private final ActiveStreamIdMapping activeMapping;

        public StreamIdMappingConflictException(
                String key, ActiveStreamIdMapping activeMapping) {
            super("Keyed stream-ID mapping is active for a different owner or stream ID at "
                + Objects.requireNonNull(key, "key") + " (streamId="
                + Objects.requireNonNull(activeMapping, "activeMapping").streamId() + ")");
            this.key = key;
            this.activeMapping = activeMapping;
        }

        /** Returns the logical mapping key that conflicted. */
        public String key() {
            return key;
        }

        /** Returns the active mapping that was preserved. */
        public ActiveStreamIdMapping activeMapping() {
            return activeMapping;
        }
    }

    /**
     * Allocates a keyed stream ID for a durable lifecycle owner.
     *
     * <p>An existing active mapping is reused only when it has the same owner. A tombstone is
     * replaced only when {@code acknowledgedFence} exactly matches its durable descriptor.
     *
     * @param key the key to bind
     * @param owner the lifecycle that will own the active mapping
     * @param acknowledgedFence the exact preceding tombstone, when replacing one
     * @return the allocated stream ID and whether this call installed the active mapping
     */
    default CompletableFuture<StreamIdAllocation> allocateStreamId(
            String key, StreamIdMappingOwner owner,
            Optional<StreamIdMappingFence> acknowledgedFence) {
        return CompletableFuture.failedFuture(new UnsupportedOperationException(
            "Lifecycle-aware keyed stream-ID allocation is not supported by this storage"));
    }

    /**
     * Binds an already allocated stream ID to a durable lifecycle owner.
     *
     * <p>This is used to atomically adopt a legacy broker-created numeric mapping without changing
     * its stream ID. A different active owner or stream ID is never overwritten. As with
     * lifecycle-aware allocation, replacing a tombstone requires its exact descriptor.
     */
    default CompletableFuture<Void> bindStreamIdMapping(
            String key, long streamId, StreamIdMappingOwner owner,
            Optional<StreamIdMappingFence> acknowledgedFence) {
        return CompletableFuture.failedFuture(new UnsupportedOperationException(
            "Lifecycle-aware keyed stream-ID binding is not supported by this storage"));
    }

    /**
     * Atomically fences or observes a keyed mapping for the expected lifecycle.
     *
     * <p>When {@code expectedStreamId} is {@code -1}, an active mapping owned by {@code
     * expectedOwner} represents an allocation that raced publication with an absent-key fence. The
     * implementation must fence that mapping's actual stream ID and return it in {@link
     * StreamIdMappingFenceResult.Fenced}. A non-matching active mapping must be preserved and
     * returned in {@link StreamIdMappingFenceResult.PreservedActive}.
     */
    default CompletableFuture<StreamIdMappingFenceResult> fenceStreamIdMappingState(
            String key, long expectedStreamId, StreamIdMappingOwner expectedOwner) {
        return CompletableFuture.failedFuture(new UnsupportedOperationException(
            "Lifecycle-aware keyed stream-ID fencing is not supported by this storage"));
    }

    /**
     * Atomically fences an active keyed mapping owned by the expected lifecycle.
     *
     * <p>The result contains the durable tombstone descriptor when a fence is installed or already
     * present. An empty result means an active mapping belongs to another owner or stream ID and
     * was left unchanged. When the mapping is absent, the implementation installs a stable
     * {@code -1} tombstone owned by {@code expectedOwner} so an older in-flight allocator cannot
     * publish afterward.
     *
     * @deprecated use {@link #fenceStreamIdMappingState(String, long, StreamIdMappingOwner)} to
     *     distinguish a durable fence from the active mapping that was preserved
     */
    @Deprecated
    default CompletableFuture<Optional<StreamIdMappingFence>> fenceStreamIdMapping(
            String key, long expectedStreamId, StreamIdMappingOwner expectedOwner) {
        return fenceStreamIdMappingState(key, expectedStreamId, expectedOwner)
            .thenApply(result -> result instanceof StreamIdMappingFenceResult.Fenced fenced
                ? Optional.of(fenced.fence()) : Optional.empty());
    }

    /**
     * Rewrites an exact durable tombstone into its canonical descriptor.
     *
     * <p>This operation lets cleanup collapse one or more retired allocation descriptors into the
     * current partition tombstone. It must never overwrite an active mapping or a different
     * tombstone.
     */
    default CompletableFuture<Void> canonicalizeStreamIdMappingFence(
            String key, StreamIdMappingFence expectedFence,
            StreamIdMappingFence canonicalFence) {
        return CompletableFuture.failedFuture(new UnsupportedOperationException(
            "Lifecycle-aware keyed stream-ID fence canonicalization is not supported"));
    }

    /**
     * Returns whether durable lifecycle-aware mapping fences are implemented atomically.
     *
     * <p>The default is {@code false} so older and custom implementations remain binary
     * compatible without being treated as safe for cleanup-journal acknowledgement.
     */
    default boolean supportsFencedStreamIdMappings() {
        return false;
    }

    /**
     * Reports that a keyed allocation became invalid after its stream registration was made
     * durable.
     *
     * <p>The allocation is retained so a lifecycle-aware caller can safely compensate resources
     * that this storage layer cannot delete without knowing whether the stream ID was published.
     */
    final class KeyedAllocationInvalidatedException extends RuntimeException {

        private final StreamIdAllocation allocation;

        public KeyedAllocationInvalidatedException(
                StreamIdAllocation allocation, Throwable cause) {
            super("Keyed stream-ID allocation was invalidated for stream ID "
                + Objects.requireNonNull(allocation, "allocation").streamId(), cause);
            this.allocation = allocation;
        }

        /** Returns the allocation that failed its final keyed-mapping validation. */
        public StreamIdAllocation allocation() {
            return allocation;
        }
    }

    /**
     * Retrieves the stream ID associated with the specified key.
     *
     * @param key The key of the stream
     * @return A CompletableFuture that resolves to the stream ID associated with the key.
     */
    CompletableFuture<Long> getStreamIdByKey(String key);

    /**
     * Deletes only an active keyed stream-ID mapping.
     *
     * <p>This operation deliberately does not delete stream data or registration metadata. It is
     * intended for callers that must remove the mapping only after data and higher-level catalog
     * metadata have been deleted successfully. An implementation that advertises {@link
     * #supportsFencedStreamIdMappings()} must never remove a durable tombstone through this legacy
     * method and may reject the operation entirely.
     *
     * @param key the key whose mapping should be deleted
     * @return a future that completes when the mapping has been deleted
     * @throws UnsupportedOperationException if unconditional mapping deletion is disabled
     * @deprecated lifecycle-aware callers must use {@link
     *     #fenceStreamIdMappingState(String, long, StreamIdMappingOwner)}
     */
    @Deprecated
    CompletableFuture<Void> deleteStreamIdMapping(String key);

    /**
     * Removes or durably fences a keyed stream-ID mapping only when it still references the
     * expected stream ID.
     *
     * <p>An implementation that advertises {@link #supportsFencedStreamIdMappings()} must leave a
     * stable tombstone rather than making the key absent, so an allocator that started before this
     * operation cannot publish after it completes.
     *
     * @param key the key whose mapping should be deleted
     * @param expectedStreamId the stream ID that the caller still owns
     * @return a future that completes when the mapping is absent or owned by another stream ID
     * @throws UnsupportedOperationException if the storage implementation does not support
     *     conditional keyed-mapping deletion
     */
    default CompletableFuture<Void> deleteStreamIdMapping(String key, long expectedStreamId) {
        return CompletableFuture.failedFuture(new UnsupportedOperationException(
            "Conditional keyed stream-ID deletion is not supported by this storage"));
    }

    /**
     * Returns whether {@link #deleteStreamIdMapping(String, long)} is implemented atomically.
     *
     * <p>The default is {@code false} for binary compatibility with existing implementations. A
     * {@code false} result does not disable metadata-only catalog operations or initial keyed
     * allocation, but operations that can destructively clean up a keyed mapping must fail before
     * deleting log data or the mapping.
     *
     * @return {@code true} only when conditional keyed-mapping deletion is atomic
     */
    default boolean supportsConditionalStreamIdMappingDeletion() {
        return false;
    }

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
     * @param data             The ByteBuf containing the data to append. The caller retains
     *                         ownership of its reference until the returned future completes and
     *                         must then release that reference exactly once.
     * @return A CompletableFuture that resolves to the EntryHeader of the appended entry
     * @implSpec Durable-fencing implementations require an active {@link StreamWriteLease} owned
     *     by the same storage instance.
     */
    CompletableFuture<AddResult> append(long streamId, int numberOfMessages, ByteBuf data);

    /**
     * Appends data to the specified stream with the specified initial offset.
     *
     * @param streamId         The ID of the stream
     * @param numberOfMessages The number of messages in the data
     * @param initialOffset    The initial offset of the entry
     * @param cumulativeSize   The specified cumulative size
     * @param data             The ByteBuf containing the data to append. The caller retains
     *                         ownership of its reference until the returned future completes and
     *                         must then release that reference exactly once.
     * @return A CompletableFuture that resolves to the EntryHeader of the appended entry
     * @implSpec Durable-fencing implementations require an active {@link StreamWriteLease} owned
     *     by the same storage instance.
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
     * @implSpec Durable-fencing implementations require an active {@link StreamWriteLease} owned
     *     by the same storage instance.
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
     * @implSpec Durable-fencing implementations require an active {@link StreamWriteLease} owned
     *     by the same storage instance.
     */
    CompletableFuture<Void> hardTrimStream(long streamId, long offsetExcluded);

    /**
     * Returns whether this storage implementation provides durable, cross-process stream write
     * fencing and ephemeral per-log-handle leases.
     */
    default boolean supportsDurableStreamWriteFencing() {
        return false;
    }

    /**
     * Acquires an ephemeral write lease for one opened writable-log handle of the numeric stream.
     *
     * <p>The lease is published before the durable fence is checked. This ordering closes the
     * open-versus-delete race: a lease ordered before the fence blocks physical deletion, while a
     * lease ordered after the fence is rejected and removed. Callers must retain the returned
     * lease for the entire lifetime of the opened log and close it only after outstanding writes
     * have drained.
     *
     * @param streamId numeric stream ID to open for writing
     * @return a future resolving to an owned, idempotently closeable lease
     */
    default CompletableFuture<StreamWriteLease> acquireStreamWriteLease(long streamId) {
        return CompletableFuture.failedFuture(new UnsupportedOperationException(
            "Durable stream write leases are not supported"));
    }

    /**
     * Runs one storage mutation while owning a durable write lease for its entire lifetime.
     *
     * <p>Cancellation of the returned future does not abandon the mutation or lease cleanup. The
     * lease is released only after the mutation settles. If both mutation and release fail, the
     * release failure is attached to the mutation failure.
     *
     * @param streamId numeric stream ID being mutated
     * @param mutation operation to run after the lease is acquired
     * @param <T> mutation result type
     * @return a future that completes only after the mutation and lease release settle
     */
    default <T> CompletableFuture<T> withStreamWriteLease(
            long streamId,
            Function<StreamWriteLease, CompletableFuture<T>> mutation) {
        Objects.requireNonNull(mutation, "mutation");
        CompletableFuture<T> result = new CompletableFuture<>();
        acquireStreamWriteLease(streamId).whenComplete((lease, acquireFailure) -> {
            if (acquireFailure != null) {
                result.completeExceptionally(acquireFailure);
                return;
            }
            final CompletableFuture<T> operation;
            try {
                operation = Objects.requireNonNull(
                    mutation.apply(lease), "storage mutation future");
            } catch (RuntimeException | Error failure) {
                completeAfterWriteLeaseClose(result, lease, null, failure);
                return;
            }
            operation.whenComplete((value, failure) ->
                completeAfterWriteLeaseClose(result, lease, value, failure));
        });
        return result;
    }

    private static <T> void completeAfterWriteLeaseClose(
            CompletableFuture<T> result, StreamWriteLease lease,
            T value, Throwable mutationFailure) {
        final CompletableFuture<Void> close;
        try {
            close = Objects.requireNonNull(lease.closeAsync(), "lease close future");
        } catch (RuntimeException | Error closeFailure) {
            completeWriteLeaseResult(result, value, mutationFailure, closeFailure);
            return;
        }
        close.whenComplete((ignored, closeFailure) ->
            completeWriteLeaseResult(result, value, mutationFailure, closeFailure));
    }

    private static <T> void completeWriteLeaseResult(
            CompletableFuture<T> result, T value,
            Throwable mutationFailure, Throwable closeFailure) {
        if (mutationFailure != null) {
            if (closeFailure != null && closeFailure != mutationFailure) {
                mutationFailure.addSuppressed(closeFailure);
            }
            result.completeExceptionally(mutationFailure);
        } else if (closeFailure != null) {
            result.completeExceptionally(closeFailure);
        } else {
            result.complete(value);
        }
    }

    /**
     * Permanently fences new write leases for a numeric stream ID.
     *
     * <p>This operation is durable and idempotent. There is intentionally no unfence operation:
     * numeric stream IDs must never be reused after fencing.
     */
    default CompletableFuture<Void> fenceStreamWrites(long streamId) {
        return CompletableFuture.failedFuture(new UnsupportedOperationException(
            "Durable stream write fencing is not supported"));
    }

    /**
     * Permanently fences a numeric stream ID and waits for every opened write lease to drain,
     * without deleting its data.
     *
     * <p>Successful retirement is terminal: no later writable handle may be opened for this
     * numeric ID. A timeout leaves the durable fence and physical data intact so lifecycle
     * reconciliation can retry safely.
     */
    default CompletableFuture<Void> fenceAndDrainStreamWrites(long streamId) {
        return fenceAndDrainStreamWrites(streamId, Duration.ofSeconds(30));
    }

    /**
     * Permanently fences a numeric stream ID and drains write leases within the supplied timeout.
     *
     * @param streamId numeric stream ID to retire
     * @param leaseDrainTimeout maximum time to wait for opened write leases
     * @return a future completing after the durable fence is installed and leases are drained
     */
    default CompletableFuture<Void> fenceAndDrainStreamWrites(
            long streamId, Duration leaseDrainTimeout) {
        Objects.requireNonNull(leaseDrainTimeout, "leaseDrainTimeout");
        return CompletableFuture.failedFuture(new UnsupportedOperationException(
            "Durable stream write fencing is not supported"));
    }

    /**
     * Deletes the specified stream.
     *
     * <p>The operation is idempotent: an already absent stream is a successful result.
     *
     * @param streamId The ID of the stream to delete
     * @return A CompletableFuture that completes when the stream is deleted
     */
    default CompletableFuture<Void> deleteStream(long streamId) {
        return deleteStream(streamId, Optional.empty());
    }

    /**
     * Deletes a numeric stream after permanently fencing new handles and draining existing write
     * leases.
     *
     * <p>Implementations that advertise {@link #supportsDurableStreamWriteFencing()} must leave the
     * durable fence installed on every outcome. If the timeout expires, the future fails with
     * {@link StreamWriteLeaseDrainTimeoutException} and physical data must remain untouched.
     * Existing implementations without durable fencing retain their legacy deletion behavior.
     *
     * @param streamId numeric stream ID to delete
     * @param leaseDrainTimeout maximum time to wait for all opened-handle leases to close
     * @return a future completing only after physical deletion succeeds
     */
    default CompletableFuture<Void> deleteStream(long streamId, Duration leaseDrainTimeout) {
        Objects.requireNonNull(leaseDrainTimeout, "leaseDrainTimeout");
        if (leaseDrainTimeout.isNegative()) {
            return CompletableFuture.failedFuture(new IllegalArgumentException(
                "leaseDrainTimeout must not be negative"));
        }
        return deleteStream(streamId);
    }

    /**
     * Deletes the specified stream through the legacy optional-key API.
     *
     * <p>The operation is idempotent when {@code key} is empty. A supplied key cannot safely be
     * checked and removed by this method: the mapping may be rebound to a replacement lifecycle
     * between validation and deletion. Lifecycle-aware callers must first use {@link
     * #fenceStreamIdMappingState(String, long, StreamIdMappingOwner)}, retain the returned durable
     * fence in their cleanup journal, delete the physical stream through {@link
     * #deleteStream(long)}, and acknowledge that exact fence only after cleanup succeeds.
     * Implementations should fail closed when {@code key} is present.
     *
     * @param streamId The ID of the stream to delete
     * @param key An obsolete keyed-mapping argument; only {@link Optional#empty()} is safe
     * @return A CompletableFuture that completes when the stream is deleted
     * @throws UnsupportedOperationException if {@code key} is present
     * @deprecated use {@link #deleteStream(long)} and the durable mapping-fence API separately
     */
    @Deprecated
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
     * @implSpec Durable-fencing implementations require an active {@link StreamWriteLease} owned
     *     by the same storage instance.
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
     * Get the log state manager, which manages the process-local state of all streams generated by
     * this storage. A durable write fence is terminal for a numeric stream ID and is never reset by
     * resolving an existing keyed mapping.
     */
    LogStateManager getStreamStateManager();
}
