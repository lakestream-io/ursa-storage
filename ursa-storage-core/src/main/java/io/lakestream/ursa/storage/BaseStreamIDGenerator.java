/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.storage;

import io.lakestream.ursa.storage.impl.exception.IDGeneratorException;

/**
 * BaseStreamIDGenerator is an interface for generating unique identifiers for streams
 * in the Ursa storage system. This interface is designed to provide a flexible way
 * to generate IDs that are unique within the context of a specific stream.
 *
 * Key points to consider when using this interface:
 * 1. Implementations should ensure that generated IDs are unique for each stream.
 * 2. The generation process should be efficient, as it may be called frequently.
 * 3. Generated IDs should be suitable for use in distributed environments.
 * 4. Implementations may use different strategies (e.g., in-memory counters, distributed
 *    ID generators) depending on the specific requirements of the storage system.
 *
 * This interface is typically used in conjunction with other components of the
 * Ursa storage system, such as CompactionService and StorageApi implementations.
 *
 * @see io.lakestream.ursa.storage.impl.compaction.CompactFileIDGenerator
 * @see io.lakestream.ursa.storage.impl.StreamIdGenerator
 */
public interface BaseStreamIDGenerator {

    /**
     * Generates a unique identifier for a given stream.
     *
     * @param streamId The ID of the stream for which to generate an identifier.
     * @return A String representing the generated unique identifier.
     * @throws IDGeneratorException If there's an error during ID generation.
     *
     * Note: Implementations should ensure that:
     * 1. The generated ID is unique within the context of the given streamId.
     * 2. The method is thread-safe, as it may be called concurrently for different streams.
     * 3. The generation process is fast and efficient to avoid becoming a bottleneck.
     * 4. The generated ID format is consistent and compatible with other parts of the system
     *    that may use or store these IDs.
     */
    String generate(long streamId) throws IDGeneratorException;
}
