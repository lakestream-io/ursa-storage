/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.storage;

import io.lakestream.api.EntryHeader;

/**
 * Represents a unique identifier for a data entry in a stream storage system.
 * This immutable record combines streamId, offset, and cumulativeSize to form a composite key.
 *
 * Important: This class is crucial for data indexing and retrieval operations.
 * Users should be careful when creating or manipulating Key objects to ensure data integrity.
 *
 * Serialization format: "{streamId:020d}-{offset:020d}-{cumulativeSize:020d}"
 * Each component is padded to 20 digits with leading zeros.
 *
 * Considerations when using this class:
 * 1. Immutability: Key objects are immutable, ensuring thread-safety and data consistency.
 * 2. Ordering: The natural order is based on streamId, then offset, then cumulativeSize.
 * 3. Serialization: toString() and parse() methods are complementary and used for serialization.
 * 4. Range Queries: largestKey and smallestKey methods facilitate range queries.
 * 5. Partitioning: Data is primarily partitioned by streamId (see partitionKey() method).
 * 6. Performance: Uses primitive types for efficiency in high-throughput scenarios.
 * 7. Error Handling: parse() method may throw exceptions for malformed input strings.
 * 8. Consistency: Ensure cumulativeSize is always consistent with actual data size up to the given offset.
 */
public record Key(long streamId, long offset, long cumulativeSize) {

    /**
     * Converts the Key to a string representation.
     * @return A formatted string containing streamId, offset, and cumulativeSize, each padded to 20 digits.
     * Note: This format is crucial for serialization and must be consistent with the parse method.
     */
    @Override
    public String toString() {
        return String.format("%020d-%020d-%020d", streamId, offset, cumulativeSize);
    }

    /**
     * Creates a Key representing the largest possible key for a given stream.
     * @param streamId The ID of the stream.
     * @return A Key with the maximum possible offset and cumulativeSize for the given streamId.
     * Use case: Typically used as the upper bound in range queries for a specific stream.
     */
    public static Key largestKey(long streamId) {
        return new Key(streamId, Long.MAX_VALUE, Long.MAX_VALUE);
    }

    /**
     * Creates a Key representing the largest possible key for a given stream and offset.
     * @param streamId The ID of the stream.
     * @param offset The specific offset.
     * @return A Key with the given streamId and offset, and maximum possible cumulativeSize.
     * Use case: Useful for range queries within a specific offset range in a stream.
     */
    public static Key largestKey(long streamId, long offset) {
        return new Key(streamId, offset, Long.MAX_VALUE);
    }

    /**
     * Creates a Key representing the smallest possible key for a given stream.
     * @param streamId The ID of the stream.
     * @return A Key with the minimum possible offset and cumulativeSize for the given streamId.
     * Use case: Typically used as the lower bound in range queries for a specific stream.
     */
    public static Key smallestKey(long streamId) {
        return new Key(streamId, 0, 0);
    }

    /**
     * Creates a Key representing the smallest possible key for a given stream and offset.
     * @param streamId The ID of the stream.
     * @param offset The specific offset.
     * @return A Key with the given streamId and offset, and minimum possible cumulativeSize.
     * Use case: Useful for starting range queries from a specific offset in a stream.
     */
    public static Key smallestKey(long streamId, long offset) {
        return new Key(streamId, offset, 0);
    }

    /**
     * Parses a string representation of a Key back into a Key object.
     * @param key The string representation of the Key.
     * @return A new Key object parsed from the input string.
     * @throws IllegalArgumentException if the input string is not in the correct format.
     * Note: Assumes the input follows the format produced by toString(). Error handling should be considered.
     */
    public static Key parse(String key) {
        String[] parts = key.split("-");
        return new Key(Long.parseLong(parts[0]), Long.parseLong(parts[1]), Long.parseLong(parts[2]));
    }

    /**
     * Generates a partition key based on the streamId.
     * @return A string representation of the streamId.
     * Note: Critical for data partitioning strategies in distributed systems.
     */
    public String partitionKey() {
        return String.format("%d", streamId);
    }

    public static Key of(long streamId, EntryHeader header) {
        return new Key(streamId, header.offset() + header.numberOfMessages(), header.cumulativeSize());
    }
}
