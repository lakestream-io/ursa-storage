/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.api;

/**
 * Type-safe identifier for a log within the storage system.
 *
 * <p>Wraps the underlying {@code long} ID to provide type safety and future
 * extensibility (e.g., composite keys for range-based layouts).
 * Maps 1:1 with {@code StorageApi}'s {@code long streamId}.
 *
 * <p>Why {@code LogId} instead of a {@code Log} interface with operations:
 * <ul>
 *   <li>Separation of concerns: identity ({@code LogId}) vs operations ({@link LogStorage})</li>
 *   <li>{@code LogId} is just data — easy to serialize, store in collections, pass between services</li>
 *   <li>Consistent with existing {@code StorageApi} pattern: operations on a service, identity as a value</li>
 * </ul>
 *
 * @param id the underlying storage stream ID
 */
public record LogId(long id) {

    /**
     * Creates a new LogId from the given storage stream ID.
     *
     * @param id the underlying storage stream ID
     * @return a new LogId instance
     */
    public static LogId of(long id) {
        return new LogId(id);
    }
}
