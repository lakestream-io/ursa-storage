/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.storage;

/**
 * Represents the value stored under the path {@code /stream-id/{stream-id}}.
 *
 * <p>This record encapsulates the key associated with a stream. The key can be {@code null},
 * which represents an empty key for the stream.
 *
 * @param key the key associated with the stream, can be {@code null} to represent an empty key
 */
public record StreamProperties(String key) {
}
