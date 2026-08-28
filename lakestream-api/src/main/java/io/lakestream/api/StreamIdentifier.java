/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.api;

/**
 * Identifies a stream within a namespace.
 *
 * <p>The namespace provides logical grouping, such as a database or application name.
 *
 * @param namespace the namespace containing the stream
 * @param name the stream name within the namespace
 */
public record StreamIdentifier(String namespace, String name) {

    /**
     * Returns the fully qualified name in {@code namespace/name} format.
     */
    public String fullName() {
        return namespace + "/" + name;
    }

    /**
     * Creates a {@code StreamIdentifier} from the given namespace and name.
     */
    public static StreamIdentifier of(String namespace, String name) {
        return new StreamIdentifier(namespace, name);
    }
}
