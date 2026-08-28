/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.api;

import io.lakestream.api.materialization.TableMaterializationPolicy;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * Namespace metadata. A namespace is a logical grouping of streams.
 *
 * <p>The optional {@code materialization} field carries the namespace-level
 * (active baseline) {@link TableMaterializationPolicy} that applies to every
 * stream in the namespace. Stream-level overrides merge on top of this
 * baseline during policy resolution.
 *
 * @param name the namespace name
 * @param properties user-defined properties (defensively copied)
 * @param materialization optional namespace-level materialization policy
 */
public record Namespace(
        String name,
        Map<String, String> properties,
        Optional<TableMaterializationPolicy> materialization) {

    /**
     * Canonical constructor: validates non-null fields and defensively copies
     * {@code properties} so callers cannot mutate the record's state.
     */
    public Namespace {
        Objects.requireNonNull(name, "name");
        Objects.requireNonNull(properties, "properties");
        Objects.requireNonNull(materialization,
                "materialization cannot be null; use Optional.empty()");
        properties = Map.copyOf(properties);
    }

    /**
     * Creates a namespace with the given properties and no materialization
     * policy. Maintains backward compatibility for existing callers.
     *
     * @param name the namespace name
     * @param properties user-defined properties
     */
    public Namespace(String name, Map<String, String> properties) {
        this(name, properties, Optional.empty());
    }

    /**
     * Creates a namespace with no properties and no materialization policy.
     *
     * @param name the namespace name
     */
    public Namespace(String name) {
        this(name, Map.of(), Optional.empty());
    }
}
