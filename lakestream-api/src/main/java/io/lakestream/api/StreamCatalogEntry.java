/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.api;

import java.util.Map;
import java.util.Objects;

/**
 * A metadata-only view of a stream lifecycle record.
 *
 * <p>Catalog entries include in-progress creation and deletion records so control planes can
 * reconcile abandoned work without opening any data-plane resources. Completed deletion
 * tombstones are intentionally omitted from catalog listings.
 *
 * @param identifier the stream identifier
 * @param state the current lifecycle state
 * @param properties the persisted stream properties
 * @param metadataVersion the catalog version of this lifecycle record
 */
public record StreamCatalogEntry(
        StreamIdentifier identifier,
        LifecycleState state,
        Map<String, String> properties,
        long metadataVersion) {

    /** Creates a defensively copied catalog entry. */
    public StreamCatalogEntry {
        Objects.requireNonNull(identifier, "identifier");
        Objects.requireNonNull(state, "state");
        Objects.requireNonNull(properties, "properties");
        properties = Map.copyOf(properties);
    }
}
