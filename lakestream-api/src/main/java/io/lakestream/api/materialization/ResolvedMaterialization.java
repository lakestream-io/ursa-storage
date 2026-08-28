/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.api.materialization;

import java.util.Objects;

/**
 * Result of policy resolution: a fully-materialised view that pairs the
 * effective {@link TableMaterializationPolicy} with the {@link TableCatalog}
 * it references and the {@link TableIdentifier} the stream writes to.
 *
 * <p>The catalog has already been looked up by name from
 * {@link TableMaterializationPolicy#catalogRef()}; consumers do not need to
 * perform another lookup.
 *
 * @param catalog          the resolved table catalog (non-null)
 * @param tableIdentifier  the resolved table identity (non-null)
 * @param effectivePolicy  the merged effective policy (non-null)
 */
public record ResolvedMaterialization(
        TableCatalog catalog,
        TableIdentifier tableIdentifier,
        TableMaterializationPolicy effectivePolicy) {

    /** Canonical constructor: validates all three fields are non-null. */
    public ResolvedMaterialization {
        Objects.requireNonNull(catalog, "catalog");
        Objects.requireNonNull(tableIdentifier, "tableIdentifier");
        Objects.requireNonNull(effectivePolicy, "effectivePolicy");
    }
}
