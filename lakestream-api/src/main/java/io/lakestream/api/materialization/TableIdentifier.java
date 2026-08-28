/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.api.materialization;

import java.util.Objects;

/**
 * A (namespace, name) pair identifying a table within a {@link TableCatalog}.
 *
 * <p>The catalog itself is implicit — it is supplied by
 * {@link TableMaterializationPolicy#catalogRef()} on the owning policy.
 *
 * @param namespace the table namespace (non-null, non-empty)
 * @param name      the table name within the namespace (non-null, non-empty)
 */
public record TableIdentifier(String namespace, String name) {

    /** Canonical constructor: validates both components are non-null and non-empty. */
    public TableIdentifier {
        Objects.requireNonNull(namespace, "namespace");
        if (namespace.isEmpty()) {
            throw new IllegalArgumentException("namespace must not be empty");
        }
        Objects.requireNonNull(name, "name");
        if (name.isEmpty()) {
            throw new IllegalArgumentException("name must not be empty");
        }
    }
}
