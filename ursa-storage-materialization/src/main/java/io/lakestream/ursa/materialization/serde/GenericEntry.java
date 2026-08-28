/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.materialization.serde;

import io.lakestream.ursa.storage.Entry;
import java.util.Optional;

/** A storage entry and its optional, protocol-neutral materialization metadata. */
public record GenericEntry(Entry entry, Optional<LakehouseEntryMetadata> metadata) {

    public GenericEntry(Entry entry) {
        this(entry, Optional.empty());
    }
}
