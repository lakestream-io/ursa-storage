/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.materialization.serde;

import java.util.Optional;

public record MaterializationRecord<T>(T record, Optional<LakehouseEntryMetadata> metadata) {

    public MaterializationRecord(T record, LakehouseEntryMetadata metadata) {
        this(record, Optional.of(metadata));
    }

    @Override
    public MaterializationRecord<T> clone() throws CloneNotSupportedException {
        Optional<LakehouseEntryMetadata> meta = Optional.empty();
        if (this.metadata.isPresent()) {
            meta = Optional.of(this.metadata.get().clone());
        }
        return new MaterializationRecord<T>(this.record(), meta);
    }
}
