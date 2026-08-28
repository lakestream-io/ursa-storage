/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.storage;

import io.lakestream.api.EntryHeader;
import io.lakestream.api.Position;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.experimental.Accessors;

/**
 * Represents the Put Result information.
 */

@AllArgsConstructor
@Data
@Accessors(fluent = true)
public class AddResult {
    private EntryHeader header;
    private Position position;
    private boolean notifyCursors;
}
