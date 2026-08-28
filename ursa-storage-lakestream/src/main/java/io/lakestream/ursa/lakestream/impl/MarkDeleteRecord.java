/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.lakestream.impl;

import java.util.Map;

public record MarkDeleteRecord(
        long offset,
        Map<String, Long> properties
) {
}
