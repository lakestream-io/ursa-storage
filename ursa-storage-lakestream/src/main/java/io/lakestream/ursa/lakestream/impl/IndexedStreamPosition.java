/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.lakestream.impl;

import io.lakestream.api.LogId;
import io.lakestream.api.StreamPosition;

/**
 * Stream position for indexed/single-log layouts.
 *
 * @param logId the log within the stream
 * @param offset the offset within the log
 */
public record IndexedStreamPosition(LogId logId, long offset) implements StreamPosition {
}
