/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.api;

/**
 * Abstract position within a stream — created by and interpreted by the layout.
 *
 * <p>Different layouts resolve positions differently:
 * <ul>
 *   <li><b>INDEXED</b>: (LogId, offset) — specific partition + offset</li>
 *   <li><b>RANGE split/merge</b>: layout-specific resolution (may span logs)</li>
 * </ul>
 *
 * <p>Positions are created via {@link StreamLayout#position(LogId, long)} and
 * consumed by {@link Stream#softTrim(StreamPosition)} and
 * {@link Stream#hardTrim(StreamPosition)}.
 *
 * @see StreamLayout#position(LogId, long)
 */
public interface StreamPosition {
}
