/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.api;

/**
 * Abstract position within a stream layout — created by and interpreted by that layout.
 *
 * <p>Different layouts resolve positions differently:
 * <ul>
 *   <li><b>INDEXED</b>: (LogId, offset) — specific partition + offset</li>
 *   <li><b>RANGE split/merge</b>: layout-specific resolution (may span logs)</li>
 * </ul>
 *
 * <p>Positions are created via {@link StreamLayout#position(LogId, long)}. A layout is available
 * from {@link StreamMetadata#layout()} or {@link StreamCatalog#getLayout(StreamIdentifier)};
 * per-log data-plane operations use a {@link Log} opened through
 * {@link StreamCatalog#openLog(StreamIdentifier, LogId)}.
 *
 * @see StreamLayout#position(LogId, long)
 */
public interface StreamPosition {
}
