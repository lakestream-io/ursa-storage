/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
/**
 * Lakestream implementations — catalog paths, stream layouts, writer, and reader.
 *
 * <p>This package contains implementations of the Lakestream API interfaces:
 * <ul>
 *   <li>{@link io.lakestream.ursa.lakestream.impl.DefaultCatalogPaths} — configurable Oxia paths</li>
 *   <li>{@link io.lakestream.ursa.lakestream.impl.DefaultCatalogPaths} — configurable prefix paths</li>
 *   <li>{@link io.lakestream.ursa.lakestream.impl.IndexedLayout} — fixed-count indexed partitioning</li>
 *   <li>{@link io.lakestream.ursa.lakestream.impl.StreamWriterImpl} — routing + append</li>
 *   <li>{@link io.lakestream.ursa.lakestream.impl.StreamReaderImpl} — layout + read</li>
 * </ul>
 */
package io.lakestream.ursa.lakestream.impl;
