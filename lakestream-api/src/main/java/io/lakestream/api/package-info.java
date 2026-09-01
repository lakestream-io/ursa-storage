/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
/**
 * Lakestream API — clean interfaces for the two-level stream storage architecture.
 *
 * <h2>Level 1: Log Storage</h2>
 * <p>Single-log operations via {@link io.lakestream.api.LogStorage},
 * identified by {@link io.lakestream.api.LogId}. Each operation
 * works on entries ({@link io.lakestream.api.LogEntry}) containing
 * one or more records.</p>
 *
 * <h2>Level 2: Stream Management</h2>
 * <p>Multi-log stream operations via {@link io.lakestream.api.StreamCatalog},
 * immutable {@link io.lakestream.api.StreamMetadata} snapshots,
 * {@link io.lakestream.api.StreamLayout},
 * {@link io.lakestream.api.StreamWriter}, and
 * {@link io.lakestream.api.StreamReader}.</p>
 *
 * <p>A stream is identified by {@link io.lakestream.api.StreamIdentifier}
 * and composed of one or more logs via {@link io.lakestream.api.StreamLayout}.
 * There is no intermediate Partition type — the layout resolves directly from
 * routing key to {@link io.lakestream.api.LogId}. Metadata operations do not
 * acquire data-plane resources; logs, readers, and writers are opened explicitly
 * through the catalog.</p>
 *
 * @see io.lakestream.api.LogStorage
 * @see io.lakestream.api.StreamCatalog
 */
package io.lakestream.api;
