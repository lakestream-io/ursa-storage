/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
/**
 * Types for the stream-to-table materialization framework.
 *
 * <p>See {@link io.lakestream.api.materialization.TableMaterializationPolicy}
 * for the per-stream / per-namespace policy record and
 * {@link io.lakestream.api.materialization.TableCatalog} for the
 * registered, named table store entity that policies reference by name.
 *
 * <p>This package contains pure data types only (records and enums); no
 * resolution logic, persistence, or runtime behaviour lives here.
 */
package io.lakestream.api.materialization;
