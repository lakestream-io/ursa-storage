/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
/**
 * Lakehouse-side wiring for the table materialization serde framework. The framework lives in
 * {@code io.lakestream.ursa.materialization.serde}; this package contains the bridge classes
 * and target-format encoders that depend on lakehouse internals (Delta, Iceberg, Parquet).
 */
package io.lakestream.ursa.lakehouse.v2.serde;
