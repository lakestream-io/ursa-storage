/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.api.materialization;

/**
 * Runtime state of a stream-to-table materialization.
 */
public enum MaterializationState {

    /** Configured but not yet running. */
    PENDING,

    /** Running and writing to the destination table. */
    RUNNING,

    /** Running with reduced functionality (e.g., retries elevated, throughput limited). */
    DEGRADED,

    /** Suspended due to repeated failures, awaiting operator intervention. */
    SUSPENDED,

    /** Paused explicitly by user/operator. */
    PAUSED
}
