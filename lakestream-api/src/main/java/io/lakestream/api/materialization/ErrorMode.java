/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.api.materialization;

/**
 * How the materializer reacts to per-record / per-batch errors.
 */
public enum ErrorMode {

    /** Suspend the materialization on error; require operator intervention. */
    SUSPEND,

    /** Skip the failed record(s) and continue. */
    SKIP,

    /** Log the failure and continue. */
    LOG
}
