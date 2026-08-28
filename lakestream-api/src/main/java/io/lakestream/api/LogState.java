/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.api;

/**
 * The operational state of a log (stream).
 */
public enum LogState {

    /** Any operation on this log is allowed. */
    NORMAL,
    /**
     * In this state, any append operation on the log will fail.
     */
    FENCED
}
