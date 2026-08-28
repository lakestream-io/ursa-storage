/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.api;

/**
 * Lifecycle state of a stream.
 */
public enum LifecycleState {

    /** Stream is being created. */
    CREATING,

    /** Stream is active and accepting reads/writes. */
    ACTIVE,

    /** Stream is sealed — no more writes accepted, reads still work. */
    SEALED,

    /** Stream is being truncated. */
    TRUNCATING,

    /** Stream is being deleted. */
    DELETING
}
