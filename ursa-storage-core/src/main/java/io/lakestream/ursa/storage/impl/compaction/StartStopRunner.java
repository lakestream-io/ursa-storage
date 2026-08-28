/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.storage.impl.compaction;

/**
 * Minimal start/stop lifecycle contract for the long-running runners that the
 * {@link CompactionStorageBindings} layer exposes to the orchestrator.
 *
 * <p>Introduced in T9 so that the orchestrator (T10) can drive
 * {@link #start()} / {@link #stop()} on runners without importing
 * lakehouse-specific runner classes. Concrete implementations (e.g. lakehouse
 * publish / commit / cleaner runners) live in their respective integration
 * modules and implement this interface.
 */
public interface StartStopRunner {

    /** Starts the runner. Must be safe to call after a prior {@link #stop()}. */
    void start();

    /** Stops the runner. Must be idempotent and safe to call multiple times. */
    void stop();
}
