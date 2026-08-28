/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.api.materialization;

import java.util.Objects;
import java.util.Optional;

/**
 * Framework-level (engine-agnostic) configuration for a materialization.
 *
 * <p>Every field is individually overridable across the namespace → stream
 * policy stack. Defaults are applied at resolution time, not here.
 *
 * @param writeMode      write strategy (append/upsert/cdc)
 * @param startPosition  where to begin reading the source stream
 * @param paused         whether materialization is paused
 * @param errorHandling  error-handling configuration
 * @param commit         commit/retry tuning
 */
public record FrameworkConf(
        Optional<WriteMode> writeMode,
        Optional<StartPosition> startPosition,
        Optional<Boolean> paused,
        Optional<ErrorHandling> errorHandling,
        Optional<CommitConfig> commit) {

    /** Canonical constructor: validates all Optional fields are non-null. */
    public FrameworkConf {
        Objects.requireNonNull(writeMode, "writeMode cannot be null; use Optional.empty()");
        Objects.requireNonNull(startPosition, "startPosition cannot be null; use Optional.empty()");
        Objects.requireNonNull(paused, "paused cannot be null; use Optional.empty()");
        Objects.requireNonNull(errorHandling, "errorHandling cannot be null; use Optional.empty()");
        Objects.requireNonNull(commit, "commit cannot be null; use Optional.empty()");
    }
}
