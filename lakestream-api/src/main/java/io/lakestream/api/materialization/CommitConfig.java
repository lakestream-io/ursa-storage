/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.api.materialization;

import java.util.Objects;
import java.util.Optional;

/**
 * Commit/retry tuning knobs for a materializer.
 *
 * <p>All fields are individually overridable at namespace and stream layers;
 * default resolution lives in the resolution helper.
 *
 * @param maxRetries    maximum retry attempts for a failed commit
 * @param retryDelayMs  base retry delay in milliseconds
 * @param batchSize     records per commit batch
 */
public record CommitConfig(
        Optional<Integer> maxRetries,
        Optional<Long> retryDelayMs,
        Optional<Integer> batchSize) {

    /** Canonical constructor: validates that no Optional field is {@code null}. */
    public CommitConfig {
        Objects.requireNonNull(maxRetries, "maxRetries cannot be null; use Optional.empty()");
        Objects.requireNonNull(retryDelayMs, "retryDelayMs cannot be null; use Optional.empty()");
        Objects.requireNonNull(batchSize, "batchSize cannot be null; use Optional.empty()");
    }
}
