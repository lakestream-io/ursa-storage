/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.api.materialization;

import java.util.Objects;
import java.util.Optional;

/**
 * Error-handling configuration for a materializer.
 *
 * @param mode     how to handle failures (required; default applied at resolve time)
 * @param dlqTopic optional dead-letter topic for failed records
 */
public record ErrorHandling(ErrorMode mode, Optional<String> dlqTopic) {

    /** Canonical constructor: validates {@code mode} and non-null Optional fields. */
    public ErrorHandling {
        Objects.requireNonNull(mode, "mode");
        Objects.requireNonNull(dlqTopic, "dlqTopic cannot be null; use Optional.empty()");
    }
}
