/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.storage.impl;

import io.lakestream.api.LogState;
import io.lakestream.api.exception.LogFencedException;
import java.util.Optional;

/**
 * Converts terminal {@link LogState} values to storage-level exceptions.
 */
final class LogStateUtil {

    private LogStateUtil() {
    }

    static Optional<LogFencedException> toException(LogState state, long streamId) {
        if (state == LogState.FENCED) {
            return Optional.of(new LogFencedException("stream " + streamId + " is fenced"));
        }
        return Optional.empty();
    }
}
