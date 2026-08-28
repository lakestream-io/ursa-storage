/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.storage.impl;

import io.oxia.client.api.PutResult;
import java.util.Objects;

/**
 * Represents the outcome of an index write for a single stream.
 */
public interface StreamIndexResult {
    record Success(PutResult putResult) implements StreamIndexResult {
        public Success {
            Objects.requireNonNull(putResult, "putResult must not be null");
        }
    }

    record Fenced() implements StreamIndexResult {}

    record Failed(Throwable cause) implements StreamIndexResult {
        public Failed {
            Objects.requireNonNull(cause, "cause must not be null");
        }
    }
}
