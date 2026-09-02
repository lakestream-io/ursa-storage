/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.lakestream.impl;

import java.util.concurrent.CompletionException;

/** Unwrapping for failures the catalog inspects before deciding what to compensate. */
final class CompletionFailures {

    private CompletionFailures() {
    }

    /**
     * Strips the {@link CompletionException} a future stage wraps its cause in, and nothing else.
     *
     * <p>Callers that want the original cause however deeply it is nested want
     * {@link io.lakestream.ursa.utils.FutureUtils#unwrapCompletionException} instead.
     */
    static Throwable unwrap(Throwable failure) {
        return failure instanceof CompletionException && failure.getCause() != null
            ? failure.getCause() : failure;
    }
}
