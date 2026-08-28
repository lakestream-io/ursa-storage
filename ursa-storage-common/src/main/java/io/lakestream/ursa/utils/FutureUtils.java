/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.utils;

import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;

/**
 * Utilities for composing and inspecting standard Java futures.
 */
public final class FutureUtils {

    private FutureUtils() {
    }

    public static CompletableFuture<Void> waitForAll(
            Collection<? extends CompletableFuture<?>> futures) {
        return CompletableFuture.allOf(futures.toArray(CompletableFuture[]::new));
    }

    public static Throwable unwrapCompletionException(Throwable failure) {
        Throwable current = failure;
        while ((current instanceof CompletionException || current instanceof ExecutionException)
                && current.getCause() != null) {
            current = current.getCause();
        }
        return current;
    }
}
