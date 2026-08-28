/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.utils;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import org.junit.jupiter.api.Test;

class FutureUtilsTest {

    @Test
    void waitsForEveryFuture() {
        CompletableFuture<Void> first = new CompletableFuture<>();
        CompletableFuture<Void> second = new CompletableFuture<>();
        CompletableFuture<Void> combined = FutureUtils.waitForAll(List.of(first, second));

        first.complete(null);
        assertFalse(combined.isDone());
        second.complete(null);

        combined.join();
    }

    @Test
    void unwrapsNestedFutureWrappers() {
        IllegalStateException cause = new IllegalStateException("failed");
        Throwable wrapped = new CompletionException(new ExecutionException(cause));

        assertEquals(cause, FutureUtils.unwrapCompletionException(wrapped));
    }
}
