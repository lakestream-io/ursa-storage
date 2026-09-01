/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.storage;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.junit.jupiter.api.Test;

class OwnedResultFuturesTest {

    @Test
    void sourceControlledCompletionCannotBeForcedByObserver() throws Exception {
        CompletableFuture<String> source = new CompletableFuture<>();
        CompletableFuture<String> exposed =
            OwnedResultFutures.nonCancellableCompletion(source);

        assertFalse(exposed.cancel(false));
        assertFalse(exposed.complete("observer value"));
        assertFalse(exposed.completeExceptionally(
            new IllegalStateException("observer failure")));
        assertThrows(UnsupportedOperationException.class,
            () -> exposed.obtrudeValue("observer value"));
        assertThrows(UnsupportedOperationException.class,
            () -> exposed.obtrudeException(
                new IllegalStateException("observer failure")));
        assertThrows(UnsupportedOperationException.class,
            () -> exposed.completeAsync(() -> "observer value"));

        exposed.orTimeout(1, TimeUnit.MILLISECONDS);
        assertThrows(TimeoutException.class,
            () -> exposed.get(50, TimeUnit.MILLISECONDS));
        assertFalse(exposed.isDone());

        source.complete("source value");

        assertEquals("source value", exposed.join());
    }
}
