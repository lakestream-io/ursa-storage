/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.api.materialization;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.Optional;
import org.junit.jupiter.api.Test;

class CommitConfigTest {

    @Test
    void testConstruction() {
        CommitConfig c = new CommitConfig(Optional.of(3), Optional.of(1_000L), Optional.of(500));
        assertEquals(Optional.of(3), c.maxRetries());
        assertEquals(Optional.of(1_000L), c.retryDelayMs());
        assertEquals(Optional.of(500), c.batchSize());
    }

    @Test
    void testAllEmptyAllowed() {
        CommitConfig c = new CommitConfig(Optional.empty(), Optional.empty(), Optional.empty());
        assertEquals(Optional.empty(), c.maxRetries());
    }

    @Test
    void testEqualsHashCode() {
        CommitConfig a = new CommitConfig(Optional.of(1), Optional.of(2L), Optional.of(3));
        CommitConfig b = new CommitConfig(Optional.of(1), Optional.of(2L), Optional.of(3));
        CommitConfig c = new CommitConfig(Optional.of(1), Optional.of(2L), Optional.of(4));
        assertEquals(a, b);
        assertEquals(a.hashCode(), b.hashCode());
        assertNotEquals(a, c);
    }

    @Test
    void testRejectsNullOptionals() {
        assertThrows(NullPointerException.class, () -> new CommitConfig(
                null, Optional.empty(), Optional.empty()));
        assertThrows(NullPointerException.class, () -> new CommitConfig(
                Optional.empty(), null, Optional.empty()));
        assertThrows(NullPointerException.class, () -> new CommitConfig(
                Optional.empty(), Optional.empty(), null));
    }
}
