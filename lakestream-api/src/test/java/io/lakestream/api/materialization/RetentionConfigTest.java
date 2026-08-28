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

class RetentionConfigTest {

    @Test
    void testConstruction() {
        RetentionConfig r = new RetentionConfig(
                Optional.of(86_400_000L), Optional.of(7), Optional.of(30L));
        assertEquals(Optional.of(86_400_000L), r.snapshotRetentionMs());
        assertEquals(Optional.of(7), r.maxSnapshots());
        assertEquals(Optional.of(30L), r.rowRetentionMs());
    }

    @Test
    void testEqualsHashCode() {
        RetentionConfig a = new RetentionConfig(Optional.of(1L), Optional.empty(), Optional.empty());
        RetentionConfig b = new RetentionConfig(Optional.of(1L), Optional.empty(), Optional.empty());
        RetentionConfig c = new RetentionConfig(Optional.of(2L), Optional.empty(), Optional.empty());
        assertEquals(a, b);
        assertEquals(a.hashCode(), b.hashCode());
        assertNotEquals(a, c);
    }

    @Test
    void testRejectsNullOptionals() {
        assertThrows(NullPointerException.class, () -> new RetentionConfig(
                null, Optional.empty(), Optional.empty()));
        assertThrows(NullPointerException.class, () -> new RetentionConfig(
                Optional.empty(), null, Optional.empty()));
        assertThrows(NullPointerException.class, () -> new RetentionConfig(
                Optional.empty(), Optional.empty(), null));
    }
}
