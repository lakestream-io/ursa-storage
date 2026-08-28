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

class EvolutionPolicyTest {

    @Test
    void testConstruction() {
        EvolutionPolicy p = new EvolutionPolicy(
                Optional.of(true),
                Optional.of(true),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty());
        assertEquals(Optional.of(true), p.addColumn());
        assertEquals(Optional.empty(), p.dropColumn());
    }

    @Test
    void testEqualsHashCode() {
        EvolutionPolicy a = EvolutionPolicy.forIceberg();
        EvolutionPolicy b = EvolutionPolicy.forDelta();
        // Iceberg and Delta share defaults — they should be equal.
        assertEquals(a, b);
        assertEquals(a.hashCode(), b.hashCode());
        assertNotEquals(a, EvolutionPolicy.forClickHouse());
    }

    @Test
    void testRejectsNullOptionals() {
        assertThrows(NullPointerException.class, () -> new EvolutionPolicy(
                null,
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty()));
        assertThrows(NullPointerException.class, () -> new EvolutionPolicy(
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                null));
    }
}
