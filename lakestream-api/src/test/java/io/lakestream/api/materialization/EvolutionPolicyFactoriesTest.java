/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.api.materialization;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Optional;
import org.junit.jupiter.api.Test;

class EvolutionPolicyFactoriesTest {

    @Test
    void testForIcebergDefaults() {
        EvolutionPolicy p = EvolutionPolicy.forIceberg();
        assertEquals(Optional.of(true), p.addColumn());
        assertEquals(Optional.of(true), p.addNullableColumn());
        assertEquals(Optional.of(true), p.widenType());
        assertEquals(Optional.of(false), p.dropColumn());
        assertEquals(Optional.of(false), p.narrowType());
        assertEquals(Optional.of(false), p.renameColumn());
        assertEquals(Optional.of(false), p.reorderColumns());
        assertEquals(Optional.of(false), p.nullabilityRelax());
        assertEquals(Optional.of(false), p.nullabilityTighten());
    }

    @Test
    void testForDeltaDefaults() {
        EvolutionPolicy p = EvolutionPolicy.forDelta();
        assertEquals(Optional.of(true), p.addColumn());
        assertEquals(Optional.of(true), p.addNullableColumn());
        assertEquals(Optional.of(true), p.widenType());
        assertEquals(Optional.of(false), p.dropColumn());
        assertEquals(Optional.of(false), p.narrowType());
        assertEquals(Optional.of(false), p.renameColumn());
        assertEquals(Optional.of(false), p.reorderColumns());
        assertEquals(Optional.of(false), p.nullabilityRelax());
        assertEquals(Optional.of(false), p.nullabilityTighten());
    }

    @Test
    void testForClickHouseDefaults() {
        EvolutionPolicy p = EvolutionPolicy.forClickHouse();
        assertEquals(Optional.of(true), p.addColumn());
        assertEquals(Optional.of(true), p.addNullableColumn());
        assertEquals(Optional.of(false), p.dropColumn());
        assertEquals(Optional.of(false), p.widenType());
        assertEquals(Optional.of(false), p.narrowType());
        assertEquals(Optional.of(false), p.renameColumn());
        assertEquals(Optional.of(false), p.reorderColumns());
        assertEquals(Optional.of(false), p.nullabilityRelax());
        assertEquals(Optional.of(false), p.nullabilityTighten());
    }

    @Test
    void testIcebergAndDeltaShareSameDefaults() {
        assertEquals(EvolutionPolicy.forIceberg(), EvolutionPolicy.forDelta());
    }
}
