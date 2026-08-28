/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.api;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

class RoutingKeyTest {

    @Test
    void testOfIndex() {
        RoutingKey key = RoutingKey.ofIndex(0);
        assertTrue(key.indexHint().isPresent());
        assertEquals(0, key.indexHint().getAsInt());
    }

    @Test
    void testRoundRobin() {
        RoutingKey key = RoutingKey.roundRobin();
        assertTrue(key.indexHint().isEmpty());
    }
}
