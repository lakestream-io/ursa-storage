/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.api.materialization;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

class TableIdentifierTest {

    @Test
    void testConstructionAndAccessors() {
        TableIdentifier id = new TableIdentifier("warehouse", "orders");
        assertEquals("warehouse", id.namespace());
        assertEquals("orders", id.name());
    }

    @Test
    void testEqualsHashCodeAndToString() {
        TableIdentifier a = new TableIdentifier("ns", "t");
        TableIdentifier b = new TableIdentifier("ns", "t");
        TableIdentifier c = new TableIdentifier("ns", "other");
        assertEquals(a, b);
        assertEquals(a.hashCode(), b.hashCode());
        assertNotEquals(a, c);
        assertNotNull(a.toString());
    }

    @Test
    void testRejectsNulls() {
        assertThrows(NullPointerException.class, () -> new TableIdentifier(null, "t"));
        assertThrows(NullPointerException.class, () -> new TableIdentifier("ns", null));
    }

    @Test
    void testRejectsEmpty() {
        assertThrows(IllegalArgumentException.class, () -> new TableIdentifier("", "t"));
        assertThrows(IllegalArgumentException.class, () -> new TableIdentifier("ns", ""));
    }
}
