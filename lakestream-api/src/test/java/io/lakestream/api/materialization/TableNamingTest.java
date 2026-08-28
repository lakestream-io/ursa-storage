/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.api.materialization;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Optional;
import org.junit.jupiter.api.Test;

class TableNamingTest {

    @Test
    void testConstruction() {
        TableNaming n = new TableNaming(Optional.of("warehouse"), "${stream.name}");
        assertEquals(Optional.of("warehouse"), n.tableNamespacePrefix());
        assertEquals("${stream.name}", n.tableNameTemplate());
    }

    @Test
    void testEmptyPrefixIsAllowed() {
        TableNaming n = new TableNaming(Optional.empty(), "t");
        assertTrue(n.tableNamespacePrefix().isEmpty());
    }

    @Test
    void testEqualsHashCode() {
        TableNaming a = new TableNaming(Optional.of("w"), "t");
        TableNaming b = new TableNaming(Optional.of("w"), "t");
        TableNaming c = new TableNaming(Optional.of("w"), "t2");
        assertEquals(a, b);
        assertEquals(a.hashCode(), b.hashCode());
        assertNotEquals(a, c);
    }

    @Test
    void testRejectsNullPrefixOptional() {
        assertThrows(NullPointerException.class, () -> new TableNaming(null, "t"));
    }

    @Test
    void testRejectsNullTemplate() {
        assertThrows(NullPointerException.class, () -> new TableNaming(Optional.empty(), null));
    }

    @Test
    void testRejectsEmptyTemplate() {
        assertThrows(IllegalArgumentException.class, () -> new TableNaming(Optional.empty(), ""));
    }
}
