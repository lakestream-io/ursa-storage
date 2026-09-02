/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.api.materialization;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.lakestream.api.StreamIdentifier;
import java.util.Map;
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

    @Test
    void interpolatesStreamProperties() {
        TableNaming naming = new TableNaming(Optional.empty(), "${stream.property.lakestream.kafka.topic.name}_v1");
        TableIdentifier table = naming.toTableIdentifier(
            StreamIdentifier.of("default", "orders-topic-id-abc"),
            Map.of("lakestream.kafka.topic.name", "orders"));
        assertEquals("orders_v1", table.name());
        assertEquals("default", table.namespace());
    }

    @Test
    void missingPropertyIsRejected() {
        TableNaming naming = new TableNaming(Optional.empty(), "${stream.property.missing}");
        assertThrows(IllegalArgumentException.class,
            () -> naming.toTableIdentifier(StreamIdentifier.of("ns", "s"), Map.of()));
    }

    @Test
    void singleArgumentOverloadRejectsPropertyVariables() {
        TableNaming naming = new TableNaming(Optional.empty(), "${stream.property.x}");
        assertThrows(IllegalArgumentException.class,
            () -> naming.toTableIdentifier(StreamIdentifier.of("ns", "s")));
    }
}
