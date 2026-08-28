/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.api.materialization;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

class TableCatalogTest {

    @Test
    void testConstructionAndAccessors() {
        TableCatalog c = new TableCatalog(
                "iceberg-prod",
                TableCatalogType.ICEBERG,
                Map.of("uri", "https://rest"),
                Map.of("target-file-size-bytes", "134217728"));
        assertEquals("iceberg-prod", c.name());
        assertEquals(TableCatalogType.ICEBERG, c.type());
        assertEquals("https://rest", c.connection().get("uri"));
        assertEquals("134217728", c.properties().get("target-file-size-bytes"));
    }

    @Test
    void testMapsAreDefensivelyCopied() {
        Map<String, String> conn = new HashMap<>();
        conn.put("uri", "https://a");
        Map<String, String> props = new HashMap<>();
        props.put("k", "v");
        TableCatalog c = new TableCatalog("name", TableCatalogType.DELTA, conn, props);

        conn.put("uri", "https://b");
        props.put("k", "v2");

        assertEquals("https://a", c.connection().get("uri"));
        assertEquals("v", c.properties().get("k"));
        assertThrows(UnsupportedOperationException.class, () -> c.connection().put("x", "y"));
        assertThrows(UnsupportedOperationException.class, () -> c.properties().put("x", "y"));
    }

    @Test
    void testRejectsNullFields() {
        assertThrows(NullPointerException.class, () -> new TableCatalog(
                null, TableCatalogType.ICEBERG, Map.of(), Map.of()));
        assertThrows(NullPointerException.class, () -> new TableCatalog(
                "n", null, Map.of(), Map.of()));
        assertThrows(NullPointerException.class, () -> new TableCatalog(
                "n", TableCatalogType.ICEBERG, null, Map.of()));
        assertThrows(NullPointerException.class, () -> new TableCatalog(
                "n", TableCatalogType.ICEBERG, Map.of(), null));
    }

    @Test
    void testRejectsEmptyName() {
        assertThrows(IllegalArgumentException.class, () -> new TableCatalog(
                "", TableCatalogType.ICEBERG, Map.of(), Map.of()));
    }

    @Test
    void testEqualsHashCodeAndToString() {
        TableCatalog a = new TableCatalog("n", TableCatalogType.DELTA, Map.of("u", "x"), Map.of());
        TableCatalog b = new TableCatalog("n", TableCatalogType.DELTA, Map.of("u", "x"), Map.of());
        TableCatalog c = new TableCatalog("n", TableCatalogType.DELTA, Map.of("u", "y"), Map.of());
        assertEquals(a, b);
        assertEquals(a.hashCode(), b.hashCode());
        assertNotEquals(a, c);
        assertNotNull(a.toString());
        assertTrue(a.toString().contains("n"));
    }
}
