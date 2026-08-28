/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.api.materialization;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.Map;
import org.junit.jupiter.api.Test;

class ResolvedMaterializationTest {

    private TableCatalog catalog() {
        return new TableCatalog("iceberg-prod", TableCatalogType.ICEBERG, Map.of(), Map.of());
    }

    @Test
    void testConstruction() {
        TableIdentifier id = new TableIdentifier("warehouse", "orders");
        TableMaterializationPolicy p = TableMaterializationPolicy.empty();
        ResolvedMaterialization r = new ResolvedMaterialization(catalog(), id, p);
        assertEquals(catalog(), r.catalog());
        assertEquals(id, r.tableIdentifier());
        assertEquals(p, r.effectivePolicy());
    }

    @Test
    void testEqualsHashCode() {
        TableIdentifier id = new TableIdentifier("ns", "t");
        TableMaterializationPolicy p = TableMaterializationPolicy.empty();
        ResolvedMaterialization a = new ResolvedMaterialization(catalog(), id, p);
        ResolvedMaterialization b = new ResolvedMaterialization(catalog(), id, p);
        ResolvedMaterialization c = new ResolvedMaterialization(
                catalog(),
                new TableIdentifier("ns", "other"),
                p);
        assertEquals(a, b);
        assertEquals(a.hashCode(), b.hashCode());
        assertNotEquals(a, c);
    }

    @Test
    void testRejectsNulls() {
        TableIdentifier id = new TableIdentifier("ns", "t");
        TableMaterializationPolicy p = TableMaterializationPolicy.empty();
        assertThrows(NullPointerException.class, () -> new ResolvedMaterialization(null, id, p));
        assertThrows(NullPointerException.class, () -> new ResolvedMaterialization(catalog(), null, p));
        assertThrows(NullPointerException.class, () -> new ResolvedMaterialization(catalog(), id, null));
    }
}
