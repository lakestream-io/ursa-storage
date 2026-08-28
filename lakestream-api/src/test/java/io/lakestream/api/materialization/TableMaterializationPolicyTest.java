/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.api.materialization;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.Test;

class TableMaterializationPolicyTest {

    private TableMaterializationPolicy sample() {
        return new TableMaterializationPolicy(
                Optional.of("iceberg-prod"),
                Optional.of(new TableNaming(Optional.of("warehouse"), "${stream.name}")),
                Optional.empty(),
                Optional.of(true),
                Optional.of(new FrameworkConf(
                        Optional.of(WriteMode.APPEND), Optional.empty(), Optional.empty(),
                        Optional.empty(), Optional.empty())),
                Optional.of(EvolutionPolicy.forIceberg()),
                Optional.of(List.of("id")),
                Optional.of(1L),
                Optional.empty(),
                Map.of("override.k", "v"));
    }

    @Test
    void testConstruction() {
        TableMaterializationPolicy p = sample();
        assertEquals(Optional.of("iceberg-prod"), p.catalogRef());
        assertEquals(Optional.of(true), p.enabled());
        assertEquals(Optional.of(1L), p.baseSchemaVersion());
        assertEquals(Map.of("override.k", "v"), p.connectionOverrides());
    }

    @Test
    void testListAndMapDefensivelyCopied() {
        List<String> pk = new ArrayList<>();
        pk.add("id");
        Map<String, String> conn = new HashMap<>();
        conn.put("k", "v");
        TableMaterializationPolicy p = new TableMaterializationPolicy(
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.of(pk),
                Optional.empty(),
                Optional.empty(),
                conn);
        pk.clear();
        conn.put("k", "v2");
        assertEquals(1, p.primaryKey().orElseThrow().size());
        assertEquals("v", p.connectionOverrides().get("k"));
        assertThrows(UnsupportedOperationException.class,
                () -> p.primaryKey().orElseThrow().add("x"));
        assertThrows(UnsupportedOperationException.class,
                () -> p.connectionOverrides().put("x", "y"));
    }

    @Test
    void testEqualsHashCode() {
        TableMaterializationPolicy a = sample();
        TableMaterializationPolicy b = sample();
        assertEquals(a, b);
        assertEquals(a.hashCode(), b.hashCode());

        TableMaterializationPolicy diff = new TableMaterializationPolicy(
                Optional.of("other"),
                a.tableNaming(),
                a.tableIdentifier(),
                a.enabled(),
                a.framework(),
                a.evolution(),
                a.primaryKey(),
                a.baseSchemaVersion(),
                a.table(),
                a.connectionOverrides());
        assertNotEquals(a, diff);
    }

    @Test
    void testRejectsNullOptionalFields() {
        assertThrows(NullPointerException.class, () -> new TableMaterializationPolicy(
                null,
                Optional.empty(), Optional.empty(), Optional.empty(),
                Optional.empty(), Optional.empty(), Optional.empty(),
                Optional.empty(), Optional.empty(), Map.of()));
        assertThrows(NullPointerException.class, () -> new TableMaterializationPolicy(
                Optional.empty(),
                null,
                Optional.empty(), Optional.empty(),
                Optional.empty(), Optional.empty(), Optional.empty(),
                Optional.empty(), Optional.empty(), Map.of()));
        assertThrows(NullPointerException.class, () -> new TableMaterializationPolicy(
                Optional.empty(), Optional.empty(), null, Optional.empty(),
                Optional.empty(), Optional.empty(), Optional.empty(),
                Optional.empty(), Optional.empty(), Map.of()));
        assertThrows(NullPointerException.class, () -> new TableMaterializationPolicy(
                Optional.empty(), Optional.empty(), Optional.empty(), null,
                Optional.empty(), Optional.empty(), Optional.empty(),
                Optional.empty(), Optional.empty(), Map.of()));
        assertThrows(NullPointerException.class, () -> new TableMaterializationPolicy(
                Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty(),
                null,
                Optional.empty(), Optional.empty(),
                Optional.empty(), Optional.empty(), Map.of()));
        assertThrows(NullPointerException.class, () -> new TableMaterializationPolicy(
                Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty(),
                Optional.empty(), null, Optional.empty(),
                Optional.empty(), Optional.empty(), Map.of()));
        assertThrows(NullPointerException.class, () -> new TableMaterializationPolicy(
                Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty(),
                Optional.empty(), Optional.empty(), null,
                Optional.empty(), Optional.empty(), Map.of()));
        assertThrows(NullPointerException.class, () -> new TableMaterializationPolicy(
                Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty(),
                Optional.empty(), Optional.empty(), Optional.empty(),
                null,
                Optional.empty(), Map.of()));
        assertThrows(NullPointerException.class, () -> new TableMaterializationPolicy(
                Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty(),
                Optional.empty(), Optional.empty(), Optional.empty(),
                Optional.empty(), null, Map.of()));
        assertThrows(NullPointerException.class, () -> new TableMaterializationPolicy(
                Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty(),
                Optional.empty(), Optional.empty(), Optional.empty(),
                Optional.empty(), Optional.empty(), null));
    }

    @Test
    void testToStringIsNonEmpty() {
        assertTrue(sample().toString().contains("iceberg-prod"));
    }
}
