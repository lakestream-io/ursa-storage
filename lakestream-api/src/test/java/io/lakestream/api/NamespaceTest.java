/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.api;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.lakestream.api.materialization.TableMaterializationPolicy;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.Test;

class NamespaceTest {

    @Test
    void testSingleArgConstructorDefaultsToEmptyMaterialization() {
        Namespace ns = new Namespace("ns");
        assertEquals("ns", ns.name());
        assertEquals(Map.of(), ns.properties());
        assertNotNull(ns.materialization());
        assertTrue(ns.materialization().isEmpty());
    }

    @Test
    void testTwoArgConstructorDefaultsToEmptyMaterialization() {
        Namespace ns = new Namespace("ns", Map.of("region", "us-east-1"));
        assertEquals("ns", ns.name());
        assertEquals("us-east-1", ns.properties().get("region"));
        assertNotNull(ns.materialization());
        assertTrue(ns.materialization().isEmpty());
    }

    @Test
    void testFullConstructorRoundTripsPolicy() {
        TableMaterializationPolicy policy = TableMaterializationPolicy.empty();
        Namespace ns = new Namespace("ns", Map.of(), Optional.of(policy));
        assertTrue(ns.materialization().isPresent());
        assertSame(policy, ns.materialization().get());
    }

    @Test
    void testRejectsNullMaterialization() {
        assertThrows(NullPointerException.class,
                () -> new Namespace("ns", Map.of(), null));
    }

    @Test
    void testRejectsNullName() {
        assertThrows(NullPointerException.class,
                () -> new Namespace(null, Map.of(), Optional.empty()));
    }

    @Test
    void testRejectsNullProperties() {
        assertThrows(NullPointerException.class,
                () -> new Namespace("ns", null, Optional.empty()));
    }

    @Test
    void testPropertiesAreDefensivelyCopied() {
        Map<String, String> props = new HashMap<>();
        props.put("k", "v");
        Namespace ns = new Namespace("ns", props, Optional.empty());

        props.put("k", "v2");

        assertEquals("v", ns.properties().get("k"));
        assertThrows(UnsupportedOperationException.class,
                () -> ns.properties().put("x", "y"));
    }
}
