/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.api.materialization;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Map;
import org.junit.jupiter.api.Test;

class TableMaterializationPolicyEmptyTest {

    @Test
    void testEmptyFactoryReturnsAllEmpty() {
        TableMaterializationPolicy p = TableMaterializationPolicy.empty();
        assertTrue(p.catalogRef().isEmpty());
        assertTrue(p.tableNaming().isEmpty());
        assertTrue(p.tableIdentifier().isEmpty());
        assertTrue(p.enabled().isEmpty());
        assertTrue(p.framework().isEmpty());
        assertTrue(p.evolution().isEmpty());
        assertTrue(p.primaryKey().isEmpty());
        assertTrue(p.baseSchemaVersion().isEmpty());
        assertTrue(p.table().isEmpty());
        assertEquals(Map.of(), p.connectionOverrides());
    }

    @Test
    void testEmptyIsIdempotent() {
        assertEquals(TableMaterializationPolicy.empty(), TableMaterializationPolicy.empty());
    }
}
