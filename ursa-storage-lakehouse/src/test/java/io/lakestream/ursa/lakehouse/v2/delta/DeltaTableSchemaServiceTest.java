/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.lakehouse.v2.delta;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.delta.kernel.Snapshot;
import io.delta.kernel.types.LongType;
import io.delta.kernel.types.StringType;
import io.delta.kernel.types.StructField;
import io.delta.kernel.types.StructType;
import io.lakestream.ursa.lakehouse.delta.UCManagedTable;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class DeltaTableSchemaServiceTest {

    @Mock
    private UCManagedTable managedTable;

    @Test
    void testManagedTableCreatesWithoutSchemaEvolution() throws Exception {
        when(managedTable.tableExists()).thenReturn(false);

        DeltaTableSchemaService service = new DeltaTableSchemaService(managedTable);
        TreeMap<Long, StructType> schemas = new TreeMap<>();
        schemas.put(1L, simpleSchema("name"));

        Set<Long> result = service.evolveTableSchema(schemas);

        assertEquals(Set.of(1L), result);
        verify(managedTable).createDeltaTable(1L, schemas.get(1L));
        verify(managedTable, never()).evolveSchemaWithVersion(1L, schemas.get(1L));
    }

    @Test
    void testManagedTableThrowsOnlyWhenSchemaEvolutionIsNeeded() throws Exception {
        when(managedTable.tableExists()).thenReturn(true);
        StructType schemaV2 = simpleSchema("name");
        org.mockito.Mockito.doThrow(new UnsupportedOperationException(
            "The UC Managed Table not supported schema evolution now."))
            .when(managedTable).evolveSchemaWithVersion(2L, schemaV2);

        DeltaTableSchemaService service = new DeltaTableSchemaService(managedTable);
        TreeMap<Long, StructType> schemas = new TreeMap<>(Map.of(2L, schemaV2));

        UnsupportedOperationException exception =
            assertThrows(UnsupportedOperationException.class, () -> service.evolveTableSchema(schemas));

        assertEquals("The UC Managed Table not supported schema evolution now.", exception.getMessage());
        verify(managedTable, never()).createDeltaTable(2L, schemaV2);
        verify(managedTable).evolveSchemaWithVersion(2L, schemaV2);
    }

    @Test
    void testGetTableSchemaReturnsLatestTableSchema() throws Exception {
        when(managedTable.getSchemaMapping()).thenReturn(Set.of(2L));
        Snapshot snapshot = org.mockito.Mockito.mock(Snapshot.class);
        StructType tableSchema = simpleSchema("email");
        when(snapshot.getSchema()).thenReturn(tableSchema);
        when(managedTable.getLatestSnapshot()).thenReturn(snapshot);

        DeltaTableSchemaService service = new DeltaTableSchemaService(managedTable);

        assertEquals(tableSchema, service.getTableSchema(2L));
        assertNull(service.getTableSchema(1L));
    }

    private StructType simpleSchema(String fieldName) {
        return new StructType()
            .add(new StructField("id", LongType.LONG, false))
            .add(new StructField(fieldName, StringType.STRING, true));
    }
}
