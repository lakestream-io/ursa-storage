/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.materialization.serde;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.lakestream.ursa.exception.ExceptionCode;
import io.lakestream.ursa.exception.MessageSerDeException;
import io.lakestream.ursa.exception.RuntimeExceptionWithCode;
import io.lakestream.ursa.materialization.serde.kafka.KafkaSchemaService;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

@Tag("lakehouse")
class SchemaEvolutionManagerTest {

    private static final String TOPIC = "default/t";

    private SchemaKey key(long version) {
        return SchemaKey.builder().topicName(TOPIC).schemaVersion(version).build();
    }

    private EntryEncoderContext ctx(Optional<Long> base) {
        return EntryEncoderContext.builder().baseSchemaVersion(base).build();
    }

    @SuppressWarnings("unchecked")
    private TableSchemaService<Long, String> tableService(long latest) throws Exception {
        TableSchemaService<Long, String> svc = mock(TableSchemaService.class);
        // Default: no schema cached. Tests that exercise the post-evolution lookup chain a
        // second `thenReturn(...)` on the relevant version (so the FIRST call sees null and
        // the SECOND call sees the evolved value).
        when(svc.getLatestSchemaVersion()).thenReturn(latest);
        lenient().when(svc.evolveTableSchema(any()))
            .thenAnswer(inv -> ((Map<Long, String>) inv.getArgument(0)).keySet());
        return svc;
    }

    @SuppressWarnings("unchecked")
    private SchemaService<String> schemaService(Map<Long, String> schemas) throws Exception {
        SchemaService<String> svc = mock(SchemaService.class);
        when(svc.getSchemaWithVersions(eq(TOPIC), any(Long.class))).thenReturn(schemas);
        return svc;
    }

    @Test
    void baseUnsetUsesEarliestSchema() throws Exception {
        var table = tableService(-1L);
        Map<Long, String> topicSchemas = new LinkedHashMap<>();
        topicSchemas.put(0L, "s0");
        topicSchemas.put(1L, "s1");
        var schema = schemaService(topicSchemas);

        // null on first call, "evolved-1" on the post-evolution call
        when(table.getTableSchema(1L)).thenReturn(null).thenReturn("evolved-1");

        var result = SchemaEvolutionManager.<String, String>evolveSchema(
                table, schema, key(1L), (s, c) -> s, ctx(Optional.empty()));

        assertEquals("evolved-1", result);
        ArgumentCaptor<TreeMap<Long, String>> captor = ArgumentCaptor.forClass(TreeMap.class);
        verify(table).evolveTableSchema(captor.capture());
        assertEquals(Map.of(0L, "s0", 1L, "s1"), captor.getValue());
    }

    @Test
    void baseSetTableNotCreatedRejectsBelowBase() throws Exception {
        var table = tableService(-1L);
        var schema = schemaService(Map.of(0L, "s0", 1L, "s1", 2L, "s2"));

        var ex = assertThrows(RuntimeExceptionWithCode.class, () ->
                SchemaEvolutionManager.<String, String>evolveSchema(
                        table, schema, key(2L), (s, c) -> s, ctx(Optional.of(5L))));
        // Verify we hit the base-version rejection branch (not the pre-existing
        // `latest > schemaVersion` branch which uses a different message).
        var cause = (MessageSerDeException) ex.getCause();
        assertEquals(ExceptionCode.MESSAGE_SCHEMA_INCOMPATIBLE, cause.getExceptionCode());
        assertTrue(cause.getMessage().contains("below the configured base schema version"),
                "Expected message to mention base-version rejection, got: " + cause.getMessage());
        verify(table, org.mockito.Mockito.never()).evolveTableSchema(any());
    }

    @Test
    void baseSetTableNotCreatedFiltersAboveBase() throws Exception {
        var table = tableService(-1L);
        Map<Long, String> topicSchemas = new LinkedHashMap<>();
        topicSchemas.put(0L, "s0");
        topicSchemas.put(1L, "s1");
        topicSchemas.put(7L, "s7");
        topicSchemas.put(8L, "s8");
        var schema = schemaService(topicSchemas);

        when(table.getTableSchema(8L)).thenReturn(null).thenReturn("evolved-8");

        var result = SchemaEvolutionManager.<String, String>evolveSchema(
                table, schema, key(8L), (s, c) -> s, ctx(Optional.of(5L)));

        assertEquals("evolved-8", result);
        ArgumentCaptor<TreeMap<Long, String>> captor = ArgumentCaptor.forClass(TreeMap.class);
        verify(table).evolveTableSchema(captor.capture());
        // smallest remaining version (7) becomes the table base; v0 and v1 filtered out
        assertEquals(Map.of(7L, "s7", 8L, "s8"), captor.getValue());
    }

    @Test
    void baseSetTableAlreadyCreatedIgnoresConfig() throws Exception {
        // table latest = 4 (already created at v0 in a prior session, evolved to v4).
        // Customer now sets base=5. Incoming v=6 must NOT be rejected: config is ignored, evolution proceeds.
        var table = tableService(4L);
        Map<Long, String> topicSchemas = new LinkedHashMap<>();
        for (long v = 0; v <= 6; v++) {
            topicSchemas.put(v, "s" + v);
        }
        var schema = schemaService(topicSchemas);

        when(table.getTableSchema(6L)).thenReturn(null).thenReturn("evolved-6");

        var result = SchemaEvolutionManager.<String, String>evolveSchema(
                table, schema, key(6L), (s, c) -> s, ctx(Optional.of(5L)));

        assertEquals("evolved-6", result);
        ArgumentCaptor<TreeMap<Long, String>> captor = ArgumentCaptor.forClass(TreeMap.class);
        verify(table).evolveTableSchema(captor.capture());
        // Config ignored: full schema range used (smallest unevolved upward).
        // Asserting the FULL map ensures a buggy implementation that wrongly applied the
        // base filter (stripping 0L..4L) would fail this test.
        assertEquals(Map.of(0L, "s0", 1L, "s1", 2L, "s2", 3L, "s3", 4L, "s4", 5L, "s5", 6L, "s6"),
                captor.getValue());
    }

    @Test
    void primitiveSchemaExemptFromBaseCheck() throws Exception {
        var table = tableService(-1L);
        long primitiveSchemaVersion = KafkaSchemaService.PRIMITIVE_SCHEMA_ID;
        var schema = schemaService(Map.of(primitiveSchemaVersion, "primitive"));

        when(table.getTableSchema(primitiveSchemaVersion))
                .thenReturn(null).thenReturn("evolved-prim");

        var result = SchemaEvolutionManager.<String, String>evolveSchema(
                table, schema, key(primitiveSchemaVersion),
                (s, c) -> s, ctx(Optional.of(5L)));

        assertEquals("evolved-prim", result);
    }
}
