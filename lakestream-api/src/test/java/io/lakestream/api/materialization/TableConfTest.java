/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.api.materialization;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.Test;

class TableConfTest {

    @Test
    void testConstruction() {
        TableConf t = new TableConf(
                Optional.of(TableMode.MANAGED),
                Optional.of(List.of(PartitionSpec.streamPartition())),
                Optional.of(List.of(new SortColumn("c", SortDirection.ASC, false))),
                Optional.of(new RetentionConfig(Optional.empty(), Optional.empty(), Optional.empty())),
                Optional.of(134_217_728L),
                Optional.of(Compression.ZSTD));
        assertEquals(Optional.of(TableMode.MANAGED), t.mode());
        assertEquals(Optional.of(Compression.ZSTD), t.compression());
        assertEquals(1, t.partitionBy().orElseThrow().size());
    }

    @Test
    void testListsAreDefensivelyCopied() {
        List<PartitionSpec> parts = new ArrayList<>();
        parts.add(PartitionSpec.streamPartition());
        List<SortColumn> sorts = new ArrayList<>();
        sorts.add(new SortColumn("c", SortDirection.ASC, false));
        TableConf t = new TableConf(
                Optional.empty(),
                Optional.of(parts),
                Optional.of(sorts),
                Optional.empty(),
                Optional.empty(),
                Optional.empty());
        parts.clear();
        sorts.clear();
        assertEquals(1, t.partitionBy().orElseThrow().size());
        assertEquals(1, t.sortBy().orElseThrow().size());
        assertThrows(UnsupportedOperationException.class,
                () -> t.partitionBy().orElseThrow().add(PartitionSpec.streamPartition()));
        assertThrows(UnsupportedOperationException.class,
                () -> t.sortBy().orElseThrow().add(new SortColumn("x", SortDirection.ASC, false)));
    }

    @Test
    void testEqualsHashCode() {
        TableConf a = new TableConf(
                Optional.of(TableMode.MANAGED),
                Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty());
        TableConf b = new TableConf(
                Optional.of(TableMode.MANAGED),
                Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty());
        TableConf c = new TableConf(
                Optional.of(TableMode.EXTERNAL),
                Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty());
        assertEquals(a, b);
        assertEquals(a.hashCode(), b.hashCode());
        assertNotEquals(a, c);
    }

    @Test
    void testRejectsNullOptionals() {
        assertThrows(NullPointerException.class, () -> new TableConf(
                null, Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty()));
        assertThrows(NullPointerException.class, () -> new TableConf(
                Optional.empty(), null, Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty()));
        assertThrows(NullPointerException.class, () -> new TableConf(
                Optional.empty(), Optional.empty(), null, Optional.empty(), Optional.empty(), Optional.empty()));
        assertThrows(NullPointerException.class, () -> new TableConf(
                Optional.empty(), Optional.empty(), Optional.empty(), null, Optional.empty(), Optional.empty()));
        assertThrows(NullPointerException.class, () -> new TableConf(
                Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty(), null, Optional.empty()));
        assertThrows(NullPointerException.class, () -> new TableConf(
                Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty(), null));
    }
}
