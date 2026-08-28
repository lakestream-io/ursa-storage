/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.api.materialization;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.Optional;
import org.junit.jupiter.api.Test;

class PartitionSpecTest {

    @Test
    void testConstruction() {
        PartitionSpec p = new PartitionSpec("event_ts", PartitionTransform.HOUR, Optional.empty());
        assertEquals("event_ts", p.column());
        assertEquals(PartitionTransform.HOUR, p.transform());
        assertEquals(Optional.empty(), p.parameter());
    }

    @Test
    void testWithParameter() {
        PartitionSpec p = new PartitionSpec("user_id", PartitionTransform.BUCKET, Optional.of("16"));
        assertEquals(Optional.of("16"), p.parameter());
    }

    @Test
    void testEqualsHashCode() {
        PartitionSpec a = new PartitionSpec("c", PartitionTransform.DAY, Optional.empty());
        PartitionSpec b = new PartitionSpec("c", PartitionTransform.DAY, Optional.empty());
        PartitionSpec c = new PartitionSpec("c", PartitionTransform.MONTH, Optional.empty());
        assertEquals(a, b);
        assertEquals(a.hashCode(), b.hashCode());
        assertNotEquals(a, c);
    }

    @Test
    void testRejectsNulls() {
        assertThrows(NullPointerException.class, () -> new PartitionSpec(
                null, PartitionTransform.IDENTITY, Optional.empty()));
        assertThrows(NullPointerException.class, () -> new PartitionSpec(
                "c", null, Optional.empty()));
        assertThrows(NullPointerException.class, () -> new PartitionSpec(
                "c", PartitionTransform.IDENTITY, null));
    }

    @Test
    void testRejectsEmptyColumn() {
        assertThrows(IllegalArgumentException.class, () -> new PartitionSpec(
                "", PartitionTransform.IDENTITY, Optional.empty()));
    }
}
