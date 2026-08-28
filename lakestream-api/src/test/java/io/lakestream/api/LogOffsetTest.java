/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.api;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

class LogOffsetTest {

    @Test
    void testConstruction() {
        LogOffset offset = new LogOffset(100, 5, 1000L);
        assertEquals(100, offset.offset());
        assertEquals(5, offset.numberOfRecords());
        assertEquals(1000L, offset.timestamp());
    }

    @Test
    void testEquality() {
        LogOffset a = new LogOffset(100, 5, 1000L);
        LogOffset b = new LogOffset(100, 5, 1000L);
        assertEquals(a, b);
    }
}
