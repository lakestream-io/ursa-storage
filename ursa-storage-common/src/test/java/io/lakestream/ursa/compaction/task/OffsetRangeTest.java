/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.compaction.task;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

class OffsetRangeTest {

    @Test
    void returnsLastIncludedOffsetForEndExclusiveRange() {
        assertEquals(0L, OffsetRange.lastIncludedOffset(0L, 1L));
        assertEquals(99L, OffsetRange.lastIncludedOffset(0L, 100L));
        assertEquals(Long.MAX_VALUE - 1,
                OffsetRange.lastIncludedOffset(Long.MAX_VALUE - 1, Long.MAX_VALUE));
    }

    @Test
    void rejectsInvalidRangesBeforeSubtracting() {
        assertThrows(IllegalArgumentException.class, () -> OffsetRange.lastIncludedOffset(-1L, 0L));
        assertThrows(IllegalArgumentException.class, () -> OffsetRange.lastIncludedOffset(1L, 1L));
        assertThrows(IllegalArgumentException.class, () -> OffsetRange.lastIncludedOffset(2L, 1L));
        assertThrows(IllegalArgumentException.class,
                () -> OffsetRange.lastIncludedOffset(0L, Long.MIN_VALUE));
    }
}
