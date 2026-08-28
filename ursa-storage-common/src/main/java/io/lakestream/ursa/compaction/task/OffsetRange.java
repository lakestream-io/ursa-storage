/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.compaction.task;

/** Utilities for logical offset ranges. */
public final class OffsetRange {

    private OffsetRange() {
    }

    /**
     * Returns the last offset included in {@code [startOffset, endOffset)}.
     *
     * @throws IllegalArgumentException if the range is negative, empty, or reversed
     */
    public static long lastIncludedOffset(long startOffset, long endOffset) {
        if (startOffset < 0 || endOffset <= startOffset) {
            throw new IllegalArgumentException("Invalid offset range [" + startOffset + ", " + endOffset
                    + "): startOffset must be non-negative and endOffset must be greater than startOffset");
        }
        return Math.subtractExact(endOffset, 1L);
    }
}
