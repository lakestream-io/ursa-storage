/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.storage.impl;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.lakestream.api.LogState;
import io.lakestream.api.exception.LogFencedException;
import org.junit.jupiter.api.Test;

class LogStateUtilTest {

    @Test
    void returnsStorageLevelExceptionForFencedLog() {
        LogFencedException exception = LogStateUtil.toException(LogState.FENCED, 42L).orElseThrow();

        assertEquals("stream 42 is fenced", exception.getMessage());
    }

    @Test
    void returnsEmptyForWritableLog() {
        assertTrue(LogStateUtil.toException(LogState.NORMAL, 42L).isEmpty());
    }
}
