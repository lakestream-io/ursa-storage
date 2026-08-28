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

class FrameworkConfTest {

    @Test
    void testConstruction() {
        FrameworkConf f = new FrameworkConf(
                Optional.of(WriteMode.UPSERT),
                Optional.of(StartPosition.EARLIEST),
                Optional.of(false),
                Optional.of(new ErrorHandling(ErrorMode.SUSPEND, Optional.empty())),
                Optional.of(new CommitConfig(Optional.empty(), Optional.empty(), Optional.empty())));
        assertEquals(Optional.of(WriteMode.UPSERT), f.writeMode());
        assertEquals(Optional.of(StartPosition.EARLIEST), f.startPosition());
        assertEquals(Optional.of(false), f.paused());
    }

    @Test
    void testAllEmptyAllowed() {
        FrameworkConf f = new FrameworkConf(
                Optional.empty(), Optional.empty(), Optional.empty(),
                Optional.empty(), Optional.empty());
        assertEquals(Optional.empty(), f.writeMode());
    }

    @Test
    void testEqualsHashCode() {
        FrameworkConf a = new FrameworkConf(
                Optional.of(WriteMode.APPEND), Optional.empty(), Optional.empty(),
                Optional.empty(), Optional.empty());
        FrameworkConf b = new FrameworkConf(
                Optional.of(WriteMode.APPEND), Optional.empty(), Optional.empty(),
                Optional.empty(), Optional.empty());
        FrameworkConf c = new FrameworkConf(
                Optional.of(WriteMode.CDC), Optional.empty(), Optional.empty(),
                Optional.empty(), Optional.empty());
        assertEquals(a, b);
        assertEquals(a.hashCode(), b.hashCode());
        assertNotEquals(a, c);
    }

    @Test
    void testRejectsNullOptionals() {
        assertThrows(NullPointerException.class, () -> new FrameworkConf(
                null, Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty()));
        assertThrows(NullPointerException.class, () -> new FrameworkConf(
                Optional.empty(), null, Optional.empty(), Optional.empty(), Optional.empty()));
        assertThrows(NullPointerException.class, () -> new FrameworkConf(
                Optional.empty(), Optional.empty(), null, Optional.empty(), Optional.empty()));
        assertThrows(NullPointerException.class, () -> new FrameworkConf(
                Optional.empty(), Optional.empty(), Optional.empty(), null, Optional.empty()));
        assertThrows(NullPointerException.class, () -> new FrameworkConf(
                Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty(), null));
    }
}
