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

class ErrorHandlingTest {

    @Test
    void testConstruction() {
        ErrorHandling e = new ErrorHandling(ErrorMode.SKIP, Optional.of("dlq.topic"));
        assertEquals(ErrorMode.SKIP, e.mode());
        assertEquals(Optional.of("dlq.topic"), e.dlqTopic());
    }

    @Test
    void testEqualsHashCode() {
        ErrorHandling a = new ErrorHandling(ErrorMode.SUSPEND, Optional.empty());
        ErrorHandling b = new ErrorHandling(ErrorMode.SUSPEND, Optional.empty());
        ErrorHandling c = new ErrorHandling(ErrorMode.LOG, Optional.empty());
        assertEquals(a, b);
        assertEquals(a.hashCode(), b.hashCode());
        assertNotEquals(a, c);
    }

    @Test
    void testRejectsNullMode() {
        assertThrows(NullPointerException.class, () -> new ErrorHandling(null, Optional.empty()));
    }

    @Test
    void testRejectsNullDlqOptional() {
        assertThrows(NullPointerException.class, () -> new ErrorHandling(ErrorMode.LOG, null));
    }
}
