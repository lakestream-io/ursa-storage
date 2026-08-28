/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.api;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

import org.junit.jupiter.api.Test;

class StreamIdentifierTest {

    @Test
    void testConstruction() {
        StreamIdentifier id = new StreamIdentifier("public/default", "my-topic");
        assertEquals("public/default", id.namespace());
        assertEquals("my-topic", id.name());
    }

    @Test
    void testEquality() {
        StreamIdentifier a = new StreamIdentifier("ns", "stream");
        StreamIdentifier b = new StreamIdentifier("ns", "stream");
        StreamIdentifier c = new StreamIdentifier("ns", "other");
        assertEquals(a, b);
        assertNotEquals(a, c);
    }

    @Test
    void testFullName() {
        StreamIdentifier id = new StreamIdentifier("public/default", "my-topic");
        assertEquals("public/default/my-topic", id.fullName());
    }

    @Test
    void testFactoryMethod() {
        StreamIdentifier id = StreamIdentifier.of("ns", "stream");
        assertEquals("ns", id.namespace());
        assertEquals("stream", id.name());
        assertEquals(new StreamIdentifier("ns", "stream"), id);
    }
}
