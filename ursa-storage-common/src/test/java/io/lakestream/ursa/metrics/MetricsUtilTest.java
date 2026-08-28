/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.metrics;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import org.junit.jupiter.api.Test;

class MetricsUtilTest {

    private static final AttributeKey<String> STREAM = AttributeKey.stringKey("ursa.stream.name");
    private static final AttributeKey<String> NAMESPACE = AttributeKey.stringKey("ursa.stream.namespace");
    private static final AttributeKey<Long> PARTITION = AttributeKey.longKey("ursa.stream.partition");

    @Test
    void createsAttributesForPartitionedStream() {
        Attributes attributes = MetricsUtil.getStreamAttributes(
                "application/orders-partition-3",
                Attributes.builder().put("base", "value").build());

        assertEquals("application/orders", attributes.get(STREAM));
        assertEquals("application", attributes.get(NAMESPACE));
        assertEquals(3L, attributes.get(PARTITION));
        assertEquals("value", attributes.get(AttributeKey.stringKey("base")));
    }

    @Test
    void preservesNestedNamespace() {
        Attributes attributes = MetricsUtil.getStreamAttributes(
                "org/analytics/events", Attributes.empty());

        assertEquals("org/analytics/events", attributes.get(STREAM));
        assertEquals("org/analytics", attributes.get(NAMESPACE));
        assertNull(attributes.get(PARTITION));
    }

    @Test
    void supportsUnqualifiedStreamName() {
        Attributes attributes = MetricsUtil.getStreamAttributes("events-partition-0", Attributes.empty());

        assertEquals("events", attributes.get(STREAM));
        assertNull(attributes.get(NAMESPACE));
        assertEquals(0L, attributes.get(PARTITION));
    }

    @Test
    void doesNotTreatMalformedPartitionSuffixAsPartition() {
        Attributes attributes = MetricsUtil.getStreamAttributes(
                "application/events-partition-01", Attributes.empty());

        assertEquals("application/events-partition-01", attributes.get(STREAM));
        assertNull(attributes.get(PARTITION));
    }

    @Test
    void rejectsInvalidStreamNames() {
        assertThrows(IllegalArgumentException.class,
                () -> MetricsUtil.getStreamAttributes(null, Attributes.empty()));
        assertThrows(IllegalArgumentException.class,
                () -> MetricsUtil.getStreamAttributes("application/", Attributes.empty()));
    }
}
