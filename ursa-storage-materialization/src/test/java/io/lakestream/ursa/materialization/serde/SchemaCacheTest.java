/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.materialization.serde;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

public class SchemaCacheTest {

    @Test
    @DisplayName("Entries expire after write even when they are accessed")
    void testExpireAfterWrite() throws Exception {
        SchemaCache cache = new SchemaCache(100, Duration.ofMillis(150));
        AtomicInteger loads = new AtomicInteger();
        Object key = "schema-key";

        Object first = cache.computeIfAbsent(key, () -> "schema-" + loads.incrementAndGet());

        Thread.sleep(75);
        assertThat(cache.get(key)).isSameAs(first);

        Thread.sleep(100);
        Object second = cache.computeIfAbsent(key, () -> "schema-" + loads.incrementAndGet());

        assertThat(second).isNotSameAs(first);
        assertThat(second).isEqualTo("schema-2");
        assertThat(loads).hasValue(2);
    }

    @Test
    @DisplayName("Entries can be invalidated explicitly")
    void testInvalidate() throws Exception {
        SchemaCache cache = new SchemaCache(100, Duration.ofMinutes(1));
        AtomicInteger loads = new AtomicInteger();
        Object key = "schema-key";

        Object first = cache.computeIfAbsent(key, () -> "schema-" + loads.incrementAndGet());
        cache.invalidate(key);
        Object second = cache.computeIfAbsent(key, () -> "schema-" + loads.incrementAndGet());

        assertThat(second).isNotSameAs(first);
        assertThat(second).isEqualTo("schema-2");
        assertThat(loads).hasValue(2);
    }
}
