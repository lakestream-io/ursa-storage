/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.api.materialization;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.lakestream.api.StreamIdentifier;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.Test;

/**
 * Construction, equality and interpolation rules for {@link TableNaming}: the
 * {@code ${stream.namespace}} / {@code ${stream.name}} / {@code ${stream.property.<key>}}
 * variables, and the prefix vs. derived-namespace fallback.
 */
class TableNamingTest {

    @Test
    void testConstruction() {
        TableNaming n = new TableNaming(Optional.of("warehouse"), "${stream.name}");
        assertEquals(Optional.of("warehouse"), n.tableNamespacePrefix());
        assertEquals("${stream.name}", n.tableNameTemplate());
    }

    @Test
    void testEmptyPrefixIsAllowed() {
        TableNaming n = new TableNaming(Optional.empty(), "t");
        assertTrue(n.tableNamespacePrefix().isEmpty());
    }

    @Test
    void testEqualsHashCode() {
        TableNaming a = new TableNaming(Optional.of("w"), "t");
        TableNaming b = new TableNaming(Optional.of("w"), "t");
        TableNaming c = new TableNaming(Optional.of("w"), "t2");
        assertEquals(a, b);
        assertEquals(a.hashCode(), b.hashCode());
        assertNotEquals(a, c);
    }

    @Test
    void testRejectsNullPrefixOptional() {
        assertThrows(NullPointerException.class, () -> new TableNaming(null, "t"));
    }

    @Test
    void testRejectsNullTemplate() {
        assertThrows(NullPointerException.class, () -> new TableNaming(Optional.empty(), null));
    }

    @Test
    void testRejectsEmptyTemplate() {
        assertThrows(IllegalArgumentException.class, () -> new TableNaming(Optional.empty(), ""));
    }

    @Test
    void interpolatesStreamProperties() {
        TableNaming naming = new TableNaming(Optional.empty(), "${stream.property.lakestream.kafka.topic.name}_v1");
        TableIdentifier table = naming.toTableIdentifier(
            StreamIdentifier.of("default", "orders-topic-id-abc"),
            Map.of("lakestream.kafka.topic.name", "orders"));
        assertEquals("orders_v1", table.name());
        assertEquals("default", table.namespace());
    }

    @Test
    void missingPropertyIsRejected() {
        TableNaming naming = new TableNaming(Optional.empty(), "${stream.property.missing}");
        assertThrows(IllegalArgumentException.class,
            () -> naming.toTableIdentifier(StreamIdentifier.of("ns", "s"), Map.of()));
    }

    @Test
    void singleArgumentOverloadRejectsPropertyVariables() {
        TableNaming naming = new TableNaming(Optional.empty(), "${stream.property.x}");
        assertThrows(IllegalArgumentException.class,
            () -> naming.toTableIdentifier(StreamIdentifier.of("ns", "s")));
    }


    private static final StreamIdentifier STREAM =
            StreamIdentifier.of("public/default", "orders");

    @Test
    void interpolateStreamName() {
        TableNaming naming = new TableNaming(Optional.empty(), "${stream.name}");

        TableIdentifier id = naming.toTableIdentifier(STREAM);

        assertThat(id.namespace()).isEqualTo("public/default");
        assertThat(id.name()).isEqualTo("orders");
    }

    @Test
    void interpolateWithPrefix() {
        TableNaming naming = new TableNaming(Optional.of("analytics"), "${stream.name}");

        TableIdentifier id = naming.toTableIdentifier(STREAM);

        assertThat(id.namespace()).isEqualTo("analytics");
        assertThat(id.name()).isEqualTo("orders");
    }

    @Test
    void interpolateBothVars() {
        TableNaming naming = new TableNaming(
                Optional.empty(), "${stream.namespace}_${stream.name}");

        TableIdentifier id = naming.toTableIdentifier(STREAM);

        assertThat(id.namespace()).isEqualTo("public/default");
        assertThat(id.name()).isEqualTo("public/default_orders");
    }

    @Test
    void interpolateLiteralOnly() {
        TableNaming naming = new TableNaming(Optional.empty(), "events");

        TableIdentifier id = naming.toTableIdentifier(STREAM);

        assertThat(id.name()).isEqualTo("events");
        assertThat(id.namespace()).isEqualTo("public/default");
    }

    @Test
    void prefixIsLiteralAndNotInterpolated() {
        // Prefix is treated as a literal — variables in the prefix are NOT expanded.
        TableNaming naming = new TableNaming(
                Optional.of("${stream.name}"), "literal_name");

        TableIdentifier id = naming.toTableIdentifier(STREAM);

        assertThat(id.namespace()).isEqualTo("${stream.name}");
        assertThat(id.name()).isEqualTo("literal_name");
    }

    @Test
    void rejectsUnknownVar() {
        TableNaming naming = new TableNaming(Optional.empty(), "${foo.bar}");

        assertThatThrownBy(() -> naming.toTableIdentifier(STREAM))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Unknown template variable: ${foo.bar}");
    }

    @Test
    void rejectsBlankResult() {
        // Construction-time guard catches "" already; whitespace-only templates
        // pass construction but yield a blank interpolation result.
        TableNaming naming = new TableNaming(Optional.empty(), "  ");

        assertThatThrownBy(() -> naming.toTableIdentifier(STREAM))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Interpolated table name is empty for template:");
    }

    @Test
    void rejectsNullStreamId() {
        TableNaming naming = new TableNaming(Optional.empty(), "${stream.name}");

        assertThatThrownBy(() -> naming.toTableIdentifier(null))
                .isInstanceOf(NullPointerException.class);
    }
}
