/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.api.materialization;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.lakestream.api.StreamIdentifier;
import java.util.Optional;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link TableNaming#toTableIdentifier(StreamIdentifier)} — verifies
 * the {@code ${stream.namespace}} / {@code ${stream.name}} interpolation rules
 * and the prefix vs. derived-namespace fallback behaviour.
 */
class TableNamingInterpolationTest {

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
