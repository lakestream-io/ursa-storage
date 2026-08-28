/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.materialization;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNullPointerException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.lakestream.api.StreamIdentifier;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.Test;

class MaterializationContextTest {

    private static final StreamIdentifier STREAM = StreamIdentifier.of("public/default", "events");

    @Test
    void rejectsNullStream() {
        assertThatNullPointerException()
                .isThrownBy(() -> new MaterializationContext(
                        null, 0L, 0L, Optional.empty(), Map.of()))
                .withMessageContaining("stream");
    }

    @Test
    void rejectsNullSourceSchemaVersion() {
        assertThatNullPointerException()
                .isThrownBy(() -> new MaterializationContext(
                        STREAM, 0L, 0L, null, Map.of()))
                .withMessageContaining("sourceSchemaVersion");
    }

    @Test
    void rejectsNullSourceMetadata() {
        assertThatNullPointerException()
                .isThrownBy(() -> new MaterializationContext(
                        STREAM, 0L, 0L, Optional.empty(), null))
                .withMessageContaining("sourceMetadata");
    }

    @Test
    void defensiveCopyOfSourceMetadata() {
        Map<String, String> source = new HashMap<>();
        source.put("header.x", "1");

        MaterializationContext ctx = new MaterializationContext(
                STREAM, 100L, 200L, Optional.of(5L), source);

        source.put("header.x", "MUTATED");
        assertThat(ctx.sourceMetadata()).containsEntry("header.x", "1");

        assertThatThrownBy(() -> ctx.sourceMetadata().put("k", "v"))
                .isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void preservesScalarFields() {
        MaterializationContext ctx = new MaterializationContext(
                STREAM, 17L, 31L, Optional.of(2L), Map.of());

        assertThat(ctx.stream()).isEqualTo(STREAM);
        assertThat(ctx.offset()).isEqualTo(17L);
        assertThat(ctx.timestamp()).isEqualTo(31L);
        assertThat(ctx.sourceSchemaVersion()).contains(2L);
    }
}
