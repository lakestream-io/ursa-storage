/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.materialization;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNullPointerException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

class CommitResultTest {

    @Test
    void rejectsNullSinkMetadata() {
        assertThatNullPointerException()
                .isThrownBy(() -> new CommitResult(0L, 0L, null))
                .withMessageContaining("sinkMetadata");
    }

    @Test
    void defensiveCopyOfSinkMetadata() {
        Map<String, String> source = new HashMap<>();
        source.put("iceberg.snapshot-id", "abc");

        CommitResult result = new CommitResult(10L, 1024L, source);

        // Mutating the source after construction must not leak through.
        source.put("iceberg.snapshot-id", "MUTATED");
        assertThat(result.sinkMetadata()).containsEntry("iceberg.snapshot-id", "abc");

        // The held map should be immutable.
        assertThatThrownBy(() -> result.sinkMetadata().put("k", "v"))
                .isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void preservesScalarFields() {
        CommitResult result = new CommitResult(42L, 2048L, Map.of("delta.version", "7"));
        assertThat(result.recordsCommitted()).isEqualTo(42L);
        assertThat(result.bytesCommitted()).isEqualTo(2048L);
        assertThat(result.sinkMetadata()).containsEntry("delta.version", "7");
    }
}
