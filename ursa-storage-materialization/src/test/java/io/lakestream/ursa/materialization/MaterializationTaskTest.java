/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.materialization;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNullPointerException;

import io.lakestream.api.StreamIdentifier;
import io.lakestream.api.materialization.ResolvedMaterialization;
import io.lakestream.api.materialization.TableCatalog;
import io.lakestream.api.materialization.TableCatalogType;
import io.lakestream.api.materialization.TableIdentifier;
import io.lakestream.api.materialization.TableMaterializationPolicy;
import java.util.Map;
import org.junit.jupiter.api.Test;

class MaterializationTaskTest {

    private static final StreamIdentifier STREAM = StreamIdentifier.of("public/default", "events");

    private static ResolvedMaterialization resolved() {
        TableCatalog catalog = new TableCatalog(
                "lake",
                TableCatalogType.ICEBERG,
                Map.of(),
                Map.of());
        return new ResolvedMaterialization(
                catalog,
                new TableIdentifier("db", "tbl"),
                TableMaterializationPolicy.empty());
    }

    @Test
    void rejectsNullStream() {
        assertThatNullPointerException()
                .isThrownBy(() -> new MaterializationTask(null, resolved(), "t", 1L, 0L, 0L))
                .withMessageContaining("stream");
    }

    @Test
    void rejectsNullResolvedMaterialization() {
        assertThatNullPointerException()
                .isThrownBy(() -> new MaterializationTask(STREAM, null, "t", 1L, 0L, 0L))
                .withMessageContaining("resolvedMaterialization");
    }

    @Test
    void rejectsNullSourceTopic() {
        assertThatNullPointerException()
                .isThrownBy(() -> new MaterializationTask(STREAM, resolved(), null, 1L, 0L, 0L))
                .withMessageContaining("sourceTopic");
    }

    @Test
    void preservesOffsetRangeAndSource() {
        MaterializationTask task =
                new MaterializationTask(STREAM, resolved(), "default/t-partition-0", 7L, 1000L, 2000L);
        assertThat(task.sourceTopic()).isEqualTo("default/t-partition-0");
        assertThat(task.streamId()).isEqualTo(7L);
        assertThat(task.startOffset()).isEqualTo(1000L);
        assertThat(task.endOffset()).isEqualTo(2000L);
    }
}
