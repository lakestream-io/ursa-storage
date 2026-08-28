/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.clickhouse;

import static org.assertj.core.api.Assertions.assertThat;

import io.lakestream.api.materialization.FrameworkConf;
import io.lakestream.api.materialization.TableMaterializationPolicy;
import io.lakestream.api.materialization.WriteMode;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.Test;

class ClickHouseTableEngineTest {

    @Test
    void defaultIsMergeTree() {
        assertThat(ClickHouseTableEngine.forPolicy(TableMaterializationPolicy.empty()))
                .isEqualTo(ClickHouseTableEngine.MERGE_TREE);
    }

    @Test
    void upsertWriteModeMapsToReplacingMergeTree() {
        TableMaterializationPolicy policy = withWriteMode(WriteMode.UPSERT);
        assertThat(ClickHouseTableEngine.forPolicy(policy))
                .isEqualTo(ClickHouseTableEngine.REPLACING_MERGE_TREE);
    }

    @Test
    void cdcWriteModeMapsToReplacingMergeTree() {
        TableMaterializationPolicy policy = withWriteMode(WriteMode.CDC);
        assertThat(ClickHouseTableEngine.forPolicy(policy))
                .isEqualTo(ClickHouseTableEngine.REPLACING_MERGE_TREE);
    }

    @Test
    void appendWriteModeMapsToMergeTree() {
        TableMaterializationPolicy policy = withWriteMode(WriteMode.APPEND);
        assertThat(ClickHouseTableEngine.forPolicy(policy))
                .isEqualTo(ClickHouseTableEngine.MERGE_TREE);
    }

    @Test
    void primaryKeyImpliesReplacingMergeTree() {
        TableMaterializationPolicy policy = new TableMaterializationPolicy(
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.of(List.of("id")),
                Optional.empty(),
                Optional.empty(),
                Map.of());
        assertThat(ClickHouseTableEngine.forPolicy(policy))
                .isEqualTo(ClickHouseTableEngine.REPLACING_MERGE_TREE);
    }

    @Test
    void emptyPrimaryKeyDoesNotForceReplacingMergeTree() {
        TableMaterializationPolicy policy = new TableMaterializationPolicy(
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.of(List.of()),
                Optional.empty(),
                Optional.empty(),
                Map.of());
        assertThat(ClickHouseTableEngine.forPolicy(policy))
                .isEqualTo(ClickHouseTableEngine.MERGE_TREE);
    }

    @Test
    void nullPolicyFallsBackToMergeTree() {
        assertThat(ClickHouseTableEngine.forPolicy(null))
                .isEqualTo(ClickHouseTableEngine.MERGE_TREE);
    }

    private static TableMaterializationPolicy withWriteMode(WriteMode mode) {
        FrameworkConf framework = new FrameworkConf(
                Optional.of(mode),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty());
        return new TableMaterializationPolicy(
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.of(framework),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Map.of());
    }
}
