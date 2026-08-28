/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.clickhouse;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.lakestream.ursa.materialization.MaterializationException;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link ClickHouseIdentifiers}, focused on the {@link ClickHouseIdentifiers#sanitizeName}
 * fold that turns a namespace-encoded table name ({@code public/test_v7.test_v4}) into a ClickHouse-legal
 * dotted identifier ({@code public.test_v7.test_v4}).
 */
class ClickHouseIdentifiersTest {

    @Test
    void sanitizeNameFoldsPathSeparatorToDot() {
        assertThat(ClickHouseIdentifiers.sanitizeName("public/test_v7.test_v4"))
                .isEqualTo("public.test_v7.test_v4");
    }

    @Test
    void sanitizeNameLeavesAlreadyDottedNameUnchanged() {
        assertThat(ClickHouseIdentifiers.sanitizeName("test_v4")).isEqualTo("test_v4");
        assertThat(ClickHouseIdentifiers.sanitizeName("a.b.c")).isEqualTo("a.b.c");
    }

    @Test
    void sanitizeNameFoldsEveryPathSeparator() {
        assertThat(ClickHouseIdentifiers.sanitizeName("public/ns/sub/topic"))
                .isEqualTo("public.ns.sub.topic");
    }

    @Test
    void sanitizeNameRejectsNullOrEmpty() {
        assertThatThrownBy(() -> ClickHouseIdentifiers.sanitizeName(null))
                .isInstanceOf(MaterializationException.class);
        assertThatThrownBy(() -> ClickHouseIdentifiers.sanitizeName(""))
                .isInstanceOf(MaterializationException.class);
    }
}
