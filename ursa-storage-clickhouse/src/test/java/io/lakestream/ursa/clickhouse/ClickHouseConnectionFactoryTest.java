/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.clickhouse;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.lakestream.api.materialization.TableCatalog;
import io.lakestream.api.materialization.TableCatalogType;
import io.lakestream.api.materialization.TableMaterializationPolicy;
import io.lakestream.ursa.exception.ExceptionCode;
import io.lakestream.ursa.materialization.MaterializationException;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.Test;

class ClickHouseConnectionFactoryTest {

    @Test
    void mergeConnectionOverlaysPolicyOnCatalog() {
        TableCatalog catalog = new TableCatalog(
                "ch",
                TableCatalogType.CLICKHOUSE,
                Map.of(
                        "dsn", "jdbc:ch://catalog:8123/db",
                        "user", "catalog-user"),
                Map.of());
        TableMaterializationPolicy policy = new TableMaterializationPolicy(
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Map.of("user", "stream-user", "password", "stream-secret"));

        Map<String, String> merged = ClickHouseConnectionFactory.mergeConnection(catalog, policy);

        assertThat(merged)
                .containsEntry("dsn", "jdbc:ch://catalog:8123/db")
                .containsEntry("user", "stream-user")
                .containsEntry("password", "stream-secret");
    }

    @Test
    void missingDsnRaisesMaterializationException() {
        TableCatalog catalog = new TableCatalog(
                "ch", TableCatalogType.CLICKHOUSE, Map.of(), Map.of());

        assertThatThrownBy(() -> ClickHouseConnectionFactory.open(catalog, null))
                .isInstanceOf(MaterializationException.class)
                .satisfies(e -> {
                    MaterializationException me = (MaterializationException) e;
                    assertThat(me.getExceptionCode())
                            .isEqualTo(ExceptionCode.LAKEHOUSE_CREATE_TABLE_WRITER_ERROR);
                });
    }

    @Test
    void nullPolicyMergesToCatalogConnectionOnly() {
        TableCatalog catalog = new TableCatalog(
                "ch",
                TableCatalogType.CLICKHOUSE,
                Map.of("dsn", "jdbc:ch://localhost:8123/db", "user", "default"),
                Map.of());

        Map<String, String> merged = ClickHouseConnectionFactory.mergeConnection(catalog, null);

        assertThat(merged)
                .hasSize(2)
                .containsEntry("dsn", "jdbc:ch://localhost:8123/db")
                .containsEntry("user", "default");
    }
}
