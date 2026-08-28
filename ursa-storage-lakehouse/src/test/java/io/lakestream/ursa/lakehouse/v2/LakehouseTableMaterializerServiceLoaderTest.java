/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.lakehouse.v2;

import static org.assertj.core.api.Assertions.assertThat;

import io.lakestream.api.materialization.TableCatalogType;
import io.lakestream.ursa.materialization.TableMaterializerFactory;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.ServiceLoader;
import java.util.Set;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag("lakehouse")
class LakehouseTableMaterializerServiceLoaderTest {

    @Test
    void serviceLoaderDiscoversAllThreeLakehouseFactories() {
        ServiceLoader<TableMaterializerFactory> loader =
                ServiceLoader.load(TableMaterializerFactory.class);

        Set<TableCatalogType> discovered = new HashSet<>();
        Set<Class<?>> discoveredClasses = new HashSet<>();
        for (TableMaterializerFactory factory : loader) {
            discovered.add(factory.catalogType());
            discoveredClasses.add(factory.getClass());
        }

        // The lakehouse module contributes factories for ICEBERG, DELTA, and DELTA_UC.
        assertThat(discovered).contains(
                TableCatalogType.ICEBERG,
                TableCatalogType.DELTA,
                TableCatalogType.DELTA_UC);
        assertThat(discoveredClasses).contains(
                LakehouseIcebergTableMaterializerFactory.class,
                LakehouseDeltaTableMaterializerFactory.class,
                LakehouseDeltaUcTableMaterializerFactory.class);
    }

    @Test
    void exactlyOneFactoryPerCatalogType() {
        ServiceLoader<TableMaterializerFactory> loader =
                ServiceLoader.load(TableMaterializerFactory.class);

        EnumSet<TableCatalogType> seen = EnumSet.noneOf(TableCatalogType.class);
        for (TableMaterializerFactory factory : loader) {
            TableCatalogType type = factory.catalogType();
            // For the lakehouse types we want exactly one factory per type; this guards against
            // accidental double-registration in META-INF/services.
            if (type == TableCatalogType.ICEBERG
                    || type == TableCatalogType.DELTA
                    || type == TableCatalogType.DELTA_UC) {
                assertThat(seen.add(type))
                        .as("duplicate factory registered for " + type)
                        .isTrue();
            }
        }
        assertThat(seen).containsExactlyInAnyOrder(
                TableCatalogType.ICEBERG,
                TableCatalogType.DELTA,
                TableCatalogType.DELTA_UC);
    }
}
