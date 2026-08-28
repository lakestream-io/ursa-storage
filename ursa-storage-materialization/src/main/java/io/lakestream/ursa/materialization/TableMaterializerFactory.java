/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.materialization;

import io.lakestream.api.Stream;
import io.lakestream.api.materialization.TableCatalog;
import io.lakestream.api.materialization.TableCatalogType;
import io.lakestream.api.materialization.TableMaterializationPolicy;
import io.lakestream.ursa.materialization.serde.TableSchemaService;
import javax.annotation.Nullable;

/**
 * Service-Provider Interface for sink-side materialization back-ends.
 *
 * <p>Each {@link TableCatalogType} has at most one factory; the framework
 * iterates the {@link java.util.ServiceLoader}-loaded factories and dispatches
 * on {@link #catalogType()}. Implementations register themselves via
 * {@code META-INF/services/io.lakestream.ursa.materialization.TableMaterializerFactory}
 * in their own module (lakehouse for T8, clickhouse for T11), not in this
 * module.
 */
public interface TableMaterializerFactory {

    /**
     * Returns the catalog type this factory implements.
     */
    TableCatalogType catalogType();

    /**
     * Builds a {@link TableMaterializer} for the given resolved policy and
     * catalog. Called once per materialization task. Implementations should
     * not retain references to the policy / catalog beyond the returned
     * materializer.
     *
     * @param policy            the effective (already resolved) policy for the stream
     * @param resolvedCatalog   the catalog the materializer writes into
     * @param stream            the source stream handle
     * @param runtime           injected framework services
     * @return a materializer ready to accept records
     */
    TableMaterializer<?> create(
            TableMaterializationPolicy policy,
            TableCatalog resolvedCatalog,
            Stream stream,
            MaterializationRuntime runtime);

    /**
     * Builds a {@link TableSchemaService} matching the sink's schema model.
     *
     * @return the schema service, or {@code null} if the sink performs no
     *     schema evolution (e.g., a raw-pass-through writer)
     */
    @Nullable
    TableSchemaService<?, ?> schemaService(
            TableMaterializationPolicy policy,
            TableCatalog resolvedCatalog,
            Stream stream);
}
