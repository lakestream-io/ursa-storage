/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.lakehouse.v2;

import io.lakestream.api.StreamMetadata;
import io.lakestream.api.materialization.EvolutionPolicy;
import io.lakestream.api.materialization.TableCatalog;
import io.lakestream.api.materialization.TableCatalogType;
import io.lakestream.api.materialization.TableMaterializationPolicy;
import io.lakestream.ursa.lakehouse.compact.DLTFailureMessageHandler;
import io.lakestream.ursa.lakehouse.compact.FailureMessage;
import io.lakestream.ursa.materialization.MaterializationRuntime;
import io.lakestream.ursa.materialization.TableMaterializer;
import io.lakestream.ursa.materialization.TableMaterializerFactory;
import io.lakestream.ursa.materialization.serde.TableSchemaService;
import java.util.Optional;
import javax.annotation.Nullable;

/**
 * {@link TableMaterializerFactory} implementation for
 * {@link TableCatalogType#DELTA} catalogs. Builds a Delta writer via
 * {@link LakehouseWriterFactory#delta} and wraps it in a
 * {@link LakehouseTableMaterializer}.
 *
 * <p>The {@link #schemaService(TableMaterializationPolicy, TableCatalog, StreamMetadata)} method returns
 * {@code null} for the same reasons noted on the Iceberg counterpart: the underlying writer owns
 * its own {@code DeltaTableSchemaService}, and eager construction here would require a live
 * connection that T8 deliberately defers to the orchestrator refactor (T9/T10).
 */
public final class LakehouseDeltaTableMaterializerFactory implements TableMaterializerFactory {

    @Override
    public TableCatalogType catalogType() {
        return TableCatalogType.DELTA;
    }

    @Override
    public TableMaterializer<?> create(TableMaterializationPolicy policy,
                                       TableCatalog resolvedCatalog,
                                       StreamMetadata streamMetadata,
                                       MaterializationRuntime runtime) {
        AbstractLakehouseWriter writer =
                LakehouseWriterFactory.delta(policy, resolvedCatalog, streamMetadata, runtime);
        Optional<LakehouseRecordWriter<FailureMessage>> dltWriter =
                LakehouseWriterFactory.externalDltWriter(policy, resolvedCatalog, streamMetadata, "delta",
                        runtime.taskProperties());
        dltWriter.ifPresent(dlt -> writer.registerFailureMessageHandler(DLTFailureMessageHandler.of(dlt)));
        return new LakehouseTableMaterializer(writer, EvolutionPolicy.forDelta(), dltWriter.orElse(null));
    }

    @Override
    @Nullable
    public TableSchemaService<?, ?> schemaService(TableMaterializationPolicy policy,
                                                  TableCatalog resolvedCatalog,
                                                  StreamMetadata streamMetadata) {
        return null;
    }
}
