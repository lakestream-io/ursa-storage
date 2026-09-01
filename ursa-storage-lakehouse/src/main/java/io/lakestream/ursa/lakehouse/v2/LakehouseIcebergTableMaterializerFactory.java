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
 * {@link TableCatalogType#ICEBERG} catalogs. Builds an Iceberg writer via
 * {@link LakehouseWriterFactory#iceberg} and wraps it in a
 * {@link LakehouseTableMaterializer}.
 *
 * <p>Iceberg sub-flavours (Glue / REST / Hadoop / Polaris / Unity) are conveyed through
 * {@link TableCatalog#connection()} (typically via the {@code catalog-impl} key); this factory
 * handles all of them uniformly.
 *
 * <p>The {@link #schemaService(TableMaterializationPolicy, TableCatalog, StreamMetadata)} method returns
 * {@code null} because the underlying {@code IcebergExternalTableWriter} owns its own
 * {@code IcebergTableSchemaService} (constructed lazily once a write triggers table creation).
 * Building a second instance here would require an eager catalog connection — T8 explicitly
 * defers that wiring to the orchestrator refactor in T9/T10.
 */
public final class LakehouseIcebergTableMaterializerFactory implements TableMaterializerFactory {

    @Override
    public TableCatalogType catalogType() {
        return TableCatalogType.ICEBERG;
    }

    @Override
    public TableMaterializer<?> create(TableMaterializationPolicy policy,
                                       TableCatalog resolvedCatalog,
                                       StreamMetadata streamMetadata,
                                       MaterializationRuntime runtime) {
        AbstractLakehouseWriter writer =
                LakehouseWriterFactory.iceberg(policy, resolvedCatalog, streamMetadata, runtime);
        Optional<LakehouseRecordWriter<FailureMessage>> dltWriter =
                LakehouseWriterFactory.externalDltWriter(policy, resolvedCatalog, streamMetadata, "iceberg",
                        runtime.taskProperties());
        dltWriter.ifPresent(dlt -> writer.registerFailureMessageHandler(DLTFailureMessageHandler.of(dlt)));
        return new LakehouseTableMaterializer(writer, EvolutionPolicy.forIceberg(), dltWriter.orElse(null));
    }

    @Override
    @Nullable
    public TableSchemaService<?, ?> schemaService(TableMaterializationPolicy policy,
                                                  TableCatalog resolvedCatalog,
                                                  StreamMetadata streamMetadata) {
        // The writer constructs its own IcebergTableSchemaService lazily; returning null here
        // avoids an eager catalog connection. T9 can route through the writer once the
        // orchestrator owns the schema-evolution lifecycle.
        return null;
    }
}
