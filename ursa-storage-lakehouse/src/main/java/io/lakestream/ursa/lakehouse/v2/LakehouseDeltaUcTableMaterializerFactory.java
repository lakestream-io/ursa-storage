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
 * {@link TableCatalogType#DELTA_UC} (Delta on Unity Catalog) catalogs.
 *
 * <p>Reuses {@link EvolutionPolicy#forDelta()} since Unity Catalog applies the same set of
 * permissible evolutions to Delta tables it manages.
 */
public final class LakehouseDeltaUcTableMaterializerFactory implements TableMaterializerFactory {

    @Override
    public TableCatalogType catalogType() {
        return TableCatalogType.DELTA_UC;
    }

    @Override
    public TableMaterializer<?> create(TableMaterializationPolicy policy,
                                       TableCatalog resolvedCatalog,
                                       StreamMetadata streamMetadata,
                                       MaterializationRuntime runtime) {
        AbstractLakehouseWriter writer =
                LakehouseWriterFactory.deltaUc(policy, resolvedCatalog, streamMetadata, runtime);
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
