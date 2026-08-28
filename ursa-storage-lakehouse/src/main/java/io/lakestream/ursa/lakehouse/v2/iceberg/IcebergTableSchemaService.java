/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.lakehouse.v2.iceberg;

import io.lakestream.ursa.lakehouse.LakehouseConfiguration;
import io.lakestream.ursa.lakehouse.iceberg.IcebergTable;
import io.lakestream.ursa.lakehouse.iceberg.TableOptions;
import io.lakestream.ursa.lakehouse.iceberg.exception.SchemaEvolutionException;
import io.lakestream.ursa.lakehouse.iceberg.exception.SchemaMappingException;
import io.lakestream.ursa.materialization.serde.TableSchemaService;
import java.util.HashSet;
import java.util.Set;
import java.util.SortedMap;
import lombok.extern.slf4j.Slf4j;
import org.apache.iceberg.Schema;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.jetbrains.annotations.NotNull;

@Slf4j
public class IcebergTableSchemaService implements TableSchemaService<Long, Schema> {

    private final IcebergTable icebergTable;
    private final LakehouseConfiguration config;

    public IcebergTableSchemaService(IcebergTable icebergTable, LakehouseConfiguration config) {
        this.icebergTable = icebergTable;
        this.config = config;
    }

    @Override
    public Set<Long> evolveTableSchema(@NotNull SortedMap<Long, Schema> schemaWithVersions) throws Exception {
        boolean createTable = false;
        if (!icebergTable.exists()) {
            log.info("Table does not exist. Creating new table for schema evolution.");
            createTable = true;
        }
        Set<Long> results = new HashSet<>();
        var versions = schemaWithVersions.keySet().stream().sorted().toList();
        for (Long version : versions) {
            var schema = schemaWithVersions.get(version);
            if (createTable) {
                TableOptions dataTableOpt = TableOptions.builder()
                        .schema(schema)
                        .partitionKey(config.getPartitionKey())
                        .identifierFields(config.getIdentifierFields())
                        .properties(config.getIcebergTableProperties())
                        .build();
                icebergTable.create(dataTableOpt);
                createTable = false;
            }
            try {
                icebergTable.evolveSchemaWithVersion(version, schema);
                results.add(version);
            } catch (SchemaMappingException e) {
                throw e;
            } catch (SchemaEvolutionException e) {
                // todo: expose the error out to let user know the detail of the failure
                log.error(e.getMessage());
            }
        }
        return results;
    }

    @Override
    public Schema getTableSchema(Long schemaVersion) throws Exception {
        try {
            return icebergTable.getSchemaByVersion(schemaVersion).orElse(null);
        } catch (NoSuchTableException e) {
            return null;
        }
    }

    public Long getLatestSchemaVersion() throws Exception {
        try {
            return icebergTable.getSchemaMapping().keySet().stream().max(Long::compareTo).orElse(-1L);
        } catch (NoSuchTableException e) {
            return -1L;
        }
    }
}
