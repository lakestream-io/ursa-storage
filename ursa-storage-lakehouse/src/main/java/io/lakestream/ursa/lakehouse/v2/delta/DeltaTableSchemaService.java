/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.lakehouse.v2.delta;

import io.delta.kernel.types.StructType;
import io.lakestream.ursa.lakehouse.delta.DeltaTable;
import io.lakestream.ursa.lakehouse.iceberg.exception.SchemaEvolutionException;
import io.lakestream.ursa.lakehouse.iceberg.exception.SchemaMappingException;
import io.lakestream.ursa.materialization.serde.TableSchemaService;
import java.util.HashSet;
import java.util.Set;
import java.util.SortedMap;
import lombok.extern.slf4j.Slf4j;
import org.apache.iceberg.exceptions.NoSuchTableException;

@Slf4j
public class DeltaTableSchemaService implements TableSchemaService<Long, StructType> {

    private final DeltaTable deltaTable;

    public DeltaTableSchemaService(DeltaTable deltaTable) {
        this.deltaTable = deltaTable;
    }

    @Override
    public Set<Long> evolveTableSchema(SortedMap<Long, StructType> schemaWithVersions) throws Exception {
        boolean createTable = false;
        if (!deltaTable.tableExists()) {
            log.info("Table does not exist. Creating new table for schema evolution.");
            createTable = true;
        }
        log.info("Start evolving Delta table schema for topic {}. createTable={}, requestedVersions={}",
            deltaTable.getParentTopic(), createTable, schemaWithVersions.keySet());
        Set<Long> results = new HashSet<>();
        var versions = schemaWithVersions.keySet().stream().sorted().toList();
        for (Long version : versions) {
            var schema = schemaWithVersions.get(version);
            if (createTable) {
                log.info("Creating Delta table for topic {} with initial schema version {}",
                    deltaTable.getParentTopic(), version);
                deltaTable.createDeltaTable(version, schema);
                createTable = false;
                results.add(version);
                log.info("Created Delta table for topic {} with initial schema version {}",
                    deltaTable.getParentTopic(), version);
                continue;
            }
            try {
                log.info("Evolving Delta table schema for topic {} with schema version {}",
                    deltaTable.getParentTopic(), version);
                deltaTable.evolveSchemaWithVersion(version, schema);
                results.add(version);
                log.info("Evolved Delta table schema for topic {} with schema version {}",
                    deltaTable.getParentTopic(), version);
            } catch (SchemaMappingException e) {
                log.error("Failed to evolve Delta table schema for topic {} due to schema mapping error. "
                        + "schemaVersion={}",
                    deltaTable.getParentTopic(), version, e);
                throw e;
            } catch (SchemaEvolutionException e) {
                log.warn("Failed to evolve Delta table schema for topic {}. schemaVersion={}, error={}",
                    deltaTable.getParentTopic(), version, e.getMessage(), e);
            }
        }
        log.info("Finished evolving Delta table schema for topic {}. evolvedVersions={}",
            deltaTable.getParentTopic(), results);
        return results;
    }

    @Override
    public StructType getTableSchema(Long schemaVersion) throws Exception {
        if (deltaTable.getSchemaMapping().contains(schemaVersion)) {
            var latestSnapshot = deltaTable.getLatestSnapshot();
            return latestSnapshot == null ? null : latestSnapshot.getSchema();
        }
        return null;
    }

    @Override
    public Long getLatestSchemaVersion() throws Exception {
        try {
            return deltaTable.getSchemaMapping().stream().max(Long::compareTo).orElse(-1L);
        } catch (NoSuchTableException e) {
            return -1L;
        }
    }
}
