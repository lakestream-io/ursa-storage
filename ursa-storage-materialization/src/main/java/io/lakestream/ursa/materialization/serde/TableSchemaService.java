/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.materialization.serde;

import java.util.Set;
import java.util.SortedMap;

/**
 * TableSchemaService is used to manage the table schema in the target table format.
 *
 * We need to make the topic schema matching the table schema. Because the table schema is independent managed.
 * Use this service to manage the table schema.
 *
 * @param <V> the schema version type
 * @param <R> the schema type, in iceberg, it is org.apache.iceberg.Schema,
 *              in Delta Lake, it is org.apache.spark.sql.types.StructType
 */
public interface TableSchemaService<V, R> {

    /**
     * Evolve the table schema with the given schema and version.
     *
     * For example, given a map of schema with version:
     *  {
     *   1: schema_v1,
     *   2: schema_v2,
     *   3: schema_v3
     *  }
     *  The table schema service should iterate each schema and evolve the table schema to the latest version.
     *  The loop will be:
     *  1. check if version 1 exists, if not, evolve to version 1
     *  2. check if version 2 exists, if not, evolve to version 2
     *  3. check if version 3 exists, if not, evolve to version 3
     *
     *  Return the set of versions that have been evolved.
     *  For example, if version 1 and 3 have been evolved, return {1, 3}
     *
     * @param schemaWithVersions the topic schema with version map
     * @return the set of schema versions that have been evolved
     */
    Set<V> evolveTableSchema(SortedMap<V, R> schemaWithVersions) throws Exception;

    /**
     * Get the table schema for the given topic schema version.
     *
     * @param schemaVersion
     * @return the table schema
     */
    R getTableSchema(V schemaVersion) throws Exception;


    /**
     * Get the latest schema version.
     *
     * @return the latest schema version
     */
    V getLatestSchemaVersion() throws Exception;
}
