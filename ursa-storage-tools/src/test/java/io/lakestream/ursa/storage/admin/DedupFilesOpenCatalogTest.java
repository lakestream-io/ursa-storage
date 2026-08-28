/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.storage.admin;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.Closeable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

/**
 * Integration test that runs the deduplication logic against a live Iceberg REST catalog such as
 * Snowflake Open Catalog (Polaris). It is DISABLED unless the {@code ICEBERG_REST_URI} environment
 * variable is set, so it never runs in CI and is invoked manually to verify against a real catalog:
 *
 * <pre>{@code
 * ICEBERG_REST_URI="https://<org>-<account>.snowflakecomputing.com/polaris/api/catalog" \
 * ICEBERG_REST_WAREHOUSE="<catalog_name>" \
 * ICEBERG_REST_CREDENTIAL="<client_id>:<client_secret>" \
 * ICEBERG_REST_SCOPE="PRINCIPAL_ROLE:ALL" \
 * ICEBERG_REST_NAMESPACE="dedup_test_ns" \
 *   mvn -B -ntp test -pl ursa-storage-tools -Dtest=DedupFilesOpenCatalogTest
 * }</pre>
 *
 * <p>Recognised settings (read from environment variable first, then JVM system property):
 * <ul>
 *   <li>{@code ICEBERG_REST_URI} (required) — REST catalog endpoint; also gates the test.</li>
 *   <li>{@code ICEBERG_REST_WAREHOUSE} — catalog/warehouse name.</li>
 *   <li>{@code ICEBERG_REST_CREDENTIAL} — OAuth2 {@code client_id:client_secret}.</li>
 *   <li>{@code ICEBERG_REST_TOKEN} — bearer token (alternative to credential).</li>
 *   <li>{@code ICEBERG_REST_SCOPE} — OAuth2 scope (default {@code PRINCIPAL_ROLE:ALL}).</li>
 *   <li>{@code ICEBERG_REST_NAMESPACE} — namespace to use (default {@code dedup_test_ns}).</li>
 *   <li>{@code ICEBERG_REST_TABLE} — table name (default {@code dedup_test_<timestamp>}).</li>
 * </ul>
 */
@EnabledIfEnvironmentVariable(named = "ICEBERG_REST_URI", matches = ".+")
public class DedupFilesOpenCatalogTest {

    /** Reads a setting from the environment first, then a JVM system property, else the default. */
    private static String cfg(String key, String defaultValue) {
        String env = System.getenv(key);
        if (env != null && !env.isBlank()) {
            return env;
        }
        String prop = System.getProperty(key);
        if (prop != null && !prop.isBlank()) {
            return prop;
        }
        return defaultValue;
    }

    private static Catalog buildRestCatalog() {
        Map<String, String> props = new HashMap<>();
        props.put(CatalogProperties.CATALOG_IMPL, "org.apache.iceberg.rest.RESTCatalog");
        props.put(CatalogProperties.URI, cfg("ICEBERG_REST_URI", null));

        String warehouse = cfg("ICEBERG_REST_WAREHOUSE", null);
        if (warehouse != null) {
            props.put(CatalogProperties.WAREHOUSE_LOCATION, warehouse);
        }
        String credential = cfg("ICEBERG_REST_CREDENTIAL", null);
        if (credential != null) {
            props.put("credential", credential);
        }
        String token = cfg("ICEBERG_REST_TOKEN", null);
        if (token != null) {
            props.put("token", token);
        }
        props.put("scope", cfg("ICEBERG_REST_SCOPE", "PRINCIPAL_ROLE:ALL"));
        // Snowflake Open Catalog / Polaris vend short-lived storage credentials to the client.
        props.put("header.X-Iceberg-Access-Delegation", "vended-credentials");

        return CatalogUtil.buildIcebergCatalog("opencatalog", props, new Configuration());
    }

//    @Test
    void deduplicatesDuplicateParquetViaRestCatalog() throws Exception {
        Catalog catalog = buildRestCatalog();
        Namespace namespace = Namespace.of("public", cfg("ICEBERG_REST_NAMESPACE", "dedup_test_ns"));
        TableIdentifier tableId = TableIdentifier.of(namespace,
                cfg("ICEBERG_REST_TABLE", "dedup_test_" + System.currentTimeMillis()));

        if (catalog instanceof SupportsNamespaces nsCatalog && !nsCatalog.namespaceExists(namespace)) {
            nsCatalog.createNamespace(namespace);
        }
        if (catalog.tableExists(tableId)) {
            catalog.dropTable(tableId, true);
        }

        Schema schema = new Schema(
                Types.NestedField.required(1, "id", Types.IntegerType.get()),
                Types.NestedField.optional(2, "data", Types.StringType.get()));
        Table table = catalog.createTable(tableId, schema, PartitionSpec.unpartitioned());
        try {
            // Commit the SAME physical parquet file twice (two snapshots) -> duplicate reference,
            // reproducing the corruption against the real REST catalog + object storage.
            DataFile duplicated = IcebergTestRecords.writeRecords(table, 1, 2, 3);
            table.newAppend().appendFile(duplicated).commit();
            table.newAppend().appendFile(duplicated).commit();
            table.newAppend().appendFile(duplicated).commit();
            table.refresh();

            // Read BEFORE the fix: the duplicated file's rows are returned twice (3 ids x2 = 6).
            List<Integer> before = IcebergTestRecords.readIds(table);
            System.out.println("Open Catalog rows BEFORE dedup: " + before);
            assertEquals(9, before.size(), "before: " + before);

            // Detect and apply the fix against the REST-loaded table.
            Map<String, List<DataFile>> duplicates = DedupFiles.findDuplicateFilePaths(table);
            assertEquals(1, duplicates.size(), "expected exactly one duplicated path");
            DedupFiles.applyDedup(table, duplicates);
            table.refresh();

            // Read AFTER the fix: each record appears exactly once (1, 2, 3).
            List<Integer> after = IcebergTestRecords.readIds(table);
            System.out.println("Open Catalog rows AFTER dedup: " + after);
            assertEquals(3, after.size(), "after: " + after);
            assertEquals(List.of(1, 2, 3),
                    after.stream().sorted().collect(Collectors.toList()), "after: " + after);
            assertEquals(0, DedupFiles.findDuplicateFilePaths(table).size(),
                    "no duplicates should remain");
        } finally {
            catalog.dropTable(tableId, true);
            if (catalog instanceof Closeable closeable) {
                closeable.close();
            }
        }
    }
}
