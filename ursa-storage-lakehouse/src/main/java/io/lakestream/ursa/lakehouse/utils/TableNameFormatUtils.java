/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.lakehouse.utils;

import static io.lakestream.ursa.lakehouse.iceberg.IcebergTable.ICEBERG_CATALOG_TYPE_BIGLAKE;
import static io.lakestream.ursa.lakehouse.iceberg.IcebergTable.ICEBERG_CATALOG_TYPE_BIGQUERY;
import static io.lakestream.ursa.lakehouse.iceberg.IcebergTable.ICEBERG_CATALOG_TYPE_HORIZON;
import static io.lakestream.ursa.lakehouse.iceberg.IcebergTable.ICEBERG_CATALOG_TYPE_S3TABLE;
import static io.lakestream.ursa.lakehouse.iceberg.IcebergTable.ICEBERG_CATALOG_TYPE_UNITYCATALOG;

import io.lakestream.ursa.lakehouse.LakehouseConfiguration;
import java.util.Arrays;
import java.util.Optional;
import java.util.stream.Stream;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;

@Slf4j
public class TableNameFormatUtils {

    public static String formatS3TableNamespaceName(String name) {
        return formatS3Identifier(name);
    }

    public static String formatS3TableName(String name) {
        return formatS3Identifier(name);
    }

    public static String formatUnityCatalogIcebergNamespaceName(String name) {
        return formatUnityCatalogIcebergIdentifier(name);
    }
    public static String formatUnityCatalogIcebergTableName(String name) {
        return formatUnityCatalogIcebergIdentifier(name);
    }

    /**
     * Naming rules for tables and namespaces in S3Table catalog.
     * https://docs.databricks.com/aws/en/data-governance/unity-catalog#securable-object-naming-requirements
     */
    public static String formatUnityCatalogIcebergIdentifier(String name) {

        if (StringUtils.isBlank(name)) {
            throw new IllegalArgumentException("Name cannot be blank");
        }

        String formatted = name.strip().replace("/", "___")
                .replace(".", "_")
                .replace("-", "__")
                .replace(":", "____")
                .replaceAll("\\s+", "_");  // Replace whitespace with underscore

        // Step 2: Handle uppercase (convert to lowercase with underscore suffix)
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < formatted.length(); i++) {
            char c = formatted.charAt(i);
            if (Character.isUpperCase(c)) {
                sb.append(Character.toLowerCase(c)).append('_');
            } else {
                sb.append(c);
            }
        }
        formatted = sb.toString();

        if (formatted.length() > 255) {
            formatted = formatted.substring(0, 255);
        }

        return formatted;
    }


    /**
     * Naming rules for tables and namespaces in S3Table catalog.
     * https://docs.aws.amazon.com/AmazonS3/latest/userguide/s3-tables-buckets-naming.html
     * The following naming rules apply to tables and namespaces within table buckets:
     * - Names must be between 1 and 255 characters long.
     * - Names can consist only of lowercase letters, numbers, and underscores (_).
     * - Names must begin and end with a letter or number.
     * - Names must not contain hyphens (-) or periods (.).
     * - A table name must be unique within a namespace.
     * - A namespace must be unique within a table bucket.
     * - Namespace names must not start with the reserved prefix aws.
     * @param name
     * @return
     */
    public static String formatS3Identifier(String name) {
        if (StringUtils.isBlank(name)) {
            throw new IllegalArgumentException("Name cannot be blank");
        }

        // Step 1: Apply your existing replacements
        String formatted = name.strip().replace("/", "___")
                .replace(".", "_")
                .replace("-", "__")
                .replace(":", "____")
                .replaceAll("\\s+", "_");  // Replace whitespace with underscore

        // Step 2: Handle uppercase (convert to lowercase with underscore suffix)
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < formatted.length(); i++) {
            char c = formatted.charAt(i);
            if (Character.isUpperCase(c)) {
                sb.append(Character.toLowerCase(c)).append('_');
            } else {
                sb.append(c);
            }
        }
        formatted = sb.toString();

        // Step 3: Replace anything not allowed with underscore
        formatted = formatted.replaceAll("[^a-z0-9_]", "_");

        // Step 4: Remove leading/trailing underscores until start/end is letter/number
        formatted = formatted.replaceAll("^_+", "").replaceAll("_+$", "");

        // Step 5: Ensure not empty
        if (formatted.isEmpty()) {
            formatted = "default";
        }

        // Step 6: Avoid reserved "aws" prefix
        if (formatted.startsWith("aws")) {
            formatted = "x_" + formatted;
        }

        // Step 7: Enforce max length
        if (formatted.length() > 255) {
            formatted = formatted.substring(0, 255);
        }

        return formatted;
    }

    public static TableIdentifier formatIdentifier(LakehouseConfiguration configuration,
                                                    Optional<String> catalogName,
                                                    TableIdentifier identifier) {
        var icebergCatalogBackendType = configuration.getIcebergCatalogBackendType(catalogName).toString();
        if (CatalogUtil.ICEBERG_CATALOG_TYPE_HIVE.equals(configuration.getIcebergCatalogType(catalogName))
                || ICEBERG_CATALOG_TYPE_S3TABLE.equalsIgnoreCase(icebergCatalogBackendType)
                || ICEBERG_CATALOG_TYPE_BIGQUERY.equalsIgnoreCase(icebergCatalogBackendType)
                || ICEBERG_CATALOG_TYPE_BIGLAKE.equalsIgnoreCase(icebergCatalogBackendType)
                || ICEBERG_CATALOG_TYPE_UNITYCATALOG.equalsIgnoreCase(icebergCatalogBackendType)
                || ICEBERG_CATALOG_TYPE_HORIZON.equalsIgnoreCase(icebergCatalogBackendType)) {
            String cluster = (String) configuration.getProperties().getOrDefault("clusterName", "ursa");

            var namespaceLevels = Stream.concat(Stream.of(cluster), Arrays.stream(identifier.namespace().levels()))
                .toList();

            String ns = String.join("_",
                namespaceLevels.stream().map(TableNameFormatUtils::formatNamespaceName).toArray(String[]::new));

            if (ICEBERG_CATALOG_TYPE_S3TABLE.equalsIgnoreCase(icebergCatalogBackendType)) {
                ns = String.join("_",
                                namespaceLevels.stream()
                                .map(TableNameFormatUtils::formatS3TableNamespaceName).toArray(String[]::new));
            } else if (ICEBERG_CATALOG_TYPE_UNITYCATALOG.equalsIgnoreCase(icebergCatalogBackendType)) {
                ns = String.join("_",
                                namespaceLevels.stream()
                                .map(TableNameFormatUtils::formatUnityCatalogIcebergNamespaceName)
                                .toArray(String[]::new));
            }

            if (StringUtils.isBlank(cluster)) {
                throw new IllegalArgumentException("cluster name should not be empty: " + cluster);
            }
            log.info("reconstruct the namespace: {} -> {} for hive/s3Table catalog", identifier.namespace(), ns);

            // handle the table name formatting for s3Table catalog
            if (ICEBERG_CATALOG_TYPE_S3TABLE
                    .equalsIgnoreCase(configuration.getIcebergCatalogBackendType(catalogName).toString())) {
                String s3TableName = formatS3TableName(identifier.name());
                log.info("reconstruct the table name: {} -> {} for s3Table catalog", identifier.name(), s3TableName);
                return TableIdentifier.of(Namespace.of(ns), s3TableName);
            }

            // handle the table name formatting for unity catalog
            if (ICEBERG_CATALOG_TYPE_UNITYCATALOG.equalsIgnoreCase(icebergCatalogBackendType)) {
                String formattedTableName = formatUnityCatalogIcebergTableName(identifier.name());
                log.info("reconstruct the table name: {} -> {} for Unity Catalog",
                        identifier.name(), formattedTableName);
                return TableIdentifier.of(Namespace.of(ns), formattedTableName);
            }
            return TableIdentifier.of(Namespace.of(ns), identifier.name());
        }
        return identifier;
    }

    public static String formatNamespaceName(String name) {
        return formatTableName(name);
    }



    public static String formatTableName(String name) {
        if (StringUtils.isBlank(name)) {
            return name;
        }

        return name.replace("/", "___")
                .replace(".", "_")
                .replace("-", "__")
                .replace(":", "____");
    }

}
