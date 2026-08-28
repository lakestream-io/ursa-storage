/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.lakehouse.iceberg;

/**
 * The catalog service provider name.
 */
public enum IcebergCatalogBackendType {
    HADOOP,
    TABULAR,
    NESSIE,
    POLARIS,
    S3TABLE,
    BIGQUERY,
    BIGLAKE,
    UNITYCATALOG,
    HORIZON
}
