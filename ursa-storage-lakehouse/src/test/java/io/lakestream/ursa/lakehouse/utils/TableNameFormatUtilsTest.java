/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.lakehouse.utils;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.lakestream.ursa.lakehouse.LakehouseConfiguration;
import io.lakestream.ursa.lakehouse.iceberg.IcebergCatalogBackendType;
import java.util.Optional;
import java.util.Properties;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;


class TableNameFormatUtilsTest {

    // Test formatTableName and formatNamespaceName
    @ParameterizedTest
    @CsvSource({
            "simple, simple",
            "my/table, my___table",
            "my.table, my_table",
            "my-table, my__table",
            "my:table, my____table",
            "my/table.name-test:value, my___table_name__test____value",
            "'', ''",
    })
    void testFormatTableName(String input, String expected) {
        assertEquals(expected, TableNameFormatUtils.formatTableName(input));
    }

    @Test
    void testFormatTableName_nullInput() {
        assertNull(TableNameFormatUtils.formatTableName(null));
    }

    @ParameterizedTest
    @CsvSource({
            "simple, simple",
            "my/namespace, my___namespace",
            "my.namespace, my_namespace",
    })
    void testFormatNamespaceName(String input, String expected) {
        assertEquals(expected, TableNameFormatUtils.formatNamespaceName(input));
    }

    // Test S3 Identifier formatting
    @ParameterizedTest
    @CsvSource({
            "simple, simple",
            "MyTable, m_yt_able",
            "my-table, my__table",
            "my.table, my_table",
            "my/table, my___table",
            "my:table, my____table",
            "My-Table.Name/Test:Value, m_y__t_able_n_ame___t_est____v_alue",
            "123table, 123table",
            "table123, table123",
            "_leading, leading",
            "trailing_, trailing",
            "___multiple___, multiple",
            "aws_table, x_aws_table",
            "awsTable, x_awst_able",
            "my@table#name, my_table_name",
    })
    void testFormatS3Identifier(String input, String expected) {
        assertEquals(expected, TableNameFormatUtils.formatS3Identifier(input));
    }

    @Test
    void testFormatS3Identifier_blankInput() {
        assertThrows(IllegalArgumentException.class,
                () -> TableNameFormatUtils.formatS3Identifier(""));
        assertThrows(IllegalArgumentException.class,
                () -> TableNameFormatUtils.formatS3Identifier("   "));
        assertThrows(IllegalArgumentException.class,
                () -> TableNameFormatUtils.formatS3Identifier(null));
    }

    @Test
    void testFormatS3Identifier_emptyAfterProcessing() {
        // Special characters only should result in "default"
        String result = TableNameFormatUtils.formatS3Identifier("___");
        assertEquals("default", result);
    }

    @Test
    void testFormatS3Identifier_maxLength() {
        // Create a string longer than 255 characters
        String longName = "a".repeat(300);
        String result = TableNameFormatUtils.formatS3Identifier(longName);
        assertEquals(255, result.length());
    }

    @Test
    void testFormatS3Identifier_awsPrefix() {
        assertEquals("x_aws", TableNameFormatUtils.formatS3Identifier("aws"));
        assertEquals("x_awstest", TableNameFormatUtils.formatS3Identifier("awstest"));
    }

    // Test Unity Catalog Identifier formatting
    @ParameterizedTest
    @CsvSource({
            "simple, simple",
            "MyTable, m_yt_able",
            "my-table, my__table",
            "my.table, my_table",
            "my/table, my___table",
            "my:table, my____table",
            "My-Table.Name/Test:Value, m_y__t_able_n_ame___t_est____v_alue",
            "my table, my_table",
            "my  table  name, my_table_name",
            "'table name with spaces', table_name_with_spaces",
            "'trailing spaces  ', trailing_spaces",
    })
    void testFormatUnityCatalogIcebergIdentifier(String input, String expected) {
        assertEquals(expected, TableNameFormatUtils.formatUnityCatalogIcebergIdentifier(input));
    }

    @Test
    void testFormatUnityCatalogIcebergIdentifier_blankInput() {
        assertThrows(IllegalArgumentException.class,
                () -> TableNameFormatUtils.formatUnityCatalogIcebergIdentifier(""));
        assertThrows(IllegalArgumentException.class,
                () -> TableNameFormatUtils.formatUnityCatalogIcebergIdentifier("   "));
        assertThrows(IllegalArgumentException.class,
                () -> TableNameFormatUtils.formatUnityCatalogIcebergIdentifier(null));
    }

    @Test
    void testFormatUnityCatalogIcebergIdentifier_maxLength() {
        // Create a string longer than 255 characters
        String longName = "a".repeat(300);
        String result = TableNameFormatUtils.formatUnityCatalogIcebergIdentifier(longName);
        assertEquals(255, result.length());
    }

    @Test
    void testFormatS3TableNamespaceName() {
        assertEquals("my___table", TableNameFormatUtils.formatS3TableNamespaceName("my/table"));
    }

    @Test
    void testFormatS3TableName() {
        assertEquals("my___table", TableNameFormatUtils.formatS3TableName("my/table"));
    }

    @Test
    void testFormatUnityCatalogIcebergNamespaceName() {
        assertEquals("my___table",
                TableNameFormatUtils.formatUnityCatalogIcebergNamespaceName("my/table"));
    }

    @Test
    void testFormatUnityCatalogIcebergTableName() {
        assertEquals("my___table",
                TableNameFormatUtils.formatUnityCatalogIcebergTableName("my/table"));
    }

    // Test formatIdentifier
    @Test
    void testFormatIdentifier_hiveType() {
        LakehouseConfiguration config = mock(LakehouseConfiguration.class);
        Properties props = new Properties();
        props.put("cluster", "ursa");

        when(config.getIcebergCatalogType(any())).thenReturn(CatalogUtil.ICEBERG_CATALOG_TYPE_HIVE);
        when(config.getIcebergCatalogBackendType(any())).thenReturn(IcebergCatalogBackendType.HADOOP);
        when(config.getProperties()).thenReturn(props);

        TableIdentifier input = TableIdentifier.of(Namespace.of("region", "analytics"), "my-table");
        TableIdentifier result = TableNameFormatUtils.formatIdentifier(config, Optional.empty(), input);

        assertEquals("ursa_region_analytics", result.namespace().toString());
        assertEquals("my-table", result.name());
    }

    @Test
    void testFormatIdentifier_s3TableType() {
        LakehouseConfiguration config = mock(LakehouseConfiguration.class);
        Properties props = new Properties();
        props.put("cluster", "ursa");

        when(config.getIcebergCatalogType(any())).thenReturn(CatalogUtil.ICEBERG_CATALOG_REST);
        when(config.getIcebergCatalogBackendType(any())).thenReturn(IcebergCatalogBackendType.S3TABLE);
        when(config.getProperties()).thenReturn(props);

        TableIdentifier input = TableIdentifier.of(Namespace.of("region", "analytics"), "my-table");
        TableIdentifier result = TableNameFormatUtils.formatIdentifier(config, Optional.empty(), input);

        assertEquals("ursa_region_analytics", result.namespace().toString());
        assertEquals("my__table", result.name());
    }

    @Test
    void testFormatIdentifier_unityCatalogType() {
        LakehouseConfiguration config = mock(LakehouseConfiguration.class);
        Properties props = new Properties();
        props.put("cluster", "ursa");

        when(config.getIcebergCatalogType(any())).thenReturn(CatalogUtil.ICEBERG_CATALOG_REST);
        when(config.getIcebergCatalogBackendType(any())).thenReturn(IcebergCatalogBackendType.UNITYCATALOG);
        when(config.getProperties()).thenReturn(props);

        TableIdentifier input = TableIdentifier.of(Namespace.of("region", "analytics"), "my-table");
        TableIdentifier result = TableNameFormatUtils.formatIdentifier(config, Optional.empty(), input);

        assertEquals("ursa_region_analytics", result.namespace().toString());
        assertEquals("my__table", result.name());
    }

    @Test
    void testFormatIdentifier_bigQueryType() {
        LakehouseConfiguration config = mock(LakehouseConfiguration.class);
        Properties props = new Properties();
        props.put("cluster", "ursa");

        when(config.getIcebergCatalogType(any())).thenReturn(CatalogUtil.ICEBERG_CATALOG_BIGQUERY);
        when(config.getIcebergCatalogBackendType(any())).thenReturn(IcebergCatalogBackendType.BIGQUERY);
        when(config.getProperties()).thenReturn(props);

        TableIdentifier input = TableIdentifier.of(Namespace.of("region", "analytics"), "my-table");
        TableIdentifier result = TableNameFormatUtils.formatIdentifier(config, Optional.empty(), input);

        assertEquals("ursa_region_analytics", result.namespace().toString());
        assertEquals("my-table", result.name());
    }

    @Test
    void testFormatIdentifier_emptyCluster() {
        LakehouseConfiguration config = mock(LakehouseConfiguration.class);
        Properties props = new Properties();
        props.put("clusterName", "");

        when(config.getIcebergCatalogType(any())).thenReturn(CatalogUtil.ICEBERG_CATALOG_TYPE_HIVE);
        when(config.getIcebergCatalogBackendType(any())).thenReturn(IcebergCatalogBackendType.HADOOP);
        when(config.getProperties()).thenReturn(props);

        TableIdentifier input = TableIdentifier.of(Namespace.of("region", "analytics"), "table");

        assertThrows(IllegalArgumentException.class,
                () -> TableNameFormatUtils.formatIdentifier(config, Optional.empty(), input));
    }

    @Test
    void testFormatIdentifier_otherCatalogType() {
        LakehouseConfiguration config = mock(LakehouseConfiguration.class);

        when(config.getIcebergCatalogType(any())).thenReturn("rest");
        when(config.getIcebergCatalogBackendType(any())).thenReturn(IcebergCatalogBackendType.NESSIE);

        TableIdentifier input = TableIdentifier.of(Namespace.of("region", "analytics"), "table");
        TableIdentifier result = TableNameFormatUtils.formatIdentifier(config, Optional.empty(), input);

        // Should return unchanged identifier
        assertEquals(input, result);
    }

    @Test
    void testFormatIdentifier_multiLevelNamespace() {
        LakehouseConfiguration config = mock(LakehouseConfiguration.class);
        Properties props = new Properties();
        props.put("cluster", "ursa");

        when(config.getIcebergCatalogType(any())).thenReturn(CatalogUtil.ICEBERG_CATALOG_REST);
        when(config.getIcebergCatalogBackendType(any())).thenReturn(IcebergCatalogBackendType.UNITYCATALOG);
        when(config.getProperties()).thenReturn(props);

        TableIdentifier input = TableIdentifier.of(
                Namespace.of("region", "analytics", "sub-ns"), "table");
        TableIdentifier result = TableNameFormatUtils.formatIdentifier(config, Optional.empty(), input);

        assertEquals("ursa_region_analytics_sub__ns", result.namespace().toString());
        assertEquals("table", result.name());
    }

    @Test
    void testS3TableNameFormat() {
        LakehouseConfiguration config = mock(LakehouseConfiguration.class);
        Properties props = new Properties();
        props.put("clusterName", "c-cluster");

        when(config.getIcebergCatalogType(any())).thenReturn(CatalogUtil.ICEBERG_CATALOG_REST);
        when(config.getIcebergCatalogBackendType(any())).thenReturn(IcebergCatalogBackendType.S3TABLE);
        when(config.getProperties()).thenReturn(props);

        TableIdentifier input = TableIdentifier.of(
            Namespace.of("region", "analytics"), "topic");
        TableIdentifier result = TableNameFormatUtils.formatIdentifier(config, Optional.empty(), input);

        assertEquals("c__cluster_region_analytics", result.namespace().toString());
        assertEquals("topic", result.name());
    }

    @Test
    void testUnityCatalogTableNameFormat() {
        LakehouseConfiguration config = mock(LakehouseConfiguration.class);
        Properties props = new Properties();
        props.put("clusterName", "c-cluster");

        when(config.getIcebergCatalogType(any())).thenReturn(CatalogUtil.ICEBERG_CATALOG_REST);
        when(config.getIcebergCatalogBackendType(any())).thenReturn(IcebergCatalogBackendType.UNITYCATALOG);
        when(config.getProperties()).thenReturn(props);

        TableIdentifier input = TableIdentifier.of(
            Namespace.of("region", "analytics"), "topic");
        TableIdentifier result = TableNameFormatUtils.formatIdentifier(config, Optional.empty(), input);

        assertEquals("c__cluster_region_analytics", result.namespace().toString());
        assertEquals("topic", result.name());
    }
}
