/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.lakehouse.iceberg;

import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Properties;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag("lakehouse")
public class IcebergSinkConfigTest {

    @Test
    void constructorShouldSplitPropertiesIntoCorrectMaps() {
        Properties props = new Properties();
        props.setProperty("iceberg.catalog.name", "testCatalog");
        props.setProperty("iceberg.hadoop.fs.defaultFS", "hdfs://example.com");
        props.setProperty("iceberg.write-props.format", "parquet");
        props.setProperty("random.prop", "value");

        IcebergSinkConfig config = new IcebergSinkConfig(props);

        assertThat(config.getCatalogProps())
            .containsEntry("name", "testCatalog")
            .hasSize(1);

        assertThat(config.getHadoopProps())
            .containsEntry("fs.defaultFS", "hdfs://example.com")
            .hasSize(1);

        assertThat(config.getWriteProps())
            .containsEntry("format", "parquet")
            .hasSize(1);

        assertThat(config.getOriginalProps())
            .containsEntry("random.prop", "value")
            .hasSize(4);
    }

    @Test
    void getPrimaryKeysShouldHandleVariousScenarios() {
        Properties emptyProps = new Properties();
        IcebergSinkConfig emptyConfig = new IcebergSinkConfig(emptyProps);
        assertThat(emptyConfig.getPrimaryKeys()).isEmpty();

        Properties singleFieldProps = new Properties();
        singleFieldProps.setProperty("identifierFields", "id");
        IcebergSinkConfig singleConfig = new IcebergSinkConfig(singleFieldProps);
        assertThat(singleConfig.getPrimaryKeys()).containsExactly("id");

        Properties multiFieldProps = new Properties();
        multiFieldProps.setProperty("identifierFields", " id ,  name ,  age ");
        IcebergSinkConfig multiConfig = new IcebergSinkConfig(multiFieldProps);
        assertThat(multiConfig.getPrimaryKeys()).containsExactly("id", "name", "age");
    }

    @Test
    void getPartitionByShouldReturnCorrectValueOrDefault() {
        Properties props = new Properties();
        props.setProperty("partitionKey", "date");
        IcebergSinkConfig configWithPartition = new IcebergSinkConfig(props);
        assertEquals("date", configWithPartition.getPartitionKey());

        Properties emptyProps = new Properties();
        IcebergSinkConfig configWithoutPartition = new IcebergSinkConfig(emptyProps);
        assertNull(configWithoutPartition.getPartitionKey());
    }

    @Test
    void getCommitBranchShouldReturnCorrectValueOrDefault() {
        Properties props = new Properties();
        props.setProperty("commitBranch", "test-branch");
        IcebergSinkConfig configWithBranch = new IcebergSinkConfig(props);
        assertEquals("test-branch", configWithBranch.getCommitBranch());

        IcebergSinkConfig configWithoutBranch = new IcebergSinkConfig(new Properties());
        assertNull(configWithoutBranch.getCommitBranch());
    }

    @Test
    void getCdcFieldShouldReturnCorrectValueOrDefault() {
        Properties props = new Properties();
        props.setProperty("iceberg.table.cdc-field", "_op");
        IcebergSinkConfig configWithCdc = new IcebergSinkConfig(props);
        assertEquals("_op", configWithCdc.getCdcField());

        IcebergSinkConfig configWithoutCdc = new IcebergSinkConfig(new Properties());
        assertNull(configWithoutCdc.getCdcField());
    }

    @Test
    void isUpsertModeEnabledShouldCheckCorrectProperties() {
        Properties directProp = new Properties();
        directProp.setProperty("upsertModeEnabled", "true");
        IcebergSinkConfig directConfig = new IcebergSinkConfig(directProp);
        assertTrue(directConfig.isUpsertModeEnabled());

        Properties legacyProp = new Properties();
        legacyProp.setProperty("iceberg.table.upsert-mode-enabled", "true");
        IcebergSinkConfig legacyConfig = new IcebergSinkConfig(legacyProp);
        assertTrue(legacyConfig.isUpsertModeEnabled());

        Properties bothProps = new Properties();
        bothProps.setProperty("upsertModeEnabled", "false");
        bothProps.setProperty("iceberg.table.upsert-mode-enabled", "true");
        IcebergSinkConfig bothConfig = new IcebergSinkConfig(bothProps);
        assertFalse(bothConfig.isUpsertModeEnabled());

        IcebergSinkConfig defaultConfig = new IcebergSinkConfig(new Properties());
        assertFalse(defaultConfig.isUpsertModeEnabled());
    }

    @Test
    void booleanFlagsShouldRespectDefaultsAndProperties() {
        Properties trueProps = new Properties();
        trueProps.setProperty("iceberg.table.evolve-schema-enabled", "true");
        trueProps.setProperty("iceberg.table.schema-force-optional", "true");
        trueProps.setProperty("iceberg.table.schema-case-insensitive", "true");

        IcebergSinkConfig trueConfig = new IcebergSinkConfig(trueProps);
        assertTrue(trueConfig.isEvolveSchemaEnabled());
        assertTrue(trueConfig.isSchemaForceOptional());
        assertTrue(trueConfig.isSchemaCaseInsensitive());

        IcebergSinkConfig defaultConfig = new IcebergSinkConfig(new Properties());
        assertFalse(defaultConfig.isEvolveSchemaEnabled());
        assertFalse(defaultConfig.isSchemaForceOptional());
        assertFalse(defaultConfig.isSchemaCaseInsensitive());
    }

    @Test
    void retrySettingsShouldParseCorrectly() {
        Properties customRetries = new Properties();
        customRetries.setProperty("iceberg.table.schema-update-retries", "5");
        customRetries.setProperty("iceberg.table.create-table-retries", "3");

        IcebergSinkConfig config = new IcebergSinkConfig(customRetries);
        assertEquals(5, config.getSchemaUpdateRetries());
        assertEquals(3, config.getCreateTableRetries());

        IcebergSinkConfig defaultConfig = new IcebergSinkConfig(new Properties());
        assertEquals(2, defaultConfig.getSchemaUpdateRetries());
        assertEquals(2, defaultConfig.getCreateTableRetries());
    }
}
