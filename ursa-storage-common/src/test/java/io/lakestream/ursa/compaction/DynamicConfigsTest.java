/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.compaction;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;

public class DynamicConfigsTest {


    @Test
    void testDynamicConfigsWithEmptyProperties() {
        var configs = DynamicConfigs.of(new Properties());
        assertTrue(configs.properties().isEmpty());
        assertFalse(configs.sdtEnabled());
        assertFalse(configs.sbtEnabled());
    }

    @Test
    void testDynamicConfigsWithInvalidProperties() {
        var properties = new Properties();
        properties.setProperty("invalidKey", "true");
        var configs = DynamicConfigs.of(properties);
        assertTrue(configs.properties().isEmpty());
        assertFalse(configs.sdtEnabled());
    }

    @Test
    void testDynamicConfigsWithValidProperties() {
        var properties = new Properties();
        var configs = DynamicConfigs.of(properties);
        configs.overrideWith(Map.of("sdt.enabled", "true"));
        assertFalse(configs.properties().isEmpty());
        assertTrue(configs.sdtEnabled());
    }

    @Test
    void testOverrideWithProperties() {
        var properties = new Properties();
        properties.setProperty("sdt.enabled", "false");
        var configs = DynamicConfigs.of(properties);
        assertFalse(configs.sdtEnabled());

        configs.overrideWith(Map.of("sdt.enabled", "true"));
        assertTrue(configs.sdtEnabled());

        configs.overrideWith(Map.of("invalidKey", "true"));
        assertTrue(configs.sdtEnabled());
    }

    @Test
    void testOverrideOrder() {
        // namespace/topic level config has higher priority than cluster level config
        var properties = new Properties();
        properties.setProperty("sdt.enabled", "false");
        properties.setProperty("cluster.sdt.enabled", "true");
        var configs = DynamicConfigs.of(properties);
        assertFalse(configs.sdtEnabled());

        // only configured the cluster level config
        configs = DynamicConfigs.of(new Properties());
        configs.overrideWith(Map.of("cluster.sdt.enabled", "true"));
        assertTrue(configs.sdtEnabled());

        // only configured the namespace/topic level config
        configs = DynamicConfigs.of(new Properties());
        configs.overrideWith(Map.of("sdt.enabled", "false"));
        assertFalse(configs.sdtEnabled());
    }

    @Test
    void testCopyOfDynamicConfig() {
        var properties = new Properties();
        properties.setProperty("sdt.enabled", "false");
        DynamicConfigs configs = DynamicConfigs.of(properties);
        configs.overrideWith(Map.of("sdt.enabled", "true"));

        DynamicConfigs configs1 = DynamicConfigs.of(configs);
        configs1.overrideWith(Map.of("sdt.catalog.name", "a"));

        assertNotEquals(configs1, configs);
    }

    @Test
    void testExtraValidKeysFromEnv() {
        var properties = new Properties();
        properties.setProperty("test.key", "value1");
        properties.setProperty("test.catalog.key", "value2");
        var configs = DynamicConfigs.of(properties);
        var map = properties.entrySet()
            .stream()
            .collect(Collectors.toMap(
                e -> e.getKey().toString(),
                e -> e.getValue().toString()));
        configs.overrideWith(map);
        assertEquals("value1", configs.properties().get("test.key"));
        assertEquals("value2", configs.properties().get("test.catalog.key"));
    }

    @Test
    void testSdtEnabledPriority() {
        var properties = new Properties();
        properties.setProperty("sdt.enabled", "false");
        properties.setProperty("cluster.sdt.enabled", "true");
        properties.setProperty("clusterSdtEnabled", "true");

        var configs = DynamicConfigs.of(properties);
        assertTrue(configs.sdtEnabled()); // only parsed the config file name

        configs.overrideWith(Map.of(
            "cluster.sdt.enabled", "false"
        ));
        assertFalse(configs.sdtEnabled());

        configs.overrideWith(Map.of("sdt.enabled", "true"));
        assertTrue(configs.sdtEnabled());
    }

    @Test
    void testSdtEnabledInvalidValues() {
        var properties = new Properties();
        properties.setProperty("sdt.enabled", "invalid");
        var configs = DynamicConfigs.of(properties);
        assertFalse(configs.sdtEnabled()); // invalid value should be false

        properties.setProperty("sdt.enabled", "");
        configs = DynamicConfigs.of(properties);
        assertFalse(configs.sdtEnabled()); // empty value should be false
    }

    @Test
    void testSbtEnabledPriority() {
        var properties = new Properties();
        properties.setProperty("sbt.enabled", "false");
        properties.setProperty("cluster.sbt.enabled", "true");
        properties.setProperty("clusterSbtEnabled", "true");

        var configs = DynamicConfigs.of(properties);
        assertTrue(configs.sbtEnabled());

        configs.overrideWith(Map.of("cluster.sbt.enabled", "false"));
        assertFalse(configs.sbtEnabled());

        configs.overrideWith(Map.of("sbt.enabled", "true"));
        assertTrue(configs.sbtEnabled());
    }

    @Test
    void testSdtCatalogNamePriority() {
        var properties = new Properties();
        properties.setProperty("sdt.catalog.name", "namespace-catalog");
        properties.setProperty("cluster.sdt.catalog.name", "cluster-catalog");
        properties.setProperty("clusterSdtCatalogName", "default-catalog");

        var configs = DynamicConfigs.of(properties);
        assertEquals("default-catalog", configs.sdtCatalogName().get());

        configs.overrideWith(Map.of("sdt.catalog.name", "namespace-catalog"));
        assertEquals("namespace-catalog", configs.sdtCatalogName().get());
    }

    @Test
    void testSdtConfigsWithOverride() {
        var properties = new Properties();
        properties.setProperty("clusterSdtEnabled", "false");
        properties.setProperty("clusterSdtCatalogName", "original-catalog");

        var configs = DynamicConfigs.of(properties);
        assertFalse(configs.sdtEnabled());
        assertEquals("original-catalog", configs.sdtCatalogName().get());

        configs.overrideWith(Map.of(
            "sdt.enabled", "true",
            "sdt.catalog.name", "new-catalog"
        ));

        assertTrue(configs.sdtEnabled());
        assertEquals("new-catalog", configs.sdtCatalogName().get());
    }

    @Test
    void testSdtConfigsClusterLevelOverride() {
        var configs = DynamicConfigs.of(new Properties());

        configs.overrideWith(Map.of(
            "cluster.sdt.enabled", "true",
            "cluster.sdt.catalog.name", "cluster-catalog"
        ));

        assertTrue(configs.sdtEnabled());
        assertEquals("cluster-catalog", configs.sdtCatalogName().get());

        configs.overrideWith(Map.of(
            "sdt.enabled", "false",
            "sdt.catalog.name", "namespace-catalog"
        ));

        assertFalse(configs.sdtEnabled());
        assertEquals("namespace-catalog", configs.sdtCatalogName().get());
    }

    @Test
    void testSbtAndSdtFlags() {
        var properties = new Properties();
        properties.setProperty("clusterSbtEnabled", "false");
        properties.setProperty("clusterSdtEnabled", "true");

        var configs = new DynamicConfigs("test-cluster", properties);
        assertFalse(configs.sbtEnabled());
        assertTrue(configs.sdtEnabled());

        configs.overrideWith(Map.of(
            "test-cluster.sbt.enabled", "true",
            "test-cluster.sdt.enabled", "false"
        ));

        assertTrue(configs.sbtEnabled());
        assertFalse(configs.sdtEnabled());
    }

    @Test
    void testToTaskPropertiesOnlyExposesAllowedConfigs() {
        var properties = new Properties();
        properties.setProperty("clusterSbtEnabled", "false");
        properties.setProperty("clusterSdtEnabled", "true");
        properties.setProperty("clusterSdtCatalogName", "test-catalog");
        properties.setProperty("clusterTailCompactDataVisibilityIntervalInSeconds", "60");

        var configs = DynamicConfigs.of(properties);
        var taskProperties = configs.toTaskProperties();

        assertEquals("false", taskProperties.get("sbt.enabled"));
        assertEquals("true", taskProperties.get("sdt.enabled"));
        assertEquals("test-catalog", taskProperties.get("sdt.catalog.name"));
        assertNull(taskProperties.get("tail.compact.data.visibility.interval.in.seconds"));
    }

    @Test
    void testBuildDynamicConfigsFromTaskProperties() {
        var baseProperties = new Properties();
        baseProperties.setProperty("clusterSbtEnabled", "true");
        baseProperties.setProperty("clusterSdtEnabled", "false");

        var configs = DynamicConfigs.fromTaskProperties(baseProperties, Map.of(
            "sbt.enabled", "false",
            "sdt.enabled", "true",
            "sdt.catalog.name", "task-catalog"
        ));

        assertFalse(configs.sbtEnabled());
        assertTrue(configs.sdtEnabled());
        assertEquals(Optional.of("task-catalog"), configs.sdtCatalogName());
    }

    @Test
    void testDynamicConfigsWithEmptyPropertiesCluster() {
        var configs = new DynamicConfigs("test-cluster", new Properties());
        assertTrue(configs.properties().isEmpty());
        assertFalse(configs.sdtEnabled());
    }

    @Test
    void testDynamicConfigsWithInvalidPropertiesCluster() {
        var properties = new Properties();
        properties.setProperty("invalidKey", "true");
        var configs = new DynamicConfigs("test-cluster", properties);
        assertTrue(configs.properties().isEmpty());
        assertFalse(configs.sdtEnabled());
    }

    @Test
    void testDynamicConfigsWithValidPropertiesCluster() {
        var properties = new Properties();
        properties.setProperty("clusterSdtEnabled", "true");
        var configs = new DynamicConfigs("test-cluster", properties);
        assertFalse(configs.properties().isEmpty());
        assertTrue(configs.sdtEnabled());
    }

    @Test
    void testOverrideWithPropertiesCluster() {
        var properties = new Properties();
        properties.setProperty("clusterSdtEnabled", "false");
        var configs = new DynamicConfigs("test-cluster", properties);
        assertFalse(configs.sdtEnabled());

        configs.overrideWith(Map.of("test-cluster.cluster.sdt.enabled", "true"));
        assertTrue(configs.sdtEnabled());

        configs.overrideWith(Map.of("test-cluster.cluster.invalidKey", "true"));
        assertTrue(configs.sdtEnabled());

        configs.overrideWith(Map.of("cluster.sdt.enabled", "false"));
        assertTrue(configs.sdtEnabled());
    }

    @Test
    void testOverrideOrderCluster() {
        // namespace/topic level config has higher priority than cluster level config
        var properties = new Properties();
        var configs = new DynamicConfigs("test-cluster", properties);
        // provide both cluster and namespace-level; namespace-level wins
        configs.overrideWith(Map.of(
            "test-cluster.cluster.sdt.enabled", "true",   // cluster level
            "test-cluster.sdt.enabled", "false"           // namespace/topic level
        ));
        assertFalse(configs.sdtEnabled());

        // only configured the cluster level config
        configs = new DynamicConfigs("test-cluster", new Properties());
        configs.overrideWith(Map.of("test-cluster.cluster.sdt.enabled", "true"));
        assertTrue(configs.sdtEnabled());

        // only configured the namespace/topic level config
        configs = new DynamicConfigs("test-cluster", new Properties());
        configs.overrideWith(Map.of("test-cluster.sdt.enabled", "false"));
        assertFalse(configs.sdtEnabled());
    }

    @Test
    void testCopyOfDynamicConfigCluster() {
        var properties = new Properties();
        properties.setProperty("clusterSdtEnabled", "false");
        DynamicConfigs configs = new DynamicConfigs("test-cluster", properties);
        configs.overrideWith(Map.of("test-cluster.sdt.enabled", "true"));

        DynamicConfigs configs1 = DynamicConfigs.of(configs);
        configs1.overrideWith(Map.of("test-cluster.sdt.catalog.name", "a"));

        assertNotEquals(configs1, configs);
    }

    @Test
    void testValidKeysInConfFileCluster() {
        var properties = new Properties();
        properties.setProperty("clusterSdtEnabled", "true");
        properties.setProperty("clusterSdtCatalogName", "catalog1");
        var configs = new DynamicConfigs("test-cluster", properties);
        assertEquals("true", configs.properties().get("clusterSdtEnabled"));
        assertEquals("catalog1", configs.properties().get("clusterSdtCatalogName"));
    }

    @Test
    void testSdtEnabledPriorityCluster() {
        var properties = new Properties();
        var configs = new DynamicConfigs("test-cluster", properties);
        // namespace level wins over cluster level
        configs.overrideWith(Map.of(
            "test-cluster.sdt.enabled", "false",
            "test-cluster.cluster.sdt.enabled", "true"
        ));
        assertFalse(configs.sdtEnabled()); // namespace level wins

        // cluster.sdt.enabled wins over clusterSdtEnabled (default)
        properties = new Properties();
        properties.setProperty("clusterSdtEnabled", "true");
        configs = new DynamicConfigs("test-cluster", properties);
        configs.overrideWith(Map.of("test-cluster.cluster.sdt.enabled", "false"));
        assertFalse(configs.sdtEnabled()); // cluster.sdt.enabled wins over clusterSdtEnabled

        // default only
        properties = new Properties();
        properties.setProperty("clusterSdtEnabled", "true");
        configs = new DynamicConfigs("test-cluster", properties);
        assertTrue(configs.sdtEnabled());
    }

    @Test
    void testSdtEnabledInvalidValuesCluster() {
        var properties = new Properties();
        var configs = new DynamicConfigs("test-cluster", properties);
        configs.overrideWith(Map.of("test-cluster.sdt.enabled", "invalid"));
        assertFalse(configs.sdtEnabled()); // invalid value should be false

        configs = new DynamicConfigs("test-cluster", new Properties());
        configs.overrideWith(Map.of("test-cluster.sdt.enabled", ""));
        assertFalse(configs.sdtEnabled()); // empty value should be false
    }

    @Test
    void testSdtCatalogNamePriorityCluster() {
        var properties = new Properties();
        properties.setProperty("clusterSdtCatalogName", "default-catalog");
        var configs = new DynamicConfigs("test-cluster", properties);
        // namespace wins over cluster level
        configs.overrideWith(Map.of(
            "test-cluster.sdt.catalog.name", "namespace-catalog",
            "test-cluster.cluster.sdt.catalog.name", "cluster-catalog"
        ));
        assertEquals("namespace-catalog", configs.sdtCatalogName().get());

        properties = new Properties();
        properties.setProperty("clusterSdtCatalogName", "default-catalog");
        configs = new DynamicConfigs("test-cluster", properties);
        configs.overrideWith(Map.of("test-cluster.cluster.sdt.catalog.name", "cluster-catalog"));
        assertEquals("cluster-catalog", configs.sdtCatalogName().get());

        properties = new Properties();
        properties.setProperty("clusterSdtCatalogName", "default-catalog");
        configs = new DynamicConfigs("test-cluster", properties);
        assertEquals("default-catalog", configs.sdtCatalogName().get());
    }

    @Test
    void testSdtCatalogNameEmptyCluster() {
        var properties = new Properties();
        var configs = new DynamicConfigs("test-cluster", properties);
        assertTrue(configs.sdtCatalogName().isEmpty());

        properties.setProperty("clusterSdtCatalogName", "");
        configs = new DynamicConfigs("test-cluster", properties);
        assertEquals("", configs.sdtCatalogName().get());
    }

    @Test
    void testSdtConfigsWithOverrideCluster() {
        var properties = new Properties();
        properties.setProperty("clusterSdtEnabled", "false");
        properties.setProperty("clusterSdtCatalogName", "original-catalog");

        var configs = new DynamicConfigs("test-cluster", properties);
        assertFalse(configs.sdtEnabled());
        assertEquals("original-catalog", configs.sdtCatalogName().get());

        configs.overrideWith(Map.of(
            "test-cluster.sdt.enabled", "true",
            "test-cluster.sdt.catalog.name", "new-catalog"
        ));

        assertTrue(configs.sdtEnabled());
        assertEquals("new-catalog", configs.sdtCatalogName().get());
    }

    @Test
    void testSdtConfigsClusterLevelOverrideCluster() {
        var configs = new DynamicConfigs("test-cluster", new Properties());

        configs.overrideWith(Map.of(
            "test-cluster.cluster.sdt.enabled", "true",
            "test-cluster.cluster.sdt.catalog.name", "cluster-catalog"
        ));

        assertTrue(configs.sdtEnabled());
        assertEquals("cluster-catalog", configs.sdtCatalogName().get());

        configs.overrideWith(Map.of(
            "test-cluster.sdt.enabled", "false",
            "test-cluster.sdt.catalog.name", "namespace-catalog"
        ));

        assertFalse(configs.sdtEnabled());
        assertEquals("namespace-catalog", configs.sdtCatalogName().get());
    }

    @Test
    void testOnlyClusterNamePrefixPropertiesAreApplied() {
        // Test that only properties with the cluster name prefix are applied
        var configs = new DynamicConfigs("test-cluster", new Properties());

        // Apply properties with different cluster name prefixes
        configs.overrideWith(Map.of(
            "test-cluster.sdt.enabled", "true",           // correct cluster name - should be applied
            "other-cluster.sdt.enabled", "false",         // different cluster name - should be ignored
            "test-cluster.sdt.catalog.name", "test-catalog", // correct cluster name - should be applied
            "other-cluster.sdt.catalog.name", "other-catalog" // different cluster name - should be ignored
        ));

        // Only properties with "test-cluster" prefix should be applied
        assertTrue(configs.sdtEnabled()); // should be true from test-cluster.sdt.enabled
        assertEquals("test-catalog",
            configs.sdtCatalogName().get()); // should be test-catalog from test-cluster.sdt.catalog.name

        // Test with a different cluster name to verify prefix matching
        var otherConfigs = new DynamicConfigs("other-cluster", new Properties());
        otherConfigs.overrideWith(Map.of(
            "test-cluster.sdt.enabled", "false",          // different cluster name - should be ignored
            "other-cluster.sdt.enabled", "true",          // correct cluster name - should be applied
            "test-cluster.sdt.catalog.name", "test-catalog", // different cluster name - should be ignored
            "other-cluster.sdt.catalog.name", "other-catalog" // correct cluster name - should be applied
        ));

        assertTrue(otherConfigs.sdtEnabled()); // should be true from other-cluster.sdt.enabled
        assertEquals("other-catalog",
            otherConfigs.sdtCatalogName().get()); // should be other-catalog from other-cluster.sdt.catalog.name
    }

    @Test
    void testDataDelayIntervalInSeconds() {
        var configs = DynamicConfigs.of(new Properties());
        assertNull(configs.properties().get("clusterTailCompactDataVisibilityIntervalInSeconds"));

        configs.overrideWith(Map.of(
            "cluster.tail.compact.data.visibility.interval.in.seconds", "150"
        ));

        assertEquals("150", configs.getProperty("tailCompactDataVisibilityIntervalInSeconds").get());

        configs.overrideWith(Map.of(
            "tail.compact.data.visibility.interval.in.seconds", "10"
        ));
        assertEquals("10", configs.getProperty("tailCompactDataVisibilityIntervalInSeconds").get());

        configs = new DynamicConfigs("test-cluster", new Properties());
        assertNull(configs.properties().get("clusterTailCompactDataVisibilityIntervalInSeconds"));
        // If we don't set cluster name prefix, the property should not be applied
        configs.overrideWith(Map.of(
            "cluster.tail.compact.data.visibility.interval.in.seconds", "200"
        ));
        assertEquals(Optional.empty(), configs.getProperty("tailCompactDataVisibilityIntervalInSeconds"));

        configs.overrideWith(Map.of(
            "test-cluster.cluster.tail.compact.data.visibility.interval.in.seconds", "250"
        ));
        assertEquals("250", configs.getProperty("tailCompactDataVisibilityIntervalInSeconds").get());

        configs = new DynamicConfigs("test-cluster", new Properties());
        configs.overrideWith(Map.of(
                "test-cluster.cluster.tail.compact.data.visibility.interval.in.seconds", "50"
        ));
        assertEquals("50", configs.getProperty("tailCompactDataVisibilityIntervalInSeconds").get());
    }

    @Test
    void testCamelToDotLower() {
        var camelConfigName = "clusterConfigA";
        var expectedPropertyName = "cluster.config.a";
        var actualPropertyName = DynamicConfigs.camelToDotLower(camelConfigName);
        assertEquals(expectedPropertyName, actualPropertyName);
    }

    @Test
    void testUpsertModeEnabledAccessor() {
        var configs = DynamicConfigs.of(new Properties());
        assertTrue(configs.upsertModeEnabled().isEmpty());

        configs.overrideWith(Map.of("upsert.mode.enabled", "true"));
        assertEquals(Optional.of(true), configs.upsertModeEnabled());

        configs.overrideWith(Map.of("upsert.mode.enabled", "false"));
        assertEquals(Optional.of(false), configs.upsertModeEnabled());
    }

    @Test
    void testIdentifierFieldsAccessor() {
        var configs = DynamicConfigs.of(new Properties());
        assertTrue(configs.identifierFields().isEmpty());

        // Topic-level (resource level) properties should work
        configs.overrideWith(Map.of("identifier.fields", "key1,key2"));
        assertEquals(Optional.of("key1,key2"), configs.identifierFields());
    }

    @Test
    void testPartitionKeyAccessor() {
        var configs = DynamicConfigs.of(new Properties());
        assertTrue(configs.partitionKey().isEmpty());

        // Topic-level (resource level) properties should work
        configs.overrideWith(Map.of("partition.key", "[{\"sourceColumn\":\"ts\"}]"));
        assertEquals(Optional.of("[{\"sourceColumn\":\"ts\"}]"), configs.partitionKey());
    }

    @Test
    void testTopicOnlyKeysIgnoredFromConfigFile() {
        // identifierFields and partitionKey are topic-specific and should not be loaded
        // from the config file as cluster-wide defaults.
        var properties = new Properties();
        properties.setProperty("clusterIdentifierFields", "defaultKey");
        properties.setProperty("clusterPartitionKey", "defaultPartition");
        properties.setProperty("clusterUpsertModeEnabled", "true");
        var configs = DynamicConfigs.of(properties);

        // Topic-only keys are filtered out from config file
        assertTrue(configs.identifierFields().isEmpty());
        assertTrue(configs.partitionKey().isEmpty());
        // Other keys still work from config file
        assertEquals(Optional.of(true), configs.upsertModeEnabled());
    }

    @Test
    void testTopicOnlyKeysIgnoredAtClusterLevel() {
        // Cluster-level properties (with "cluster." prefix) should be ignored for topic-only keys
        var configs = DynamicConfigs.of(new Properties());

        configs.overrideWith(Map.of(
            "cluster.identifier.fields", "cluster-value",
            "cluster.partition.key", "cluster-partition"
        ));
        // Cluster-level settings are ignored for topic-only keys
        assertTrue(configs.identifierFields().isEmpty());
        assertTrue(configs.partitionKey().isEmpty());
    }

    @Test
    void testTopicOnlyKeysIgnoredAtClusterLevelWithClusterName() {
        var configs = new DynamicConfigs("test-cluster", new Properties());

        configs.overrideWith(Map.of(
            "test-cluster.cluster.identifier.fields", "cluster-value",
            "test-cluster.cluster.partition.key", "cluster-partition"
        ));
        // Cluster-level settings are ignored for topic-only keys
        assertTrue(configs.identifierFields().isEmpty());
        assertTrue(configs.partitionKey().isEmpty());
    }

    @Test
    void testTopicOnlyKeysWorkAtTopicLevel() {
        // Topic-level (resource level, without "cluster." prefix) should work
        var configs = DynamicConfigs.of(new Properties());

        configs.overrideWith(Map.of(
            "identifier.fields", "key1,key2",
            "partition.key", "[{\"sourceColumn\":\"ts\"}]"
        ));
        assertEquals(Optional.of("key1,key2"), configs.identifierFields());
        assertEquals(Optional.of("[{\"sourceColumn\":\"ts\"}]"), configs.partitionKey());
    }

    @Test
    void testTopicOnlyKeysWorkAtTopicLevelWithClusterName() {
        var configs = new DynamicConfigs("test-cluster", new Properties());

        configs.overrideWith(Map.of(
            "test-cluster.identifier.fields", "key1,key2",
            "test-cluster.partition.key", "[{\"sourceColumn\":\"ts\"}]"
        ));
        assertEquals(Optional.of("key1,key2"), configs.identifierFields());
        assertEquals(Optional.of("[{\"sourceColumn\":\"ts\"}]"), configs.partitionKey());
    }

    @Test
    void testToTaskPropertiesIncludesTopicOnlyKeysFromTopicLevel() {
        // When topic-only keys are set at topic level, toTaskProperties should export them
        var configs = DynamicConfigs.of(new Properties());
        configs.overrideWith(Map.of(
            "identifier.fields", "key1,key2",
            "partition.key", "[{\"sourceColumn\":\"ts\"}]"
        ));

        var taskProperties = configs.toTaskProperties();

        // Dot-separated keys
        assertEquals("key1,key2", taskProperties.get("identifier.fields"));
        assertEquals("[{\"sourceColumn\":\"ts\"}]", taskProperties.get("partition.key"));

        // CamelCase aliases for IcebergSinkConfig compatibility
        assertEquals("key1,key2", taskProperties.get("identifierFields"));
        assertEquals("[{\"sourceColumn\":\"ts\"}]", taskProperties.get("partitionKey"));
    }

    @Test
    void testToTaskPropertiesExcludesTopicOnlyKeysFromConfigFile() {
        // When topic-only keys are only set in config file, they should NOT appear in task properties
        var properties = new Properties();
        properties.setProperty("clusterUpsertModeEnabled", "true");
        properties.setProperty("clusterIdentifierFields", "defaultKey");
        properties.setProperty("clusterPartitionKey", "defaultPartition");

        var configs = DynamicConfigs.of(properties);
        var taskProperties = configs.toTaskProperties();

        // upsertModeEnabled works from config file
        assertEquals("true", taskProperties.get("upsert.mode.enabled"));
        assertEquals("true", taskProperties.get("upsertModeEnabled"));

        // Topic-only keys are not exported from config file
        assertNull(taskProperties.get("identifier.fields"));
        assertNull(taskProperties.get("identifierFields"));
        assertNull(taskProperties.get("partition.key"));
        assertNull(taskProperties.get("partitionKey"));
    }

    @Test
    void testNewConfigsPriorityResolution() {
        // Config file level
        var properties = new Properties();
        properties.setProperty("clusterUpsertModeEnabled", "false");
        var configs = DynamicConfigs.of(properties);
        assertEquals(Optional.of(false), configs.upsertModeEnabled());

        // Cluster level overrides config file
        configs.overrideWith(Map.of("cluster.upsert.mode.enabled", "true"));
        assertEquals(Optional.of(true), configs.upsertModeEnabled());

        // Resource (topic/namespace) level overrides cluster level
        configs.overrideWith(Map.of("upsert.mode.enabled", "false"));
        assertEquals(Optional.of(false), configs.upsertModeEnabled());
    }

    @Test
    void testNewConfigsPriorityWithClusterName() {
        var configs = new DynamicConfigs("test-cluster", new Properties());

        configs.overrideWith(Map.of(
            "test-cluster.cluster.upsert.mode.enabled", "true"
        ));
        assertEquals(Optional.of(true), configs.upsertModeEnabled());

        // Topic level overrides cluster level
        configs.overrideWith(Map.of(
            "test-cluster.upsert.mode.enabled", "false"
        ));
        assertEquals(Optional.of(false), configs.upsertModeEnabled());
    }

    @Test
    void testToTaskPropertiesIncludesNewConfigsWithAliases() {
        var properties = new Properties();
        properties.setProperty("clusterUpsertModeEnabled", "true");

        var configs = DynamicConfigs.of(properties);
        var taskProperties = configs.toTaskProperties();

        assertEquals("true", taskProperties.get("upsert.mode.enabled"));
        assertEquals("true", taskProperties.get("upsertModeEnabled"));
    }

    @Test
    void testFromTaskPropertiesRoundTripWithNewConfigs() {
        var baseProperties = new Properties();
        baseProperties.setProperty("clusterUpsertModeEnabled", "false");

        var configs = DynamicConfigs.fromTaskProperties(baseProperties, Map.of(
            "upsert.mode.enabled", "true",
            "identifier.fields", "myKey",
            "partition.key", "myPartition"
        ));

        assertEquals(Optional.of(true), configs.upsertModeEnabled());
        // fromTaskProperties uses overrideWith which accepts topic-level keys at resource level
        assertEquals(Optional.of("myKey"), configs.identifierFields());
        assertEquals(Optional.of("myPartition"), configs.partitionKey());
    }

    @Test
    void testTopicLevelIdentifierFieldsAndPartitionKeyEndToEnd() {
        // Simulate the real flow in PublishCompactTaskRunner.resolveDynamicConfigProperties:
        // 1. Base config has cluster-wide defaults (topic-only keys are ignored)
        // 2. Topic properties carry identifierFields and partitionKey
        // 3. Resolved task properties must include camelCase aliases for IcebergSinkConfig
        var baseProperties = new Properties();
        baseProperties.setProperty("clusterSbtEnabled", "true");
        baseProperties.setProperty("clusterSdtEnabled", "true");
        baseProperties.setProperty("clusterIdentifierFields", "should-be-ignored");
        baseProperties.setProperty("clusterPartitionKey", "should-be-ignored");

        var dynamicConfigs = DynamicConfigs.of(baseProperties);

        // Stream properties set through the external control plane.
        Map<String, String> topicProperties = Map.of(
            "identifier.fields", "key1,key2",
            "partition.key", "[{\"sourceColumn\":\"ts\"}]"
        );

        // Mirror the flow: start with topic properties, override, then merge task properties
        var taskProperties = new HashMap<>(topicProperties);
        dynamicConfigs.overrideWith(taskProperties);
        taskProperties.putAll(dynamicConfigs.toTaskProperties());

        // Verify camelCase aliases are present (required by IcebergSinkConfig)
        assertEquals("key1,key2", taskProperties.get("identifierFields"));
        assertEquals("[{\"sourceColumn\":\"ts\"}]", taskProperties.get("partitionKey"));

        // Verify dot-separated keys are also present
        assertEquals("key1,key2", taskProperties.get("identifier.fields"));
        assertEquals("[{\"sourceColumn\":\"ts\"}]", taskProperties.get("partition.key"));

        // Verify config-file defaults for topic-only keys did NOT leak through
        assertNotEquals("should-be-ignored", taskProperties.get("identifierFields"));
        assertNotEquals("should-be-ignored", taskProperties.get("partitionKey"));

        // Verify other dynamic configs still resolve from config file
        assertEquals("true", taskProperties.get("sbt.enabled"));
        assertEquals("true", taskProperties.get("sdt.enabled"));
    }

    @Test
    void testTopicLevelIdentifierFieldsAndPartitionKeyWithClusterName() {
        // Simulate a cluster-scoped control-plane flow.
        var baseProperties = new Properties();
        baseProperties.setProperty("clusterSdtEnabled", "true");
        var dynamicConfigs = new DynamicConfigs("test-cluster", baseProperties);

        // Topic properties from getTaskProperties() have cluster name prefix
        Map<String, String> topicProperties = Map.of(
            "test-cluster.identifier.fields", "id1,id2",
            "test-cluster.partition.key", "[{\"sourceColumn\":\"col1\"}]",
            "test-cluster.upsert.mode.enabled", "true"
        );

        var taskProperties = new HashMap<>(topicProperties);
        dynamicConfigs.overrideWith(taskProperties);
        taskProperties.putAll(dynamicConfigs.toTaskProperties());

        // camelCase aliases present for IcebergSinkConfig compatibility
        assertEquals("id1,id2", taskProperties.get("identifierFields"));
        assertEquals("[{\"sourceColumn\":\"col1\"}]", taskProperties.get("partitionKey"));
        assertEquals("true", taskProperties.get("upsertModeEnabled"));

        // dot-separated keys also present
        assertEquals("id1,id2", taskProperties.get("identifier.fields"));
        assertEquals("[{\"sourceColumn\":\"col1\"}]", taskProperties.get("partition.key"));
    }

    @Test
    void testTopicLevelOverridesDoNotLeakClusterDefaults() {
        // Simulate: cluster-level sn/system has identifierFields, but topic overrides it
        var baseProperties = new Properties();
        var dynamicConfigs = new DynamicConfigs("test-cluster", baseProperties);

        // First overrideWith: cluster-level (sn/system) tries to set identifier.fields
        dynamicConfigs.overrideWith(Map.of(
            "test-cluster.cluster.identifier.fields", "cluster-default"
        ));
        // Cluster-level is ignored for topic-only keys
        assertTrue(dynamicConfigs.identifierFields().isEmpty());

        // Second overrideWith: topic-level sets identifier.fields
        dynamicConfigs.overrideWith(Map.of(
            "test-cluster.identifier.fields", "topic-value"
        ));
        assertEquals(Optional.of("topic-value"), dynamicConfigs.identifierFields());

        // toTaskProperties emits the topic-level value with alias
        var taskProperties = dynamicConfigs.toTaskProperties();
        assertEquals("topic-value", taskProperties.get("identifierFields"));
        assertEquals("topic-value", taskProperties.get("identifier.fields"));
    }

    @Test
    void testTopicPropertiesWithoutTopicOnlyKeysStillWork() {
        // When topic properties don't include identifierFields/partitionKey,
        // they should remain empty (not filled from config-file defaults)
        var baseProperties = new Properties();
        baseProperties.setProperty("clusterSdtEnabled", "true");
        baseProperties.setProperty("clusterIdentifierFields", "should-not-appear");

        var dynamicConfigs = DynamicConfigs.of(baseProperties);

        // Topic only sets sdt-related properties, no identifierFields
        dynamicConfigs.overrideWith(Map.of("sdt.enabled", "false"));

        var taskProperties = dynamicConfigs.toTaskProperties();
        assertNull(taskProperties.get("identifierFields"));
        assertNull(taskProperties.get("identifier.fields"));
        assertNull(taskProperties.get("partitionKey"));
        assertNull(taskProperties.get("partition.key"));
        assertEquals("false", taskProperties.get("sdt.enabled"));
    }

    @Test
    void testDynamicConfigsAreNotWorkingWithCheckClusterNameDisabled() {
        Properties clusterConfiguration = new Properties();
        clusterConfiguration.put("clusterSdtEnabled", "true");
        var config = new DynamicConfigs("cluster-name", clusterConfiguration, false);

        assertTrue(config.sdtEnabled());

        // cluster name prefix doesn't work because we skipped check the cluster name
        config.overrideWith(Map.of("cluster-name.cluster.sdt.enabled", "false"));
        assertFalse(config.sdtEnabled());

        config.overrideWith(Map.of("sdt.enabled", "true"));
        assertTrue(config.sdtEnabled());

        config.overrideWith(Map.of("sdt.enabled", "false"));
        assertFalse(config.sdtEnabled());
    }

    @Test
    void testBaseSchemaVersionUnset() {
        var configs = DynamicConfigs.of(new Properties());
        assertTrue(configs.baseSchemaVersion().isEmpty());
    }

    @Test
    void testBaseSchemaVersionTopicLevelProperty() {
        var configs = DynamicConfigs.of(new Properties());
        configs.overrideWith(Map.of("base.schema.version", "5"));
        assertEquals(Optional.of(5L), configs.baseSchemaVersion());
    }

    @Test
    void testBaseSchemaVersionRejectsClusterLevelProperty() {
        // base.schema.version is topic-level only — cluster.base.schema.version must be ignored.
        var configs = DynamicConfigs.of(new Properties());
        configs.overrideWith(Map.of("cluster.base.schema.version", "7"));
        assertTrue(configs.baseSchemaVersion().isEmpty());
    }

    @Test
    void testBaseSchemaVersionRejectsConfigFileDefault() {
        // Topic-level only keys must not be loaded from the configuration file as cluster-wide defaults.
        var properties = new Properties();
        properties.setProperty("clusterBaseSchemaVersion", "9");
        var configs = DynamicConfigs.of(properties);
        assertTrue(configs.baseSchemaVersion().isEmpty());
    }

    @Test
    void testBaseSchemaVersionLenientParse() {
        // Non-numeric value: log warn, treat as unset (matches upsertModeEnabled lenient pattern).
        var configs = DynamicConfigs.of(new Properties());
        configs.overrideWith(Map.of("base.schema.version", "not-a-number"));
        assertTrue(configs.baseSchemaVersion().isEmpty());
    }

    @Test
    void testFromPropertiesHandlesNull() {
        // Mockito @Mock LakehouseConfiguration returns null for getProperties();
        // fromProperties must treat that as empty rather than NPE.
        var configs = DynamicConfigs.fromProperties(null);
        assertTrue(configs.baseSchemaVersion().isEmpty());
        assertFalse(configs.sdtEnabled());
    }

    @Test
    void testFromPropertiesPreservesValues() {
        var properties = new Properties();
        properties.setProperty("base.schema.version", "5");
        var configs = DynamicConfigs.fromProperties(properties);
        assertEquals(Optional.of(5L), configs.baseSchemaVersion());
    }

}
