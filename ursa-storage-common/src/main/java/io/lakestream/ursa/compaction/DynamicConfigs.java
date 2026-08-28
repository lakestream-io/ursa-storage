/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.compaction;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.jetbrains.annotations.NotNull;

/**
 * DynamicConfigs collect the compaction service configurations from the namespace/topic properties.
 * It allows to change the configuration on each compaction task by the task properties.
 */
@Slf4j
public class DynamicConfigs {

    @Getter
    private String clusterName;
    @Getter
    private boolean checkClusterName = true;
    private ConcurrentMap<String, String> properties;
    private final Set<String> validKeysInConfFile;
    private final Set<String> validKeysInProperties;

    // Valid keys in the configuration file.
    // It will auto convert those configuration to a properties key
    // For example, if you define a key in the configuration file, like 'clusterSdtEnabled', you can configure it by
    // setting topic/namespace properties. The property key will be 'sdt.enabled'.
    private static final Set<String> VALID_KEYS_IN_CONF_FILE = Set.of(
        "clusterSdtEnabled",
        "clusterSdtSuspended",
        "clusterSbtEnabled",
        "clusterSdtCatalogName",
        "clusterTailCompactDataVisibilityIntervalInSeconds",
        "clusterUpsertModeEnabled",
        "clusterIdentifierFields",
        "clusterPartitionKey",
        "clusterBaseSchemaVersion",
        "clusterCommitBatchSize"
    );

    private static final Set<String> TASK_EXPOSED_CONFIG_KEYS = Set.of(
        "clusterSbtEnabled",
        "clusterSdtEnabled",
        "clusterSdtCatalogName",
        "clusterUpsertModeEnabled",
        "clusterIdentifierFields",
        "clusterPartitionKey",
        "clusterBaseSchemaVersion",
        "clusterCommitBatchSize"
    );

    // Aliases map config-file keys to camelCase task property names that IcebergSinkConfig expects.
    // toTaskProperties() emits both the standard dot-separated key and the alias.
    private static final Map<String, String> TASK_PROPERTY_ALIASES = Map.of(
        "clusterUpsertModeEnabled", "upsertModeEnabled",
        "clusterIdentifierFields", "identifierFields",
        "clusterPartitionKey", "partitionKey"
    );

    // Topic-level only keys: these are topic-specific properties (e.g., partition key, identifier fields)
    // that should not be set as cluster-wide or namespace-wide defaults.
    // They are kept in VALID_KEYS_IN_CONF_FILE so their dot-separated forms are recognized in overrideWith(),
    // but are excluded from config-file defaults (constructor) and cluster-level overrides.
    private static final Set<String> TOPIC_LEVEL_ONLY_KEYS = Set.of(
        "clusterIdentifierFields",
        "clusterPartitionKey",
        "clusterBaseSchemaVersion"
    );

    // Pre-computed cluster-level dot-separated forms of topic-only keys, used to filter
    // them out in overrideWith() step 2 (cluster-level properties with "cluster." prefix).
    private static final Set<String> TOPIC_LEVEL_ONLY_CLUSTER_PROPERTY_KEYS;
    static {
        TOPIC_LEVEL_ONLY_CLUSTER_PROPERTY_KEYS = TOPIC_LEVEL_ONLY_KEYS.stream()
            .map(DynamicConfigs::camelToDotLower)
            .collect(Collectors.toUnmodifiableSet());
    }

    private static final Set<String> EXTRA_VALID_KEYS_IN_CONF_FILE =
        Optional.ofNullable(
            System.getenv("LAKEHOUSE_DYNAMIC_EXTRA_VALID_KEYS_IN_CONF_FILE"))
            .map(s -> Arrays.stream(s.split(","))
                .map(String::trim)
                .filter(k -> !k.isEmpty())
                .collect(Collectors.toUnmodifiableSet()))
            .orElseGet(Set::of);

    // used for testing the existing behaviours purpose
    public static DynamicConfigs of (Properties properties) {
        return new DynamicConfigs("", properties.stringPropertyNames().stream()
            .collect(Collectors.toConcurrentMap(key -> key, properties::getProperty)), false);
    }

    public static DynamicConfigs of(DynamicConfigs dynamicConfigs) {
        return new DynamicConfigs(dynamicConfigs.getClusterName(),
            new ConcurrentHashMap<>(dynamicConfigs.properties()), dynamicConfigs.isCheckClusterName());
    }

    public DynamicConfigs(String clusterName, Properties properties) {
        this(clusterName,  properties.stringPropertyNames().stream()
            .collect(Collectors.toConcurrentMap(key -> key, properties::getProperty)), true);
    }

    public DynamicConfigs(String clusterName, Properties properties, boolean checkClusterName) {
        this(clusterName, properties.stringPropertyNames().stream()
            .collect(Collectors.toConcurrentMap(key -> key, properties::getProperty)), checkClusterName);
    }

    public DynamicConfigs(String clusterName, ConcurrentMap<String, String> properties, boolean checkClusterName) {
        this.validKeysInConfFile = getValidKeysForConfigFile();
        this.validKeysInProperties = getValidKeysForProperties();
        this.properties = properties.entrySet().stream()
            .map(e -> Map.entry(formatKeyInConfigFile(e.getKey()), e.getValue()))
            .filter(e -> validKeyInConfFile().contains(e.getKey()))
            // Topic-level only keys should not be loaded from the config file as cluster-wide defaults
            .filter(e -> !TOPIC_LEVEL_ONLY_KEYS.contains(e.getKey()))
            .collect(Collectors.toConcurrentMap(
                Map.Entry::getKey,
                Map.Entry::getValue));
        this.clusterName = clusterName;
        this.checkClusterName = checkClusterName;
    }

    public ConcurrentMap<String, String> properties() {
        return properties;
    }

    private Set<String> validKeyInConfFile() {
        return validKeysInConfFile;
    }

    private Set<String> validKeysInProperties() {
        return validKeysInProperties;
    }

    /**
     * Override the dynamic configurations with the topic/namespace properties.
     *
     * @param givenProperties
     *          properties comes from the topic/namespace
     */
    public void overrideWith(@NotNull Map<String, String> givenProperties) {
        var properties = givenProperties;
        if (checkClusterName) {
            // filter out the properties with the cluster name
            properties = givenProperties.entrySet().stream()
                .filter(e -> e.getKey().startsWith(clusterName))
                .collect(Collectors.toMap(
                    e -> e.getKey().replaceFirst(clusterName + ".", ""),
                    Map.Entry::getValue));
        } else {
            // remove the cluster name from the properties, used to compatible with the old logic
            properties = givenProperties.entrySet().stream()
                .map(e -> {
                    var key = e.getKey();
                    if (e.getKey().startsWith(clusterName + ".")) {
                        key = key.replaceFirst(clusterName + ".", "");
                    }
                    return Map.entry(key, e.getValue());
                })
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        }

        // override the properties with the order:
        // 1. namespace/topic level configurations
        properties.entrySet().stream()
                .filter(entry -> validKeysInProperties().contains(entry.getKey()))
                .filter(entry -> !entry.getKey().startsWith("cluster."))
                .forEach(entry -> this.properties.put(entry.getKey(), entry.getValue()));

        // 2. put cluster level configurations (with "cluster." prefix) if the key is absent
        // Topic-level only keys (e.g., identifier.fields, partition.key) are skipped here
        // because they should not be set as cluster-wide defaults.
        properties.entrySet().stream()
                .filter(entry -> validKeysInProperties().contains(entry.getKey()))
                .filter(entry -> entry.getKey().startsWith("cluster."))
                .filter(entry -> !TOPIC_LEVEL_ONLY_CLUSTER_PROPERTY_KEYS.contains(entry.getKey()))
                .forEach(entry -> {
                    String withoutPrefix = entry.getKey().substring("cluster.".length());
                    this.properties.putIfAbsent(withoutPrefix, entry.getValue());
                });

    }

    public boolean sdtEnabled() {
        var value = getProperty("clusterSdtEnabled");
        return value.map(Boolean::parseBoolean).orElse(false);
    }

    public boolean sdtSuspended() {
        var value = getProperty("clusterSdtSuspended");
        return value.map(Boolean::parseBoolean).orElse(false);
    }

    public boolean sbtEnabled() {
        var value = getProperty("clusterSbtEnabled");
        return value.map(Boolean::parseBoolean).orElse(sdtEnabled());
    }

    public Optional<String> sdtCatalogName() {
        return getProperty("clusterSdtCatalogName");
    }

    public Optional<Boolean> upsertModeEnabled() {
        return getProperty("clusterUpsertModeEnabled").map(Boolean::parseBoolean);
    }

    public Optional<String> identifierFields() {
        return getProperty("clusterIdentifierFields");
    }

    public Optional<String> partitionKey() {
        return getProperty("clusterPartitionKey");
    }

    public Optional<Long> baseSchemaVersion() {
        return getProperty("clusterBaseSchemaVersion").flatMap(value -> {
            try {
                return Optional.of(Long.parseLong(value));
            } catch (NumberFormatException e) {
                log.warn("Invalid clusterBaseSchemaVersion value '{}', treating as unset", value);
                return Optional.empty();
            }
        });
    }

    /**
     * Records-per-commit-batch for the materialization sink (e.g. the ClickHouse INSERT batch size).
     * Empty when unset, letting the sink apply its own default.
     */
    public Optional<Integer> commitBatchSize() {
        return getProperty("clusterCommitBatchSize").flatMap(value -> {
            try {
                int parsed = Integer.parseInt(value);
                return parsed > 0 ? Optional.of(parsed) : Optional.empty();
            } catch (NumberFormatException e) {
                log.warn("Invalid clusterCommitBatchSize value '{}', treating as unset", value);
                return Optional.empty();
            }
        });
    }

    public Map<String, String> toTaskProperties() {
        return toTaskProperties(TASK_EXPOSED_CONFIG_KEYS);
    }

    public Map<String, String> toTaskProperties(Set<String> exposedConfigKeys) {
        Map<String, String> taskProperties = new HashMap<>();
        exposedConfigKeys.stream()
            .filter(validKeyInConfFile()::contains)
            .forEach(configKey -> getProperty(configKey).ifPresent(value -> {
                taskProperties.put(toTaskPropertyKey(configKey), value);
                String alias = TASK_PROPERTY_ALIASES.get(configKey);
                if (alias != null) {
                    taskProperties.put(alias, value);
                }
            }));
        return taskProperties;
    }

    public static DynamicConfigs fromTaskProperties(Properties baseProperties, Map<String, String> taskProperties) {
        // in the compaction worker, the dynamic configs only resolved the task properties. When the dynamic configs
        // put into the task properties, it will remove the cluster name prefix for the property. So we don't need
        // to check the cluster name in the compaction worker.
        var dynamicConfigs = new DynamicConfigs("", baseProperties, false);
        dynamicConfigs.overrideWith(taskProperties);
        return dynamicConfigs;
    }

    public static DynamicConfigs fromProperties(Properties properties) {
        if (properties == null) {
            return fromTaskProperties(Map.of());
        }
        Map<String, String> taskProperties = properties.entrySet().stream()
            .collect(Collectors.toMap(
                e -> String.valueOf(e.getKey()),
                e -> String.valueOf(e.getValue())));
        return fromTaskProperties(taskProperties);
    }

    public static DynamicConfigs fromTaskProperties(Map<String, String> taskProperties) {
        return fromTaskProperties(new Properties(), taskProperties);
    }

    public Optional<String> getProperty(String configName) {
        configName = formatKeyInConfigFile(configName);

        // get properties from topic/namespace level
        var clusterLevelPropertyName = camelToDotLower(configName);
        var resourceLevelPropertyName = getResourceLevelPropertyName(clusterLevelPropertyName);

        // check the resource level
        var resourceValue = this.properties.get(resourceLevelPropertyName);
        if (resourceValue != null) {
            return Optional.of(resourceValue);
        }

        // check cluster level
        var clusterValue = this.properties.get(clusterLevelPropertyName);
        if (clusterValue != null) {
            return Optional.of(clusterValue);
        }

        // default in the configuration file
        var configValue = this.properties.get(configName);
        if (configValue != null) {
            return Optional.of(configValue);
        }

        return Optional.empty();
    }

    static Set<String> getValidKeysForConfigFile() {
        return Stream.concat(VALID_KEYS_IN_CONF_FILE.stream(), EXTRA_VALID_KEYS_IN_CONF_FILE.stream())
            .collect(Collectors.toUnmodifiableSet());
    }

    static Set<String> getValidKeysForProperties() {
        return getValidKeysForConfigFile().stream()
            .flatMap(k -> {
                var clusterLevelPropertyName = camelToDotLower(k);
                var resourceLevelPropertyName = getResourceLevelPropertyName(clusterLevelPropertyName);
                return Stream.of(clusterLevelPropertyName, resourceLevelPropertyName);

            })
            .collect(Collectors.toUnmodifiableSet());
    }

    // the clusterLevelPropertyName must start with the 'cluster.' prefix. Otherwise, it will be treated as a
    // resource level property name. The resource level measn the topic or namespace level.
    static String getResourceLevelPropertyName(String clusterLevelPropertyName) {
        if (clusterLevelPropertyName.startsWith("cluster.")) {
            return clusterLevelPropertyName.substring("cluster.".length());
        }
        return clusterLevelPropertyName;
    }

    static String toTaskPropertyKey(String configName) {
        return getResourceLevelPropertyName(camelToDotLower(formatKeyInConfigFile(configName)));
    }

    static String formatKeyInConfigFile(String propertyName) {
        if (propertyName == null || propertyName.isEmpty()) {
            return propertyName;
        }
        if (propertyName.startsWith("cluster")) {
            return propertyName;
        }
        char[] chars = propertyName.toCharArray();
        chars[0] = Character.toUpperCase(chars[0]);
        var newPropertyName =  new String(chars);
        return "cluster" + newPropertyName;
    }

    static String camelToDotLower(String camel) {
        StringBuilder sb = new StringBuilder();
        for (char c : camel.toCharArray()) {
            if (Character.isUpperCase(c)) {
                sb.append('.').append(Character.toLowerCase(c));
            } else {
                sb.append(c);
            }
        }
        return sb.toString();
    }

    @Override
    public String toString() {
        return "DynamicConfigs{"
               + "clusterName='" + clusterName + '\''
               + ", checkClusterName=" + checkClusterName
               + ", properties=" + properties
               + '}';
    }
}
