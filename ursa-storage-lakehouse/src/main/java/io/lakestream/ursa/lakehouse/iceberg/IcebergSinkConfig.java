/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.lakehouse.iceberg;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.iceberg.util.PropertyUtil;

@Slf4j
public class IcebergSinkConfig {

    public static final String CATALOG_PROP_PREFIX = "iceberg.catalog.";
    public static final String HADOOP_PROP_PREFIX = "iceberg.hadoop.";
    public static final String WRITE_PROP_PREFIX = "iceberg.write-props.";
    public static final String TABLE_PROP_PREFIX = "iceberg.table-props.";

    public static final String IDENTIFIER_FIELDS = "identifierFields";
    public static final String PARTITION_KEY = "partitionKey";
    public static final String COMMIT_BRANCH = "commitBranch";
    public static final String HADOOP_CONF_DIR_PROP = "iceberg.hadoop-conf-dir";

    @Getter
    private final Map<String, String> originalProps;
    @Getter
    private final Map<String, String> catalogProps;
    @Getter
    private final Map<String, String> hadoopProps;
    @Getter
    private final Map<String, String> writeProps;
    @Getter
    private final Map<String, String> tableProps;

    public static final String TABLE_CDC_FIELD_PROP = "iceberg.table.cdc-field";
    public static final String TABLE_UPSERT_MODE_ENABLED_PROP = "iceberg.table.upsert-mode-enabled";
    public static final String UPSERT_MODE_ENABLED = "upsertModeEnabled";
    public static final String TABLE_EVOLVE_SCHEMA_ENABLED_PROP = "iceberg.table.evolve-schema-enabled";
    public static final String TABLE_SCHEMA_FORCE_OPTIONAL_PROP = "iceberg.table.schema-force-optional";
    public static final String TABLE_SCHEMA_CASE_INSENSITIVE_PROP = "iceberg.table.schema-case-insensitive";
    public static final String TABLE_SCHEMA_UPDATE_RETRIES_PROP = "iceberg.table.schema-update-retries";
    public static final String TABLE_CREATE_TABLE_RETRIES_PROP = "iceberg.table.create-table-retries";

    private static final String TABLE_DEFAULT_COMMIT_BRANCH = null;
    private static final String TABLE_DEFAULT_IDENTIFIER_FIELDS = null;
    private static final String TABLE_DEFAULT_PARTITION_KEY = null;
    private static final String TABLE_DEFAULT_CDC_FIELD = null;
    private static final boolean TABLE_DEFAULT_UPSERT_MODE_ENABLED = false;
    private static final boolean TABLE_DEFAULT_EVOLVE_SCHEMA_ENABLED = false;
    private static final boolean TABLE_DEFAULT_SCHEMA_FORCE_OPTIONAL = false;
    private static final boolean TABLE_DEFAULT_SCHEMA_CASE_INSENSITIVE = false;
    public static final int SCHEMA_UPDATE_DEFAULT_RETRIES = 2; // 3 total attempts
    public static final int CREATE_TABLE_DEFAULT_RETRIES = 2; // 3 total attempts


    public IcebergSinkConfig(Properties props) {
        this.originalProps = new HashMap<>();
        props.forEach((key, value) -> originalProps.put(key.toString(), value.toString()));
        this.catalogProps = PropertyUtil.propertiesWithPrefix(originalProps, CATALOG_PROP_PREFIX);
        this.hadoopProps = PropertyUtil.propertiesWithPrefix(originalProps, HADOOP_PROP_PREFIX);
        this.writeProps = PropertyUtil.propertiesWithPrefix(originalProps, WRITE_PROP_PREFIX);
        this.tableProps = PropertyUtil.propertiesWithPrefix(originalProps, TABLE_PROP_PREFIX);
    }


    public List<String> getPrimaryKeys() {
        String identifierFieldsStr = originalProps.getOrDefault(IDENTIFIER_FIELDS, TABLE_DEFAULT_IDENTIFIER_FIELDS);

        if (StringUtils.isBlank(identifierFieldsStr)) {
            return Collections.emptyList();
        } else {
            return List.of(Arrays.stream(identifierFieldsStr.split(","))
                .map(String::strip).toArray(String[]::new));
        }
    }

    public String getPartitionKey() {
        return originalProps.getOrDefault(PARTITION_KEY, TABLE_DEFAULT_PARTITION_KEY);
    }

    public String getCommitBranch() {
        return originalProps.getOrDefault(COMMIT_BRANCH, TABLE_DEFAULT_COMMIT_BRANCH);
    }

    public String getCdcField() {
        return originalProps.getOrDefault(TABLE_CDC_FIELD_PROP, TABLE_DEFAULT_CDC_FIELD);
    }

    // TODO: Add unit test for this fallback.
    public boolean isUpsertModeEnabled() {
        String upsertModeEnabledKey = originalProps.get(UPSERT_MODE_ENABLED) != null
            ? UPSERT_MODE_ENABLED : TABLE_UPSERT_MODE_ENABLED_PROP;
        return Boolean.parseBoolean(originalProps.getOrDefault(upsertModeEnabledKey,
            String.valueOf(TABLE_DEFAULT_UPSERT_MODE_ENABLED)));
    }

    public boolean isEvolveSchemaEnabled() {
        return Boolean.parseBoolean(originalProps.getOrDefault(TABLE_EVOLVE_SCHEMA_ENABLED_PROP,
            String.valueOf(TABLE_DEFAULT_EVOLVE_SCHEMA_ENABLED)));
    }

    public boolean isSchemaForceOptional() {
        return Boolean.parseBoolean(originalProps.getOrDefault(TABLE_SCHEMA_FORCE_OPTIONAL_PROP,
            String.valueOf(TABLE_DEFAULT_SCHEMA_FORCE_OPTIONAL)));
    }

    public boolean isSchemaCaseInsensitive() {
        return Boolean.parseBoolean(originalProps.getOrDefault(TABLE_SCHEMA_CASE_INSENSITIVE_PROP,
            String.valueOf(TABLE_DEFAULT_SCHEMA_CASE_INSENSITIVE)));
    }

    public int getSchemaUpdateRetries() {
        return Integer.parseInt(originalProps.getOrDefault(TABLE_SCHEMA_UPDATE_RETRIES_PROP,
            String.valueOf(SCHEMA_UPDATE_DEFAULT_RETRIES)));
    }

    public int getCreateTableRetries() {
        return Integer.parseInt(originalProps.getOrDefault(TABLE_CREATE_TABLE_RETRIES_PROP,
            String.valueOf(CREATE_TABLE_DEFAULT_RETRIES)));
    }

    public String hadoopConfDir() {
        return originalProps.getOrDefault(HADOOP_CONF_DIR_PROP, null);
    }
}
