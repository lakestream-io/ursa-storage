/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.lakehouse;

import static com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystemConfiguration.GCS_PROJECT_ID;
import static com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystemConfiguration.GCS_ROOT_URL;
import static io.lakestream.ursa.lakehouse.iceberg.IcebergSinkConfig.CATALOG_PROP_PREFIX;
import static io.lakestream.ursa.lakehouse.iceberg.IcebergSinkConfig.COMMIT_BRANCH;
import static io.lakestream.ursa.lakehouse.iceberg.IcebergSinkConfig.HADOOP_CONF_DIR_PROP;
import static io.lakestream.ursa.lakehouse.iceberg.IcebergSinkConfig.HADOOP_PROP_PREFIX;
import static io.lakestream.ursa.lakehouse.iceberg.IcebergSinkConfig.IDENTIFIER_FIELDS;
import static io.lakestream.ursa.lakehouse.iceberg.IcebergSinkConfig.PARTITION_KEY;
import static io.lakestream.ursa.lakehouse.iceberg.IcebergSinkConfig.TABLE_CDC_FIELD_PROP;
import static io.lakestream.ursa.lakehouse.iceberg.IcebergSinkConfig.TABLE_CREATE_TABLE_RETRIES_PROP;
import static io.lakestream.ursa.lakehouse.iceberg.IcebergSinkConfig.TABLE_EVOLVE_SCHEMA_ENABLED_PROP;
import static io.lakestream.ursa.lakehouse.iceberg.IcebergSinkConfig.TABLE_PROP_PREFIX;
import static io.lakestream.ursa.lakehouse.iceberg.IcebergSinkConfig.TABLE_SCHEMA_CASE_INSENSITIVE_PROP;
import static io.lakestream.ursa.lakehouse.iceberg.IcebergSinkConfig.TABLE_SCHEMA_FORCE_OPTIONAL_PROP;
import static io.lakestream.ursa.lakehouse.iceberg.IcebergSinkConfig.TABLE_SCHEMA_UPDATE_RETRIES_PROP;
import static io.lakestream.ursa.lakehouse.iceberg.IcebergSinkConfig.TABLE_UPSERT_MODE_ENABLED_PROP;
import static io.lakestream.ursa.lakehouse.iceberg.IcebergSinkConfig.UPSERT_MODE_ENABLED;
import static io.lakestream.ursa.lakehouse.iceberg.IcebergSinkConfig.WRITE_PROP_PREFIX;
import static org.apache.hadoop.fs.s3a.Constants.AWS_CREDENTIALS_PROVIDER;
import static org.apache.hadoop.fs.s3a.Constants.AWS_REGION;
import static org.apache.hadoop.fs.s3a.Constants.ENDPOINT;
import static org.apache.hadoop.fs.s3a.Constants.REQUEST_TIMEOUT;
import static org.apache.hadoop.fs.s3a.Constants.SOCKET_TIMEOUT;

import com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem;
import io.lakestream.ursa.compaction.DynamicConfigs;
import io.lakestream.ursa.lakehouse.compact.CompactFileType;
import io.lakestream.ursa.lakehouse.iceberg.IcebergCatalogBackendType;
import io.lakestream.ursa.storage.impl.StorageConfig;
import java.io.IOException;
import java.net.InetAddress;
import java.net.URI;
import java.net.UnknownHostException;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import lombok.Data;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Hdfs;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.azure.NativeAzureFileSystem;
import org.apache.hadoop.fs.azurebfs.AzureBlobFileSystem;
import org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys;
import org.apache.hadoop.fs.azurebfs.oauth2.WorkloadIdentityTokenProvider;
import org.apache.hadoop.fs.local.LocalFs;
import org.apache.hadoop.fs.s3a.S3AFileSystem;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.CatalogUtil;
import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.record.CompressionType;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider;
import software.amazon.awssdk.auth.credentials.WebIdentityTokenFileCredentialsProvider;

@Slf4j
@Data
public class LakehouseConfiguration {
    public static final String STORAGE_PATH = "storagePath";
    public static final String DIRECT_EXTERNAL_STORAGE_PATH = "directExternalStoragePath";
    public static final String HADOOP_CONF_PREFIX = "hadoop.";
    public static final String GCS_ID = "googleCloudProjectID";
    public static final String GCS_SA = "googleCloudServiceAccountFile";
    public static final String DELTA_PREFIX = "delta.";
    public static final String DELTA_CATALOG_PREFIX = "delta.catalog.";
    public static final String ICEBERG_PREFIX = "iceberg.";
    public static final String ICEBERG_CATALOG_PREFIX = "iceberg.catalog.";
    public static final String PARQUET_COMPRESSION_TYPE = "compressType";
    private static final String DEFAULT_PARQUET_COMPRESSION_TYPE = CompressionCodecName.ZSTD.name();
    private static final String DELTA_ROW_GROUP_SIZE = "rowGroupSize";
    private static final String DEFAULT_DELTA_ROW_GROUP_SIZE = "10485760";
    public static final String LAKEHOUSE_COMMIT_MAX_RETRY_TIMES = "lakehouseCommitMaxRetryTimes";
    public static final String DEFAULT_LAKEHOUSE_COMMIT_MAX_RETRY_TIMES = "3";
    public static final String NONE_PARTITION_KEY = "none";
    public static final String FIXED_PARTITION_KEY = "__partition";
    public static final String ICEBERG_CREDENTIAL_FILE = "iceberg.credentialFile";
    public static final String UNITY_CATALOG_TOKEN_FILE = "unityCatalogTokenFile";
    public static final String STREAM_TABLE_MODE = "streamTableMode";
    public static final String DEFAULT_CATALOG_NAME = "catalog.default";
    public static final String CATALOG_BACKEND_TYPE = "catalog-backend";
    // This configuration is used to set catalog name in namespace or topic properties,
    // and we use the catalog name to get catalog information from Lakehouse Configuration.
    public static final String CATALOG_NAME = "catalog.name";
    public static final String DLT_SUFFIX = "dlt.suffix";
    public static final String DEFAULT_DLT_SUFFIX = "_dlt";
    public static final String DELTA_DLT_ENABLED = "delta.dlt.enabled";

    // Checks if input schema and table schema are same(default: false)
    public static final String CHECK_ORDERING = "check-ordering";
    public static final boolean CHECK_ORDERING_DEFAULT = false;
    //  Sets the nullable check on fields(default: true)
    public static final String CHECK_NULLABILITY = "check-nullability";
    public static final boolean CHECK_NULLABILITY_DEFAULT = true;
    //  Adds newly-introduced fields as optional/nullable during schema evolution
    //  instead of routing the record to the DLT (default: true)
    public static final String MAKE_NEW_FIELDS_OPTIONAL = "make-new-fields-optional";
    public static final boolean MAKE_NEW_FIELDS_OPTIONAL_DEFAULT = false;

    public static final Set<String> TOPIC_PROPERTIES_FOR_EXTERNAL_TABLE = Set.of(IDENTIFIER_FIELDS, PARTITION_KEY,
        COMMIT_BRANCH, TABLE_CDC_FIELD_PROP, TABLE_UPSERT_MODE_ENABLED_PROP, TABLE_EVOLVE_SCHEMA_ENABLED_PROP,
        UPSERT_MODE_ENABLED, TABLE_SCHEMA_FORCE_OPTIONAL_PROP, TABLE_SCHEMA_CASE_INSENSITIVE_PROP,
        TABLE_CREATE_TABLE_RETRIES_PROP, TABLE_SCHEMA_UPDATE_RETRIES_PROP, HADOOP_CONF_DIR_PROP, CATALOG_NAME,
        DELTA_DLT_ENABLED);

    public static final Set<String> TOPIC_PROPERTIES_PREFIX =
        Set.of(CATALOG_PROP_PREFIX, HADOOP_PROP_PREFIX, WRITE_PROP_PREFIX, TABLE_PROP_PREFIX, STREAM_TABLE_MODE);

    // Kafka
    public static final String KAFKA_COMPRESSION_TYPE = "kafka.compression.type";
    public static final String DEFAULT_KAFKA_COMPRESSION_TYPE = CompressionType.LZ4.name;

    public static final String DEFAULT_STORAGE_PATH = Paths.get("data").toAbsolutePath().toString();

    @Getter
    protected final Properties properties = new Properties();
    private final Configuration hadoopConfiguration;

    private final Map<String, String> deltaProperties;
    private final Map<String, Map<String, String>> deltaCatalogPropertiesMap;
    private final Map<String, String> icebergProperties;
    private final Map<String, Map<String, String>> icebergCatalogPropertiesMap;
    private final String defaultCatalogName;
    private final Map<String, String> icebergTableProperties;
    private final String storagePath;
    private final String bucketPath; // The format is like: "s3a://bucket-name/"
    private final Optional<Long> baseSchemaVersion;

    public enum LakehouseType {
        DELTA,
        ICEBERG,
        DELTA_AND_ICEBERG,
        NONE
    }

    public enum StreamTableMode {
        MANAGED,
        EXTERNAL,
        CUSTOM
    }

    // used for testing purpose
    public LakehouseConfiguration() {
        this(new Properties());
    }

    public LakehouseConfiguration(Properties properties) {
        this.properties.putAll(properties);
        injectDefaultConfiguration();
        this.storagePath = getStoragePath(properties);
        var compactionPrefix = getCompactionPrefix(properties);
        var bucketPath = storagePath.substring(0, storagePath.length() - compactionPrefix.length());
        this.bucketPath = bucketPath.endsWith("/") ? bucketPath : bucketPath + "/";
        this.hadoopConfiguration = generateHadoopConfiguration(properties);
        this.deltaProperties = generateDeltaConfiguration(properties);
        this.deltaCatalogPropertiesMap = generateDeltaCatalogMap(properties);
        this.icebergProperties = generateIcebergConfiguration(properties);
        this.icebergCatalogPropertiesMap = generateicebergCatalogMap(properties);
        this.icebergTableProperties = generateIcebergTableProperties(properties);
        this.defaultCatalogName = properties.getProperty(DEFAULT_CATALOG_NAME);
        this.baseSchemaVersion = DynamicConfigs.fromProperties(this.properties).baseSchemaVersion();
    }

    private void injectDefaultConfiguration() {
        this.properties.putIfAbsent("clusterSdtEnabled", "true");
        this.properties.putIfAbsent("clusterSbtEnabled", "true");
    }

    // iceberg.catalog.<catalog_name>.<property_name>=<property_value>
    public static Map<String, Map<String, String>> generateicebergCatalogMap(Properties properties) {
        Map<String, Map<String, String>> catalogMap = new HashMap<>();
        properties.entrySet().stream()
            .filter(c -> c.getKey().toString().startsWith(ICEBERG_CATALOG_PREFIX))
            .forEach(k -> {
                String key = k.getKey().toString().replaceFirst(ICEBERG_CATALOG_PREFIX, "");
                if (StringUtils.isBlank(key)) {
                    return;
                }
                String[] parts = key.split("\\.", 2);
                if (parts.length != 2) {
                    return;
                }
                catalogMap.computeIfAbsent(parts[0], k1 -> new HashMap<>())
                    .put(parts[1], k.getValue().toString());
            });
        return Map.copyOf(catalogMap);
    }

    public static Map<String, String> generateIcebergTableProperties(Properties properties) {
        Map<String, String> props = new HashMap<>();

        properties.entrySet().stream()
            .filter(c -> c.getKey().toString().startsWith(TABLE_PROP_PREFIX)
                || c.getKey().toString().startsWith(WRITE_PROP_PREFIX))
            .forEach(k -> {
                String key = k.getKey().toString()
                    .replaceFirst(TABLE_PROP_PREFIX, "")
                    .replaceFirst(WRITE_PROP_PREFIX, "");
                if (StringUtils.isBlank(key)) {
                    return;
                }

                props.put(key, k.getValue().toString());
            });
        return Map.copyOf(props);
    }

    public Map<String, String> generateIcebergConfiguration(Properties properties) {
        Map<String, String> props = new HashMap<>();

        // filter properties that start with `iceberg.` but not start with `iceberg.catalog.`
        properties.entrySet().stream()
            .filter(c -> c.getKey().toString().startsWith(ICEBERG_PREFIX))
            .filter(c -> !c.getKey().toString().startsWith(ICEBERG_CATALOG_PREFIX))
            .forEach(k -> props.put(k.getKey().toString()
                            .replaceFirst(ICEBERG_PREFIX, ""), k.getValue().toString()));

        if (StringUtils.isEmpty(props.get(CatalogProperties.WAREHOUSE_LOCATION))) {
            props.put(CatalogProperties.WAREHOUSE_LOCATION, getStoragePath());
        }

        if (StringUtils.isEmpty(props.get(CatalogUtil.ICEBERG_CATALOG_TYPE))
            && StringUtils.isEmpty(props.get(CatalogProperties.CATALOG_IMPL))) {
            props.put(CatalogUtil.ICEBERG_CATALOG_TYPE, "hadoop");
        }

        String credentialFile = properties.getProperty(ICEBERG_CREDENTIAL_FILE);
        if (StringUtils.isNotEmpty(credentialFile)) {
            try {
                props.put("credential", StorageConfig.loadCredentialsFromFile(credentialFile));
            } catch (IOException e) {
                log.error("Failed to load credentials from file: {}", credentialFile, e);
                throw new IllegalArgumentException("Failed to load credentials from file: " + credentialFile, e);
            }
        }

        return Map.copyOf(props);
    }

    public static String getStoragePath(Properties properties) {
        String compactionBackendStorageType = getCompactionBackendStorageType(properties);
        CompactFileType type = CompactFileType.valueOf(compactionBackendStorageType.toUpperCase(Locale.ROOT));
        String storagePath;
        switch (type) {
            case S3 -> storagePath = generateS3StoragePath(properties);
            case GCS -> storagePath = generateGCSStoragePath(properties);
            case AZUREBLOB, AZUREDFS, AZURELOCAL -> storagePath = generateAzureStoragePath(properties);
            case LOCAL -> storagePath = (String) properties.getOrDefault(STORAGE_PATH, DEFAULT_STORAGE_PATH);
            default -> throw new IllegalArgumentException("Unsupported storage type: " + type);
        }
        return storagePath;
    }

    public boolean isAzure() {
        String compactionBackendStorageType = getCompactionBackendStorageType(properties);
        CompactFileType type = CompactFileType.valueOf(compactionBackendStorageType.toUpperCase(Locale.ROOT));
        return CompactFileType.AZUREBLOB == type
                || CompactFileType.AZUREDFS == type
                || CompactFileType.AZURELOCAL == type;
    }

    protected static String generateGCSStoragePath(Properties properties) {
        String gcsBucket = getCompactionBucket(properties).strip();
        String gcsPrefix = getCompactionPrefix(properties).strip();
        if (!StringUtils.isBlank(gcsPrefix) && gcsPrefix.startsWith("/")) {
            gcsPrefix = gcsPrefix.substring(1);
        }
        String storagePath =  gcsBucket + (gcsPrefix.isEmpty() ? "" : "/" + gcsPrefix);
        if (!storagePath.contains("://")) {
            storagePath = "gs://" + storagePath;
        }
        return storagePath;
    }

    protected static String generateS3StoragePath(Properties properties) {
        String storagePath = getCompactionBucket(properties).strip();
        String storagePrefix = getCompactionPrefix(properties).strip();
        if (!StringUtils.isBlank(storagePath) && storagePath.endsWith("/")) {
            storagePath = storagePath.substring(0, storagePath.length() - 1);
        }

        if (!StringUtils.isBlank(storagePrefix) && storagePrefix.startsWith("/")) {
            storagePrefix = storagePrefix.substring(1);
        }

        storagePath = storagePath + (storagePrefix.isEmpty() ? "" : "/" + storagePrefix);
        if (!storagePath.contains("://")) {
            storagePath = "s3a://" + storagePath;
        }
        if (storagePath.startsWith("s3://")) {
            storagePath = storagePath.replace("s3://", "s3a://");
        }
        return storagePath;
    }

    protected static String generateAzureStoragePath(Properties properties) {
        String compactionBackendStorageType = getCompactionBackendStorageType(properties);
        CompactFileType type = CompactFileType.valueOf(compactionBackendStorageType.toUpperCase(Locale.ROOT));
        String azureBucket = getCompactionBucket(properties).strip();
        if (StringUtils.isBlank(azureBucket)
                || !azureBucket.contains("@")) {
            throw new IllegalArgumentException("Invalid Azure storage path: " + azureBucket);
        }
        String[] split = azureBucket.split("@");
        String accountName = split[0];
        String containerName = split[1];
        String storagePath;
        if (CompactFileType.AZUREBLOB == type || CompactFileType.AZURELOCAL == type) {
            storagePath = containerName + "@" + accountName + ".blob.core.windows.net";
        } else if (CompactFileType.AZUREDFS == type) {
            storagePath = containerName + "@" + accountName + ".dfs.core.windows.net";
        } else {
            throw new IllegalArgumentException("Unsupported Azure storage type: " + type);
        }
        String storagePrefix = getCompactionPrefix(properties).strip();
        if (!StringUtils.isBlank(storagePrefix) && storagePrefix.startsWith("/")) {
            storagePrefix = storagePrefix.substring(1);
        }
        storagePath = storagePath + (storagePrefix.isEmpty() ? "" : "/" + storagePrefix);
        if (!storagePath.contains("://")) {
            if (CompactFileType.AZUREBLOB == type) {
                storagePath = "wasbs://" + storagePath;
            } else if (CompactFileType.AZUREDFS == type) {
                storagePath = "abfss://" + storagePath;
            } else {
                storagePath = "wasb://" + storagePath;
            }
        }
        return storagePath;
    }

    public static String resolveAzureHost(String url) {
        try {
            // Extract the part after '@'
            int atIndex = url.indexOf('@');
            if (atIndex != -1) {
                String hostPart = url.substring(atIndex + 1);

                // Extract the part before the first '/' to get the full host
                int slashIndex = hostPart.indexOf('/');
                if (slashIndex != -1) {
                    return hostPart.substring(0, slashIndex);
                }
                return hostPart; // No path found, return the whole host part
            } else {
                throw new IllegalArgumentException("Invalid URL format: '@' not found.");
            }
        } catch (Exception e) {
            throw new IllegalArgumentException("Error parsing the URL: " + e.getMessage(), e);
        }
    }

    public static Configuration generateHadoopConfiguration(Properties properties) {
        Configuration hadoopConf = new Configuration();
        properties.entrySet().stream()
            .filter(c -> c.getKey().toString().startsWith(HADOOP_CONF_PREFIX))
            .forEach(prop -> {
                hadoopConf.set(
                    prop.getKey().toString().replaceFirst(HADOOP_CONF_PREFIX, ""),
                    prop.getValue().toString());
            });
        hadoopConf.set("fs.AbstractFileSystem.hdfs.impl", Hdfs.class.getName());
        hadoopConf.set("fs.AbstractFileSystem.file.impl", LocalFs.class.getName());
        hadoopConf.set("fs.hdfs.impl", DistributedFileSystem.class.getName());
        hadoopConf.set("fs.file.impl", LocalFileSystem.class.getName());
        String storagePath = getStoragePath(properties);
        if (storagePath.startsWith("s3a://")) {
            generateS3HadoopConfiguration(hadoopConf, properties);
        } else if (storagePath.startsWith("gs://")) {
            generateGCSHadoopConfiguration(hadoopConf, properties);
        } else if (storagePath.startsWith("abfss://") || storagePath.startsWith("wasbs://")
                || storagePath.startsWith("wasb://")) {
            generateAzureHadoopConfiguration(hadoopConf, storagePath, properties);
        }
        return hadoopConf;
    }

    public static void generateS3HadoopConfiguration(Configuration hadoopConf, Properties properties) {
        // https://issues.apache.org/jira/browse/HADOOP-19559
        // https://github.com/awslabs/analytics-accelerator-s3#memory-used-by-library
        // hadoop changed the type to the analytics which takes a lot of memory.
        hadoopConf.set("fs.s3a.input.stream.type", "classic");
        hadoopConf.set(SOCKET_TIMEOUT, "15000");
        hadoopConf.set(REQUEST_TIMEOUT, "15000");
        hadoopConf.set(AWS_CREDENTIALS_PROVIDER,
            ProfileCredentialsProvider.class.getName() + "," + WebIdentityTokenFileCredentialsProvider.class.getName());

        hadoopConf.set("fs.s3a.impl", S3AFileSystem.class.getName());

        String region = properties.getProperty("compactionBucketRegion");
        if (StringUtils.isNotEmpty(region)) {
            hadoopConf.set(AWS_REGION, region);
        }
        String s3Endpoint = properties.getProperty("cloudStorageEndpoint");
        if (!StringUtils.isBlank(s3Endpoint)) {
            hadoopConf.set(AWS_CREDENTIALS_PROVIDER, DefaultCredentialsProvider.class.getName());
            try {
                String resolvedEndPoint = resolveEndpoint(s3Endpoint);
                log.info("Resolve endpoint: {} to {}", s3Endpoint, resolvedEndPoint);
                hadoopConf.set(ENDPOINT, resolvedEndPoint);
            } catch (UnknownHostException e) {
                log.info("Can't resolve the host {}, use it as 'fs.s3a.endpoint' directly", s3Endpoint);
                hadoopConf.set(ENDPOINT, s3Endpoint);
            }
        }
    }

    public static void generateGCSHadoopConfiguration(Configuration hadoopConf, Properties properties) {
        hadoopConf.set("fs.gs.impl", GoogleHadoopFileSystem.class.getName());
        // in hadoop 3.5, it introduced the gcp connector in the hadoop lib, which is conflicted with the gcs-connector.
        // set the following configuration explicitly to avoid reading the conflicted gcp connector configuration
        // in hadoop lib.
        hadoopConf.set("fs.gs.block.size", "67108864"); // 64m
        hadoopConf.set("fs.gs.outputstream.buffer.size", "8388608"); // 8m
        hadoopConf.set("fs.gs.inputstream.inplace.seek.limit", "8388608"); // 8m
        hadoopConf.set("fs.gs.inputstream.min.range.request.size", "2097152");
        String gcsId = properties.getProperty(GCS_ID);
        if (StringUtils.isNotEmpty(gcsId)) {
            hadoopConf.set(GCS_PROJECT_ID.getKey(), gcsId);
        }
        String gcsSa = properties.getProperty(GCS_SA);
        if (StringUtils.isNotEmpty(gcsSa)) {
            hadoopConf.set("google.cloud.auth.service.account.enable", "true");
            hadoopConf.set("google.cloud.auth.service.account.json.keyfile", gcsSa);
        }

        String gcsEndpoint = properties.getProperty("cloudStorageEndpoint");
        if (!StringUtils.isBlank(gcsEndpoint)) {
            hadoopConf.set("fs.gs.auth.null.enable", "true");
            hadoopConf.set("fs.gs.auth.service.account.enable", "false");
            try {
                String resolvedEndPoint = resolveEndpoint(gcsEndpoint);
                log.info("Resolve endpoint: {} to {}", gcsEndpoint, resolvedEndPoint);
                hadoopConf.set(GCS_ROOT_URL.getKey(), resolvedEndPoint);
            } catch (UnknownHostException e) {
                log.info("Can't resolve the host {}, use it as 'fs.gs.endpoint' directly", gcsEndpoint);
                hadoopConf.set(GCS_ROOT_URL.getKey(), gcsEndpoint);
            }
        }
    }

    public static void generateAzureHadoopConfiguration(Configuration hadoopConf, String storagePath,
                                                        Properties properties) {
        String compactionBackendStorageType = getCompactionBackendStorageType(properties);
        CompactFileType type = CompactFileType.valueOf(compactionBackendStorageType.toUpperCase(Locale.ROOT));
        String azureHost = resolveAzureHost(storagePath);
        Map<String, String> env = System.getenv();
        if (CompactFileType.AZUREDFS == type || CompactFileType.AZUREBLOB == type) {
            if (CompactFileType.AZUREDFS == type) {
                hadoopConf.set("fs.abfss.impl", AzureBlobFileSystem.class.getName());
            } else {
                hadoopConf.set("fs.wasbs.impl", NativeAzureFileSystem.class.getName());
            }
            hadoopConf.set(ConfigurationKeys.FS_AZURE_ACCOUNT_AUTH_TYPE_PROPERTY_NAME + "." + azureHost, "OAuth");
            hadoopConf.set(ConfigurationKeys.FS_AZURE_ACCOUNT_TOKEN_PROVIDER_TYPE_PROPERTY_NAME + "." + azureHost,
                    WorkloadIdentityTokenProvider.class.getName());
            hadoopConf.set(ConfigurationKeys.FS_AZURE_ACCOUNT_OAUTH_MSI_AUTHORITY + "." + azureHost,
                    env.get("AZURE_AUTHORITY_HOST"));
            hadoopConf.set(ConfigurationKeys.FS_AZURE_ACCOUNT_OAUTH_CLIENT_ID + "." + azureHost,
                    env.get("AZURE_CLIENT_ID"));
            hadoopConf.set(ConfigurationKeys.FS_AZURE_ACCOUNT_OAUTH_MSI_TENANT + "." + azureHost,
                    env.get("AZURE_TENANT_ID"));
            hadoopConf.set(ConfigurationKeys.FS_AZURE_ACCOUNT_OAUTH_TOKEN_FILE + "." + azureHost,
                    env.get("AZURE_FEDERATED_TOKEN_FILE"));
        } else {
            hadoopConf.set("fs.wasb.impl", NativeAzureFileSystem.class.getName());
            hadoopConf.set(ConfigurationKeys.FS_AZURE_ACCOUNT_AUTH_TYPE_PROPERTY_NAME + "." + azureHost, "SharedKey");
            hadoopConf.set(ConfigurationKeys.FS_AZURE_ACCOUNT_KEY_PROPERTY_NAME + "." + azureHost,
                    "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==");
            hadoopConf.set("fs.azure.storage.emulator.account.name", azureHost);
        }
    }

    public static String resolveEndpoint(String endPoint) throws UnknownHostException {
        URI uri = URI.create(endPoint);

        InetAddress inetAddress = InetAddress.getByName(uri.getHost());
        String ip = inetAddress.getHostAddress();

        StringBuilder endPointBuilder = new StringBuilder();
        if (StringUtils.isNotBlank(uri.getScheme())) {
            endPointBuilder.append(uri.getScheme()).append("://");
        }
        endPointBuilder.append(ip).append(":").append(uri.getPort());
        if (StringUtils.isNotBlank(uri.getPath())) {
            endPointBuilder.append(uri.getPath());
        }
        return endPointBuilder.toString();
    }

    public static Map<String, String> generateDeltaConfiguration(Properties properties) {
        Map<String, String> props = new HashMap<>();
        properties.entrySet().stream()
            .filter(c -> c.getKey().toString().startsWith(DELTA_PREFIX))
            .filter(c -> !c.getKey().toString().startsWith(DELTA_CATALOG_PREFIX))
            .forEach(k ->
                props.put(k.getKey().toString(), k.getValue().toString())
            );
        return props;
    }

    // delta.catalog.<catalog_name>.<property_name>=<property_value>
    public static Map<String, Map<String, String>> generateDeltaCatalogMap(Properties properties) {
        Map<String, Map<String, String>> catalogMap = new HashMap<>();
        properties.entrySet().stream()
            .filter(c -> c.getKey().toString().startsWith(DELTA_CATALOG_PREFIX))
            .forEach(k -> {
                String key = k.getKey().toString().replaceFirst(DELTA_CATALOG_PREFIX, "");
                if (StringUtils.isBlank(key)) {
                    return;
                }
                String[] parts = key.split("\\.", 2);
                if (parts.length != 2) {
                    return;
                }
                catalogMap.computeIfAbsent(parts[0], k1 -> new HashMap<>())
                    .put(parts[1], k.getValue().toString());
            });
        return Map.copyOf(catalogMap);
    }

    public String getCompressType() {
        return properties.getOrDefault(PARQUET_COMPRESSION_TYPE, DEFAULT_PARQUET_COMPRESSION_TYPE).toString();
    }

    public boolean makeNewFieldsOptionalOnEvolution() {
        return Boolean.parseBoolean(
            properties.getOrDefault(MAKE_NEW_FIELDS_OPTIONAL,
                String.valueOf(MAKE_NEW_FIELDS_OPTIONAL_DEFAULT)).toString());
    }

    public long getRowGroupSize() {
        return Long.parseLong(properties.getOrDefault(DELTA_ROW_GROUP_SIZE, DEFAULT_DELTA_ROW_GROUP_SIZE).toString());
    }

    public int getLakehouseCommitMaxRetryTimes() {
        return Integer.parseInt(properties.getOrDefault(LAKEHOUSE_COMMIT_MAX_RETRY_TIMES,
            DEFAULT_LAKEHOUSE_COMMIT_MAX_RETRY_TIMES).toString());
    }

    public static Compression getKafkaCompressionType(Properties properties) {
        return Compression.of(
                properties.getOrDefault(KAFKA_COMPRESSION_TYPE, DEFAULT_KAFKA_COMPRESSION_TYPE)
                        .toString().toLowerCase(Locale.ROOT)).build();
    }

    public String getIcebergCatalogType(Optional<String> catalogName) {
        String effectiveCatalog = catalogName.filter(StringUtils::isNotBlank)
            .orElse(defaultCatalogName);

        String defaultType = icebergProperties.getOrDefault(
            CatalogUtil.ICEBERG_CATALOG_TYPE,
            CatalogUtil.ICEBERG_CATALOG_TYPE_HADOOP
        );

        if (StringUtils.isBlank(effectiveCatalog)) {
            return defaultType;
        }

        Map<String, String> catalogProperties = icebergCatalogPropertiesMap.get(effectiveCatalog);
        if (catalogProperties == null || catalogProperties.isEmpty()) {
            return defaultType;
        }

        String catalogType = catalogProperties.get(CatalogUtil.ICEBERG_CATALOG_TYPE);
        if (StringUtils.isBlank(catalogType)) {
            log.warn("Catalog type for '{}' not specified. Using default catalog type '{}'.",
                effectiveCatalog, defaultType);
            return defaultType;
        }

        return catalogType;
    }

    public boolean checkIcebergOrdering() {
        return icebergProperties.getOrDefault(CHECK_ORDERING, String.valueOf(CHECK_ORDERING_DEFAULT))
                .equalsIgnoreCase("true");
    }

    public boolean checkIcebergNullability() {
        return icebergProperties.getOrDefault(CHECK_NULLABILITY, String.valueOf(CHECK_NULLABILITY_DEFAULT))
                .equalsIgnoreCase("true");
    }

    public IcebergCatalogBackendType getIcebergCatalogBackendType(Optional<String> catalogName) {
        String effectiveCatalog = catalogName.filter(StringUtils::isNotBlank)
            .orElse(defaultCatalogName);

        String defaultType = icebergProperties.getOrDefault(
            CATALOG_BACKEND_TYPE,
            "hadoop"
        );
        IcebergCatalogBackendType defaultCatalogBackendType =
            IcebergCatalogBackendType.valueOf(defaultType.toUpperCase(Locale.ROOT));

        if (StringUtils.isBlank(effectiveCatalog)) {
            return defaultCatalogBackendType;
        }

        Map<String, String> catalogProperties = icebergCatalogPropertiesMap.get(effectiveCatalog);
        if (catalogProperties == null || catalogProperties.isEmpty()) {
            return defaultCatalogBackendType;
        }

        String catalogType = catalogProperties.get(CATALOG_BACKEND_TYPE);
        if (StringUtils.isBlank(catalogType)) {
            log.warn("Catalog type for '{}' not specified. Using default catalog backend type '{}'.",
                effectiveCatalog, defaultType);
            return defaultCatalogBackendType;
        }


        return IcebergCatalogBackendType.valueOf(catalogType.toUpperCase(Locale.ROOT));
    }

    public String getPartitionKey() {
        // Managed table always use fixed partition key or none partition key
        if (getStreamTableMode() == StreamTableMode.MANAGED) {
                String partitionKey = properties.getProperty("partitionKey", FIXED_PARTITION_KEY);
                if (FIXED_PARTITION_KEY.equals(partitionKey)
                    || (!StringUtils.isBlank(partitionKey) && partitionKey.contains(FIXED_PARTITION_KEY))) {
                    return FIXED_PARTITION_KEY;
                }
                return NONE_PARTITION_KEY;
        } else {
            // External table supports user defined key or use none partition key
            String partitionKey = properties.getProperty("partitionKey", NONE_PARTITION_KEY);
            //Handle the wrong config case.
            if (partitionKey.equals(FIXED_PARTITION_KEY)) {
                return NONE_PARTITION_KEY;
            }
            return partitionKey;
        }
    }

    public Set<String> getIdentifierFields() {
        var identifierFields = properties.getProperty("identifierFields");
        if (StringUtils.isBlank(identifierFields)) {
            return Collections.emptySet();
        } else {
            return Set.of(Arrays.stream(identifierFields.split(",")).map(String::strip).toArray(String[]::new));
        }
    }

    public LakehouseType getLakehouseType() {
        return LakehouseType.valueOf(
            properties.getProperty("lakehouseType", LakehouseType.NONE.name()).toUpperCase(Locale.ROOT));
    }

    /**
     * Like {@link #getLakehouseType()} but returns {@link LakehouseType#NONE} when {@code lakehouseType}
     * is not a recognized managed/SBT lakehouse table format. With the LIP-161 pipeline,
     * {@code lakehouseType} can carry a non-lakehouse SDT sink selector (e.g. {@code CLICKHOUSE}, which
     * is a {@code TableCatalogType} routed through the materialization catalog, not an Iceberg/Delta
     * managed table). The SBT/managed-parquet commit path uses this accessor so a non-lakehouse SDT sink
     * is treated as "no external lakehouse commit" — the managed Compacted Object is still registered and
     * the offload cursor advanced — rather than throwing on the unknown enum value.
     */
    public LakehouseType getLakehouseTypeOrNone() {
        try {
            return getLakehouseType();
        } catch (IllegalArgumentException e) {
            return LakehouseType.NONE;
        }
    }

    public String getUnityCatalogUri() {
        return getDeltaCatalogProperty("unityCatalogUri");
    }

    public String getUnityCatalogName() {
        return getDeltaCatalogProperty("unityCatalogName");
    }

    public String getUnityCatalogToken() {
        String unityCatalogTokenFile = getDeltaCatalogProperty(UNITY_CATALOG_TOKEN_FILE);
        if (StringUtils.isNotEmpty(unityCatalogTokenFile)) {
            try {
                return StorageConfig.loadCredentialsFromFile(unityCatalogTokenFile);
            } catch (IOException e) {
                log.error("Failed to load unity catalog token from file: {}", unityCatalogTokenFile, e);
                throw new IllegalArgumentException("Failed to load unity catalog token from file: "
                    + unityCatalogTokenFile, e);
            }
        }

        return getDeltaCatalogProperty("unityCatalogToken");
    }

    public String getUnityCatalogUserAgent() {
        return properties.getProperty("unityCatalogUserAgent");
    }

    public String getUnityCatalogClientId() {
        return getDeltaCatalogProperty("unityCatalogClientId");
    }

    public String getUnityCatalogClientSecret() {
        return getDeltaCatalogProperty("unityCatalogClientSecret");
    }

    public String getDirectExternalStoragePath() {
        String storagePath = properties.getProperty(DIRECT_EXTERNAL_STORAGE_PATH);
        if (StringUtils.isBlank(storagePath)) {
            throw new IllegalArgumentException(
                "directExternalStoragePath must be configured for delta direct external table.");
        }
        return normalizeDirectExternalStoragePath(storagePath.strip(), properties);
    }

    static String normalizeDirectExternalStoragePath(String storagePath, Properties properties) {
        if (StringUtils.isBlank(storagePath)) {
            throw new IllegalArgumentException("directExternalStoragePath must not be blank.");
        }

        String normalized = storagePath.strip();
        while (normalized.length() > 1 && normalized.endsWith("/")) {
            normalized = normalized.substring(0, normalized.length() - 1);
        }

        if (normalized.startsWith("s3://")) {
            return normalized.replaceFirst("s3://", "s3a://");
        }
        if (normalized.startsWith("s3a://")
            || normalized.startsWith("gs://")
            || normalized.startsWith("abfss://")
            || normalized.startsWith("wasbs://")
            || normalized.startsWith("wasb://")
            || normalized.startsWith("file://")
            || normalized.startsWith("/")) {
            return normalized;
        }

        if (!normalized.contains("://")) {
            String backendStorageType = getCompactionBackendStorageType(properties).toUpperCase(Locale.ROOT);
            return switch (CompactFileType.valueOf(backendStorageType)) {
                case S3 -> "s3a://" + normalized;
                case GCS -> "gs://" + normalized;
                case LOCAL -> normalized;
                case AZUREBLOB -> "wasbs://" + normalized;
                case AZUREDFS -> "abfss://" + normalized;
                case AZURELOCAL -> "wasb://" + normalized;
            };
        }
        return normalized;
    }

    public boolean isUnityCatalogByolEnabled() {
        return Boolean.parseBoolean(properties.getProperty("unityCatalogByolEnabled", "false"));
    }

    public String getUnityCatalogByolSystemType() {
        return properties.getProperty("unityCatalogByolSystemType", "KAFKA");
    }

    public boolean isUpsertMode() {
        return Boolean.parseBoolean(properties.getProperty("upsertMode", "false"));
    }

    public boolean isMockUnityCatalog() {
        return Boolean.parseBoolean(properties.getProperty("mockUnityCatalog", "false"));
    }

    public String getMockedUnityCatalogRootStorage() {
        return getDeltaCatalogProperty("mockedUnityCatalogRootStorage");
    }

    private String getDeltaCatalogProperty(String propertyName) {
        Optional<String> catalogNameOpt = getCatalogName().filter(StringUtils::isNotBlank);
        if (catalogNameOpt.isPresent()) {
            Map<String, String> catalogProperties = deltaCatalogPropertiesMap.get(catalogNameOpt.get());
            if (catalogProperties != null) {
                String value = catalogProperties.get(propertyName);
                if (StringUtils.isNotBlank(value)) {
                    return value;
                }
            }
        }

        return properties.getProperty(propertyName);
    }

    public int getIcebergSnapshotExpirationInterval() {
        return Integer.parseInt(
            properties.getProperty("icebergSnapshotExpirationIntervalInSeconds", "-1"));
    }

    public StreamTableMode getStreamTableMode() {
        return StreamTableMode.valueOf(properties.getProperty("streamTableMode",
            StreamTableMode.MANAGED.name()).toUpperCase(Locale.ROOT));
    }

    public int getDeltaKernelWriteBatchSize() {
        return Integer.parseInt(
                properties.getProperty("deltaKernelWriteBatchSize", "1000"));
    }

    // The default value is 50MB/s
    public long getWalReadRateLimitInBytesPerSecond() {
        return Long.parseLong(properties.getProperty("walReadRateLimitInBytesPerSecond", "52428800"));
    }

    public static String getCompactionBackendStorageType(Properties properties) {
        String compactionBackendStorageType = properties.getProperty("compactionBackendStorageType");
        String backendStorageType = properties.getProperty("backendStorageType", CompactFileType.LOCAL.name());
        return StringUtils.isEmpty(compactionBackendStorageType) ? backendStorageType : compactionBackendStorageType;
    }

    public static String getCompactionBucket(Properties properties) {
        String compactionBucket = properties.getProperty("compactionBucket");
        String s3CompactionBucket = properties.getProperty("s3CompactionBucket", DEFAULT_STORAGE_PATH);
        return StringUtils.isEmpty(compactionBucket) ? s3CompactionBucket : compactionBucket;
    }

    public static String getCompactionPrefix(Properties properties) {
        String compactionPrefix = properties.getProperty("compactionPrefix");
        String s3CompactionPrefix = properties.getProperty("s3CompactionPrefix", "");
        return StringUtils.isEmpty(compactionPrefix) ? s3CompactionPrefix : compactionPrefix;
    }

    public Map<String, String> getIcebergProperties(Optional<String> catalogName) {
        String effectiveCatalog = catalogName.filter(s -> StringUtils.isNotBlank(s)
                && icebergCatalogPropertiesMap.containsKey(s))
            .orElse(defaultCatalogName);

        if (StringUtils.isBlank(effectiveCatalog)) {
            log.warn("Catalog name is not present and default catalog name is blank. "
                + "Using default iceberg properties.");
            return icebergProperties;
        }

        // if the defaultCatalogName doesn't exist in the icebergCatalogPropertiesMap, fallback to the icebergProperties
        Map<String, String> catalogProperties = icebergCatalogPropertiesMap.get(effectiveCatalog);
        if (catalogProperties == null || catalogProperties.isEmpty()) {
            log.warn("Catalog {} not found or empty in iceberg catalog properties map. "
                    + "Using default iceberg properties.", effectiveCatalog);
            return icebergProperties;
        }

        Map<String, String> merged = new HashMap<>(icebergProperties);
        merged.putAll(catalogProperties);

        return Map.copyOf(merged); // unmodifiable
    }

    public Optional<String> getCatalogName() {
        return Optional.ofNullable(properties.getProperty(CATALOG_NAME, defaultCatalogName));
    }

    public Duration getCatalogMaxOpenTime() {
        var interval = properties.getProperty("catalogMaxOpenTimeInSeconds");
        if (interval == null || interval.isEmpty()) {
            return Duration.ofDays(365 * 100L);
        }
        return Duration.ofSeconds(Long.parseLong(interval));
    }

    public int getCatalogRetryMaxAttempts() {
        return Integer.parseInt(properties.getProperty("catalogOpsRetryMaxAttempts", "3"));
    }

    public long getCatalogRetryDelayMs() {
        return Long.parseLong(properties.getProperty("catalogOpsRetryDelayMs", "100"));
    }

    public boolean isSchemaEvolutionEnabled() {
        return Boolean.parseBoolean(properties.getProperty("tableEvolveSchemaEnabled", "true"));
    }

    public boolean isAllowIcebergV3() {
        return Boolean.parseBoolean(properties.getProperty("allowIcebergV3", "false"));
    }

    // This is for Delta Variant Type
    public boolean isVariantTypeEnabled() {
        return Boolean.parseBoolean(properties.getProperty("variantTypeEnabled", "false"));
    }

    public boolean isPersistExtraMetadata() {
        return Boolean.parseBoolean(properties.getProperty("persistExtraMetadata", "false"));
    }

    public boolean isPersistKey() {
        return Boolean.parseBoolean(properties.getProperty("persistKey", "false"));
    }

    public String getDltSuffix() {
        return properties.getProperty(DLT_SUFFIX, DEFAULT_DLT_SUFFIX);
    }

    public boolean isDeltaDltEnabled() {
        return Boolean.parseBoolean(properties.getProperty(DELTA_DLT_ENABLED, "true"));
    }

    public boolean isUseJsonHandlerV2() {
        return Boolean.parseBoolean(properties.getProperty("useJsonHandlerV2", "false"));
    }

    public boolean isDeltaSupportManagedCommit() {
        return Boolean.parseBoolean(properties.getProperty("deltaSupportManagedCommit", "false"));
    }

    /**
     * This configuration is used to enable the floor operation on the
     * {@link io.lakestream.ursa.lakehouse.v2.io.parquet.IndexFileReader#seekBySecondaryIndex(String)}.
     * In Kafka case, it may read from the middle of the batch, but our secondary index built from the write batch
     * with the batched offset. When the reading from the middle, it can not read successfully because it can not
     * get the right index.
     *
     * @return
     */
    public boolean allowApproximateMatching() {
        return Boolean.parseBoolean(properties.getProperty("allowApproximateMatching", "false"));
    }

    public void setAllowApproximateMatching() {
        properties.putIfAbsent("allowApproximateMatching", "true");
    }
}
