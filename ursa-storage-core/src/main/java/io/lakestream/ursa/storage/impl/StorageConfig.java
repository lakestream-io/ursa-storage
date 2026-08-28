/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.storage.impl;

import io.netty.util.internal.PlatformDependent;
import java.io.FileInputStream;
import java.io.IOException;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

@NoArgsConstructor
@AllArgsConstructor
@Builder(toBuilder = true)
@Data
@Slf4j
public class StorageConfig {

    /**
     * PlatformDependent.maxDirectMemory can be inaccurate if java property `io.netty.maxDirectMemory` are setted.
     * Cache the result in this field.
     */
    static final long JVM_MAX_DIRECT_MEMORY = PlatformDependent.estimateMaxDirectMemory();
    public static final Set<String> CREDENTIAL_KEYS = Set.of("s3SecretAccessKey", "s3AccessKeyId", "unityCatalogToken",
        "unityCatalogClientSecret", "iceberg.credential");

    static final int MB = 1024 * 1024;
    static final int KB = 1024;

    @Category
    private static final String CATEGORY_WAL = "wal";
    @Category
    private static final String CATEGORY_STORAGE = "storage";
    @Category
    private static final String CATEGORY_COMPACT = "compact";

    @Builder.Default
    private Properties properties = new Properties();

    public static final int STRING_VERSION = 2;

    public static final int PROTOBUF_VERSION = 3;

    public static final int DEFAULT_INDEX_SERIALIZE_FORMAT_VERSION = 3;

    @Builder.Default
    @FieldContext(
        category = CATEGORY_WAL,
        doc = "The index serilize formt version for saving the info into oxia. "
            + "Available value is: [1, 2, 3]. Default is 3."
    )
    private int indexSerializeFormatVersion = DEFAULT_INDEX_SERIALIZE_FORMAT_VERSION;

    @Builder.Default
    @FieldContext(
        category = CATEGORY_WAL,
        doc = "When reading a batch of messages from the storage, we need to read the index from the oxia first. "
            + "We need to prepare a list for saving the index info. This is the default size for the list."
    )
    private int defaultReadBatchContextInitializeSize = 32;

    @Builder.Default
    @FieldContext(
        category = CATEGORY_WAL,
        doc = "The wal add worker threads count."
    )
    private int numAddWorkerThreads = 1;

    @Builder.Default
    @FieldContext(
        category = CATEGORY_WAL,
        doc = "Max pending add requests per thread."
    )
    private long maxPendingAddRequestsUsedBytes = Math.round(JVM_MAX_DIRECT_MEMORY * 0.15);

    @Builder.Default
    @FieldContext(
        category = CATEGORY_WAL,
        doc = "Whether enable task execution stats."
    )
    private boolean enableTaskExecutionStats = false;

    @Builder.Default
    @FieldContext(
        category = CATEGORY_WAL,
        doc = "Whether preserve mdc for task execution."
    )
    private boolean preserveMdcForTaskExecution = false;

    @Builder.Default
    @FieldContext(
        category = CATEGORY_WAL,
        doc = "The wal write buffer size."
    )
    private int writeBufferSize = 4 * MB;

    @Builder.Default
    @FieldContext(
        category = CATEGORY_WAL,
        doc = "The wal write buffer segment."
    )
    private int writeBufferSegment = -1;

    public int getWriteBufferSegment() {
        if (writeBufferSegment == -1) {
            return Math.toIntExact(Math.round(JVM_MAX_DIRECT_MEMORY * 0.25 / writeBufferSize));
        } else {
            return writeBufferSegment;
        }
    }

    @Builder.Default
    @FieldContext(
        category = CATEGORY_WAL,
        doc = "The data size of write buffer flush."
    )
    private long writeBufferFlushSize = 256 * MB;

    @Builder.Default
    @FieldContext(
            category = CATEGORY_WAL,
            doc = "The max number of stream ids of a write buffer."
    )
    private long writeBufferMaxStreamIds = 4;


    @Builder.Default
    @FieldContext(
        category = CATEGORY_WAL,
        doc = "The wal write buffer flush interval in milliseconds."
    )
    private long writeBufferFlushIntervalMs = 250;

    @Builder.Default
    @FieldContext(
            category = CATEGORY_WAL,
            doc = "Enable of disable the write cache."
    )
    private boolean writeCacheEnabled = true;

    @Builder.Default
    @FieldContext(
        category = CATEGORY_WAL,
        doc = "Add entry max throttle time in milliseconds."
    )
    private long addEntryMaxThrottleTimeMs = 500;

    @Builder.Default
    @FieldContext(
        category = CATEGORY_WAL,
        doc = "Add entry max timeout in milliseconds."
    )
    private long addEntryTimeoutMs = 30_000;

    @Builder.Default
    private long pollDelayMs = 300;

    @Builder.Default
    @FieldContext(
        category = CATEGORY_WAL,
        doc = "The wal read worker threads count."
    )
    private int readThreadNum = 4;

    @Builder.Default
    @FieldContext(
        category = CATEGORY_WAL,
        doc = "The read cache in memory size."
    )
    private long readCacheMemorySize = Math.round(JVM_MAX_DIRECT_MEMORY * 0.15);

    @Builder.Default
    @FieldContext(
            category = CATEGORY_WAL,
            doc = "The read cache spillable disk dir."
    )
    private String readCacheSpillableDiskDir = "/tmp";

    @Builder.Default
    @FieldContext(
            category = CATEGORY_WAL,
            doc = "Is the read cache spillable to disk persist enable compression."
    )
    private boolean readCacheToDiskCompressionEnable = true;

    @Builder.Default
    @FieldContext(
        category = CATEGORY_WAL,
        doc = "The backend storage type."
    )
    private String backendStorageType = "S3";

    @Builder.Default
    @FieldContext(
        category = CATEGORY_WAL,
        doc = "The id generator type for the wal log file. Available value is: [MEMORY, RANDOM, OXIA, UUID]"
    )
    private String idGeneratorType = "DATEUUID";

    @FieldContext(
        category = CATEGORY_WAL,
        doc = "The backend storage path."
    )
    private String storagePath;

    @Deprecated
    @FieldContext(
        category = CATEGORY_STORAGE,
        doc = "The S3 backend region"
    )
    private String s3Region;

    @FieldContext(
        category = CATEGORY_STORAGE,
        doc = "The object storage bucket region"
    )
    private String region;

    @FieldContext(
        category = CATEGORY_STORAGE,
        doc = "The object storage bucket name used to save wal files"
    )
    private String bucket;

    @FieldContext(
        category = CATEGORY_STORAGE,
        doc = "The object storage prefix used to save wal files"
    )
    private String prefix;

    @Deprecated
    @FieldContext(
        category = CATEGORY_STORAGE,
        doc = "The S3 backend bucket"
    )
    private String s3Bucket;

    @Deprecated
    @FieldContext(
        category = CATEGORY_STORAGE,
        doc = "The S3 backend prefix"
    )
    private String s3Prefix;

    @Builder.Default
    @FieldContext(
        category = CATEGORY_STORAGE,
        doc = "The cloud storage max concurrency request. Default to write buffer segment numbers."
    )
    private int cloudStorageMaxConcurrencyRequest =
        Math.max(1, Math.toIntExact(Math.round(JVM_MAX_DIRECT_MEMORY * 0.25 / (4 * MB))));

    @Builder.Default
    @FieldContext(
        category = CATEGORY_STORAGE,
        doc = "The cloud storage pending connections acquires."
    )
    private int cloudStorageMaxPendingConnectionAcquires = -1;

    @Builder.Default
    @FieldContext(
        category = CATEGORY_STORAGE,
        doc = "The cloud storage pending connections acquires."
    )
    private int cloudStorageMaxPendingAcquireTimeoutInMs = -1;

    @Builder.Default
    @FieldContext(
            category = CATEGORY_STORAGE,
            doc = "max delay when cursors scan entry indexes and cache them. 0 to disable this delay"
    )
    private long maxIndexesCacheBuildDelayInMillis = 1000;

    @FieldContext(
        category = CATEGORY_STORAGE,
        doc = "The cloud storage ops rate limit per second."
    )
    private int cloudStorageOpsRateLimitPerSecond = -1;

    @Deprecated
    @FieldContext(
        category = CATEGORY_STORAGE,
        doc = "The S3 Ops rate limit per second. Default is 500"
    )
    private int s3OpsRateLimitPerSecond = -1;

    @Deprecated
    @FieldContext(
        category = CATEGORY_STORAGE,
        doc = "The S3 max pending connections acquires. Default is 500"
    )
    private int s3MaxPendingConnectionAcquires = -1;

    @Deprecated
    @FieldContext(
        category = CATEGORY_STORAGE,
        doc = "The S3 connection acquisition timeout in milliseconds. Default is 10_000"
    )
    private int s3ConnectionAcquisitionTimeoutMs = -1;

    @FieldContext(
        category = CATEGORY_STORAGE,
        doc = "The S3 ops max retries. Default is 3"
    )
    private int s3OpsMaxRetries = -1;

    @FieldContext
    private String cloudStorageEndpoint;

    @FieldContext
    private String s3SecretAccessKey;

    @FieldContext
    private String s3AccessKeyId;

    @Builder.Default
    @FieldContext
    private boolean disableS3ExpressSessionAuth = false;

    @Builder.Default
    @FieldContext(
            category = CATEGORY_COMPACT,
            doc = "The publish thread pending tasks."
    )
    private int publishThreadPendingTasks = 10000;

    @Builder.Default
    @FieldContext(
            category = CATEGORY_COMPACT,
            doc = "The publish task thread num"
    )
    private int publishThreadNum = Runtime.getRuntime().availableProcessors();

    @Builder.Default
    @FieldContext(
            category = CATEGORY_COMPACT,
            doc = "Whether this compactor publishes compaction tasks internally. Disable this when an external "
                    + "scheduler owns task publication. Default is true."
    )
    private boolean internalCompactionTaskPublisherEnabled = true;

    @Builder.Default
    @FieldContext(
            category = CATEGORY_COMPACT,
            doc = "Check compact message step length"
    )
    private int checkCompactMessageStepLength = 10000;

    @Builder.Default
    @FieldContext(
            category = CATEGORY_COMPACT,
            doc = "Compacted file size limit"
    )
    private long compactedFileSizeLimit = 256 * MB;

    @Builder.Default
    @FieldContext(
            category = CATEGORY_COMPACT,
            doc = "The commit parquet file runner commit interval. Default 180s."
    )
    private int maxCommitIntervalInSeconds = 180;

    @Builder.Default
    @FieldContext(
            category = CATEGORY_COMPACT,
            doc = "The commit parquet file runner combine commit task size."
    )
    private int maxTaskCombineSize = 250;

    @Builder.Default
    @FieldContext(
            category = CATEGORY_COMPACT,
            doc = "The commit parquet file thread num."
    )
    private int commitThreadNum = Runtime.getRuntime().availableProcessors();

    @Builder.Default
    @FieldContext(
            category = CATEGORY_COMPACT,
            doc = "The tail compact data visibility interval in seconds. Default is 180"
    )
    private int tailCompactDataVisibilityIntervalInSeconds = 180;

    @Builder.Default
    @FieldContext(
            category = CATEGORY_COMPACT,
            doc = "Max number of compact tasks to publish per tick in publishOffloadEvent. "
                + "Used to avoid flooding the task store (e.g. Oxia) when a topic enables SDT "
                + "and needs to replay many ranges from the earliest position. Default is 5. "
                + "Set to 0 or a negative value to disable the cap."
    )
    private int tailCompactMaxTasksPerTick = 20;

    @Builder.Default
    @FieldContext(
            category = CATEGORY_COMPACT,
            doc = "Compacted thread num"
    )
    private int compactedThreadNum = Math.max(1, Runtime.getRuntime().availableProcessors() - 1);

    @Builder.Default
    @FieldContext(
            category = CATEGORY_COMPACT,
            doc = "Each read max entries of compact"
    )
    private int perReadMaxEntriesOfCompact = 4000;

    @Builder.Default
    @FieldContext(
            category = CATEGORY_COMPACT,
            doc = "Low watermark of each read max entries of compact"
    )
    private int perReadMaxEntriesOfCompactLowWatermark = 100;

    @Builder.Default
    @FieldContext(
        category = CATEGORY_COMPACT,
        doc = "The compaction service class. Deprecated: use materializationServiceClass instead."
    )
    private String compactionServiceClass = "io.lakestream.ursa.lakehouse.compact.LakehouseCompactionServiceImpl";

    @Builder.Default
    @FieldContext(
        category = CATEGORY_COMPACT,
        doc = "The materialization service class. Replaces the deprecated compactionServiceClass. "
            + "If both are set, materializationServiceClass wins. If only compactionServiceClass is set, "
            + "its value is used and a deprecation warning is logged."
    )
    private String materializationServiceClass =
            "io.lakestream.ursa.lakehouse.compact.LakehouseMaterializationService";

    @Builder.Default
    @FieldContext(
        category = CATEGORY_COMPACT,
        doc = "The compaction storage bindings class — wires the long-running compaction runners "
            + "(publish/commit/cleaner) the orchestrator dispatches to. Default is the lakehouse-backed "
            + "bindings; integration modules supply their own implementation."
    )
    private String compactionStorageBindingsClass =
            "io.lakestream.ursa.lakehouse.compact.LakehouseCompactionStorageBindings";

    @FieldContext(
        category = CATEGORY_COMPACT,
        doc = "The metadata store url."
    )
    private String metadataStoreUrl = "";

    @FieldContext(
        category = CATEGORY_COMPACT,
        doc = "The Oxia metadata store client configuration JSON."
    )
    private String metadataStoreConfig = "";

    @Builder.Default
    @FieldContext(
        category = CATEGORY_COMPACT,
        doc = "The Oxia storage service URL."
    )
    private String oxiaStorageUrl = "";

    @Builder.Default
    @FieldContext(
        category = CATEGORY_COMPACT,
        doc = "The Oxia storage client configuration JSON."
    )
    private String oxiaStorageConfig = "";

    /**
     * Deprecated, Use compactionBucket directly.
     */
    @Deprecated
    @FieldContext(
        category = CATEGORY_COMPACT,
        doc = "The S3 compaction backend bucket."
    )
    private String s3CompactionBucket;

    /**
     * Deprecated, Use compactionPrefix directly.
     */
    @Deprecated
    @FieldContext(
        category = CATEGORY_COMPACT,
        doc = "The S3 compaction backend prefix."
    )
    private String s3CompactionPrefix;

    @FieldContext(
            category = CATEGORY_WAL,
            doc = "The compaction backend storage type. Available value is: [S3, GCS, AZUREBLOB, LOCAL]"
    )
    private String compactionBackendStorageType;

    @FieldContext(
        category = CATEGORY_COMPACT,
        doc = "The compaction backend bucket region"
    )
    private String compactionBucketRegion;

    @FieldContext(
            category = CATEGORY_COMPACT,
            doc = "The compaction backend bucket."
    )
    private String compactionBucket;

    @FieldContext(
            category = CATEGORY_COMPACT,
            doc = "The compaction backend prefix."
    )
    private String compactionPrefix;

    @Builder.Default
    @FieldContext(
            category = CATEGORY_COMPACT,
            doc = "The topic to stream cache data expire time in seconds, Default 60s"
    )
    private int topicToStreamCacheExpireTimeInSeconds = 60;

    @Builder.Default
    @FieldContext(
            category = CATEGORY_COMPACT,
            doc = "The black namespace of compact. Example: 'public/__system, public/default'"
    )
    private Set<String> blackNamespaceOfCompact = new HashSet<>();

    @Builder.Default
    @FieldContext(
        category = CATEGORY_COMPACT,
        doc = "The topics excluded from compaction, using fully qualified Lakestream names without partition suffix. "
            + "Example: 'public/default/my-topic'"
    )
    private Set<String> blackTopicOfCompact = new HashSet<>();

    @Builder.Default
    @FieldContext(
            category = CATEGORY_COMPACT,
            doc = "The refresh topic interval in seconds."
    )
    private long refreshLocalTopicInternalInSeconds = 60;

    @Builder.Default
    @FieldContext(
            category = CATEGORY_COMPACT,
            doc = "The compaction maintenance interval in seconds. "
                + "This drives cache cleanup and other periodic compaction maintenance tasks."
    )
    private long compactionMaintenanceIntervalInSeconds = 300;

    @Builder.Default
    @FieldContext(
            category = CATEGORY_COMPACT,
            doc = "The minial refresh task interval in seconds. Default is 30 seconds. "
    )
    private long refreshLocalTaskIntervalInSeconds = 30;

    @Builder.Default
    @FieldContext(
        category = CATEGORY_COMPACT,
        doc = "The max refresh task interval in seconds. Default is 300 seconds"
    )
    private long maxRefreshLocalTaskIntervalInSeconds = 300;

    @Builder.Default
    @FieldContext(
        category = CATEGORY_COMPACT,
        doc = "The minimal pending task that trigger back-off waiting. Default is 1000."
    )
    private int minBackoffTasksThreshold = 1000;

    @Builder.Default
    @FieldContext(
        category = CATEGORY_COMPACT,
        doc = "The minimal ready to commit task percentage. "
            + "Below this percentage will trigger get commit task backoff. Default is 0.3."
    )
    private double minReadyToCommitPercentage = 0.3;

    @Builder.Default
    @FieldContext(
            category = CATEGORY_COMPACT,
            doc = "Quarantine retryable task in seconds. Default is 30."
    )
    private int retryableQuarantineInSeconds = 30;

    @FieldContext(
        category = CATEGORY_COMPACT,
        doc = "Quarantine non retryable task in seconds. Default is 300."
    )
    private int nonRetryableQuarantineInSeconds = 300;

    @FieldContext(
        category = CATEGORY_STORAGE,
        doc = "The wal files cleanup job running interval in hours."
    )
    private int cleanupJobIntervalInHours = 12;

    // Configurations for Lakehouse reader
    @FieldContext(
        category = CATEGORY_STORAGE,
        doc = "The Lakehouse reader prefetch cache custom expire time in milliseconds. Default 60s"
    )
    private int customExpireTimeMs = 60_000;
    @FieldContext(
        category = CATEGORY_STORAGE,
        doc = "The Lakehouse reader prefetch cache default expire time in milliseconds. Default 120s"
    )
    private int defaultExpireTimeMs = 120_000;
    @FieldContext(
        category = CATEGORY_STORAGE,
        doc = "The Lakehouse reader prefetch cache eviction watermark. Default is 0.9"
    )
    private double cacheEvictionWatermark = 0.9;
    @FieldContext(
        category = CATEGORY_STORAGE,
        doc = "The Lakehouse reader max inflight reading tasks. Default is 20 * availableProcessors"
    )
    private int maxInflightReadingTasks = 20 * Runtime.getRuntime().availableProcessors();

    @FieldContext(
        category = CATEGORY_STORAGE,
        doc = "The Lakehouse IO thread num. Default is availableProcessors"
    )
    private int lakehouseIOThreadNum = Runtime.getRuntime().availableProcessors();

    @Builder.Default
    @FieldContext(
            category = CATEGORY_STORAGE,
            doc = "max entry index cache size"
    )
    private int maxEntryIndexCacheSize = Math.toIntExact(Math.round(JVM_MAX_DIRECT_MEMORY * 0.01 / KB));

    @Builder.Default
    @FieldContext(
            category = CATEGORY_STORAGE,
            doc = "entry index cache ttl in secs"
    )
    private int entryIndexCacheTTLInSecs = 600;

    @FieldContext(
            category = CATEGORY_COMPACT,
            doc = "The unity catalog server uri."
    )
    private String unityCatalogUri;


    @FieldContext(
            category = CATEGORY_COMPACT,
            doc = "The unity catalog name."
    )
    private String unityCatalogName;

    @FieldContext(
            category = CATEGORY_COMPACT,
            doc = "The unity catalog token."
    )
    private String unityCatalogToken;

    // Managed Table and External Table
    @FieldContext(
        category = CATEGORY_COMPACT,
        doc = "The stream table mode, available value is: [MANAGED, EXTERNAL]"
    )
    private String streamTableMode = "MANAGED";

    @FieldContext(
        category = CATEGORY_COMPACT,
        doc = "Whether support upsert operation. The upsert only support in EXTERNAL table mode. Default is false."
    )
    private boolean upsertModeEnabled = false;

    @FieldContext(
        category = CATEGORY_COMPACT,
        doc = "Whether use third-party Confluent compatible schema registry. Default is false."
    )
    private boolean thirdPartySchemaRegistryEnabled = false;

    @Builder.Default
    @FieldContext(
            category = CATEGORY_COMPACT,
            doc = "The compacted data cleanup job running interval in seconds."
    )
    private int compactedDataCleanupJobIntervalInSecs = 12 * 60 * 60;

    @Builder.Default
    @FieldContext(
            category = CATEGORY_COMPACT,
            doc = "The number of threads used for cleaning up compacted data in parallel. "
                    + "This thread pool is used by AsyncCompactedDataCleaner to process multiple topic partitions "
                    + "simultaneously. "
                    + "The default value is set to the number of available processors in the system."
    )
    private int compactedDataCleanupThreadNum = Runtime.getRuntime().availableProcessors();

    @Builder.Default
    @FieldContext(
            category = CATEGORY_COMPACT,
            doc = "The max pending tasks for compacted data cleanup."
    )
    private int compactedDataCleanupPendingTasks = 100;

    @Builder.Default
    @FieldContext(
        category = CATEGORY_COMPACT,
        doc = "WAL read rate limit in bytes per second. "
            + "This is used to limit the read rate of WAL files to avoid overwhelming the system. "
            + "0 or -1 means no limit. Default is 50MB."
    )
    private long walReadRateLimitInBytesPerSecond = 50 * MB;

    @FieldContext(
            category = CATEGORY_COMPACT,
            doc = "Whether to replay the failed committed tasks from DLQ when first start the compaction service. "
                + "This is useful when the compaction service is restarted and needs to recover from the last state."
    )
    private boolean replayDLQTasksEnabled = true;

    @FieldContext(
            category = CATEGORY_COMPACT,
            doc = "The threshold to record the non committable task count. Default is 500."
    )
    private int recordNonCommittableTaskThreshold = 500;

    @FieldContext(
        category = CATEGORY_COMPACT,
        doc = "Enable LIP-161 stream-to-table materialization dispatch in the compaction worker. "
                + "When true, the worker dispatches resolved streams through the MaterializationService "
                + "SPI and the legacy lakehouse committer runners are not started (the SPI path owns the "
                + "catalog commit). When false (default), behaviour is unchanged: the legacy committer "
                + "runners commit and materialization dispatch is a no-op. Default: false"
    )
    private boolean materializationEnabled = false;

    @FieldContext(
            category = CATEGORY_COMPACT,
            doc = "The catalog max open time in seconds. Default is 1200 (20 minutes)."
    )
    private long catalogMaxOpenTimeInSeconds = 1200;

    @FieldContext(
        category = CATEGORY_COMPACT,
        doc = "Metastore ops rate limit per second. Default is 500."
    )
    private int metastoreRequestRateLimitPerSecond = 500;

    @FieldContext(
        category = CATEGORY_COMPACT,
        doc = "Whether to check the cluster name for the dynamic configurations."
    )
    private boolean checkClusterNameForDynamicConfigs = true;

    public Properties getProperties() {
        return properties;
    }

    public void setProperties(Properties properties) {
        if (this.properties == null) {
            this.properties = new Properties();
        }

        this.properties.putAll(properties);
    }

    public String getCompactionBackendStorageType() {
        return StringUtils.isEmpty(compactionBackendStorageType) ? backendStorageType : compactionBackendStorageType;
    }

    public String getCompactionBucket() {
        return StringUtils.isEmpty(compactionBucket) ? s3CompactionBucket : compactionBucket;
    }

    public String getCompactionPrefix() {
        return StringUtils.isEmpty(compactionPrefix) ? s3CompactionPrefix : compactionPrefix;
    }

    public String getBucket() {
        return StringUtils.isEmpty(bucket) ? s3Bucket : bucket;
    }

    public String getPrefix() {
        return StringUtils.isEmpty(prefix) ? s3Prefix : prefix;
    }

    public static String loadCredentialsFromFile(String path) throws IOException  {
        return Files.readString(Path.of(path));
    }

    public static Properties loadConfigurationFile(String filePath) throws IOException {
        Properties properties = new Properties();
        try (FileInputStream is = new FileInputStream(filePath)) {
            properties.load(is);
            log.info("Load configuration from file {}: {}", filePath, properties);
            return properties;
        }
    }

    public static Map<String, Object> loadPropertiesFileToMap(String filePath) throws IOException {
        Properties properties = loadConfigurationFile(filePath);
        Map<String, Object> map = new HashMap<>();
        for (String key : properties.stringPropertyNames()) {
            map.put(key, properties.get(key));
        }
        return map;
    }

    /**
     * Creates a storage configuration from Java properties.
     * Unknown properties are retained so that integration-specific settings remain available.
     */
    public static StorageConfig fromProperties(Properties properties) {
        StorageConfig config = new StorageConfig();
        config.applyProperties(properties);
        return config;
    }

    private void applyProperties(Map<?, ?> values) {
        if (values == null || values.isEmpty()) {
            return;
        }
        Properties copied = new Properties();
        values.forEach((key, value) -> copied.put(String.valueOf(key), value));
        setProperties(copied);

        values.forEach((key, value) -> {
            String fieldName = String.valueOf(key);
            try {
                Field field = StorageConfig.class.getDeclaredField(fieldName);
                if (Modifier.isStatic(field.getModifiers()) || "properties".equals(fieldName)) {
                    return;
                }
                field.setAccessible(true);
                field.set(this, convertValue(field, value));
            } catch (NoSuchFieldException ignored) {
                // Keep unknown keys in properties for integration-specific consumers.
            } catch (ReflectiveOperationException e) {
                throw new IllegalStateException("Failed to set storage configuration field '" + fieldName + "'", e);
            } catch (IllegalArgumentException e) {
                throw new IllegalArgumentException(
                        "Invalid value '" + value + "' for storage configuration field '" + fieldName + "'", e);
            }
        });
    }

    private Object convertValue(Field field, Object value) {
        Class<?> type = field.getType();
        if (value != null && type.isInstance(value)) {
            if (value instanceof Set<?> set) {
                return new LinkedHashSet<>(set);
            }
            return value;
        }
        String text = value == null ? null : String.valueOf(value).trim();
        if (type == String.class) {
            return value == null ? null : String.valueOf(value);
        }
        if (type == boolean.class) {
            return Boolean.parseBoolean(text);
        }
        if (type == int.class) {
            return Integer.parseInt(text);
        }
        if (type == long.class) {
            return Long.parseLong(text);
        }
        if (type == double.class) {
            return Double.parseDouble(text);
        }
        if (Set.class.isAssignableFrom(type)) {
            return parseStringSet(text);
        }
        throw new IllegalArgumentException("Unsupported configuration field type: " + type.getName());
    }

    @Override
    public StorageConfig clone() {
        try {
            Properties copiedProperties = new Properties();
            if (this.properties != null) {
                copiedProperties.putAll(this.properties);
            }
            return this.toBuilder()
                // Deep copy mutable fields
                .blackNamespaceOfCompact(new HashSet<>(this.blackNamespaceOfCompact))
                .blackTopicOfCompact(new HashSet<>(this.blackTopicOfCompact))
                .properties(copiedProperties)
                .build();
        } catch (Exception e) {
            log.error("Failed to clone StorageConfig", e);
            throw new RuntimeException("Clone failed", e);
        }
    }


    /**
     * Create a new configuration instance with overridden values.
     * This method properly handles field-level overrides
     */
    public StorageConfig withOverrides(Map<String, String> overrides) {
        if (overrides == null || overrides.isEmpty()) {
            return this;
        }

        try {
            Properties newProperties = new Properties();
            if (this.properties != null) {
                newProperties.putAll(this.properties);
            }
            newProperties.putAll(overrides);

            StorageConfig result = clone();
            result.applyProperties(newProperties);
            return result;
        } catch (RuntimeException e) {
            log.warn("Failed to apply storage configuration overrides; retaining the original configuration", e);
            return this;
        }

    }

    /**
     * Helper method to parse comma-separated string into Set<String>.
     */
    private Set<String> parseStringSet(String value) {
        Set<String> result = new LinkedHashSet<>();
        if (value != null && !value.trim().isEmpty()) {
            String[] parts = value.trim().split(",");
            for (String part : parts) {
                result.add(part.trim());
            }
        }
        return result;
    }
}

@interface Category {
}

@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.FIELD)
@interface FieldContext {
    String category() default "";

    String doc() default "";
}
