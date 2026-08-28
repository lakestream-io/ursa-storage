/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.storage.impl;

import static io.lakestream.ursa.storage.impl.StorageConfig.DEFAULT_INDEX_SERIALIZE_FORMAT_VERSION;
import static io.lakestream.ursa.storage.impl.StorageConfig.MB;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

public class StorageConfigTest {

    private Path tempFile;

    private StorageConfig baseConfig;

    @BeforeEach
    void setUp() throws IOException {
        // Create a unique temporary file for each test
        tempFile = Files.createTempFile("test_config", ".properties");
        baseConfig = StorageConfig.builder()
            .streamTableMode("MANAGED")
            .upsertModeEnabled(false)
            .thirdPartySchemaRegistryEnabled(false)
            .backendStorageType("S3")
            .bucket("test-bucket")
            .prefix("test-prefix")
            .region("us-west-2")
            .writeBufferSize(4 * 1024 * 1024) // 4MB
            .writeBufferFlushSize(256 * 1024 * 1024L) // 256MB
            .writeBufferMaxStreamIds(4L)
            .writeCacheEnabled(true)
            .readThreadNum(4)
            .readCacheMemorySize(1024 * 1024L) // 1MB
            .unityCatalogUri("http://localhost:8080")
            .unityCatalogName("test-catalog")
            .unityCatalogToken("test-token")
            .build();
    }

    @AfterEach
    void tearDown() throws IOException {
        // Delete the temporary file after each test
        Files.deleteIfExists(tempFile);
    }

    @Test
    public void testDefaultConfigurations() throws IOException {
        Properties properties = new Properties();
        properties.put("backendStorageType", "Azure");
        properties.put("s3CompactionBucket", "compaction-test-bucket");
        properties.put("s3CompactionPrefix", "compaction-test-prefix");
        properties.put("s3Bucket", "test-bucket");
        properties.put("s3Prefix", "test-prefix");
        StorageConfig config = StorageConfig.fromProperties(properties);
        assertEquals("Azure", config.getBackendStorageType());
        assertEquals("Azure", config.getCompactionBackendStorageType());
        assertEquals("compaction-test-bucket", config.getS3CompactionBucket());
        assertEquals("compaction-test-bucket", config.getCompactionBucket());
        assertEquals("compaction-test-prefix", config.getS3CompactionPrefix());
        assertEquals("compaction-test-prefix", config.getCompactionPrefix());
        assertEquals("test-bucket", config.getBucket());
        assertEquals("test-bucket", config.getS3Bucket());
        assertEquals("test-prefix", config.getS3Prefix());
        assertEquals("test-prefix", config.getPrefix());
        assertTrue(config.isInternalCompactionTaskPublisherEnabled());
    }

    @Test
    void testExternalTaskPublisherConfiguration() {
        Properties properties = new Properties();
        properties.put("internalCompactionTaskPublisherEnabled", "false");

        StorageConfig config = StorageConfig.fromProperties(properties);

        assertFalse(config.isInternalCompactionTaskPublisherEnabled());
    }

    @Test
    public void testConfigurations() throws IOException {
        Properties properties = new Properties();
        properties.put("backendStorageType", "Azure");
        properties.put("compactionBackendStorageType", "GCS");
        properties.put("s3CompactionBucket", "compaction-test-bucket");
        properties.put("compactionBucket", "new-compaction-test-bucket");
        properties.put("s3CompactionPrefix", "compaction-test-prefix");
        properties.put("compactionPrefix", "new-compaction-test-prefix");
        properties.put("s3Bucket", "test-bucket");
        properties.put("bucket", "new-test-bucket");
        properties.put("s3Prefix", "test-prefix");
        properties.put("prefix", "new-test-prefix");
        properties.put("region", "us-west-1");
        properties.put("compactionBucketRegion", "us-west-2");
        StorageConfig config = StorageConfig.fromProperties(properties);
        assertEquals("Azure", config.getBackendStorageType());
        assertEquals("GCS", config.getCompactionBackendStorageType());
        assertEquals("compaction-test-bucket", config.getS3CompactionBucket());
        assertEquals("new-compaction-test-bucket", config.getCompactionBucket());
        assertEquals("compaction-test-prefix", config.getS3CompactionPrefix());
        assertEquals("new-compaction-test-prefix", config.getCompactionPrefix());
        assertEquals("test-bucket", config.getS3Bucket());
        assertEquals("new-test-bucket", config.getBucket());
        assertEquals("test-prefix", config.getS3Prefix());
        assertEquals("new-test-prefix", config.getPrefix());
        assertEquals("us-west-1", config.getRegion());
        assertEquals("us-west-2", config.getCompactionBucketRegion());
    }

    @Test
    void testOxiaStorageProperties() {
        StorageConfig defaults = StorageConfig.builder().build();
        assertEquals("", defaults.getOxiaStorageUrl());
        assertEquals("", defaults.getOxiaStorageConfig());

        Properties properties = new Properties();
        properties.put("oxiaStorageUrl", "oxia://neutral");
        properties.put("oxiaStorageConfig", "{\"client\":\"neutral\"}");

        StorageConfig config = StorageConfig.fromProperties(properties);

        assertEquals("oxia://neutral", config.getOxiaStorageUrl());
        assertEquals("{\"client\":\"neutral\"}", config.getOxiaStorageConfig());
    }

    @Test
    void testOxiaStorageOverrides() {
        Properties properties = new Properties();
        properties.put("oxiaStorageUrl", "oxia://neutral-base");
        properties.put("oxiaStorageConfig", "{\"client\":\"neutral-base\"}");
        StorageConfig base = StorageConfig.fromProperties(properties);

        StorageConfig overridden = base.withOverrides(Map.of(
                "oxiaStorageUrl", "oxia://neutral-override",
                "oxiaStorageConfig", "{\"client\":\"neutral-override\"}"));

        assertEquals("oxia://neutral-override", overridden.getOxiaStorageUrl());
        assertEquals("{\"client\":\"neutral-override\"}", overridden.getOxiaStorageConfig());
    }

    @Test
    void testStoragePropertiesFromJavaProperties() {
        Properties properties = new Properties();
        properties.put("backendStorageType", "GCS");
        properties.put("storagePath", "/var/lib/ursa");
        properties.put("oxiaStorageUrl", "oxia://storage");
        properties.put("writeBufferSize", "8388608");
        properties.put("writeBufferFlushSize", "536870912");
        properties.put("writeBufferFlushIntervalMs", "750");
        properties.put("cloudStorageEndpoint", "https://storage.example.test");
        properties.put("bucket", "stream-bucket");
        properties.put("prefix", "stream-prefix");
        properties.put("region", "us-west-2");
        properties.put("s3AccessKeyId", "access-key");
        properties.put("s3SecretAccessKey", "secret-key");
        properties.put("blackNamespaceOfCompact", "org-a/team-a,org-b/team-b");
        properties.put("blackTopicOfCompact", "org-a/team-a/topic-a");

        StorageConfig config = StorageConfig.fromProperties(properties);

        assertEquals("GCS", config.getBackendStorageType());
        assertEquals("/var/lib/ursa", config.getStoragePath());
        assertEquals("oxia://storage", config.getOxiaStorageUrl());
        assertEquals(8388608, config.getWriteBufferSize());
        assertEquals(536870912L, config.getWriteBufferFlushSize());
        assertEquals(750L, config.getWriteBufferFlushIntervalMs());
        assertEquals("https://storage.example.test", config.getCloudStorageEndpoint());
        assertEquals("stream-bucket", config.getBucket());
        assertEquals("stream-prefix", config.getPrefix());
        assertEquals("us-west-2", config.getRegion());
        assertEquals("access-key", config.getS3AccessKeyId());
        assertEquals("secret-key", config.getS3SecretAccessKey());
        assertEquals(Set.of("org-a/team-a", "org-b/team-b"), config.getBlackNamespaceOfCompact());
        assertEquals(Set.of("org-a/team-a/topic-a"), config.getBlackTopicOfCompact());

        StorageConfig clone = config.clone();
        assertNotSame(config.getBlackNamespaceOfCompact(), clone.getBlackNamespaceOfCompact());
        assertNotSame(config.getBlackTopicOfCompact(), clone.getBlackTopicOfCompact());
        clone.getBlackNamespaceOfCompact().add("org-c/team-c");
        assertFalse(config.getBlackNamespaceOfCompact().contains("org-c/team-c"));
    }

    @Test
    void testFieldContextRemainsAvailableForRuntimeIntrospection() throws NoSuchFieldException {
        FieldContext fieldContext = StorageConfig.class
                .getDeclaredField("backendStorageType")
                .getAnnotation(FieldContext.class);

        assertNotNull(fieldContext);
        assertEquals("wal", fieldContext.category());
        assertFalse(fieldContext.doc().isBlank());
    }

    @Test
    public void checkDefaultValues() throws IOException {
        Properties properties = new Properties();
        properties.put("writeBufferSegment", "30");
        properties.put("storagePath", "/tmp");
        StorageConfig config = StorageConfig.fromProperties(properties);
        assertEquals(DEFAULT_INDEX_SERIALIZE_FORMAT_VERSION, config.getIndexSerializeFormatVersion());
        assertEquals(32, config.getDefaultReadBatchContextInitializeSize());
        assertEquals(1, config.getNumAddWorkerThreads());
        assertFalse(config.isEnableTaskExecutionStats());
        assertFalse(config.isPreserveMdcForTaskExecution());
        assertEquals(4 * MB, config.getWriteBufferSize());
        assertEquals(30, config.getWriteBufferSegment());
        assertEquals(256 * MB, config.getWriteBufferFlushSize());
        assertEquals(4, config.getWriteBufferMaxStreamIds());
        assertEquals(250, config.getWriteBufferFlushIntervalMs());
        assertTrue(config.isWriteCacheEnabled());
        assertEquals(500, config.getAddEntryMaxThrottleTimeMs());
        assertEquals(30_000, config.getAddEntryTimeoutMs());
        assertEquals(300, config.getPollDelayMs());
        assertEquals(4, config.getReadThreadNum());
        assertEquals("/tmp", config.getReadCacheSpillableDiskDir());
        assertTrue(config.isReadCacheToDiskCompressionEnable());
        assertEquals("S3", config.getBackendStorageType());
        assertEquals("DATEUUID", config.getIdGeneratorType());
        assertEquals("/tmp", config.getStoragePath());
        assertEquals(1000, config.getMaxIndexesCacheBuildDelayInMillis());
        assertEquals(-1, config.getCloudStorageOpsRateLimitPerSecond());
        assertEquals(-1, config.getS3OpsRateLimitPerSecond());
        assertEquals(-1, config.getS3MaxPendingConnectionAcquires());
        assertEquals(-1, config.getS3ConnectionAcquisitionTimeoutMs());
        assertEquals(-1, config.getS3OpsMaxRetries());
        assertFalse(config.isDisableS3ExpressSessionAuth());
        assertEquals(10_000, config.getPublishThreadPendingTasks());
        assertEquals(10_000, config.getCheckCompactMessageStepLength());
        assertEquals(256 * MB, config.getCompactedFileSizeLimit());
        assertEquals(180, config.getMaxCommitIntervalInSeconds());
        assertEquals(250, config.getMaxTaskCombineSize());
        assertEquals(180, config.getTailCompactDataVisibilityIntervalInSeconds());
        assertEquals(4_000, config.getPerReadMaxEntriesOfCompact());
        assertEquals(1_00, config.getPerReadMaxEntriesOfCompactLowWatermark());
        assertEquals("io.lakestream.ursa.lakehouse.compact.LakehouseCompactionServiceImpl",
                config.getCompactionServiceClass());
        assertEquals("", config.getMetadataStoreUrl());
        assertEquals("", config.getMetadataStoreConfig());
        assertEquals("", config.getOxiaStorageUrl());
        assertEquals("", config.getOxiaStorageConfig());
        assertEquals(60, config.getTopicToStreamCacheExpireTimeInSeconds());
        assertEquals(60, config.getRefreshLocalTopicInternalInSeconds());
        assertEquals(300, config.getCompactionMaintenanceIntervalInSeconds());
        assertEquals(30, config.getRefreshLocalTaskIntervalInSeconds());
        assertEquals(30, config.getRetryableQuarantineInSeconds());
        assertEquals(300, config.getNonRetryableQuarantineInSeconds());
        assertEquals(12, config.getCleanupJobIntervalInHours());
        assertEquals(60_000, config.getCustomExpireTimeMs());
        assertEquals(120_000, config.getDefaultExpireTimeMs());
        assertEquals(0.9, config.getCacheEvictionWatermark());
        assertEquals(600, config.getEntryIndexCacheTTLInSecs());
        assertEquals(1200, config.getCatalogMaxOpenTimeInSeconds());
        assertFalse(config.isUpsertModeEnabled());
        assertFalse(config.isThirdPartySchemaRegistryEnabled());
    }

    @Test
    void testOxiaJsonConfigs() throws IOException {
        String metadataStoreConfig = "{\"enableTls\":\"true\",\"authPluginClassName\":\"io.oxia.client.auth"
                + ".TokenAuthentication\",\"authParams\":\"token:metadata\"}";
        String storageConfig = "{\"enableTls\":\"true\",\"authPluginClassName\":\"io.oxia.client.auth"
                + ".TokenAuthentication\",\"authParams\":\"token:storage\"}";
        Properties properties = new Properties();
        properties.put("metadataStoreConfig", metadataStoreConfig);
        properties.put("oxiaStorageConfig", storageConfig);

        StorageConfig config = StorageConfig.fromProperties(properties);

        assertEquals(metadataStoreConfig, config.getMetadataStoreConfig());
        assertEquals(storageConfig, config.getOxiaStorageConfig());
    }

    @Test
    void testLoadConfigurationFile_Success() throws IOException {
        // Arrange: Write some valid properties to the temporary file
        String fileContent = "key1=value1\nkey2=value2 with spaces\n#comment\nkey3=123";
        Files.write(tempFile, fileContent.getBytes(StandardCharsets.UTF_8));

        // Act: Load the configuration
        Properties loadedProperties = StorageConfig.loadConfigurationFile(tempFile.toString());

        // Assert: Verify the content
        assertNotNull(loadedProperties);
        assertEquals(3, loadedProperties.size());
        assertEquals("value1", loadedProperties.getProperty("key1"));
        assertEquals("value2 with spaces", loadedProperties.getProperty("key2"));
        assertEquals("123", loadedProperties.getProperty("key3"));
    }

    @Test
    void testLoadConfigurationFile_EmptyFile() throws IOException {
        // Arrange: Create an empty file (no content written)

        // Act: Load the configuration
        Properties loadedProperties = StorageConfig.loadConfigurationFile(tempFile.toString());

        // Assert: Verify it's an empty Properties object
        assertNotNull(loadedProperties);
        assertTrue(loadedProperties.isEmpty());
    }

    @Test
    void testLoadConfigurationFile_FileWithOnlyComments() throws IOException {
        // Arrange: Write only comments to the file
        String fileContent = "# This is a comment\n!Another comment\n\n";
        Files.write(tempFile, fileContent.getBytes(StandardCharsets.UTF_8));

        // Act: Load the configuration
        Properties loadedProperties = StorageConfig.loadConfigurationFile(tempFile.toString());

        // Assert: Verify it's an empty Properties object
        assertNotNull(loadedProperties);
        assertTrue(loadedProperties.isEmpty());
    }

    @Test
    void testLoadConfigurationFile_NonExistentFile() {
        // Arrange: Do not create the tempFile, ensure it doesn't exist
        Path nonExistentPath = Paths.get("non_existent_file_" + System.currentTimeMillis() + ".properties");

        // Act & Assert: Expect FileNotFoundException
        assertThrows(FileNotFoundException.class, () -> {
            StorageConfig.loadConfigurationFile(nonExistentPath.toString());
        });
    }

    // --- Tests for loadPropertiesFileToMap(String filePath) ---

    @Test
    void testLoadPropertiesFileToMap_Success() throws IOException {
        // Arrange: Create a mock Properties object that loadConfigurationFile will return
        Properties mockProperties = new Properties();
        mockProperties.setProperty("map.key1", "map_value1");
        mockProperties.setProperty("map.key2", "map_value2");
        mockProperties.setProperty("map.number", "123"); // Value is string in Properties

        // Use Mockito to mock the static method loadConfigurationFile
        // This ensures isolation: we're testing the conversion logic, not the file loading
        try (MockedStatic<StorageConfig> mockedStatic = Mockito.mockStatic(StorageConfig.class)) {
            // Configure the mock to return our mockProperties when loadConfigurationFile is called
            mockedStatic.when(() -> StorageConfig.loadConfigurationFile(anyString()))
                .thenReturn(mockProperties);
            // We also need to mock the real method for loadPropertiesFileToMap
            // if it calls the static loadConfigurationFile, because we are mocking the class itself.
            // This is a common pattern when mocking static methods of the class under test.
            mockedStatic.when(() -> StorageConfig.loadPropertiesFileToMap(anyString()))
                .thenCallRealMethod();


            // Act: Load the properties into a map
            Map<String, Object> loadedMap = StorageConfig.loadPropertiesFileToMap("dummy_path.properties");

            // Assert: Verify the map content
            assertNotNull(loadedMap);
            assertEquals(3, loadedMap.size());
            assertEquals("map_value1", loadedMap.get("map.key1"));
            assertEquals("map_value2", loadedMap.get("map.key2"));
            assertEquals("123", loadedMap.get("map.number")); // Values are still Strings (Objects)
        }
    }

    @Test
    void testLoadPropertiesFileToMap_EmptyProperties() throws IOException {
        // Arrange: Create an empty mock Properties object
        Properties emptyProperties = new Properties();

        // Mock the static method
        try (MockedStatic<StorageConfig> mockedStatic = Mockito.mockStatic(StorageConfig.class)) {
            mockedStatic.when(() -> StorageConfig.loadConfigurationFile(anyString()))
                .thenReturn(emptyProperties);
            mockedStatic.when(() -> StorageConfig.loadPropertiesFileToMap(anyString()))
                .thenCallRealMethod();

            // Act: Load into map
            Map<String, Object> loadedMap = StorageConfig.loadPropertiesFileToMap("dummy_path.properties");

            // Assert: Map should be empty
            assertNotNull(loadedMap);
            assertTrue(loadedMap.isEmpty());
        }
    }

    @Test
    void testLoadPropertiesFileToMap_IOExceptionPropagated() {
        // Arrange: Mock loadConfigurationFile to throw an IOException
        try (MockedStatic<StorageConfig> mockedStatic = Mockito.mockStatic(StorageConfig.class)) {
            IOException testException = new IOException("Simulated file read error");
            mockedStatic.when(() -> StorageConfig.loadConfigurationFile(anyString()))
                .thenThrow(testException);
            mockedStatic.when(() -> StorageConfig.loadPropertiesFileToMap(anyString()))
                .thenCallRealMethod();

            // Act & Assert: Verify that IOException is thrown
            IOException thrown = assertThrows(IOException.class, () -> {
                StorageConfig.loadPropertiesFileToMap("error_path.properties");
            });
            assertEquals("Simulated file read error", thrown.getMessage());
        }
    }

    @Test
    public void testCloneCreatesDeepCopy() {
        Properties props = new Properties();
        props.setProperty("key1", "value1");

        StorageConfig original = StorageConfig.builder()
            .blackNamespaceOfCompact(Set.of("public/default"))
            .properties(props)
            .indexSerializeFormatVersion(2)
            .writeBufferSize(8 * 1024 * 1024)
            .build();

        StorageConfig clone = original.clone();

        assertNotSame(original, clone);
        assertEquals(original, clone);

        assertNotSame(original.getProperties(), clone.getProperties());
        assertEquals("value1", clone.getProperties().getProperty("key1"));

        original.getProperties().setProperty("key1", "changed");
        assertNotEquals(original.getProperties().getProperty("key1"), clone.getProperties().getProperty("key1"));

        Map<String, String> kv = new HashMap<>();
        kv.put("key2", "value2");
        kv.put("key3", "value3");
        clone.getProperties().putAll(kv);
        assertEquals("value2", clone.getProperties().getProperty("key2"));
        assertEquals("value3", clone.getProperties().getProperty("key3"));
    }

    @Test
    @DisplayName("Should return same instance when overrides are null")
    void testWithOverrides_NullOverrides() {
        StorageConfig result = baseConfig.withOverrides(null);
        assertSame(baseConfig, result);
    }

    @Test
    @DisplayName("Should return same instance when overrides are empty")
    void testWithOverrides_EmptyOverrides() {
        Map<String, String> emptyOverrides = new HashMap<>();
        StorageConfig result = baseConfig.withOverrides(emptyOverrides);
        assertSame(baseConfig, result);
    }

    @Test
    @DisplayName("Should override string fields correctly")
    void testWithOverrides_StringFields() {
        Map<String, String> overrides = new HashMap<>();
        overrides.put("streamTableMode", "EXTERNAL");
        overrides.put("backendStorageType", "GCS");
        overrides.put("bucket", "new-bucket");
        overrides.put("prefix", "new-prefix");
        overrides.put("region", "us-east-1");

        StorageConfig result = baseConfig.withOverrides(overrides);

        assertNotSame(baseConfig, result);
        assertEquals("EXTERNAL", result.getStreamTableMode());
        assertEquals("GCS", result.getBackendStorageType());
        assertEquals("new-bucket", result.getBucket());
        assertEquals("new-prefix", result.getPrefix());
        assertEquals("us-east-1", result.getRegion());

        // Verify original config is unchanged
        assertEquals("MANAGED", baseConfig.getStreamTableMode());
        assertEquals("S3", baseConfig.getBackendStorageType());
    }

    @Test
    @DisplayName("Should override boolean fields correctly")
    void testWithOverrides_BooleanFields() {
        Map<String, String> overrides = new HashMap<>();
        overrides.put("upsertModeEnabled", "true");
        overrides.put("thirdPartySchemaRegistryEnabled", "true");
        overrides.put("writeCacheEnabled", "false");

        StorageConfig result = baseConfig.withOverrides(overrides);

        assertTrue(result.isUpsertModeEnabled());
        assertTrue(result.isThirdPartySchemaRegistryEnabled());
        assertFalse(result.isWriteCacheEnabled());

        // Verify original config is unchanged
        assertFalse(baseConfig.isUpsertModeEnabled());
        assertFalse(baseConfig.isThirdPartySchemaRegistryEnabled());
        assertTrue(baseConfig.isWriteCacheEnabled());
    }

    @ParameterizedTest
    @DisplayName("Should handle boolean string variations")
    @ValueSource(strings = {"true", "TRUE", "True", "false", "FALSE", "False"})
    void testWithOverrides_BooleanStringVariations(String booleanValue) {
        Map<String, String> overrides = new HashMap<>();
        overrides.put("upsertModeEnabled", booleanValue);

        StorageConfig result = baseConfig.withOverrides(overrides);

        boolean expected = Boolean.parseBoolean(booleanValue);
        assertEquals(expected, result.isUpsertModeEnabled());
    }

    @Test
    @DisplayName("Should override integer fields correctly")
    void testWithOverrides_IntegerFields() {
        Map<String, String> overrides = new HashMap<>();
        overrides.put("writeBufferSize", "8388608"); // 8MB
        overrides.put("readThreadNum", "8");

        StorageConfig result = baseConfig.withOverrides(overrides);

        assertEquals(8388608, result.getWriteBufferSize());
        assertEquals(8, result.getReadThreadNum());

        // Verify original config is unchanged
        assertEquals(4 * 1024 * 1024, baseConfig.getWriteBufferSize());
        assertEquals(4, baseConfig.getReadThreadNum());
    }

    @Test
    @DisplayName("Should override long fields correctly")
    void testWithOverrides_LongFields() {
        Map<String, String> overrides = new HashMap<>();
        overrides.put("writeBufferFlushSize", "536870912"); // 512MB
        overrides.put("writeBufferMaxStreamIds", "8");
        overrides.put("readCacheMemorySize", "2097152"); // 2MB

        StorageConfig result = baseConfig.withOverrides(overrides);

        assertEquals(536870912L, result.getWriteBufferFlushSize());
        assertEquals(8L, result.getWriteBufferMaxStreamIds());
        assertEquals(2097152L, result.getReadCacheMemorySize());
    }

    @Test
    @DisplayName("Should override Set fields correctly")
    void testWithOverrides_SetFields() {
        Map<String, String> overrides = new HashMap<>();
        overrides.put("blackNamespaceOfCompact", "org1/team1, org2/team2 , org3/team3");
        overrides.put("blackTopicOfCompact", "org1/team1/topic1, org2/team2/topic2");

        StorageConfig result = baseConfig.withOverrides(overrides);

        Set<String> expectedNamespaces = Set.of("org1/team1", "org2/team2", "org3/team3");
        Set<String> expectedTopics = Set.of("org1/team1/topic1", "org2/team2/topic2");

        assertEquals(expectedNamespaces, result.getBlackNamespaceOfCompact());
        assertEquals(expectedTopics, result.getBlackTopicOfCompact());
    }

    @Test
    @DisplayName("Should handle empty Set fields correctly")
    void testWithOverrides_EmptySetFields() {
        Map<String, String> overrides = new HashMap<>();
        overrides.put("blackNamespaceOfCompact", "");
        overrides.put("blackTopicOfCompact", "   ");

        StorageConfig result = baseConfig.withOverrides(overrides);

        assertTrue(result.getBlackNamespaceOfCompact().isEmpty());
        assertTrue(result.getBlackTopicOfCompact().isEmpty());
    }

    @Test
    @DisplayName("Should override Unity Catalog fields correctly")
    void testWithOverrides_UnityCatalogFields() {
        Map<String, String> overrides = new HashMap<>();
        overrides.put("unityCatalogUri", "http://new-catalog:9090");
        overrides.put("unityCatalogName", "new-catalog");
        overrides.put("unityCatalogToken", "new-token");

        StorageConfig result = baseConfig.withOverrides(overrides);

        assertEquals("http://new-catalog:9090", result.getUnityCatalogUri());
        assertEquals("new-catalog", result.getUnityCatalogName());
        assertEquals("new-token", result.getUnityCatalogToken());
    }

    @Test
    @DisplayName("Should update properties map correctly")
    void testWithOverrides_PropertiesUpdated() {
        Properties initialProperties = new Properties();
        initialProperties.setProperty("existing.key", "existing.value");
        baseConfig.setProperties(initialProperties);

        Map<String, String> overrides = new HashMap<>();
        overrides.put("streamTableMode", "EXTERNAL");
        overrides.put("new.property", "new.value");

        StorageConfig result = baseConfig.withOverrides(overrides);

        Properties resultProperties = result.getProperties();
        assertEquals("existing.value", resultProperties.getProperty("existing.key"));
        assertEquals("EXTERNAL", resultProperties.getProperty("streamTableMode"));
        assertEquals("new.value", resultProperties.getProperty("new.property"));

        // Verify original properties are unchanged
        assertNull(baseConfig.getProperties().getProperty("streamTableMode"));
        assertNull(baseConfig.getProperties().getProperty("new.property"));
    }

    @Test
    @DisplayName("Should handle unknown configuration keys gracefully")
    void testWithOverrides_UnknownKeys() {
        Map<String, String> overrides = new HashMap<>();
        overrides.put("streamTableMode", "EXTERNAL");
        overrides.put("unknownKey", "unknownValue");
        overrides.put("anotherUnknownKey", "anotherValue");

        // This should not throw an exception
        StorageConfig result = baseConfig.withOverrides(overrides);

        assertEquals("EXTERNAL", result.getStreamTableMode());

        // Unknown keys should still be in properties
        assertEquals("unknownValue", result.getProperties().getProperty("unknownKey"));
        assertEquals("anotherValue", result.getProperties().getProperty("anotherUnknownKey"));
    }

    @Test
    @DisplayName("Should atomically retain the original configuration for invalid overrides")
    void testWithOverrides_InvalidNumberFormats() {
        Map<String, String> overrides = new HashMap<>();
        overrides.put("writeBufferSize", "invalid-number");
        overrides.put("streamTableMode", "EXTERNAL");

        StorageConfig result = baseConfig.withOverrides(overrides);

        assertSame(baseConfig, result);
        assertEquals(baseConfig.getWriteBufferSize(), result.getWriteBufferSize());
        assertEquals("MANAGED", result.getStreamTableMode());
    }

    @ParameterizedTest
    @ValueSource(strings = {"writeBufferSize", "writeBufferFlushSize", "minReadyToCommitPercentage"})
    @DisplayName("Should fail fast for invalid numeric startup properties")
    void testFromProperties_InvalidNumberFormats(String fieldName) {
        Properties properties = new Properties();
        properties.setProperty(fieldName, "invalid-number");

        IllegalArgumentException error = assertThrows(
                IllegalArgumentException.class,
                () -> StorageConfig.fromProperties(properties));

        assertTrue(error.getMessage().contains(fieldName));
    }

    @Test
    @DisplayName("Should parse non-true boolean values as false")
    void testBooleanConversionOfNonTrueValues() {
        Properties properties = new Properties();
        properties.setProperty("writeCacheEnabled", "not-a-boolean");

        StorageConfig config = StorageConfig.fromProperties(properties);
        StorageConfig overridden = baseConfig.withOverrides(Map.of(
                "writeCacheEnabled", "not-a-boolean"));

        assertFalse(config.isWriteCacheEnabled());
        assertNotSame(baseConfig, overridden);
        assertFalse(overridden.isWriteCacheEnabled());
    }

    @Test
    @DisplayName("Should preserve ordered Set parsing including empty middle tokens")
    void testSetConversionPreservesOrdering() {
        Properties properties = new Properties();
        properties.setProperty("blackNamespaceOfCompact", " org-a/team-a, , org-b/team-b,org-a/team-a ");

        StorageConfig config = StorageConfig.fromProperties(properties);

        assertEquals(List.of("org-a/team-a", "", "org-b/team-b"),
                new ArrayList<>(config.getBlackNamespaceOfCompact()));
    }

    @Test
    @DisplayName("Should handle multiple field types in single override")
    void testWithOverrides_MultipleFieldTypes() {
        Map<String, String> overrides = new HashMap<>();
        overrides.put("streamTableMode", "EXTERNAL");
        overrides.put("upsertModeEnabled", "true");
        overrides.put("writeBufferSize", "16777216"); // 16MB
        overrides.put("writeBufferFlushSize", "1073741824"); // 1GB
        overrides.put("blackNamespaceOfCompact", "org1/team1,org2/team2");
        overrides.put("unityCatalogUri", "http://updated:8080");

        StorageConfig result = baseConfig.withOverrides(overrides);

        assertEquals("EXTERNAL", result.getStreamTableMode());
        assertTrue(result.isUpsertModeEnabled());
        assertEquals(16777216, result.getWriteBufferSize());
        assertEquals(1073741824L, result.getWriteBufferFlushSize());
        assertEquals(Set.of("org1/team1", "org2/team2"), result.getBlackNamespaceOfCompact());
        assertEquals("http://updated:8080", result.getUnityCatalogUri());
    }

    @Test
    @DisplayName("Should preserve immutability of original config")
    void testWithOverrides_ImmutabilityOfOriginal() {
        Map<String, String> overrides = new HashMap<>();
        overrides.put("streamTableMode", "EXTERNAL");
        overrides.put("upsertModeEnabled", "true");
        overrides.put("writeBufferSize", "8388608");

        // Store original values
        String originalStreamTableMode = baseConfig.getStreamTableMode();
        boolean originalUpsertMode = baseConfig.isUpsertModeEnabled();
        int originalWriteBufferSize = baseConfig.getWriteBufferSize();

        StorageConfig result = baseConfig.withOverrides(overrides);

        // Verify original config is completely unchanged
        assertEquals(originalStreamTableMode, baseConfig.getStreamTableMode());
        assertEquals(originalUpsertMode, baseConfig.isUpsertModeEnabled());
        assertEquals(originalWriteBufferSize, baseConfig.getWriteBufferSize());

        // Verify new config has overridden values
        assertEquals("EXTERNAL", result.getStreamTableMode());
        assertTrue(result.isUpsertModeEnabled());
        assertEquals(8388608, result.getWriteBufferSize());
    }

    @Test
    @DisplayName("Should handle catalogMaxOpenTimeInSeconds configuration")
    void testCatalogMaxOpenTimeInSeconds() throws IOException {
        // Test default value
        Properties defaultProps = new Properties();
        StorageConfig defaultConfig = StorageConfig.fromProperties(defaultProps);
        assertEquals(1200, defaultConfig.getCatalogMaxOpenTimeInSeconds());

        // Test custom value
        Properties customProps = new Properties();
        customProps.put("catalogMaxOpenTimeInSeconds", "3600");
        StorageConfig customConfig = StorageConfig.fromProperties(customProps);
        assertEquals(3600, customConfig.getCatalogMaxOpenTimeInSeconds());

        // Test override
        Map<String, String> overrides = new HashMap<>();
        overrides.put("catalogMaxOpenTimeInSeconds", "7200");
        StorageConfig overriddenConfig = defaultConfig.withOverrides(overrides);
        assertEquals(7200, overriddenConfig.getCatalogMaxOpenTimeInSeconds());
    }

    @Test
    @DisplayName("Should handle compactionMaintenanceIntervalInSeconds configuration")
    void testCompactionMaintenanceIntervalInSeconds() throws IOException {
        Properties defaultProps = new Properties();
        StorageConfig defaultConfig = StorageConfig.fromProperties(defaultProps);
        assertEquals(300, defaultConfig.getCompactionMaintenanceIntervalInSeconds());

        Properties customProps = new Properties();
        customProps.put("compactionMaintenanceIntervalInSeconds", "12");
        StorageConfig customConfig = StorageConfig.fromProperties(customProps);
        assertEquals(12, customConfig.getCompactionMaintenanceIntervalInSeconds());

        Map<String, String> overrides = new HashMap<>();
        overrides.put("compactionMaintenanceIntervalInSeconds", "7");
        StorageConfig overriddenConfig = defaultConfig.withOverrides(overrides);
        assertEquals(7, overriddenConfig.getCompactionMaintenanceIntervalInSeconds());
    }

}
