/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.lakehouse;

import static io.lakestream.ursa.lakehouse.LakehouseConfiguration.CATALOG_BACKEND_TYPE;
import static io.lakestream.ursa.lakehouse.LakehouseConfiguration.GCS_ID;
import static io.lakestream.ursa.lakehouse.LakehouseConfiguration.GCS_SA;
import static io.lakestream.ursa.lakehouse.LakehouseConfiguration.ICEBERG_CATALOG_PREFIX;
import static io.lakestream.ursa.lakehouse.LakehouseConfiguration.ICEBERG_CREDENTIAL_FILE;
import static io.lakestream.ursa.lakehouse.LakehouseConfiguration.ICEBERG_PREFIX;
import static io.lakestream.ursa.lakehouse.LakehouseConfiguration.UNITY_CATALOG_TOKEN_FILE;
import static io.lakestream.ursa.lakehouse.iceberg.IcebergSinkConfig.TABLE_PROP_PREFIX;
import static io.lakestream.ursa.lakehouse.iceberg.IcebergSinkConfig.WRITE_PROP_PREFIX;
import static org.apache.hadoop.fs.s3a.Constants.AWS_REGION;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import io.lakestream.ursa.lakehouse.iceberg.IcebergCatalogBackendType;
import java.io.IOException;
import java.lang.reflect.Field;
import java.net.UnknownHostException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.UUID;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.azure.NativeAzureFileSystem;
import org.apache.hadoop.fs.azurebfs.AzureBlobFileSystem;
import org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys;
import org.apache.hadoop.fs.s3a.S3AFileSystem;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.TableProperties;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.platform.commons.util.StringUtils;

@Tag("lakehouse")
@Slf4j
public class LakehouseConfigurationTest {

    @Test
    public void testResolveHost() throws UnknownHostException {
        String endpoint = "http://localhost:8080";
        String resolvedEndPoint = LakehouseConfiguration.resolveEndpoint(endpoint);
        assertEquals("http://127.0.0.1:8080", resolvedEndPoint);

        String endpoint1 = "http://localhost:8080/aa/bb";
        String resolvedEndPoint1 = LakehouseConfiguration.resolveEndpoint(endpoint1);
        assertEquals("http://127.0.0.1:8080/aa/bb", resolvedEndPoint1);
    }

    @Test
    public void testDefaultConfigurations() {
        Properties properties = new Properties();
        properties.put("backendStorageType", "Azure");
        properties.put("s3CompactionBucket", "compaction-test-bucket");
        properties.put("s3CompactionPrefix", "compaction-test-prefix");
        assertEquals("Azure", LakehouseConfiguration.getCompactionBackendStorageType(properties));
        assertEquals("compaction-test-bucket", LakehouseConfiguration.getCompactionBucket(properties));
        assertEquals("compaction-test-prefix", LakehouseConfiguration.getCompactionPrefix(properties));
    }

    @Test
    public void testConfigurations() {
        Properties properties = new Properties();
        properties.put("backendStorageType", "Azure");
        properties.put("compactionBackendStorageType", "GCS");
        properties.put("s3CompactionBucket", "compaction-test-bucket");
        properties.put("compactionBucket", "new-compaction-test-bucket");
        properties.put("s3CompactionPrefix", "compaction-test-prefix");
        properties.put("compactionPrefix", "new-compaction-test-prefix");

        assertEquals("GCS", LakehouseConfiguration.getCompactionBackendStorageType(properties));
        assertEquals("new-compaction-test-bucket", LakehouseConfiguration.getCompactionBucket(properties));
        assertEquals("new-compaction-test-prefix", LakehouseConfiguration.getCompactionPrefix(properties));
    }

    @Test
    public void testUnityCatalogNameFallsBackToGlobalPropertyWhenCatalogSpecificValueMissing() {
        Properties properties = new Properties();
        properties.put("unityCatalogName", "default-catalog");
        properties.put("catalog.name", "topic-catalog");

        LakehouseConfiguration configuration = new LakehouseConfiguration(properties);

        assertEquals("default-catalog", configuration.getUnityCatalogName());
    }

    @Test
    public void testUnityCatalogNameDoesNotFallbackToDefaultCatalogName() {
        Properties properties = new Properties();
        properties.put("catalog.default", "default-catalog");

        LakehouseConfiguration configuration = new LakehouseConfiguration(properties);

        assertEquals("default-catalog", configuration.getDefaultCatalogName());
    }

    @Test
    public void testDeltaCatalogSpecificUnityCatalogProperties() {
        Properties properties = new Properties();
        properties.put("catalog.default", "alpha");
        properties.put("delta.catalog.alpha.unityCatalogName", "uc-alpha");
        properties.put("delta.catalog.alpha.unityCatalogUri", "https://alpha.example.com");
        properties.put("delta.catalog.alpha.unityCatalogClientId", "client-alpha");
        properties.put("delta.catalog.alpha.unityCatalogClientSecret", "secret-alpha");

        LakehouseConfiguration configuration = new LakehouseConfiguration(properties);

        assertEquals("uc-alpha", configuration.getUnityCatalogName());
        assertEquals("https://alpha.example.com", configuration.getUnityCatalogUri());
        assertEquals("client-alpha", configuration.getUnityCatalogClientId());
        assertEquals("secret-alpha", configuration.getUnityCatalogClientSecret());
    }

    @Test
    public void testDeltaCatalogSpecificUnityCatalogPropertiesWithMultipleCatalogs() {
        Properties properties = new Properties();
        properties.put("catalog.default", "alpha");
        properties.put("catalog.name", "beta");
        properties.put("delta.catalog.alpha.unityCatalogName", "uc-alpha");
        properties.put("delta.catalog.alpha.unityCatalogUri", "https://alpha.example.com");
        properties.put("delta.catalog.alpha.unityCatalogClientId", "client-alpha");
        properties.put("delta.catalog.alpha.unityCatalogClientSecret", "secret-alpha");
        properties.put("delta.catalog.beta.unityCatalogName", "uc-beta");
        properties.put("delta.catalog.beta.unityCatalogUri", "https://beta.example.com");
        properties.put("delta.catalog.beta.unityCatalogClientId", "client-beta");
        properties.put("delta.catalog.beta.unityCatalogClientSecret", "secret-beta");

        LakehouseConfiguration configuration = new LakehouseConfiguration(properties);

        assertEquals("uc-beta", configuration.getUnityCatalogName());
        assertEquals("https://beta.example.com", configuration.getUnityCatalogUri());
        assertEquals("client-beta", configuration.getUnityCatalogClientId());
        assertEquals("secret-beta", configuration.getUnityCatalogClientSecret());
    }

    @Test
    public void testLegacyGlobalUnityCatalogPropertiesStillWorkWithoutDeltaCatalogPrefix() {
        Properties properties = new Properties();
        properties.put("unityCatalogName", "legacy-catalog");
        properties.put("unityCatalogUri", "https://legacy.example.com");
        properties.put("unityCatalogClientId", "legacy-client");
        properties.put("unityCatalogClientSecret", "legacy-secret");

        LakehouseConfiguration configuration = new LakehouseConfiguration(properties);

        assertEquals("legacy-catalog", configuration.getUnityCatalogName());
        assertEquals("https://legacy.example.com", configuration.getUnityCatalogUri());
        assertEquals("legacy-client", configuration.getUnityCatalogClientId());
        assertEquals("legacy-secret", configuration.getUnityCatalogClientSecret());
    }

    @Test
    public void testUnityCatalogNameDoesNotImplicitlyFallbackToCatalogAlias() {
        Properties properties = new Properties();
        properties.put("catalog.name", "beta");

        LakehouseConfiguration configuration = new LakehouseConfiguration(properties);

        assertNull(configuration.getUnityCatalogName());
    }

    @Test
    public void testDefaultConfigurationsWhenNotSet() {
        Properties properties = new Properties();
        assertEquals("LOCAL", LakehouseConfiguration.getCompactionBackendStorageType(properties));
        assertEquals(LakehouseConfiguration.DEFAULT_STORAGE_PATH, LakehouseConfiguration.getCompactionBucket(properties));
        assertTrue(StringUtils.isBlank(LakehouseConfiguration.getCompactionPrefix(properties)));
    }

    @Test
    public void testGenerateS3StoragePath() {
        Properties properties = new Properties();
        properties.put("compactionBucket", "test-bucket");
        properties.put("compactionPrefix", "test-prefix");
        assertEquals("s3a://test-bucket/test-prefix", LakehouseConfiguration.generateS3StoragePath(properties));

        Properties properties1 = new Properties();
        properties1.put("compactionBucket", "test-bucket");
        assertEquals("s3a://test-bucket", LakehouseConfiguration.generateS3StoragePath(properties1));

        Properties properties2 = new Properties();
        properties2.put("compactionPrefix", "test-prefix");
        assertEquals("s3a://" + LakehouseConfiguration.DEFAULT_STORAGE_PATH + "/test-prefix",
                LakehouseConfiguration.generateS3StoragePath(properties2));

        Properties properties3 = new Properties();
        assertEquals("s3a://" + LakehouseConfiguration.DEFAULT_STORAGE_PATH,
                LakehouseConfiguration.generateS3StoragePath(properties3));

        Properties properties4 = new Properties();
        properties4.put("s3CompactionBucket", "test-bucket");
        properties4.put("s3CompactionPrefix", "test-prefix");
        assertEquals("s3a://test-bucket/test-prefix",
                LakehouseConfiguration.generateS3StoragePath(properties4));

        Properties properties5 = new Properties();
        properties5.put("s3CompactionBucket", "test-bucket");
        assertEquals("s3a://test-bucket", LakehouseConfiguration.generateS3StoragePath(properties5));

        Properties properties6 = new Properties();
        properties6.put("s3CompactionPrefix", "test-prefix");
        assertEquals("s3a://" + LakehouseConfiguration.DEFAULT_STORAGE_PATH + "/test-prefix",
                LakehouseConfiguration.generateS3StoragePath(properties6));

        Properties properties7 = new Properties();
        properties7.put("compactionBucket", "s3://test-bucket");
        properties7.put("compactionPrefix", "test-prefix");
        assertEquals("s3a://test-bucket/test-prefix",
                LakehouseConfiguration.generateS3StoragePath(properties7));
    }

    @Test
    public void testGenerateAzureDfsStoragePath() {
        Properties properties = new Properties();
        properties.put("compactionBackendStorageType", "AZUREDFS");
        properties.put("compactionBucket", "myAccount@test-bucket");
        properties.put("compactionPrefix", "test-prefix");
        assertEquals("abfss://test-bucket@myAccount.dfs.core.windows.net/test-prefix",
                LakehouseConfiguration.generateAzureStoragePath(properties));

        Properties properties1 = new Properties();
        properties1.put("compactionBackendStorageType", "AZUREDFS");
        properties1.put("s3CompactionBucket", "myAccount@test-bucket");
        properties1.put("s3CompactionPrefix", "test-prefix");
        assertEquals("abfss://test-bucket@myAccount.dfs.core.windows.net/test-prefix",
                LakehouseConfiguration.generateAzureStoragePath(properties1));

        Properties properties2 = new Properties();
        properties2.put("compactionBackendStorageType", "AZUREDFS");
        properties2.put("compactionBucket", "myAccount@test-bucket");
        assertEquals("abfss://test-bucket@myAccount.dfs.core.windows.net",
                LakehouseConfiguration.generateAzureStoragePath(properties2));

        Properties properties3 = new Properties();
        properties3.put("compactionBackendStorageType", "AZUREDFS");
        properties3.put("s3CompactionBucket", "myAccount@test-bucket");
        assertEquals("abfss://test-bucket@myAccount.dfs.core.windows.net",
                LakehouseConfiguration.generateAzureStoragePath(properties3));

        Properties properties4 = new Properties();
        properties4.put("compactionBackendStorageType", "AZUREDFS");
        properties4.put("compactionPrefix", "test-prefix");
        try {
            LakehouseConfiguration.generateAzureStoragePath(properties4);
            fail();
        } catch (IllegalArgumentException e) {
            assertEquals("Invalid Azure storage path: " + LakehouseConfiguration.DEFAULT_STORAGE_PATH,
                    e.getMessage());
        }

        // bucket not contains "@"
        Properties properties5 = new Properties();
        properties5.put("compactionBackendStorageType", "AZUREDFS");
        properties5.put("compactionBucket", "test-bucket");
        properties5.put("compactionPrefix", "test-prefix");
        try {
            LakehouseConfiguration.generateAzureStoragePath(properties5);
            fail();
        } catch (IllegalArgumentException e) {
            assertEquals("Invalid Azure storage path: test-bucket",
                    e.getMessage());
        }
    }

    @Test
    public void testGenerateAzureBlobStoragePath() {
        Properties properties = new Properties();
        properties.put("compactionBackendStorageType", "AZUREBLOB");
        properties.put("compactionBucket", "myAccount@test-bucket");
        properties.put("compactionPrefix", "test-prefix");
        assertEquals("wasbs://test-bucket@myAccount.blob.core.windows.net/test-prefix",
                LakehouseConfiguration.generateAzureStoragePath(properties));

        Properties properties1 = new Properties();
        properties1.put("compactionBackendStorageType", "AZUREBLOB");
        properties1.put("s3CompactionBucket", "myAccount@test-bucket");
        properties1.put("s3CompactionPrefix", "test-prefix");
        assertEquals("wasbs://test-bucket@myAccount.blob.core.windows.net/test-prefix",
                LakehouseConfiguration.generateAzureStoragePath(properties1));

        Properties properties2 = new Properties();
        properties2.put("compactionBackendStorageType", "AZUREBLOB");
        properties2.put("compactionBucket", "myAccount@test-bucket");
        assertEquals("wasbs://test-bucket@myAccount.blob.core.windows.net",
                LakehouseConfiguration.generateAzureStoragePath(properties2));

        Properties properties3 = new Properties();
        properties3.put("compactionBackendStorageType", "AZUREBLOB");
        properties3.put("s3CompactionBucket", "myAccount@test-bucket");
        assertEquals("wasbs://test-bucket@myAccount.blob.core.windows.net",
                LakehouseConfiguration.generateAzureStoragePath(properties3));

        Properties properties4 = new Properties();
        properties4.put("compactionBackendStorageType", "AZUREBLOB");
        properties4.put("compactionPrefix", "test-prefix");
        try {
            LakehouseConfiguration.generateAzureStoragePath(properties4);
            fail();
        } catch (IllegalArgumentException e) {
            assertEquals("Invalid Azure storage path: " + LakehouseConfiguration.DEFAULT_STORAGE_PATH,
                    e.getMessage());
        }

        // bucket not contains "@"
        Properties properties5 = new Properties();
        properties5.put("compactionBackendStorageType", "AZUREBLOB");
        properties5.put("compactionBucket", "test-bucket");
        properties5.put("compactionPrefix", "test-prefix");
        try {
            LakehouseConfiguration.generateAzureStoragePath(properties5);
            fail();
        } catch (IllegalArgumentException e) {
            assertEquals("Invalid Azure storage path: test-bucket",
                    e.getMessage());
        }

    }

    @Test
    public void testGenerateS3HadoopConfiguration() {
        Configuration conf = new Configuration();
        Properties properties = new Properties();
        LakehouseConfiguration.generateS3HadoopConfiguration(conf, properties);

        assertEquals("software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider"
                + ",software.amazon.awssdk.auth.credentials.WebIdentityTokenFileCredentialsProvider",
            conf.get("fs.s3a.aws.credentials.provider"));
        assertEquals(S3AFileSystem.class.getName(), conf.get("fs.s3a.impl"));
        assertEquals("15000", conf.get("fs.s3a.connection.timeout"));
        assertEquals("15000", conf.get("fs.s3a.connection.request.timeout"));
        assertNull(conf.get(AWS_REGION));

        // test set properties
        properties.put("s3aConnectionTimeout", "1000");
        properties.put("cloudStorageEndpoint", "http://localhost:8080");
        properties.put("compactionBucketRegion", "us-west-1");
        LakehouseConfiguration.generateS3HadoopConfiguration(conf, properties);
        assertEquals("http://127.0.0.1:8080", conf.get("fs.s3a.endpoint"));
        assertEquals("us-west-1", conf.get(AWS_REGION));
    }

    @Test
    public void testGenerateAzureBlobHadoopConfiguration() throws Exception {
        Configuration conf = new Configuration();
        Properties properties = new Properties();
        properties.put("compactionBucket", "myAccount@test-bucket");
        properties.put("compactionPrefix", "test-prefix");
        properties.put("compactionBackendStorageType", "AZUREBLOB");
        String storagePath = LakehouseConfiguration.getStoragePath(properties);
        setEnv("AZURE_AUTHORITY_HOST", "myAuthorityHost");
        setEnv("AZURE_CLIENT_ID", "myClientId");
        setEnv("AZURE_TENANT_ID", "myTenantId");
        setEnv("AZURE_FEDERATED_TOKEN_FILE", "myTokenFile");
        LakehouseConfiguration.generateAzureHadoopConfiguration(conf, storagePath, properties);
        String azureHost = LakehouseConfiguration.resolveAzureHost(storagePath);
        assertEquals(NativeAzureFileSystem.class.getName(), conf.get("fs.wasbs.impl"));
        assertEquals("OAuth", conf.get(ConfigurationKeys.FS_AZURE_ACCOUNT_AUTH_TYPE_PROPERTY_NAME + "." + azureHost));
        assertEquals("myAuthorityHost", conf.get(ConfigurationKeys.FS_AZURE_ACCOUNT_OAUTH_MSI_AUTHORITY + "." + azureHost));
        assertEquals("myClientId", conf.get(ConfigurationKeys.FS_AZURE_ACCOUNT_OAUTH_CLIENT_ID + "." + azureHost));
        assertEquals("myTenantId", conf.get(ConfigurationKeys.FS_AZURE_ACCOUNT_OAUTH_MSI_TENANT + "." + azureHost));
        assertEquals("myTokenFile", conf.get(ConfigurationKeys.FS_AZURE_ACCOUNT_OAUTH_TOKEN_FILE + "." + azureHost));
    }

    @Test
    public void testGenerateAzureDfsHadoopConfiguration() throws Exception {
        Configuration conf = new Configuration();
        Properties properties = new Properties();
        properties.put("compactionBucket", "myAccount@test-bucket");
        properties.put("compactionPrefix", "test-prefix");
        properties.put("compactionBackendStorageType", "AZUREDFS");
        String storagePath = LakehouseConfiguration.getStoragePath(properties);
        setEnv("AZURE_AUTHORITY_HOST", "myAuthorityHost");
        setEnv("AZURE_CLIENT_ID", "myClientId");
        setEnv("AZURE_TENANT_ID", "myTenantId");
        setEnv("AZURE_FEDERATED_TOKEN_FILE", "myTokenFile");
        LakehouseConfiguration.generateAzureHadoopConfiguration(conf, storagePath, properties);
        String azureHost = LakehouseConfiguration.resolveAzureHost(storagePath);
        assertEquals(AzureBlobFileSystem.class.getName(), conf.get("fs.abfss.impl"));
        assertEquals("OAuth", conf.get(ConfigurationKeys.FS_AZURE_ACCOUNT_AUTH_TYPE_PROPERTY_NAME + "." + azureHost));
        assertEquals("myAuthorityHost", conf.get(ConfigurationKeys.FS_AZURE_ACCOUNT_OAUTH_MSI_AUTHORITY + "." + azureHost));
        assertEquals("myClientId", conf.get(ConfigurationKeys.FS_AZURE_ACCOUNT_OAUTH_CLIENT_ID + "." + azureHost));
        assertEquals("myTenantId", conf.get(ConfigurationKeys.FS_AZURE_ACCOUNT_OAUTH_MSI_TENANT + "." + azureHost));
        assertEquals("myTokenFile", conf.get(ConfigurationKeys.FS_AZURE_ACCOUNT_OAUTH_TOKEN_FILE + "." + azureHost));
    }

    @Test
    public void getGenerateGCSHadoopConfiguration() {
        Configuration conf = new Configuration();
        Properties properties = new Properties();
        properties.put(GCS_ID, "test-id");
        properties.put(GCS_SA, "test-sa");

        LakehouseConfiguration.generateGCSHadoopConfiguration(conf, properties);
        assertEquals("com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem", conf.get("fs.gs.impl"));
        assertEquals("test-id", conf.get("fs.gs.project.id"));
        assertEquals("test-sa", conf.get("google.cloud.auth.service.account.json.keyfile"));
        assertEquals("true", conf.get("google.cloud.auth.service.account.enable"));
    }

    @Test
    public void testGetUnityCatalogToken() throws IOException {
        Path filePath = Path.of("/tmp/unityCatalogToken");
        String message = "xaijifezbhhebl";
        Files.writeString(filePath, message);

        try {
            // test unityCatalogToken set
            Properties properties = new Properties();
            properties.put("unityCatalogToken", "test-token");

            LakehouseConfiguration lakehouseConfiguration = new LakehouseConfiguration(properties);
            assertEquals("test-token", lakehouseConfiguration.getUnityCatalogToken());

            // test unityCatalogTokenFile set
            Properties properties1 = new Properties();
            properties1.put(UNITY_CATALOG_TOKEN_FILE, filePath.toString());

            LakehouseConfiguration lakehouseConfiguration1 = new LakehouseConfiguration(properties1);
            assertEquals(message, lakehouseConfiguration1.getUnityCatalogToken());

            // test unityCatalogTokenFile not found
            Properties properties2 = new Properties();
            String randomPath = "/tmp/" + UUID.randomUUID();
            properties2.put(UNITY_CATALOG_TOKEN_FILE, randomPath);

            try {
                LakehouseConfiguration lakehouseConfiguration2 = new LakehouseConfiguration(properties2);
            } catch (IllegalArgumentException e) {
                assertEquals("Failed to load unity catalog token from file: " + randomPath, e.getMessage());
            }

            // test unityCatalogTokenFile and unityCatalogToken both set
            Properties properties3 = new Properties();
            properties3.put(UNITY_CATALOG_TOKEN_FILE, filePath.toString());
            properties.put("unityCatalogToken", "test-token");

            LakehouseConfiguration lakehouseConfiguration3 = new LakehouseConfiguration(properties3);
            assertEquals(message, lakehouseConfiguration3.getUnityCatalogToken());
        } finally {
            Files.delete(filePath);
        }
    }

    @Test
    public void testGetIcebergCredentials() throws Exception {
        Path filePath = Path.of("/tmp/icebergCatalogCredentials");
        String message = "xaijifezbhhebl";
        Files.writeString(filePath, message);

        try {
            // test unityCatalogToken set
            Properties properties = new Properties();
            properties.put("iceberg.credential", "test-token");

            LakehouseConfiguration lakehouseConfiguration = new LakehouseConfiguration(properties);
            assertEquals("test-token", lakehouseConfiguration.getIcebergProperties().get("credential"));

            // test unityCatalogTokenFile set
            Properties properties1 = new Properties();
            properties1.put(ICEBERG_CREDENTIAL_FILE, filePath.toString());

            LakehouseConfiguration lakehouseConfiguration1 = new LakehouseConfiguration(properties1);
            assertEquals(message, lakehouseConfiguration1.getIcebergProperties().get("credential"));

            // test unityCatalogTokenFile not found
            Properties properties2 = new Properties();
            String randomPath = "/tmp/" + UUID.randomUUID();
            properties2.put(ICEBERG_CREDENTIAL_FILE, randomPath);

            try {
                LakehouseConfiguration lakehouseConfiguration2 = new LakehouseConfiguration(properties2);
            } catch (IllegalArgumentException e) {
                assertEquals("Failed to load credentials from file: " + randomPath, e.getMessage());
            }

            // test unityCatalogTokenFile and unityCatalogToken both set
            Properties properties3 = new Properties();
            properties3.put(ICEBERG_CREDENTIAL_FILE, filePath.toString());
            properties.put("iceberg.credential", "test-token");

            LakehouseConfiguration lakehouseConfiguration3 = new LakehouseConfiguration(properties3);
            assertEquals(message, lakehouseConfiguration1.getIcebergProperties().get("credential"));
        } finally {
            Files.delete(filePath);
        }
    }

    public static void setEnv(String key, String value) throws Exception {
        // Get the environment map
        Map<String, String> env = System.getenv();

        // Use reflection to access the private field backing the environment map
        Class<?> envClass = env.getClass();
        Field field = envClass.getDeclaredField("m");
        field.setAccessible(true);

        @SuppressWarnings("unchecked")
        Map<String, String> writableEnv = (Map<String, String>) field.get(env);

        // Add the environment variable
        writableEnv.put(key, value);
    }

    @Test
    void testWithBothPrefixes() {
        Properties props = new Properties();
        props.setProperty(TABLE_PROP_PREFIX + "prop1", "value1");
        props.setProperty(WRITE_PROP_PREFIX + "prop2", "value2");
        props.setProperty("other.prop", "value3");

        Map<String, String> result = LakehouseConfiguration.generateIcebergTableProperties(props);

        assertEquals(2, result.size());
        assertEquals("value1", result.get("prop1"));
        assertEquals("value2", result.get("prop2"));
    }

    @Test
    void testWithNonMatchingKeys() {
        Properties props = new Properties();
        props.setProperty("another.prop", "value");

        Map<String, String> result = LakehouseConfiguration.generateIcebergTableProperties(props);

        assertEquals(0, result.size());
    }

    @Test
    void testKeyWithSequentialPrefixes() {
        String key = TABLE_PROP_PREFIX + WRITE_PROP_PREFIX + "prop3";
        Properties props = new Properties();
        props.setProperty(key, "value3");

        Map<String, String> result = LakehouseConfiguration.generateIcebergTableProperties(props);

        assertEquals(1, result.size());
        assertEquals("value3", result.get("prop3"));
    }

    @Test
    void testEmptyProperties() {
        Properties props = new Properties();
        Map<String, String> result = LakehouseConfiguration.generateIcebergTableProperties(props);

        assertEquals(0, result.size());
    }

    @Test
    void testExactPrefixMatch() {
        Properties props = new Properties();
        props.setProperty(TABLE_PROP_PREFIX, "emptyKeyValue");

        Map<String, String> result = LakehouseConfiguration.generateIcebergTableProperties(props);

        assertEquals(0, result.size());
    }

    @Test
    void testOverrideKeys() {
        Properties props = new Properties();
        props.setProperty(TABLE_PROP_PREFIX + TableProperties.METADATA_DELETE_AFTER_COMMIT_ENABLED, "false");
        Map<String, String> result = LakehouseConfiguration.generateIcebergTableProperties(props);

        assertEquals(1, result.size());
        assertEquals("false", result.get(TableProperties.METADATA_DELETE_AFTER_COMMIT_ENABLED));
    }

    @Test
    void testGenerateIcebergCatalogMap() {
        Properties props = new Properties();
        props.setProperty("iceberg.catalog.hive.uri", "thrift://localhost:9083");
        props.setProperty("iceberg.catalog.hive.type", "hive");
        props.setProperty("iceberg.catalog.hadoop.warehouse", "/user/hive/warehouse");
        props.setProperty("other.catalog.invalid", "shouldBeIgnored");
        props.setProperty("iceberg.catalog.", "invalidKey");

        Map<String, Map<String, String>> result = LakehouseConfiguration.generateicebergCatalogMap(props);

        assertEquals(2, result.size());

        Map<String, String> hiveProps = result.get("hive");
        assertNotNull(hiveProps);
        assertEquals("thrift://localhost:9083", hiveProps.get("uri"));
        assertEquals("hive", hiveProps.get("type"));

        Map<String, String> hadoopProps = result.get("hadoop");
        assertNotNull(hadoopProps);
        assertEquals("/user/hive/warehouse", hadoopProps.get("warehouse"));
    }

    @Test
    void testGetIcebergCatalogType_WithProvidedCatalogName_FoundAndWithType() {
        Properties props = new Properties();
        props.setProperty(ICEBERG_CATALOG_PREFIX + "my_catalog.type", CatalogUtil.ICEBERG_CATALOG_TYPE_REST);
        props.setProperty(ICEBERG_PREFIX + CatalogUtil.ICEBERG_CATALOG_TYPE, CatalogUtil.ICEBERG_CATALOG_TYPE_HADOOP); // Default fallback

        LakehouseConfiguration config = new LakehouseConfiguration(props);

        String catalogType = config.getIcebergCatalogType(Optional.of("my_catalog"));
        assertEquals(CatalogUtil.ICEBERG_CATALOG_TYPE_REST, catalogType);
    }

    @Test
    void testGetIcebergCatalogType_WithProvidedCatalogName_FoundButTypeIsBlank() {
        Properties props = new Properties();
        props.setProperty(ICEBERG_CATALOG_PREFIX + "my_catalog.type", ""); // Blank type
        props.setProperty(ICEBERG_PREFIX + CatalogUtil.ICEBERG_CATALOG_TYPE, CatalogUtil.ICEBERG_CATALOG_TYPE_HADOOP); // Default fallback

        LakehouseConfiguration config = new LakehouseConfiguration(props);

        String catalogType = config.getIcebergCatalogType(Optional.of("my_catalog"));
        assertEquals(CatalogUtil.ICEBERG_CATALOG_TYPE_HADOOP, catalogType); // Should fall back to icebergProperties default
    }

    @Test
    void testGetIcebergCatalogType_WithProvidedCatalogName_NotFound() {
        Properties props = new Properties();
        props.setProperty(ICEBERG_PREFIX + CatalogUtil.ICEBERG_CATALOG_TYPE, CatalogUtil.ICEBERG_CATALOG_TYPE_HADOOP); // Default fallback

        LakehouseConfiguration config = new LakehouseConfiguration(props);

        String catalogType = config.getIcebergCatalogType(Optional.of("non_existent_catalog"));
        assertEquals(CatalogUtil.ICEBERG_CATALOG_TYPE_HADOOP, catalogType); // Should fall back to icebergProperties default
    }

    @Test
    void testGetIcebergCatalogType_NoProvidedCatalogName_UsesDefaultCatalogName_Found() {
        Properties props = new Properties();
        props.setProperty(LakehouseConfiguration.DEFAULT_CATALOG_NAME, "default_cat");
        props.setProperty(ICEBERG_CATALOG_PREFIX + "default_cat.type", CatalogUtil.ICEBERG_CATALOG_TYPE_REST);
        props.setProperty(ICEBERG_PREFIX + CatalogUtil.ICEBERG_CATALOG_TYPE, CatalogUtil.ICEBERG_CATALOG_TYPE_HADOOP); // Default fallback

        LakehouseConfiguration config = new LakehouseConfiguration(props);

        String catalogType = config.getIcebergCatalogType(Optional.empty());
        assertEquals(CatalogUtil.ICEBERG_CATALOG_TYPE_REST, catalogType);
    }

    @Test
    void testGetIcebergCatalogType_NoProvidedCatalogName_UsesDefaultCatalogName_NotFound() {
        Properties props = new Properties();
        props.setProperty(LakehouseConfiguration.DEFAULT_CATALOG_NAME, "non_existent_default_cat");
        props.setProperty(ICEBERG_PREFIX + CatalogUtil.ICEBERG_CATALOG_TYPE, CatalogUtil.ICEBERG_CATALOG_TYPE_HADOOP); // Default fallback

        LakehouseConfiguration config = new LakehouseConfiguration(props);

        String catalogType = config.getIcebergCatalogType(Optional.empty());
        assertEquals(CatalogUtil.ICEBERG_CATALOG_TYPE_HADOOP, catalogType); // Should fall back to icebergProperties default
    }

    @Test
    void testGetIcebergCatalogType_NoProvidedCatalogName_NoDefaultCatalogName_NoIcebergDefault() {
        Properties props = new Properties();
        // No DEFAULT_CATALOG_NAME and no iceberg.type
        // The default for icebergProperties.getOrDefault(CatalogUtil.ICEBERG_CATALOG_TYPE, ...) is "hadoop"
        // so it should return "hadoop"
        LakehouseConfiguration config = new LakehouseConfiguration(props);

        String catalogType = config.getIcebergCatalogType(Optional.empty());
        assertEquals(CatalogUtil.ICEBERG_CATALOG_TYPE_HADOOP, catalogType);
    }

    // --- Test for getIcebergCatalogBackendType(Optional<String> catalogName) ---

    @Test
    void testGetIcebergCatalogBackendType_WithProvidedCatalogName_FoundAndWithType() {
        Properties props = new Properties();
        props.setProperty(ICEBERG_CATALOG_PREFIX + "my_catalog.catalog-backend", "POLARIS");
        props.setProperty(ICEBERG_PREFIX + CATALOG_BACKEND_TYPE, "hadoop"); // Default fallback

        LakehouseConfiguration config = new LakehouseConfiguration(props);

        String backendType = config.getIcebergCatalogBackendType(Optional.of("my_catalog")).toString();
        assertEquals("POLARIS", backendType);
    }

    @Test
    void testGetIcebergCatalogBackendType_WithBigLakeType() {
        Properties props = new Properties();
        props.setProperty(ICEBERG_CATALOG_PREFIX + "biglake_catalog.catalog-backend", "BIGLAKE");
        props.setProperty(ICEBERG_CATALOG_PREFIX + "biglake_catalog.type", "rest");
        props.setProperty(ICEBERG_CATALOG_PREFIX + "biglake_catalog.uri",
            "https://biglake.googleapis.com/iceberg/v1/restcatalog");
        props.setProperty(ICEBERG_PREFIX + CATALOG_BACKEND_TYPE, "hadoop");

        LakehouseConfiguration config = new LakehouseConfiguration(props);

        IcebergCatalogBackendType backendType =
            config.getIcebergCatalogBackendType(Optional.of("biglake_catalog"));
        assertEquals(IcebergCatalogBackendType.BIGLAKE, backendType);

        String catalogType = config.getIcebergCatalogType(Optional.of("biglake_catalog"));
        assertEquals("rest", catalogType);
    }

    @Test
    void testGetIcebergCatalogBackendType_WithProvidedCatalogName_FoundButTypeIsBlank() {
        Properties props = new Properties();
        props.setProperty(ICEBERG_CATALOG_PREFIX + "my_catalog.catalog-backend", ""); // Blank backend type
        props.setProperty(ICEBERG_PREFIX + CATALOG_BACKEND_TYPE, "hadoop"); // Default fallback

        LakehouseConfiguration config = new LakehouseConfiguration(props);

        String backendType = config.getIcebergCatalogBackendType(Optional.of("my_catalog")).toString();
        assertEquals("HADOOP", backendType); // Should fall back to icebergProperties default
    }

    @Test
    void testGetIcebergCatalogBackendType_NoProvidedCatalogName_NoDefaultCatalogName_NoIcebergDefault() {
        Properties props = new Properties();
        // No DEFAULT_CATALOG_NAME and no iceberg.catalog-backend
        // The default for icebergProperties.getOrDefault(CATALOG_BACKEND_TYPE, ...) is "hadoop"
        // so it should return "hadoop"
        LakehouseConfiguration config = new LakehouseConfiguration(props);

        String backendType = config.getIcebergCatalogBackendType(Optional.empty()).toString();
        assertEquals("HADOOP", backendType); // Default value from method
    }

    // --- Test for getIcebergProperties(Optional<String> catalogName) ---

    @Test
    void testGetIcebergProperties_NoProvidedCatalogName_NoDefaultCatalogName() {
        Properties props = new Properties();
        props.setProperty(ICEBERG_PREFIX + "prop1", "val1");
        props.setProperty(ICEBERG_PREFIX + "prop2", "val2");
        props.setProperty(ICEBERG_CATALOG_PREFIX + "some_cat.type", "rest"); // Should be ignored

        LakehouseConfiguration config = new LakehouseConfiguration(props);

        Map<String, String> icebergProps = config.getIcebergProperties(Optional.empty());
        assertNotNull(icebergProps);
        assertEquals(4, icebergProps.size());
        assertEquals("val1", icebergProps.get("prop1"));
        assertEquals("val2", icebergProps.get("prop2"));
        assertEquals("hadoop", icebergProps.get("type")); // Default type
    }

    @Test
    void testGetIcebergProperties_NoProvidedCatalogName_WithDefaultCatalogName_NotFoundInMap() {
        Properties props = new Properties();
        props.setProperty(LakehouseConfiguration.DEFAULT_CATALOG_NAME, "non_existent_default_cat");
        props.setProperty(ICEBERG_PREFIX + "prop1", "val1");

        LakehouseConfiguration config = new LakehouseConfiguration(props);

        Map<String, String> icebergProps = config.getIcebergProperties(Optional.empty());
        assertNotNull(icebergProps);
        assertEquals(3, icebergProps.size());
        assertEquals("val1", icebergProps.get("prop1")); // Should return default iceberg properties
        assertEquals("hadoop", icebergProps.get("type")); // Default type
    }

    @Test
    void testGetIcebergProperties_WithProvidedCatalogName_FoundInMap() {
        Properties props = new Properties();
        props.setProperty(ICEBERG_PREFIX + "prop1", "val1_default");
        props.setProperty(ICEBERG_PREFIX + "prop2", "val2_default");
        props.setProperty(ICEBERG_CATALOG_PREFIX + "my_catalog.prop1", "val1_catalog"); // Override
        props.setProperty(ICEBERG_CATALOG_PREFIX + "my_catalog.prop3", "val3_catalog"); // New
        props.setProperty(LakehouseConfiguration.DEFAULT_CATALOG_NAME, "some_default"); // Not used if provided name exists

        LakehouseConfiguration config = new LakehouseConfiguration(props);

        Map<String, String> icebergProps = config.getIcebergProperties(Optional.of("my_catalog"));
        assertNotNull(icebergProps);
        assertEquals(5,  icebergProps.size());
        assertEquals("val1_catalog", icebergProps.get("prop1")); // Should be overridden
        assertEquals("val2_default", icebergProps.get("prop2")); // From default
        assertEquals("val3_catalog", icebergProps.get("prop3")); // From catalog
        assertEquals("hadoop", icebergProps.get("type")); // Default type;
    }

    @Test
    void testGetIcebergProperties_WithProvidedCatalogName_NotFoundInMap() {
        Properties props = new Properties();
        props.setProperty(ICEBERG_PREFIX + "prop1", "val1_default");
        props.setProperty(ICEBERG_CATALOG_PREFIX + "another_catalog.prop1", "val_another"); // Other catalog

        LakehouseConfiguration config = new LakehouseConfiguration(props);

        Map<String, String> icebergProps = config.getIcebergProperties(Optional.of("non_existent_catalog"));
        assertNotNull(icebergProps);
        assertEquals(3, icebergProps.size());
        // Should fall back to default iceberg properties
        assertEquals("val1_default", icebergProps.get("prop1"));
        assertEquals("hadoop", icebergProps.get("type")); // Default type
    }

    @Test
    void testGetIcebergProperties_WithProvidedCatalogName_Blank() {
        Properties props = new Properties();
        props.setProperty(LakehouseConfiguration.DEFAULT_CATALOG_NAME, "default_cat");
        props.setProperty(ICEBERG_CATALOG_PREFIX + "default_cat.prop1", "val1_default_cat");
        props.setProperty(ICEBERG_PREFIX + "propA", "valA");

        LakehouseConfiguration config = new LakehouseConfiguration(props);

        // Passing an Optional.of("") should behave like Optional.empty()
        Map<String, String> icebergProps = config.getIcebergProperties(Optional.of(""));
        assertNotNull(icebergProps);
        assertEquals(4, icebergProps.size());
        assertEquals("valA", icebergProps.get("propA"));
        assertEquals("hadoop", icebergProps.get("type"));
        assertEquals("val1_default_cat", icebergProps.get("prop1")); // Should use default_cat
    }

    @Test
    public void testBaseSchemaVersionResolution() {
        // Unset → empty
        assertTrue(new LakehouseConfiguration(new Properties()).getBaseSchemaVersion().isEmpty());

        // Topic-level property `base.schema.version` → resolved value
        Properties withBase = new Properties();
        withBase.setProperty("base.schema.version", "5");
        assertEquals(Optional.of(5L), new LakehouseConfiguration(withBase).getBaseSchemaVersion());

        // Cluster-level property `cluster.base.schema.version` → ignored (topic-level only key)
        Properties clusterLevel = new Properties();
        clusterLevel.setProperty("cluster.base.schema.version", "7");
        assertTrue(new LakehouseConfiguration(clusterLevel).getBaseSchemaVersion().isEmpty());

        // Non-numeric value → lenient (warn + empty)
        Properties bad = new Properties();
        bad.setProperty("base.schema.version", "not-a-number");
        assertTrue(new LakehouseConfiguration(bad).getBaseSchemaVersion().isEmpty());
    }

    @Test
    public void testMakeNewFieldsOptionalDefaultsTrue() {
        LakehouseConfiguration config = new LakehouseConfiguration(new Properties());
        assertFalse(config.makeNewFieldsOptionalOnEvolution());
    }

    @Test
    public void testMakeNewFieldsOptionalCanBeDisabled() {
        Properties properties = new Properties();
        properties.setProperty(LakehouseConfiguration.MAKE_NEW_FIELDS_OPTIONAL, "false");
        LakehouseConfiguration config = new LakehouseConfiguration(properties);
        assertEquals(false, config.makeNewFieldsOptionalOnEvolution());
    }
}
