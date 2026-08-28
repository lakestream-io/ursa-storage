/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.kafka.reader;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem;
import java.util.Map;
import java.util.Properties;
import org.apache.hadoop.fs.azurebfs.AzureBlobFileSystem;
import org.apache.hadoop.fs.azurebfs.oauth2.WorkloadIdentityTokenProvider;
import org.junit.jupiter.api.Test;

class ReaderConfigurationTest {

    @Test
    void mapsLocalStackCredentialsEndpointRegionAndPathStyleToS3A() {
        Properties properties = new Properties();
        properties.setProperty("compactionBackendStorageType", "S3");
        properties.setProperty("compactionBucket", "test-bucket");
        properties.setProperty("compactionPrefix", "/lakehouse");
        properties.setProperty("cloudStorageEndpoint", "http://localhost:4566");
        properties.setProperty("compactionBucketRegion", "us-east-1");
        properties.setProperty("s3AccessKeyId", "access-key");
        properties.setProperty("s3SecretAccessKey", "secret-key");
        properties.setProperty("s3SessionToken", "session-token");

        ReaderConfiguration configuration = new ReaderConfiguration(properties);

        assertThat(configuration.storagePath()).isEqualTo("s3a://test-bucket/lakehouse");
        assertThat(configuration.hadoopConfiguration().get("fs.s3a.endpoint"))
                .isEqualTo("http://localhost:4566");
        assertThat(configuration.hadoopConfiguration().get("fs.s3a.endpoint.region"))
                .isEqualTo("us-east-1");
        assertThat(configuration.hadoopConfiguration().get("fs.s3a.access.key"))
                .isEqualTo("access-key");
        assertThat(configuration.hadoopConfiguration().get("fs.s3a.secret.key"))
                .isEqualTo("secret-key");
        assertThat(configuration.hadoopConfiguration().get("fs.s3a.session.token"))
                .isEqualTo("session-token");
        assertThat(configuration.hadoopConfiguration().get("fs.s3a.aws.credentials.provider"))
                .isEqualTo("org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider");
        assertThat(configuration.hadoopConfiguration().getBoolean("fs.s3a.path.style.access", false)).isTrue();
    }

    @Test
    void preservesArbitraryHadoopOverrides() {
        Properties properties = new Properties();
        properties.setProperty("storagePath", "/tmp/lakehouse");
        properties.setProperty("hadoop.fs.file.impl.disable.cache", "true");

        ReaderConfiguration configuration = new ReaderConfiguration(properties);

        assertThat(configuration.hadoopConfiguration().getBoolean("fs.file.impl.disable.cache", false)).isTrue();
    }

    @Test
    void configuresGcsConnectorCredentialsAndEndpoint() {
        Properties properties = new Properties();
        properties.setProperty("compactionBackendStorageType", "GCS");
        properties.setProperty("compactionBucket", "test-bucket");
        properties.setProperty("compactionPrefix", "/lakehouse");
        properties.setProperty("googleCloudProjectID", "test-project");
        properties.setProperty("googleCloudServiceAccountFile", "/var/run/gcp/key.json");
        properties.setProperty("cloudStorageEndpoint", "http://localhost:4443");

        ReaderConfiguration configuration = new ReaderConfiguration(properties);

        assertThat(configuration.storagePath()).isEqualTo("gs://test-bucket/lakehouse");
        assertThat(configuration.hadoopConfiguration().get("fs.gs.impl"))
                .isEqualTo(GoogleHadoopFileSystem.class.getName());
        assertThat(configuration.hadoopConfiguration().get("fs.gs.project.id"))
                .isEqualTo("test-project");
        assertThat(configuration.hadoopConfiguration()
                .getBoolean("google.cloud.auth.service.account.enable", false)).isTrue();
        assertThat(configuration.hadoopConfiguration().get("google.cloud.auth.service.account.json.keyfile"))
                .isEqualTo("/var/run/gcp/key.json");
        assertThat(configuration.hadoopConfiguration().getBoolean("fs.gs.auth.null.enable", false)).isTrue();
        assertThat(configuration.hadoopConfiguration()
                .getBoolean("fs.gs.auth.service.account.enable", true)).isFalse();
        assertThat(configuration.hadoopConfiguration().get("fs.gs.storage.root.url"))
                .isEqualTo("http://localhost:4443");
    }

    @Test
    void rejectsAzureBlobBecauseHadoopNoLongerProvidesWasb() {
        Properties properties = new Properties();
        properties.setProperty("compactionBackendStorageType", "AZURE_BLOB");
        properties.setProperty("compactionBucket", "storage-account@container");
        properties.setProperty("compactionPrefix", "/lakehouse");

        assertThatThrownBy(() -> new ReaderConfiguration(properties, Map.of()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("AZUREBLOB")
                .hasMessageContaining("Hadoop 3.5 removed the WASB connector")
                .hasMessageContaining("use AZUREDFS");
    }

    @Test
    void configuresAzureDfsConnector() {
        Properties properties = new Properties();
        properties.setProperty("compactionBackendStorageType", "AZUREDFS");
        properties.setProperty("compactionBucket", "storage-account@container");
        properties.setProperty("compactionPrefix", "/lakehouse");

        ReaderConfiguration configuration = new ReaderConfiguration(properties, Map.of(
                "AZURE_AUTHORITY_HOST", "https://login.microsoftonline.com/",
                "AZURE_CLIENT_ID", "client-id",
                "AZURE_TENANT_ID", "tenant-id",
                "AZURE_FEDERATED_TOKEN_FILE", "/var/run/secrets/azure/tokens/azure-identity-token"));
        String azureHost = "storage-account.dfs.core.windows.net";

        assertThat(configuration.storagePath())
                .isEqualTo("abfss://container@storage-account.dfs.core.windows.net/lakehouse");
        assertThat(configuration.hadoopConfiguration().get("fs.abfss.impl"))
                .isEqualTo(AzureBlobFileSystem.class.getName());
        assertThat(configuration.hadoopConfiguration().get("fs.azure.account.auth.type." + azureHost))
                .isEqualTo("OAuth");
        assertThat(configuration.hadoopConfiguration().get("fs.azure.account.oauth.provider.type." + azureHost))
                .isEqualTo(WorkloadIdentityTokenProvider.class.getName());
        assertThat(configuration.hadoopConfiguration().get("fs.azure.account.oauth2.msi.authority." + azureHost))
                .isEqualTo("https://login.microsoftonline.com/");
        assertThat(configuration.hadoopConfiguration().get("fs.azure.account.oauth2.client.id." + azureHost))
                .isEqualTo("client-id");
        assertThat(configuration.hadoopConfiguration().get("fs.azure.account.oauth2.msi.tenant." + azureHost))
                .isEqualTo("tenant-id");
        assertThat(configuration.hadoopConfiguration().get("fs.azure.account.oauth2.token.file." + azureHost))
                .isEqualTo("/var/run/secrets/azure/tokens/azure-identity-token");
    }

    @Test
    void rejectsAzureLocalBecauseHadoopNoLongerProvidesWasb() {
        Properties properties = new Properties();
        properties.setProperty("compactionBackendStorageType", "AZURELOCAL");
        properties.setProperty("compactionBucket", "storage-account@container");

        assertThatThrownBy(() -> new ReaderConfiguration(properties, Map.of()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("AZURELOCAL")
                .hasMessageContaining("Hadoop 3.5 removed the WASB connector")
                .hasMessageContaining("use AZUREDFS");
    }

    @Test
    void supportsLegacyCompactionBucketAliasesForGcs() {
        Properties properties = new Properties();
        properties.setProperty("compactionBackendStorageType", "GCS");
        properties.setProperty("s3CompactionBucket", "legacy-bucket");
        properties.setProperty("s3CompactionPrefix", "/lakehouse");

        ReaderConfiguration configuration = new ReaderConfiguration(properties);

        assertThat(configuration.storagePath()).isEqualTo("gs://legacy-bucket/lakehouse");
    }

    @Test
    void rejectsInvalidAzureDfsBucket() {
        Properties properties = new Properties();
        properties.setProperty("compactionBackendStorageType", "AZUREDFS");
        properties.setProperty("compactionBucket", "missing-container");

        assertThatThrownBy(() -> new ReaderConfiguration(properties))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Invalid Azure storage path");
    }

    @Test
    void rejectsLegacyWasbPathForAzureDfs() {
        Properties properties = new Properties();
        properties.setProperty("compactionBackendStorageType", "AZUREDFS");
        properties.setProperty(
                "compactionBucket", "wasbs://container@storage-account.blob.core.windows.net");

        assertThatThrownBy(() -> new ReaderConfiguration(properties))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("must use the abfss:// scheme");
    }

    @Test
    void rejectsPartialStaticCredentials() {
        Properties properties = new Properties();
        properties.setProperty("compactionBackendStorageType", "S3");
        properties.setProperty("compactionBucket", "test-bucket");
        properties.setProperty("s3AccessKeyId", "access-key");

        assertThatThrownBy(() -> new ReaderConfiguration(properties))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("s3SecretAccessKey");
    }
}
