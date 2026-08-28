/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.kafka.reader;

import com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem;
import java.nio.file.Paths;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.azurebfs.AzureBlobFileSystem;
import org.apache.hadoop.fs.azurebfs.oauth2.WorkloadIdentityTokenProvider;

final class ReaderConfiguration {

    private static final String HADOOP_PREFIX = "hadoop.";
    private static final String DEFAULT_STORAGE_PATH = Paths.get("data").toAbsolutePath().toString();

    private final Properties properties;
    private final Configuration hadoopConfiguration;
    private final String storagePath;

    ReaderConfiguration(Properties properties) {
        this(properties, System.getenv());
    }

    ReaderConfiguration(Properties properties, Map<String, String> environment) {
        this.properties = new Properties();
        this.properties.putAll(properties);
        this.storagePath = storagePath(this.properties);
        this.hadoopConfiguration = hadoopConfiguration(this.properties, storagePath, environment);
    }

    Properties properties() {
        return properties;
    }

    Configuration hadoopConfiguration() {
        return hadoopConfiguration;
    }

    String storagePath() {
        return storagePath;
    }

    boolean allowApproximateMatching() {
        return Boolean.parseBoolean(properties.getProperty("allowApproximateMatching", "true"));
    }

    private static Configuration hadoopConfiguration(Properties properties, String storagePath,
                                                       Map<String, String> environment) {
        Configuration configuration = new Configuration();
        properties.forEach((key, value) -> {
            String name = String.valueOf(key);
            if (name.startsWith(HADOOP_PREFIX)) {
                configuration.set(name.substring(HADOOP_PREFIX.length()), String.valueOf(value));
            }
        });
        if (storagePath.startsWith("s3a://")) {
            configuration.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem");
            configuration.set("fs.s3a.input.stream.type", "classic");
            configuration.set("fs.s3a.connection.timeout", "15000");
            configuration.set("fs.s3a.connection.request.timeout", "15000");
            copy(properties, configuration, "cloudStorageEndpoint", "fs.s3a.endpoint");
            copyFirst(properties, configuration, "fs.s3a.endpoint.region",
                    "compactionBucketRegion", "s3Region");
            copy(properties, configuration, "s3AccessKeyId", "fs.s3a.access.key");
            copy(properties, configuration, "s3SecretAccessKey", "fs.s3a.secret.key");
            copy(properties, configuration, "s3SessionToken", "fs.s3a.session.token");
            configureCredentialsProvider(properties, configuration);
            boolean pathStyle = Boolean.parseBoolean(properties.getProperty(
                    "s3PathStyleAccess", properties.containsKey("cloudStorageEndpoint") ? "true" : "false"));
            configuration.setBoolean("fs.s3a.path.style.access", pathStyle);
        } else if (storagePath.startsWith("gs://")) {
            configureGcs(properties, configuration);
        } else if (storagePath.startsWith("abfss://")) {
            configureAzure(storagePath, configuration, environment);
        }
        return configuration;
    }

    private static void configureGcs(Properties properties, Configuration configuration) {
        configuration.set("fs.gs.impl", GoogleHadoopFileSystem.class.getName());
        configuration.set("fs.gs.block.size", "67108864");
        configuration.set("fs.gs.outputstream.buffer.size", "8388608");
        configuration.set("fs.gs.inputstream.inplace.seek.limit", "8388608");
        configuration.set("fs.gs.inputstream.min.range.request.size", "2097152");
        copy(properties, configuration, "googleCloudProjectID", "fs.gs.project.id");

        String serviceAccountFile = properties.getProperty("googleCloudServiceAccountFile");
        if (hasText(serviceAccountFile)) {
            configuration.setBoolean("google.cloud.auth.service.account.enable", true);
            configuration.set("google.cloud.auth.service.account.json.keyfile", serviceAccountFile);
        }
        String endpoint = properties.getProperty("cloudStorageEndpoint");
        if (hasText(endpoint)) {
            configuration.setBoolean("fs.gs.auth.null.enable", true);
            configuration.setBoolean("fs.gs.auth.service.account.enable", false);
            configuration.set("fs.gs.storage.root.url", endpoint);
        }
    }

    private static void configureAzure(String storagePath, Configuration configuration,
                                       Map<String, String> environment) {
        String azureHost = azureHost(storagePath);
        configuration.set("fs.abfss.impl", AzureBlobFileSystem.class.getName());
        configuration.set("fs.azure.account.auth.type." + azureHost, "OAuth");
        configuration.set("fs.azure.account.oauth.provider.type." + azureHost,
                WorkloadIdentityTokenProvider.class.getName());
        copyEnvironment(environment, configuration, "AZURE_AUTHORITY_HOST",
                "fs.azure.account.oauth2.msi.authority." + azureHost);
        copyEnvironment(environment, configuration, "AZURE_CLIENT_ID",
                "fs.azure.account.oauth2.client.id." + azureHost);
        copyEnvironment(environment, configuration, "AZURE_TENANT_ID",
                "fs.azure.account.oauth2.msi.tenant." + azureHost);
        copyEnvironment(environment, configuration, "AZURE_FEDERATED_TOKEN_FILE",
                "fs.azure.account.oauth2.token.file." + azureHost);
    }

    private static void copyEnvironment(Map<String, String> environment, Configuration configuration,
                                        String source, String target) {
        String value = environment.get(source);
        if (hasText(value)) {
            configuration.set(target, value);
        }
    }

    private static void configureCredentialsProvider(Properties properties, Configuration configuration) {
        boolean hasAccessKey = hasText(properties.getProperty("s3AccessKeyId"));
        boolean hasSecretKey = hasText(properties.getProperty("s3SecretAccessKey"));
        boolean hasSessionToken = hasText(properties.getProperty("s3SessionToken"));
        if (hasAccessKey != hasSecretKey) {
            throw new IllegalArgumentException("Both s3AccessKeyId and s3SecretAccessKey must be configured");
        }
        if (hasSessionToken && !hasAccessKey) {
            throw new IllegalArgumentException("s3SessionToken requires access and secret keys");
        }
        if (hasSessionToken) {
            configuration.set("fs.s3a.aws.credentials.provider",
                    "org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider");
        } else if (hasAccessKey) {
            configuration.set("fs.s3a.aws.credentials.provider",
                    "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider");
        } else {
            configuration.set("fs.s3a.aws.credentials.provider",
                    "software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider,"
                            + "software.amazon.awssdk.auth.credentials.WebIdentityTokenFileCredentialsProvider");
        }
    }

    private static void copy(Properties properties, Configuration configuration, String source, String target) {
        String value = properties.getProperty(source);
        if (value != null && !value.isBlank()) {
            configuration.set(target, value);
        }
    }

    private static void copyFirst(Properties properties, Configuration configuration, String target,
                                  String... sources) {
        for (String source : sources) {
            String value = properties.getProperty(source);
            if (hasText(value)) {
                configuration.set(target, value);
                return;
            }
        }
    }

    private static boolean hasText(String value) {
        return value != null && !value.isBlank();
    }

    private static String storagePath(Properties properties) {
        String backend = normalizedBackend(properties);
        if ("LOCAL".equals(backend)) {
            return properties.getProperty("storagePath", DEFAULT_STORAGE_PATH);
        }
        String bucket = properties.getProperty("compactionBucket",
                properties.getProperty("s3CompactionBucket", DEFAULT_STORAGE_PATH)).strip();
        String prefix = properties.getProperty("compactionPrefix",
                properties.getProperty("s3CompactionPrefix", "")).strip();
        bucket = stripTrailingSlash(bucket);
        prefix = stripLeadingSlash(prefix);
        String path = bucket + (prefix.isEmpty() ? "" : "/" + prefix);
        if ("S3".equals(backend)) {
            if (path.startsWith("s3://")) {
                return "s3a://" + path.substring("s3://".length());
            }
            return path.contains("://") ? path : "s3a://" + path;
        }
        if ("GCS".equals(backend)) {
            return path.contains("://") ? path : "gs://" + path;
        }
        if ("AZUREDFS".equals(backend)) {
            return azureStoragePath(bucket, prefix);
        }
        if ("AZUREBLOB".equals(backend) || "AZURELOCAL".equals(backend)) {
            throw new IllegalArgumentException(
                    "Kafka lakehouse reader storage type " + backend
                            + " is not supported because Hadoop 3.5 removed the WASB connector; use AZUREDFS");
        }
        throw new IllegalArgumentException("Unsupported Kafka lakehouse reader storage type: " + backend);
    }

    private static String normalizedBackend(Properties properties) {
        return properties.getProperty("compactionBackendStorageType",
                        properties.getProperty("backendStorageType", "LOCAL"))
                .toUpperCase(Locale.ROOT)
                .replace("_", "");
    }

    private static String azureStoragePath(String bucket, String prefix) {
        if (bucket.contains("://")) {
            if (!bucket.startsWith("abfss://")) {
                throw new IllegalArgumentException(
                        "Kafka lakehouse reader AZUREDFS paths must use the abfss:// scheme: " + bucket);
            }
            return bucket + (prefix.isEmpty() ? "" : "/" + prefix);
        }
        String[] accountAndContainer = bucket.split("@", -1);
        if (accountAndContainer.length != 2
                || !hasText(accountAndContainer[0])
                || !hasText(accountAndContainer[1])) {
            throw new IllegalArgumentException("Invalid Azure storage path: " + bucket);
        }

        String account = accountAndContainer[0];
        String container = accountAndContainer[1];
        String host = account + ".dfs.core.windows.net";
        return "abfss://" + container + "@" + host + (prefix.isEmpty() ? "" : "/" + prefix);
    }

    private static String azureHost(String storagePath) {
        int at = storagePath.indexOf('@');
        if (at < 0) {
            throw new IllegalArgumentException("Invalid Azure storage path: " + storagePath);
        }
        int slash = storagePath.indexOf('/', at);
        return slash < 0 ? storagePath.substring(at + 1) : storagePath.substring(at + 1, slash);
    }

    private static String stripLeadingSlash(String value) {
        int start = 0;
        while (start < value.length() && value.charAt(start) == '/') {
            start++;
        }
        return value.substring(start);
    }

    private static String stripTrailingSlash(String value) {
        int end = value.length();
        while (end > 0 && value.charAt(end - 1) == '/') {
            end--;
        }
        return value.substring(0, end);
    }
}
