/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.compact;

import io.lakestream.ursa.storage.impl.StorageConfig;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class CompactionMain {

    private static final String ICEBERG_CATALOG_PROPERTY_PREFIX = "iceberg.catalog.";
    private static final String CREDENTIAL_PROPERTY_SUFFIX = ".credential";

    public static void main(String[] args) {
        int retCode = doMain(args);
        Runtime.getRuntime().exit(retCode);
    }
    static int doMain(String[] args) {
        StorageConfig config;
        try {
            config = parseArgs(args);
            if (config == null) {
                throw new IllegalArgumentException("Missing configuration file");
            }
        } catch (IllegalArgumentException e) {
            log.error("Failed to parse args", e);
            if (e.getMessage() != null) {
                System.err.println(e.getMessage());
            }
            printUsage();
            return ExitCode.INVALID_CONF;
        }

        try {
            CompactionScheduler compactionScheduler = new CompactionScheduler(config);
            compactionScheduler.start();

            CompletableFuture<Object> future = new CompletableFuture<>();
            new Thread(() -> {
                // register shutdown hook to aggregate stats
                Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                    try {
                        log.info("Shutting down compaction scheduler");
                        compactionScheduler.close();
                        future.complete(null);
                    } catch (Exception e) {
                        log.error("Failed to shutdown compaction scheduler", e);
                        future.completeExceptionally(e);
                    }
                }));
            }, "shutdown-hook").start();
            future.get();
        } catch (Exception e) {
            log.error("Failed to start compaction scheduler", e);
            return ExitCode.SERVER_EXCEPTION;
        }
        return ExitCode.OK;
    }


    private static void printUsage() {
        System.err.println("Usage: CompactionMain -c <configuration-file>");
    }

    private static StorageConfig parseArgs(String[] args)
            throws IllegalArgumentException {
        try {
            if (args.length == 1 && ("-h".equals(args[0]) || "--help".equals(args[0]))) {
                throw new IllegalArgumentException();
            }
            if (args.length == 2 && ("-c".equals(args[0]) || "--conf".equals(args[0]))) {
                return getConfiguration(args[1]);
            }
            if (args.length == 1 && args[0].startsWith("--conf=")) {
                return getConfiguration(args[0].substring("--conf=".length()));
            }
            if (args.length == 0) {
                return null;
            }
            throw new IllegalArgumentException("unexpected arguments [" + String.join(" ", args) + "]");
        } catch (IOException e) {
            throw new IllegalArgumentException(e);
        }
    }

    private static StorageConfig getConfiguration(String confFile)
            throws IOException {
        Properties properties = new Properties();
        StorageConfig configuration;
        try (FileInputStream fis = new FileInputStream(confFile)) {
            properties.load(fis);
            configuration = StorageConfig.fromProperties(properties);
            printConfiguration(properties, confFile);
        }

        return configuration;
    }

    public static StorageConfig printConfiguration(Properties properties, String confFile) throws IOException {
        Properties safeProperties = new Properties();
        safeProperties.putAll(properties);
        for (String credentialKey : StorageConfig.CREDENTIAL_KEYS) {
            safeProperties.computeIfPresent(credentialKey, (k, v) -> "******");
        }
        safeProperties.entrySet().removeIf(entry -> isIcebergCatalogCredential(entry.getKey().toString()));

        StorageConfig safeConfiguration = StorageConfig.fromProperties(safeProperties);
        log.info("Load configuration from file {}: {}", confFile, safeConfiguration);
        return safeConfiguration;
    }

    private static boolean isIcebergCatalogCredential(String propertyName) {
        return propertyName.startsWith(ICEBERG_CATALOG_PROPERTY_PREFIX)
                && propertyName.endsWith(CREDENTIAL_PROPERTY_SUFFIX)
                && propertyName.length()
                > ICEBERG_CATALOG_PROPERTY_PREFIX.length() + CREDENTIAL_PROPERTY_SUFFIX.length();
    }
}
