/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.storage.admin;

import io.lakestream.ursa.storage.StorageApi;
import io.lakestream.ursa.storage.UrsaStorage;
import io.lakestream.ursa.storage.impl.StorageConfig;
import io.opentelemetry.sdk.autoconfigure.AutoConfiguredOpenTelemetrySdk;
import java.io.FileInputStream;
import java.util.Properties;
import java.util.concurrent.Callable;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

/**
 * Main Admin class for Ursa storage operations.
 */
@Command(
    name = "ursa-admin",
    description = "Ursa storage administration tool",
    subcommands = {
        Read.class,
        Delete.class,
        ListStream.class,
        SetDeletePrefixes.class,
        GetCompactTasks.class,
        GetFirstNCompactTask.class,
        CheckLock.class,
        Table.class,
        ManuallyCommitTasks.class,
        MigrateStreamID.class,
        UpdatePublishTaskOffset.class
    }
)
public class Admin implements Callable<Integer> {

    @Option(names = {"-c", "--config"}, description = "Configuration file path", scope = CommandLine.ScopeType.INHERIT)
    private String configFile;

    @Option(names = {"-h", "--help"}, usageHelp = true, description = "Display this help message")
    private boolean helpRequested = false;

    private static UrsaStorage ursaStorage;
    private static StorageConfig config;

    /**
     * Main entry point for the admin CLI.
     *
     * @param args Command line arguments
     */
    public static void main(String[] args) {
        int exitCode = new CommandLine(new Admin()).execute(args);
        System.exit(exitCode);
    }

    @Override
    public Integer call() {
        CommandLine.usage(this, System.out);
        return 0;
    }

    /**
     * Initializes the storage components.
     *
     * @param configFile Path to the configuration file
     * @return Initialized StorageApi instance
     * @throws Exception If initialization fails
     */
    protected static StorageApi initializeStorage(String configFile) throws Exception {
        Properties properties = new Properties();
        try (FileInputStream fis = new FileInputStream(configFile)) {
            properties.load(fis);
        }
        config = StorageConfig.fromProperties(properties);

        AutoConfiguredOpenTelemetrySdk sdk = AutoConfiguredOpenTelemetrySdk.builder().build();
        ursaStorage = new UrsaStorage(properties, sdk.getOpenTelemetrySdk());
        return ursaStorage.getDefaultStorageApi();
    }

    protected static StorageConfig  getStorageConfig(String configFile) throws Exception {
        Properties properties = new Properties();
        try (FileInputStream fis = new FileInputStream(configFile)) {
            properties.load(fis);
        }
        return StorageConfig.fromProperties(properties);
    }

    /**
     * Cleans up resources.
     */
    protected static void cleanup() {
        if (ursaStorage != null) {
            try {
                ursaStorage.close();
            } catch (Exception e) {
                // ignore
            }
        }
    }

    /**
     * Gets the UrsaStorage instance.
     */
    protected static UrsaStorage getUrsaStorage() {
        return ursaStorage;
    }

    /**
     * Gets the configuration instance.
     */
    protected static StorageConfig getConfig() {
        return config;
    }

    /**
     * Gets the configured config file path.
     */
    public String getConfigFile() {
        return configFile;
    }
}
