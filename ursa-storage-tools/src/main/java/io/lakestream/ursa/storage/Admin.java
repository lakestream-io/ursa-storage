/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.storage;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import io.lakestream.ursa.storage.impl.StorageConfig;
import io.opentelemetry.sdk.autoconfigure.AutoConfiguredOpenTelemetrySdk;
import java.io.FileInputStream;
import java.util.List;
import java.util.Properties;
import java.util.Set;

public class Admin {

    static final class AdminArguments {
        @Parameter(
            names = {"-h", "--help"},
            description = "Help message",
            help = true)
        boolean help;
    }

    private static final AdminArguments arguments = new AdminArguments();

    public static void main(String[] args) {
        JCommander jc = new JCommander(arguments);
        jc.setProgramName("ursa admin");

        Read readCommand = new Read();
        Delete deleteCommand = new Delete();
        ListStream listCommand = new ListStream();
        SetDeletePrefixes setDeletePrefixes = new SetDeletePrefixes();
        jc.addCommand("read", readCommand);
        jc.addCommand("delete", deleteCommand);
        jc.addCommand("list", listCommand);
        jc.addCommand("set-delete-prefixes", setDeletePrefixes);

        try {
            jc.parse(args);
        } catch (ParameterException e) {
            System.out.println(e.getMessage());
            jc.usage();
            System.exit(1);
        }

        if (arguments.help) {
            jc.usage();
            System.exit(1);
        }

        String command = jc.getParsedCommand();
        try {
            if ("read".equals(command)) {
                readCommand.run();
            } else if ("delete".equals(command)) {
                deleteCommand.run();
            } else if ("list".equals(command)) {
                listCommand.run();
            } else if ("set-delete-prefixes".equals(command)) {
                setDeletePrefixes.run();
            } else {
                System.out.println("Unknown command: " + command);
                jc.usage();
                System.exit(1);
            }
        } catch (Exception e) {
            System.err.println("Error executing command: " + e.getMessage());
            e.printStackTrace();
            System.exit(1);
        }
    }

    private static UrsaStorage ursaStorage;
    private static StorageConfig config;

    private static StorageApi initializeStorage(String configFile) throws Exception {
        // Load config
        Properties properties = new Properties();
        try (FileInputStream fis = new FileInputStream(configFile)) {
            properties.load(fis);
        }
        config = StorageConfig.fromProperties(properties);

        // Initialize storage components
        AutoConfiguredOpenTelemetrySdk sdk = AutoConfiguredOpenTelemetrySdk.builder().build();
        ursaStorage = new UrsaStorage(properties, sdk.getOpenTelemetrySdk());
        return ursaStorage.getDefaultStorageApi();
    }

    private static void cleanup() throws Exception {
        if (ursaStorage != null) {
            try {
                ursaStorage.close();
            } catch (Exception e) {
                // ignore
            }
        }
    }

    static class Read {
        @Parameter(
            names = {"-s", "--stream-id"},
            description = "Stream ID to read from",
            required = true)
        long streamId;

        @Parameter(
            names = {"-o", "--offset"},
            description = "Starting offset to read from",
            required = true)
        long offset;

        @Parameter(
            names = {"-n", "--num-entries"},
            description = "Maximum number of entries to read",
            required = true)
        int numEntries;

        @Parameter(
            names = {"-ms", "--max-size"},
            description = "Maximum size in bytes to read",
            required = false)
        int maxSize = 1024 * 1024; // 1MB default

        @Parameter(
            names = {"-c", "--config"},
            description = "Configuration file path",
            required = true)
        String configFile;


        public void run() throws Exception {
            StorageApi storageApi = initializeStorage(configFile);

            try {
                // Read entries
                List<Entry> entries = storageApi.readEntries(streamId, offset, numEntries, maxSize).get();

                // Print entries
                System.out.printf("Read %d entries from stream %d starting at offset %d:%n",
                    entries.size(), streamId, offset);
                for (Entry entry : entries) {
                    System.out.printf("Entry[offset=%d, messages=%d, size=%d bytes]%n",
                        entry.header().offset(),
                        entry.header().numberOfMessages(),
                        entry.header().entrySize());
                    entry.payload().release();
                }
            } finally {
                cleanup();
                storageApi.close();
            }
        }
    }

    static class Delete {
        @Parameter(
            names = {"-s", "--stream-id"},
            description = "Stream ID to delete",
            required = true)
        long streamId;

        @Parameter(
            names = {"-c", "--config"},
            description = "Configuration file path",
            required = true)
        String configFile;

        public void run() throws Exception {
            StorageApi storageApi = initializeStorage(configFile);

            try {
                // Delete stream
                storageApi.deleteStream(streamId).get();
                System.out.printf("Successfully deleted stream %d%n", streamId);
            } finally {
                cleanup();
                storageApi.close();
            }
        }
    }

    static class ListStream {
        @Parameter(
            names = {"-c", "--config"},
            description = "Configuration file path",
            required = true)
        String configFile;

        public void run() throws Exception {
            StorageApi storageApi = initializeStorage(configFile);
            try {
                // List all streams
                Set<Long> streams = storageApi.listStreams().get();
                if (streams.isEmpty()) {
                    System.out.println("No streams found");
                } else {
                    System.out.println("Available streams: " + streams);
                }
            } finally {
                cleanup();
                storageApi.close();
            }
        }
    }

    static class SetDeletePrefixes {
        @Parameter(
            names = {"-p", "--prefixes"},
            description = "Storage path prefixes to delete, use comma to separate multiple prefixes",
            required = true)
        Set<String> prefixes;

        @Parameter(
            names = {"-c", "--config"},
            description = "Configuration file path",
            required = true)
        String configFile;

        public void run() throws Exception {
            initializeStorage(configFile);
            try {
                ursaStorage.getFileStorage().deleteWithDatePrefixes(prefixes).get();
                System.out.printf("Successfully set delete policy for the bucket %s with prefixes %s%n",
                    config.getBucket(), prefixes);
            } finally {
                cleanup();
            }
        }
    }
}
