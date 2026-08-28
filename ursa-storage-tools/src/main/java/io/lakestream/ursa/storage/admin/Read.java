/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.storage.admin;

import io.lakestream.ursa.storage.Entry;
import io.lakestream.ursa.storage.StorageApi;
import java.util.List;
import java.util.concurrent.Callable;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

/**
 * Command to read entries from a stream.
 */
@Command(name = "read", description = "Read entries from a stream")
public class Read implements Callable<Integer> {

    @Option(names = {"-s", "--stream-id"}, description = "Stream ID to read from", required = true)
    private long streamId;

    @Option(names = {"-o", "--offset"}, description = "Starting offset to read from", required = true)
    private long offset;

    @Option(names = {"-n", "--num-entries"}, description = "Maximum number of entries to read", required = true)
    private int numEntries;

    @Option(names = {"-ms", "--max-size"}, description = "Maximum size in bytes to read", defaultValue = "1048576")
    private int maxSize;

    @CommandLine.ParentCommand
    private Admin parent;

    @Override
    public Integer call() throws Exception {
        StorageApi storageApi = Admin.initializeStorage(parent.getConfigFile());

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
            return 0;
        } catch (Exception e) {
            System.err.println("Error reading entries: " + e.getMessage());
            return 1;
        } finally {
            Admin.cleanup();
            storageApi.close();
        }
    }
}
