/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.storage.admin;

import io.lakestream.ursa.storage.StorageApi;
import java.util.concurrent.Callable;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

/**
 * Command to delete a stream.
 */
@Command(name = "delete", description = "Delete a stream")
public class Delete implements Callable<Integer> {

    @Option(names = {"-s", "--stream-id"}, description = "Stream ID to delete", required = true)
    private long streamId;

    @CommandLine.ParentCommand
    private Admin parent;

    @Override
    public Integer call() throws Exception {
        StorageApi storageApi = Admin.initializeStorage(parent.getConfigFile());

        try {
            // Delete stream
            storageApi.deleteStream(streamId).get();
            System.out.printf("Successfully deleted stream %d%n", streamId);
            return 0;
        } catch (Exception e) {
            System.err.println("Error deleting stream: " + e.getMessage());
            return 1;
        } finally {
            Admin.cleanup();
            storageApi.close();
        }
    }
}
