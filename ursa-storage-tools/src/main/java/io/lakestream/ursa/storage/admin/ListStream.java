/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.storage.admin;

import io.lakestream.ursa.storage.StorageApi;
import java.util.Set;
import java.util.concurrent.Callable;
import picocli.CommandLine;
import picocli.CommandLine.Command;

/**
 * Command to list all streams.
 */
@Command(name = "list", description = "List all streams")
public class ListStream implements Callable<Integer> {

    @CommandLine.ParentCommand
    private Admin parent;

    @Override
    public Integer call() throws Exception {
        StorageApi storageApi = Admin.initializeStorage(parent.getConfigFile());
        try {
            // List all streams
            Set<Long> streams = storageApi.listStreams().get();
            if (streams.isEmpty()) {
                System.out.println("No streams found");
            } else {
                System.out.println("Available streams: " + streams);
            }
            return 0;
        } catch (Exception e) {
            System.err.println("Error listing streams: " + e.getMessage());
            return 1;
        } finally {
            Admin.cleanup();
            storageApi.close();
        }
    }
}
