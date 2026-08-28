/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.storage.admin;

import java.util.Set;
import java.util.concurrent.Callable;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

/**
 * Command to set delete prefixes for storage cleanup.
 */
@Command(name = "set-delete-prefixes", description = "Set storage path prefixes for deletion policy")
public class SetDeletePrefixes implements Callable<Integer> {

    @Option(
        names = {"-p", "--prefixes"},
        description = "Storage path prefixes to delete, use comma to separate multiple prefixes",
        required = true,
        split = ","
    )
    private Set<String> prefixes;

    @CommandLine.ParentCommand
    private Admin parent;

    @Override
    public Integer call() throws Exception {
        Admin.initializeStorage(parent.getConfigFile());
        try {
            Admin.getUrsaStorage().getFileStorage().deleteWithDatePrefixes(prefixes).get();
            System.out.printf("Successfully set delete policy for the bucket %s with prefixes %s%n",
                Admin.getConfig().getBucket(), prefixes);
            return 0;
        } catch (Exception e) {
            System.err.println("Error setting delete prefixes: " + e.getMessage());
            return 1;
        } finally {
            Admin.cleanup();
        }
    }
}
