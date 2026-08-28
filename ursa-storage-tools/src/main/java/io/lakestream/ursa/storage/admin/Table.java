/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.storage.admin;

import java.util.concurrent.Callable;
import picocli.CommandLine;
import picocli.CommandLine.Command;

/**
 * Group command for table-related operations.
 */
@Command(
    name = "table",
    description = "Table related commands",
    subcommands = {
        ProbeTable.class,
        ExpireSnapshots.class,
        DedupFiles.class
    }
)
public class Table implements Callable<Integer> {

    @CommandLine.ParentCommand
    private Admin parent;

    @Override
    public Integer call() {
        CommandLine.usage(this, System.out);
        return 0;
    }

    public Admin getParent() {
        return parent;
    }
}
