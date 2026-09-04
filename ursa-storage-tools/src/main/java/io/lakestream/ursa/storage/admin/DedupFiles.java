/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.storage.admin;

import io.lakestream.ursa.lakehouse.LakehouseConfiguration;
import io.lakestream.ursa.lakehouse.iceberg.IcebergTable;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Callable;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFiles;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.Transaction;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.io.CloseableIterable;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

/** Command to detect and fix duplicate data-file references in a stream's Iceberg table. */
@Command(
    name = "dedup-files",
    description = "Detect and fix duplicate parquet data-file references in a stream's Iceberg table")
public class DedupFiles implements Callable<Integer> {

    @CommandLine.ParentCommand
    private io.lakestream.ursa.storage.admin.Table parent;

    @Option(
        names = {"-t", "--topic"},
        description = "Stream name",
        required = true)
    private String topic;

    @Option(
        names = {"--catalog-name"},
        description = "Iceberg catalog name. Required: it must be specified explicitly so the "
                + "repair always targets the intended catalog.",
        required = true)
    private String catalogName;

    @Option(
        names = {"--apply"},
        description = "Commit the fix. When absent, the command only detects and reports (dry-run).")
    private boolean apply;

    /** Returns true if the snapshot summary reports at least one delete file. */
    static boolean hasDeleteFiles(Map<String, String> summary) {
        if (summary == null) {
            return false;
        }
        String value = summary.get("total-delete-files");
        if (value == null) {
            return false;
        }
        try {
            return Long.parseLong(value.trim()) > 0;
        } catch (NumberFormatException e) {
            return false;
        }
    }

    /**
     * Scans the current snapshot of the given table and returns a map from file path to the list
     * of {@link DataFile} entries that share that path. Only paths appearing more than once are
     * included.
     */
    static Map<String, List<DataFile>> findDuplicateFilePaths(Table table) {
        Map<String, List<DataFile>> byPath = new LinkedHashMap<>();
        if (table.currentSnapshot() == null) {
            return Collections.emptyMap();
        }
        long snapshotId = table.currentSnapshot().snapshotId();
        try (CloseableIterable<FileScanTask> tasks =
                table.newScan().useSnapshot(snapshotId).planFiles()) {
            for (FileScanTask task : tasks) {
                DataFile file = task.file();
                String path = file.path().toString();
                byPath.computeIfAbsent(path, k -> new ArrayList<>()).add(file);
            }
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to scan data files for snapshot " + snapshotId, e);
        }
        Map<String, List<DataFile>> duplicates = new LinkedHashMap<>();
        for (Map.Entry<String, List<DataFile>> entry : byPath.entrySet()) {
            if (entry.getValue().size() > 1) {
                duplicates.put(entry.getKey(), entry.getValue());
            }
        }
        return duplicates;
    }

    /**
     * Groups all snapshots in the table by their {@code lakestream.tags} summary value and
     * returns only the groups that contain more than one snapshot (i.e. the same tag was committed
     * at least twice). This is the root-cause fingerprint of the double-commit bug.
     */
    static Map<String, List<Snapshot>> findDuplicateTags(Table table) {
        Map<String, List<Snapshot>> byTag = new LinkedHashMap<>();
        for (Snapshot snapshot : table.snapshots()) {
            Map<String, String> summary = snapshot.summary();
            if (summary == null) {
                continue;
            }
            String tags = summary.get("lakestream.tags");
            if (tags == null) {
                continue;
            }
            byTag.computeIfAbsent(tags, k -> new ArrayList<>()).add(snapshot);
        }
        Map<String, List<Snapshot>> duplicates = new LinkedHashMap<>();
        for (Map.Entry<String, List<Snapshot>> entry : byTag.entrySet()) {
            if (entry.getValue().size() > 1) {
                duplicates.put(entry.getKey(), entry.getValue());
            }
        }
        return duplicates;
    }

    /**
     * Removes duplicate data-file references so each path is referenced exactly once.
     * Runs a single transaction: delete every manifest entry for each duplicated path,
     * then re-append one DataFile per path. Two snapshots (one delete, one append) are
     * added to history. The {@code duplicates} map must reflect the current snapshot; on
     * retry after a failure, call {@code table.refresh()} and re-detect before re-invoking.
     */
    static void applyDedup(Table table, Map<String, List<DataFile>> duplicates) {
        if (duplicates.isEmpty()) {
            return;
        }
        table.refresh();
        Transaction txn = table.newTransaction();

        // 1) Delete by path: removes ALL manifest entries for each duplicated path.
        DeleteFiles delete = txn.newDelete();
        for (String path : duplicates.keySet()) {
            delete.deleteFile(path);
        }
        delete.commit();

        // 2) Re-append exactly one DataFile per path (preserving original metrics).
        AppendFiles append = txn.newAppend();
        for (List<DataFile> entries : duplicates.values()) {
            append.appendFile(entries.get(0));
        }
        append.commit();

        // Two snapshots are added to history: one delete, one append.
        txn.commitTransaction();
    }

    @Override
    public Integer call() {
        IcebergTable icebergTable = null;
        try {
            String configFile = parent.getParent().getConfigFile();
            Properties properties = new Properties();
            try (FileInputStream fis = new FileInputStream(configFile)) {
                properties.load(fis);
            }

            // The catalog name is required and used as-is; it is never auto-resolved from compact
            // tasks, so this destructive repair always targets exactly the catalog the operator named.
            properties.setProperty(LakehouseConfiguration.CATALOG_NAME, catalogName);
            System.out.println("Using catalog: " + catalogName);

            LakehouseConfiguration configuration = new LakehouseConfiguration(properties);
            TableIdentifier identifier = IcebergTable.getTableIdentifierByTopic(topic, configuration);
            System.out.println("Table identifier: " + identifier);

            icebergTable = new IcebergTable(configuration, identifier);
            icebergTable.loadTable();
            Table table = icebergTable.getTable();
            table.refresh();

            if (table.currentSnapshot() == null) {
                System.out.println("No snapshots found. Nothing to check.");
                return 0;
            }

            // Guard: refuse tables with delete files.
            if (hasDeleteFiles(table.currentSnapshot().summary())) {
                System.err.println("Table has delete files (position/equality). "
                        + "This repair targets append-only tables only. Aborting.");
                return 1;
            }

            // Diagnostic: duplicate lakestream.tags across snapshots.
            Map<String, List<Snapshot>> dupTags = findDuplicateTags(table);
            if (!dupTags.isEmpty()) {
                System.out.println("\nDuplicate lakestream.tags across snapshots (root-cause diagnostic):");
                for (Map.Entry<String, List<Snapshot>> entry : dupTags.entrySet()) {
                    System.out.println("  tag=" + entry.getKey());
                    for (Snapshot s : entry.getValue()) {
                        System.out.printf("    seq=%d snapshot-id=%d%n",
                                s.sequenceNumber(), s.snapshotId());
                    }
                }
            }

            // Primary: duplicate file paths in the current snapshot.
            Map<String, List<DataFile>> duplicates = findDuplicateFilePaths(table);
            if (duplicates.isEmpty()) {
                System.out.println("\nNo duplicate data-file paths found in the current snapshot. "
                        + "Nothing to fix.");
                return 0;
            }

            int totalExtraRefs = 0;
            System.out.println("\nDuplicate data-file paths in the current snapshot:");
            for (Map.Entry<String, List<DataFile>> entry : duplicates.entrySet()) {
                int occurrences = entry.getValue().size();
                totalExtraRefs += occurrences - 1;
                System.out.printf("  %s (occurrences=%d)%n", entry.getKey(), occurrences);
            }

            if (!apply) {
                System.out.printf("%n[DRY RUN] Would remove %d duplicate reference(s) across %d path(s). "
                        + "Re-run with --apply to commit the fix.%n", totalExtraRefs, duplicates.size());
                return 0;
            }

            System.out.println("\nApplying fix...");
            applyDedup(table, duplicates);
            table.refresh();

            Map<String, List<DataFile>> remaining = findDuplicateFilePaths(table);
            if (!remaining.isEmpty()) {
                System.err.printf("Fix did not remove all duplicates: %d path(s) still duplicated.%n",
                        remaining.size());
                return 1;
            }
            System.out.printf("Fix complete. Removed %d duplicate reference(s) across %d path(s). "
                    + "0 duplicates remain.%n", totalExtraRefs, duplicates.size());
            return 0;
        } catch (Exception e) {
            System.err.println("Error deduplicating files: " + e.getMessage());
            e.printStackTrace();
            return 1;
        } finally {
            if (icebergTable != null) {
                icebergTable.close();
            }
        }
    }
}
