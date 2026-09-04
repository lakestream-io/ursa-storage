/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.storage.admin;

import io.lakestream.ursa.lakehouse.LakehouseConfiguration;
import io.lakestream.ursa.lakehouse.iceberg.IcebergTable;
import java.io.FileInputStream;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

/** Command to expire snapshots of an Iceberg table associated with a stream. */
@Command(name = "expire-snapshots", description = "Expire snapshots of a stream's Iceberg table")
public class ExpireSnapshots implements Callable<Integer> {

    private static final DateTimeFormatter TIMESTAMP_FORMATTER =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss z").withZone(ZoneId.of("UTC"));

    private static final Pattern DURATION_PATTERN = Pattern.compile("(\\d+)([dhms])");

    @CommandLine.ParentCommand
    private io.lakestream.ursa.storage.admin.Table parent;

    @Option(
        names = {"-t", "--topic"},
        description = "Stream name",
        required = true
    )
    private String topic;

    @Option(
        names = {"--older-than"},
        description = "Expire snapshots older than this duration (e.g. 7d, 24h, 30m). "
                + "If not set, expires all snapshots except the ones retained by --retain-last."
    )
    private String olderThan;

    @Option(
        names = {"--retain-last"},
        description = "Number of most recent snapshots to retain (default: 1)",
        defaultValue = "1"
    )
    private int retainLast;

    @Option(
        names = {"--dry-run"},
        description = "List snapshots that would be expired without actually expiring them"
    )
    private boolean dryRun;

    @Option(
        names = {"--catalog-name"},
        description = "Iceberg catalog name. Required: it must be specified explicitly so the "
                + "operation always targets the intended catalog.",
        required = true
    )
    private String catalogName;

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
            // tasks, so the operation always targets exactly the catalog the operator named.
            properties.setProperty(LakehouseConfiguration.CATALOG_NAME, catalogName);
            System.out.println("Using catalog: " + catalogName);

            LakehouseConfiguration configuration = new LakehouseConfiguration(properties);

            TableIdentifier identifier = IcebergTable.getTableIdentifierByTopic(topic, configuration);
            System.out.println("Table identifier: " + identifier);

            icebergTable = new IcebergTable(configuration, identifier);
            icebergTable.loadTable();
            Table table = icebergTable.getTable();

            int snapshotCount = countSnapshots(table);
            long metadataSize = IcebergTable.getLatestMetadataSize(table);
            System.out.printf("Current state: %d snapshots, metadata size: %d bytes%n",
                    snapshotCount, metadataSize);

            if (snapshotCount == 0) {
                System.out.println("No snapshots found. Nothing to expire.");
                return 0;
            }

            printSnapshots(table);

            if (dryRun) {
                System.out.println("\n[DRY RUN] No snapshots were expired.");
                return 0;
            }

            System.out.println("\nExpiring snapshots...");
            org.apache.iceberg.ExpireSnapshots expire = table.expireSnapshots()
                    .cleanExpiredMetadata(true)
                    .retainLast(retainLast);

            if (olderThan != null) {
                expire.expireOlderThan(System.currentTimeMillis() - parseDuration(olderThan));
            } else {
                expire.expireOlderThan(System.currentTimeMillis());
            }

            expire.commit();
            System.out.printf("Successfully expired snapshots%n");

            table.refresh();
            int newSnapshotCount = countSnapshots(table);
            long newMetadataSize = IcebergTable.getLatestMetadataSize(table);
            System.out.printf("After expiration: %d snapshots, metadata size: %d bytes%n",
                    newSnapshotCount, newMetadataSize);
            System.out.printf("Expired %d snapshots. Metadata size reduced by %d bytes.%n",
                    snapshotCount - newSnapshotCount, metadataSize - newMetadataSize);

            return 0;
        } catch (Exception e) {
            System.err.println("Error expiring snapshots: " + e.getMessage());
            e.printStackTrace();
            return 1;
        } finally {
            if (icebergTable != null) {
                icebergTable.close();
            }
        }
    }

    private void printSnapshots(Table table) {
        System.out.println("\nSnapshots:");
        System.out.printf("  %-20s %-28s %-12s %s%n", "Snapshot ID", "Timestamp", "Operation", "Summary");
        System.out.println("  " + "-".repeat(90));
        for (Snapshot snapshot : table.snapshots()) {
            String timestamp = TIMESTAMP_FORMATTER.format(Instant.ofEpochMilli(snapshot.timestampMillis()));
            String operation = snapshot.operation() != null ? snapshot.operation() : "N/A";
            String summary = snapshot.summary() != null ? snapshot.summary().toString() : "";
            System.out.printf("  %-20d %-28s %-12s %s%n",
                    snapshot.snapshotId(), timestamp, operation, summary);
        }
    }

    private static int countSnapshots(Table table) {
        int count = 0;
        for (Snapshot ignored : table.snapshots()) {
            count++;
        }
        return count;
    }

    static long parseDuration(String duration) {
        Matcher matcher = DURATION_PATTERN.matcher(duration.toLowerCase(java.util.Locale.ROOT));
        if (!matcher.matches()) {
            throw new IllegalArgumentException(
                    "Invalid duration format: '" + duration + "'. Expected format: <number><unit> "
                            + "where unit is d (days), h (hours), m (minutes), or s (seconds). "
                            + "Examples: 7d, 24h, 30m, 3600s");
        }
        long value = Long.parseLong(matcher.group(1));
        String unit = matcher.group(2);
        return switch (unit) {
            case "d" -> value * 86400_000L;
            case "h" -> value * 3600_000L;
            case "m" -> value * 60_000L;
            case "s" -> value * 1000L;
            default -> throw new IllegalArgumentException("Unknown time unit: " + unit);
        };
    }
}
