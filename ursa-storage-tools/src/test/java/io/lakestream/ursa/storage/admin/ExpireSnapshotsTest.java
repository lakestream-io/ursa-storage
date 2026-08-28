/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.storage.admin;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.lakestream.ursa.lakehouse.iceberg.IcebergTable;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.Map;
import java.util.Properties;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import picocli.CommandLine;

public class ExpireSnapshotsTest {

    private static final String CATALOG_NAME = "testcatalog";

    private Path tempDir;
    private Catalog catalog;

    @BeforeEach
    void setUp() throws Exception {
        tempDir = Files.createTempDirectory("expire-snapshots-test");
        // Build the catalog using the same path that the command will use via LakehouseConfiguration
        catalog = CatalogUtil.buildIcebergCatalog(CATALOG_NAME,
                Map.of("type", "hadoop", "warehouse", tempDir.toString()),
                new org.apache.hadoop.conf.Configuration());
    }

    @AfterEach
    void tearDown() throws Exception {
        if (catalog instanceof java.io.Closeable closeable) {
            closeable.close();
        }
        if (tempDir != null) {
            Files.walk(tempDir)
                    .sorted(Comparator.reverseOrder())
                    .map(Path::toFile)
                    .forEach(File::delete);
        }
    }

    // --- parseDuration unit tests ---

    @Test
    void testParseDurationDays() {
        assertEquals(7 * 86400_000L, ExpireSnapshots.parseDuration("7d"));
    }

    @Test
    void testParseDurationHours() {
        assertEquals(24 * 3600_000L, ExpireSnapshots.parseDuration("24h"));
    }

    @Test
    void testParseDurationMinutes() {
        assertEquals(30 * 60_000L, ExpireSnapshots.parseDuration("30m"));
    }

    @Test
    void testParseDurationSeconds() {
        assertEquals(3600 * 1000L, ExpireSnapshots.parseDuration("3600s"));
    }

    @Test
    void testParseDurationUpperCase() {
        assertEquals(7 * 86400_000L, ExpireSnapshots.parseDuration("7D"));
    }

    @Test
    void testParseDurationInvalid() {
        assertThrows(IllegalArgumentException.class, () -> ExpireSnapshots.parseDuration("abc"));
        assertThrows(IllegalArgumentException.class, () -> ExpireSnapshots.parseDuration("7x"));
        assertThrows(IllegalArgumentException.class, () -> ExpireSnapshots.parseDuration(""));
    }

    // --- Command integration tests ---

    @Test
    void testDryRunListsSnapshotsWithoutExpiring() throws Exception {
        createTableWithSnapshots("test_ns/dry-run-topic", 3);

        File configFile = writeConfigFile();
        CapturedOutput captured = runCommand(configFile,
                "-t", "test_ns/dry-run-topic",
                "--catalog-name", CATALOG_NAME, "--dry-run");

        assertEquals(0, captured.exitCode, "stderr: " + captured.stderr);
        assertTrue(captured.stdout.contains("Current state: 3 snapshots"), "stdout: " + captured.stdout);
        assertTrue(captured.stdout.contains("[DRY RUN]"), "stdout: " + captured.stdout);

        // Verify no snapshots were actually expired
        TableIdentifier tableId = TableIdentifier.of(Namespace.of("test_ns"), "dry-run-topic");
        Table table = catalog.loadTable(tableId);
        table.refresh();
        assertEquals(3, countSnapshots(table));
    }

    @Test
    void testExpiresSnapshotsRetainLast() throws Exception {
        createTableWithSnapshots("test_ns/expire-topic", 5);

        File configFile = writeConfigFile();
        CapturedOutput captured = runCommand(configFile,
                "-t", "test_ns/expire-topic", "--retain-last", "2",
                "--catalog-name", CATALOG_NAME);

        assertEquals(0, captured.exitCode, "stderr: " + captured.stderr);
        assertTrue(captured.stdout.contains("Current state: 5 snapshots"), "stdout: " + captured.stdout);
        assertTrue(captured.stdout.contains("After expiration:"), "stdout: " + captured.stdout);

        TableIdentifier tableId = TableIdentifier.of(Namespace.of("test_ns"), "expire-topic");
        Table table = catalog.loadTable(tableId);
        table.refresh();
        assertEquals(2, countSnapshots(table));
    }

    @Test
    void testNoSnapshotsTable() throws Exception {
        // Create an empty table with no snapshots
        TableIdentifier tableId = TableIdentifier.of(Namespace.of("test_ns"), "empty-topic");
        Schema schema = new Schema(Types.NestedField.required(1, "id", Types.IntegerType.get()));
        catalog.createTable(tableId, schema, PartitionSpec.unpartitioned());

        File configFile = writeConfigFile();
        CapturedOutput captured = runCommand(configFile,
                "-t", "test_ns/empty-topic",  "--catalog-name", CATALOG_NAME);

        assertEquals(0, captured.exitCode, "stderr: " + captured.stderr);
        assertTrue(captured.stdout.contains("No snapshots found"), "stdout: " + captured.stdout);
    }

    @Test
    void testNonExistentStreamReturnsError() throws Exception {
        File configFile = writeConfigFile();
        CapturedOutput captured = runCommand(configFile,
                "-t", "no_ns/no-stream",  "--catalog-name", CATALOG_NAME);

        assertEquals(1, captured.exitCode);
        assertTrue(captured.stderr.contains("Error expiring snapshots"), "stderr: " + captured.stderr);
    }

    @Test
    void testExplicitCatalogName() throws Exception {
        createTableWithSnapshots("test_ns/catalog-topic", 3);

        File configFile = writeConfigFile();
        CapturedOutput captured = runCommand(configFile,
                "-t", "test_ns/catalog-topic",
                "--catalog-name", CATALOG_NAME, "--dry-run");

        assertEquals(0, captured.exitCode, "stderr: " + captured.stderr);
        assertTrue(captured.stdout.contains("Using catalog: " + CATALOG_NAME), "stdout: " + captured.stdout);
        assertTrue(captured.stdout.contains("Current state: 3 snapshots"), "stdout: " + captured.stdout);
    }

    @Test
    void testExplicitCatalogNameWithExpiration() throws Exception {
        createTableWithSnapshots("test_ns/catalog-expire-topic", 4);

        File configFile = writeConfigFile();
        CapturedOutput captured = runCommand(configFile,
                "-t", "test_ns/catalog-expire-topic",
                "--catalog-name", CATALOG_NAME, "--retain-last", "2");

        assertEquals(0, captured.exitCode, "stderr: " + captured.stderr);
        assertTrue(captured.stdout.contains("Using catalog: " + CATALOG_NAME), "stdout: " + captured.stdout);
        assertTrue(captured.stdout.contains("After expiration:"), "stdout: " + captured.stdout);

        TableIdentifier tableId = TableIdentifier.of(
                Namespace.of("test_ns"), "catalog-expire-topic");
        Table table = catalog.loadTable(tableId);
        table.refresh();
        assertEquals(2, countSnapshots(table));
    }

    // --- Helpers ---

    private void createTableWithSnapshots(String stream, int snapshotCount) throws Exception {
        // Derive table identifier the same way the command does
        TableIdentifier tableId = IcebergTable.getTableIdentifierByTopic(stream);

        Schema schema = new Schema(Types.NestedField.required(1, "id", Types.IntegerType.get()));
        Table table = catalog.createTable(tableId, schema, PartitionSpec.unpartitioned());

        for (int i = 0; i < snapshotCount; i++) {
            Path dataPath = tempDir.resolve("data-" + tableId.name() + "-" + i + ".parquet");
            Files.createFile(dataPath);
            DataFile dataFile = DataFiles.builder(table.spec())
                    .withPath(dataPath.toString())
                    .withFileSizeInBytes(100)
                    .withFormat(FileFormat.PARQUET)
                    .withRecordCount(10)
                    .build();
            AppendFiles append = table.newAppend();
            append.appendFile(dataFile);
            append.commit();
            table.refresh();
        }
    }

    private File writeConfigFile() throws Exception {
        Properties props = new Properties();
        props.setProperty("backendStorageType", "local");
        props.setProperty("storagePath", tempDir + "/storage/");
        props.setProperty("catalog.default", CATALOG_NAME);
        props.setProperty("iceberg.catalog." + CATALOG_NAME + ".type", "hadoop");
        props.setProperty("iceberg.catalog." + CATALOG_NAME + ".warehouse", tempDir.toString());
        File configFile = new File(tempDir.toFile(), "test-config.properties");
        try (FileOutputStream fos = new FileOutputStream(configFile)) {
            props.store(fos, "test config");
        }
        return configFile;
    }

    private static CapturedOutput runCommand(File configFile, String... extraArgs) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        ByteArrayOutputStream err = new ByteArrayOutputStream();
        PrintStream originalOut = System.out;
        PrintStream originalErr = System.err;
        System.setOut(new PrintStream(out));
        System.setErr(new PrintStream(err));
        int exitCode;
        try {
            String[] baseArgs = {"-c", configFile.getAbsolutePath(), "table", "expire-snapshots"};
            String[] allArgs = new String[baseArgs.length + extraArgs.length];
            System.arraycopy(baseArgs, 0, allArgs, 0, baseArgs.length);
            System.arraycopy(extraArgs, 0, allArgs, baseArgs.length, extraArgs.length);
            exitCode = new CommandLine(new Admin()).execute(allArgs);
        } finally {
            System.setOut(originalOut);
            System.setErr(originalErr);
        }
        return new CapturedOutput(exitCode, out.toString(), err.toString());
    }

    private static int countSnapshots(Table table) {
        int count = 0;
        for (Snapshot ignored : table.snapshots()) {
            count++;
        }
        return count;
    }

    private record CapturedOutput(int exitCode, String stdout, String stderr) { }
}
