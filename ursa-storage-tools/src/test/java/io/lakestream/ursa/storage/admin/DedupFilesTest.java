/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.storage.admin;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.lakestream.ursa.lakehouse.LakehouseConfiguration;
import io.lakestream.ursa.lakehouse.iceberg.IcebergTable;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileMetadata;
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

public class DedupFilesTest {

    private static final String CATALOG_NAME = "testcatalog";

    private Path tempDir;
    private Catalog catalog;

    @BeforeEach
    void setUp() throws Exception {
        tempDir = Files.createTempDirectory("dedup-files-test");
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

    /** Create a table and append the given file paths, one append (= one snapshot) per path. */
    private Table createTableAppending(String stream, String... dataFilePaths) throws Exception {
        TableIdentifier tableId = IcebergTable.getTableIdentifierByTopic(
                stream, new LakehouseConfiguration(new Properties()));
        Schema schema = new Schema(Types.NestedField.required(1, "id", Types.IntegerType.get()));
        Table table = catalog.createTable(tableId, schema, PartitionSpec.unpartitioned());
        for (String relativePath : dataFilePaths) {
            Path dataPath = tempDir.resolve(relativePath);
            if (!Files.exists(dataPath)) {
                Files.createFile(dataPath);
            }
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
        return table;
    }

    @Test
    void testHasDeleteFiles() {
        assertTrue(DedupFiles.hasDeleteFiles(Map.of("total-delete-files", "2")));
        assertFalse(DedupFiles.hasDeleteFiles(Map.of("total-delete-files", "0")));
        assertFalse(DedupFiles.hasDeleteFiles(Map.of()));
        assertFalse(DedupFiles.hasDeleteFiles(Map.of("other", "1")));
        assertFalse(DedupFiles.hasDeleteFiles(null));
    }

    @Test
    void testFindDuplicateFilePaths() throws Exception {
        // Append the SAME file path twice (two snapshots) plus a distinct file.
        Table table = createTableAppending("ns/dup-detect",
                "dup.parquet", "dup.parquet", "unique.parquet");

        Map<String, List<DataFile>> dups = DedupFiles.findDuplicateFilePaths(table);

        assertEquals(1, dups.size());
        String dupPath = dups.keySet().iterator().next();
        assertTrue(dupPath.endsWith("dup.parquet"));
        assertEquals(2, dups.get(dupPath).size());
    }

    @Test
    void testFindDuplicateTags() throws Exception {
        Table table = createTableAppending("ns/dup-tags");

        // Two snapshots carrying the SAME lakestream.tags value, one distinct.
        appendWithTag(table, "a.parquet", "{\"stream-p0\":\"1:5\"}");
        appendWithTag(table, "b.parquet", "{\"stream-p0\":\"1:5\"}");
        appendWithTag(table, "c.parquet", "{\"stream-p0\":\"2:9\"}");

        Map<String, List<Snapshot>> dupTags = DedupFiles.findDuplicateTags(table);

        assertEquals(1, dupTags.size());
        assertEquals(2, dupTags.get("{\"stream-p0\":\"1:5\"}").size());
    }

    @Test
    void testApplyDedupRemovesDuplicateReference() throws Exception {
        Table table = createTableAppending("ns/dup-fix",
                "dup.parquet", "dup.parquet", "unique.parquet");

        Map<String, List<DataFile>> before = DedupFiles.findDuplicateFilePaths(table);
        assertEquals(1, before.size());

        // Capture the kept file's record count to assert it is preserved.
        long keptRecordCount = before.values().iterator().next().get(0).recordCount();

        DedupFiles.applyDedup(table, before);
        table.refresh();

        // No duplicates remain.
        assertEquals(0, DedupFiles.findDuplicateFilePaths(table).size());

        // Both distinct paths still present, each referenced exactly once, record count preserved.
        Map<String, Integer> counts = new java.util.HashMap<>();
        long dupPathRecords = -1;
        try (org.apache.iceberg.io.CloseableIterable<org.apache.iceberg.FileScanTask> tasks =
                table.newScan().planFiles()) {
            for (org.apache.iceberg.FileScanTask t : tasks) {
                String p = t.file().path().toString();
                counts.merge(p, 1, Integer::sum);
                if (p.endsWith("dup.parquet")) {
                    dupPathRecords = t.file().recordCount();
                }
            }
        }
        assertEquals(2, counts.size());
        for (Integer c : counts.values()) {
            assertEquals(1, c);
        }
        assertEquals(keptRecordCount, dupPathRecords);
    }

    @Test
    void testRecordContentsDeduplicatedAfterApply() throws Exception {
        // A table with real records, NOT the fake-metrics helper, so we can read rows back.
        TableIdentifier tableId = TableIdentifier.of(Namespace.of("t", "ns"), "records");
        Schema schema = new Schema(
                Types.NestedField.required(1, "id", Types.IntegerType.get()),
                Types.NestedField.optional(2, "data", Types.StringType.get()));
        Table table = catalog.createTable(tableId, schema, PartitionSpec.unpartitioned());

        // One physical file holding 3 records, committed TWICE (two snapshots) -> duplicate
        // reference; plus a distinct file with 1 record.
        DataFile dup = IcebergTestRecords.writeRecords(table, 1, 2, 3);
        table.newAppend().appendFile(dup).commit();
        table.newAppend().appendFile(dup).commit();
        DataFile unique = IcebergTestRecords.writeRecords(table, 4);
        table.newAppend().appendFile(unique).commit();
        table.refresh();

        // BEFORE: the duplicated file's rows are returned twice (3 ids x2 + 1 = 7 rows).
        List<Integer> before = IcebergTestRecords.readIds(table);
        assertEquals(7, before.size(), "before: " + before);
        assertEquals(2, Collections.frequency(before, 1), "before: " + before);
        assertEquals(2, Collections.frequency(before, 2), "before: " + before);
        assertEquals(2, Collections.frequency(before, 3), "before: " + before);
        assertEquals(1, Collections.frequency(before, 4), "before: " + before);

        // Apply the fix.
        Map<String, List<DataFile>> dups = DedupFiles.findDuplicateFilePaths(table);
        assertEquals(1, dups.size());
        DedupFiles.applyDedup(table, dups);
        table.refresh();

        // AFTER: each record appears exactly once (4 distinct rows), no duplicates remain.
        List<Integer> after = IcebergTestRecords.readIds(table);
        assertEquals(4, after.size(), "after: " + after);
        assertEquals(List.of(1, 2, 3, 4),
                after.stream().sorted().collect(Collectors.toList()), "after: " + after);
        assertEquals(0, DedupFiles.findDuplicateFilePaths(table).size());
    }

    @Test
    void testApplyDedupNoDuplicatesIsNoOp() throws Exception {
        Table table = createTableAppending("ns/noop", "x.parquet");
        long snapshotIdBefore = table.currentSnapshot().snapshotId();

        DedupFiles.applyDedup(table, java.util.Map.of());
        table.refresh();

        assertEquals(snapshotIdBefore, table.currentSnapshot().snapshotId());
    }

    @Test
    void testDryRunReportsDuplicatesWithoutFixing() throws Exception {
        Table table = createTableAppending("ns/dry",
                "dup.parquet", "dup.parquet", "unique.parquet");

        File configFile = writeConfigFile();
        CapturedOutput captured = runCommand(configFile,
                "-t", "ns/dry", "--catalog-name", CATALOG_NAME);

        assertEquals(0, captured.exitCode, "stderr: " + captured.stderr);
        assertTrue(captured.stdout.contains("Duplicate data-file paths"), "stdout: " + captured.stdout);
        assertTrue(captured.stdout.contains("[DRY RUN]"), "stdout: " + captured.stdout);

        table.refresh();
        assertEquals(1, DedupFiles.findDuplicateFilePaths(table).size());
    }

    @Test
    void testApplyFixesDuplicates() throws Exception {
        Table table = createTableAppending("ns/applyfix",
                "dup.parquet", "dup.parquet", "unique.parquet");

        File configFile = writeConfigFile();
        CapturedOutput captured = runCommand(configFile,
                "-t", "ns/applyfix", "--catalog-name", CATALOG_NAME, "--apply");

        assertEquals(0, captured.exitCode, "stderr: " + captured.stderr);
        assertTrue(captured.stdout.contains("Fix complete"), "stdout: " + captured.stdout);

        table.refresh();
        assertEquals(0, DedupFiles.findDuplicateFilePaths(table).size());
    }

    @Test
    void testNoDuplicatesReportsNothingToFix() throws Exception {
        createTableAppending("ns/clean", "a.parquet", "b.parquet");

        File configFile = writeConfigFile();
        CapturedOutput captured = runCommand(configFile,
                "-t", "ns/clean", "--catalog-name", CATALOG_NAME);

        assertEquals(0, captured.exitCode, "stderr: " + captured.stderr);
        assertTrue(captured.stdout.contains("No duplicate data-file paths found"),
                "stdout: " + captured.stdout);
    }

    @Test
    void testAbortsOnTablesWithDeleteFiles() throws Exception {
        Table table = createTableAppending("ns/has-deletes", "data.parquet");

        Path deletePath = tempDir.resolve("deletes.parquet");
        Files.createFile(deletePath);
        DeleteFile deleteFile = FileMetadata.deleteFileBuilder(table.spec())
                .ofPositionDeletes()
                .withPath(deletePath.toString())
                .withFileSizeInBytes(50)
                .withFormat(FileFormat.PARQUET)
                .withRecordCount(1)
                .build();
        table.newRowDelta().addDeletes(deleteFile).commit();
        table.refresh();

        File configFile = writeConfigFile();
        CapturedOutput captured = runCommand(configFile,
                "-t", "ns/has-deletes", "--catalog-name", CATALOG_NAME, "--apply");

        assertEquals(1, captured.exitCode, "stderr: " + captured.stderr);
        assertTrue(captured.stderr.contains("delete files"),
                "Expected 'delete files' in stderr but got: " + captured.stderr);
    }

    @Test
    void testReportsDuplicateTagsDiagnostic() throws Exception {
        Table table = createTableAppending("ns/tagdiag");
        appendWithTag(table, "f1.parquet", "{\"stream-p0\":\"1:5\"}");
        appendWithTag(table, "f2.parquet", "{\"stream-p0\":\"1:5\"}");

        File configFile = writeConfigFile();
        CapturedOutput captured = runCommand(configFile,
                "-t", "ns/tagdiag", "--catalog-name", CATALOG_NAME);

        assertEquals(0, captured.exitCode, "stderr: " + captured.stderr);
        assertTrue(captured.stdout.contains("Duplicate lakestream.tags across snapshots"),
                "stdout: " + captured.stdout);
        assertTrue(captured.stdout.contains("No duplicate data-file paths found"),
                "stdout: " + captured.stdout);
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
            String[] baseArgs = {"-c", configFile.getAbsolutePath(), "table", "dedup-files"};
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

    private record CapturedOutput(int exitCode, String stdout, String stderr) { }

    private void appendWithTag(Table table, String relativePath, String tagsJson) throws Exception {
        Path dataPath = tempDir.resolve(relativePath);
        if (!Files.exists(dataPath)) {
            Files.createFile(dataPath);
        }
        DataFile dataFile = DataFiles.builder(table.spec())
                .withPath(dataPath.toString())
                .withFileSizeInBytes(100)
                .withFormat(FileFormat.PARQUET)
                .withRecordCount(10)
                .build();
        AppendFiles append = table.newAppend();
        append.appendFile(dataFile);
        append.set("lakestream.tags", tagsJson);
        append.commit();
        table.refresh();
    }
}
