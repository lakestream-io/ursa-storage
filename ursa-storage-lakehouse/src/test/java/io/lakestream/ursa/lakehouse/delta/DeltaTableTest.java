/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.lakehouse.delta;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.delta.kernel.Operation;
import io.delta.kernel.Table;
import io.delta.kernel.Transaction;
import io.delta.kernel.TransactionCommitResult;
import io.delta.kernel.data.Row;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.hook.PostCommitHook;
import io.delta.kernel.internal.SnapshotImpl;
import io.delta.kernel.internal.hook.CheckpointHook;
import io.delta.kernel.types.LongType;
import io.delta.kernel.types.StructType;
import io.delta.kernel.utils.CloseableIterable;
import io.delta.kernel.utils.CloseableIterator;
import io.lakestream.ursa.compaction.task.CompactStreamTask;
import io.lakestream.ursa.lakehouse.DeltaCommitter;
import io.lakestream.ursa.lakehouse.LakehouseConfiguration;
import io.lakestream.ursa.lakehouse.writer.ParquetFileStat;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Pattern;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class DeltaTableTest {

    @TempDir
    static Path path;

    @Test
    public void testDeltaTable_Properties() throws Exception {
        Properties properties = new Properties();
        properties.put("storagePath", path.toString());
        properties.put("partitionKey", "none");


        LakehouseConfiguration config = new LakehouseConfiguration(properties);
        String topic = "testDeltaTable_Properties";
        ManagedDeltaTable deltaManagedTable = new ManagedDeltaTable(config, topic);

        StructType schema = new StructType().add("id", LongType.LONG);

        deltaManagedTable.createDeltaTable(null, schema);

        Table table = deltaManagedTable.getTable();
        Engine engine = deltaManagedTable.getEngine();

        //add property a:a
        {
            Map<String, String> tableProperties = new HashMap<>();
            tableProperties.put("a", "a");
            Transaction tx = table.createTransactionBuilder(engine, "test", Operation.MANUAL_UPDATE)
                .withTableProperties(engine, tableProperties).build(engine);
            tx.commit(engine, CloseableIterable.emptyIterable());

            SnapshotImpl latestSnapshot = (SnapshotImpl) table.getLatestSnapshot(engine);
            Map<String, String> configuration = latestSnapshot.getMetadata().getConfiguration();
            assertEquals(3, configuration.size());
            assertEquals("a", configuration.get("a"));
            assertEquals("1", configuration.get("delta.columnMapping.maxColumnId"));
            assertEquals("name", configuration.get("delta.columnMapping.mode"));
        }

        //add property b:b
        {
            Map<String, String> tableProperties = new HashMap<>();
            tableProperties.put("b", "b");
            Transaction tx = table.createTransactionBuilder(engine, "test", Operation.MANUAL_UPDATE)
                .withTableProperties(engine, tableProperties).build(engine);
            tx.commit(engine, CloseableIterable.emptyIterable());

            SnapshotImpl latestSnapshot = (SnapshotImpl) table.getLatestSnapshot(engine);
            Map<String, String> configuration = latestSnapshot.getMetadata().getConfiguration();
            assertEquals(4, configuration.size());
            assertEquals("a", configuration.get("a"));
            assertEquals("b", configuration.get("b"));
            assertEquals("1", configuration.get("delta.columnMapping.maxColumnId"));
            assertEquals("name", configuration.get("delta.columnMapping.mode"));
        }

        //no properties
        {
            Transaction tx = table.createTransactionBuilder(engine, "test", Operation.MANUAL_UPDATE)
                .build(engine);
            tx.commit(engine, CloseableIterable.emptyIterable());

            SnapshotImpl latestSnapshot = (SnapshotImpl) table.getLatestSnapshot(engine);
            Map<String, String> configuration = latestSnapshot.getMetadata().getConfiguration();
            assertEquals(4, configuration.size());
            assertEquals("a", configuration.get("a"));
            assertEquals("b", configuration.get("b"));
            assertEquals("1", configuration.get("delta.columnMapping.maxColumnId"));
            assertEquals("name", configuration.get("delta.columnMapping.mode"));
        }
    }

    @Test
    public void testDelTable_getTableAddActionIterator() throws Exception {
        Properties properties = new Properties();
        properties.put("storagePath", path.toString());
        properties.put("partitionKey", "none");

        LakehouseConfiguration config = new LakehouseConfiguration(properties);
        String topic = "testDelTable_getTableAddActionIterator";
        ManagedDeltaTable deltaManagedTable = new ManagedDeltaTable(config, topic);

        StructType schema = new StructType().add("id", LongType.LONG);

        deltaManagedTable.createDeltaTable(0L, schema);

        for (int i = 0; i < 100; i++) {
            String file = "part-0000" + i + ".parquet";
            Map<String, String> tags = new HashMap<>();
            tags.put("streamId", "1");
            tags.put("startOffset", String.valueOf(i));
            tags.put("endOffset", String.valueOf(i + 1));
            ParquetFileStat parquetFileStat =
                new ParquetFileStat(file, path + "/" + file, 100L, "", Collections.emptyMap(), tags);
            deltaManagedTable.commit(Collections.singletonList(parquetFileStat));
        }

        //The iterator is reversed
        int i = 100;
        CloseableIterator<AddFileAction> tableAddActionIterator = deltaManagedTable.getTableAddActionIterator();
        while (tableAddActionIterator.hasNext()) {
            i--;
            AddFileAction addFile = tableAddActionIterator.next();
            String path = addFile.getPath();
            Map<String, String> tags = addFile.getTags();
            Long size = addFile.getSize();

            String file = "part-0000" + i + ".parquet";
            assertEquals(file, path);

            assertEquals(3, tags.size());
            assertEquals("1", tags.get("streamId"));
            assertEquals(String.valueOf(i + 1), tags.get("endOffset"));
            assertEquals("true", tags.get(DeltaTable.ORDER_TAG));

            assertEquals(100L, size);
        }
        assertEquals(0, i);
        tableAddActionIterator.close();
    }

    @Test
    public void testDelTable_getTableAddActionIteratorFailsWhenCheckpointInterrupted() throws Exception {
        Properties properties = new Properties();
        properties.put("storagePath", path.toString());
        properties.put("partitionKey", "none");

        LakehouseConfiguration config = new LakehouseConfiguration(properties);
        String topic = "testDelTable_getTableAddActionIteratorFailsWhenCheckpointInterrupted";
        InterruptedCheckpointManagedDeltaTable deltaManagedTable =
            new InterruptedCheckpointManagedDeltaTable(config, topic, 100L);

        StructType schema = new StructType().add("id", LongType.LONG);
        deltaManagedTable.createDeltaTable(0L, schema);

        for (int i = 0; i < 100; i++) {
            String file = "part-0000" + i + ".parquet";
            Map<String, String> tags = new HashMap<>();
            tags.put("streamId", "1");
            tags.put("startOffset", String.valueOf(i));
            tags.put("endOffset", String.valueOf(i + 1));
            ParquetFileStat parquetFileStat =
                new ParquetFileStat(file, path + "/" + file, 100L, "", Collections.emptyMap(), tags);
            deltaManagedTable.commit(Collections.singletonList(parquetFileStat));
        }

        RuntimeException exception = null;
        try {
            deltaManagedTable.getTableAddActionIterator();
        } catch (RuntimeException e) {
            exception = e;
        }
        assertTrue(exception != null, "Expected getTableAddActionIterator to fail on dirty checkpoint");
        assertTrue(Pattern.compile(
                "io\\.delta\\.kernel\\.defaults\\.internal\\.parquet\\.ParquetIOUtils\\$1@\\w+ "
                    + "is not a Parquet file \\(length is too low: 4\\)")
            .matcher(exception.getMessage())
            .find(), "Unexpected exception message: " + exception.getMessage());

        Files.delete(deltaManagedTable.getCorruptedCheckpointFile());

        int actionCount = 0;
        try (CloseableIterator<AddFileAction> iterator = deltaManagedTable.getTableAddActionIterator()) {
            while (iterator.hasNext()) {
                iterator.next();
                actionCount++;
            }
        }
        assertEquals(100, actionCount);
    }

    @Test
    public void testDelTableCombineCommit_getTableAddActionIterator() throws Exception {
        Properties properties = new Properties();
        properties.put("storagePath", path.toString());
        properties.put("partitionKey", "none");


        LakehouseConfiguration config = new LakehouseConfiguration(properties);
        String topic = "testDelTableCombineCommit_getTableAddActionIterator";
        ManagedDeltaTable deltaManagedTable = new ManagedDeltaTable(config, topic);

        StructType schema = new StructType().add("id", LongType.LONG);

        deltaManagedTable.createDeltaTable(0L, schema);

        List<ParquetFileStat> parquetFileStats = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            String file = "part-0000" + i + ".parquet";
            Map<String, String> tags = new HashMap<>();
            tags.put("streamId", "1");
            tags.put("startOffset", String.valueOf(i));
            tags.put("endOffset", String.valueOf(i + 1));
            ParquetFileStat parquetFileStat =
                new ParquetFileStat(file, path + "/" + file, 100L, "", Collections.emptyMap(), tags);
            parquetFileStats.add(parquetFileStat);
            if (parquetFileStats.size() == 10) {
                deltaManagedTable.commit(parquetFileStats);
                parquetFileStats.clear();
            }
        }

        //The iterator is reversed
        int i = 100;
        CloseableIterator<AddFileAction> tableAddActionIterator = deltaManagedTable.getTableAddActionIterator();
        while (tableAddActionIterator.hasNext()) {
            i--;
            AddFileAction addFile = tableAddActionIterator.next();
            String path = addFile.getPath();
            Map<String, String> tags = addFile.getTags();
            Long size = addFile.getSize();

            String file = "part-0000" + i + ".parquet";
            assertEquals(file, path);

            assertEquals(3, tags.size());
            assertEquals("1", tags.get("streamId"));
            assertEquals(String.valueOf(i + 1), tags.get("endOffset"));
            assertEquals("true", tags.get(DeltaTable.ORDER_TAG));

            assertEquals(100L, size);
        }
        assertEquals(0, i);
        tableAddActionIterator.close();
    }

    private static final class InterruptedCheckpointManagedDeltaTable extends ManagedDeltaTable {

        private final long interruptedCheckpointVersion;
        private Path corruptedCheckpointFile;

        private InterruptedCheckpointManagedDeltaTable(LakehouseConfiguration config, String parentTopic,
                                                    long interruptedCheckpointVersion) {
            super(config, parentTopic);
            this.interruptedCheckpointVersion = interruptedCheckpointVersion;
        }

        @Override
        public synchronized void commitSnapshot(List<Row> actions) {
            if (actions.isEmpty()) {
                return;
            }
            Transaction txn = table.createTransactionBuilder(engine, URSA_DELTA_ENGINE, Operation.WRITE)
                .build(engine);
            TransactionCommitResult commitResult =
                txn.commit(engine, DeltaTableUtils.toCloseableIterable(actions));

            List<PostCommitHook> hooks = commitResult.getPostCommitHooks();
            for (PostCommitHook hook : hooks) {
                if (hook instanceof CheckpointHook checkpointHook) {
                    try {
                        if (commitResult.getVersion() == interruptedCheckpointVersion) {
                            createCorruptedCheckpointFile(commitResult.getVersion());
                        } else {
                            checkpointHook.threadSafeInvoke(engine);
                        }
                    } catch (Exception e) {
                        throw new RuntimeException("Failed to create corrupted checkpoint file", e);
                    }
                }
            }
        }

        private void createCorruptedCheckpointFile(long version) throws Exception {
            Path logDir = Path.of(tableLocation, "_delta_log");
            Files.createDirectories(logDir);
            corruptedCheckpointFile = logDir.resolve(String.format("%020d.checkpoint.parquet", version));
            Files.write(corruptedCheckpointFile, new byte[] {'P', 'A', 'R', '1'});
        }

        private Path getCorruptedCheckpointFile() {
            return corruptedCheckpointFile;
        }
    }

    @Test
    public void testDelTableMultiStreamId_getTableAddActionIterator() throws Exception {
        Properties properties = new Properties();
        properties.put("storagePath", path.toString());
        properties.put("partitionKey", "none");

        LakehouseConfiguration config = new LakehouseConfiguration(properties);
        String topic = "testDelTableMultiStreamId_getTableAddActionIterator";
        ManagedDeltaTable deltaManagedTable = new ManagedDeltaTable(config, topic);

        StructType schema = new StructType().add("id", LongType.LONG);
        deltaManagedTable.createDeltaTable(0L, schema);

        List<ParquetFileStat> parquetFileStats = new ArrayList<>();

        for (int i = 0; i < 100; i++) {
            String file = "part-0000" + i + ".parquet";
            Map<String, String> tags = new HashMap<>();

            String streamId = i < 50 ? "1" : "2";
            tags.put("streamId", streamId);
            tags.put("startOffset", String.valueOf(i));
            tags.put("endOffset", String.valueOf(i + 1));

            ParquetFileStat parquetFileStat =
                new ParquetFileStat(file, path + "/" + file, 100L, "", Collections.emptyMap(), tags);
            parquetFileStats.add(parquetFileStat);

            if (parquetFileStats.size() == 10) {
                deltaManagedTable.commit(parquetFileStats);
                parquetFileStats.clear();
            }
        }

        CloseableIterator<AddFileAction> tableAddActionIterator = deltaManagedTable.getTableAddActionIterator();

        int expectedIndex = 99;
        String expectedStreamId = "2";

        while (tableAddActionIterator.hasNext()) {
            AddFileAction addFile = tableAddActionIterator.next();
            String filePath = addFile.getPath();
            Map<String, String> tags = addFile.getTags();
            Long size = addFile.getSize();

            String expectedFile = "part-0000" + expectedIndex + ".parquet";
            assertEquals(expectedFile, filePath);

            assertEquals(3, tags.size());

            if (expectedIndex < 50) {
                expectedStreamId = "1";
            }
            assertEquals(expectedStreamId, tags.get("streamId"));
            assertEquals(String.valueOf(expectedIndex + 1), tags.get("endOffset"));
            assertEquals("true", tags.get(DeltaTable.ORDER_TAG));

            assertEquals(100L, size);

            expectedIndex--;

            if (expectedIndex < 0) {
                break;
            }
        }

        assertEquals(-1, expectedIndex);
        tableAddActionIterator.close();
    }

    @Test
    public void testIsTheCompactStreamTaskCommitted_WithTopicTag_HigherStreamId() throws Exception {
        Properties properties = new Properties();
        properties.put("storagePath", path.toString());
        properties.put("partitionKey", "none");

        LakehouseConfiguration config = new LakehouseConfiguration(properties);
        String topic = "testIsTheCompactStreamTaskCommitted_WithTopicTag_HigherStreamId";
        DeltaCommitter deltaCommitter = new DeltaCommitter(config, topic);

        StructType schema = new StructType().add("id", LongType.LONG);
        ManagedDeltaTable deltaManagedTable = new ManagedDeltaTable(config, topic);
        deltaManagedTable.createDeltaTable(0L, schema);

        // Add files with topic tags
        Map<String, String> tags = new HashMap<>();
        tags.put("topic", topic);
        tags.put("streamId", "5");
        tags.put("startOffset", "100");
        tags.put("endOffset", "200");
        ParquetFileStat parquetFileStat =
            new ParquetFileStat("file1.parquet", path + "/file1.parquet", 1000L, "", Collections.emptyMap(), tags);
        deltaManagedTable.commit(Collections.singletonList(parquetFileStat));

        // Create a task with lower streamId - should be considered committed
        CompactStreamTask task = new CompactStreamTask();
        task.setTopic(topic);
        task.setStreamId(3L);
        task.setStartOffset(50L);
        task.setEndOffset(100L);

        boolean result = deltaCommitter.isTheCompactStreamTaskCommitted(task);
        assertTrue(result, "Task with lower streamId than committed should be considered committed");
    }

    @Test
    public void testIsTheCompactStreamTaskCommitted_WithTopicTag_SameStreamIdHigherOffset() throws Exception {
        Properties properties = new Properties();
        properties.put("storagePath", path.toString());
        properties.put("partitionKey", "none");

        LakehouseConfiguration config = new LakehouseConfiguration(properties);
        String topic = "testIsTheCompactStreamTaskCommitted_WithTopicTag_SameStreamIdHigherOffset";
        DeltaCommitter deltaCommitter = new DeltaCommitter(config, topic);

        StructType schema = new StructType().add("id", LongType.LONG);
        ManagedDeltaTable deltaManagedTable = new ManagedDeltaTable(config, topic);
        deltaManagedTable.createDeltaTable(0L, schema);

        // Add files with topic tags
        Map<String, String> tags = new HashMap<>();
        tags.put("topic", topic);
        tags.put("streamId", "5");
        tags.put("startOffset", "100");
        tags.put("endOffset", "200");
        ParquetFileStat parquetFileStat =
            new ParquetFileStat("file1.parquet", path + "/file1.parquet", 1000L, "", Collections.emptyMap(), tags);
        deltaManagedTable.commit(Collections.singletonList(parquetFileStat));

        // Create a task with same streamId but lower offset - should be considered committed
        CompactStreamTask task = new CompactStreamTask();
        task.setTopic(topic);
        task.setStreamId(5L);
        task.setStartOffset(50L);
        task.setEndOffset(100L);

        boolean result = deltaCommitter.isTheCompactStreamTaskCommitted(task);
        assertTrue(result, "Task with startOffset < committed endOffset should be considered committed");
    }

    @Test
    public void testIsTheCompactStreamTaskCommitted_WithTopicTag_SameStreamIdEqualOrHigherOffset() throws Exception {
        Properties properties = new Properties();
        properties.put("storagePath", path.toString());
        properties.put("partitionKey", "none");

        LakehouseConfiguration config = new LakehouseConfiguration(properties);
        String topic = "testIsTheCompactStreamTaskCommitted_WithTopicTag_SameStreamIdEqualOrHigherOffset";
        DeltaCommitter deltaCommitter = new DeltaCommitter(config, topic);

        StructType schema = new StructType().add("id", LongType.LONG);
        ManagedDeltaTable deltaManagedTable = new ManagedDeltaTable(config, topic);
        deltaManagedTable.createDeltaTable(0L, schema);

        // Add files with topic tags
        Map<String, String> tags = new HashMap<>();
        tags.put("topic", topic);
        tags.put("streamId", "5");
        tags.put("startOffset", "100");
        tags.put("endOffset", "200");
        ParquetFileStat parquetFileStat =
            new ParquetFileStat("file1.parquet", path + "/file1.parquet", 1000L, "", Collections.emptyMap(), tags);
        deltaManagedTable.commit(Collections.singletonList(parquetFileStat));

        // Test 1: Task with same streamId and startOffset = endOffset - should NOT be committed
        CompactStreamTask task1 = new CompactStreamTask();
        task1.setTopic(topic);
        task1.setStreamId(5L);
        task1.setStartOffset(200L);
        task1.setEndOffset(300L);

        boolean result1 = deltaCommitter.isTheCompactStreamTaskCommitted(task1);
        assertFalse(result1, "Task with startOffset = committed endOffset should NOT be considered committed");

        // Test 2: Task with same streamId and startOffset > endOffset - should NOT be committed
        CompactStreamTask task2 = new CompactStreamTask();
        task2.setTopic(topic);
        task2.setStreamId(5L);
        task2.setStartOffset(250L);
        task2.setEndOffset(350L);

        boolean result2 = deltaCommitter.isTheCompactStreamTaskCommitted(task2);
        assertFalse(result2, "Task with startOffset > committed endOffset should NOT be considered committed");
    }

    @Test
    public void testIsTheCompactStreamTaskCommitted_WithTopicTag_LowerStreamId() throws Exception {
        Properties properties = new Properties();
        properties.put("storagePath", path.toString());
        properties.put("partitionKey", "none");

        LakehouseConfiguration config = new LakehouseConfiguration(properties);
        String topic = "testIsTheCompactStreamTaskCommitted_WithTopicTag_LowerStreamId";
        DeltaCommitter deltaCommitter = new DeltaCommitter(config, topic);

        StructType schema = new StructType().add("id", LongType.LONG);
        ManagedDeltaTable deltaManagedTable = new ManagedDeltaTable(config, topic);
        deltaManagedTable.createDeltaTable(0L, schema);

        // Add files with topic tags
        Map<String, String> tags = new HashMap<>();
        tags.put("topic", topic);
        tags.put("streamId", "5");
        tags.put("startOffset", "100");
        tags.put("endOffset", "200");
        ParquetFileStat parquetFileStat =
            new ParquetFileStat("file1.parquet", path + "/file1.parquet", 1000L, "", Collections.emptyMap(), tags);
        deltaManagedTable.commit(Collections.singletonList(parquetFileStat));

        // Create a task with higher streamId - should NOT be considered committed
        CompactStreamTask task = new CompactStreamTask();
        task.setTopic(topic);
        task.setStreamId(7L);
        task.setStartOffset(0L);
        task.setEndOffset(100L);

        boolean result = deltaCommitter.isTheCompactStreamTaskCommitted(task);
        assertFalse(result, "Task with higher streamId than committed should NOT be considered committed");
    }

    @Test
    public void testIsTheCompactStreamTaskCommitted_WithTopicTag_DifferentTopic() throws Exception {
        Properties properties = new Properties();
        properties.put("storagePath", path.toString());
        properties.put("partitionKey", "none");

        LakehouseConfiguration config = new LakehouseConfiguration(properties);
        String topic = "testIsTheCompactStreamTaskCommitted_WithTopicTag_DifferentTopic";
        DeltaCommitter deltaCommitter = new DeltaCommitter(config, topic);

        StructType schema = new StructType().add("id", LongType.LONG);
        ManagedDeltaTable deltaManagedTable = new ManagedDeltaTable(config, topic);
        deltaManagedTable.createDeltaTable(0L, schema);

        // Add files with topic tags
        Map<String, String> tags = new HashMap<>();
        tags.put("topic", "different-topic");
        tags.put("streamId", "5");
        tags.put("startOffset", "100");
        tags.put("endOffset", "200");
        ParquetFileStat parquetFileStat =
            new ParquetFileStat("file1.parquet", path + "/file1.parquet", 1000L, "", Collections.emptyMap(), tags);
        deltaManagedTable.commit(Collections.singletonList(parquetFileStat));

        // Create a task with same streamId but different topic - should NOT be considered committed
        CompactStreamTask task = new CompactStreamTask();
        task.setTopic(topic);
        task.setStreamId(5L);
        task.setStartOffset(50L);
        task.setEndOffset(100L);

        boolean result = deltaCommitter.isTheCompactStreamTaskCommitted(task);
        assertFalse(result, "Task with different topic should NOT be considered committed");
    }

    @Test
    public void testIsTheCompactStreamTaskCommitted_EarlyTerminationWithIterator() throws Exception {
        Properties properties = new Properties();
        properties.put("storagePath", path.toString());
        properties.put("partitionKey", "none");

        LakehouseConfiguration config = new LakehouseConfiguration(properties);
        String topic = "testIsTheCompactStreamTaskCommitted_EarlyTerminationWithIterator";
        DeltaCommitter deltaCommitter = new DeltaCommitter(config, topic);

        StructType schema = new StructType().add("id", LongType.LONG);
        ManagedDeltaTable deltaManagedTable = new ManagedDeltaTable(config, topic);
        deltaManagedTable.createDeltaTable(0L, schema);

        // Add multiple files with different streamIds (reverse order to test iterator)
        // The iterator returns in reverse order (newest first)
        for (int streamId = 1; streamId <= 10; streamId++) {
            for (int offset = 0; offset < 100; offset += 10) {
                Map<String, String> tags = new HashMap<>();
                tags.put("topic", topic);
                tags.put("streamId", String.valueOf(streamId));
                tags.put("startOffset", String.valueOf(offset));
                tags.put("endOffset", String.valueOf(offset + 10));
                String fileName = String.format("file-%d-%d.parquet", streamId, offset);
                ParquetFileStat stat =
                    new ParquetFileStat(fileName, path + "/" + fileName, 1000L, "", Collections.emptyMap(), tags);
                deltaManagedTable.commit(Collections.singletonList(stat));
            }
        }

        // Test early termination - task with streamId 5, should find match quickly
        CompactStreamTask task = new CompactStreamTask();
        task.setTopic(topic);
        task.setStreamId(5L);
        task.setStartOffset(20L);
        task.setEndOffset(30L);

        // The iterator should terminate early when it finds streamId 5 with endOffset > 20
        boolean result = deltaCommitter.isTheCompactStreamTaskCommitted(task);
        assertTrue(result, "Task should be found as committed with early termination");
    }

    @Test
    public void testIsTheCompactStreamTaskCommitted_MixedTopicAndOldLogic() throws Exception {
        Properties properties = new Properties();
        properties.put("storagePath", path.toString());
        properties.put("partitionKey", "none");

        LakehouseConfiguration config = new LakehouseConfiguration(properties);
        String topic = "testIsTheCompactStreamTaskCommitted_MixedTopicAndOldLogic";
        DeltaCommitter deltaCommitter = new DeltaCommitter(config, topic);

        StructType schema = new StructType().add("id", LongType.LONG);
        ManagedDeltaTable deltaManagedTable = new ManagedDeltaTable(config, topic);
        deltaManagedTable.createDeltaTable(0L, schema);

        // Add file with new topic-based tags
        Map<String, String> newTags = new HashMap<>();
        newTags.put("topic", topic);
        newTags.put("streamId", "5");
        newTags.put("startOffset", "100");
        newTags.put("endOffset", "200");
        ParquetFileStat newFile =
            new ParquetFileStat("new-file.parquet", path + "/new-file.parquet", 1000L, "", Collections.emptyMap(),
                newTags);

        // Add file with old tags (no topic field)
        Map<String, String> oldTags = new HashMap<>();
        oldTags.put("streamId", "3");
        oldTags.put("startOffset", "50");
        oldTags.put("endOffset", "100");
        ParquetFileStat oldFile =
            new ParquetFileStat("old-file.parquet", path + "/old-file.parquet", 1000L, "", Collections.emptyMap(),
                oldTags);

        List<ParquetFileStat> files = new ArrayList<>();
        files.add(newFile);
        files.add(oldFile);
        deltaManagedTable.commit(files);

        // Test 1: Task that matches the new logic (with topic)
        CompactStreamTask task1 = new CompactStreamTask();
        task1.setTopic(topic);
        task1.setStreamId(4L);  // Between old and new streamIds
        task1.setStartOffset(0L);
        task1.setEndOffset(50L);

        boolean result1 = deltaCommitter.isTheCompactStreamTaskCommitted(task1);
        assertTrue(result1, "Task with streamId < 5 should be committed based on new logic");

        // Test 2: Task that would match old logic exactly
        CompactStreamTask task2 = new CompactStreamTask();
        task2.setTopic(topic);
        task2.setStreamId(3L);
        task2.setStartOffset(50L);
        task2.setEndOffset(100L);

        boolean result2 = deltaCommitter.isTheCompactStreamTaskCommitted(task2);
        assertTrue(result2, "Task should match old file with exact streamId/offset match");
    }

    @Test
    public void testIsTheCompactStreamTaskCommitted_WithNullAndEmptyTags() throws Exception {
        Properties properties = new Properties();
        properties.put("storagePath", path.toString());
        properties.put("partitionKey", "none");

        LakehouseConfiguration config = new LakehouseConfiguration(properties);
        String topic = "testIsTheCompactStreamTaskCommitted_WithNullAndEmptyTags";
        DeltaCommitter deltaCommitter = new DeltaCommitter(config, topic);

        StructType schema = new StructType().add("id", LongType.LONG);
        ManagedDeltaTable deltaManagedTable = new ManagedDeltaTable(config, topic);
        deltaManagedTable.createDeltaTable(0L, schema);

        // Add file with valid tags
        Map<String, String> validTags = new HashMap<>();
        validTags.put("topic", topic);
        validTags.put("streamId", "5");
        validTags.put("startOffset", "100");
        validTags.put("endOffset", "200");
        ParquetFileStat validFile =
            new ParquetFileStat("valid.parquet", path + "/valid.parquet", 1000L, "", Collections.emptyMap(), validTags);

        // Add file with empty tags
        ParquetFileStat emptyTagsFile =
            new ParquetFileStat("empty.parquet", path + "/empty.parquet", 1000L, "", Collections.emptyMap(),
                new HashMap<>());

        // Add file with null tags
        ParquetFileStat nullTagsFile =
            new ParquetFileStat("null.parquet", path + "/null.parquet", 1000L, "", Collections.emptyMap(), null);

        List<ParquetFileStat> files = new ArrayList<>();
        files.add(validFile);
        files.add(emptyTagsFile);
        files.add(nullTagsFile);
        deltaManagedTable.commit(files);

        // Test that valid task is still found despite null/empty tags in other files
        CompactStreamTask task = new CompactStreamTask();
        task.setTopic(topic);
        task.setStreamId(3L);
        task.setStartOffset(50L);
        task.setEndOffset(100L);

        boolean result = deltaCommitter.isTheCompactStreamTaskCommitted(task);
        assertTrue(result, "Task should be considered committed based on valid file");
    }
}
