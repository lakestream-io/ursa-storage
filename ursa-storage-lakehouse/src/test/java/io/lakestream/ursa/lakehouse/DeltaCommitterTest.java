/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.lakehouse;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

import io.delta.kernel.Snapshot;
import io.delta.kernel.Table;
import io.delta.kernel.data.MapValue;
import io.delta.kernel.data.Row;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.internal.SnapshotImpl;
import io.delta.kernel.internal.TableConfig;
import io.delta.kernel.types.IntegerType;
import io.delta.kernel.types.LongType;
import io.delta.kernel.types.MapType;
import io.delta.kernel.types.StringType;
import io.delta.kernel.types.StructField;
import io.delta.kernel.types.StructType;
import io.delta.kernel.utils.CloseableIterator;
import io.lakestream.ursa.compaction.task.CompactStreamTask;
import io.lakestream.ursa.lakehouse.delta.AddFileAction;
import io.lakestream.ursa.lakehouse.delta.AvroToDeltaConvert;
import io.lakestream.ursa.lakehouse.delta.CloseableIterators;
import io.lakestream.ursa.lakehouse.delta.DeltaTable;
import io.lakestream.ursa.lakehouse.delta.DeltaTableUtils;
import io.lakestream.ursa.lakehouse.delta.GenericRow;
import io.lakestream.ursa.lakehouse.delta.ManagedDeltaTable;
import io.lakestream.ursa.lakehouse.delta.MapValueImpl;
import io.lakestream.ursa.lakehouse.delta.ParquetRowWriter;
import io.lakestream.ursa.lakehouse.iceberg.exception.SchemaEvolutionException;
import io.lakestream.ursa.lakehouse.iceberg.exception.SchemaMappingException;
import io.lakestream.ursa.lakehouse.utils.AvroSchemaUtilExtended;
import io.lakestream.ursa.lakehouse.writer.ParquetFileStat;
import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.io.ParquetDecodingException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;


@ExtendWith(MockitoExtension.class)
class DeltaCommitterTest {

    private DeltaCommitter committer;

    @Mock
    private Engine engine;

    @Mock
    private Table table;

    @TempDir
    static Path path;

    @BeforeEach
    void setUp() throws NoSuchFieldException, IllegalAccessException {
        committer = new DeltaCommitter(new LakehouseConfiguration(), "test-topic");
        Field deltaTableField = DeltaCommitter.class.getDeclaredField("deltaTable");
        deltaTableField.setAccessible(true);
        ManagedDeltaTable deltaTable = (ManagedDeltaTable) deltaTableField.get(committer);
        Field engineField = DeltaTable.class.getDeclaredField("engine");
        engineField.setAccessible(true);
        engineField.set(deltaTable, engine);

        Field tableField = ManagedDeltaTable.class.getDeclaredField("table");
        tableField.setAccessible(true);
        tableField.set(deltaTable, table);
    }

    @Test
    void testIsTheCompactStreamTaskCommitted_WhenOldTagsMatch_ReturnsTrue() throws IOException {
        // Arrange
        CompactStreamTask task = createTask(100L, 0L, 1000L, 5000L, 2000L, "/path/to/file");

        Map<String, String> tags = createTags(100L, 0L, 1000L, 5000L, 2000L, "/path/to/file");
        AddFileAction matchingFile = createAddFileAction(tags);

        CloseableIterator<AddFileAction> iterator = CloseableIterators.singleton(matchingFile);

        try (MockedStatic<DeltaTableUtils> mockedUtils = mockStatic(DeltaTableUtils.class)) {
            mockedUtils.when(() -> DeltaTableUtils.getAddActionIterator(table.getLatestSnapshot(engine), engine))
                .thenReturn(iterator);

            // Act
            boolean result = committer.isTheCompactStreamTaskCommitted(task);

            // Assert
            assertTrue(result);
            iterator.close();
        }
    }

    @Test
    void testIsTheCompactStreamTaskCommitted_WhenOldTagsNotFound_ReturnsFalse() throws IOException {
        // Arrange
        CompactStreamTask task = createTask(100L, 0L, 1000L, 5000L, 2000L, "/path/to/file");

        Map<String, String> tags = createTags(200L, 0L, 1000L, 5000L, 2000L, "/path/to/file");
        AddFileAction nonMatchingFile = createAddFileAction(tags);

        CloseableIterator<AddFileAction> iterator = CloseableIterators.singleton(nonMatchingFile);


        try (MockedStatic<DeltaTableUtils> mockedUtils = mockStatic(DeltaTableUtils.class)) {
            mockedUtils.when(() -> DeltaTableUtils.getAddActionIterator(table.getLatestSnapshot(engine), engine))
                .thenReturn(iterator);

            // Act
            boolean result = committer.isTheCompactStreamTaskCommitted(task);

            // Assert
            assertFalse(result);
            iterator.close();
        }
    }

    @Test
    void testIsTheCompactStreamTaskCommitted_WhenTagsAreNull_ReturnsFalse() throws IOException {
        // Arrange
        CompactStreamTask task = createTask(100L, 0L, 1000L, 5000L, 2000L, "/path/to/file");

        AddFileAction fileWithNullTags = mock(AddFileAction.class);
        when(fileWithNullTags.getTags()).thenReturn(null);

        CloseableIterator<AddFileAction> iterator = CloseableIterators.singleton(fileWithNullTags);

        try (MockedStatic<DeltaTableUtils> mockedUtils = mockStatic(DeltaTableUtils.class)) {
            mockedUtils.when(() -> DeltaTableUtils.getAddActionIterator(table.getLatestSnapshot(engine), engine))
                .thenReturn(iterator);

            // Act
            boolean result = committer.isTheCompactStreamTaskCommitted(task);

            // Assert
            assertFalse(result);
            iterator.close();
        }
    }

    @Test
    void testIsTheCompactStreamTaskCommitted_WhenTagsAreEmpty_ReturnsFalse() throws IOException {
        // Arrange
        CompactStreamTask task = createTask(100L, 0L, 1000L, 5000L, 2000L, "/path/to/file");

        AddFileAction fileWithEmptyTags = createAddFileAction(new HashMap<>());

        CloseableIterator<AddFileAction> iterator = CloseableIterators.singleton(fileWithEmptyTags);

        try (MockedStatic<DeltaTableUtils> mockedUtils = mockStatic(DeltaTableUtils.class)) {
            mockedUtils.when(() -> DeltaTableUtils.getAddActionIterator(table.getLatestSnapshot(engine), engine))
                .thenReturn(iterator);

            // Act
            boolean result = committer.isTheCompactStreamTaskCommitted(task);

            // Assert
            assertFalse(result);
            iterator.close();
        }
    }

    @Test
    void testIsTheCompactStreamTaskCommitted_WhenStreamIdKeyMissing_ReturnsFalse() throws IOException {
        // Arrange
        CompactStreamTask task = createTask(100L, 0L, 1000L, 5000L, 2000L, "/path/to/file");

        Map<String, String> tagsWithoutStreamId = new HashMap<>();
        tagsWithoutStreamId.put("startOffset", "0");
        tagsWithoutStreamId.put("endOffset", "1000");
        AddFileAction file = createAddFileAction(tagsWithoutStreamId);

        CloseableIterator<AddFileAction> iterator = CloseableIterators.singleton(file);

        try (MockedStatic<DeltaTableUtils> mockedUtils = mockStatic(DeltaTableUtils.class)) {
            mockedUtils.when(() -> DeltaTableUtils.getAddActionIterator(table.getLatestSnapshot(engine), engine))
                .thenReturn(iterator);

            // Act
            boolean result = committer.isTheCompactStreamTaskCommitted(task);

            // Assert
            assertFalse(result);
        }
    }

    @Test
    void testIsTheCompactStreamTaskCommitted_WhenStreamIdDifferent_ReturnsFalse() throws IOException {
        // Arrange
        CompactStreamTask task = createTask(100L, 0L, 1000L, 5000L, 2000L, "/path/to/file");

        Map<String, String> tags = createTags(999L, 0L, 1000L, 5000L, 2000L, "/path/to/file");
        AddFileAction file = createAddFileAction(tags);

        CloseableIterator<AddFileAction> iterator = CloseableIterators.singleton(file);

        try (MockedStatic<DeltaTableUtils> mockedUtils = mockStatic(DeltaTableUtils.class)) {
            mockedUtils.when(() -> DeltaTableUtils.getAddActionIterator(table.getLatestSnapshot(engine), engine))
                .thenReturn(iterator);

            // Act
            boolean result = committer.isTheCompactStreamTaskCommitted(task);

            // Assert
            assertFalse(result);
            iterator.close();
        }
    }

    @Test
    void testIsTheCompactStreamTaskCommitted_WhenStartOffsetDifferent_ReturnsFalse() throws IOException {
        // Arrange
        CompactStreamTask task = createTask(100L, 0L, 1000L, 5000L, 2000L, "/path/to/file");

        Map<String, String> tags = createTags(100L, 100L, 1000L, 5000L, 2000L, "/path/to/file");
        AddFileAction file = createAddFileAction(tags);

        CloseableIterator<AddFileAction> iterator = CloseableIterators.singleton(file);


        try (MockedStatic<DeltaTableUtils> mockedUtils = mockStatic(DeltaTableUtils.class)) {
            mockedUtils.when(() -> DeltaTableUtils.getAddActionIterator(table.getLatestSnapshot(engine), engine))
                .thenReturn(iterator);

            // Act
            boolean result = committer.isTheCompactStreamTaskCommitted(task);

            // Assert
            assertFalse(result);
            iterator.close();
        }
    }

    @Test
    void testIsTheCompactStreamTaskCommitted_WhenEndOffsetDifferent_ReturnsFalse() throws IOException {
        // Arrange
        CompactStreamTask task = createTask(100L, 0L, 1000L, 5000L, 2000L, "/path/to/file");

        Map<String, String> tags = createTags(100L, 0L, 2000L, 5000L, 2000L, "/path/to/file");
        AddFileAction file = createAddFileAction(tags);

        CloseableIterator<AddFileAction> iterator = CloseableIterators.singleton(file);

        try (MockedStatic<DeltaTableUtils> mockedUtils = mockStatic(DeltaTableUtils.class)) {
            mockedUtils.when(() -> DeltaTableUtils.getAddActionIterator(table.getLatestSnapshot(engine), engine))
                .thenReturn(iterator);

            // Act
            boolean result = committer.isTheCompactStreamTaskCommitted(task);

            // Assert
            assertFalse(result);
            iterator.close();
        }
    }

    @Test
    void testIsTheCompactStreamTaskCommitted_WhenNumberFormatException_ReturnsFalse() throws IOException {
        // Arrange
        CompactStreamTask task = createTask(100L, 0L, 1000L, 5000L, 2000L, "/path/to/file");

        Map<String, String> invalidTags = new HashMap<>();
        invalidTags.put("streamId", "invalid_number");
        invalidTags.put("startOffset", "0");
        AddFileAction file = createAddFileAction(invalidTags);

        CloseableIterator<AddFileAction> iterator = CloseableIterators.singleton(file);


        try (MockedStatic<DeltaTableUtils> mockedUtils = mockStatic(DeltaTableUtils.class)) {
            mockedUtils.when(() -> DeltaTableUtils.getAddActionIterator(table.getLatestSnapshot(engine), engine))
                .thenReturn(iterator);

            // Act
            boolean result = committer.isTheCompactStreamTaskCommitted(task);

            // Assert
            assertFalse(result);
            iterator.close();
        }
    }

    @Test
    void testIsTheCompactStreamTaskCommitted_WhenMultipleOldTagFilesWithMatch_ReturnsTrue() throws IOException {
        // Arrange
        CompactStreamTask task = createTask(100L, 0L, 1000L, 5000L, 2000L, "/path/to/file");

        List<AddFileAction> files = Arrays.asList(
            createAddFileAction(createTags(200L, 0L, 500L, 3000L, 1000L, "/other/file")),
            createAddFileAction(createTags(100L, 0L, 1000L, 5000L, 2000L, "/path/to/file")),
            createAddFileAction(createTags(300L, 100L, 2000L, 8000L, 4000L, "/another/file"))
        );

        CloseableIterator<Object> iterator = CloseableIterators.of(files.toArray());

        try (MockedStatic<DeltaTableUtils> mockedUtils = mockStatic(DeltaTableUtils.class)) {
            mockedUtils.when(() -> DeltaTableUtils.getAddActionIterator(table.getLatestSnapshot(engine), engine))
                .thenReturn(iterator);

            // Act
            boolean result = committer.isTheCompactStreamTaskCommitted(task);

            // Assert
            assertTrue(result);
            iterator.close();
        }
    }

    @Test
    void testIsTheCompactStreamTaskCommitted_WhenMultipleOldTagFilesWithoutMatch_ReturnsFalse() throws IOException {
        // Arrange
        CompactStreamTask task = createTask(100L, 0L, 1000L, 5000L, 2000L, "/path/to/file");

        List<AddFileAction> files = Arrays.asList(
            createAddFileAction(createTags(200L, 0L, 500L, 3000L, 1000L, "/other/file")),
            createAddFileAction(createTags(300L, 100L, 2000L, 8000L, 4000L, "/another/file"))
        );

        CloseableIterator<Object> iterator = CloseableIterators.of(files.toArray());


        try (MockedStatic<DeltaTableUtils> mockedUtils = mockStatic(DeltaTableUtils.class)) {
            mockedUtils.when(() -> DeltaTableUtils.getAddActionIterator(table.getLatestSnapshot(engine), engine))
                .thenReturn(iterator);

            // Act
            boolean result = committer.isTheCompactStreamTaskCommitted(task);

            // Assert
            assertFalse(result);
            iterator.close();
        }
    }

    @Test
    void testIsTheCompactStreamTaskCommitted_WhenIOExceptionThrown_PropagatesException() throws IOException {
        // Arrange
        CompactStreamTask task = createTask(100L, 0L, 1000L, 5000L, 2000L, "/path/to/file");

        try (MockedStatic<DeltaTableUtils> mockedUtils = mockStatic(DeltaTableUtils.class)) {

            mockedUtils.when(() -> DeltaTableUtils.getAddActionIterator(table.getLatestSnapshot(engine), engine))
                .thenThrow(new RuntimeException("Test Exception"));

            // Act & Assert
            IOException exception = assertThrows(IOException.class, () -> {
                committer.isTheCompactStreamTaskCommitted(task);
            });

            assertEquals("Failed to scan delta add actions", exception.getMessage());
            assertEquals("Test Exception", exception.getCause().getMessage());
        }
    }

    @Test
    void testIsTheCompactStreamTaskCommitted_WithEmptyFileList_ReturnsFalse() throws IOException {
        // Arrange
        CompactStreamTask task = createTask(100L, 0L, 1000L, 5000L, 2000L, "/path/to/file");

        try (MockedStatic<DeltaTableUtils> mockedUtils = mockStatic(DeltaTableUtils.class)) {
            mockedUtils.when(() -> DeltaTableUtils.getAddActionIterator(table.getLatestSnapshot(engine), engine))
                .thenReturn(CloseableIterators.empty());
            // Act
            boolean result = committer.isTheCompactStreamTaskCommitted(task);

            // Assert
            assertFalse(result);
        }
    }

    @Test
    void testIsTheCompactStreamTaskCommitted_WhenMixedValidAndInvalidTags_FindsValidMatch() throws IOException {
        // Arrange
        CompactStreamTask task = createTask(100L, 0L, 1000L, 5000L, 2000L, "/path/to/file");

        Map<String, String> invalidTags = new HashMap<>();
        invalidTags.put("streamId", "not_a_number");

        List<AddFileAction> files = Arrays.asList(
            createAddFileAction(invalidTags),
            createAddFileAction(null),
            createAddFileAction(createTags(100L, 0L, 1000L, 5000L, 2000L, "/path/to/file"))
        );

        CloseableIterator<Object> iterator = CloseableIterators.of(files.toArray());

        try (MockedStatic<DeltaTableUtils> mockedUtils = mockStatic(DeltaTableUtils.class)) {
            mockedUtils.when(() -> DeltaTableUtils.getAddActionIterator(table.getLatestSnapshot(engine), engine))
                .thenReturn(iterator);

            // Act
            boolean result = committer.isTheCompactStreamTaskCommitted(task);

            // Assert
            assertTrue(result);
            iterator.close();
        }
    }

    @Test
    void testIsTheCompactStreamTaskCommitted_WhenSameStreamIdButOtherFieldsDifferent_ReturnsFalse() throws IOException {
        // Arrange
        CompactStreamTask task = createTask(100L, 0L, 1000L, 5000L, 2000L, "/path/to/file");

        // Same streamId but all other fields different
        Map<String, String> tags = createTags(100L, 500L, 2000L, 9999L, 8888L, "/completely/different");
        AddFileAction file = createAddFileAction(tags);

        CloseableIterator<AddFileAction> iterator = CloseableIterators.singleton(file);

        try (MockedStatic<DeltaTableUtils> mockedUtils = mockStatic(DeltaTableUtils.class)) {
            mockedUtils.when(() -> DeltaTableUtils.getAddActionIterator(table.getLatestSnapshot(engine), engine))
                .thenReturn(iterator);

            // Act
            boolean result = committer.isTheCompactStreamTaskCommitted(task);

            // Assert
            assertFalse(result);
            iterator.close();
        }
    }

    @Test
    void testIsTheCompactStreamTaskCommitted_WhenOldTagsDoNotMatch_ScansAllEntries() throws IOException {
        // Arrange
        CompactStreamTask task = createTask(100L, 3000L, 4000L, 5000L, 2000L, "/path/to/file3");

        List<AddFileAction> files = Arrays.asList(
            createAddFileAction(createTags(200L, 0L, 1000L, 5000L, 2000L, "/path/to/file0")),
            createAddFileAction(createTags(100L, 2000L, 3000L, 5000L, 2000L, "/path/to/file2")),
            createAddFileAction(createTags(100L, 1000L, 2000L, 5000L, 2000L, "/path/to/file1")),
            createAddFileAction(createTags(100L, 0L, 1000L, 5000L, 2000L, "/path/to/file0"))
        );

        CloseableIterators.CountingCloseableIterator<Object> countingIterator =
            new CloseableIterators.CountingCloseableIterator<>(CloseableIterators.of(files.toArray()));

        try (MockedStatic<DeltaTableUtils> mockedUtils = mockStatic(DeltaTableUtils.class)) {
            mockedUtils.when(() -> DeltaTableUtils.getAddActionIterator(table.getLatestSnapshot(engine), engine))
                .thenReturn(countingIterator);
            // Act
            boolean result = committer.isTheCompactStreamTaskCommitted(task);

            // Assert
            assertFalse(result);

            assertEquals(4, countingIterator.getNextCount());
            countingIterator.close();
        }
    }

    @Test
    void testIsTheCompactStreamTaskCommitted_WhenOldTagsOutOfOrder_FindsLaterExactMatch() throws IOException {
        CompactStreamTask task = createTask(100L, 3000L, 4000L, 5000L, 2000L, "/path/to/file3");

        List<AddFileAction> files = Arrays.asList(
            createAddFileAction(createTags(100L, 0L, 1000L, 5000L, 2000L, "/path/to/file0")),
            createAddFileAction(createTags(100L, 3000L, 4000L, 5000L, 2000L, "/path/to/file3"))
        );

        CloseableIterator<Object> iterator = CloseableIterators.of(files.toArray());

        try (MockedStatic<DeltaTableUtils> mockedUtils = mockStatic(DeltaTableUtils.class)) {
            mockedUtils.when(() -> DeltaTableUtils.getAddActionIterator(table.getLatestSnapshot(engine), engine))
                .thenReturn(iterator);

            boolean result = committer.isTheCompactStreamTaskCommitted(task);

            assertTrue(result);
            iterator.close();
        }
    }

    @Test
    void testIsTheCompactStreamTaskCommitted_WhenTopicTagContainsInvalidNumber_ContinuesScanning() throws IOException {
        CompactStreamTask task = createTask(100L, 0L, 1000L, 5000L, 2000L, "/path/to/file");
        task.setTopic("test-topic");

        Map<String, String> invalidTopicTags = new HashMap<>();
        invalidTopicTags.put("topic", "test-topic");
        invalidTopicTags.put("streamId", "invalid_number");
        invalidTopicTags.put("endOffset", "1000");

        Map<String, String> validTopicTags = new HashMap<>();
        validTopicTags.put("topic", "test-topic");
        validTopicTags.put("streamId", "100");
        validTopicTags.put("endOffset", "1001");

        List<AddFileAction> files = Arrays.asList(
            createAddFileAction(invalidTopicTags),
            createAddFileAction(validTopicTags)
        );

        CloseableIterator<Object> iterator = CloseableIterators.of(files.toArray());

        try (MockedStatic<DeltaTableUtils> mockedUtils = mockStatic(DeltaTableUtils.class)) {
            mockedUtils.when(() -> DeltaTableUtils.getAddActionIterator(table.getLatestSnapshot(engine), engine))
                .thenReturn(iterator);

            boolean result = committer.isTheCompactStreamTaskCommitted(task);

            assertTrue(result);
            iterator.close();
        }
    }

    @Test
    void testIsTheCompactStreamTaskCommitted_WhenTopicTagsWithoutOrderTag_UsesMessageIdSemantics() throws IOException {
        CompactStreamTask task = createTask(5L, 50L, 100L, 5000L, 2000L, "/path/to/file");
        task.setTopic("test-topic");

        Map<String, String> topicTagsWithoutOrder = new HashMap<>();
        topicTagsWithoutOrder.put("topic", "test-topic");
        topicTagsWithoutOrder.put("streamId", "5");
        topicTagsWithoutOrder.put("endOffset", "200");

        CloseableIterator<AddFileAction> iterator =
            CloseableIterators.singleton(createAddFileAction(topicTagsWithoutOrder));

        try (MockedStatic<DeltaTableUtils> mockedUtils = mockStatic(DeltaTableUtils.class)) {
            mockedUtils.when(() -> DeltaTableUtils.getAddActionIterator(table.getLatestSnapshot(engine), engine))
                .thenReturn(iterator);

            boolean result = committer.isTheCompactStreamTaskCommitted(task);

            assertTrue(result);
            iterator.close();
        }
    }

    @Test
    void testIsTheCompactStreamTaskCommitted_WhenOrderedTopicTagsDoNotMatch_BreaksEarly() throws IOException {
        CompactStreamTask task = createTask(5L, 250L, 300L, 5000L, 2000L, "/path/to/file");
        task.setTopic("test-topic");

        Map<String, String> orderedTopicTags = new HashMap<>();
        orderedTopicTags.put("topic", "test-topic");
        orderedTopicTags.put("streamId", "5");
        orderedTopicTags.put("endOffset", "200");
        orderedTopicTags.put(DeltaTable.ORDER_TAG, "true");

        List<AddFileAction> files = Arrays.asList(
            createAddFileAction(orderedTopicTags),
            createAddFileAction(createTags(5L, 250L, 300L, 5000L, 2000L, "/path/to/file"))
        );

        CloseableIterators.CountingCloseableIterator<Object> countingIterator =
            new CloseableIterators.CountingCloseableIterator<>(CloseableIterators.of(files.toArray()));

        try (MockedStatic<DeltaTableUtils> mockedUtils = mockStatic(DeltaTableUtils.class)) {
            mockedUtils.when(() -> DeltaTableUtils.getAddActionIterator(table.getLatestSnapshot(engine), engine))
                .thenReturn(countingIterator);

            boolean result = committer.isTheCompactStreamTaskCommitted(task);

            assertFalse(result);
            assertEquals(1, countingIterator.getNextCount());
            countingIterator.close();
        }
    }

    @Test
    void testIsTheCompactStreamTaskCommitted_WhenOrderedTopicTagsMatch_BreaksEarly() throws IOException {
        CompactStreamTask task = createTask(5L, 50L, 100L, 5000L, 2000L, "/path/to/file");
        task.setTopic("test-topic");

        Map<String, String> orderedTopicTags = new HashMap<>();
        orderedTopicTags.put("topic", "test-topic");
        orderedTopicTags.put("streamId", "5");
        orderedTopicTags.put("endOffset", "200");
        orderedTopicTags.put(DeltaTable.ORDER_TAG, "true");

        List<AddFileAction> files = Arrays.asList(
            createAddFileAction(orderedTopicTags),
            createAddFileAction(createTags(5L, 50L, 100L, 5000L, 2000L, "/path/to/file"))
        );

        CloseableIterators.CountingCloseableIterator<Object> countingIterator =
            new CloseableIterators.CountingCloseableIterator<>(CloseableIterators.of(files.toArray()));

        try (MockedStatic<DeltaTableUtils> mockedUtils = mockStatic(DeltaTableUtils.class)) {
            mockedUtils.when(() -> DeltaTableUtils.getAddActionIterator(table.getLatestSnapshot(engine), engine))
                .thenReturn(countingIterator);

            boolean result = committer.isTheCompactStreamTaskCommitted(task);

            assertTrue(result);
            assertEquals(1, countingIterator.getNextCount());
            countingIterator.close();
        }
    }

    public static Schema createSchemaV1() {
        Schema locationSchema = SchemaBuilder.record("locationRecord")
            .namespace("testNamespace")
            .fields()
            .requiredInt("id")
            .requiredLong("x")
            .requiredLong("y")
            .endRecord();

        return SchemaBuilder.record("testRecord")
            .namespace("testNamespace")
            .fields()
            .requiredInt("id")
            .requiredString("name")
            .name("location").type(locationSchema).noDefault()
            .endRecord();
    }


    public static Schema createSchemaV2() {
        //location x from require to optional
        //add new field location z. and reorder the fields
        Schema locationSchemaV2 = SchemaBuilder.record("locationRecord")
            .namespace("testNamespace")
            .fields()
            .name("z")
            .type()
            .nullable()
            .longType().longDefault(0L)
            .requiredLong("y")
            .optionalLong("x")
            .requiredInt("id")
            .endRecord();

        //add new field status
        //remove old field name
        //reorder the fields
        return SchemaBuilder.record("testRecord")
            .namespace("testNamespace")
            .fields()
            .name("status")
            .type()
            .nullable()
            .stringType()
            .stringDefault("active")
            .name("location").type(locationSchemaV2).noDefault()
            .requiredInt("id")
            .endRecord();
    }

    public static Schema createSchemaV3() {
        Schema locationSchema = SchemaBuilder.record("locationRecord")
            .namespace("testNamespace")
            .fields()
            .optionalInt("id")
            .requiredInt("x")
            .requiredInt("y")
            .endRecord();


        Schema nullableLocationSchema = Schema.createUnion(Schema.create(Schema.Type.NULL), locationSchema);
        return SchemaBuilder.record("testRecord")
            .namespace("testNamespace")
            .fields()
            .requiredInt("id")
            .optionalString("name")
            .name("location").type(nullableLocationSchema).withDefault(null)
            .endRecord();
    }

    public static Schema createSchemaV4() {
        Schema locationSchema = SchemaBuilder.record("locationRecord")
            .namespace("testNamespace")
            .fields()
            .requiredInt("id")
            .requiredLong("x")
            .requiredLong("y")
            .endRecord();

        return SchemaBuilder.record("testRecord")
            .namespace("testNamespace")
            .fields()
            .requiredInt("id")
            .requiredString("name")
            //V4 add a new required field 'age', it's not allowed.
            .requiredInt("age")
            .name("location").type(locationSchema).noDefault()
            .endRecord();
    }

    @Test
    public void testSchemaEvolutionForOldTableWithSoftDeleteDisabled()
        throws SchemaMappingException, SchemaEvolutionException {
        testSchemaEvolutionForOldTable(false);
    }

    @Test
    public void testSchemaEvolutionForOldTableWithSoftDeleteEnabled()
        throws SchemaMappingException, SchemaEvolutionException {
        testSchemaEvolutionForOldTable(true);
    }

    private void testSchemaEvolutionForOldTable(boolean softDeleteEnabled)
        throws SchemaMappingException, SchemaEvolutionException {
        //The table is created by the old kernel.
        //The table schema {id, name, age, location{x,y}}
        String topic = "test_topic";
        // The legacy fixture includes an extra namespace directory; use its contents as the
        // storage root for this unqualified topic.
        Path sourceTablePath = Path.of("src/test/resources/test_delta_table/public");
        Path testTablePath = path.resolve("test_delta_table_" + softDeleteEnabled);
        copyDirectory(sourceTablePath, testTablePath);
        String location = testTablePath.toString();
        Properties properties = new Properties();
        properties.put("storagePath", location);
        properties.put(DeltaTable.SCHEMA_EVOLUTION_SOFT_DELETE_ENABLED, String.valueOf(softDeleteEnabled));
        LakehouseConfiguration lakehouseConfiguration = new LakehouseConfiguration(properties);
        ManagedDeltaTable deltaManagedTable =
            new ManagedDeltaTable(lakehouseConfiguration, topic);

        Table table = deltaManagedTable.getTable();
        Engine engine = deltaManagedTable.getEngine();
        {
            Snapshot latestSnapshot = table.getLatestSnapshot(engine);
            StructType tableSchema = latestSnapshot.getSchema();

            StructField idField = tableSchema.get("id");
            assertFalse(idField.isNullable());
            assertInstanceOf(IntegerType.class, idField.getDataType());

            StructField nameField = tableSchema.get("name");
            assertTrue(nameField.isNullable());
            assertInstanceOf(StringType.class, nameField.getDataType());

            StructField ageField = tableSchema.get("age");
            assertFalse(ageField.isNullable());
            assertInstanceOf(IntegerType.class, ageField.getDataType());

            StructField locationField = tableSchema.get("location");
            assertTrue(locationField.isNullable());
            assertInstanceOf(StructType.class, locationField.getDataType());

            StructType locationSchema = (StructType) locationField.getDataType();

            StructField locationXField = locationSchema.get("x");
            assertFalse(locationXField.isNullable());
            assertInstanceOf(IntegerType.class, locationXField.getDataType());

            StructField locationYField = locationSchema.get("y");
            assertFalse(locationYField.isNullable());
            assertInstanceOf(IntegerType.class, locationYField.getDataType());
        }
        //schemaV3 removed field age. add field location#id
        Schema schemaV3 = createSchemaV3();
        deltaManagedTable.evolveSchemaWithVersion(1, AvroSchemaUtilExtended.toDelta(schemaV3, false));

        Set<Long> schemaMapping = deltaManagedTable.getSchemaMapping();
        assertEquals(1, schemaMapping.size());
        assertTrue(schemaMapping.contains(1L));

        {
            Snapshot latestSnapshot = table.getLatestSnapshot(engine);
            StructType tableSchema = latestSnapshot.getSchema();

            StructField idField = tableSchema.get("id");
            assertFalse(idField.isNullable());
            assertInstanceOf(IntegerType.class, idField.getDataType());

            StructField nameField = tableSchema.get("name");
            assertTrue(nameField.isNullable());
            assertInstanceOf(StringType.class, nameField.getDataType());

            int ageIndex = tableSchema.indexOf("age");
            if (softDeleteEnabled) {
                assertTrue(ageIndex >= 0);
                StructField ageField = tableSchema.get("age");
                assertNotNull(ageField);
                assertTrue(ageField.isNullable());
                assertInstanceOf(IntegerType.class, ageField.getDataType());
            } else {
                assertEquals(-1, ageIndex);
            }

            StructField locationField = tableSchema.get("location");
            assertTrue(locationField.isNullable());
            assertInstanceOf(StructType.class, locationField.getDataType());

            StructType locationSchema = (StructType) locationField.getDataType();

            StructField locationIdField = locationSchema.get("id");
            assertTrue(locationIdField.isNullable());
            assertInstanceOf(IntegerType.class, locationIdField.getDataType());

            StructField locationXField = locationSchema.get("x");
            assertFalse(locationXField.isNullable());
            assertInstanceOf(IntegerType.class, locationXField.getDataType());

            StructField locationYField = locationSchema.get("y");
            assertFalse(locationYField.isNullable());
            assertInstanceOf(IntegerType.class, locationYField.getDataType());
        }
    }

    @Test
    public void testDeltaCommiterSchemaEvolutionWithSoftDeleteDisabled() throws Exception {
        testDeltaCommiterSchemaEvolution(false);
    }

    @Test
    public void testDeltaCommiterSchemaEvolutionWithSoftDeleteEnabled() throws Exception {
        testDeltaCommiterSchemaEvolution(true);
    }

    @Test
    void testDeltaCommitSucceedsButReadFailsForRequiredFieldValueNull() {
        StructType deltaSchema = new StructType(List.of(
            new StructField("SequenceId", LongType.LONG, false)));
        GenericRow row = new GenericRow(deltaSchema, new HashMap<>());
        row.put(deltaSchema.indexOf("SequenceId"), null);

        ParquetDecodingException error =
            assertThrows(ParquetDecodingException.class, () -> writeCommitAndLoadDeltaRows(deltaSchema, row));

        assertTrue(error.getMessage().contains("Can not read value"));
    }

    @Test
    void testDeltaCommitSucceedsButReadFailsForMapKeyNull() {
        StructType deltaSchema = new StructType(List.of(
            new StructField("Metadata",
                new MapType(StringType.STRING, LongType.LONG, true), false)));
        Map<Object, Object> metadata = new HashMap<>();
        metadata.put(null, 123L);

        GenericRow row = new GenericRow(deltaSchema, new HashMap<>());
        row.put(deltaSchema.indexOf("Metadata"), new MapValueImpl(metadata, StringType.STRING, LongType.LONG));

        ParquetDecodingException error =
            assertThrows(ParquetDecodingException.class, () -> writeCommitAndLoadDeltaRows(deltaSchema, row));

        assertTrue(error.getMessage().contains("Can not read value"));
    }

    @Test
    void testDeltaCommitAndReadMapValueNull() throws Exception {
        StructType deltaSchema = new StructType(List.of(
            new StructField("Metadata",
                new MapType(StringType.STRING, LongType.LONG, true), false)));
        Map<Object, Object> metadata = new HashMap<>();
        metadata.put("ActorId", null);
        metadata.put("MessageId", 123L);

        GenericRow row = new GenericRow(deltaSchema, new HashMap<>());
        row.put(deltaSchema.indexOf("Metadata"), new MapValueImpl(metadata, StringType.STRING, LongType.LONG));

        List<Row> rows = writeCommitAndLoadDeltaRows(deltaSchema, row);

        assertEquals(1, rows.size());
        MapValue result = rows.get(0).getMap(rows.get(0).getSchema().indexOf("Metadata"));
        assertEquals(2, result.getSize());

        boolean foundNullValue = false;
        boolean foundMessageId = false;
        for (int i = 0; i < result.getSize(); i++) {
            String key = result.getKeys().getString(i);
            if ("ActorId".equals(key)) {
                assertTrue(result.getValues().isNullAt(i));
                foundNullValue = true;
            } else if ("MessageId".equals(key)) {
                assertEquals(123L, result.getValues().getLong(i));
                foundMessageId = true;
            }
        }
        assertTrue(foundNullValue);
        assertTrue(foundMessageId);
    }

    private void testDeltaCommiterSchemaEvolution(boolean softDeleteEnabled) throws Exception {
        ManagedDeltaTable managedTable = createSchemaEvolutionTestTable(softDeleteEnabled);

        Table table = managedTable.getTable();
        Engine engine = managedTable.getEngine();

        Schema schemaV1 = createSchemaV1();
        managedTable.createDeltaTable(null, AvroSchemaUtilExtended.toDelta(schemaV1, false));

        Set<Long> schemaMapping = managedTable.getSchemaMapping();
        assertEquals(0, schemaMapping.size());

        {
            Snapshot latestSnapshot = table.getLatestSnapshot(engine);
            StructType tableSchema = latestSnapshot.getSchema();

            StructField idField = tableSchema.get("id");
            assertFalse(idField.isNullable());
            assertInstanceOf(IntegerType.class, idField.getDataType());

            StructField nameField = tableSchema.get("name");
            assertFalse(nameField.isNullable());
            assertInstanceOf(StringType.class, nameField.getDataType());

            StructField locationField = tableSchema.get("location");
            assertFalse(locationField.isNullable());
            assertInstanceOf(StructType.class, locationField.getDataType());

            StructType locationSchema = (StructType) locationField.getDataType();

            StructField locationIdField = locationSchema.get("id");
            assertFalse(locationIdField.isNullable());
            assertInstanceOf(IntegerType.class, locationIdField.getDataType());

            StructField locationXField = locationSchema.get("x");
            assertFalse(locationXField.isNullable());
            assertInstanceOf(LongType.class, locationXField.getDataType());

            StructField locationYField = locationSchema.get("y");
            assertFalse(locationYField.isNullable());
            assertInstanceOf(LongType.class, locationYField.getDataType());
        }

        String location = managedTable.getTableLocation();
        List<ParquetFileStat> v1File = writeV1Data(location, schemaV1);
        managedTable.commit(v1File);

        {
            Snapshot latestSnapshot = table.getLatestSnapshot(engine);
            List<Row> rows = DeltaTableUtils.loadData(engine, latestSnapshot);
            assertEquals(100, rows.size());

            int i = 0;
            for (Row row : rows) {
                StructType schema = row.getSchema();
                int id = row.getInt(schema.indexOf("id"));
                assertEquals(i, id);

                String name = row.getString(schema.indexOf("name"));
                assertEquals("name" + i, name);

                Row locationRow = row.getStruct(schema.indexOf("location"));

                StructType locationStruct = locationRow.getSchema();

                int locationId = locationRow.getInt(locationStruct.indexOf("id"));
                assertEquals(i, locationId);

                long locationX = locationRow.getLong(locationStruct.indexOf("x"));
                assertEquals(i, locationX);

                long locationY = locationRow.getLong(locationStruct.indexOf("y"));
                assertEquals(i + 1, locationY);
                i++;
            }
        }

        Schema schemaV2 = createSchemaV2();
        managedTable.evolveSchemaWithVersion(1L, AvroSchemaUtilExtended.toDelta(schemaV2, false));

        schemaMapping = managedTable.getSchemaMapping();
        assertEquals(1, schemaMapping.size());
        assertTrue(schemaMapping.contains(1L));

        {
            Snapshot latestSnapshot = table.getLatestSnapshot(engine);
            StructType tableSchema = latestSnapshot.getSchema();

            StructField idField = tableSchema.get("id");
            assertFalse(idField.isNullable());
            assertInstanceOf(IntegerType.class, idField.getDataType());

            int nameIndex = tableSchema.indexOf("name");
            if (softDeleteEnabled) {
                assertTrue(nameIndex >= 0);
                StructField nameField = tableSchema.get("name");
                assertNotNull(nameField);
                assertTrue(nameField.isNullable());
                assertInstanceOf(StringType.class, nameField.getDataType());
            } else {
                assertEquals(-1, nameIndex);
            }

            StructField statusField = tableSchema.get("status");
            assertTrue(statusField.isNullable());
            assertInstanceOf(StringType.class, statusField.getDataType());

            StructField locationField = tableSchema.get("location");
            assertFalse(locationField.isNullable());
            assertInstanceOf(StructType.class, locationField.getDataType());

            StructType locationSchema = (StructType) locationField.getDataType();

            StructField locationIdField = locationSchema.get("id");
            assertFalse(locationIdField.isNullable());
            assertInstanceOf(IntegerType.class, locationIdField.getDataType());

            StructField locationXField = locationSchema.get("x");
            assertTrue(locationXField.isNullable());
            assertInstanceOf(LongType.class, locationXField.getDataType());

            StructField locationYField = locationSchema.get("y");
            assertFalse(locationYField.isNullable());
            assertInstanceOf(LongType.class, locationYField.getDataType());

            StructField locationZField = locationSchema.get("z");
            assertTrue(locationZField.isNullable());
            assertInstanceOf(LongType.class, locationZField.getDataType());
        }

        List<ParquetFileStat> v2File = writeV2Data(location, schemaV2);
        managedTable.commit(v2File);

        {
            Snapshot latestSnapshot = table.getLatestSnapshot(engine);
            List<Row> rows = DeltaTableUtils.loadData(engine, latestSnapshot);
            assertEquals(200, rows.size());

            int index = 100;
            int index1 = 0;
            for (Row row : rows) {
                StructType schema = row.getSchema();
                int id = row.getInt(schema.indexOf("id"));
                //The v2 data
                if (id >= 100) {
                    int i = index;
                    assertEquals(i, id);

                    int nameIndex = schema.indexOf("name");
                    if (softDeleteEnabled) {
                        assertTrue(nameIndex >= 0);
                        assertNull(row.getString(nameIndex));
                    } else {
                        assertEquals(-1, nameIndex);
                    }

                    String status = row.getString(schema.indexOf("status"));
                    if (i % 2 == 0) {
                        assertEquals("inactive", status);
                    } else {
                        assertNull(status);
                    }

                    Row locationRow = row.getStruct(schema.indexOf("location"));

                    StructType locationStruct = locationRow.getSchema();

                    int locationId = locationRow.getInt(locationStruct.indexOf("id"));
                    assertEquals(i, locationId);

                    long locationX = locationRow.getLong(locationStruct.indexOf("x"));
                    assertEquals(0L, locationX);

                    long locationY = locationRow.getLong(locationStruct.indexOf("y"));
                    assertEquals(i + 1, locationY);

                    long locationZ = locationRow.getLong(locationStruct.indexOf("z"));
                    assertEquals(i + 2, locationZ);
                    index++;
                } else {
                    int i = index1;
                    //The v1 data
                    assertEquals(i, id);

                    int nameIndex = schema.indexOf("name");
                    if (softDeleteEnabled) {
                        assertTrue(nameIndex >= 0);
                        assertEquals("name" + i, row.getString(nameIndex));
                    } else {
                        assertEquals(-1, nameIndex);
                    }

                    String status = row.getString(schema.indexOf("status"));
                    assertNull(status);

                    Row locationRow = row.getStruct(schema.indexOf("location"));

                    StructType locationStruct = locationRow.getSchema();

                    int locationId = locationRow.getInt(locationStruct.indexOf("id"));
                    assertEquals(i, locationId);

                    long locationX = locationRow.getLong(locationStruct.indexOf("x"));
                    assertEquals(i, locationX);

                    long locationY = locationRow.getLong(locationStruct.indexOf("y"));
                    assertEquals(i + 1, locationY);

                    try {
                        locationRow.getLong(locationStruct.indexOf("z"));
                        fail();
                    } catch (Exception e) {
                        assertInstanceOf(NullPointerException.class, e);
                    }
                    index1++;
                }
            }
        }
    }

    @Test
    public void testDeltaCommiterSchemaEvolutionCommitPreviousDataWithSoftDeleteDisabled() throws Exception {
        testDeltaCommiterSchemaEvolutionCommitPreviousData(false);
    }

    @Test
    public void testDeltaCommiterSchemaEvolutionCommitPreviousDataWithSoftDeleteEnabled() throws Exception {
        testDeltaCommiterSchemaEvolutionCommitPreviousData(true);
    }

    private void testDeltaCommiterSchemaEvolutionCommitPreviousData(boolean softDeleteEnabled) throws Exception {
        ManagedDeltaTable managedTable = createSchemaEvolutionTestTable(softDeleteEnabled);

        Table table = managedTable.getTable();
        Engine engine = managedTable.getEngine();

        Schema schemaV1 = createSchemaV1();
        managedTable.createDeltaTable(null, AvroSchemaUtilExtended.toDelta(schemaV1, false));

        Set<Long> schemaMapping = managedTable.getSchemaMapping();
        assertEquals(0, schemaMapping.size());

        Schema schemaV2 = createSchemaV2();
        managedTable.evolveSchemaWithVersion(1L, AvroSchemaUtilExtended.toDelta(schemaV2, false));

        schemaMapping = managedTable.getSchemaMapping();
        assertEquals(1, schemaMapping.size());
        assertTrue(schemaMapping.contains(1L));

        String location = managedTable.getTableLocation();
        //Write v2 data first
        List<ParquetFileStat> v2File = writeV2Data(location, schemaV2);
        managedTable.commit(v2File);

        {
            Snapshot latestSnapshot = table.getLatestSnapshot(engine);
            List<Row> rows = DeltaTableUtils.loadData(engine, latestSnapshot);
            assertEquals(100, rows.size());

            int index = 100;
            for (Row row : rows) {
                StructType schema = row.getSchema();
                int id = row.getInt(schema.indexOf("id"));
                //The v2 data
                if (id >= 100) {
                    int i = index;
                    assertEquals(i, id);

                    int nameIndex = schema.indexOf("name");
                    if (softDeleteEnabled) {
                        assertTrue(nameIndex >= 0);
                        assertNull(row.getString(nameIndex));
                    } else {
                        assertEquals(-1, nameIndex);
                    }

                    String status = row.getString(schema.indexOf("status"));
                    if (i % 2 == 0) {
                        assertEquals("inactive", status);
                    } else {
                        assertNull(status);
                    }

                    Row locationRow = row.getStruct(schema.indexOf("location"));

                    StructType locationStruct = locationRow.getSchema();

                    int locationId = locationRow.getInt(locationStruct.indexOf("id"));
                    assertEquals(i, locationId);

                    long locationX = locationRow.getLong(locationStruct.indexOf("x"));
                    assertEquals(0L, locationX);

                    long locationY = locationRow.getLong(locationStruct.indexOf("y"));
                    assertEquals(i + 1, locationY);

                    long locationZ = locationRow.getLong(locationStruct.indexOf("z"));
                    assertEquals(i + 2, locationZ);
                    index++;
                }
            }
        }

        //Write previous v1 data
        List<ParquetFileStat> v1File = writeV1Data(location, schemaV1);
        managedTable.commit(v1File);

        {
            Snapshot latestSnapshot = table.getLatestSnapshot(engine);
            List<Row> rows = DeltaTableUtils.loadData(engine, latestSnapshot);
            assertEquals(200, rows.size());

            int index = 100;
            int index1 = 0;
            for (Row row : rows) {
                StructType schema = row.getSchema();
                int id = row.getInt(schema.indexOf("id"));
                //The v2 data
                if (id >= 100) {
                    int i = index;
                    assertEquals(i, id);

                    int nameIndex = schema.indexOf("name");
                    if (softDeleteEnabled) {
                        assertTrue(nameIndex >= 0);
                        assertTrue(row.isNullAt(nameIndex));
                    } else {
                        assertEquals(-1, nameIndex);
                    }

                    String status = row.getString(schema.indexOf("status"));
                    if (i % 2 == 0) {
                        assertEquals("inactive", status);
                    } else {
                        assertNull(status);
                    }

                    Row locationRow = row.getStruct(schema.indexOf("location"));

                    StructType locationStruct = locationRow.getSchema();

                    int locationId = locationRow.getInt(locationStruct.indexOf("id"));
                    assertEquals(i, locationId);

                    long locationX = locationRow.getLong(locationStruct.indexOf("x"));
                    assertEquals(0L, locationX);

                    long locationY = locationRow.getLong(locationStruct.indexOf("y"));
                    assertEquals(i + 1, locationY);

                    long locationZ = locationRow.getLong(locationStruct.indexOf("z"));
                    assertEquals(i + 2, locationZ);
                    index++;
                } else {
                    int i = index1;
                    //The v1 data
                    assertEquals(i, id);

                    int nameIndex = schema.indexOf("name");
                    if (softDeleteEnabled) {
                        assertTrue(nameIndex >= 0);
                        assertEquals("name" + i, row.getString(nameIndex));
                    } else {
                        assertEquals(-1, nameIndex);
                    }

                    String status = row.getString(schema.indexOf("status"));
                    assertNull(status);

                    Row locationRow = row.getStruct(schema.indexOf("location"));

                    StructType locationStruct = locationRow.getSchema();

                    int locationId = locationRow.getInt(locationStruct.indexOf("id"));
                    assertEquals(i, locationId);

                    long locationX = locationRow.getLong(locationStruct.indexOf("x"));
                    assertEquals(i, locationX);

                    long locationY = locationRow.getLong(locationStruct.indexOf("y"));
                    assertEquals(i + 1, locationY);

                    try {
                        locationRow.getLong(locationStruct.indexOf("z"));
                        fail();
                    } catch (Exception e) {
                        assertInstanceOf(NullPointerException.class, e);
                    }
                    index1++;
                }
            }
        }
    }

    @Test
    public void testDeltaCommiterSchemaEvolutionWithConflictCase() throws Exception {
        Properties properties = new Properties();
        properties.put("storagePath", path.toString());
        properties.put("partitionKey", "none");
        properties.put("make-new-fields-optional", "true");
        properties.put(TableConfig.COLUMN_MAPPING_MODE.getKey(), "id");
        LakehouseConfiguration lakehouseConfiguration = new LakehouseConfiguration(properties);
        String topic = "testTopic" + UUID.randomUUID();
        ManagedDeltaTable managedDeltaCommiter =
            new ManagedDeltaTable(lakehouseConfiguration, topic);

        Table table = managedDeltaCommiter.getTable();
        Engine engine = managedDeltaCommiter.getEngine();

        Schema schemaV1 = createSchemaV1();
        managedDeltaCommiter.createDeltaTable(null, AvroSchemaUtilExtended.toDelta(schemaV1, false));

        {
            Snapshot latestSnapshot = table.getLatestSnapshot(engine);
            StructType tableSchema = latestSnapshot.getSchema();

            StructField idField = tableSchema.get("id");
            assertFalse(idField.isNullable());
            assertInstanceOf(IntegerType.class, idField.getDataType());

            StructField nameField = tableSchema.get("name");
            assertFalse(nameField.isNullable());
            assertInstanceOf(StringType.class, nameField.getDataType());

            StructField locationField = tableSchema.get("location");
            assertFalse(locationField.isNullable());
            assertInstanceOf(StructType.class, locationField.getDataType());

            StructType locationSchema = (StructType) locationField.getDataType();

            StructField locationIdField = locationSchema.get("id");
            assertFalse(locationIdField.isNullable());
            assertInstanceOf(IntegerType.class, locationIdField.getDataType());

            StructField locationXField = locationSchema.get("x");
            assertFalse(locationXField.isNullable());
            assertInstanceOf(LongType.class, locationXField.getDataType());

            StructField locationYField = locationSchema.get("y");
            assertFalse(locationYField.isNullable());
            assertInstanceOf(LongType.class, locationYField.getDataType());
        }

        //V4 adds a new required field 'age'; with make-new-fields-optional (default on) it is added
        //as nullable instead of being rejected.
        Schema schemaV4 = createSchemaV4();
        managedDeltaCommiter.evolveSchemaWithVersion(1L, AvroSchemaUtilExtended.toDelta(schemaV4, false));

        StructField ageField = table.getLatestSnapshot(engine).getSchema().get("age");
        assertNotNull(ageField);
        assertTrue(ageField.isNullable());
        assertInstanceOf(IntegerType.class, ageField.getDataType());
    }

    @Test
    void testDeltaCommitterSchemaEvolutionDeletesMissingField() throws Exception {
        ManagedDeltaTable managedTable = createSchemaEvolutionTestTable(false);

        StructType schemaV1 = new StructType()
            .add(new StructField("id", LongType.LONG, false))
            .add(new StructField("name", StringType.STRING, true));
        managedTable.createDeltaTable(1L, schemaV1);

        StructType schemaV2 = new StructType()
            .add(new StructField("id", LongType.LONG, false));
        managedTable.evolveSchemaWithVersion(2L, schemaV2);

        StructType resultSchema = managedTable.getLatestSnapshot().getSchema();
        assertEquals(1, resultSchema.length());
        assertEquals(-1, resultSchema.indexOf("name"));
        assertEquals(Set.of(1L, 2L), managedTable.getSchemaMapping());
    }

    @Test
    void testDeltaCommitterSchemaEvolutionTreatsRenameAsDropAndAdd() throws Exception {
        ManagedDeltaTable managedTable = createSchemaEvolutionTestTable(false);

        StructType schemaV1 = new StructType()
            .add(new StructField("id", LongType.LONG, false))
            .add(new StructField("name", StringType.STRING, true));
        managedTable.createDeltaTable(1L, schemaV1);

        StructType schemaV2 = new StructType()
            .add(new StructField("id", LongType.LONG, false))
            .add(new StructField("full_name", StringType.STRING, true));
        managedTable.evolveSchemaWithVersion(2L, schemaV2);

        StructType resultSchema = managedTable.getLatestSnapshot().getSchema();
        assertEquals(2, resultSchema.length());
        assertEquals(-1, resultSchema.indexOf("name"));
        assertTrue(resultSchema.indexOf("full_name") >= 0);
    }

    @Test
    void testDeltaCommitterSchemaEvolutionRejectsIncompatibleTypeChange() throws Exception {
        ManagedDeltaTable managedTable = createSchemaEvolutionTestTable(true);

        StructType schemaV1 = new StructType()
            .add(new StructField("id", LongType.LONG, false))
            .add(new StructField("value", StringType.STRING, true));
        managedTable.createDeltaTable(1L, schemaV1);

        StructType schemaV2 = new StructType()
            .add(new StructField("id", LongType.LONG, false))
            .add(new StructField("value", IntegerType.INTEGER, true));

        assertThrows(SchemaEvolutionException.class, () -> managedTable.evolveSchemaWithVersion(2L, schemaV2));
    }

    @Test
    void testDeltaCommitterSchemaEvolutionTypePromotionIntToLongAutoEnablesTypeWidening() throws Exception {
        Properties properties = new Properties();
        properties.put("storagePath", path.toString());
        properties.put("partitionKey", "none");
        LakehouseConfiguration lakehouseConfiguration = new LakehouseConfiguration(properties);
        ManagedDeltaTable managedTable =
            new ManagedDeltaTable(lakehouseConfiguration, "testTopic" + UUID.randomUUID());

        StructType schemaV1 = new StructType()
            .add(new StructField("id", LongType.LONG, false))
            .add(new StructField("count", IntegerType.INTEGER, true));
        managedTable.createDeltaTable(1L, schemaV1);

        SnapshotImpl createdSnapshot = (SnapshotImpl) managedTable.getLatestSnapshot();
        assertNull(createdSnapshot.getMetadata().getConfiguration().get(TableConfig.TYPE_WIDENING_ENABLED.getKey()));

        StructType schemaV2 = new StructType()
            .add(new StructField("id", LongType.LONG, false))
            .add(new StructField("count", LongType.LONG, true));

        managedTable.evolveSchemaWithVersion(2L, schemaV2);

        SnapshotImpl latestSnapshot = (SnapshotImpl) managedTable.getLatestSnapshot();
        Map<String, String> configuration = latestSnapshot.getMetadata().getConfiguration();
        assertEquals("true", configuration.get(TableConfig.TYPE_WIDENING_ENABLED.getKey()));
        assertEquals("name", configuration.get(TableConfig.COLUMN_MAPPING_MODE.getKey()));
        assertEquals(Set.of(1L, 2L), managedTable.getSchemaMapping());

        StructType latestSchema = latestSnapshot.getSchema();
        assertEquals(2, latestSchema.length());
        assertInstanceOf(LongType.class, latestSchema.get("count").getDataType());
    }

    @Test
    void testDeltaCommitterSchemaEvolutionAddsNestedStructField() throws Exception {
        ManagedDeltaTable managedTable = createSchemaEvolutionTestTable(true);

        StructType profileV1 = new StructType()
            .add(new StructField("name", StringType.STRING, true));
        StructType schemaV1 = new StructType()
            .add(new StructField("id", LongType.LONG, false))
            .add(new StructField("profile", profileV1, true));
        managedTable.createDeltaTable(1L, schemaV1);

        StructType profileV2 = new StructType()
            .add(new StructField("name", StringType.STRING, true))
            .add(new StructField("email", StringType.STRING, true));
        StructType schemaV2 = new StructType()
            .add(new StructField("id", LongType.LONG, false))
            .add(new StructField("profile", profileV2, true));
        managedTable.evolveSchemaWithVersion(2L, schemaV2);

        StructType resultSchema = managedTable.getLatestSnapshot().getSchema();
        StructType resultProfile = (StructType) resultSchema.get("profile").getDataType();
        assertEquals(2, resultProfile.length());
        assertTrue(resultProfile.indexOf("email") >= 0);
    }

    @Test
    void testDeltaCommitterSchemaEvolutionSoftDeleteKeepsMissingField() throws Exception {
        ManagedDeltaTable managedTable = createSchemaEvolutionTestTable(true);

        StructType schemaV1 = new StructType()
            .add(new StructField("id", LongType.LONG, false))
            .add(new StructField("name", StringType.STRING, false));
        managedTable.createDeltaTable(1L, schemaV1);

        StructType schemaV2 = new StructType()
            .add(new StructField("id", LongType.LONG, false))
            .add(new StructField("email", StringType.STRING, true));
        managedTable.evolveSchemaWithVersion(2L, schemaV2);

        StructType resultSchema = managedTable.getLatestSnapshot().getSchema();
        StructField nameField = resultSchema.get("name");
        assertEquals(3, resultSchema.length());
        assertTrue(resultSchema.indexOf("name") >= 0);
        assertTrue(resultSchema.indexOf("email") >= 0);
        assertTrue(nameField.isNullable());
        assertEquals(Set.of(1L, 2L), managedTable.getSchemaMapping());
    }

    @Test
    void testDeltaCommitterSchemaEvolutionSoftDeleteKeepsMissingNestedField() throws Exception {
        ManagedDeltaTable managedTable = createSchemaEvolutionTestTable(true);

        StructType profileV1 = new StructType()
            .add(new StructField("name", StringType.STRING, false))
            .add(new StructField("phone", StringType.STRING, true));
        StructType schemaV1 = new StructType()
            .add(new StructField("id", LongType.LONG, false))
            .add(new StructField("profile", profileV1, true));
        managedTable.createDeltaTable(1L, schemaV1);

        StructType profileV2 = new StructType()
            .add(new StructField("phone", StringType.STRING, true))
            .add(new StructField("email", StringType.STRING, true));
        StructType schemaV2 = new StructType()
            .add(new StructField("id", LongType.LONG, false))
            .add(new StructField("profile", profileV2, true));
        managedTable.evolveSchemaWithVersion(2L, schemaV2);

        StructType resultSchema = managedTable.getLatestSnapshot().getSchema();
        StructType resultProfile = (StructType) resultSchema.get("profile").getDataType();
        StructField nameField = resultProfile.get("name");
        assertEquals(3, resultProfile.length());
        assertTrue(resultProfile.indexOf("name") >= 0);
        assertTrue(resultProfile.indexOf("phone") >= 0);
        assertTrue(resultProfile.indexOf("email") >= 0);
        assertTrue(nameField.isNullable());
        assertEquals(Set.of(1L, 2L), managedTable.getSchemaMapping());
    }

    private ManagedDeltaTable createSchemaEvolutionTestTable(boolean softDeleteEnabled) {
        Properties properties = new Properties();
        properties.put("storagePath", path.toString());
        properties.put("partitionKey", "none");
        properties.put(TableConfig.COLUMN_MAPPING_MODE.getKey(), "id");
        properties.put(DeltaTable.SCHEMA_EVOLUTION_SOFT_DELETE_ENABLED, String.valueOf(softDeleteEnabled));
        LakehouseConfiguration lakehouseConfiguration = new LakehouseConfiguration(properties);
        return new ManagedDeltaTable(lakehouseConfiguration, "testTopic" + UUID.randomUUID());
    }

    private List<Row> writeCommitAndLoadDeltaRows(StructType deltaSchema, GenericRow row) throws Exception {
        ManagedDeltaTable managedTable = createSchemaEvolutionTestTable(false);
        managedTable.createDeltaTable(null, deltaSchema);

        ParquetRowWriter parquetRowWriter = new ParquetRowWriter(
            managedTable.getTableLocation(), new Configuration(), Collections.emptyList(), deltaSchema, 1000);
        parquetRowWriter.write(row);
        List<ParquetFileStat> fileStats = parquetRowWriter.close();
        managedTable.commit(fileStats);

        return DeltaTableUtils.loadData(managedTable.getEngine(), managedTable.getLatestSnapshot());
    }

    private List<ParquetFileStat> writeV1Data(String location, Schema schema) throws IOException {
        StructType deltaSchema = AvroSchemaUtilExtended.toDelta(schema, false);
        Configuration hadoopConfig = new Configuration();
        ParquetRowWriter parquetRowWriter =
            new ParquetRowWriter(location, hadoopConfig, Collections.emptyList(), deltaSchema, 1000);

        for (int i = 0; i < 100; i++) {
            GenericRecord avroRecord = new GenericData.Record(schema);
            avroRecord.put("id", i);
            avroRecord.put("name", "name" + i);

            Schema locationSchema = schema.getField("location").schema();
            GenericData.Record record = new GenericData.Record(locationSchema);
            record.put("id", i);
            record.put("x", i);
            record.put("y", i + 1);

            avroRecord.put("location", record);

            GenericRow genericRow = AvroToDeltaConvert.convert(avroRecord, deltaSchema);
            parquetRowWriter.write(genericRow);
        }
        return parquetRowWriter.close();
    }

    private List<ParquetFileStat> writeV2Data(String location, Schema schema) throws IOException {
        StructType deltaSchema = AvroSchemaUtilExtended.toDelta(schema, false);
        Configuration hadoopConfig = new Configuration();
        ParquetRowWriter parquetRowWriter =
            new ParquetRowWriter(location, hadoopConfig, Collections.emptyList(), deltaSchema, 1000);

        for (int i = 100; i < 100 + 100; i++) {
            GenericRecord avroRecord = new GenericData.Record(schema);
            avroRecord.put("id", i);

            Schema locationSchema = schema.getField("location").schema();
            GenericData.Record record = new GenericData.Record(locationSchema);
            record.put("id", i);
            //location x already optional, not put it.
            record.put("y", i + 1);
            record.put("z", i + 2);
            avroRecord.put("location", record);
            if (i % 2 == 0) {
                avroRecord.put("status", "inactive");
            }

            GenericRow genericRow = AvroToDeltaConvert.convert(avroRecord, deltaSchema);
            parquetRowWriter.write(genericRow);
        }
        return parquetRowWriter.close();
    }


    // Helper methods
    private CompactStreamTask createTask(long streamId, long startOffset, long endOffset,
                                         long totalSize, long cumulativeSize, String filePath) {
        CompactStreamTask task = new CompactStreamTask();
        task.setStreamId(streamId);
        task.setStartOffset(startOffset);
        task.setEndOffset(endOffset);
        task.setTotalSize(totalSize);
        task.setCumulativeSize(cumulativeSize);
        task.setFilePath(filePath);
        task.setTopic("test-topic");
        return task;
    }

    private void copyDirectory(Path source, Path target) {
        try (var paths = Files.walk(source)) {
            paths.forEach(sourcePath -> {
                try {
                    Path relativePath = source.relativize(sourcePath);
                    Path targetPath = target.resolve(relativePath);
                    if (Files.isDirectory(sourcePath)) {
                        Files.createDirectories(targetPath);
                    } else {
                        Files.createDirectories(targetPath.getParent());
                        Files.copy(sourcePath, targetPath, StandardCopyOption.REPLACE_EXISTING);
                    }
                } catch (IOException e) {
                    throw new RuntimeException("Failed to copy test directory", e);
                }
            });
        } catch (IOException e) {
            throw new RuntimeException("Failed to copy test directory", e);
        }
    }

    private Map<String, String> createTags(long streamId, long startOffset, long endOffset,
                                           long totalSize, long cumulativeSize, String filePath) {
        Map<String, String> tags = new HashMap<>();
        tags.put("streamId", String.valueOf(streamId));
        tags.put("startOffset", String.valueOf(startOffset));
        tags.put("endOffset", String.valueOf(endOffset));
        tags.put("totalSize", String.valueOf(totalSize));
        tags.put("cumulativeSize", String.valueOf(cumulativeSize));
        tags.put("filePath", filePath);
        return tags;
    }

    private AddFileAction createAddFileAction(Map<String, String> tags) {
        AddFileAction action = mock(AddFileAction.class);
        lenient().when(action.getTags()).thenReturn(tags);
        return action;
    }
}
