/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.lakehouse;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

import io.lakestream.ursa.compaction.task.CompactStreamTask;
import io.lakestream.ursa.compaction.task.CompactStreamTaskSerde;
import io.lakestream.ursa.lakehouse.delta.DeltaCompactStreamTask;
import io.lakestream.ursa.lakehouse.iceberg.IcebergCompactStreamTask;
import io.lakestream.ursa.lakehouse.writer.ParquetFileStat;
import java.io.ByteArrayOutputStream;
import java.io.ObjectOutputStream;
import java.lang.reflect.Constructor;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileContent;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Metrics;
import org.apache.iceberg.PartitionData;
import org.apache.iceberg.io.WriteResult;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag("lakehouse")
public class TestTaskCompatibility {

    @Test
    public void testNormalSerializeAndDeserialize() throws Exception {
        CompactStreamTask compactStreamTask = new CompactStreamTask();
        compactStreamTask.setStreamId(100);
        compactStreamTask.setStartOffset(0);
        compactStreamTask.setEndOffset(100);
        compactStreamTask.setTaskName("328792a9-4d60-4902-a593-934ebc401650");

        byte[] content = CompactStreamTaskSerde.INSTANCE.serialize(compactStreamTask);

        CompactStreamTask deserialize = CompactStreamTaskSerde.INSTANCE.deserialize(content);

        assertEquals(deserialize.getStreamId(), compactStreamTask.getStreamId());
        assertEquals(deserialize.getStartOffset(), compactStreamTask.getStartOffset());
        assertEquals(deserialize.getEndOffset(), compactStreamTask.getEndOffset());
        assertEquals(deserialize.getTaskName(), compactStreamTask.getTaskName());
    }

    @Test
    public void testIcebergSerializeAndDeserialize() throws Exception {
        CompactStreamTask compactStreamTask = new CompactStreamTask();
        compactStreamTask.setStreamId(100);
        compactStreamTask.setStartOffset(0);
        compactStreamTask.setEndOffset(100);
        compactStreamTask.setTaskName("328792a9-4d60-4902-a593-934ebc401650");

        Class<?> dataFileClazz = Class.forName("org.apache.iceberg.GenericDataFile");

        Constructor<?> dataFileConstructor = dataFileClazz.getDeclaredConstructor(
                int.class,
                String.class,
                FileFormat.class,
                PartitionData.class,
                long.class,
                Metrics.class,
                ByteBuffer.class,
                List.class,
                Integer.class,
                Long.class
        );

        dataFileConstructor.setAccessible(true);

        int specId = 0;
        String filePath = "/tmp/test.parquet";
        FileFormat format = FileFormat.PARQUET;
        Metrics metrics = new Metrics(100L, Collections.emptyMap(), Collections.emptyMap(), Collections.emptyMap(), Collections.emptyMap(), Collections.emptyMap(), Collections.emptyMap());
        ByteBuffer keyMetadata = ByteBuffer.allocate(2);
        keyMetadata.put("aa".getBytes());
        keyMetadata.flip();
        List<Long> splitOffsets = Arrays.asList(0L);
        Integer sortOrderId = 1;
        Long firstRowId = 1L;

        Object[] params = new Object[]{
                specId,
                filePath,
                format,
                null,
                12345L,
                metrics,
                keyMetadata,
                splitOffsets,
                sortOrderId,
                firstRowId
        };

        Object addFile = dataFileConstructor.newInstance(params);


        Class<?> deleteFileClazz = Class.forName("org.apache.iceberg.GenericDeleteFile");

        Constructor<?> deleteFileConstructor = deleteFileClazz.getDeclaredConstructor(
                int.class,
                FileContent.class,
                String.class,
                FileFormat.class,
                PartitionData.class,
                long.class,
                Metrics.class,
                int[].class,
                Integer.class,
                List.class,
                ByteBuffer.class,
                String.class,
                Long.class,
                Long.class
        );

        deleteFileConstructor.setAccessible(true);

        FileContent fileContent = FileContent.POSITION_DELETES;
        String deleteFilePath = "/tmp/test-delete.parquet";
        int[] equalityFieldIds = new int[]{1, 2};

        String referencedDataFile = "/tmp/test.parquet";
        Long contentOffset = 123L;
        Long contentSizeInBytes = 456L;

        keyMetadata = ByteBuffer.allocate(2);
        keyMetadata.put("aa".getBytes());
        keyMetadata.flip();
        Object[] params1 = new Object[]{
                specId,
                fileContent,
                deleteFilePath,
                format,
                null,
                12345L,
                metrics,
                equalityFieldIds,
                sortOrderId,
                splitOffsets,
                keyMetadata,
                referencedDataFile,
                contentOffset,
                contentSizeInBytes
        };

        Object deleteFile = deleteFileConstructor.newInstance(params1);

        WriteResult writeResult = WriteResult.builder().addDataFiles((DataFile) addFile).addDeleteFiles(
                (DeleteFile) deleteFile).build();

        IcebergCompactStreamTask icebergCompactStreamTask = new IcebergCompactStreamTask(compactStreamTask);
        icebergCompactStreamTask.setWriteResult(writeResult);

        byte[] content = CompactStreamTaskSerde.INSTANCE.serialize(icebergCompactStreamTask);

        IcebergCompactStreamTask deserialize =
                (IcebergCompactStreamTask) CompactStreamTaskSerde.INSTANCE.deserialize(content);

        assertEquals(deserialize.getStreamId(), compactStreamTask.getStreamId());
        assertEquals(deserialize.getStartOffset(), compactStreamTask.getStartOffset());
        assertEquals(deserialize.getEndOffset(), compactStreamTask.getEndOffset());
        assertEquals(deserialize.getTaskName(), compactStreamTask.getTaskName());

        WriteResult writeResult1 = deserialize.getWriteResult();
        DataFile[] dataFiles = writeResult1.dataFiles();

        DataFile deseriaDataFile = dataFiles[0];
        assertEquals(deseriaDataFile.specId(), 0);
        assertEquals(deseriaDataFile.location(), "/tmp/test.parquet");
        assertEquals(deseriaDataFile.format(), FileFormat.PARQUET);
        assertEquals(deseriaDataFile.fileSizeInBytes(), 12345L);
        assertEquals(deseriaDataFile.recordCount(), 100L);

        assertArrayEquals(deseriaDataFile.keyMetadata().array(), "aa".getBytes());
        assertEquals(deseriaDataFile.splitOffsets(), splitOffsets);
        assertEquals(deseriaDataFile.sortOrderId(), 1);
        assertEquals(deseriaDataFile.firstRowId(), 1L);


        DeleteFile[] deleteFiles = writeResult1.deleteFiles();

        DeleteFile deseriaDeleteFile = deleteFiles[0];
        assertEquals(deseriaDeleteFile.specId(), 0);
        assertEquals(deseriaDeleteFile.content(), FileContent.POSITION_DELETES);
        assertEquals(deseriaDeleteFile.location(), "/tmp/test-delete.parquet");
        assertEquals(deseriaDeleteFile.format(), FileFormat.PARQUET);
        assertEquals(deseriaDeleteFile.fileSizeInBytes(), 12345L);
        assertEquals(deseriaDeleteFile.recordCount(), 100L);
        assertEquals(deseriaDeleteFile.equalityFieldIds(), Arrays.asList(1, 2));
        assertEquals(deseriaDeleteFile.sortOrderId(), 1);
        assertEquals(deseriaDeleteFile.splitOffsets(), splitOffsets);
        assertArrayEquals(deseriaDeleteFile.keyMetadata().array(), "aa".getBytes());
        assertEquals(deseriaDeleteFile.referencedDataFile(), "/tmp/test.parquet");
        assertEquals(deseriaDeleteFile.contentOffset(), 123L);
        assertEquals(deseriaDeleteFile.contentSizeInBytes(), 456L);
    }

    @Test
    public void testDeltaSerializeAndDeserialize() throws Exception {
        CompactStreamTask compactStreamTask = new CompactStreamTask();
        compactStreamTask.setStreamId(100);
        compactStreamTask.setStartOffset(0);
        compactStreamTask.setEndOffset(100);
        compactStreamTask.setTaskName("328792a9-4d60-4902-a593-934ebc401650");

        ParquetFileStat fileStat = new ParquetFileStat("test.parquet", "/tmp/test.parquet", 1000L, "",
                Collections.emptyMap(), Collections.emptyMap());
        DeltaCompactStreamTask deltaCompactStreamTask = new DeltaCompactStreamTask(compactStreamTask);
        deltaCompactStreamTask.setDeltaFiles(Collections.singletonList(fileStat));

        byte[] content = CompactStreamTaskSerde.INSTANCE.serialize(deltaCompactStreamTask);

        DeltaCompactStreamTask deserialize =
                (DeltaCompactStreamTask) CompactStreamTaskSerde.INSTANCE.deserialize(content);

        assertEquals(deserialize.getStreamId(), compactStreamTask.getStreamId());
        assertEquals(deserialize.getStartOffset(), compactStreamTask.getStartOffset());
        assertEquals(deserialize.getEndOffset(), compactStreamTask.getEndOffset());
        assertEquals(deserialize.getTaskName(), compactStreamTask.getTaskName());

        List<ParquetFileStat> deltaFiles = deserialize.getDeltaFiles();
        assertEquals(1, deltaFiles.size());
        fileStat = deltaFiles.get(0);
        assertEquals("test.parquet", fileStat.getFilePath());
        assertEquals("/tmp/test.parquet", fileStat.getFileFullPath());
        assertEquals(1000L, fileStat.getFileSize());
    }

    @Test
    public void testJavaSerializationFallback() throws Exception {
        CompactStreamTask compactStreamTask = new CompactStreamTask();
        compactStreamTask.setStreamId(100);
        compactStreamTask.setStartOffset(0);
        compactStreamTask.setEndOffset(100);
        compactStreamTask.setTaskName("328792a9-4d60-4902-a593-934ebc401650");

        ParquetFileStat fileStat = new ParquetFileStat("test.parquet", "/tmp/test.parquet", 1000L, "",
                Collections.emptyMap(), Collections.emptyMap());
        DeltaCompactStreamTask deltaCompactStreamTask = new DeltaCompactStreamTask(compactStreamTask);
        deltaCompactStreamTask.setDeltaFiles(Collections.singletonList(fileStat));

        byte[] content;
        try (ByteArrayOutputStream bytes = new ByteArrayOutputStream();
             ObjectOutputStream output = new ObjectOutputStream(bytes)) {
            output.writeObject(deltaCompactStreamTask);
            output.flush();
            content = bytes.toByteArray();
        }

        DeltaCompactStreamTask deserialize =
                (DeltaCompactStreamTask) CompactStreamTaskSerde.INSTANCE.deserialize(content);

        assertEquals(deserialize.getStreamId(), compactStreamTask.getStreamId());
        assertEquals(deserialize.getStartOffset(), compactStreamTask.getStartOffset());
        assertEquals(deserialize.getEndOffset(), compactStreamTask.getEndOffset());
        assertEquals(deserialize.getTaskName(), compactStreamTask.getTaskName());

        List<ParquetFileStat> deltaFiles = deserialize.getDeltaFiles();
        assertEquals(1, deltaFiles.size());
        fileStat = deltaFiles.get(0);
        assertEquals("test.parquet", fileStat.getFilePath());
        assertEquals("/tmp/test.parquet", fileStat.getFileFullPath());
        assertEquals(1000L, fileStat.getFileSize());
    }
}
