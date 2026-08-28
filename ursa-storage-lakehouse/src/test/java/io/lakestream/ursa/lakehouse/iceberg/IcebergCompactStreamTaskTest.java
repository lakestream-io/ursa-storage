/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.lakehouse.iceberg;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

import io.lakestream.ursa.compaction.task.CompactStreamTask;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.io.WriteResult;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

@Tag("lakehouse")
class IcebergCompactStreamTaskTest {

    @Mock
    private WriteResult mockWriteResult;
    private CompactStreamTask baseTask;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);

        Map<String, String> properties = new HashMap<>();
        properties.put("compression.type", "zstd");
        properties.put("retention.hours", "72");

        Map<String, String> partitions = new HashMap<>();
        partitions.put("date", "2023-01-01");
        partitions.put("region", "us-west");

        baseTask = new CompactStreamTask();
        baseTask.setStreamId(12345L);
        baseTask.setStartOffset(1000L);
        baseTask.setEndOffset(2000L);
        baseTask.setTotalSize(1024L);
        baseTask.setCumulativeSize(2048L);
        baseTask.setTopic("user-events");
        baseTask.setTaskName("compact-task-001");
        baseTask.setStatus(CompactStreamTask.INIT);
        baseTask.setFilePath("/data/compact");
        baseTask.setFileFullPath("s3a://bucket/data/compact");
        baseTask.setFileSize(512L);
        baseTask.setPartitionValues(partitions);
        baseTask.setUnCommittedIndex(new ArrayList<>(List.of(1, 3, 5)));
        baseTask.setStats("{\"records\":500}");
        baseTask.setRealStartOffset(950L);
        baseTask.setRealEndOffset(1950L);
        baseTask.setMessageWrittenToUrsaTime(System.currentTimeMillis());
        baseTask.setProperties(properties);
    }

    @Test
    void shouldCopyAllFieldsFromBaseTask() {
        // When
        IcebergCompactStreamTask icebergTask = new IcebergCompactStreamTask(baseTask);

        // Then
        assertThat(icebergTask)
            .usingRecursiveComparison()
            .ignoringFields("writeResult")
            .ignoringFields("writeResults")
            .ignoringFields("dltWriteResults")
            .isEqualTo(baseTask);
    }

    @Test
    void shouldHandleWriteResultField() {
        // Given
        IcebergCompactStreamTask icebergTask = new IcebergCompactStreamTask(baseTask);
        WriteResult writeResult = mock(WriteResult.class);

        // When
        icebergTask.setWriteResults(List.of(writeResult));

        // Then
        assertThat(icebergTask.getWriteResults().get(0)).isSameAs(writeResult);
    }

    @Test
    void shouldMaintainCompareToOrdering() {
        // Given
        CompactStreamTask lowerTask = new CompactStreamTask();
        lowerTask.setStartOffset(500L);

        IcebergCompactStreamTask icebergTask1 = new IcebergCompactStreamTask(baseTask);
        IcebergCompactStreamTask icebergTask2 = new IcebergCompactStreamTask(lowerTask);

        // When/Then
        assertThat(icebergTask1.compareTo(icebergTask2)).isPositive();
        assertThat(icebergTask2.compareTo(icebergTask1)).isNegative();
    }

    @Test
    void shouldHandleNullCollectionsGracefully() {
        // Given
        baseTask.setPartitionValues(null);
        baseTask.setUnCommittedIndex(null);

        // When
        IcebergCompactStreamTask icebergTask = new IcebergCompactStreamTask(baseTask);

        // Then
        assertThat(icebergTask.getPartitionValues()).isNull();
        assertThat(icebergTask.getUnCommittedIndex()).isNull();
    }

    @Test
    void shouldShareMutableStateWithOriginalTask() {
        // Given
        IcebergCompactStreamTask icebergTask = new IcebergCompactStreamTask(baseTask);

        // When
        baseTask.getPartitionValues().put("hour", "12");
        baseTask.getUnCommittedIndex().add(7);

        // Then
        assertThat(icebergTask.getPartitionValues())
            .containsEntry("hour", "12");
        assertThat(icebergTask.getUnCommittedIndex())
            .containsExactly(1, 3, 5, 7);
    }

    @Test
    void shouldHandleNullBaseTaskFields() {
        // Given
        baseTask.setFileFullPath(null);
        baseTask.setStats(null);

        // When
        IcebergCompactStreamTask icebergTask = new IcebergCompactStreamTask(baseTask);

        // Then
        assertThat(icebergTask.getFileFullPath()).isNull();
        assertThat(icebergTask.getStats()).isNull();
    }
}
