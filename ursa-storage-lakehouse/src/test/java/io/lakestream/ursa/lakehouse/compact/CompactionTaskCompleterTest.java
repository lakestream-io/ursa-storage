/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.lakehouse.compact;

import static io.lakestream.ursa.lakehouse.v2.AbstractLakehouseWriter.BATCH_MESSAGE_COUNT;
import static io.lakestream.ursa.lakehouse.v2.AbstractLakehouseWriter.LAST_BATCH_ID_IN_FILE;
import static io.lakestream.ursa.lakehouse.v2.AbstractLakehouseWriter.LAST_ENTRY_ID_IN_FILE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.groups.Tuple.tuple;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.lakestream.ursa.compaction.CompactTaskManager;
import io.lakestream.ursa.compaction.task.CompactStreamTask;
import io.lakestream.ursa.compaction.task.ManagedWriteResult;
import io.lakestream.ursa.exception.ExceptionWithCode;
import io.lakestream.ursa.lakehouse.iceberg.IcebergCompactStreamTask;
import io.lakestream.ursa.lakehouse.v2.IWriteResult;
import io.lakestream.ursa.lakehouse.v2.iceberg.IcebergWriteResult;
import io.lakestream.ursa.lakehouse.v2.io.parquet.ParquetWriteResult;
import java.net.URI;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.iceberg.io.WriteResult;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

/**
 * Unit tests for {@link CompactionTaskCompleter} — the extracted task-completion + persistence
 * logic shared by the legacy {@code UrsaCompactionWorker} and the new
 * {@code LakehouseMaterializationService}. It records write results onto the task, marks it
 * {@code COMPACTED}, and persists it via {@link CompactTaskManager} so the group-commit runner
 * finalizes the catalog snapshot — it does NOT commit to the catalog itself.
 */
@Tag("lakehouse")
class CompactionTaskCompleterTest {

    private static CompactStreamTask task() {
        CompactStreamTask task = new CompactStreamTask();
        task.setTopic("default/events-partition-0");
        task.setStartOffset(0L);
        task.setEndOffset(5L);
        return task;
    }

    private static ParquetWriteResult parquetResult(String dataFile, long fileSize, long messages,
                                                    long lastEntryId, long lastBatchId) {
        Map<String, Object> extra = new HashMap<>();
        extra.put(BATCH_MESSAGE_COUNT, new AtomicLong(messages));
        extra.put(LAST_ENTRY_ID_IN_FILE, lastEntryId);
        extra.put(LAST_BATCH_ID_IN_FILE, lastBatchId);
        return new ParquetWriteResult(URI.create("s3://bucket/default/events-partition-0/"), dataFile,
                fileSize, messages, dataFile + ".index", null, extra);
    }

    /**
     * Regression test for issue #11: the compacted-object readers can only locate a Parquet file
     * through the {@code ManagedTableFileIndex}, which the commit runner builds from the task's
     * {@link ManagedWriteResult}s. They must therefore be recorded unconditionally, otherwise every
     * compacted range is unreadable.
     */
    @Test
    void managedParquetResultsAlwaysRecordManagedWriteResults() throws Exception {
        CompactTaskManager ctm = mock(CompactTaskManager.class);
        when(ctm.updateCompactTask(any())).thenReturn(CompletableFuture.completedFuture(null));
        CompactionTaskCompleter completer = new CompactionTaskCompleter(ctm);

        ParquetWriteResult first = parquetResult("ursa-a.parquet", 100L, 3L, 2L, 0L);
        ParquetWriteResult second = parquetResult("ursa-b.parquet", 200L, 2L, 4L, 1L);
        completer.completeCompaction(task(), List.of(first, second), List.of(), List.of());

        ArgumentCaptor<CompactStreamTask> captor = ArgumentCaptor.forClass(CompactStreamTask.class);
        verify(ctm).updateCompactTask(captor.capture());
        CompactStreamTask persisted = captor.getValue();
        assertThat(persisted.getStatus()).isEqualTo(CompactStreamTask.COMPACTED);
        assertThat(persisted.getFilePath()).isEqualTo("ursa-a.parquet");
        assertThat(persisted.getNumberOfRecordsInCompactedFile()).isEqualTo(3);

        assertThat(persisted.getManagedWriteResults()).hasSize(2);
        assertThat(persisted.getManagedWriteResults())
                .extracting(ManagedWriteResult::getFilePath, ManagedWriteResult::getFullFilePath,
                        ManagedWriteResult::getFileSize, ManagedWriteResult::getNumberOfMessages,
                        ManagedWriteResult::getLastEntryId, ManagedWriteResult::getLastBatchId)
                .containsExactlyInAnyOrder(
                        tuple("ursa-a.parquet",
                                "s3://bucket/default/events-partition-0/ursa-a.parquet", 100L, 3, 2L, 0L),
                        tuple("ursa-b.parquet",
                                "s3://bucket/default/events-partition-0/ursa-b.parquet", 200L, 2, 4L, 1L));
    }

    @Test
    void externalResultsPersistTaskAsCompactedIcebergTask() throws Exception {
        CompactTaskManager ctm = mock(CompactTaskManager.class);
        when(ctm.updateCompactTask(any())).thenReturn(CompletableFuture.completedFuture(null));
        CompactionTaskCompleter completer = new CompactionTaskCompleter(ctm);

        IWriteResult result = new IcebergWriteResult(mock(WriteResult.class));
        completer.completeCompaction(task(), List.of(), List.of(result), List.of());

        ArgumentCaptor<CompactStreamTask> captor = ArgumentCaptor.forClass(CompactStreamTask.class);
        verify(ctm).updateCompactTask(captor.capture());
        CompactStreamTask persisted = captor.getValue();
        assertThat(persisted.getStatus()).isEqualTo(CompactStreamTask.COMPACTED);
        assertThat(persisted).isInstanceOf(IcebergCompactStreamTask.class);
        // The runner reads COMPACTED tasks from Oxia and group-commits; the completer must not
        // have committed to any catalog itself (no committer involved here).
    }

    @Test
    void dltOnlyResultsStillPersistTaskAsCompacted() throws Exception {
        // Every record failed serde: no data results, only DLT results. completeExternalCompaction
        // must still wrap + persist the task (using the DLT result type) so the failures are not lost.
        CompactTaskManager ctm = mock(CompactTaskManager.class);
        when(ctm.updateCompactTask(any())).thenReturn(CompletableFuture.completedFuture(null));
        CompactionTaskCompleter completer = new CompactionTaskCompleter(ctm);

        IWriteResult dlt = new IcebergWriteResult(mock(WriteResult.class));
        completer.completeCompaction(task(), List.of(), List.of(), List.of(dlt));

        ArgumentCaptor<CompactStreamTask> captor = ArgumentCaptor.forClass(CompactStreamTask.class);
        verify(ctm).updateCompactTask(captor.capture());
        assertThat(captor.getValue().getStatus()).isEqualTo(CompactStreamTask.COMPACTED);
        assertThat(captor.getValue()).isInstanceOf(IcebergCompactStreamTask.class);
    }

    @Test
    void noWriteResultsIsRejected() {
        CompactTaskManager ctm = mock(CompactTaskManager.class);
        CompactionTaskCompleter completer = new CompactionTaskCompleter(ctm);

        assertThatThrownBy(() ->
                completer.completeCompaction(task(), List.of(), List.of(), List.of()))
                .isInstanceOf(ExceptionWithCode.class)
                .hasMessageContaining("No write results");
    }
}
