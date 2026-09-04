/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.lakehouse.compact;


import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.AtomicDouble;
import io.lakestream.api.materialization.TableMode;
import io.lakestream.ursa.compaction.CompactTaskManager;
import io.lakestream.ursa.compaction.metrics.CompactionMetrics;
import io.lakestream.ursa.compaction.task.CompactStreamTask;
import io.lakestream.ursa.exception.ExceptionCode;
import io.lakestream.ursa.exception.ExceptionWithCode;
import io.lakestream.ursa.exception.RuntimeExceptionWithCode;
import io.lakestream.ursa.lakehouse.utils.StreamTableNaming;
import io.lakestream.ursa.lakehouse.v2.IWriteResult;
import io.lakestream.ursa.lakehouse.v2.LakehouseFactory;
import io.lakestream.ursa.lakehouse.v2.LakehouseRecordWriter;
import io.lakestream.ursa.materialization.serde.GenericEntry;
import io.lakestream.ursa.storage.impl.StorageConfig;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.common.AttributesBuilder;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class LakehouseCompactionWorker implements CompactionTaskProcessor {

    LakehouseFactory lakehouseFactory;
    EntryProcessFactory entryReaderFactory;
    @Getter
    @VisibleForTesting
    private final CompactTaskManager compactTaskManager;
    private final CompactionMetrics compactionMetrics;
    private final AtomicDouble avgEntrySize = new AtomicDouble(1024);
    private final boolean skipMarkerMessages;
    private final long readTimeoutSeconds;
    private final long maxWaitForTxnResolutionSeconds;
    private final CompactionTaskCompleter taskCompleter;
    // Tracks when each task was first attempted by this worker, so the reader can bound its wait
    // on potentially-pending transactions. Reset on worker restart (over-wait, safe direction).
    private final ConcurrentHashMap<String, Long> firstAttemptTimes = new ConcurrentHashMap<>();

    public LakehouseCompactionWorker(LakehouseFactory lakehouseFactory,
                                  EntryProcessFactory entryReaderFactory,
                                  CompactTaskManager compactTaskManager, CompactionMetrics metrics) {
        this(lakehouseFactory, entryReaderFactory, compactTaskManager, metrics, new StorageConfig());
    }

    public LakehouseCompactionWorker(LakehouseFactory lakehouseFactory,
                                  EntryProcessFactory entryReaderFactory,
                                  CompactTaskManager compactTaskManager, CompactionMetrics metrics,
                                  StorageConfig storageConfig) {
        this.lakehouseFactory = lakehouseFactory;
        this.entryReaderFactory = entryReaderFactory;
        this.compactTaskManager = compactTaskManager;
        this.compactionMetrics = metrics;
        CompactionTaskCompleter.warnIfDeprecatedFlagConfigured(storageConfig.getProperties()
            .get(CompactionTaskCompleter.MANAGED_TABLE_SCHEMA_EVOLUTION_ENABLED));
        this.skipMarkerMessages = Boolean.parseBoolean(storageConfig.getProperties()
            .getOrDefault("skipMarkerMessages", "false").toString());
        this.readTimeoutSeconds = Long.parseLong(storageConfig.getProperties()
            .getOrDefault("walReadTimeoutSeconds",
                String.valueOf(EntryReaderOptions.DEFAULT_READ_TIMEOUT_SECONDS)).toString());
        this.maxWaitForTxnResolutionSeconds = Long.parseLong(storageConfig.getProperties()
            .getOrDefault("walReadMaxWaitForTxnResolutionSeconds",
                String.valueOf(EntryReaderOptions.DEFAULT_MAX_WAIT_FOR_TXN_RESOLUTION_SECONDS)).toString());
        this.taskCompleter = new CompactionTaskCompleter(compactTaskManager);
    }

    public void doCompact(CompactStreamTask task) throws Exception {
        long compactStartTime = System.nanoTime();
        var topic = task.getTopic();
        var streamId = task.getStreamId();
        var startOffset = task.getStartOffset();
        var endOffset = task.getEndOffset();
        log.info("Starting to compact topic {} streamId {} range: [{}-{}]", topic, streamId, startOffset, endOffset);

        var propertiesForWriter = new HashMap<String, String>();
        if (task.getProperties() != null) {
            propertiesForWriter.putAll(task.getProperties());
        }
        long firstAttemptTimeMs = firstAttemptTimes.computeIfAbsent(task.getTaskName(),
            k -> System.currentTimeMillis());
        var entryReaderOptions = new EntryReaderOptions(skipMarkerMessages, readTimeoutSeconds,
            firstAttemptTimeMs, maxWaitForTxnResolutionSeconds);
        Optional<LakehouseRecordWriter<GenericEntry>> managedWriter = Optional.empty();
        Optional<LakehouseRecordWriter<GenericEntry>> externalWriter = Optional.empty();
        Optional<LakehouseRecordWriter<FailureMessage>> dltWriter = Optional.empty();
        long totalReadSize = 0;
        try (var reader =
                 entryReaderFactory.createEntryReader(topic, streamId, startOffset, endOffset, avgEntrySize.get(),
                     entryReaderOptions)) {
            managedWriter = lakehouseFactory.getManagedWriter(topic, propertiesForWriter);
            externalWriter = lakehouseFactory.getExternalWriter(topic, propertiesForWriter);
            if (externalWriter.isPresent()) {
                // Legacy workers create writers before policy resolution. Resolve the SDT destination
                // now and persist it on the task so the asynchronous committer uses the same identity.
                // Do this only after constructing the SBT writer so SDT naming cannot affect its path.
                Properties namingProperties = new Properties();
                namingProperties.putAll(propertiesForWriter);
                Map<String, String> resolvedProperties = StreamTableNaming.withResolvedTableIdentifier(
                        propertiesForWriter,
                        StreamTableNaming.resolveForWriter(topic, namingProperties, TableMode.EXTERNAL));
                propertiesForWriter.clear();
                propertiesForWriter.putAll(resolvedProperties);
                task.setProperties(resolvedProperties);
            }
            dltWriter = externalWriter.flatMap(writer -> {
                Optional<LakehouseRecordWriter<FailureMessage>> candidate =
                        lakehouseFactory.getExternalDLTWriter(topic, propertiesForWriter);
                candidate.ifPresent(w -> writer.registerFailureMessageHandler(DLTFailureMessageHandler.of(w)));
                return candidate;
            });
            GenericEntry ge = null;
            long readStartTime = System.nanoTime();
            while ((ge = reader.read()) != null) {
                try {
                    compactionMetrics.getReadMessagesFromWalLatency().recordSuccess(System.nanoTime() - readStartTime);

                    var size = ge.entry().payload().readableBytes();
                    totalReadSize += size;
                    compactionMetrics.getCompactedBytesSize().add(size);

                    long writeStartTime = System.nanoTime();
                    if (externalWriter.isPresent()) {
                        var genericEntry = new GenericEntry(ge.entry().retainedDuplicate(), ge.metadata());
                        externalWriter.get().write(genericEntry);
                    }
                    if (managedWriter.isPresent()) {
                        var genericEntry = new GenericEntry(ge.entry().retainedDuplicate(), ge.metadata());
                        managedWriter.get().write(genericEntry);
                    }
                    compactionMetrics.getWriteMessagesToParquetLatency()
                        .recordSuccess(System.nanoTime() - writeStartTime);

                    readStartTime = System.nanoTime();
                } finally {
                    ge.entry().payload().release();
                }
            }
            List<IWriteResult> managedResult = managedWriter.isPresent()
                ? managedWriter.get().close() : Collections.emptyList();
            List<IWriteResult> externalResult = externalWriter.isPresent()
                    ? externalWriter.get().close() : Collections.emptyList();
            List<IWriteResult> externalDLTResult =
                    dltWriter.isPresent() ? dltWriter.get().close() : Collections.emptyList();

            double nextAverageEntrySize = updatedAverageEntrySize(
                    avgEntrySize.get(), totalReadSize, startOffset, endOffset);
            taskCompleter.completeCompaction(task, managedResult, externalResult, externalDLTResult);

            // [startOffset, endOffset) contains end-start entries. Use that half-open range
            // length when updating the estimate used to rate-limit the next compaction task.
            avgEntrySize.set(nextAverageEntrySize);
            compactionMetrics.getPublishedTaskBytes().set(totalReadSize);
        } catch (Throwable e) {
            // close resource and ignore the results
            try {
                if (managedWriter.isPresent()) {
                    managedWriter.get().close();
                }
                externalWriter.ifPresent(w -> {
                    try {
                        w.close();
                    } catch (Throwable ex) {
                        log.warn("Failed to close the external writer for topic {}", topic, ex);
                        // ignore
                    }
                });
            } catch (Throwable ex) {
                log.warn("Failed to close the writers for topic {}", topic, ex);
            }
            ExceptionWithCode ewc = exceptionWithCode(e);
            var isDeleteCompactTaskErrorErr = handleDeleteCompactTask(ewc, task);
            if (isDeleteCompactTaskErrorErr) {
                return;
            }
            compactionMetrics.getCompactLatency().recordFailure(System.nanoTime() - compactStartTime);
            // Source reads, throttling and temporary exhaustion are retryable
            // signals (reconnect-redelivery, rate limiter, broker temporarily empty); surfacing them
            // to the customer-visible compactionErrorHappenTime metric is noise.
            if (!isRetryableCode(ewc.getExceptionCode())) {
                AttributesBuilder attrBuilder = Attributes.builder()
                    .put(AttributeKey.stringKey("topic"), task.getTopic())
                    .put(AttributeKey.longKey("errorCode"), (long) ewc.getExceptionCode().getCode());
                compactionMetrics.getCompactionErrorHappenTime().set(System.currentTimeMillis(), attrBuilder.build());
            }
            throw ewc;
        }
        compactionMetrics.getCompactLatency().recordSuccess(System.nanoTime() - compactStartTime);
        firstAttemptTimes.remove(task.getTaskName());
    }

    private static boolean isRetryableCode(ExceptionCode code) {
        return code == ExceptionCode.SOURCE_READ_ERROR
            || code == ExceptionCode.SOURCE_THROTTLED
            || code == ExceptionCode.NO_MORE_RECORDS;
    }

    private static ExceptionWithCode exceptionWithCode(Throwable error) {
        if (error instanceof ExceptionWithCode exceptionWithCode) {
            return exceptionWithCode;
        }
        if (error instanceof RuntimeExceptionWithCode runtimeExceptionWithCode) {
            return runtimeExceptionWithCode.getRealException();
        }
        return new ExceptionWithCode(ExceptionCode.INTERNAL_ERROR,
                "Failed to compact the data into the lakehouse", error);
    }

    @VisibleForTesting
    static double updatedAverageEntrySize(
            double previousAverage, long totalReadSize, long startOffset, long endOffset) {
        long entryCount = Math.subtractExact(endOffset, startOffset);
        if (entryCount <= 0) {
            throw new IllegalArgumentException(
                    "Invalid offset range [" + startOffset + ", " + endOffset + ")");
        }
        return previousAverage * 0.9 + ((double) totalReadSize / entryCount) * 0.1;
    }

    private boolean handleDeleteCompactTask(ExceptionWithCode e, CompactStreamTask task)
            throws InterruptedException {
        if (e.getExceptionCode() == ExceptionCode.NO_SUCH_LOG
            || e.getExceptionCode() == ExceptionCode.NO_SUCH_STREAM
            || e.getExceptionCode() == ExceptionCode.NO_SUCH_ENTRIES
            || e.getExceptionCode() == ExceptionCode.NO_SUCH_OFFSET
            || e.getExceptionCode() == ExceptionCode.COMPACTION_NO_WRITE_RESULT) {
            log.info("Delete the task {} because of error {}", task.getTaskName(), e.getExceptionCode());
            try {
                compactTaskManager.deleteCompactTask(task).get();
                firstAttemptTimes.remove(task.getTaskName());
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                throw interrupted;
            } catch (Exception ex) {
                log.warn("Failed to delete compact task {}, it will be retry in the next round", task, ex);
            }
            return true;
        }
        return false;
    }

    @Override
    public void close() throws Exception {

    }
}
