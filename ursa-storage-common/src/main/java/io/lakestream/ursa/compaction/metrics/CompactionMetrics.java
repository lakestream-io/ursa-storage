/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.compaction.metrics;

import io.lakestream.ursa.metrics.Counter;
import io.lakestream.ursa.metrics.InstrumentProvider;
import io.lakestream.ursa.metrics.LatencyHistogram;
import io.lakestream.ursa.metrics.RequestSizeHistogram;
import io.lakestream.ursa.metrics.Unit;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.metrics.LongGauge;
import lombok.Getter;

@Getter
public class CompactionMetrics {

    public static final CompactionMetrics NOOP = new CompactionMetrics(InstrumentProvider.NOOP);

    private final InstrumentProvider provider;

    // Compaction service
    private final LatencyHistogram compactLatency;
    private final LatencyHistogram readMessagesFromWalLatency;
    private final LatencyHistogram writeMessagesToParquetLatency;
    private final CompactionLatencyHistogram messageFromUrsaToParquetLatency;
    private final CompactionLatencyHistogram messageEndToEndCompactLatency;
    private final LatencyHistogram compactTaskCommitLatency;
    private final LatencyHistogram commitToLakehouseLatency;
    private final RequestSizeHistogram nonCommittableTaskHistogram;

    // Counter
    private final Counter compactedBytesSize;
    private final Counter compactedMessagesCount;
    private final Counter failedCompactTaskCount;
    private final Counter publishTaskFailedCount;
    private final Counter publicationLeaseUnavailableCount;
    private final Counter tableCorruptedCount;

    // Gauge
    private final LongGauge compactionLag;
    private final LongGauge latestMessageOffset;
    private final LongGauge latestPublishedOffset;
    private final LongGauge commitTaskBatchSize;
    private final LongGauge lastCompactedOffset;
    private final LongGauge ongoingCompactionTaskCount;
    private final LongGauge ongoingCompactionTopicCount;
    private final LongGauge publishedTaskBytes;
    private final LongGauge committedParquetFileBytes;
    private final LongGauge quarantinedTopicsCount;
    private final LongGauge topicsInDLQ;
    private final LongGauge tasksInDLQ;
    private final LongGauge tasksInInitState;
    private final LongGauge tasksInCompactedState;
    private final LongGauge tasksInPreparedCommitState;
    private final LongGauge tasksInCommittedState;
    private final LongGauge nonCommittableTaskCount;
    private final LongGauge compactionErrorHappenTime;
    private final LongGauge lastCommitTime;
    private final LongGauge lakehouseMetadataFileSize;


    public CompactionMetrics(InstrumentProvider provider) {
        this.provider = provider;
        // compaction service
        // One task for a streamId compact duration
        this.compactLatency = provider.newLatencyHistogram("ursa.storage.compact.duration",
                "Compaction service compact latency for one streamId", Attributes.empty());
        this.readMessagesFromWalLatency = provider.newLatencyHistogram("ursa.storage.compact.read.messages.duration",
                "Compaction service read messages from wal latency for one streamId", Attributes.empty());
        this.writeMessagesToParquetLatency = provider.newLatencyHistogram(
            "ursa.storage.compact.write.messages.duration",
                "Compaction service write messages to parquet latency for one streamId", Attributes.empty());
        this.messageFromUrsaToParquetLatency = provider.newCompactionLatencyHistogram(
            "ursa.storage.compact.message.from.ursa.to.parquet.duration",
                "Compaction service message from ursa to parquet latency for one streamId", Attributes.empty());
        this.messageEndToEndCompactLatency = provider.newCompactionLatencyHistogram(
                "ursa.storage.compact.message.end.to.end.duration",
                "Compaction service message from written to Ursa engine to commit to lakehouse end to end latency",
                Attributes.empty());
        this.compactTaskCommitLatency = provider.newLatencyHistogram("ursa.storage.compact.task.commit.duration",
                "Compaction service compact task commit latency for one streamId", Attributes.empty());
        this.commitToLakehouseLatency = provider.newLatencyHistogram(
            "ursa.storage.compact.commit.to.lakehouse.duration",
                "Compaction service commit to lakehouse latency for one streamId", Attributes.empty());
        this.nonCommittableTaskHistogram = provider.newRequestSizeHistogram(
                "ursa.storage.compact.non.committable.task.histogram",
                "Compaction service non-committable task count histogram for one streamId", Attributes.empty());

        // Counter
        this.compactedBytesSize = provider.newCounter("ursa.storage.compact.bytes",
                Unit.Bytes, "Compacted bytes count", Attributes.empty());
        this.compactedMessagesCount = provider.newCounter("ursa.storage.compact.messages",
                Unit.Messages, "Compacted messages count", Attributes.empty());
        this.failedCompactTaskCount = provider.newCounter("ursa.storage.compact.failed.task.count",
                Unit.Messages, "Compacted failed task count", Attributes.empty());
        this.publishTaskFailedCount = provider.newCounter("ursa.storage.compact.publish.task.failed.count",
                Unit.Messages, "Compacted publish task failed count", Attributes.empty());
        this.publicationLeaseUnavailableCount = provider.newCounter(
                "ursa.storage.compact.publication.lease.unavailable.count",
                Unit.Sessions,
                "Compaction task publication lease contention transitions",
                Attributes.empty());
        this.tableCorruptedCount = provider.newCounter("ursa.storage.compact.table.corrupted.count",
                Unit.Messages, "Compacted table corrupted count", Attributes.empty());

        // Gauge
        this.compactionLag = provider.getMeter().gaugeBuilder("ursa.storage.compact.lag")
                .setDescription("Compaction service lag for one streamId")
                .setUnit(Unit.Messages.toString())
                .ofLongs()
                .build();
        this.latestMessageOffset = provider.getMeter().gaugeBuilder("ursa.storage.compact.latest.message.offset")
                .setDescription("Compaction service latest message offset for one streamId")
                .setUnit(Unit.Messages.toString())
                .ofLongs()
                .build();
        this.latestPublishedOffset = provider.getMeter().gaugeBuilder("ursa.storage.compact.latest.published.offset")
                .setDescription("Compaction service latest published offset for one streamId")
                .setUnit(Unit.Messages.toString())
                .ofLongs()
                .build();
        this.commitTaskBatchSize = provider.getMeter().gaugeBuilder("ursa.storage.compact.commit.task.batch.size")
                .setDescription("Compaction service commit task batch size for one streamId")
                .setUnit(Unit.Messages.toString())
                .ofLongs()
                .build();
        this.lastCompactedOffset = provider.getMeter().gaugeBuilder("ursa.storage.compact.last.compacted.offset")
                .setDescription("Compaction service last compacted offset for one streamId")
                .setUnit(Unit.Messages.toString())
                .ofLongs()
                .build();
        this.ongoingCompactionTaskCount = provider.getMeter().gaugeBuilder("ursa.storage.compact.ongoing.task.count")
                .setDescription("Compaction service ongoing task count")
                .setUnit(Unit.Messages.toString())
                .ofLongs()
                .build();
        this.ongoingCompactionTopicCount = provider.getMeter()
            .gaugeBuilder("ursa.storage.compact.ongoing.topic.count")
                .setDescription("Compaction service ongoing topic count")
                .setUnit(Unit.Messages.toString())
                .ofLongs()
                .build();
        this.publishedTaskBytes = provider.getMeter()
                .gaugeBuilder("ursa.storage.compact.published.task.bytes")
                .setDescription("Compaction service published task bytes size")
                .setUnit(Unit.Bytes.toString())
                .ofLongs()
                .build();
        this.committedParquetFileBytes = provider.getMeter()
                .gaugeBuilder("ursa.storage.compact.committed.parquet.file.bytes")
                .setDescription("Compaction service committed parquet file bytes size")
                .setUnit(Unit.Bytes.toString())
                .ofLongs()
                .build();
        this.quarantinedTopicsCount = provider.getMeter()
                .gaugeBuilder("ursa.storage.compact.quarantined.topics.count")
                .setDescription("Compaction service quarantined topics count")
                .setUnit(Unit.Messages.toString())
                .ofLongs()
                .build();

        this.topicsInDLQ = provider.getMeter()
                .gaugeBuilder("ursa.storage.compact.topics.in.dlq")
                .setDescription("Compaction service topics in DLQ")
                .setUnit(Unit.Messages.toString())
                .ofLongs()
                .build();
        this.tasksInDLQ = provider.getMeter()
                .gaugeBuilder("ursa.storage.compact.tasks.in.dlq")
                .setDescription("Compaction service tasks in DLQ")
                .setUnit(Unit.Messages.toString())
                .ofLongs()
                .build();

        this.tasksInInitState = provider.getMeter()
                .gaugeBuilder("ursa.storage.compact.tasks.in.init.state")
                .setDescription("Compaction service tasks in INIT state")
                .setUnit(Unit.Messages.toString())
                .ofLongs()
                .build();
        this.tasksInCompactedState = provider.getMeter()
                .gaugeBuilder("ursa.storage.compact.tasks.in.compacted.state")
                .setDescription("Compaction service tasks in COMPACTED state")
                .setUnit(Unit.Messages.toString())
                .ofLongs()
                .build();
        this.tasksInPreparedCommitState = provider.getMeter()
                .gaugeBuilder("ursa.storage.compact.tasks.in.prepared.commit.state")
                .setDescription("Compaction service tasks in PREPARED_COMMIT state")
                .setUnit(Unit.Messages.toString())
                .ofLongs()
                .build();
        this.tasksInCommittedState = provider.getMeter()
                .gaugeBuilder("ursa.storage.compact.tasks.in.committed.state")
                .setDescription("Compaction service tasks in COMMITTED state")
                .setUnit(Unit.Messages.toString())
                .ofLongs()
                .build();
        this.nonCommittableTaskCount = provider.getMeter()
                .gaugeBuilder("ursa.storage.compact.non.committable.task.count")
                .setDescription("Compaction service non-committable task count")
                .setUnit(Unit.Messages.toString())
                .ofLongs()
                .build();
        this.compactionErrorHappenTime = provider.getMeter()
            .gaugeBuilder("ursa.storage.compact.tasks.error.happen.time")
            .setDescription("Compaction service tasks error code")
            .ofLongs()
            .build();
        this.lastCommitTime = provider.getMeter()
            .gaugeBuilder("ursa.storage.compact.tasks.last.commit.time")
            .setDescription("Compaction service last commit time")
            .ofLongs()
            .build();
        this.lakehouseMetadataFileSize = provider.getMeter()
            .gaugeBuilder("ursa.storage.compact.lakehouse.metadata.file.size")
            .setDescription("Compaction service lakehouse metadata file size")
            .setUnit(Unit.Bytes.toString())
            .ofLongs()
            .build();
    }
}
