/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.lakehouse;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import io.lakestream.ursa.compaction.task.CompactStreamTask;
import io.lakestream.ursa.lakehouse.exception.LakehouseException;
import io.lakestream.ursa.lakehouse.iceberg.IcebergCompactStreamTask;
import io.lakestream.ursa.lakehouse.iceberg.IcebergTable;
import io.lakestream.ursa.lakehouse.iceberg.TableOptions;
import io.lakestream.ursa.lakehouse.utils.AvroSchemaUtilExtended;
import io.lakestream.ursa.lakehouse.utils.StreamTableNaming;
import io.lakestream.ursa.lakehouse.v2.MessageId;
import io.lakestream.ursa.lakehouse.writer.ParquetFileStat;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;

@Slf4j
public class IcebergCommitter implements LakehouseCommitter {

    private final LakehouseConfiguration config;
    @Getter
    @VisibleForTesting
    private final IcebergTable icebergTable;
    private final TableIdentifier identifier;
    private final String parentTopic;
    private final IcebergCommitType lakehouseWriterType;
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private final Map<String, MessageId> topicMessageIdMap = new HashMap<>();
    private long lastUpdatedSnapshotSequenceNumber = -1;

    public enum IcebergCommitType {
        EXTERNAL_ICEBERG,
        MANAGED
    }


    public IcebergCommitter(LakehouseConfiguration config, String parentTopic) {
        this(config, parentTopic, StreamTableNaming.resolve(parentTopic, config.getProperties()));
    }

    public IcebergCommitter(
            LakehouseConfiguration config,
            String parentTopic,
            io.lakestream.api.materialization.TableIdentifier resolvedIdentifier) {
        this.config = config;
        this.parentTopic = parentTopic;
        this.identifier = TableIdentifier.of(
                Namespace.of(resolvedIdentifier.namespace()), resolvedIdentifier.name());
        this.icebergTable = new IcebergTable(config, identifier);
        this.lakehouseWriterType = getIcebergCommitType(config);
    }

    @Override
    public boolean tableExists() throws LakehouseException {
        return icebergTable.exists();
    }

    @Override
    public synchronized void createTable(org.apache.avro.Schema schema) throws LakehouseException {
        var tableOptionsBuilder = TableOptions.builder();
        // table schema
        var tableSchema = AvroSchemaUtilExtended.toIceberg(schema, config.isAllowIcebergV3());
        tableOptionsBuilder.schema(tableSchema);
        // TODO: We need to get the partition keys from topic properties.
        tableOptionsBuilder.identifierFields(config.getIdentifierFields());
        tableOptionsBuilder.partitionKey(config.getPartitionKey());
        tableOptionsBuilder.properties(config.getIcebergTableProperties());

        var tableOptions = tableOptionsBuilder.build();
        // todo: maybe we only need to load table? Once we supportted the schema evolution,
        //  commit processor should never create table
        icebergTable.create(tableOptions);
    }

    private void loadLatestTopicMessageIdMapIfNeeded() throws IOException {
        try {
            Snapshot currentSnapshot = icebergTable.getTable().currentSnapshot();
            if (currentSnapshot == null || lastUpdatedSnapshotSequenceNumber == currentSnapshot.sequenceNumber()) {
                return;
            }

            // Due to the snapshots iterator is not from latest to earliest order, we need to traverse all snapshots
            Iterable<Snapshot> snapshots = icebergTable.snapshots();
            if (snapshots == null) {
                return;
            }

            for (Snapshot snapshot : snapshots) {
                if (snapshot == null) {
                    continue;
                }
                // The snapshot sequence number is increasing order
                if (snapshot.sequenceNumber() <= lastUpdatedSnapshotSequenceNumber) {
                    continue;
                }

                Map<String, String> summary = snapshot.summary();
                if (summary == null) {
                    continue;
                }
                // check new tags
                if (StringUtils.isNotBlank(summary.get("lakestream.tags"))) {
                    parseTags(summary.get("lakestream.tags"), topicMessageIdMap);
                }
            }
            lastUpdatedSnapshotSequenceNumber = icebergTable.getTable().currentSnapshot().sequenceNumber();
        } catch (Exception e) {
            log.warn("Failed to load the latest topic messageId map for table: {} ", identifier, e);
            throw new IOException(e);
        }
    }

    private void parseTags(String tags, Map<String, MessageId> topicMessageIdMap) throws IOException{
        Map<String, String> tagMap = OBJECT_MAPPER.readValue(tags, Map.class);
        for (Map.Entry<String, String> entry : tagMap.entrySet()) {
            String topic = entry.getKey();
            String messageIdStr = entry.getValue();
            if (StringUtils.isBlank(messageIdStr)) {
                continue;
            }

            MessageId messageId = MessageId.fromString(messageIdStr);
            topicMessageIdMap.merge(topic, messageId, (oldVal, newVal) -> {
                if (newVal.compareTo(oldVal) > 0) {
                    return newVal;
                } else {
                    return oldVal;
                }
            });
        }
    }

    public boolean isTheCompactStreamTaskCommitted(CompactStreamTask compactStreamTask) throws IOException {
        boolean result = false;
        try {
            Iterable<Snapshot> snapshots = icebergTable.snapshots();
            if (snapshots == null || !snapshots.iterator().hasNext()) {
                return result;
            }

            loadLatestTopicMessageIdMapIfNeeded();
            if (topicMessageIdMap.containsKey(compactStreamTask.getTopic())) {
                MessageId committedMessageId = topicMessageIdMap.get(compactStreamTask.getTopic());
                MessageId compactTaskEndMessageId =
                        new MessageId(compactStreamTask.getStreamId(), compactStreamTask.getStartOffset());
                // Inclusive source ranges
                // task1 [0, 10]
                // task2 [11, 20]
                // task3 [21, 30]

                // committed = 0:20
                // compactTaskEndMessageId = 0:0 -> committed
                // compactTaskEndMessageId = 0:11 -> committed
                // compactTaskEndMessageId = 0:21 -> not committed
                // compactTaskEndMessageId = 0:31 -> not committed

                // Ursa
                // task1 [0, 10)
                // task2 [10, 20)
                // task3 [20, 30)

                // committed = 0:20
                // compactTaskEndMessageId = 0:0 -> committed
                // compactTaskEndMessageId = 0:10 -> committed
                // compactTaskEndMessageId = 0:20 -> committed
                // compactTaskEndMessageId = 0:30 -> not committed
                return committedMessageId.compareTo(compactTaskEndMessageId) >= 0;
            }

            // for old logic compatibility
            long targetStreamId = compactStreamTask.getStreamId();
            for (Snapshot snapshot : snapshots) {
                Map<String, String> summary = snapshot.summary();
                if (summary == null) {
                    continue;
                }
                // check by task name first
                if (compactStreamTask.getTaskName().equals(summary.get("taskId"))) {
                    return true;
                }

                if (summary.get(targetStreamId + ".streamId") == null) {
                    continue;
                }

                long startOffset = Long.parseLong(summary.get(targetStreamId + ".startOffset"));
                long endOffset = Long.parseLong(summary.get(targetStreamId + ".endOffset"));
                long totalSize = Long.parseLong(summary.get(targetStreamId + ".totalSize"));
                long cumulativeSize = Long.parseLong(summary.get(targetStreamId + ".cumulativeSize"));
                if (compactStreamTask.getStartOffset() == startOffset
                    && compactStreamTask.getEndOffset() == endOffset
                    && compactStreamTask.getTotalSize() == totalSize
                    && compactStreamTask.getCumulativeSize() == cumulativeSize) {
                    if (!(compactStreamTask instanceof IcebergCompactStreamTask)) {
                        String filePath = summary.get(targetStreamId + ".filePath");
                        return compactStreamTask.getFilePath().equals(filePath);
                    }
                    result = true;
                    return result;
                }
            }

            return result;
        } finally {
            if (result) {
                log.info("The compactStreamTask: {} is already committed to iceberg table: {}",
                    compactStreamTask.getTaskName(), identifier);
            }
        }
    }

    @Override
    public long commit(List<ParquetFileStat> fileStats) throws LakehouseException {
        if (!tableExists()) {
            throw new LakehouseException("Table not exists for topic: " + parentTopic);
        }
        return icebergTable.commit(lakehouseWriterType, fileStats);
    }

    @Override
    public void delete(List<ParquetFileStat> fileStats) throws LakehouseException {
        if (!tableExists()) {
            throw new LakehouseException("Table not exists for topic: " + parentTopic);
        }
        log.info("Delete iceberg table files for topic: {}, fileStats: {}", parentTopic, fileStats);
        icebergTable.delete(fileStats);
    }

    @Override
    public void updateTablePropertiesIfNeeded(Map<String, String> properties) throws LakehouseException {
        if (!tableExists()) {
            throw new LakehouseException("Table not exists for topic: " + parentTopic);
        }

        icebergTable.updateTableProperties(properties);
    }

    private boolean isPartitionKeysExistsInSchema(Schema schema, List<String> partitionKeys) {
        for (String key : partitionKeys) {
            if (schema.findField(key) == null) {
                return false;
            }
        }
        return true;
    }

    @Override
    public String getName() {
        return "iceberg";
    }

    protected static IcebergCommitType getIcebergCommitType(LakehouseConfiguration config) {
        if (config.getStreamTableMode() == LakehouseConfiguration.StreamTableMode.MANAGED) {
            return IcebergCommitType.MANAGED;
        } else if (config.getStreamTableMode() == LakehouseConfiguration.StreamTableMode.EXTERNAL
            && config.getLakehouseType() == LakehouseConfiguration.LakehouseType.ICEBERG) {
            return IcebergCommitType.EXTERNAL_ICEBERG;
        } else {
            throw new UnsupportedOperationException("Not support lakehouse type: " + config.getLakehouseType());
        }
    }

    @Override
    public void close() throws Exception {
        icebergTable.close();
    }
}
