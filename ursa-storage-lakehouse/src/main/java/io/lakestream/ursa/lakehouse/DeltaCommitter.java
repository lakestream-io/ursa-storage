/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.lakehouse;

import io.delta.kernel.types.StructType;
import io.delta.kernel.utils.CloseableIterator;
import io.lakestream.api.materialization.TableIdentifier;
import io.lakestream.ursa.compaction.task.CompactStreamTask;
import io.lakestream.ursa.lakehouse.delta.AddFileAction;
import io.lakestream.ursa.lakehouse.delta.DeltaTable;
import io.lakestream.ursa.lakehouse.delta.ExternalDeltaTableFactory;
import io.lakestream.ursa.lakehouse.delta.ManagedDeltaTable;
import io.lakestream.ursa.lakehouse.exception.LakehouseException;
import io.lakestream.ursa.lakehouse.utils.AvroSchemaUtilExtended;
import io.lakestream.ursa.lakehouse.utils.StreamTableNaming;
import io.lakestream.ursa.lakehouse.v2.MessageId;
import io.lakestream.ursa.lakehouse.writer.ParquetFileStat;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.commons.lang3.StringUtils;

@Slf4j
public class DeltaCommitter implements LakehouseCommitter {

    public static final String SCHEMA_TAG = "schema.tag";
    private final Map<String, MessageId> topicMessageIdMap = new ConcurrentHashMap<>();
    private final LakehouseConfiguration config;
    private final String parentTopic;

    private DeltaTable deltaTable;

    public DeltaCommitter(LakehouseConfiguration config, String parentTopic) {
        this(config, parentTopic, StreamTableNaming.resolve(parentTopic, config.getProperties()));
    }

    public DeltaCommitter(
            LakehouseConfiguration config,
            String parentTopic,
            TableIdentifier resolvedIdentifier) {
        this.config = config;
        this.parentTopic = parentTopic;
        String destination = StreamTableNaming.qualifiedName(resolvedIdentifier);
        boolean isManagedMode = config.getStreamTableMode() == LakehouseConfiguration.StreamTableMode.MANAGED;
        if (isManagedMode) {
            this.deltaTable = new ManagedDeltaTable(config, destination);
        } else {
            this.deltaTable = ExternalDeltaTableFactory.getDeltaTable(config, destination);
        }
    }

    public boolean tableExists() {
        return deltaTable.tableExists();
    }

    @Override
    public void createTable(Schema schema) throws LakehouseException {
        createTable(AvroSchemaUtilExtended.toDelta(schema, config.isVariantTypeEnabled()));
    }

    public void createTable(StructType deltaSchema) {
        deltaTable.createDeltaTable(null, deltaSchema);
    }

    @Override
    public boolean isTheCompactStreamTaskCommitted(CompactStreamTask compactStreamTask) throws IOException {
        MessageId compactTaskEndMessageId =
            new MessageId(compactStreamTask.getStreamId(), compactStreamTask.getStartOffset());

        String taskTopic = compactStreamTask.getTopic();
        MessageId committedMessageId = topicMessageIdMap.get(taskTopic);
        if (committedMessageId != null && committedMessageId.compareTo(compactTaskEndMessageId) > 0) {
            return true;
        }

        try (CloseableIterator<AddFileAction> addFileActionsIterator = deltaTable.getTableAddActionIterator()) {
            while (addFileActionsIterator.hasNext()) {
                AddFileAction allFile = addFileActionsIterator.next();
                Map<String, String> tags = allFile.getTags();
                if (tags == null || tags.isEmpty()) {
                    continue;
                }
                boolean hasTopicMessageIdTags = tags.containsKey("topic")
                    && tags.containsKey("streamId")
                    && tags.containsKey("endOffset");
                boolean ordered = tags.containsKey(DeltaTable.ORDER_TAG);
                if (hasTopicMessageIdTags) {
                    String topic = tags.get("topic");
                    String streamIdStr = tags.get("streamId");
                    if (StringUtils.isEmpty(streamIdStr)) {
                        continue;
                    }
                    String endOffsetStr = tags.get("endOffset");
                    if (StringUtils.isEmpty(endOffsetStr)) {
                        continue;
                    }
                    long committedStreamId;
                    long committedEndOffset;
                    try {
                        committedStreamId = Long.parseLong(streamIdStr);
                        committedEndOffset = Long.parseLong(endOffsetStr);
                    } catch (NumberFormatException e) {
                        log.warn("Failed to parse committed message id from tags: {}", tags, e);
                        continue;
                    }
                    committedMessageId  = new MessageId(committedStreamId, committedEndOffset);
                    if (ordered) {
                        topicMessageIdMap.merge(topic, committedMessageId, (oldVal, newVal) -> {
                            if (newVal.compareTo(oldVal) > 0) {
                                return newVal;
                            } else {
                                return oldVal;
                            }
                        });
                    }
                    if (!compactStreamTask.getTopic().equals(topic)) {
                        continue;
                    }
                    // Inclusive source ranges
                    // task1 0-[0, 10]
                    // task2 1-[11, 20]
                    // task3 1-[21, 30]

                    // committed = 1:20
                    // compactTaskEndMessageId = 0:0 -> committed
                    // compactTaskEndMessageId = 1:11 -> committed
                    // compactTaskEndMessageId = 1:21 -> not committed
                    // compactTaskEndMessageId = 1:31 -> not committed
                    // compactTaskEndMessageId = 2:0 -> not committed

                    // Ursa
                    // task1 [0, 10)
                    // task2 [10, 20)
                    // task3 [20, 30)

                    // committed = 0:20
                    // compactTaskEndMessageId = 0:0 -> committed
                    // compactTaskEndMessageId = 0:10 -> committed
                    // compactTaskEndMessageId = 0:20 -> not committed
                    // compactTaskEndMessageId = 0:30 -> not committed
                    if (committedMessageId.compareTo(compactTaskEndMessageId) > 0) {
                        return true;
                    }
                    if (ordered) {
                        return false;
                    }
                } else {
                    //for old logic
                    if (!tags.containsKey("streamId")) {
                        continue;
                    }
                    try {
                        long streamId = Long.parseLong(tags.get("streamId"));
                        if (streamId != compactStreamTask.getStreamId()) {
                            continue;
                        }
                        long startOffset = Long.parseLong(tags.get("startOffset"));
                        long endOffset = Long.parseLong(tags.get("endOffset"));
                        if (compactStreamTask.getStartOffset() == startOffset
                            && compactStreamTask.getEndOffset() == endOffset) {
                            return true;
                        }
                    } catch (NumberFormatException e) {
                        log.warn("Failed to parse streamId from tags: {}", tags, e);
                    }
                }
            }
        } catch (RuntimeException e) {
            throw new IOException("Failed to scan delta add actions", e);
        }
        return false;
    }

    @Override
    public long commit(List<ParquetFileStat> fileStats)
            throws LakehouseException {
        return deltaTable.commit(fileStats);
    }

    @Override
    public void delete(List<ParquetFileStat> fileStats) throws LakehouseException {
        deltaTable.delete(fileStats);
    }

    @Override
    public void updateTablePropertiesIfNeeded(Map<String, String> properties) throws LakehouseException {
        //
    }

    @Override
    public String getName() {
        return "delta-commiter";
    }

    @Override
    public void close() throws Exception {

    }
}
