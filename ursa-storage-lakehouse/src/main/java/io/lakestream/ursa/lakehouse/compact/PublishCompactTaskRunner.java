/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.lakehouse.compact;

import io.confluent.kafka.schemaregistry.client.SchemaMetadata;
import io.lakestream.api.EntryHeader;
import io.lakestream.ursa.compaction.CompactTaskManager;
import io.lakestream.ursa.compaction.DynamicConfigs;
import io.lakestream.ursa.compaction.metrics.CompactionMetrics;
import io.lakestream.ursa.compaction.task.CompactStreamTask;
import io.lakestream.ursa.compaction.task.CompactedOffset;
import io.lakestream.ursa.compaction.task.OffsetRange;
import io.lakestream.ursa.compaction.task.PreparedCompactStreamTask;
import io.lakestream.ursa.lakehouse.exception.FetchSchemaFailedException;
import io.lakestream.ursa.lakehouse.exception.SchemaNotFoundException;
import io.lakestream.ursa.lakehouse.exception.TopicNotFoundException;
import io.lakestream.ursa.lakehouse.schema.SchemaRegistry;
import io.lakestream.ursa.lakehouse.utils.TopicName;
import io.lakestream.ursa.storage.StorageApi;
import io.lakestream.ursa.storage.impl.StorageConfig;
import io.lakestream.ursa.storage.impl.compaction.StartStopRunner;
import io.lakestream.ursa.storage.impl.compaction.TopicManager;
import io.lakestream.ursa.storage.impl.compaction.TopicMetadata;
import io.lakestream.ursa.storage.impl.compaction.TopicProvider;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.oxia.client.api.exceptions.KeyAlreadyExistsException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.Pair;

@Slf4j
public class PublishCompactTaskRunner implements Runnable, StartStopRunner {

    private Future<?> scanTopicFuture;
    private volatile boolean isCancel = false;
    private final TopicManager topicManager;
    private final TopicProvider topicProvider;
    private final ExecutorService scanTopicExecutor;
    @Getter
    private final ScheduledExecutorService publishTaskExecutor;
    private final int checkMessageStepLength;
    private final StorageApi storageApi;
    private final CompactTaskManager compactTaskManager;
    private final long compactedFileSizeLimit;
    private final long waitForAvailableTopicIntervalInMs;
    private final SchemaRegistry schemaRegistry;
    private final Set<String> schemaCacheTopics = ConcurrentHashMap.newKeySet();
    @Getter
    private final Map<String, Long> fetchSchemaFailedQuarantineTime = new ConcurrentHashMap<>();
    @Getter
    private final Map<String, Long> notEnoughDataQuarantineTime = new ConcurrentHashMap<>();
    private final Map<String, AtomicInteger> schemaNotFoundCont = new ConcurrentHashMap<>();
    private final Map<Long, Long> lastTailUnCompactTimestampMap = new ConcurrentHashMap<>();
    private final long retryableTopicQuarantineInMs;
    private final long nonRetryableTopicQuarantineInMs;
    private final long tailCompactDataVisibilityIntervalInMs;
    private final CompactionMetrics compactionMetrics;
    private final Properties baseProperties;
    @Getter
    private final Set<String> runningTopics = ConcurrentHashMap.newKeySet();
    private final Map<String, Future<?>> publishTaskFutures = new ConcurrentHashMap<>();
    @Getter
    private final Map<String, ScheduledFuture<?>> delayPublishTaskFutures = new ConcurrentHashMap<>();
    private final List<String> removedTmpTopics = new ArrayList<>();
    private long lastPrintTime = 0;

    public PublishCompactTaskRunner(StorageApi storageApi, CompactTaskManager compactTaskManager,
                                    TopicManager topicManager,
                                    ExecutorService scanTopicExecutor,
                                    ScheduledExecutorService publishTaskExecutor,
                                    TopicProvider topicProvider, StorageConfig storageConfig,
                                    SchemaRegistry schemaRegistry,
                                    CompactionMetrics compactionMetrics) {
        this.storageApi = storageApi;
        this.compactTaskManager = compactTaskManager;
        this.topicManager = topicManager;
        this.topicProvider = topicProvider;
        this.retryableTopicQuarantineInMs =
                TimeUnit.SECONDS.toMillis(storageConfig.getRetryableQuarantineInSeconds());
        this.nonRetryableTopicQuarantineInMs =
                TimeUnit.SECONDS.toMillis(storageConfig.getNonRetryableQuarantineInSeconds());
        this.scanTopicExecutor = scanTopicExecutor;
        this.publishTaskExecutor = publishTaskExecutor;
        this.checkMessageStepLength = storageConfig.getCheckCompactMessageStepLength();
        this.compactedFileSizeLimit = storageConfig.getCompactedFileSizeLimit();
        this.waitForAvailableTopicIntervalInMs =
                TimeUnit.SECONDS.toMillis(storageConfig.getRefreshLocalTaskIntervalInSeconds());
        this.tailCompactDataVisibilityIntervalInMs =
                TimeUnit.SECONDS.toMillis(storageConfig.getTailCompactDataVisibilityIntervalInSeconds());
        this.schemaRegistry = schemaRegistry;
        this.compactionMetrics = compactionMetrics;
        this.baseProperties = storageConfig.getProperties();
    }

    public enum SchemaStatus {
        SUPPORTED,
        NOT_SUPPORTED,
        FETCH_FAILED,
        NOT_FOUND,
        INVALID
    }

    public void publishStreamCompactTask(String topic, TopicMetadata topicMetadata)
            throws IOException, ExecutionException, InterruptedException {
        long streamId = topicMetadata.streamId();
        SchemaStatus schemaStatus = checkSchemaExist(topic, schemaCacheTopics, schemaRegistry);
        if (schemaStatus != SchemaStatus.SUPPORTED) {
            if (retryableTopicQuarantineInMs <= 0) {
                return;
            }

            long quarantineUntil = System.currentTimeMillis();
            switch (schemaStatus) {
                case FETCH_FAILED:
                    quarantineUntil += retryableTopicQuarantineInMs;
                    log.info("Quarantine topic {} for {}ms until {} due to fetch schema failed",
                        topic, retryableTopicQuarantineInMs, quarantineUntil);
                    fetchSchemaFailedQuarantineTime.put(TopicName.get(topic).getPartitionedTopicName(),
                        quarantineUntil);
                    return;
                case INVALID:
                case NOT_SUPPORTED:
                    quarantineUntil += nonRetryableTopicQuarantineInMs;
                    log.info("Quarantine topic {} for {}ms until {} due to not supported schema",
                        topic, nonRetryableTopicQuarantineInMs, quarantineUntil);
                    fetchSchemaFailedQuarantineTime.put(TopicName.get(topic).getPartitionedTopicName(),
                        quarantineUntil);
                    return;
                case NOT_FOUND:
                    // schema not found. It should be three cases:
                    // - The topic is newly created and doesn't write any data
                    // - The schema registry fetch schema failed
                    // - The topic has primitive schema, which doesn't register to the schema registry
                    // Retry to fetch schema when the topic has data to compact
                    break;
                default:
                    return;
            }
        }

        PreparedCompactStreamTask preparedCompactStreamTask =
            compactTaskManager.getPreparedStreamTask(streamId);
        if (preparedCompactStreamTask != null) {
            int status = preparedCompactStreamTask.getStatus();
            long recoveredPublishedOffset = OffsetRange.lastIncludedOffset(
                    preparedCompactStreamTask.getStartOffset(), preparedCompactStreamTask.getEndOffset());
            if (status == PreparedCompactStreamTask.INIT) {
                CompactStreamTask compactStreamTask = preparedCompactStreamTask.toCompactStreamTask();
                try {
                    compactTaskManager.publishCompactTask(compactStreamTask);
                } catch (ExecutionException e) {
                    if (e.getCause() instanceof KeyAlreadyExistsException) {
                        log.info("The task {} already pushed, ignore it.", compactStreamTask);
                    } else {
                        throw e;
                    }
                }
                compactTaskManager.publishPackagedTaskName(preparedCompactStreamTask.getTaskName());
                preparedCompactStreamTask.setStatus(PreparedCompactStreamTask.PUSHED_TASK);
                compactTaskManager.updatePreparedCompactTask(preparedCompactStreamTask, Optional.empty());
                compactTaskManager.updatePublishedOffset(streamId, recoveredPublishedOffset,
                        preparedCompactStreamTask.getCumulativeSize());
                recordPublishedOffsetMetrics(topic, streamId, recoveredPublishedOffset);
                compactTaskManager.deletePreparedCompactTask(streamId);
            }
            if (status == PreparedCompactStreamTask.PUSHED_TASK) {
                compactTaskManager.updatePublishedOffset(streamId, recoveredPublishedOffset,
                        preparedCompactStreamTask.getCumulativeSize());
                recordPublishedOffsetMetrics(topic, streamId, recoveredPublishedOffset);
                compactTaskManager.deletePreparedCompactTask(streamId);
            }
        }
        CompactedOffset compactedOffset = compactTaskManager.getPublishedOffset(streamId);
        if (compactedOffset == null) {
            compactedOffset = new CompactedOffset(streamId, -1, 0);
        }
        long startOffset = compactedOffset.getOffset() + 1;
        Pair<Long, Long> pair = calculateTheEndOffset(streamId, startOffset, compactedOffset.getCumulativeSize());
        if (pair == null || pair.equals(Pair.of(0L, 0L))) {
            // pair == null means the topic has new messages, but the number of new messages is not enough for the batch
            // Pair.of(0L, 0L) means the topic doesn't have new messages
            if (retryableTopicQuarantineInMs > 0) {
                long quarantineMs = Math.min(retryableTopicQuarantineInMs, tailCompactDataVisibilityIntervalInMs);
                long quarantineUntil = System.currentTimeMillis() + quarantineMs;
                // Only log the topic that has new messages but not enough for the batch
                if (pair == null) {
                    log.info("Quarantine topic {} for {}ms until {} due to not enough data",
                        topic, quarantineMs, quarantineUntil);
                }
                notEnoughDataQuarantineTime.put(TopicName.get(topic).toString(), quarantineUntil);
            }
            return;
        }
        // We have data to compaction, check if the schema is NOT_FOUND
        if (schemaStatus == SchemaStatus.NOT_FOUND) {
            String partitionedTopicName = TopicName.get(topic).getPartitionedTopicName();
            schemaCacheTopics.add(partitionedTopicName);
        }

        Long endOffset = pair.getLeft();
        Long endCumulativeSize = pair.getRight();
        long publishedOffset = OffsetRange.lastIncludedOffset(startOffset, endOffset);
        long totalSize = endCumulativeSize - compactedOffset.getCumulativeSize();
        String taskName = generateTaskName();

        // Resolve deployment defaults with topic-level overrides.
        // This ensures cluster/namespace-level defaults are applied with topic-level overrides.
        Map<String, String> resolvedProperties = resolveDynamicConfigProperties(topicMetadata.properties());

        PreparedCompactStreamTask initTask =
                new PreparedCompactStreamTask(streamId, startOffset, endOffset, totalSize, endCumulativeSize,
                        PreparedCompactStreamTask.INIT, taskName, topic, resolvedProperties);

        EntryHeader lacHeader = storageApi.getLastEntry(streamId).get().header();
        long latestOffset = OffsetRange.lastIncludedOffset(lacHeader.offset(),
                Math.addExact(lacHeader.offset(), lacHeader.numberOfMessages()));
        compactionMetrics.getLatestMessageOffset().set(latestOffset,
                Attributes.of(AttributeKey.stringKey("topic"), topic));

        compactTaskManager.publishPreparedCompactTask(initTask, Optional.empty());
        compactTaskManager.publishCompactTask(initTask.toCompactStreamTask());
        compactTaskManager.publishPackagedTaskName(initTask.getTaskName());
        initTask.setStatus(PreparedCompactStreamTask.PUSHED_TASK);
        compactTaskManager.updatePreparedCompactTask(initTask, Optional.empty());
        compactTaskManager.updatePublishedOffset(streamId, publishedOffset, endCumulativeSize);
        recordPublishedOffsetMetricsWithLatest(topic, latestOffset, publishedOffset);
        compactTaskManager.deletePreparedCompactTask(streamId);
    }

    private void recordPublishedOffsetMetrics(String topic, long streamId, long publishedOffset)
            throws ExecutionException, InterruptedException {
        EntryHeader lastEntryHeader = storageApi.getLastEntry(streamId).get().header();
        long latestOffset = OffsetRange.lastIncludedOffset(lastEntryHeader.offset(),
                Math.addExact(lastEntryHeader.offset(), lastEntryHeader.numberOfMessages()));
        compactionMetrics.getLatestMessageOffset().set(latestOffset,
                Attributes.of(AttributeKey.stringKey("topic"), topic));
        recordPublishedOffsetMetricsWithLatest(topic, latestOffset, publishedOffset);
    }

    private void recordPublishedOffsetMetricsWithLatest(String topic, long latestOffset, long publishedOffset) {
        Attributes attributes = Attributes.of(AttributeKey.stringKey("topic"), topic);
        compactionMetrics.getLatestPublishedOffset().set(publishedOffset, attributes);
        compactionMetrics.getCompactionLag().set(Math.subtractExact(latestOffset, publishedOffset), attributes);
    }

    public static SchemaStatus checkSchemaExist(String topic,
                                           Set<String> schemaCacheTopics,
                                           SchemaRegistry schemaRegistry) {
        String partitionedTopicName = TopicName.get(topic).getPartitionedTopicName();
        if (schemaCacheTopics.contains(partitionedTopicName)) {
            return SchemaStatus.SUPPORTED;
        }
        try {
            SchemaMetadata schemaMetadata = schemaRegistry.fetchLatest(topic);
            switch (schemaMetadata.getSchemaType()) {
                case "AVRO":
                case "JSON":
                case "PROTOBUF":
                    break;
                default:
                    return SchemaStatus.NOT_SUPPORTED;
            }
            schemaCacheTopics.add(partitionedTopicName);
            return SchemaStatus.SUPPORTED;
        } catch (FetchSchemaFailedException e) {
            return SchemaStatus.FETCH_FAILED;
        } catch (SchemaNotFoundException ex) {
            return SchemaStatus.NOT_FOUND;
        } catch (Exception ee) {
            log.warn("Unexpected error when fetch schema for topic {} ", topic, ee);
            return SchemaStatus.INVALID;
        }
    }

    private Map<String, String> resolveDynamicConfigProperties(Map<String, String> topicProperties) {
        String clusterName = baseProperties.getProperty("clusterName");
        DynamicConfigs dynamicConfigs = (clusterName == null || clusterName.isBlank())
            ? DynamicConfigs.of(baseProperties)
            : new DynamicConfigs(clusterName, baseProperties);
        Map<String, String> taskProperties = new HashMap<>(
                topicProperties != null ? topicProperties : Collections.emptyMap());
        dynamicConfigs.overrideWith(taskProperties);
        taskProperties.putAll(dynamicConfigs.toTaskProperties());
        return taskProperties;
    }

    private String generateTaskName() {
        return UUID.randomUUID().toString();
    }

    private Pair<Long, Long> calculateTheEndOffset(long streamId, long startOffset, long startCumulativeSize)
            throws ExecutionException, InterruptedException {
        long baseOffset = startOffset + checkMessageStepLength - 1;
        long endOffset;
        long endCumulativeSize;
        long diffSize;
        long latestWriteTimestamp = -1;
        while (true) {
            EntryHeader entryHeader = storageApi.readEntryHeader(streamId, baseOffset).get();
            if (EntryHeader.NOT_FOUND == entryHeader) {
                entryHeader = storageApi.getLastEntry(streamId).get().header();
                diffSize = entryHeader.cumulativeSize() - startCumulativeSize;
                endOffset = entryHeader.offset() + entryHeader.numberOfMessages();
                endCumulativeSize = entryHeader.cumulativeSize();
                if (latestWriteTimestamp == -1) {
                    EntryHeader firstEntryHeader = storageApi.readEntryHeader(streamId, startOffset).get();
                    if (EntryHeader.NOT_FOUND != firstEntryHeader) {
                        latestWriteTimestamp = firstEntryHeader.writtenTimestamp();
                    } else {
                        latestWriteTimestamp = entryHeader.writtenTimestamp();
                    }
                }
                break;
            }
            diffSize = entryHeader.cumulativeSize() - startCumulativeSize;
            endOffset = entryHeader.offset() + entryHeader.numberOfMessages();
            endCumulativeSize = entryHeader.cumulativeSize();
            // record first uncompacted message's write timestamp
            if (latestWriteTimestamp == -1) {
                latestWriteTimestamp = entryHeader.writtenTimestamp();
            }
            if (compactedFileSizeLimit > 0 && diffSize >= compactedFileSizeLimit) {
                break;
            }
            baseOffset += checkMessageStepLength;
        }
        if (compactedFileSizeLimit > 0 && diffSize >= compactedFileSizeLimit) {
            compactionMetrics.getPublishedTaskBytes().set(diffSize);
            return Pair.of(endOffset, endCumulativeSize);
        } else {
            if (diffSize > 0) {
                if (latestWriteTimestamp != -1
                        && System.currentTimeMillis() - latestWriteTimestamp >= tailCompactDataVisibilityIntervalInMs) {
                    compactionMetrics.getPublishedTaskBytes().set(diffSize);
                    return Pair.of(endOffset, endCumulativeSize);
                } else if (latestWriteTimestamp == -1) { // use to handle `latestWriteTimestamp == -1` case
                    Long lastTailUnCompactTimestamp = lastTailUnCompactTimestampMap.get(streamId);
                    if (lastTailUnCompactTimestamp == null) {
                        lastTailUnCompactTimestampMap.put(streamId, System.currentTimeMillis());
                        return null;
                    } else if (System.currentTimeMillis() - lastTailUnCompactTimestamp
                            >= tailCompactDataVisibilityIntervalInMs) {
                        lastTailUnCompactTimestampMap.remove(streamId);
                        return Pair.of(endOffset, endCumulativeSize);
                    } else {
                        return null;
                    }
                } else {
                    return null;
                }
            } else {
                // Represent no new messages in the topic
                return Pair.of(0L, 0L);
            }
        }
    }

    @Override
    public void run() {
        if (isCancel) {
            return;
        }
        try {
            List<String> topics = topicProvider.getAllTopics();
            removedTmpTopics.clear();
            for (String runningTopic : runningTopics) {
                if (!topics.contains(runningTopic)) {
                    removedTmpTopics.add(runningTopic);
                }
            }
            removedTmpTopics.forEach(ele -> {
                runningTopics.remove(ele);
                Future<?> publishTaskFuture = publishTaskFutures.remove(ele);
                if (publishTaskFuture != null) {
                    publishTaskFuture.cancel(false);
                }
                ScheduledFuture<?> delayPublishTaskFuture = delayPublishTaskFutures.remove(ele);
                if (delayPublishTaskFuture != null) {
                    delayPublishTaskFuture.cancel(false);
                }

            });
            if (topics.isEmpty()) {
                if (log.isDebugEnabled()) {
                    log.debug("No available topic, wait for {}ms. ", waitForAvailableTopicIntervalInMs);
                }
                Thread.sleep(waitForAvailableTopicIntervalInMs);
                return;
            }
            for (String pickedTopic : topics) {
                if (runningTopics.contains(pickedTopic)) {
                    continue;
                }
                runningTopics.add(pickedTopic);
                publishTaskFutures.put(pickedTopic, submitPublishTaskNow(pickedTopic));
            }
            Thread.sleep(1000);
        } catch (Throwable e) {
            log.warn("Error during the publish stream compact task.", e);
            compactionMetrics.getPublishTaskFailedCount().increment();
            // TODO we need shutdown the runner when encontered exception and trigger another round leader election.
        } finally {
            start();
        }
    }

    public ScheduledFuture<?> submitPublishTaskDelay(String pickedTopic, long delayMillis) {
        return publishTaskExecutor.schedule(() -> {
            publishTaskFutures.put(pickedTopic, submitPublishTaskNow(pickedTopic));
        }, delayMillis, TimeUnit.MILLISECONDS);
    }

    public Future<?> submitPublishTaskNow(String pickedTopic) {
        return publishTaskExecutor.submit(() -> {
            if (isCancel || !runningTopics.contains(pickedTopic)) {
                return null;
            }
            delayPublishTaskFutures.remove(pickedTopic);
            long current = System.currentTimeMillis();
            if (retryableTopicQuarantineInMs > 0 && !fetchSchemaFailedQuarantineTime.isEmpty()) {
                String fetchSchemaFailedTopic =
                        TopicName.get(pickedTopic).getPartitionedTopicName();
                Long quarantineTime = fetchSchemaFailedQuarantineTime.get(fetchSchemaFailedTopic);
                if (quarantineTime != null) {
                    long differTime = current - quarantineTime;
                    if (differTime >= 0) {
                        fetchSchemaFailedQuarantineTime.remove(pickedTopic);
                    } else {
                        //delay
                        ScheduledFuture<?> delayPublishTask = submitPublishTaskDelay(pickedTopic, -differTime);
                        delayPublishTaskFutures.put(pickedTopic, delayPublishTask);
                        return null;
                    }
                }
            }

            if (tailCompactDataVisibilityIntervalInMs > 0 && !notEnoughDataQuarantineTime.isEmpty()) {
                Long quarantineTime = notEnoughDataQuarantineTime.get(pickedTopic);
                if (quarantineTime != null) {
                    long differTime = current - quarantineTime;
                    if (differTime >= 0) {
                        notEnoughDataQuarantineTime.remove(pickedTopic);
                    } else {
                        //delay
                        ScheduledFuture<?> delayPublishTask = submitPublishTaskDelay(pickedTopic, -differTime);
                        delayPublishTaskFutures.put(pickedTopic, delayPublishTask);
                        return null;
                    }
                }
            }
            boolean delayTask = false;
            try {
                TopicMetadata topicMetadata = topicManager.getTopicMetadata(pickedTopic).get();
                publishStreamCompactTask(TopicName.get(pickedTopic).toString(), topicMetadata);
            } catch (Throwable e) {
                if (e instanceof ExecutionException && e.getCause() instanceof TopicNotFoundException) {
                    delayTask = true;
                    log.warn("Can't get topic metadata for topic {}", pickedTopic);
                }

                if (System.currentTimeMillis() - lastPrintTime > 5000) {
                    log.warn("Error during the publish stream compact task for {}.", pickedTopic, e);
                    lastPrintTime = System.currentTimeMillis();
                }
            } finally {
                if (!isCancel && runningTopics.contains(pickedTopic)) {
                    if (delayTask) {
                        //delay
                        ScheduledFuture<?> delayPublishTask = submitPublishTaskDelay(pickedTopic,
                                nonRetryableTopicQuarantineInMs);
                        delayPublishTaskFutures.put(pickedTopic, delayPublishTask);
                    } else {
                        publishTaskFutures.put(pickedTopic, submitPublishTaskNow(pickedTopic));
                    }
                }
            }
            return null;
        });
    }

    @Override
    public void start() {
        if (!isCancel && scanTopicExecutor != null) {
            scanTopicFuture = scanTopicExecutor.submit(this);
        }
    }

    @Override
    public void stop() {
        isCancel = true;
        if (scanTopicFuture != null) {
            scanTopicFuture.cancel(false);
        }
        for (Future<?> publishTaskFuture : publishTaskFutures.values()) {
            publishTaskFuture.cancel(false);
        }
        publishTaskFutures.clear();

        for (ScheduledFuture<?> delayPublishTaskFuture : delayPublishTaskFutures.values()) {
            delayPublishTaskFuture.cancel(false);
        }
        delayPublishTaskFutures.clear();
    }

}
