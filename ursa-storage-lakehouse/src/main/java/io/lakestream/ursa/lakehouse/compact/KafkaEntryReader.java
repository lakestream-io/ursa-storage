/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.lakehouse.compact;

import io.lakestream.api.EntryHeader;
import io.lakestream.ursa.exception.DataSourceException;
import io.lakestream.ursa.exception.ExceptionCode;
import io.lakestream.ursa.lakehouse.utils.TopicName;
import io.lakestream.ursa.materialization.serde.GenericEntry;
import io.lakestream.ursa.materialization.serde.KafkaEntry;
import io.lakestream.ursa.storage.Entry;
import java.time.Duration;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetOutOfRangeException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;

@Slf4j
public class KafkaEntryReader implements IEntryReader {

    private final Consumer<byte[], byte[]> consumer;
    private final String topic;
    private final int partitionIndex;
    private final long startOffset;
    private final long endOffset;
    private final TopicPartition partition;
    private long currentOffset;
    private boolean initialized = false;
    private final EntryReaderOptions options;
    private final String expectedTopicId;
    private final TopicIdLookup topicIdLookup;
    private Iterator<ConsumerRecord<byte[], byte[]>> currentBatchIterator;

    public KafkaEntryReader(String topic, long startOffset, long endOffset,
                            double avgEntrySize, EntryReaderOptions options,
                            Properties kafkaProperties) {
        this(createConsumer(kafkaProperties), topic, startOffset, endOffset, avgEntrySize, options);
    }

    KafkaEntryReader(Consumer<byte[], byte[]> consumer, String topic, long startOffset, long endOffset,
                     double avgEntrySize, EntryReaderOptions options) {
        this(consumer, topic, startOffset, endOffset, avgEntrySize, options, null, null);
    }

    KafkaEntryReader(Consumer<byte[], byte[]> consumer, String topic, long startOffset, long endOffset,
                     double avgEntrySize, EntryReaderOptions options, String expectedTopicId,
                     TopicIdLookup topicIdLookup) {
        this.consumer = consumer;
        var topicName = TopicName.get(topic);
        this.topic = TopicName.get(topicName.getPartitionedTopicName()).getLocalName();
        this.partitionIndex = topicName.getPartitionIndex() == -1 ? 0 : topicName.getPartitionIndex();
        this.startOffset = startOffset;
        this.endOffset = endOffset;
        this.partition = new TopicPartition(this.topic, partitionIndex);
        this.currentOffset = startOffset;
        this.options = options;
        this.expectedTopicId = expectedTopicId;
        this.topicIdLookup = topicIdLookup;
    }

    static Consumer<byte[], byte[]> createConsumer(Properties kafkaProperties) {
        Properties props = new Properties();
        kafkaProperties.forEach((key, value) -> props.put(key, value));
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        return new KafkaConsumer<>(props);
    }

    private void seekToStartOffset() throws DataSourceException {
        try {
            validateTopicIncarnation();
            consumer.assign(Collections.singletonList(partition));
            currentOffset = adjustStartOffsetForRetention(startOffset);
            consumer.seek(partition, currentOffset);
            validateTopicIncarnation();
            initialized = true;
            log.debug("Seeked to start offset {} for topic {} partition {}", startOffset, topic, partitionIndex);
        } catch (DataSourceException e) {
            throw e;
        } catch (Exception e) {
            log.error("Failed to seek to start offset {} for topic {} partition {}", startOffset, topic,
                partitionIndex, e);
            throw new DataSourceException(ExceptionCode.SOURCE_CLIENT_ERROR, "Failed to seek to start offset", e);
        }
    }

    private void readInternally() throws DataSourceException {
        if (currentBatchIterator != null && currentBatchIterator.hasNext()) {
            return;
        }
        var records = poll();
        if (records.isEmpty()) {
            throw new DataSourceException(ExceptionCode.NO_MORE_RECORDS, "No more records available");
        }
        currentBatchIterator = records.iterator();
    }

    private ConsumerRecords<byte[], byte[]> poll() throws DataSourceException {
        validateTopicIncarnation();
        try {
            ConsumerRecords<byte[], byte[]> records = consumer.poll(Duration.ofSeconds(3));
            validateTopicIncarnation();
            return records;
        } catch (OffsetOutOfRangeException e) {
            validateTopicIncarnation();
            currentOffset = adjustStartOffsetForRetention(currentOffset);
            consumer.seek(partition, currentOffset);
            ConsumerRecords<byte[], byte[]> records = consumer.poll(Duration.ofSeconds(3));
            validateTopicIncarnation();
            return records;
        }
    }

    void validateTopicIncarnation() throws DataSourceException {
        if (topicIdLookup == null) {
            return;
        }
        Uuid expected;
        try {
            if (expectedTopicId == null || expectedTopicId.isBlank()) {
                throw new IllegalArgumentException("missing sourceTopicId");
            }
            expected = Uuid.fromString(expectedTopicId);
        } catch (IllegalArgumentException e) {
            throw new DataSourceException(ExceptionCode.NO_SUCH_LOG,
                    "Kafka source task has a missing or invalid sourceTopicId for topic " + topic, e);
        }

        try {
            Uuid actual = topicIdLookup.topicId(topic);
            if (!expected.equals(actual)) {
                throw new DataSourceException(ExceptionCode.NO_SUCH_LOG,
                        "Kafka topic incarnation changed for " + topic + ": expected "
                                + expected + " but found " + actual);
            }
        } catch (DataSourceException e) {
            throw e;
        } catch (Exception e) {
            if (causedByUnknownTopic(e)) {
                throw new DataSourceException(ExceptionCode.NO_SUCH_LOG,
                        "Kafka source topic no longer exists: " + topic, e);
            }
            if (causedByInterruption(e)) {
                Thread.currentThread().interrupt();
            }
            throw new DataSourceException(ExceptionCode.SOURCE_READ_ERROR,
                    "Failed to verify Kafka topic incarnation for " + topic, e);
        }
    }

    private static boolean causedByUnknownTopic(Throwable error) {
        return causedBy(error, UnknownTopicOrPartitionException.class);
    }

    private static boolean causedByInterruption(Throwable error) {
        return causedBy(error, InterruptedException.class);
    }

    private static boolean causedBy(Throwable error, Class<? extends Throwable> type) {
        Throwable current = error;
        while (current != null) {
            if (type.isInstance(current)) {
                return true;
            }
            current = current.getCause();
        }
        return false;
    }

    private long adjustStartOffsetForRetention(long requestedOffset) throws DataSourceException {
        Map<TopicPartition, Long> beginningOffsets = consumer.beginningOffsets(Collections.singletonList(partition));
        long beginningOffset = beginningOffsets.getOrDefault(partition, 0L);
        if (requestedOffset >= beginningOffset) {
            return requestedOffset;
        }
        if (endOffset >= 0 && beginningOffset >= endOffset) {
            var msg = String.format("The offset range [%d, %d) for topic %s partition %d has been deleted by "
                                    + "retention policy. Current beginning offset is %d.",
                startOffset, endOffset, topic, partitionIndex, beginningOffset);
            throw new DataSourceException(ExceptionCode.NO_SUCH_OFFSET, msg);
        }
        log.info("Adjust Kafka read start offset from {} to {} for topic {} partition {} because older offsets "
                 + "were deleted by retention policy.", requestedOffset, beginningOffset, topic, partitionIndex);
        return beginningOffset;
    }

    @Override
    public GenericEntry read() throws DataSourceException {
        if (!initialized) {
            seekToStartOffset();
        }

        if (endOffset >= 0 && currentOffset >= endOffset) {
            return null;
        }

        try {
            while (true) {
                readInternally();
                if (currentBatchIterator == null || !currentBatchIterator.hasNext()) {
                    return null;
                }

                var record = currentBatchIterator.next();
                if (record.offset() < currentOffset) {
                    continue;
                }
                if (endOffset >= 0 && record.offset() >= endOffset) {
                    currentOffset = record.offset();
                    return null;
                }
                currentOffset = Math.addExact(record.offset(), 1L);

                var kafkaEntry = new KafkaEntry(record.key(), record.value());
                var payload = kafkaEntry.toByteBuf();
                try {
                    EntryHeader header = new EntryHeader(
                        record.offset(),
                        1,
                        record.timestamp(),
                        payload.readableBytes(),
                        record.offset()
                    );
                    return new GenericEntry(Entry.of(header, payload));
                } catch (Throwable error) {
                    payload.release();
                    throw error;
                }
            }
        } catch (DataSourceException e) {
            throw e;
        } catch (Exception e) {
            log.error("Error reading from Kafka topic {} partition {} at offset {}", topic,
                partitionIndex, currentOffset, e);
            throw new DataSourceException(ExceptionCode.SOURCE_READ_ERROR, "Error reading from Kafka", e);
        }
    }

    @Override
    public void close() {
        if (consumer != null) {
            try {
                consumer.close();
                log.debug("Closed Kafka consumer for topic {} partition {}", topic, partitionIndex);
            } catch (Exception e) {
                log.error("Error closing Kafka consumer for topic {} partition {}", topic, partitionIndex, e);
            }
        }
    }

    @FunctionalInterface
    interface TopicIdLookup {
        Uuid topicId(String topic) throws Exception;
    }
}
