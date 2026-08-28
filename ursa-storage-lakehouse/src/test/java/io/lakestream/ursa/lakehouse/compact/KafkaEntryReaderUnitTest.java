/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.lakehouse.compact;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.lakestream.ursa.exception.DataSourceException;
import io.lakestream.ursa.exception.ExceptionCode;
import io.lakestream.ursa.lakehouse.LakehouseConfiguration;
import io.lakestream.ursa.materialization.serde.GenericEntry;
import io.lakestream.ursa.materialization.serde.KafkaEntry;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.junit.jupiter.api.Test;

class KafkaEntryReaderUnitTest {

    @Test
    void endOffsetIsExclusive() throws Exception {
        String topic = "range-contract";
        MockConsumer<byte[], byte[]> consumer = consumerWithRecords(topic,
                record(topic, 0, "zero"), record(topic, 1, "one"), record(topic, 2, "two"));
        try (KafkaEntryReader reader = new KafkaEntryReader(
                consumer, topic, 0, 2, 1024.0, EntryReaderOptions.DEFAULT)) {
            GenericEntry first = reader.read();
            GenericEntry second = reader.read();
            try {
                assertEquals(0, first.entry().header().offset());
                assertEquals(1, second.entry().header().offset());
                assertNull(reader.read());
            } finally {
                first.entry().payload().release();
                second.entry().payload().release();
            }
        }
    }

    @Test
    void tombstoneIsReturnedAsANullKafkaValue() throws Exception {
        String topic = "tombstone";
        byte[] key = "deleted-key".getBytes(StandardCharsets.UTF_8);
        MockConsumer<byte[], byte[]> consumer = new MockConsumer<>(OffsetResetStrategy.EARLIEST);
        TopicPartition partition = new TopicPartition(topic, 0);
        consumer.updateBeginningOffsets(Map.of(partition, 0L));
        consumer.schedulePollTask(() -> consumer.addRecord(
                new ConsumerRecord<>(topic, 0, 0, key, null)));

        try (KafkaEntryReader reader = new KafkaEntryReader(
                consumer, topic, 0, 1, 1024.0, EntryReaderOptions.DEFAULT)) {
            GenericEntry entry = reader.read();
            try {
                KafkaEntry decoded = KafkaEntry.fromByteBuf(entry.entry().payload().duplicate());
                assertArrayEquals(key, decoded.key());
                assertNull(decoded.value());
                assertEquals(entry.entry().payload().readableBytes(), entry.entry().header().entrySize());
                assertNull(reader.read());
            } finally {
                entry.entry().payload().release();
            }
        }
    }

    @Test
    void rejectsTaskWhenCurrentTopicIdDoesNotMatchBeforeAssigningConsumer() {
        String topic = "recreated-topic";
        Uuid expectedTopicId = Uuid.randomUuid();
        Uuid currentTopicId = Uuid.randomUuid();
        MockConsumer<byte[], byte[]> consumer = consumerWithRecords(topic, record(topic, 0, "new-value"));

        try (KafkaEntryReader reader = new KafkaEntryReader(
                consumer, topic + "-partition-0", 0, 1, 1024.0, EntryReaderOptions.DEFAULT,
                expectedTopicId.toString(), ignored -> currentTopicId)) {
            DataSourceException error = assertThrows(DataSourceException.class, reader::read);

            assertEquals(ExceptionCode.NO_SUCH_LOG, error.getExceptionCode());
            assertTrue(error.getMessage().contains(expectedTopicId.toString()));
            assertTrue(error.getMessage().contains(currentTopicId.toString()));
            assertTrue(consumer.assignment().isEmpty());
        }
    }

    @Test
    void rejectsRecreatedTopicAfterPollWithoutReturningItsRecords() {
        String topic = "recreated-during-poll";
        Uuid expectedTopicId = Uuid.randomUuid();
        Uuid recreatedTopicId = Uuid.randomUuid();
        AtomicReference<Uuid> currentTopicId = new AtomicReference<>(expectedTopicId);
        MockConsumer<byte[], byte[]> consumer = new MockConsumer<>(OffsetResetStrategy.EARLIEST);
        TopicPartition partition = new TopicPartition(topic, 0);
        consumer.updateBeginningOffsets(Map.of(partition, 0L));
        consumer.schedulePollTask(() -> {
            currentTopicId.set(recreatedTopicId);
            consumer.addRecord(record(topic, 0, "new-incarnation-value"));
        });

        try (KafkaEntryReader reader = new KafkaEntryReader(
                consumer, topic + "-partition-0", 0, 1, 1024.0, EntryReaderOptions.DEFAULT,
                expectedTopicId.toString(), ignored -> currentTopicId.get())) {
            DataSourceException error = assertThrows(DataSourceException.class, reader::read);

            assertEquals(ExceptionCode.NO_SUCH_LOG, error.getExceptionCode());
            assertTrue(error.getMessage().contains(recreatedTopicId.toString()));
        }
    }

    @Test
    void factoryUsesSourceTopicAndSourceTopicIdFromTaskProperties() throws Exception {
        String sourceTopic = "orders";
        Uuid topicId = Uuid.randomUuid();
        AtomicReference<String> lookedUpTopic = new AtomicReference<>();
        MockConsumer<byte[], byte[]> consumer = consumerWithRecords(
                sourceTopic, record(sourceTopic, 0, "value"));
        LakehouseConfiguration configuration = new LakehouseConfiguration();
        configuration.getProperties().setProperty("kafka.consumer.bootstrap.servers", "unused:9092");

        try (KafkaEntryProcessFactory factory = new KafkaEntryProcessFactory(
                configuration,
                ignored -> consumer,
                topic -> {
                    lookedUpTopic.set(topic);
                    return topicId;
                });
             IEntryReader reader = factory.createEntryReader(
                     "uuid-qualified-canonical-log", 1L, 0L, 1L, 1024.0,
                     EntryReaderOptions.DEFAULT,
                     Map.of(
                             KafkaEntryProcessFactory.SOURCE_TOPIC_PROPERTY, sourceTopic + "-partition-0",
                             KafkaEntryProcessFactory.SOURCE_TOPIC_ID_PROPERTY, topicId.toString()))) {
            GenericEntry entry = reader.read();
            try {
                assertEquals(0L, entry.entry().header().offset());
                assertEquals(sourceTopic, lookedUpTopic.get());
            } finally {
                entry.entry().payload().release();
            }
        }
    }

    @Test
    void factoryFencesMismatchedTaskBeforeReturningReader() throws Exception {
        String sourceTopic = "recreated-before-open";
        Uuid expectedTopicId = Uuid.randomUuid();
        Uuid recreatedTopicId = Uuid.randomUuid();
        MockConsumer<byte[], byte[]> consumer = consumerWithRecords(
                sourceTopic, record(sourceTopic, 0, "new-incarnation-value"));
        LakehouseConfiguration configuration = new LakehouseConfiguration();
        configuration.getProperties().setProperty("kafka.consumer.bootstrap.servers", "unused:9092");

        try (KafkaEntryProcessFactory factory = new KafkaEntryProcessFactory(
                configuration, ignored -> consumer, ignored -> recreatedTopicId)) {
            DataSourceException error = assertThrows(DataSourceException.class,
                    () -> factory.createEntryReader(
                            "uuid-qualified-canonical-log", 1L, 0L, 1L, 1024.0,
                            EntryReaderOptions.DEFAULT,
                            Map.of(
                                    KafkaEntryProcessFactory.SOURCE_TOPIC_PROPERTY,
                                    sourceTopic + "-partition-0",
                                    KafkaEntryProcessFactory.SOURCE_TOPIC_ID_PROPERTY,
                                    expectedTopicId.toString())));

            assertEquals(ExceptionCode.NO_SUCH_LOG, error.getExceptionCode());
            assertTrue(error.getMessage().contains(expectedTopicId.toString()));
            assertTrue(error.getMessage().contains(recreatedTopicId.toString()));
            assertTrue(consumer.assignment().isEmpty());
        }
    }

    private static MockConsumer<byte[], byte[]> consumerWithRecords(
            String topic, ConsumerRecord<byte[], byte[]>... records) {
        MockConsumer<byte[], byte[]> consumer = new MockConsumer<>(OffsetResetStrategy.EARLIEST);
        TopicPartition partition = new TopicPartition(topic, 0);
        consumer.updateBeginningOffsets(Map.of(partition, 0L));
        consumer.schedulePollTask(() -> {
            for (ConsumerRecord<byte[], byte[]> record : records) {
                consumer.addRecord(record);
            }
        });
        return consumer;
    }

    private static ConsumerRecord<byte[], byte[]> record(String topic, long offset, String value) {
        return new ConsumerRecord<>(topic, 0, offset, null, value.getBytes(StandardCharsets.UTF_8));
    }
}
