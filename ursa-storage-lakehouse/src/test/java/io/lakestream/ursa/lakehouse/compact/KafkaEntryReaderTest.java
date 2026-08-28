/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.lakehouse.compact;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.lakestream.ursa.exception.DataSourceException;
import io.lakestream.ursa.exception.ExceptionCode;
import io.lakestream.ursa.lakehouse.LakehouseConfiguration;
import io.lakestream.ursa.materialization.serde.GenericEntry;
import io.lakestream.ursa.materialization.serde.KafkaEntry;
import io.lakestream.ursa.test.containers.util.KafkaStandalone;
import io.netty.buffer.ByteBufUtil;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.RecordsToDelete;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Slf4j
@Tag("lakehouse")
public class KafkaEntryReaderTest {

    private static KafkaStandalone kafkaStandalone;

    @BeforeAll
    static void startKafkaStandalone() {
        kafkaStandalone = new KafkaStandalone();
        kafkaStandalone.start();
    }

    @AfterAll
    static void stopKafkaStandalone() {
        if (kafkaStandalone != null) {
            kafkaStandalone.stop();
        }
    }

    record Message(byte[] key, byte[] content, long offset) { }

    private List<Message> generateMessages(String topic, int numberOfMessages) throws Exception {
        var producerProps = kafkaStandalone.producerProps();
        @Cleanup
        var producer = new KafkaProducer<String, byte[]>(producerProps);
        List<Message> messages = new ArrayList<>();
        for (int i = 0; i < numberOfMessages; i++) {
            var value = ("message" + i).getBytes(StandardCharsets.UTF_8);
            var record = new ProducerRecord<>(topic, (String) null, value);
            var future = producer.send(record);
            var metadata = future.get();
            log.info("send offset: {}", metadata.offset());
            messages.add(new Message(null, value, metadata.offset()));
        }
        producer.flush();
        return messages;
    }

    private void deleteRecordsBefore(String topic, long offset) throws Exception {
        @Cleanup
        var admin = AdminClient.create(kafkaStandalone.connProps());
        var partition = new TopicPartition(topic, 0);
        admin.deleteRecords(Collections.singletonMap(partition, RecordsToDelete.beforeOffset(offset)))
            .all().get();
    }

    @Test
    public void testReader() throws Exception {
        final var topic = "test-reader";
        final int numberOfMessages = 10;
        var messages = generateMessages(topic, numberOfMessages);

        var startOffset = messages.get(0).offset();
        var endOffset = messages.get(numberOfMessages - 1).offset() + 1;
        var kafkaProps = kafkaStandalone.consumerProps();

        @Cleanup
        var reader = new KafkaEntryReader(topic, startOffset, endOffset, 1024.0,
            EntryReaderOptions.DEFAULT, kafkaProps);

        List<GenericEntry> entries = new ArrayList<>();
        GenericEntry e;
        while ((e = reader.read()) != null) {
            log.info("Read offset: {}", e.entry().header().offset());
            entries.add(e);
        }

        assertEquals(numberOfMessages, entries.size());
        for (int i = 0; i < entries.size(); i++) {
            var readEntry = entries.get(i);
            var payload = readEntry.entry().payload();
            var sentValue = messages.get(i).content();
            var sentKey = messages.get(i).key();
            var kafkaEntry = new KafkaEntry(sentKey, sentValue);
            var buf = kafkaEntry.toByteBuf();
            assertEquals(0, ByteBufUtil.compare(payload, buf));
            payload.release();
            buf.release();
        }
    }

    @Test
    public void testReadWithRange() throws Exception {
        final var topic = "test-range";
        final int numberOfMessages = 100;
        var messages = generateMessages(topic, numberOfMessages);

        var startOffset = messages.get(10).offset();
        var endOffset = messages.get(50).offset();
        var kafkaProps = kafkaStandalone.consumerProps();

        @Cleanup
        var reader = new KafkaEntryReader(topic, startOffset, endOffset, 1024.0,
            EntryReaderOptions.DEFAULT, kafkaProps);

        List<GenericEntry> entries = new ArrayList<>();
        GenericEntry e;
        while ((e = reader.read()) != null) {
            entries.add(e);
        }

        assertEquals(40, entries.size());
        releaseEntries(entries);
    }

    @Test
    public void testReaderUsesKafkaTopicNameWithoutPartitionSuffix() throws Exception {
        final var topic = "test-partitioned-topic-name";
        final int numberOfMessages = 10;
        var messages = generateMessages(topic, numberOfMessages);

        var startOffset = messages.get(0).offset();
        var endOffset = messages.get(numberOfMessages - 1).offset() + 1;
        var kafkaProps = kafkaStandalone.consumerProps();

        @Cleanup
        var reader = new KafkaEntryReader(topic + "-partition-0", startOffset, endOffset, 1024.0,
            EntryReaderOptions.DEFAULT, kafkaProps);

        List<GenericEntry> entries = new ArrayList<>();
        GenericEntry e;
        while ((e = reader.read()) != null) {
            entries.add(e);
        }

        assertEquals(numberOfMessages, entries.size());
        releaseEntries(entries);
    }

    @Test
    public void testReadEmptyTopic() throws Exception {
        final var topic = "test-empty-" + System.currentTimeMillis();
        var startOffset = 0L;
        var endOffset = 100L;
        var kafkaProps = kafkaStandalone.consumerProps();

        @Cleanup
        var reader = new KafkaEntryReader(topic, startOffset, endOffset, 1024.0,
            EntryReaderOptions.DEFAULT, kafkaProps);

        assertThrows(DataSourceException.class, () -> reader.read());
    }

    @Test
    public void testReaderWithFactory() throws Exception {
        final var topic = "test-factory";
        final int numberOfMessages = 50;
        var messages = generateMessages(topic, numberOfMessages);

        var startOffset = messages.get(0).offset();
        var endOffset = messages.get(numberOfMessages - 1).offset() + 1;

        var config = new LakehouseConfiguration();
        Properties props = config.getProperties();
        props.setProperty("kafka.consumer.bootstrap.servers", kafkaStandalone.getBootstrapServers());
        props.setProperty("kafka.consumer.group.id", "test-factory-group");
        props.setProperty("kafka.consumer.auto.offset.reset", "earliest");
        props.setProperty("kafka.consumer.enable.auto.commit", "false");

        @Cleanup
        var factory = new KafkaEntryProcessFactory(config);

        @Cleanup
        var reader = factory.createEntryReader(topic, 0, startOffset, endOffset, 1024.0);

        List<GenericEntry> entries = new ArrayList<>();
        GenericEntry e;
        while ((e = reader.read()) != null) {
            entries.add(e);
        }

        assertEquals(numberOfMessages, entries.size());
        releaseEntries(entries);
    }

    @Test
    public void testEntryHeader() throws Exception {
        final var topic = "test-header";
        final int numberOfMessages = 10;
        var messages = generateMessages(topic, numberOfMessages);

        var startOffset = messages.get(0).offset();
        var endOffset = messages.get(numberOfMessages - 1).offset() + 1;
        var kafkaProps = kafkaStandalone.consumerProps();

        @Cleanup
        var reader = new KafkaEntryReader(topic, startOffset, endOffset, 1024.0,
            EntryReaderOptions.DEFAULT, kafkaProps);

        GenericEntry entry = reader.read();
        assertNotNull(entry);

        var header = entry.entry().header();
        assertEquals(startOffset, header.offset());
        assertEquals(1, header.numberOfMessages());
        assertTrue(header.entrySize() > 0);
        assertEquals(startOffset, header.cumulativeSize());
        entry.entry().payload().release();
    }

    @Test
    public void testStartOffsetCleanedByRetention() throws Exception {
        final var topic = "test-retention-" + System.currentTimeMillis();
        var messages = generateMessages(topic, 10);
        deleteRecordsBefore(topic, messages.get(5).offset());
        var kafkaProps = kafkaStandalone.consumerProps();

        @Cleanup
        var reader = new KafkaEntryReader(topic, messages.get(0).offset(), messages.get(4).offset(),
            1024.0, EntryReaderOptions.DEFAULT, kafkaProps);

        var e = assertThrows(DataSourceException.class, reader::read);

        assertEquals(ExceptionCode.NO_SUCH_OFFSET, e.getExceptionCode());
        assertTrue(e.getMessage().contains("deleted by retention policy"));
    }

    @Test
    public void testPartiallyCleanedRangeContinuesFromBeginningOffset() throws Exception {
        final var topic = "test-partial-retention-" + System.currentTimeMillis();
        var messages = generateMessages(topic, 10);
        deleteRecordsBefore(topic, messages.get(5).offset());
        var kafkaProps = kafkaStandalone.consumerProps();

        @Cleanup
        var reader = new KafkaEntryReader(topic, messages.get(0).offset(), messages.get(6).offset(),
            1024.0, EntryReaderOptions.DEFAULT, kafkaProps);

        List<GenericEntry> entries = new ArrayList<>();
        GenericEntry e;
        while ((e = reader.read()) != null) {
            entries.add(e);
        }

        assertEquals(1, entries.size());
        assertEquals(messages.get(5).offset(), entries.get(0).entry().header().offset());
        releaseEntries(entries);
    }

    private static void releaseEntries(List<GenericEntry> entries) {
        entries.forEach(entry -> entry.entry().payload().release());
    }
}
