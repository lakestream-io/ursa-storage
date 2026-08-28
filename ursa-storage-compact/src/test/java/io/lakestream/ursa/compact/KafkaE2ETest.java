/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.compact;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

import io.lakestream.ursa.compaction.CompactionManager;
import io.lakestream.ursa.compaction.OxiaCompactTaskManager;
import io.lakestream.ursa.compaction.task.PreparedCompactStreamTask;
import io.lakestream.ursa.lakehouse.LakehouseConfiguration;
import io.lakestream.ursa.lakehouse.compact.KafkaEntryProcessFactory;
import io.lakestream.ursa.lakehouse.iceberg.IcebergTable;
import io.lakestream.ursa.storage.impl.StorageConfig;
import io.lakestream.ursa.test.containers.util.KafkaStandalone;
import io.lakestream.ursa.test.containers.util.StaticVariables;
import io.opentelemetry.api.GlobalOpenTelemetry;
import io.oxia.testcontainers.OxiaContainer;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Duration;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.testcontainers.shaded.org.awaitility.Awaitility;
import org.testcontainers.utility.DockerImageName;

@Slf4j
public class KafkaE2ETest {

    @TempDir
    static Path path;

    static KafkaStandalone kafkaStandalone;
    static OxiaContainer oxiaContainer;
    static CompactionScheduler compactionScheduler;
    static StorageConfig config;

    @BeforeAll
    static void prepareService() throws Exception {
        GlobalOpenTelemetry.resetForTest();
        kafkaStandalone = new KafkaStandalone();
        kafkaStandalone.start();

        oxiaContainer = new OxiaContainer(DockerImageName.parse(StaticVariables.OXIA_IMAGE));
        oxiaContainer.start();

        Properties properties = new Properties();
        properties.put("oxiaStorageUrl", String.format("oxia://%s/default", oxiaContainer.getServiceAddress()));
        properties.put("metadataStoreUrl", String.format("oxia://%s/default", oxiaContainer.getServiceAddress()));
        properties.put("compactionBackendStorageType", "local");
        properties.put("backendStorageType", "local");
        properties.put("storagePath", path.toAbsolutePath().toString());
        properties.put("clusterSbtEnabled", "false");
        properties.put("clusterSdtEnabled", "true");
        properties.put("lakehouseType", "iceberg");
        properties.put("streamTableMode", "EXTERNAL");
        properties.put("persistKey", "true");
        properties.put("dataSourceForCompaction", "URSA");
        properties.put("entryFormat", "KAFKA");
        properties.put("entrySerDeType", "KAFKA_BATCHED_RAW_PARQUET");
        // adjust the interval to ensure the test can get passed fast
        properties.put("maxCommitIntervalInSeconds", "5");
        properties.put("refreshLocalTaskIntervalInSeconds", "5");
        // kafka entry reader related configuration
        properties.put("kafka.consumer.bootstrap.servers", kafkaStandalone.getBootstrapServers());
        config = StorageConfig.fromProperties(properties);
        compactionScheduler = new CompactionScheduler(config);
        compactionScheduler.start();
    }

    @AfterAll
    static void shutdownService() throws Exception {
        if (kafkaStandalone != null) {
            kafkaStandalone.stop();
        }

        if (compactionScheduler != null) {
            compactionScheduler.close();
        }
        if (oxiaContainer != null) {
            oxiaContainer.stop();
        }
    }

    @Test
    public void e2eTest() throws Exception {
        var topic = "e2e-" + RandomStringUtils.secure().nextAlphabetic(4);
        var numberOfMessages = 100;

        var producerProps = kafkaStandalone.producerProps();
        @Cleanup
        var producer = new KafkaProducer<String, byte[]>(producerProps);

        LinkedList<Long> offsets = new LinkedList<>();
        for (int i = 0; i < numberOfMessages; i++) {
            var key = "key-" + i;
            var value = ("message-" + i).getBytes(StandardCharsets.UTF_8);
            var record = new ProducerRecord<>(topic, key, value);
            var future = producer.send(record);
            var metadata = future.get();
            offsets.add(metadata.offset());
        }
        producer.flush();

        String sourceTopicId;
        try (var admin = Admin.create(kafkaStandalone.connProps())) {
            sourceTopicId = admin.describeTopics(List.of(topic)).allTopicNames().get()
                    .get(topic).topicId().toString();
        }

        CompactionManager manager = new CompactionManager(compactionScheduler.getCompactTaskManager());

        var task = new PreparedCompactStreamTask();
        task.setStreamId(1);
        String canonicalTopic = "default/" + topic + "-partition-0";
        task.setTopic(canonicalTopic);
        task.setStartOffset(offsets.getFirst());
        task.setEndOffset(Math.addExact(offsets.getLast(), 1L));
        task.setProperties(Map.of(
                "entryFormat", "KAFKA",
                "entrySerDeType", "KAFKA_BATCHED_RAW_PARQUET",
                KafkaEntryProcessFactory.SOURCE_TOPIC_PROPERTY, topic + "-partition-0",
                KafkaEntryProcessFactory.SOURCE_TOPIC_ID_PROPERTY, sourceTopicId,
                KafkaEntryProcessFactory.SOURCE_SCHEMA_TOPIC_PROPERTY, topic));
        task.setTaskName(UUID.randomUUID().toString());
        task.setStatus(PreparedCompactStreamTask.INIT);
        manager.publishTask(task);
        var compactTaskKey = OxiaCompactTaskManager.buildSubTaskKey(task.toCompactStreamTask());

        @Cleanup var icebergTable = getIcebergTable(topic);
        waitingForIcebergRecords(icebergTable, numberOfMessages);
        waitingForCompactTaskRemoval(compactTaskKey);
        @Cleanup var records = IcebergGenerics.read(icebergTable.getTable()).build();
        int readCount = 0;
        for (Record record : records) {
            var key = (ByteBuffer) record.getField("__key");
            var expectedKey = "key-" + readCount;
            assertArrayEquals(expectedKey.getBytes(StandardCharsets.UTF_8), key.array());
            var message = (ByteBuffer) record.getField("payload");
            var expected = ("message-" + readCount).getBytes(StandardCharsets.UTF_8);
            assertArrayEquals(expected, message.array());
            readCount++;
        }
        assertEquals(numberOfMessages, readCount);
    }

    IcebergTable getIcebergTable(String topic) {
        LakehouseConfiguration lakehouseConfiguration = new LakehouseConfiguration(config.getProperties());
        var identifier = IcebergTable.getTableIdentifierByTopic(topic);
        IcebergTable icebergTable = new IcebergTable(lakehouseConfiguration, identifier);
        return icebergTable;
    }

    void waitingForIcebergRecords(IcebergTable icebergTable, int expectedCount) {
        log.info("Waiting for {} records in Iceberg table {}", expectedCount, icebergTable.getIdentifier());
        Awaitility.await()
            .atMost(Duration.ofMinutes(3))
            .pollInterval(Duration.ofSeconds(1))
            .until(() -> {
                if (!icebergTable.exists()) {
                    return false;
                }
                icebergTable.loadTable();
                icebergTable.getTable().refresh();
                if (icebergTable.getTable().currentSnapshot() == null) {
                    return false;
                }
                try (var records = IcebergGenerics.read(icebergTable.getTable()).build()) {
                    int count = 0;
                    for (Record ignored : records) {
                        count++;
                    }
                    return count == expectedCount;
                }
            });
    }

    void waitingForCompactTaskRemoval(String compactTaskKey) {
        var taskManager = (OxiaCompactTaskManager) compactionScheduler.getCompactTaskManager();
        Awaitility.await()
            .atMost(Duration.ofSeconds(30))
            .pollInterval(Duration.ofMillis(200))
            .until(() -> taskManager.getCompactStreamTask(compactTaskKey).get() == null);
    }
}
