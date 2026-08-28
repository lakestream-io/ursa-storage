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
import io.lakestream.ursa.lakehouse.iceberg.IcebergTable;
import io.lakestream.ursa.lakehouse.utils.TopicNames;
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
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
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

        CompactionManager manager = new CompactionManager(compactionScheduler.getCompactTaskManager());

        var task = new PreparedCompactStreamTask();
        task.setStreamId(1);
        String canonicalTopic = "default/" + topic + "-partition-0";
        task.setTopic(canonicalTopic);
        task.setStartOffset(offsets.getFirst());
        task.setEndOffset(Math.addExact(offsets.getLast(), 1L));
        task.setProperties(Map.of(
                "entryFormat", "KAFKA",
                "entrySerDeType", "KAFKA_BATCHED_RAW_PARQUET"));
        task.setTaskName(UUID.randomUUID().toString());
        task.setStatus(PreparedCompactStreamTask.INIT);
        manager.publishTask(task);

        waitingForCompactTaskCompleteForTopic(canonicalTopic);

        var icebergTable = getIcebergTable(topic);
        icebergTable.loadTable();
        var records = IcebergGenerics.read(icebergTable.getTable()).build();
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

    void waitingForCompactTaskCompleteForTopic(String topic) {
        log.info("Waiting for compaction task to complete for topic {}", topic);
        var oxiaTaskManager = (OxiaCompactTaskManager) compactionScheduler.getCompactTaskManager();
        Awaitility.await()
            .atMost(Duration.ofMinutes(3))
            .pollDelay(Duration.ofSeconds(10))
            .pollInterval(Duration.ofSeconds(10))
            .until(() -> {
                var tasksByTopic = oxiaTaskManager.getFirstNTasksOfTopic(100).get();
                log.info("All tasks in the manager {}", tasksByTopic);
                var tasks = tasksByTopic.entrySet().stream()
                    .filter(e ->
                        TopicNames.partitionedTopicName(e.getKey()).equals(TopicNames.partitionedTopicName(topic)))
                    .map(Map.Entry::getValue)
                    .toList();
                log.info("Waiting for compaction tasks for topic {} to complete, remaining tasks: {}", topic, tasks);
                return tasks.isEmpty();
            });
    }
}
