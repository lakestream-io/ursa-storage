/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.lakehouse.compact;

import io.lakestream.ursa.exception.DataSourceException;
import io.lakestream.ursa.lakehouse.LakehouseConfiguration;
import io.lakestream.ursa.lakehouse.utils.TopicNames;
import io.lakestream.ursa.materialization.serde.EntryFormat;
import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.Uuid;

@Slf4j
public class KafkaEntryProcessFactory implements EntryProcessFactory {

    private static final String KAFKA_CONSUMER_PREFIX = "kafka.consumer.";
    private static final String DEFAULT_GROUP_ID = "__sdt";
    public static final String SOURCE_TOPIC_PROPERTY = "sourceTopic";
    public static final String SOURCE_TOPIC_ID_PROPERTY = "sourceTopicId";
    public static final String SOURCE_SCHEMA_TOPIC_PROPERTY = "sourceSchemaTopic";

    private final LakehouseConfiguration configuration;
    private final Properties kafkaProperties;
    private final ConsumerFactory consumerFactory;
    private final KafkaEntryReader.TopicIdLookup topicIdLookup;
    private final Admin admin;

    public KafkaEntryProcessFactory(LakehouseConfiguration configuration) {
        this.configuration = configuration;
        this.kafkaProperties = extractKafkaConsumerProperties(configuration.getProperties());
        this.consumerFactory = KafkaEntryReader::createConsumer;
        this.admin = Admin.create(kafkaProperties);
        this.topicIdLookup = topic -> lookupTopicId(admin, topic);
    }

    KafkaEntryProcessFactory(LakehouseConfiguration configuration,
                             ConsumerFactory consumerFactory,
                             KafkaEntryReader.TopicIdLookup topicIdLookup) {
        this.configuration = configuration;
        this.kafkaProperties = extractKafkaConsumerProperties(configuration.getProperties());
        this.consumerFactory = consumerFactory;
        this.topicIdLookup = topicIdLookup;
        this.admin = null;
    }

    private Properties extractKafkaConsumerProperties(Properties sourceProperties) {
        Properties kafkaProps = new Properties();
        String groupId = null;

        for (String key : sourceProperties.stringPropertyNames()) {
            if (key.startsWith(KAFKA_CONSUMER_PREFIX)) {
                String kafkaKey = key.substring(KAFKA_CONSUMER_PREFIX.length());
                if (ConsumerConfig.GROUP_ID_CONFIG.equals(kafkaKey)) {
                    groupId = sourceProperties.getProperty(key);
                }
                kafkaProps.put(kafkaKey, sourceProperties.getProperty(key));
            }
        }

        if (groupId == null) {
            kafkaProps.put(ConsumerConfig.GROUP_ID_CONFIG, DEFAULT_GROUP_ID);
            log.debug("Using default group id: {}", DEFAULT_GROUP_ID);
        }

        if (!kafkaProps.containsKey(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG)) {
            throw new IllegalArgumentException(
                "Kafka bootstrap servers not configured. "
                    + "Please set " + KAFKA_CONSUMER_PREFIX + ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG);
        }

        log.debug("Extracted Kafka consumer properties: {}", kafkaProps);
        return kafkaProps;
    }

    @Override
    public IEntryReader createEntryReader(String topic, long streamId, long startOffset, long endOffset,
                                          double avgEntrySize, EntryReaderOptions options) {
        return new KafkaEntryReader(consumerFactory.create(kafkaProperties), topic, startOffset, endOffset,
                avgEntrySize, options);
    }

    @Override
    public IEntryReader createEntryReader(String topic, long streamId, long startOffset, long endOffset,
                                          double avgEntrySize, EntryReaderOptions options,
                                          Map<String, String> taskProperties) throws DataSourceException {
        String sourceTopic = property(taskProperties, SOURCE_TOPIC_PROPERTY);
        String sourceTopicId = property(taskProperties, SOURCE_TOPIC_ID_PROPERTY);
        boolean hasSourceTopic = sourceTopic != null && !sourceTopic.isBlank();
        String effectiveSourceTopic = resolveSourceTopic(topic, EntryFormat.KAFKA, taskProperties);
        String expectedTopicId = hasSourceTopic ? sourceTopicId : null;
        KafkaEntryReader reader = new KafkaEntryReader(consumerFactory.create(kafkaProperties),
                effectiveSourceTopic, startOffset, endOffset, avgEntrySize, options,
                expectedTopicId, topicIdLookup);
        try {
            reader.validateTopicIncarnation();
            return reader;
        } catch (DataSourceException e) {
            reader.close();
            throw e;
        }
    }

    @Override
    public void close() {
        if (admin != null) {
            admin.close(Duration.ofSeconds(5));
        }
        log.debug("Closing KafkaEntryProcessFactory");
    }

    public Properties getKafkaProperties() {
        return kafkaProperties;
    }

    /**
     * Resolves the physical Kafka topic carried by a compaction task without changing the task's
     * canonical stream identity. Non-Kafka tasks always retain their canonical topic.
     */
    public static String resolveSourceTopic(String taskTopic, EntryFormat entryFormat,
                                            Map<String, String> taskProperties) {
        if (entryFormat != EntryFormat.KAFKA) {
            return taskTopic;
        }
        String sourceTopic = property(taskProperties, SOURCE_TOPIC_PROPERTY);
        return sourceTopic == null || sourceTopic.isBlank() ? taskTopic : sourceTopic;
    }

    /** Resolves the unpartitioned logical Kafka topic used only for schema-registry lookup. */
    public static String resolveSchemaTopic(String destinationTopic, Map<String, String> taskProperties) {
        String schemaTopic = property(taskProperties, SOURCE_SCHEMA_TOPIC_PROPERTY);
        if (schemaTopic != null && !schemaTopic.isBlank()) {
            return schemaTopic;
        }
        String sourceTopic = property(taskProperties, SOURCE_TOPIC_PROPERTY);
        String legacyTopic = sourceTopic == null || sourceTopic.isBlank() ? destinationTopic : sourceTopic;
        return TopicNames.partitionedLocalName(legacyTopic);
    }

    private static String property(Map<String, String> properties, String name) {
        return properties == null ? null : properties.get(name);
    }

    private static Uuid lookupTopicId(Admin admin, String topic) throws Exception {
        TopicDescription description = admin.describeTopics(Collections.singleton(topic))
                .allTopicNames().get().get(topic);
        return description == null ? null : description.topicId();
    }

    @FunctionalInterface
    interface ConsumerFactory {
        Consumer<byte[], byte[]> create(Properties properties);
    }
}
