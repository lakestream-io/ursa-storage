/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.test.containers.util;

import io.confluent.kafka.schemaregistry.SchemaProvider;
import io.confluent.kafka.schemaregistry.avro.AvroSchemaProvider;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.json.JsonSchemaProvider;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchemaProvider;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

@Slf4j
public class KafkaStandalone extends GenericContainer<KafkaStandalone> {

    private static final int KAFKA_PORT = 9092;
    private static final DockerImageName DEFAULT_IMAGE = DockerImageName.parse(StaticVariables.KAFKA_IMAGE);

    private final int kafkaPort;
    private final Network network = Network.newNetwork();
    private SchemaRegistryContainer schemaRegistry;

    public KafkaStandalone() {
        this(DEFAULT_IMAGE, false);
    }

    public KafkaStandalone(boolean withSchemaRegistry) {
        this(DEFAULT_IMAGE, withSchemaRegistry);
    }

    public KafkaStandalone(DockerImageName dockerImageName, boolean withSchemaRegistry) {
        super(dockerImageName);
        this.kafkaPort = PortManager.nextLockedFreePort();

        // 1. Configure Kafka
        withNetwork(network);
        withNetworkAliases("kafka-broker");
        addFixedExposedPort(kafkaPort, KAFKA_PORT);

        withEnv("KAFKA_NODE_ID", "1");
        withEnv("KAFKA_PROCESS_ROLES", "broker,controller");
        withEnv("KAFKA_LISTENERS", "EXTERNAL://:9092,PLAINTEXT://:9094,CONTROLLER://:9093");
        withEnv("KAFKA_ADVERTISED_LISTENERS",
            "EXTERNAL://localhost:" + kafkaPort + "," +    // For your Java tests
            "PLAINTEXT://kafka-broker:9094"                 // For Schema Registry
        );
        withEnv("KAFKA_CONTROLLER_QUORUM_VOTERS", "1@localhost:9093");
        withEnv("KAFKA_CONTROLLER_LISTENER_NAMES", "CONTROLLER");
        withEnv("KAFKA_LISTENER_SECURITY_PROTOCOL_MAP", "EXTERNAL:PLAINTEXT,PLAINTEXT:PLAINTEXT,CONTROLLER:PLAINTEXT");
        withEnv("KAFKA_INTER_BROKER_LISTENER_NAME", "PLAINTEXT");
        withEnv("KAFKA_AUTO_CREATE_TOPICS_ENABLE", "true");

        // 1. Set internal topic replication to 1
        withEnv("KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR", "1");

        // 2. Set transaction logs replication to 1 (prevents similar errors with producers)
        withEnv("KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR", "1");
        withEnv("KAFKA_TRANSACTION_STATE_LOG_MIN_ISR", "1");

        // 3. For Schema Registry compatibility
        withEnv("KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS", "0");

        waitingFor(Wait.forLogMessage(".*Kafka Server started.*\\n", 1));

        // 2. Initialize Inner Class Registry if flag is set
        if (withSchemaRegistry) {
            this.schemaRegistry = new SchemaRegistryContainer(network, "kafka-broker:9094");
        }
    }

    @Override
    public void start() {
        super.start();
        if (schemaRegistry != null) {
            schemaRegistry.start();
        }
    }

    @Override
    public void stop() {
        if (schemaRegistry != null) {
            schemaRegistry.stop();
        }
        super.stop();
        PortManager.releaseLockedPort(kafkaPort);
        network.close();
    }

    public String getBootstrapServers() {
        return String.format("%s:%d", getHost(), kafkaPort);
    }

    public String getSchemaRegistryUrl() {
        return schemaRegistry != null ? schemaRegistry.getUrl() : null;
    }

    // --- Properties Helpers ---

    public Properties connProps() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, getBootstrapServers());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "test-group");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

        if (getSchemaRegistryUrl() != null) {
            props.put("schema.registry.url", getSchemaRegistryUrl());
        }
        return props;
    }

    public Properties producerProps() {
        Properties props = connProps();
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, org.apache.kafka.common.serialization.StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, org.apache.kafka.common.serialization.ByteArraySerializer.class);
        return props;
    }

    public Properties consumerProps() {
        Properties props = connProps();
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, org.apache.kafka.common.serialization.StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, org.apache.kafka.common.serialization.ByteArrayDeserializer.class);
        return props;
    }

    public SchemaRegistryClient getSchemaRegistryClient() {
        String url = getSchemaRegistryUrl();
        if (url == null) {
            log.warn("Schema Registry is not enabled for this KafkaStandalone instance.");
            return null;
        }
        JsonSchemaProvider jsonSchemaProvider = new JsonSchemaProvider();
        AvroSchemaProvider avroSchemaProvider = new AvroSchemaProvider();
        ProtobufSchemaProvider protobufSchemaProvider = new ProtobufSchemaProvider();
        List<SchemaProvider> schemaProviders = List.of(jsonSchemaProvider, avroSchemaProvider,
            protobufSchemaProvider);
        return new CachedSchemaRegistryClient(url, 100, schemaProviders, Map.of());
    }
    /**
     * Inner class defining the Schema Registry Service.
     */
    private static class SchemaRegistryContainer extends GenericContainer<SchemaRegistryContainer> {
        private static final int REGISTRY_PORT = 8081;
        private final int hostPort;

        public SchemaRegistryContainer(Network network, String bootstrapServers) {
            super(DockerImageName.parse("confluentinc/cp-schema-registry:7.5.0"));
            this.hostPort = PortManager.nextLockedFreePort();

            withNetwork(network);
            addFixedExposedPort(hostPort, REGISTRY_PORT);

            withEnv("SCHEMA_REGISTRY_HOST_NAME", "schema-registry");
            withEnv("SCHEMA_REGISTRY_LISTENERS", "http://0.0.0.0:8081");

            // Point to the INTERNAL listener we set up on Kafka
            withEnv("SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS", "PLAINTEXT://" + bootstrapServers);

            // Map INTERNAL to the standard PLAINTEXT implementation inside the Registry
            // (This prevents the registry from trying to look up a custom security class)
            withEnv("SCHEMA_REGISTRY_INTER_INSTANCE_PROTOCOL", "http");

            withEnv("SCHEMA_REGISTRY_KAFKASTORE_TOPIC_REPLICATION_FACTOR", "1");

            waitingFor(Wait.forHttp("/subjects").forStatusCode(200));
        }

        public String getUrl() {
            return String.format("http://%s:%d", getHost(), hostPort);
        }

        @Override
        public void stop() {
            super.stop();
            PortManager.releaseLockedPort(hostPort);
        }
    }
}
