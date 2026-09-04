/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.lakehouse.schema;

import io.confluent.kafka.schemaregistry.SchemaProvider;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaMetadata;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.RestService;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.lakestream.ursa.lakehouse.exception.FetchSchemaFailedException;
import io.lakestream.ursa.lakehouse.exception.SchemaNotFoundException;
import io.lakestream.ursa.materialization.serde.kafka.schema.RawSchemaProvider;
import io.lakestream.ursa.storage.impl.StorageConfig;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import lombok.Getter;

/** Confluent-compatible schema registry used by Kafka and Ursa Kafka-formatted entries. */
public class KafkaSchemaRegistry implements SchemaRegistry {
    public static final String NAME = "kafka";
    public static final String URL = "schemaRegistryUrl";
    public static final String CONFIG_PREFIX = "schemaRegistryConfig";
    public static final String HTTP_HEADER_PREFIX = "schemaRegistryHttpHeader";
    public static final String AUTHORIZATION_FILE = "schemaRegistryHttpHeaderAuthorizationFile";

    @Getter
    private final SchemaRegistryClient schemaRegistryClient;

    public KafkaSchemaRegistry(Properties properties) {
        this.schemaRegistryClient = createClient(properties);
    }

    /**
     * Creates a registry client for Kafka schema-aware records. When no registry URL is configured,
     * an in-memory client is used so raw and primitive Kafka records remain materializable without
     * accidentally constructing an HTTP client for the literal URL {@code "null"}.
     *
     * <p>JSON Schema and Protobuf schemas are kept as opaque text ({@link RawSchemaProvider}); only the
     * Apache-2.0 Avro provider parses schemas on the client.
     */
    public static SchemaRegistryClient createClient(Properties properties) {
        List<SchemaProvider> providers = RawSchemaProvider.defaultProviders();
        String url = properties.getProperty(URL);
        if (url == null || url.isBlank()) {
            return new MockSchemaRegistryClient(providers);
        }
        return new CachedSchemaRegistryClient(
                new RestService(url),
                1000,
                providers,
                configs(properties),
                httpHeaders(properties));
    }

    @Override
    public String name() {
        return NAME;
    }

    @Override
    public SchemaRegistryClient client() {
        return schemaRegistryClient;
    }

    @Override
    public SchemaMetadata fetchLatest(String logicalTopic)
            throws FetchSchemaFailedException, SchemaNotFoundException {
        String subject = valueSubject(logicalTopic);
        try {
            return schemaRegistryClient.getLatestSchemaMetadata(subject);
        } catch (RestClientException e) {
            if (e.getErrorCode() == 40401 || e.getErrorCode() == 40403) {
                throw new SchemaNotFoundException("Schema not found for " + subject, e);
            }
            throw new FetchSchemaFailedException("Failed to fetch schema for " + subject, e);
        } catch (IOException e) {
            throw new FetchSchemaFailedException("Failed to fetch schema for " + subject, e);
        }
    }

    /**
     * Builds the TopicNameStrategy value subject for a logical Kafka topic. The topic must already be the
     * logical source name; the UUID-qualified storage name would produce a subject that never exists.
     * A namespace prefix is tolerated for streams created without Kafka lifecycle metadata.
     */
    static String valueSubject(String logicalTopic) {
        if (logicalTopic == null || logicalTopic.isBlank()) {
            throw new IllegalArgumentException("Logical topic must not be blank");
        }
        int slash = logicalTopic.lastIndexOf('/');
        String localName = slash < 0 ? logicalTopic : logicalTopic.substring(slash + 1);
        return localName + "-value";
    }

    public static Map<String, String> configs(Properties properties) {
        Map<String, String> configs = new HashMap<>();
        configs.put("schema.registry.http.connect.timeout.ms", "30000");
        configs.put("schema.registry.http.read.timeout.ms", "30000");
        properties.forEach((key, value) -> {
            String name = key.toString();
            if (name.startsWith(CONFIG_PREFIX)) {
                configs.put(name.substring(CONFIG_PREFIX.length()).toLowerCase(Locale.ROOT), value.toString());
            }
        });
        return configs;
    }

    public static Map<String, String> httpHeaders(Properties properties) {
        Map<String, String> headers = new HashMap<>();
        properties.forEach((key, value) -> {
            String name = key.toString();
            if (name.startsWith(HTTP_HEADER_PREFIX)) {
                headers.put(name.substring(HTTP_HEADER_PREFIX.length()).toLowerCase(Locale.ROOT), value.toString());
            }
            if (AUTHORIZATION_FILE.equalsIgnoreCase(name)) {
                try {
                    headers.put("authorization", StorageConfig.loadCredentialsFromFile(value.toString()));
                } catch (IOException e) {
                    throw new IllegalArgumentException("Failed to load credentials from file: " + value, e);
                }
            }
        });
        return headers;
    }
}
