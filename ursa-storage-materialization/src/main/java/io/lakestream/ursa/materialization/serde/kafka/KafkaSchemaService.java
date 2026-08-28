/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.materialization.serde.kafka;

import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.client.SchemaMetadata;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.lakestream.ursa.materialization.serde.SchemaService;
import io.lakestream.ursa.materialization.util.kafka.json.KafkaJsonSchemaDeserializer;
import io.lakestream.ursa.materialization.util.kafka.protobuf.KafkaProtobufDeserializer;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;

public class KafkaSchemaService implements SchemaService<SchemaMetadata> {

    // kafka valid schema id is between [1, 2^31-1], it doesn't have the primitive schema on the schema registry.
    // But in our code, we need to handle the primitive schemas. All the kafka messages without schema are bytes,
    // So we define the schema id 0 for the messages without the schemas.
    public static final int PRIMITIVE_SCHEMA_ID = 0;
    private static final String PRIMITIVE_SCHEMA_TYPE = "PRIMITIVE";

    private final SchemaRegistryClient schemaRegistryClient;
    private Map<String, Deserializer<?>> deserializerMap;
    private Map<String, Serializer<Object>> serializerMap;
    private ConcurrentMap<String, Boolean> hasSchema = new ConcurrentHashMap<>();
    private ConcurrentMap<String, SchemaMetadata> primitiveSchemaMap = new ConcurrentHashMap<>();
    private final boolean useQualifiedTopicName;

    public KafkaSchemaService(SchemaRegistryClient schemaRegistryClient) {
        this(schemaRegistryClient, true);
    }

    public KafkaSchemaService(SchemaRegistryClient schemaRegistryClient, boolean useQualifiedTopicName) {
        this.useQualifiedTopicName = useQualifiedTopicName;
        this.schemaRegistryClient = schemaRegistryClient;
        this.deserializerMap = new HashMap<>();
        this.serializerMap = new HashMap<>();

        deserializerMap.put("AVRO", new KafkaAvroDeserializer(schemaRegistryClient));
        deserializerMap.put("JSON", new KafkaJsonSchemaDeserializer<>(schemaRegistryClient));
        deserializerMap.put("PROTOBUF", new KafkaProtobufDeserializer<>(schemaRegistryClient));
        serializerMap.put("AVRO", new KafkaAvroSerializer(schemaRegistryClient));
    }

    public boolean hasSchema(String topic) {
        var result = hasSchema.get(topic);
        if (result == null) {
            result = hasSchema0(topic);
            hasSchema.put(topic, result);
            return result;
        }
        return result;
    }

    private boolean hasSchema0(String topic) {
        try {
            var subject = getSubject(topic);
            schemaRegistryClient.getLatestSchemaMetadata(subject);
            return true;
        } catch (RestClientException re) {
            if (re.getErrorCode() == 40401) {
                return false;
            }
            throw new RuntimeException(re);
        } catch (IOException ioException) {
            throw new RuntimeException(ioException);
        }
    }

    public SchemaMetadata getPrimitiveSchemaMetadata(String topic) {
        return new SchemaMetadata(PRIMITIVE_SCHEMA_ID, 0, PRIMITIVE_SCHEMA_TYPE, Collections.emptyList(), null);
    }

    public SchemaMetadata fetchSchema(String topic, int schemaId) {
        try {
            if (schemaId == PRIMITIVE_SCHEMA_ID) {
                return getPrimitiveSchemaMetadata(topic);
            }
            var subject = getSubject(topic);
            var parsedSchema = fetchParsedSchemaById(topic, schemaId);
            var version = schemaRegistryClient.getVersion(subject, parsedSchema);
            return schemaRegistryClient.getSchemaMetadata(subject, version, true);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public ParsedSchema fetchParsedSchemaById(String topic, int schemaId) {
        try {
            var subject = getSubject(topic);
            return schemaRegistryClient.getSchemaBySubjectAndId(subject, schemaId);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public SchemaMetadata getLatestSchemaMetadata(String topic) {
        try {
            var subject = getSubject(topic);
            return schemaRegistryClient.getLatestSchemaMetadata(subject);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public SchemaMetadata fetchSchemaByVersion(String topic, int version) {
        try {
            if (version == PRIMITIVE_SCHEMA_ID) {
                return getPrimitiveSchemaMetadata(topic);
            }
            var subject = getSubject(topic);
            return schemaRegistryClient.getSchemaMetadata(subject, version, true);
        } catch (Throwable e) {
            throw new RuntimeException(e);
        }
    }

    public void registerPrimitiveSchema(String topic, SchemaMetadata schemaMetadata) {
        primitiveSchemaMap.computeIfAbsent(topic, k -> schemaMetadata);
    }

    public Map<Long, SchemaMetadata> getSchemaWithVersions(String topic, long schemaVersion) throws ExecutionException {
        try {
            var subject = getSubject(topic);
            Map<Long, SchemaMetadata> result = new TreeMap<>();
            // because the schema depends on the message, the primitiveSchemaMap only be filled when then encoder
            // calling the fetch schema. So if you are calling this method without calling fetch schema, the
            // topic won't register the primitive schema.
            // todo: but there is one case, if two different task run in different compaction service, then
            //       we will get one compaction job does the schema evolution with (0:primitiveSchema, 1:normalSchema),
            //       another compaction job does the schema evolution with (1: normalSchema). There will be a race
            //       condition will causing this into a chaos. But looks like we cannot do anything on it.
            if (primitiveSchemaMap.containsKey(topic)) {
                var primitiveSchema = primitiveSchemaMap.get(topic);
                result.put((long) primitiveSchema.getId(), primitiveSchema);
            }
            if (schemaVersion > 0) {
                List<Integer> allVersions = schemaRegistryClient.getAllVersions(subject, true);
                for (Integer version : allVersions) {
                    if (version <= schemaVersion) {
                        var schemaMetadata = schemaRegistryClient.getSchemaMetadata(subject, version, true);
                        result.put(Long.valueOf(version), schemaMetadata);
                    }
                }
            }
            return result;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public Deserializer<?> getDeserializer(String schemaType) {
        return deserializerMap.get(schemaType);
    }

    public Serializer<Object> getSerializer(String schemaType) {
        return serializerMap.get(schemaType);
    }

    public String getSubject(String topic) {
        if (useQualifiedTopicName) {
            return partitionedTopicName(topic) + "-value";
        } else {
            return localName(topic) + "-value";
        }
    }

    private static String partitionedTopicName(String topic) {
        int suffix = partitionSuffix(topic);
        return suffix < 0 ? topic : topic.substring(0, suffix);
    }

    private static String localName(String topic) {
        int slash = topic.lastIndexOf('/');
        return slash < 0 ? topic : topic.substring(slash + 1);
    }

    private static int partitionSuffix(String topic) {
        int suffix = topic.lastIndexOf("-partition-");
        if (suffix < 0) {
            return -1;
        }
        try {
            int partition = Integer.parseInt(topic.substring(suffix + "-partition-".length()));
            return partition < 0 ? -1 : suffix;
        } catch (NumberFormatException ignored) {
            return -1;
        }
    }

    @Override
    public void close() {
        hasSchema.clear();
        primitiveSchemaMap.clear();
        // do nothing
    }

}
