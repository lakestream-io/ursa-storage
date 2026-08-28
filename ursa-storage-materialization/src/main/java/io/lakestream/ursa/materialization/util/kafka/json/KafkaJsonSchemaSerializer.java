/*
 * SPDX-License-Identifier: Apache-2.0
 */

/*
 * Copyright 2020 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.lakestream.ursa.materialization.util.kafka.json;

import static io.confluent.kafka.schemaregistry.json.JsonSchemaUtils.isEnvelope;

import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.kjetland.jackson.jsonSchema.JsonSchemaConfig;
import com.kjetland.jackson.jsonSchema.JsonSchemaDraft;
import com.kjetland.jackson.jsonSchema.JsonSchemaGenerator;
import com.kjetland.jackson.jsonSchema.SubclassesResolver;
import io.confluent.kafka.schemaregistry.annotations.Schema;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaReference;
import io.confluent.kafka.schemaregistry.json.JsonSchema;
import io.confluent.kafka.schemaregistry.json.JsonSchemaUtils;
import io.confluent.kafka.schemaregistry.utils.BoundedConcurrentHashMap;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Serializer;
import org.everit.json.schema.loader.SpecificationVersion;

public class KafkaJsonSchemaSerializer<T> extends AbstractKafkaJsonSchemaSerializer<T>
        implements Serializer<T> {
    private static final int DEFAULT_CACHE_CAPACITY = 1000;
    private static final String ENVELOPE_SCHEMA_FIELD_NAME = "schema";
    private static final String ENVELOPE_REFERENCES_FIELD_NAME = "references";
    private static final String ENVELOPE_RESOLVED_REFS_FIELD_NAME = "resolvedReferences";

    private boolean isKey;
    private Map<ObjectNode, JsonSchema> nodeToSchemaCache;
    private Map<Class<?>, JsonSchema> classToSchemaCache;

    /**
     * Constructor used by Kafka producer.
     */
    public KafkaJsonSchemaSerializer() {
        this.nodeToSchemaCache = new BoundedConcurrentHashMap<>(DEFAULT_CACHE_CAPACITY);
        this.classToSchemaCache = new BoundedConcurrentHashMap<>(DEFAULT_CACHE_CAPACITY);
    }

    public KafkaJsonSchemaSerializer(SchemaRegistryClient client) {
        this.schemaRegistry = client;
        this.ticker = ticker(client);
        this.nodeToSchemaCache = new BoundedConcurrentHashMap<>(DEFAULT_CACHE_CAPACITY);
        this.classToSchemaCache = new BoundedConcurrentHashMap<>(DEFAULT_CACHE_CAPACITY);
    }

    public KafkaJsonSchemaSerializer(SchemaRegistryClient client, Map<String, ?> props) {
        this(client, props, DEFAULT_CACHE_CAPACITY);
    }

    public KafkaJsonSchemaSerializer(SchemaRegistryClient client, Map<String, ?> props,
                                     int cacheCapacity) {
        this.schemaRegistry = client;
        this.ticker = ticker(client);
        configure(serializerConfig(props));
        this.nodeToSchemaCache = new BoundedConcurrentHashMap<>(cacheCapacity);
        this.classToSchemaCache = new BoundedConcurrentHashMap<>(cacheCapacity);
    }

    @Override
    public void configure(Map<String, ?> config, boolean isKey) {
        this.isKey = isKey;
        configure(new KafkaJsonSchemaSerializerConfig(config));
    }


    @Override
    public byte[] serialize(String topic, T record) {
        return serialize(topic, null, record);
    }

    @Override
    public byte[] serialize(String topic, Headers headers, T record) {
        if (record == null) {
            return null;
        }
        JsonSchema schema;
        if (isEnvelope(record)) {
            schema = nodeToSchemaCache.computeIfAbsent(
                    copyEnvelopeWithoutPayload((ObjectNode) record),
                    k -> getSchema(record));
        } else {
            schema = classToSchemaCache.computeIfAbsent(record.getClass(), k -> getSchema(record));
        }
        Object value = JsonSchemaUtils.getValue(record);
        return serializeImpl(
                getSubjectName(topic, isKey, value, schema), topic, headers, (T) value, schema);
    }

    static ObjectNode copyEnvelopeWithoutPayload(ObjectNode jsonValue) {
        ObjectNode result = JsonNodeFactory.instance.objectNode();
        JsonNode schemaNode = jsonValue.get(ENVELOPE_SCHEMA_FIELD_NAME);
        result.set(ENVELOPE_SCHEMA_FIELD_NAME, schemaNode);
        JsonNode referencesNode = jsonValue.get(ENVELOPE_REFERENCES_FIELD_NAME);
        if (referencesNode != null && !referencesNode.isEmpty()) {
            result.set(ENVELOPE_REFERENCES_FIELD_NAME, referencesNode);
        }
        JsonNode resolvedReferencesNode = jsonValue.get(ENVELOPE_RESOLVED_REFS_FIELD_NAME);
        if (resolvedReferencesNode != null && !resolvedReferencesNode.isEmpty()) {
            result.set(ENVELOPE_RESOLVED_REFS_FIELD_NAME, resolvedReferencesNode);
        }
        return result;
    }

    private static JsonSchema getSchemaFromEnvelope(ObjectMapper objectMapper, JsonNode jsonValue) {
        JsonNode referencesNode = jsonValue.get(ENVELOPE_REFERENCES_FIELD_NAME);
        List<SchemaReference> references = Collections.emptyList();
        if (referencesNode != null && !referencesNode.isEmpty()) {
            JavaType type = objectMapper.getTypeFactory().constructParametricType(
                    List.class, SchemaReference.class);
            references = objectMapper.convertValue(referencesNode, type);
        }
        JsonNode resolvedReferencesNode = jsonValue.get(ENVELOPE_RESOLVED_REFS_FIELD_NAME);
        Map<String, String> resolvedReferences = Collections.emptyMap();
        if (resolvedReferencesNode != null && !resolvedReferencesNode.isEmpty()) {
            JavaType type = objectMapper.getTypeFactory().constructParametricType(
                    Map.class, String.class, JsonNode.class);
            Map<String, JsonNode> resolved = objectMapper.convertValue(resolvedReferencesNode, type);
            resolvedReferences = resolved.entrySet().stream()
                    .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().toString()));
        }
        JsonNode schemaNode = jsonValue.get(ENVELOPE_SCHEMA_FIELD_NAME);
        return new JsonSchema(schemaNode.toString(), references, resolvedReferences, null);
    }

    private JsonSchema getSchema(T record) {
        try {
            return getSchema(record, specVersion, scanPackages, oneofForNullables,
                    failUnknownProperties, objectMapper, schemaRegistry);
        } catch (IOException e) {
            throw new SerializationException(e);
        }
    }

    private static JsonSchema getSchema(
            Object object,
            SpecificationVersion specVersion,
            List<String> scanPackages,
            boolean useOneofForNullables,
            boolean failUnknownProperties,
            ObjectMapper objectMapper,
            SchemaRegistryClient client) throws IOException {
        if (object == null) {
            return null;
        }
        if (specVersion == null) {
            specVersion = SpecificationVersion.DRAFT_7;
        }
        if (isEnvelope(object)) {
            return getSchemaFromEnvelope(objectMapper, (JsonNode) object);
        }
        Class<?> cls = object.getClass();
        if (cls.isAnnotationPresent(Schema.class)) {
            Schema schema = (Schema) cls.getAnnotation(Schema.class);
            List<SchemaReference> references = Arrays.stream(schema.refs())
                    .map(ref -> new SchemaReference(ref.name(), ref.subject(), ref.version()))
                    .collect(Collectors.toList());
            if (client == null) {
                if (!references.isEmpty()) {
                    throw new IllegalArgumentException("Cannot resolve schema " + schema.value()
                            + " with refs " + references);
                }
                return new JsonSchema(schema.value());
            } else {
                return (JsonSchema) client.parseSchema(JsonSchema.TYPE, schema.value(), references)
                        .orElseThrow(() -> new IOException("Invalid schema " + schema.value()
                                + " with refs " + references));
            }
        }
        JsonSchemaConfig.JsonSchemaConfigBuilder config = JsonSchemaConfig.builder()
                .useOneOfForNullables(useOneofForNullables)
                .failOnUnknownProperties(failUnknownProperties);
        JsonSchemaDraft draft;
        switch (specVersion) {
            case DRAFT_4:
                draft = JsonSchemaDraft.DRAFT_04;
                break;
            case DRAFT_6:
                draft = JsonSchemaDraft.DRAFT_06;
                break;
            case DRAFT_7:
                draft = JsonSchemaDraft.DRAFT_07;
                break;
            default:
                draft = JsonSchemaDraft.DRAFT_07;
                break;
        }
        config = config.jsonSchemaDraft(draft);
        if (scanPackages != null && !scanPackages.isEmpty()) {
            config = config.subclassesResolver(new SubclassesResolver(scanPackages, null));
        }
        JsonSchemaGenerator jsonSchemaGenerator = new JsonSchemaGenerator(objectMapper, config.build());
        JsonNode jsonSchema = jsonSchemaGenerator.generateJsonSchema(cls);
        return new JsonSchema(jsonSchema.toString());
    }

    @Override
    public void close() {
        try {
            super.close();
        } catch (IOException e) {
            throw new RuntimeException("Exception while closing serializer", e);
        }
    }
}
