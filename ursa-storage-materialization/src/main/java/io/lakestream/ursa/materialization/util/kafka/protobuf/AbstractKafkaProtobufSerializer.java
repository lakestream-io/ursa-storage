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

package io.lakestream.ursa.materialization.util.kafka.protobuf;

import static io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema.ANY_LOCATION;
import static io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema.API_LOCATION;
import static io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema.CALENDAR_PERIOD_LOCATION;
import static io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema.COLOR_LOCATION;
import static io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema.DATETIME_LOCATION;
import static io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema.DATE_LOCATION;
import static io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema.DAY_OF_WEEK_LOCATION;
import static io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema.DECIMAL_LOCATION;
import static io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema.DESCRIPTOR_LOCATION;
import static io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema.DURATION_LOCATION;
import static io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema.EMPTY_LOCATION;
import static io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema.EXPR_LOCATION;
import static io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema.FIELD_MASK_LOCATION;
import static io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema.FRACTION_LOCATION;
import static io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema.INTERVAL_LOCATION;
import static io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema.LATLNG_LOCATION;
import static io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema.MONEY_LOCATION;
import static io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema.MONTH_LOCATION;
import static io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema.PHONE_NUMBER_LOCATION;
import static io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema.POSTAL_ADDRESS_LOCATION;
import static io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema.QUATERNION_LOCATION;
import static io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema.SOURCE_CONTEXT_LOCATION;
import static io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema.STRUCT_LOCATION;
import static io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema.TIMESTAMP_LOCATION;
import static io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema.TIME_OF_DAY_LOCATION;
import static io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema.TYPE_LOCATION;
import static io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema.WRAPPER_LOCATION;

import com.google.protobuf.Message;
import com.squareup.wire.schema.internal.parser.ProtoFileElement;
import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.entities.RuleMode;
import io.confluent.kafka.schemaregistry.client.rest.entities.Schema;
import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaReference;
import io.confluent.kafka.schemaregistry.client.rest.entities.requests.RegisterSchemaResponse;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.schemaregistry.protobuf.MessageIndexes;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchemaProvider;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDe;
import io.confluent.kafka.serializers.subject.strategy.ReferenceSubjectNameStrategy;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.errors.InvalidConfigurationException;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.header.Headers;

@Slf4j
public abstract class AbstractKafkaProtobufSerializer<T extends Message>
        extends AbstractKafkaSchemaSerDe {

    protected boolean normalizeSchema;
    protected boolean autoRegisterSchema;
    protected boolean propagateSchemaTags;
    protected boolean onlyLookupReferencesBySchema;
    protected int useSchemaId = -1;
    protected boolean idCompatStrict;
    protected boolean latestCompatStrict;
    protected String schemaFormat;
    protected boolean skipKnownTypes;
    protected ReferenceSubjectNameStrategy referenceSubjectNameStrategy;

    protected void configure(KafkaProtobufSerializerConfig config) {
        configureClientProperties(config, new ProtobufSchemaProvider());
        this.normalizeSchema = config.normalizeSchema();
        this.autoRegisterSchema = config.autoRegisterSchema();
        this.propagateSchemaTags = config.propagateSchemaTags();
        this.onlyLookupReferencesBySchema = config.onlyLookupReferencesBySchema();
        this.useSchemaId = config.useSchemaId();
        this.idCompatStrict = config.getIdCompatibilityStrict();
        this.latestCompatStrict = config.getLatestCompatibilityStrict();
        this.schemaFormat = config.getSchemaFormat();
        this.skipKnownTypes = config.skipKnownTypes();
        this.referenceSubjectNameStrategy = config.referenceSubjectNameStrategyInstance();
    }

    protected KafkaProtobufSerializerConfig serializerConfig(Map<String, ?> props) {
        try {
            return new KafkaProtobufSerializerConfig(props);
        } catch (ConfigException e) {
            throw new ConfigException(e.getMessage());
        }
    }

    protected byte[] serializeImpl(
            String subject, String topic, boolean isKey, T object, ProtobufSchema schema
    ) throws SerializationException, InvalidConfigurationException {
        return serializeImpl(subject, topic, isKey, null, object, schema);
    }

    @SuppressWarnings("unchecked")
    protected byte[] serializeImpl(
            String subject, String topic, boolean isKey, Headers headers, T object, ProtobufSchema schema
    ) throws SerializationException, InvalidConfigurationException {
        if (schemaRegistry == null) {
            throw new InvalidConfigurationException(
                    "SchemaRegistryClient not found. You need to configure the serializer "
                            + "or use serializer constructor with SchemaRegistryClient.");
        }
        // null needs to treated specially since the client most likely just wants to send
        // an individual null value instead of making the subject a null type. Also, null in
        // Kafka has a special meaning for deletion in a topic with the compact retention policy.
        // Therefore, we will bypass schema registration and return a null value in Kafka, instead
        // of an encoded null.
        if (object == null) {
            return null;
        }
        String restClientErrorMsg = "";
        try {
            boolean autoRegisterForDeps = autoRegisterSchema && !onlyLookupReferencesBySchema;
            boolean useLatestForDeps = useLatestVersion && !onlyLookupReferencesBySchema;
            schema = resolveDependencies(schemaRegistry, normalizeSchema, autoRegisterForDeps,
                    useLatestForDeps, latestCompatStrict, latestVersionsCache(),
                    skipKnownTypes, referenceSubjectNameStrategy, topic, isKey, schema);
            int id;
            if (autoRegisterSchema) {
                restClientErrorMsg = "Error registering Protobuf schema: ";
                if (schemaFormat != null) {
                    log.info("schemaFormat isn't supported. ignoring schemaFormat {}", schemaFormat);
                    //String formatted = schema.formattedString(schemaFormat);
                    //schema = schema.copyWithSchema(formatted);
                }
                Schema s =
                        registerWithResponse(subject, schema, normalizeSchema, propagateSchemaTags);
                if (s.getSchema() != null) {
                    Optional<ParsedSchema> optSchema = schemaRegistry.parseSchema(s);
                    if (optSchema.isPresent()) {
                        schema = (ProtobufSchema) optSchema.get();
                        schema = schema.copy(s.getVersion());
                    }
                }
                id = s.getId();
            } else if (useSchemaId >= 0) {
                restClientErrorMsg = "Error retrieving schema ID";
                if (schemaFormat != null) {
                    log.info("schemaFormat isn't supported. ignoring schemaFormat {}", schemaFormat);
//          String formatted = schema.formattedString(schemaFormat);
//          schema = schema.copyWithSchema(formatted);
                }
                schema = (ProtobufSchema)
                        lookupSchemaBySubjectAndId(subject, useSchemaId, schema, idCompatStrict);
                id = useSchemaId;
            } else if (metadata != null) {
                restClientErrorMsg = "Error retrieving latest with metadata '" + metadata + "'";
                ExtendedSchema extendedSchema = getLatestWithMetadata(subject);
                schema = (ProtobufSchema) extendedSchema.getSchema();
                id = extendedSchema.getId();
            } else if (useLatestVersion) {
                restClientErrorMsg = "Error retrieving latest version: ";
                ExtendedSchema extendedSchema = lookupLatestVersion(subject, schema, latestCompatStrict);
                schema = (ProtobufSchema) extendedSchema.getSchema();
                id = extendedSchema.getId();
            } else {
                restClientErrorMsg = "Error retrieving Protobuf schema: ";
                id = schemaRegistry.getId(subject, schema, normalizeSchema);
            }
            object = (T) executeRules(subject, topic, headers, RuleMode.WRITE, null, schema, object);
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            out.write(MAGIC_BYTE);
            out.write(ByteBuffer.allocate(idSize).putInt(id).array());
            MessageIndexes indexes = schema.toMessageIndexes(object.getDescriptorForType().getFullName());
            out.write(indexes.toByteArray());
            object.writeTo(out);
            byte[] bytes = out.toByteArray();
            out.close();
            return bytes;
        } catch (InterruptedIOException e) {
            throw new TimeoutException("Error serializing Protobuf message", e);
        } catch (IOException | RuntimeException e) {
            throw new SerializationException("Error serializing Protobuf message", e);
        } catch (RestClientException e) {
            throw toKafkaException(e, restClientErrorMsg + schema);
        } finally {
            postOp(object);
        }
    }


    /**
     * Resolve schema dependencies recursively.
     *
     * @param schemaRegistry     schema registry client
     * @param autoRegisterSchema whether to automatically register schemas
     * @param useLatestVersion   whether to use the latest subject version for serialization
     * @param latestCompatStrict whether to check that the latest subject version is backward
     *                           compatible with the schema of the object
     * @param latestVersions     an optional cache of latest subject versions, may be null
     * @param strategy           the strategy for determining the subject name for a reference
     * @param topic              the topic
     * @param isKey              whether the object is the record key
     * @param schema             the schema
     * @return the schema with resolved dependencies
     */
    public static ProtobufSchema resolveDependencies(
            SchemaRegistryClient schemaRegistry,
            boolean autoRegisterSchema,
            boolean useLatestVersion,
            boolean latestCompatStrict,
            Map<SubjectSchema, ExtendedSchema> latestVersions,
            ReferenceSubjectNameStrategy strategy,
            String topic,
            boolean isKey,
            ProtobufSchema schema
    ) throws IOException, RestClientException {
        return resolveDependencies(
                schemaRegistry,
                autoRegisterSchema,
                useLatestVersion,
                latestCompatStrict,
                latestVersions,
                true,
                strategy,
                topic,
                isKey,
                schema);
    }

    /**
     * Resolve schema dependencies recursively.
     *
     * @param schemaRegistry     schema registry client
     * @param autoRegisterSchema whether to automatically register schemas
     * @param useLatestVersion   whether to use the latest subject version for serialization
     * @param latestCompatStrict whether to check that the latest subject version is backward
     *                           compatible with the schema of the object
     * @param latestVersions     an optional cache of latest subject versions, may be null
     * @param skipKnownTypes     whether to skip known types when resolving schema dependencies
     * @param strategy           the strategy for determining the subject name for a reference
     * @param topic              the topic
     * @param isKey              whether the object is the record key
     * @param schema             the schema
     * @return the schema with resolved dependencies
     */
    public static ProtobufSchema resolveDependencies(
            SchemaRegistryClient schemaRegistry,
            boolean autoRegisterSchema,
            boolean useLatestVersion,
            boolean latestCompatStrict,
            Map<SubjectSchema, ExtendedSchema> latestVersions,
            boolean skipKnownTypes,
            ReferenceSubjectNameStrategy strategy,
            String topic,
            boolean isKey,
            ProtobufSchema schema
    ) throws IOException, RestClientException {
        return resolveDependencies(
                schemaRegistry,
                false,
                autoRegisterSchema,
                useLatestVersion,
                latestCompatStrict,
                latestVersions,
                skipKnownTypes,
                strategy,
                topic,
                isKey,
                schema
        );
    }

    public static ProtobufSchema resolveDependencies(
            SchemaRegistryClient schemaRegistry,
            boolean normalizeSchema,
            boolean autoRegisterSchema,
            boolean useLatestVersion,
            boolean latestCompatStrict,
            Map<SubjectSchema, ExtendedSchema> latestVersions,
            boolean skipKnownTypes,
            ReferenceSubjectNameStrategy strategy,
            String topic,
            boolean isKey,
            ProtobufSchema schema
    ) throws IOException, RestClientException {
        return resolveDependencies(
                schemaRegistry,
                normalizeSchema,
                autoRegisterSchema,
                false,
                useLatestVersion,
                latestCompatStrict,
                latestVersions,
                skipKnownTypes,
                strategy,
                topic,
                isKey,
                schema
        );
    }

    /**
     * Resolve schema dependencies recursively.
     *
     * @param schemaRegistry      schema registry client
     * @param normalizeSchema     whether to normalized the schema
     * @param autoRegisterSchema  whether to automatically register schemas
     * @param propagateSchemaTags whether to propagate tags during registration
     * @param useLatestVersion    whether to use the latest subject version for serialization
     * @param latestCompatStrict  whether to check that the latest subject version is backward
     *                            compatible with the schema of the object
     * @param latestVersions      an optional cache of latest subject versions, may be null
     * @param skipKnownTypes      whether to skip known types when resolving schema dependencies
     * @param strategy            the strategy for determining the subject name for a reference
     * @param topic               the topic
     * @param isKey               whether the object is the record key
     * @param schema              the schema
     * @return the schema with resolved dependencies
     */
    public static ProtobufSchema resolveDependencies(
            SchemaRegistryClient schemaRegistry,
            boolean normalizeSchema,
            boolean autoRegisterSchema,
            boolean propagateSchemaTags,
            boolean useLatestVersion,
            boolean latestCompatStrict,
            Map<SubjectSchema, ExtendedSchema> latestVersions,
            boolean skipKnownTypes,
            ReferenceSubjectNameStrategy strategy,
            String topic,
            boolean isKey,
            ProtobufSchema schema
    ) throws IOException, RestClientException {
        if (schema.dependencies().isEmpty() || !schema.references().isEmpty()) {
            // Dependencies already resolved
            return schema;
        }
        HashMap<String, ProtoFileElement> dependencies = new HashMap<>(schema.dependencies());
        Schema s = resolveDependencies(schemaRegistry,
                normalizeSchema,
                autoRegisterSchema,
                propagateSchemaTags,
                useLatestVersion,
                latestCompatStrict,
                latestVersions,
                skipKnownTypes,
                strategy,
                topic,
                isKey,
                null,
                schema.rawSchema(),
                dependencies
        );

        return schema.copy(s.getReferences());
    }

    private static Schema resolveDependencies(
            SchemaRegistryClient schemaRegistry,
            boolean normalizeSchema,
            boolean autoRegisterSchema,
            boolean propagateSchemaTags,
            boolean useLatestVersion,
            boolean latestCompatStrict,
            Map<SubjectSchema, ExtendedSchema> latestVersions,
            boolean skipKnownTypes,
            ReferenceSubjectNameStrategy strategy,
            String topic,
            boolean isKey,
            String name,
            ProtoFileElement protoFileElement,
            Map<String, ProtoFileElement> dependencies
    ) throws IOException, RestClientException {
        List<SchemaReference> references = new ArrayList<>();
        for (String dep : protoFileElement.getImports()) {
            if (skipKnownTypes && KNOWN_DEPENDENCIES.contains(dep)) {
                dependencies.remove(dep);
                continue;
            }
            Schema subschema = resolveDependencies(schemaRegistry,
                    normalizeSchema,
                    autoRegisterSchema,
                    propagateSchemaTags,
                    useLatestVersion,
                    latestCompatStrict,
                    latestVersions,
                    skipKnownTypes,
                    strategy,
                    topic,
                    isKey,
                    dep,
                    dependencies.get(dep),
                    dependencies
            );
            references.add(new SchemaReference(dep, subschema.getSubject(), subschema.getVersion()));
        }
        for (String dep : protoFileElement.getPublicImports()) {
            if (skipKnownTypes && KNOWN_DEPENDENCIES.contains(dep)) {
                dependencies.remove(dep);
                continue;
            }
            Schema subschema = resolveDependencies(schemaRegistry,
                    normalizeSchema,
                    autoRegisterSchema,
                    propagateSchemaTags,
                    useLatestVersion,
                    latestCompatStrict,
                    latestVersions,
                    skipKnownTypes,
                    strategy,
                    topic,
                    isKey,
                    dep,
                    dependencies.get(dep),
                    dependencies
            );
            references.add(new SchemaReference(dep, subschema.getSubject(), subschema.getVersion()));
        }
        ProtobufSchema schema = new ProtobufSchema(protoFileElement, references, dependencies);
        Integer id = null;
        Integer version = null;
        String subject = name != null ? strategy.subjectName(name, topic, isKey, schema) : null;
        if (subject != null) {
            if (autoRegisterSchema) {
                RegisterSchemaResponse response = schemaRegistry.registerWithResponse(
                        subject, schema, normalizeSchema, propagateSchemaTags);
                if (response.getSchema() != null) {
                    Optional<ParsedSchema> optSchema =
                            schemaRegistry.parseSchema(new Schema(subject, response));
                    if (optSchema.isPresent()) {
                        schema = (ProtobufSchema) optSchema.get();
                        schema = schema.copy(response.getVersion());
                    }
                }
                id = response.getId();
                version = schemaRegistry.getVersion(subject, schema, normalizeSchema);
            } else if (useLatestVersion) {
                ExtendedSchema extendedSchema = lookupLatestVersion(
                        schemaRegistry, subject, schema, latestVersions, latestCompatStrict);
                schema = (ProtobufSchema) extendedSchema.getSchema();
                id = extendedSchema.getId();
                version = extendedSchema.getVersion();
            } else {
                id = schemaRegistry.getId(subject, schema, normalizeSchema);
                version = schemaRegistry.getVersion(subject, schema, normalizeSchema);
            }
        }
        return new Schema(
                subject,
                version,
                id,
                schema
        );
    }

    private static final Set<String> KNOWN_DEPENDENCIES;

    static {
        KNOWN_DEPENDENCIES = new HashSet<>();
        KNOWN_DEPENDENCIES.add(CALENDAR_PERIOD_LOCATION);
        KNOWN_DEPENDENCIES.add(COLOR_LOCATION);
        KNOWN_DEPENDENCIES.add(DATE_LOCATION);
        KNOWN_DEPENDENCIES.add(DATETIME_LOCATION);
        KNOWN_DEPENDENCIES.add(DAY_OF_WEEK_LOCATION);
        KNOWN_DEPENDENCIES.add(DECIMAL_LOCATION);
        KNOWN_DEPENDENCIES.add(EXPR_LOCATION);
        KNOWN_DEPENDENCIES.add(FRACTION_LOCATION);
        KNOWN_DEPENDENCIES.add(INTERVAL_LOCATION);
        KNOWN_DEPENDENCIES.add(LATLNG_LOCATION);
        KNOWN_DEPENDENCIES.add(MONEY_LOCATION);
        KNOWN_DEPENDENCIES.add(MONTH_LOCATION);
        KNOWN_DEPENDENCIES.add(PHONE_NUMBER_LOCATION);
        KNOWN_DEPENDENCIES.add(POSTAL_ADDRESS_LOCATION);
        KNOWN_DEPENDENCIES.add(QUATERNION_LOCATION);
        KNOWN_DEPENDENCIES.add(TIME_OF_DAY_LOCATION);
        KNOWN_DEPENDENCIES.add(ANY_LOCATION);
        KNOWN_DEPENDENCIES.add(API_LOCATION);
        KNOWN_DEPENDENCIES.add(DESCRIPTOR_LOCATION);
        KNOWN_DEPENDENCIES.add(DURATION_LOCATION);
        KNOWN_DEPENDENCIES.add(EMPTY_LOCATION);
        KNOWN_DEPENDENCIES.add(FIELD_MASK_LOCATION);
        KNOWN_DEPENDENCIES.add(SOURCE_CONTEXT_LOCATION);
        KNOWN_DEPENDENCIES.add(STRUCT_LOCATION);
        KNOWN_DEPENDENCIES.add(TIMESTAMP_LOCATION);
        KNOWN_DEPENDENCIES.add(TYPE_LOCATION);
        KNOWN_DEPENDENCIES.add(WRAPPER_LOCATION);
    }
}
