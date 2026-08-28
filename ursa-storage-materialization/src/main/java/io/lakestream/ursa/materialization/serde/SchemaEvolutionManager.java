/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.materialization.serde;

import io.lakestream.ursa.exception.ExceptionCode;
import io.lakestream.ursa.exception.MessageSerDeException;
import io.lakestream.ursa.exception.RuntimeExceptionWithCode;
import io.lakestream.ursa.materialization.serde.kafka.KafkaSchemaService;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class SchemaEvolutionManager {
    /**
     * Functional interface for converting a topic schema to a lakehouse table schema,
     * using message-level metadata carried by the encoder context.
     *
     * @param <T> the topic schema type
     * @param <V> the lakehouse table schema type (Iceberg Schema or Delta StructType)
     */
    @FunctionalInterface
    public interface SchemaConverter<T, V> {
        V convert(T topicSchema, EntryEncoderContext context);
    }

    /**
     * Evolve the table schema with the topic's schemas.
     *
     * Typically, when the transform method is called, we will transform the data with topic's schema into the
     * iceberg data with the iceberg schema. When topic's schema evolves, we need to evolve the iceberg schema
     * accordingly.
     *
     * The logic to evolve the iceberg schema is:
     * 1. Get the table schema from the tableSchemaService with the topic's schema version.
     * 2. If the table schema is not null, return it.
     * 3. Get all the topic's schemas with versions smaller than or equal to the current schema version.
     * 4. Convert the topic's schemas to iceberg schemas.
     *
     * We passed all the topic schemas to the tableSchemaService to evolve the table schema. The tableSchemaService
     * will handle the schema evolution logic.
     *
     * @param tableSchemaService
     *          the lakehouse table schema service
     * @param schemaService
     *          the topic schema service
     * @param schemaKey
     * @param loadLakehouseTableSchema
     *          the function used to convert the topic schema to a lakehouse table schema
     * @return V
     *          the lakehouse table schema
     * @param <T>
     *          the topic schema type, normally SchemaMetadata for Kafka.
     * @param <V>
     *          the lakehouse table schema. Should be Iceberg schema or the Delta schema
     */
    public static <T, V> V evolveSchema(TableSchemaService<Long, V> tableSchemaService,
                                        SchemaService schemaService,
                                        SchemaKey schemaKey,
                                        SchemaConverter<T, V> loadLakehouseTableSchema,
                                        EntryEncoderContext context) throws Exception {
        var schemaVersion = schemaKey.getSchemaVersion();
        var topic = schemaKey.getTopicName();
        var tableSchema = tableSchemaService.getTableSchema(schemaVersion);
        if (tableSchema != null) {
            log.info("Schema evolution skipped for topic {} because schema version {} already exists in table",
                topic, schemaVersion);
            return tableSchema;
        }
        var latestSchemaVersionInTable = tableSchemaService.getLatestSchemaVersion();
        log.info("Start schema evolution for topic {}. requestedSchemaVersion={}, latestSchemaVersionInTable={}, "
                + "baseSchemaVersion={}",
            topic, schemaVersion, latestSchemaVersionInTable, context.baseSchemaVersion());
        if (schemaVersion != KafkaSchemaService.PRIMITIVE_SCHEMA_ID
            && latestSchemaVersionInTable > schemaVersion) {
            throw new RuntimeExceptionWithCode(
                new MessageSerDeException(ExceptionCode.MESSAGE_SCHEMA_INCOMPATIBLE,
                    String.format("Latest schema version %s in table is larger than the request schema version %s,"
                                  + " the table was evolved but failed", latestSchemaVersionInTable,
                        schemaVersion)));
        }

        // Base-version config applies only when the table has not yet been created
        // (both Iceberg and Delta TableSchemaService impls return -1L from getLatestSchemaVersion()
        //  when the table doesn't exist). Once created, the existing `latest > schemaVersion` check
        //  above is the sole post-creation guard. Primitive schemas are exempt — they predate
        //  any topic-level schema versioning.
        var baseSchemaVersion = context.baseSchemaVersion();
        boolean tableNotYetCreated = latestSchemaVersionInTable < 0;
        boolean baseVersionApplies = baseSchemaVersion.isPresent() && tableNotYetCreated
            && schemaVersion != KafkaSchemaService.PRIMITIVE_SCHEMA_ID;

        if (baseVersionApplies && schemaVersion < baseSchemaVersion.get()) {
            throw new RuntimeExceptionWithCode(
                new MessageSerDeException(ExceptionCode.MESSAGE_SCHEMA_INCOMPATIBLE,
                    String.format("Schema version %s for topic %s is below the configured base schema version %s",
                        schemaVersion, topic, baseSchemaVersion.get())));
        }

        Map<Long, T> topicSchemas = schemaService.getSchemaWithVersions(topic, schemaVersion);
        log.info("Loaded topic schemas for topic {} before base-version filtering. requestedSchemaVersion={}, "
                + "availableSchemaVersions={}",
            topic, schemaVersion, topicSchemas.keySet());
        if (baseVersionApplies) {
            long base = baseSchemaVersion.get();
            topicSchemas = topicSchemas.entrySet().stream()
                .filter(e -> e.getKey() >= base)
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
            log.info("Applied base schema version filter for topic {}. baseSchemaVersion={}, "
                    + "filteredSchemaVersions={}",
                topic, base, topicSchemas.keySet());
        }

        TreeMap<Long, V> tableSchemas = new TreeMap<>();
        for (Map.Entry<Long, T> topicSchemaInfo : topicSchemas.entrySet()) {
            try {
                var lakehouseSchema = loadLakehouseTableSchema.convert(topicSchemaInfo.getValue(), context);
                tableSchemas.put(topicSchemaInfo.getKey(), lakehouseSchema);
                log.info("Converted topic schema for topic {}. schemaVersion={}, convertedSchemaType={}",
                    topic, topicSchemaInfo.getKey(),
                    lakehouseSchema == null ? "null" : lakehouseSchema.getClass().getSimpleName());
            } catch (Throwable e) {
                log.info("Failed to convert table schema from the topic schema version {}",
                    topicSchemaInfo.getKey(), e);
            }
        }
        log.info("Prepared table schema candidates for topic {}. candidateVersions={}",
            topic, tableSchemas.keySet());
        var evolvedVersions = tableSchemaService.evolveTableSchema(tableSchemas);
        if (evolvedVersions.contains(schemaVersion)) {
            log.info("Evolved schema for topic: {}, schema version: {}", topic, schemaVersion);
            return tableSchemaService.getTableSchema(schemaVersion);
        } else {
            log.warn("Schema version: {} for topic: {} has not been evolved, evolved versions: {}",
                    schemaVersion, topic, evolvedVersions);
            throw new RuntimeExceptionWithCode(
                new MessageSerDeException(ExceptionCode.MESSAGE_SCHEMA_INCOMPATIBLE,
                    "Schema version: " + schemaVersion + " for topic: " + topic
                    + " has not been evolved, evolved versions: " + evolvedVersions));
        }
    }
}
