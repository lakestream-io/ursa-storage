/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.lakehouse.v2;

import com.google.common.annotations.VisibleForTesting;
import io.lakestream.ursa.compaction.DynamicConfigs;
import io.lakestream.ursa.lakehouse.LakehouseConfiguration;
import io.lakestream.ursa.lakehouse.compact.FailureMessage;
import io.lakestream.ursa.lakehouse.compact.KeyedObjectPoolManager;
import io.lakestream.ursa.lakehouse.compact.ObjectPool;
import io.lakestream.ursa.lakehouse.v2.delta.DeltaExternalDLTTableWriter;
import io.lakestream.ursa.lakehouse.v2.delta.DeltaExternalTableWriter;
import io.lakestream.ursa.lakehouse.v2.iceberg.IcebergExternalDLTTableWriter;
import io.lakestream.ursa.lakehouse.v2.iceberg.IcebergExternalTableWriter;
import io.lakestream.ursa.lakehouse.v2.iceberg.IcebergManagedTableWriter;
import io.lakestream.ursa.lakehouse.v2.io.parquet.ParquetConfig;
import io.lakestream.ursa.materialization.serde.EntrySerdeFactory;
import io.lakestream.ursa.materialization.serde.GenericEntry;
import io.lakestream.ursa.materialization.serde.SchemaService;
import io.lakestream.ursa.materialization.serde.kafka.KafkaSourceMetadata;
import io.lakestream.ursa.metrics.InstrumentProvider;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class LakehouseFactory implements AutoCloseable {

    private final InstrumentProvider provider;

    private final EntrySerdeFactory entrySerdeFactory;
    private final LakehouseConfiguration configuration;
    private final Map<String, LakehouseReader> readers = new ConcurrentHashMap<>();
    private final Map<Integer, LakehouseConfiguration> configurationCache = new ConcurrentHashMap<>();

    private final KeyedObjectPoolManager<String, LakehouseReader> readerPool;
    private final Semaphore readerSemaphore;

    @VisibleForTesting
    public LakehouseFactory(LakehouseConfiguration configuration, SchemaService schemaService) {
        this(configuration, schemaService, InstrumentProvider.NOOP);
    }

    public LakehouseFactory(LakehouseConfiguration configuration, SchemaService schemaService,
                                  InstrumentProvider provider) {
        this.provider = provider;
        this.configuration = configuration;
        this.entrySerdeFactory = new EntrySerdeFactory(schemaService);
        this.readerSemaphore = getReaderSemaphore(configuration);
        this.readerPool = new KeyedObjectPoolManager<>(
            (topic) -> {
                if (!readerSemaphore.tryAcquire()) {
                    throw new IllegalStateException("No more reading is allowed");
                }
                return new LakehouseReader(topic, entrySerdeFactory, configuration, provider);
            }, 30, 0);
        this.readerPool.setCloseAction(r -> {
            readerSemaphore.release();
        });
    }

    Semaphore getReaderSemaphore(LakehouseConfiguration configuration) {
        var parquetConfig = new ParquetConfig(configuration);
        int permits = parquetConfig.estimateMaxOpenParquetFilesForReading();
        return new Semaphore(permits);
    }

    public Optional<LakehouseRecordWriter<GenericEntry>> getManagedWriter(String topic, Map<String, String> prop) {
        log.info("Creating managed writer for topic: {} with properties: {}", topic, prop);
        LakehouseConfiguration lakehouseConfiguration = generateLakehouseConfiguration(prop);
        String schemaTopic = KafkaSourceMetadata.topicName(topic, prop);

        var dynamicConfigs = resolveDynamicConfigs(lakehouseConfiguration);
        if (!dynamicConfigs.sbtEnabled()) {
            log.info("Skip creating the managed writer for the topic {} because sbt is disabled", topic);
            return Optional.empty();
        }

        // todo: to make the kafka tests pass, should remove after support write kafka to the manage writer
        var skipManagedWriter = lakehouseConfiguration.getProperties().getProperty("skipManagedWriter", "false");
        if (Boolean.parseBoolean(skipManagedWriter)) {
            log.info("Skip creating the managed writer for the topic {}", topic);
            return Optional.empty();
        }

        if (lakehouseConfiguration.getStreamTableMode() != LakehouseConfiguration.StreamTableMode.MANAGED) {
            return Optional.of(new LakehouseWriter(
                    topic, schemaTopic, entrySerdeFactory, lakehouseConfiguration, provider));
        }

        return switch (lakehouseConfiguration.getLakehouseType()) {
            case ICEBERG ->
                Optional.of(new IcebergManagedTableWriter(
                        topic, schemaTopic, entrySerdeFactory, lakehouseConfiguration, provider));
            case DELTA ->
                throw new UnsupportedOperationException("Delta Lakehouse managed writer is not supported yet.");
            case NONE ->
                Optional.of(new LakehouseWriter(
                        topic, schemaTopic, entrySerdeFactory, lakehouseConfiguration, provider));
            default -> throw new IllegalArgumentException("Unsupported lakehouse type: "
                + lakehouseConfiguration.getLakehouseType());
        };
    }

    public Optional<LakehouseRecordWriter<GenericEntry>> getExternalWriter(
            String topic, Map<String, String> properties) {
        log.info("Creating external writer for topic: {} with properties: {}", topic, properties);
        LakehouseConfiguration lakehouseConfiguration = generateLakehouseConfiguration(properties);
        String schemaTopic = KafkaSourceMetadata.topicName(topic, properties);
        var dynamicConfigs = resolveDynamicConfigs(lakehouseConfiguration);
        if (!dynamicConfigs.sdtEnabled()) {
            log.info("Skip creating the external writer for the topic {} because sdt is disabled", topic);
            return Optional.empty();
        }
        if (lakehouseConfiguration.getStreamTableMode() != LakehouseConfiguration.StreamTableMode.EXTERNAL) {
            return Optional.empty();
        }

        return switch (lakehouseConfiguration.getLakehouseType()) {
            case ICEBERG -> Optional.of(
                // TODO: reuse the IcebergExternalTableWriter if it already exists
                new IcebergExternalTableWriter(
                        topic, schemaTopic, entrySerdeFactory, lakehouseConfiguration, provider));
            case DELTA ->
                Optional.of(new DeltaExternalTableWriter(
                        topic, schemaTopic, entrySerdeFactory, lakehouseConfiguration, provider));
            default -> throw new IllegalArgumentException("Unsupported lakehouse type: "
                + lakehouseConfiguration.getLakehouseType());
        };
    }

    public Optional<LakehouseRecordWriter<FailureMessage>> getExternalDLTWriter(
            String topic, Map<String, String> properties) {
        log.info("Creating external DLT writer for topic: {} with properties: {}", topic, properties);
        LakehouseConfiguration lakehouseConfiguration = generateLakehouseConfiguration(properties);
        var dynamicConfigs = resolveDynamicConfigs(lakehouseConfiguration);
        if (!dynamicConfigs.sdtEnabled()) {
            log.info("Skip creating the external DLT writer for the topic {} because sdt is disabled", topic);
            return Optional.empty();
        }
        if (lakehouseConfiguration.getStreamTableMode() != LakehouseConfiguration.StreamTableMode.EXTERNAL) {
            return Optional.empty();
        }

        return switch (lakehouseConfiguration.getLakehouseType()) {
            case ICEBERG -> Optional.of(
                    new IcebergExternalDLTTableWriter(topic, lakehouseConfiguration, provider));
            case DELTA -> {
                if (!lakehouseConfiguration.isDeltaDltEnabled()) {
                    log.info("Skip creating the external Delta DLT writer for topic {} because {} is false",
                        topic, LakehouseConfiguration.DELTA_DLT_ENABLED);
                    yield Optional.empty();
                }
                yield Optional.of(new DeltaExternalDLTTableWriter(topic, lakehouseConfiguration, provider));
            }
            default -> throw new IllegalArgumentException("Unsupported lakehouse type: "
                    + lakehouseConfiguration.getLakehouseType());
        };
    }

    public ObjectPool.PooledObject<LakehouseReader> getPooledReader(String topic) {
        return readerPool.borrow(topic);
    }

    public void releasePooledReader(String topic, ObjectPool.PooledObject<LakehouseReader> reader) {
        readerPool.release(topic, reader, false);
    }

    public void cleanUp() {
        readerPool.cleanUp();
    }

    public LakehouseReader getReader(String topic) {
        return readers.computeIfAbsent(topic,
            t -> new LakehouseReader(t, entrySerdeFactory, configuration, provider));
    }

    public void close() throws IOException {
        List<String> closeFailedTopics = new ArrayList<>();
        for (Map.Entry<String, LakehouseReader> entry : readers.entrySet()) {
            String topic = entry.getKey();
            LakehouseReader reader = entry.getValue();
            try {
                reader.close();
            } catch (IOException e) {
                closeFailedTopics.add(topic);
                log.error("Failed to close LakehouseReader for topic: {}", topic, e);
            }
        }
        if (!closeFailedTopics.isEmpty()) {
            throw new IOException("Failed to close LakehouseReaders for topics: " + closeFailedTopics);
        }

        readerPool.close();
    }

    protected LakehouseConfiguration generateLakehouseConfiguration(Map<String, String> prop) {
        if (prop == null || prop.isEmpty()) {
            return configuration;
        }

        int configHash = (new TreeMap<>(prop)).hashCode();
        return configurationCache.computeIfAbsent(configHash, k -> {
            Properties properties = new Properties();
            properties.putAll(configuration.getProperties());
            properties.putAll(prop);
            return new LakehouseConfiguration(properties);
        });
    }

    public DynamicConfigs resolveDynamicConfigs(Map<String, String> properties) {
        return DynamicConfigs.fromTaskProperties(
            configuration.getProperties(), properties == null ? Map.of() : properties);
    }

    public DynamicConfigs resolveDynamicConfigs(LakehouseConfiguration lakehouseConfiguration) {
        return DynamicConfigs.fromTaskProperties(
            configuration.getProperties(),
            lakehouseConfiguration.getProperties().entrySet().stream()
                .collect(Collectors.toMap(
                    e -> String.valueOf(e.getKey()),
                    e -> String.valueOf(e.getValue())
                ))
        );
    }

}
