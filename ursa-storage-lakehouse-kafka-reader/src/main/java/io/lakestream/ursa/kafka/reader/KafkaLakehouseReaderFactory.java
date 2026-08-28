/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.kafka.reader;

import io.lakestream.ursa.lakestream.reader.CompactedObjectReader;
import io.lakestream.ursa.lakestream.reader.CompactedObjectReaderFactory;
import io.lakestream.ursa.metrics.InstrumentProvider;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Kafka-only lakehouse reader factory.
 *
 * <p>This factory deliberately supports the Kafka integration's V2
 * {@code KAFKA_BATCHED_RAW_PARQUET} files, which are addressed by a
 * {@code ManagedTableFileIndex} in each Lakestream entry index. It does not support the
 * historical generic V1 lakehouse format. The implementation is isolated from the generic
 * writer and mixed-source materialization classes in {@code ursa-storage-lakehouse}.
 */
public final class KafkaLakehouseReaderFactory implements CompactedObjectReaderFactory {

    private final AtomicBoolean closed = new AtomicBoolean();
    private ExecutorService executor;
    private ReaderConfiguration configuration;

    @Override
    public synchronized void initialize(Properties properties, InstrumentProvider provider) {
        if (closed.get()) {
            throw new IllegalStateException("Kafka lakehouse reader factory is closed");
        }
        if (executor != null) {
            throw new IllegalStateException("Kafka lakehouse reader factory is already initialized");
        }
        ReaderConfiguration newConfiguration = new ReaderConfiguration(properties);
        int threadCount = Integer.parseInt(properties.getProperty("lakehouseIOThreadNum", "4"));
        if (threadCount <= 0) {
            throw new IllegalArgumentException("lakehouseIOThreadNum must be positive");
        }
        ExecutorService newExecutor = Executors.newFixedThreadPool(threadCount, namedThreadFactory());
        configuration = newConfiguration;
        executor = newExecutor;
    }

    @Override
    public CompactedObjectReader open(String logName) {
        if (closed.get()) {
            throw new IllegalStateException("Kafka lakehouse reader factory is closed");
        }
        if (configuration == null || executor == null) {
            throw new IllegalStateException("Kafka lakehouse reader factory is not initialized");
        }
        return new KafkaLakehouseReader(logName, configuration, executor);
    }

    @Override
    public synchronized void close() {
        if (closed.compareAndSet(false, true) && executor != null) {
            executor.shutdown();
        }
    }

    private static ThreadFactory namedThreadFactory() {
        AtomicInteger sequence = new AtomicInteger();
        return task -> new Thread(task, "kafka-lakehouse-reader-" + sequence.getAndIncrement());
    }
}
