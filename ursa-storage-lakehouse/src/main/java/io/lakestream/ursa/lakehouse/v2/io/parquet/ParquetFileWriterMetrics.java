/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.lakehouse.v2.io.parquet;

import io.lakestream.ursa.metrics.InstrumentProvider;
import io.lakestream.ursa.metrics.LatencyHistogram;
import io.opentelemetry.api.common.Attributes;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

@Getter(value = AccessLevel.PACKAGE)
@Slf4j
class ParquetFileWriterMetrics {
    private final LatencyHistogram writeRecord;
    private final LatencyHistogram writeMetadata;

    private static final Map<InstrumentProvider, ParquetFileWriterMetrics> instances = new ConcurrentHashMap<>();

    public static ParquetFileWriterMetrics getInstance(InstrumentProvider provider) {
        var instance = instances.computeIfAbsent(provider, ParquetFileWriterMetrics::new);
        if (instances.size() > 1) {
            log.warn("Multiple instances of ParquetFileWriterMetrics detected. This may lead to unexpected behavior.");
        }
        return instance;
    }

    private ParquetFileWriterMetrics(InstrumentProvider provider) {
        this.writeRecord = provider.newLatencyHistogram(
            "ursa.storage.lakehouse.parquet.write_record.duration",
            "Parquet file writer write record latency", Attributes.empty());
        this.writeMetadata = provider.newLatencyHistogram(
            "ursa.storage.lakehouse.parquet.write_metadata.duration",
            "Parquet file writer write metadata latency", Attributes.empty());
    }
}
