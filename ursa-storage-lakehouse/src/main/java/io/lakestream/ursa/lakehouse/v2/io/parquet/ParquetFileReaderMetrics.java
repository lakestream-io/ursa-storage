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
class ParquetFileReaderMetrics {

    private final LatencyHistogram readRecord;
    private final LatencyHistogram readMetadata;
    private final LatencyHistogram seekByOffset;
    private final LatencyHistogram seekBySecondaryIndex;

    private static final Map<InstrumentProvider, ParquetFileReaderMetrics> instances = new ConcurrentHashMap<>();

    public static ParquetFileReaderMetrics getInstance(InstrumentProvider provider) {
        var instance = instances.computeIfAbsent(provider, ParquetFileReaderMetrics::new);
        if (instances.size() > 1) {
            log.warn("Multiple instances of ParquetFileReaderMetrics detected. This may lead to unexpected behavior.");
        }
        return instance;
    }

    private ParquetFileReaderMetrics(InstrumentProvider provider) {
        this.readRecord = provider.newLatencyHistogram(
            "ursa.storage.lakehouse.parquet.read_record.duration",
            "Parquet file reader read record latency", Attributes.empty());
        this.readMetadata = provider.newLatencyHistogram(
            "ursa.storage.lakehouse.parquet.read_metadata.duration",
            "Parquet file reader read metadata latency", Attributes.empty());
        this.seekByOffset = provider.newLatencyHistogram(
            "ursa.storage.lakehouse.parquet.seek_by_offset.duration",
            "Parquet file reader seek by offset latency", Attributes.empty());
        this.seekBySecondaryIndex = provider.newLatencyHistogram(
            "ursa.storage.lakehouse.parquet.seek_by_secondary_index.duration",
            "Parquet file reader seek by secondary index latency", Attributes.empty());
    }
}
