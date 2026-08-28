/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.lakehouse.v2;

import io.lakestream.ursa.metrics.InstrumentProvider;
import io.lakestream.ursa.metrics.LatencyHistogram;
import io.opentelemetry.api.common.Attributes;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

@Getter(value = AccessLevel.MODULE)
@Slf4j
public class LakehouseWriterMetrics {

    // Constants for metric attributes
    public static final String WRITER_CLASS_NAME = "writer_class";
    public static final String WRITER_SERDE_TYPE = "writer_serde_type";

    private final LatencyHistogram beforeWrite;
    private final LatencyHistogram writeAll;
    private final LatencyHistogram writeRecord;
    private final LatencyHistogram encode;

    private static final Map<InstrumentProvider, LakehouseWriterMetrics> instances = new ConcurrentHashMap<>();

    public static LakehouseWriterMetrics getInstance(InstrumentProvider provider) {
        var instance = instances.computeIfAbsent(provider, LakehouseWriterMetrics::new);
        if (instances.size() > 1) {
            log.warn("Multiple instances of LakehouseWriterMetrics detected. This may lead to unexpected behavior.");
        }
        return instance;
    }

    private LakehouseWriterMetrics(InstrumentProvider provider) {
        this.beforeWrite = provider.newLatencyHistogram(
            "ursa.storage.lakehouse.writer.before_write.duration",
            "Lakehouse writer before write latency", Attributes.empty());
        this.writeAll = provider.newLatencyHistogram(
            "ursa.storage.lakehouse.writer.write_all.duration",
            "Lakehouse writer write all latency", Attributes.empty());
        this.writeRecord = provider.newLatencyHistogram(
            "ursa.storage.lakehouse.writer.write_record.duration",
            "Lakehouse writer write record latency", Attributes.empty());
        this.encode = provider.newLatencyHistogram(
            "ursa.storage.lakehouse.writer.encode.duration",
            "Lakehouse writer encode latency", Attributes.empty());
    }
}
