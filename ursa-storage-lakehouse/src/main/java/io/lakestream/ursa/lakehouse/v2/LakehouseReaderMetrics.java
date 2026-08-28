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
class LakehouseReaderMetrics {

    // Constants for metric attributes
    public static final String READER_SERDE_TYPE = "reader_serde_type";

    private final InstrumentProvider instrumentProvider;

    private final LatencyHistogram seek;
    private final LatencyHistogram readAll;
    private final LatencyHistogram readRecord;
    private final LatencyHistogram decode;

    private static final Map<InstrumentProvider, LakehouseReaderMetrics> instances = new ConcurrentHashMap<>();

    public static LakehouseReaderMetrics getInstance(InstrumentProvider provider) {
        var instance = instances.computeIfAbsent(provider, LakehouseReaderMetrics::new);
        if (instances.size() > 1) {
            log.warn("Multiple instances of LakehouseReaderMetrics detected. This may lead to unexpected behavior.");
        }
        return instance;
    }

    private LakehouseReaderMetrics(InstrumentProvider provider) {
        this.instrumentProvider = provider;
        this.seek = provider.newLatencyHistogram(
            "ursa.storage.lakehouse.reader.seek.duration",
            "Lakehouse reader seek latency", Attributes.empty());
        this.readAll = provider.newLatencyHistogram(
            "ursa.storage.lakehouse.reader.read_all.duration",
            "Lakehouse reader read all latency", Attributes.empty());
        this.readRecord = provider.newLatencyHistogram(
            "ursa.storage.lakehouse.reader.read_record.duration",
            "Lakehouse reader read record latency", Attributes.empty());
        this.decode = provider.newLatencyHistogram(
            "ursa.storage.lakehouse.reader.decode.duration",
            "Lakehouse reader decode latency", Attributes.empty());
    }
}
