/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.storage;

import io.lakestream.ursa.metrics.Counter;
import io.lakestream.ursa.metrics.InstrumentProvider;
import io.lakestream.ursa.metrics.LatencyHistogram;
import io.lakestream.ursa.metrics.Unit;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import lombok.Getter;

@Getter
public class FileStorageMetrics {

    private final Counter requests;
    private final LatencyHistogram writeStorageLatency;
    private final LatencyHistogram readStorageLatency;
    private final LatencyHistogram readMetadataStorageLatency;
    private final LatencyHistogram calculateCrcLatency;
    private final LatencyHistogram deleteStorageLatency;
    private final Counter writeBytesCount;
    private final Counter readBytesCount;

    public FileStorageMetrics(InstrumentProvider instrumentProvider, String type) {
        this.requests = instrumentProvider.newCounter("ursa.storage.backend.storage.request", Unit.Request,
            "The operation request numbers to the backend storage, the storage can be "
                + "local file storage or the cloud storage",
            Attributes.of(AttributeKey.stringKey("component"), "ursa-storage-file-storage"));


        var typeAttribute = Attributes.of(AttributeKey.stringKey("type"), type);
        this.writeStorageLatency = instrumentProvider.newLatencyHistogram("ursa.storage.backend.write.duration",
            "BackendStorage write latency", typeAttribute);
        this.readStorageLatency = instrumentProvider.newLatencyHistogram("ursa.storage.backend.read.duration",
            "BackendStorage read latency", typeAttribute);
        this.readMetadataStorageLatency =
            instrumentProvider.newLatencyHistogram("ursa.storage.backend.metadata.read.duration",
                "BackendStorage read metadata latency", typeAttribute);
        this.calculateCrcLatency = instrumentProvider.newLatencyHistogram("ursa.storage.backend.crc.duration",
            "BackendStorage CRC calculate latency", typeAttribute);
        this.deleteStorageLatency = instrumentProvider.newLatencyHistogram("ursa.storage.backend.delete.duration",
            "BackendStorage delete latency", typeAttribute);
        this.writeBytesCount = instrumentProvider.newCounter("ursa.storage.backend.write.bytes.count",
            Unit.Bytes, "BackendStorage write bytes count", typeAttribute);
        this.readBytesCount = instrumentProvider.newCounter("ursa.storage.backend.read.bytes.count",
            Unit.Bytes, "BackendStorage read bytes count", typeAttribute);
    }
}
