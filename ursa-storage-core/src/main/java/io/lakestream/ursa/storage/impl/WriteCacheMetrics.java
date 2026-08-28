/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.storage.impl;

import io.lakestream.ursa.metrics.InstrumentProvider;
import io.lakestream.ursa.metrics.Unit;
import io.lakestream.ursa.metrics.UpDownCounter;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.metrics.LongGauge;
import lombok.Getter;

@Getter
final class WriteCacheMetrics {

    private final LongGauge bufferUsedSize;
    private final UpDownCounter bufferSegmentUsedCount;
    private final UpDownCounter cacheSegmentUsedCount;

    WriteCacheMetrics(InstrumentProvider provider, int segment, int capacity) {
        registerDefaultMetrics(provider, segment, capacity);

        this.bufferUsedSize = provider.getMeter()
            .gaugeBuilder("ursa.storage.wal.writeCache.used")
            .setUnit(Unit.Bytes.toString())
            .setDescription("The used size of one cache")
            .ofLongs().build();
        this.bufferSegmentUsedCount = provider.newUpDownCounter(
            "ursa.storage.wal.writeCache.bufferSegment.used",
            Unit.Messages,
            "The number of used buffer segments in the write cache",
            Attributes.empty());
        this.cacheSegmentUsedCount = provider.newUpDownCounter(
            "ursa.storage.wal.writeCache.cacheSegment.used",
            Unit.Messages,
            "The number of used cache segments in the write cache",
            Attributes.empty());
    }

    private void registerDefaultMetrics(InstrumentProvider provider, int segment, int capacity) {
        var cacheSegmentGauge = provider.getMeter()
            .gaugeBuilder("ursa.storage.wal.writeCache.segment.count")
            .setUnit(Unit.Messages.toString())
            .setDescription("The number of buffer segments in the write cache")
            .ofLongs().build();
        cacheSegmentGauge.set(segment);

        var cacheCapacityGauge = provider.getMeter()
            .gaugeBuilder("ursa.storage.wal.writeCache.capacity")
            .setUnit(Unit.Bytes.toString())
            .setDescription("The capacity of each buffer segment in the write cache")
            .ofLongs().build();
        cacheCapacityGauge.set(capacity);
    }
}
