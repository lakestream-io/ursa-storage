/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.storage;

import io.lakestream.ursa.metrics.Counter;
import io.lakestream.ursa.metrics.InstrumentProvider;
import io.lakestream.ursa.metrics.LatencyHistogram;
import io.lakestream.ursa.metrics.Unit;
import io.lakestream.ursa.storage.impl.StorageConfig;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.metrics.ObservableLongGauge;
import java.util.concurrent.BlockingQueue;
import java.util.function.Function;
import lombok.Getter;

@Getter
public class WalStorageMetrics {

    public static final WalStorageMetrics NULL =
        new WalStorageMetrics("null", InstrumentProvider.NOOP, new StorageConfig());


    private final InstrumentProvider provider;
    private final String storageType;

    private final Counter putEntryRequestCount;
    private final Counter rejectedPutEntryRequestCount;
    private ObservableLongGauge putEntryPendingGauge;
    private final LatencyHistogram putEntryLatency;
    private final LatencyHistogram putEntryPendingLatency;
    private final LatencyHistogram putEntryToCacheLatency;
    private final LatencyHistogram getEntryDuration;
    private final LatencyHistogram getEntriesDuration;

    private ObservableLongGauge writeCacheFlushCallbackPendingGauge;
    private final LatencyHistogram writeCacheFlushLatency;


    private final Counter readCacheLoadingCount;
    private final Counter readCacheEvictionCount;
    private final LatencyHistogram readCacheLoadingDuration;
    private final Counter getEntriesCacheMiss;
    private ObservableLongGauge readCacheSizeInBytes;
    private ObservableLongGauge leasedSegments;

    public WalStorageMetrics(String storageType, InstrumentProvider provider, StorageConfig config) {
        this.provider = provider;
        this.storageType = storageType;

        this.putEntryRequestCount = provider.newCounter("ursa.storage.wal.putEntry.count", Unit.Request,
            "unrejected put request count", Attributes.builder().put("type", storageType).build());
        this.rejectedPutEntryRequestCount = provider.newCounter("ursa.storage.wal.putEntry.rejected.count",
            Unit.Request, "rejected put request count",
            Attributes.builder().put("type", storageType).build());
        this.putEntryLatency = provider.newLatencyHistogram("ursa.storage.wal.putEntry.duration",
            "put entry request latency", Attributes.builder().put("type", storageType).build());
        this.putEntryPendingLatency = provider.newLatencyHistogram("ursa.storage.wal.putEntry.pending.duration",
            "put entry request queue pending latency",
            Attributes.builder().put("type", storageType).build());
        this.putEntryToCacheLatency = provider.newLatencyHistogram("ursa.storage.wal.putEntry.cache.duration",
            "put entry request into buffer cache latency",
            Attributes.builder().put("type", storageType).build());
        this.getEntryDuration = provider.newLatencyHistogram(
            "ursa.storage.wal.getEntry.duration",
            "The simple wal storage get entry duration",
            Attributes.builder().put("type", storageType).build());
        this.getEntriesDuration = provider.newLatencyHistogram(
            "ursa.storage.wal.getEntries.duration",
            "The simple wal storage get entries duration",
            Attributes.builder().put("type", storageType).build());

        this.writeCacheFlushLatency = provider.newLatencyHistogram("ursa.storage.wal.writeCache.flush.duration",
            "write cache buffer flush latency",
            Attributes.builder().put("type", storageType).build());

        this.readCacheLoadingCount = provider.newCounter(
            "ursa.storage.wal.readCache.loading.count", Unit.Messages,
            "The simple wal storage reading cache loading count",
            Attributes.builder().put("type", storageType).build());
        this.readCacheEvictionCount = provider.newCounter(
            "ursa.storage.wal.readCache.eviction.count", Unit.Messages,
            "The simple wal storage reading cache eviction count",
            Attributes.builder().put("type", storageType).build());
        this.readCacheLoadingDuration = provider.newLatencyHistogram(
            "ursa.storage.wal.readCache.loading.duration",
            "The simple wal storage reading cache loading duration",
            Attributes.builder().put("type", storageType).build());
        this.getEntriesCacheMiss = provider.newCounter(
            "ursa.storage.wal.read.cache.missed", Unit.Messages,
            "The read cache missed count from either write cache or read cache",
            Attributes.of(AttributeKey.stringKey("component"), "ursa-storage-wal"));
    }

    public void buildPutEntryPendingGaugeWithCallback(BlockingQueue<?> queue) {
        this.putEntryPendingGauge = provider.getMeter().gaugeBuilder("ursa.storage.wal.putEntry.pending.count")
            .setUnit(Unit.Request.toString())
            .setDescription("put entry request queue pending count")
            .ofLongs()
            .buildWithCallback((gauge) -> {
                gauge.record(queue.size(), Attributes.builder().put("type", storageType).build());
            });
    }

    public void buildWriteCacheFlushCallbackPendingGauge(BlockingQueue<?> queue) {
        this.writeCacheFlushCallbackPendingGauge = provider.getMeter()
            .gaugeBuilder("ursa.storage.wal.writeCache.flushCallback.pending.count")
            .setUnit(Unit.Request.toString())
            .setDescription("write cache buffer flush callback pending count")
            .ofLongs()
            .buildWithCallback((gauge) -> {
                gauge.record(queue.size(), Attributes.builder().put("type", storageType).build());
            });
    }

    /**
     * Publishes the number of cache segments currently held under a read lease.
     *
     * <p>A read lease lives for one synchronous copy()/get() call, so in steady state this hovers at
     * or near zero and a sustained non-zero value means a lease was acquired and never released --
     * which pins a segment's buffer. This gauge is the production tripwire for that.
     */
    public void buildLeasedSegmentsGauge(Function<Void, Long> leaseCompute) {
        this.leasedSegments = provider.getMeter()
            .gaugeBuilder("ursa.storage.wal.cache.leasedSegments")
            .setUnit(Unit.Request.toString())
            .setDescription("The number of cache segments currently held under a read lease")
            .ofLongs()
            .buildWithCallback((gauge) -> {
                gauge.record(leaseCompute.apply(null),
                    Attributes.builder().put("type", storageType).build());
            });
    }

    public void buildReadCacheSizeInBytesGauge(Function<Void, Long> sizeCompute) {
        this.readCacheSizeInBytes = provider.getMeter()
            .gaugeBuilder("ursa.storage.wal.readCache.size")
            .setUnit(Unit.Bytes.toString())
            .setDescription("The readCache size in bytes")
            .ofLongs()
            .buildWithCallback((gauge) -> {
                gauge.record(sizeCompute.apply(null));
            });
    }
}
