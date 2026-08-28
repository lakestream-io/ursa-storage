/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.storage.impl;

import io.lakestream.api.Position;
import io.lakestream.ursa.metrics.Counter;
import io.lakestream.ursa.metrics.LatencyHistogram;
import io.lakestream.ursa.storage.WalStorage;
import io.opentelemetry.api.metrics.ObservableLongGauge;
import java.util.List;

abstract class AbstractWalStorage implements WalStorage {

    protected Counter putEntryRequestCount;
    protected Counter rejectedPutEntryRequestCount;
    protected ObservableLongGauge putEntryPendingGauge;

    protected LatencyHistogram putEntryLatency;
    protected LatencyHistogram putEntryPendingLatency;
    protected LatencyHistogram putEntryToCacheLatency;

    protected ObservableLongGauge writeCacheCountGauge;
    protected ObservableLongGauge writeCacheAvailableCountGauge;
    protected ObservableLongGauge writeCacheSizeGauge;
    protected ObservableLongGauge writeCacheCapacityGauge;
    protected ObservableLongGauge writeCacheFlushCallbackPendingGauge;
    protected LatencyHistogram writeCacheFlushLatency;

    protected Counter readCacheLoadingCount;
    protected Counter readCacheEvicationCount;
    protected LatencyHistogram readCacheLoadingDuration;
    protected LatencyHistogram getEntryDuration;
    protected LatencyHistogram getEntriesDuration;


    public void cleanup() {
        if (putEntryPendingGauge != null) {
            putEntryPendingGauge.close();
        }
        if (writeCacheSizeGauge != null) {
            writeCacheSizeGauge.close();
        }
        if (writeCacheCountGauge != null) {
            writeCacheCountGauge.close();
        }
        if (writeCacheCapacityGauge != null) {
            writeCacheCapacityGauge.close();
        }
        if (writeCacheAvailableCountGauge != null) {
            writeCacheAvailableCountGauge.close();
        }
        if (writeCacheFlushCallbackPendingGauge != null) {
            writeCacheFlushCallbackPendingGauge.close();
        }
    }

    @Override
    public void preFetch(long id, List<Position> positions) {
        // no-op
    }
}
