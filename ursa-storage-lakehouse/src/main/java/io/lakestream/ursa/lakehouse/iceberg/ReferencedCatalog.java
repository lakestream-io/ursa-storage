/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.lakehouse.iceberg;

import java.io.Closeable;
import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.iceberg.catalog.Catalog;

@Slf4j
public class ReferencedCatalog {
    @Getter(AccessLevel.PACKAGE)
    private final Catalog catalog;
    private final AtomicInteger refCnt = new AtomicInteger(0);

    // Monotonic clock — unaffected by wall-clock jumps (NTP, manual changes).
    @Getter(AccessLevel.PACKAGE)
    private final long creationNanos;
    private final Duration maxOpenTime;
    private final AtomicBoolean closed = new AtomicBoolean(false);

    public ReferencedCatalog(Catalog catalog, Duration maxOpenTime) {
        this.catalog = catalog;
        this.creationNanos = System.nanoTime();
        this.maxOpenTime = maxOpenTime;
    }

    public void retain() {
        refCnt.incrementAndGet();
    }

    public boolean isExpired() {
        return (System.nanoTime() - creationNanos) > maxOpenTime.toNanos();
    }

    public int release() {
        var cnt = refCnt.decrementAndGet();
        if (cnt == 0 && isExpired()) {
            close();
        } else if (cnt < 0) {
            throw new IllegalStateException("Reference count for catalog " + catalog.name() + " went negative: " + cnt);
        }
        return cnt;
    }

    public int getRefCount() {
        return refCnt.get();
    }

    public boolean isClosed() {
        return closed.get();
    }

    public void safeClose() {
        if (getRefCount() == 0) {
            close();
        } else {
            log.warn("Catalog {} is still in use with ref count {}, cannot close now.",
                    catalog.name(), getRefCount());
        }
    }

    public void close() {
        if (closed.compareAndSet(false, true)) {
            if (catalog instanceof Closeable closeableCatalog) {
                try {
                    closeableCatalog.close();
                    log.info("Closed catalog instance {}", catalog.name());
                } catch (IOException e) {
                    log.error("Failed to close catalog {}", catalog.name(), e);
                }
            }
        }
    }
}
