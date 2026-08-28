/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.compaction.metrics;

import com.google.common.collect.Lists;
import io.lakestream.ursa.metrics.Unit;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.metrics.DoubleHistogram;
import io.opentelemetry.api.metrics.DoubleHistogramBuilder;
import io.opentelemetry.api.metrics.Meter;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class CompactionLatencyHistogram {

    // Used for tests
    public static final CompactionLatencyHistogram NOOP = new CompactionLatencyHistogram() {
        public void recordSuccess(long latencyNanos) {
        }

        public void recordFailure(long latencyNanos) {
        }
    };

    private static final List<Double> latencyHistogramBuckets =
            Lists.newArrayList(10.0, 30.0, 60.0, 90.0, 120.0, 180.0, 240.0, 300.0, 600.0, 1200.0, 1800.0);

    private static final double NANOS = TimeUnit.SECONDS.toNanos(1);

    private final Attributes successAttributes;

    private final Attributes failedAttributes;
    private final DoubleHistogram histogram;

    private CompactionLatencyHistogram() {
        successAttributes = null;
        failedAttributes = null;
        histogram = null;
    }

    public CompactionLatencyHistogram(Meter meter, String name, String description, Attributes attributes) {
        DoubleHistogramBuilder builder = meter.histogramBuilder(name)
                .setDescription(description)
                .setUnit(Unit.Seconds.toString())
                .setExplicitBucketBoundariesAdvice(latencyHistogramBuckets);

        successAttributes = attributes.toBuilder()
                .put("ursa.response.status", "success")
                .build();
        failedAttributes = attributes.toBuilder()
                .put("ursa.response.status", "failed")
                .build();
        this.histogram = builder.build();
    }

    private CompactionLatencyHistogram(DoubleHistogram histogram,
                                       Attributes successAttributes,
                                       Attributes failedAttributes) {
        this.histogram = histogram;
        this.successAttributes = successAttributes;
        this.failedAttributes = failedAttributes;
    }

    /**
     * Create a new histograms that inherits the old histograms attributes and adds new ones.
     */
    public CompactionLatencyHistogram withAttributes(Attributes attributes) {
        return new CompactionLatencyHistogram(
                histogram,
                successAttributes.toBuilder().putAll(attributes).build(),
                failedAttributes.toBuilder().putAll(attributes).build()
        );
    }


    public void recordSuccess(long latencyNanos) {
        histogram.record(latencyNanos / NANOS, successAttributes);
    }

    public void recordFailure(long latencyNanos) {
        histogram.record(latencyNanos / NANOS, failedAttributes);
    }
}
