/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.metrics;

import com.google.common.collect.Lists;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.metrics.LongHistogram;
import io.opentelemetry.api.metrics.LongHistogramBuilder;
import io.opentelemetry.api.metrics.Meter;
import java.util.List;

public class RequestSizeHistogram {

    // Used for tests
    public static final RequestSizeHistogram NOOP = new RequestSizeHistogram() {
        public void recordSuccess(long size) {}
        public void recordFailure(long size) {}
    };

    private static final List<Long> requestSizeBuckets =
            Lists.newArrayList(10L, 50L, 100L, 200L, 300L, 400L, 500L, 1000L);

    private final Attributes successAttributes;
    private final Attributes failedAttributes;
    private final LongHistogram histogram;

    private RequestSizeHistogram() {
        this.successAttributes = null;
        this.failedAttributes = null;
        this.histogram = null;
    }

    public RequestSizeHistogram(Meter meter, String name, String description, Attributes attributes) {
        LongHistogramBuilder builder = meter.histogramBuilder(name)
                .ofLongs()
                .setDescription(description)
                .setUnit("By") // bytes, or whatever unit makes sense
                .setExplicitBucketBoundariesAdvice(requestSizeBuckets);

        this.successAttributes = attributes.toBuilder()
                .put("ursa.response.status", "success")
                .build();
        this.failedAttributes = attributes.toBuilder()
                .put("ursa.response.status", "failed")
                .build();

        this.histogram = builder.build();
    }

    private RequestSizeHistogram(LongHistogram histogram, Attributes successAttributes, Attributes failedAttributes) {
        this.histogram = histogram;
        this.successAttributes = successAttributes;
        this.failedAttributes = failedAttributes;
    }

    public RequestSizeHistogram withAttributes(Attributes attributes) {
        return new RequestSizeHistogram(
                histogram,
                successAttributes.toBuilder().putAll(attributes).build(),
                failedAttributes.toBuilder().putAll(attributes).build()
        );
    }

    public void recordSuccess(long size) {
        histogram.record(size, successAttributes);
    }

    public void recordFailure(long size) {
        histogram.record(size, failedAttributes);
    }
}
