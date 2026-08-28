/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.storage.impl;

import static org.assertj.core.api.Assertions.assertThat;

import io.lakestream.ursa.metrics.InstrumentProvider;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.metrics.SdkMeterProvider;
import io.opentelemetry.sdk.metrics.data.MetricData;
import io.opentelemetry.sdk.testing.assertj.OpenTelemetryAssertions;
import io.opentelemetry.sdk.testing.exporter.InMemoryMetricReader;
import java.util.Collection;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import lombok.Cleanup;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

public class TestWriteCache {

    private InstrumentProvider buildInstrumentProvider(InMemoryMetricReader inMemoryMetricReader) {
        SdkMeterProvider meterProvider = SdkMeterProvider.builder()
            .registerMetricReader(inMemoryMetricReader)
            .build();
        OpenTelemetry otel = OpenTelemetrySdk.builder()
            .setMeterProvider(meterProvider)
            .build();
        return new InstrumentProvider(otel);
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testMetricsWithCacheEnabled(boolean enableCache) {
        InMemoryMetricReader inMemoryMetricReader = InMemoryMetricReader.create();
        InstrumentProvider provider = buildInstrumentProvider(inMemoryMetricReader);
        StorageConfig config = new StorageConfig();
        config.setWriteBufferSegment(10);
        config.setWriteCacheEnabled(enableCache);

        @Cleanup
        WriteCache writeCache = new WriteCache(PooledByteBufAllocator.DEFAULT, config, provider);

        Collection<MetricData> metrics = inMemoryMetricReader.collectAllMetrics();
        assertMetricLongGaugeValue(metrics, "ursa.storage.wal.writeCache.segment.count",
            Attributes.empty(), config.getWriteBufferSegment());
        assertMetricLongGaugeValue(metrics, "ursa.storage.wal.writeCache.capacity",
            Attributes.empty(), config.getWriteBufferSize());

        PersistCache cache = writeCache.poll();
        metrics = inMemoryMetricReader.collectAllMetrics();
        assertMetricUpDownCounterValue(metrics, "ursa.storage.wal.writeCache.bufferSegment.used",
            Attributes.empty(), 1);

        var pendingAdd = new PendingAdd(1, 1, Unpooled.wrappedBuffer(new byte[1024]), new CompletableFuture<>(), null);
        cache.put(pendingAdd);
        writeCache.returnToCache(cache, Optional.of("test"));
        metrics = inMemoryMetricReader.collectAllMetrics();
        assertMetricLongGaugeValue(metrics, "ursa.storage.wal.writeCache.used",
            Attributes.empty(), 1024);
        assertMetricUpDownCounterValue(metrics, "ursa.storage.wal.writeCache.bufferSegment.used",
            Attributes.empty(), 0);
        if (enableCache) {
            assertMetricUpDownCounterValue(metrics, "ursa.storage.wal.writeCache.cacheSegment.used",
                Attributes.empty(), 1);
        }


        for (int i = 0; i < 6; i++) {
            cache = writeCache.poll();
            writeCache.returnToCache(cache, Optional.of("test" + i));
        }
        metrics = inMemoryMetricReader.collectAllMetrics();
        assertMetricUpDownCounterValue(metrics, "ursa.storage.wal.writeCache.bufferSegment.used",
            Attributes.empty(), 0);
        if (enableCache) {
            assertMetricUpDownCounterValue(metrics, "ursa.storage.wal.writeCache.cacheSegment.used",
                Attributes.empty(), 5);
        }
    }

    static void assertMetricLongGaugeValue(Collection<MetricData> metrics, String metricName,
                                                  Attributes attributes, long expected) {
        assertMetricLongGaugeValue(metrics, metricName, attributes, actual -> assertThat(actual).isEqualTo(expected));
    }

    static void assertMetricLongGaugeValue(Collection<MetricData> metrics, String metricName,
                                                  Attributes attributes, Consumer<Long> valueConsumer) {
        assertThat(metrics)
            .anySatisfy(metric -> OpenTelemetryAssertions.assertThat(metric)
                .hasName(metricName)
                .hasLongGaugeSatisfying(gauge -> gauge.satisfies(
                    pointData -> assertThat(pointData.getPoints()).anySatisfy(
                        point -> {
                            assertThat(point.getAttributes()).isEqualTo(attributes);
                            valueConsumer.accept(point.getValue());
                        }))));
    }

    static void assertMetricUpDownCounterValue(Collection<MetricData> metrics, String metricName,
                                               Attributes attributes, long expected) {
        assertMetricUpDownCounterValue(metrics, metricName, attributes, actual -> assertThat(actual).isEqualTo(expected));
    }

    static void assertMetricUpDownCounterValue(Collection<MetricData> metrics, String metricName,
                                               Attributes attributes, Consumer<Long> valueConsumer) {
        assertThat(metrics)
            .anySatisfy(metric -> OpenTelemetryAssertions.assertThat(metric)
                .hasName(metricName)
                .hasLongSumSatisfying(sum -> sum.satisfies(
                    sumData -> assertThat(sumData.getPoints()).anySatisfy(
                        point -> {
                            assertThat(point.getAttributes()).isEqualTo(attributes);
                            valueConsumer.accept(point.getValue());
                        }))));
    }

}
