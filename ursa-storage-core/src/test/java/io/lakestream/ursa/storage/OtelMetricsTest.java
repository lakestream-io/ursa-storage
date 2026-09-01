/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.storage;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;

import io.lakestream.ursa.metrics.InstrumentProvider;
import io.lakestream.ursa.storage.StorageApi.StreamWriteLease;
import io.lakestream.ursa.storage.impl.PersistStorageApi;
import io.lakestream.ursa.storage.impl.StorageConfig;
import io.netty.buffer.Unpooled;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.metrics.SdkMeterProvider;
import io.opentelemetry.sdk.metrics.data.MetricData;
import io.opentelemetry.sdk.testing.assertj.OpenTelemetryAssertions;
import io.opentelemetry.sdk.testing.exporter.InMemoryMetricReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

@Slf4j
public class OtelMetricsTest {

    private UrsaStorageTestBase ursaStorageTestBase;
    private InMemoryMetricReader inMemoryMetricReader;
    private PersistStorageApi storage;

    public void setup(boolean enableWriteCache) throws Exception {
        inMemoryMetricReader = InMemoryMetricReader.create();
        SdkMeterProvider meterProvider = SdkMeterProvider.builder()
            .registerMetricReader(inMemoryMetricReader)
            .build();
        OpenTelemetry otel = OpenTelemetrySdk.builder()
            .setMeterProvider(meterProvider)
            .build();

        InstrumentProvider instrumentProvider = new InstrumentProvider(otel);
        ursaStorageTestBase = new UrsaStorageTestBase();
        ursaStorageTestBase.setup(UrsaStorageTestBase.UrsaStorageTestConfig.builder()
            .instrumentProvider(instrumentProvider)
            .openTelemetry(otel)
            .ursaConfig(StorageConfig.builder()
                .writeCacheEnabled(enableWriteCache)
                .readCacheMemorySize(16 * 1024 * 1024)
                .build())
            .build());
        this.storage = ursaStorageTestBase.createStorageApi(instrumentProvider);
    }

    public void cleanup() throws Exception {
        ursaStorageTestBase.cleanup();
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testMetrics(boolean enableWriteCache) throws Exception {
        Map<Long, StreamWriteLease> writeLeases = new HashMap<>();
        try {
            setup(enableWriteCache);
            Random random = new Random();
            Map<Long, AtomicLong> counter = new HashMap<>();
            Map<Long, List<CompletableFuture<AddResult>>> futures = new HashMap<>();
            for (int i = 0; i < 100; i++) {
                long streamId = random.nextInt(10);
                AtomicLong entryCounter = counter.computeIfAbsent(streamId, k -> new AtomicLong(0));
                List<CompletableFuture<AddResult>> result = futures.computeIfAbsent(streamId, k -> new ArrayList<>());
                writeLeases.computeIfAbsent(
                    streamId, id -> storage.acquireStreamWriteLease(id).join());
                result.add(storage.append(streamId, 1,
                    Unpooled.wrappedBuffer(("entry-" + entryCounter.getAndIncrement()).getBytes())));
                if (i == 50) {
                    TimeUnit.MILLISECONDS.sleep(300);
                }
            }
            List<CompletableFuture<AddResult>> list = new ArrayList<>();
            for (List<CompletableFuture<AddResult>> value : futures.values()) {
                list.addAll(value);
            }
            CompletableFuture.allOf(list.toArray(new CompletableFuture[0])).join();

            Map<Long, AtomicLong> verifyCounter = new HashMap<>();
            Set<Long> keys = futures.keySet();
            for (Long key : keys) {
                for (CompletableFuture<AddResult> entryHeaderCompletableFuture : futures.get(key)) {
                    AddResult addResult = entryHeaderCompletableFuture.join();
                    var eh = addResult.header();
                    assertEquals(verifyCounter.computeIfAbsent(key, k -> new AtomicLong(0)).getAndIncrement(), eh.offset());
                    assertEquals(1, eh.numberOfMessages());
                }
            }

            long lastKey = 0;
            for (Long key : keys) {
                List<Entry> entries = storage.readEntries(key, 0, 100, Integer.MAX_VALUE).join();
                assertEquals(verifyCounter.get(key).get(), entries.size());
                for (int i = 0; i < entries.size(); i++) {
                    Entry e = entries.get(i);
                    assertEquals(i, e.header().offset());
                    assertEquals(1, e.header().numberOfMessages());
                    assertEquals("entry-" + i, e.payload().toString(StandardCharsets.UTF_8));
                    e.payload().release();
                }
                lastKey = key;
            }
            storage.read(lastKey, 0).get();
        } finally {
            writeLeases.values().forEach(StreamWriteLease::close);
            cleanup();
        }
            Collection<MetricData> metrics = inMemoryMetricReader.collectAllMetrics();
            // storage layer metrics
            if (enableWriteCache) {
                assertThat(metrics).noneSatisfy(metric ->
                    OpenTelemetryAssertions.assertThat(metric).hasName("ursa.storage.backend.read.bytes.count"));
                assertThat(metrics).noneSatisfy(metric ->
                    OpenTelemetryAssertions.assertThat(metric).hasName("ursa.storage.backend.read.duration"));
                assertThat(metrics).noneSatisfy(metric ->
                    OpenTelemetryAssertions.assertThat(metric).hasName("ursa.storage.wal.read.cache.missed"));
                assertThat(metrics).noneSatisfy(metric ->
                    OpenTelemetryAssertions.assertThat(metric).hasName("ursa.storage.backend.read.duration"));
                assertThat(metrics).noneSatisfy(metric ->
                    OpenTelemetryAssertions.assertThat(metric).hasName("ursa.storage.wal.readCache.loading.duration"));
                assertThat(metrics).noneSatisfy(metric ->
                    OpenTelemetryAssertions.assertThat(metric).hasName("ursa.storage.wal.readCache.loading.count"));
                assertThat(metrics).noneSatisfy(metric ->
                    OpenTelemetryAssertions.assertThat(metric).hasName("ursa.storage.wal.readCache.eviction.count"));
            } else {
                assertThat(metrics).anySatisfy(metric ->
                    OpenTelemetryAssertions.assertThat(metric).hasName("ursa.storage.backend.read.bytes.count"));
                assertThat(metrics).anySatisfy(metric ->
                    OpenTelemetryAssertions.assertThat(metric).hasName("ursa.storage.backend.read.duration"));
                assertThat(metrics).anySatisfy(metric ->
                    OpenTelemetryAssertions.assertThat(metric).hasName("ursa.storage.wal.read.cache.missed"));
                assertThat(metrics).anySatisfy(metric ->
                    OpenTelemetryAssertions.assertThat(metric).hasName("ursa.storage.backend.read.duration"));
                assertThat(metrics).anySatisfy(metric ->
                    OpenTelemetryAssertions.assertThat(metric).hasName("ursa.storage.wal.readCache.loading.duration"));
                assertThat(metrics).anySatisfy(metric ->
                    OpenTelemetryAssertions.assertThat(metric).hasName("ursa.storage.wal.readCache.loading.count"));
                assertThat(metrics).anySatisfy(metric ->
                    OpenTelemetryAssertions.assertThat(metric).hasName("ursa.storage.wal.readCache.eviction.count"));
            }
            assertThat(metrics).anySatisfy(metric ->
                OpenTelemetryAssertions.assertThat(metric).hasName("ursa.storage.backend.write.duration"));
            assertThat(metrics).anySatisfy(metric ->
                OpenTelemetryAssertions.assertThat(metric).hasName("ursa.storage.add.entry.duration"));
            assertThat(metrics).anySatisfy(metric ->
                OpenTelemetryAssertions.assertThat(metric).hasName("ursa.storage.read.entry.size"));
            assertThat(metrics).anySatisfy(metric ->
                OpenTelemetryAssertions.assertThat(metric).hasName("ursa.storage.backend.write.bytes.count"));
            assertThat(metrics).anySatisfy(metric ->
                OpenTelemetryAssertions.assertThat(metric).hasName("ursa.storage.read.entry.count"));
            assertThat(metrics).anySatisfy(metric ->
                OpenTelemetryAssertions.assertThat(metric).hasName("ursa.storage.add.messages.count"));
            assertThat(metrics).anySatisfy(metric ->
                OpenTelemetryAssertions.assertThat(metric).hasName("ursa.storage.add.messages.size"));
            assertThat(metrics).anySatisfy(metric ->
                OpenTelemetryAssertions.assertThat(metric).hasName("ursa.storage.backend.crc.duration"));
            assertThat(metrics).anySatisfy(metric ->
                OpenTelemetryAssertions.assertThat(metric).hasName("ursa.storage.read.entry.duration"));

            // oxia client metrics
            assertThat(metrics).anySatisfy(metric ->
                OpenTelemetryAssertions.assertThat(metric).hasName("oxia.client.ops.size"));
            assertThat(metrics).anySatisfy(metric ->
                OpenTelemetryAssertions.assertThat(metric).hasName("oxia.client.ops.req"));
            assertThat(metrics).anySatisfy(metric ->
                OpenTelemetryAssertions.assertThat(metric).hasName("oxia.client.ops"));
            assertThat(metrics).anySatisfy(metric ->
                OpenTelemetryAssertions.assertThat(metric).hasName("oxia.client.ops.pending"));
            assertThat(metrics).anySatisfy(metric ->
                OpenTelemetryAssertions.assertThat(metric).hasName("oxia.client.shard.assignments.count"));
            assertThat(metrics).anySatisfy(metric ->
                OpenTelemetryAssertions.assertThat(metric).hasName("oxia.client.ops.outstanding"));

            // file storage metrics
            assertThat(metrics).anySatisfy(metric ->
                OpenTelemetryAssertions.assertThat(metric).hasName("ursa.storage.backend.storage.request"));

            // wal storage metrics
            assertThat(metrics).anySatisfy(metric ->
                OpenTelemetryAssertions.assertThat(metric).hasName("ursa.storage.wal.getEntry.duration"));
            assertThat(metrics).anySatisfy(metric ->
                OpenTelemetryAssertions.assertThat(metric).hasName("ursa.storage.wal.getEntries.duration"));

    }
}
