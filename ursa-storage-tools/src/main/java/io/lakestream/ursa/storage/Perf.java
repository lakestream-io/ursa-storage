/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.storage;

import static java.util.concurrent.TimeUnit.NANOSECONDS;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.google.common.util.concurrent.RateLimiter;
import io.lakestream.ursa.metrics.InstrumentProvider;
import io.lakestream.ursa.storage.impl.StorageConfig;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.opentelemetry.sdk.autoconfigure.AutoConfiguredOpenTelemetrySdk;
import io.oxia.client.api.AsyncOxiaClient;
import io.oxia.client.api.OxiaClientBuilder;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Function;
import lombok.extern.slf4j.Slf4j;
import org.HdrHistogram.Histogram;
import org.HdrHistogram.Recorder;

@Slf4j
public class Perf {
    static final class PerfArguments {
        @Parameter(
            names = {"-h", "--help"},
            description = "Help message",
            help = true)
        boolean help;

        @Parameter(
            names = {"-c", "--configuration"},
            description = "Configuration file")
        String configuration = "ursa-storage.conf";

        @Parameter(
            names = {"-s", "--streams"},
            description = "Streams numbers")
        int streams = 1;

        @Parameter(names = {"-so", "--start-offset"})
        long startOffset = 0;

        @Parameter(
            names = {"-rr", "--requests-rate"},
            description = "Rate of requests per second")
        double requestsRate = 1000;

        @Parameter(
            names = {"-vs", "--value-size"},
            description = "Size of each value in bytes")
        int valueSize = 1024;

        @Parameter(
            names = {"-mo", "--max-outstanding"},
            description = "Maximum number of outstanding requests")
        int maxOutstandingRequests = 1000;

        @Parameter(
            names = {"-me", "--max-entries"},
            description = "Maximum number of entries to read per request")
        int maxEntriesPerRead = 100;

        @Parameter(
            names = {"-ms", "--max-size"},
            description = "Maximum size in bytes to read per request")
        int maxSizePerRead = 1024 * 1024;

        @Parameter(
            names = {"-d", "--delay-read"},
            description = "Delay read in seconds")
        int delayReadInSeconds = -1;
    }

    private static final ConcurrentHashMap<Long, Long> writeOffsets = new ConcurrentHashMap<>();
    private static final ConcurrentHashMap<Long, Long> readOffsets = new ConcurrentHashMap<>();

    private static final LongAdder writeOps = new LongAdder();
    private static final LongAdder writeBytes = new LongAdder();
    private static final LongAdder readOps = new LongAdder();
    private static final LongAdder readBytes = new LongAdder();
    private static final LongAdder writeFailed = new LongAdder();
    private static final LongAdder readFailed = new LongAdder();
    private static final Recorder writeLatency = new Recorder(TimeUnit.SECONDS.toMicros(120_000), 5);
    private static final Recorder readLatency = new Recorder(TimeUnit.SECONDS.toMicros(120_000), 5);

    static final Function<Double, String> DEC_FORMAT = d -> String.format("%7.1f", d);
    static final Function<Double, String> INT_FORMAT = d -> String.format("%7.0f", d);

    private static final PerfArguments arguments = new PerfArguments();
    public static void main(String[] args) throws Exception {
        JCommander jc = new JCommander(arguments);
        jc.setProgramName("ursa perf");

        try {
            jc.parse(args);
        } catch (ParameterException e) {
            System.out.println(e.getMessage());
            jc.usage();
            System.exit(1);
        }

        if (arguments.help) {
            jc.usage();
            System.exit(1);
        }

        AutoConfiguredOpenTelemetrySdk sdk = AutoConfiguredOpenTelemetrySdk.builder()
            .build();
        InstrumentProvider instrumentProvider = new InstrumentProvider(sdk.getOpenTelemetrySdk());

        StorageConfig config = StorageConfig.fromProperties(
                StorageConfig.loadConfigurationFile(arguments.configuration));
        var oxiaUrl = Utils.validateOxiaUrl(config.getOxiaStorageUrl());
        AsyncOxiaClient oxiaClient = OxiaClientBuilder.create(oxiaUrl.getLeft())
            .namespace(oxiaUrl.getRight())
            .openTelemetry(sdk.getOpenTelemetrySdk())
            .asyncClient().get();
        var storageApi = new UrsaStorage(config, sdk.getOpenTelemetrySdk(), oxiaClient).getDefaultStorageApi();

        ExecutorService executor = Executors.newCachedThreadPool();

        List<Long> streams = new ArrayList<>(arguments.streams);
        for (int i = 0; i < arguments.streams; i++) {
            var streamId = storageApi.generateStreamId().get();
            streams.add(streamId);
            log.info("Generate traffic for stream {}", streamId);
            executor.execute(() -> generateWriteTraffic(storageApi, streamId));
            executor.execute(() -> generateReadTraffic(storageApi, streamId));
        }

        Histogram writeReportHistogram = null;
        Histogram readReportHistogram = null;

        long oldTime = System.nanoTime();
        while (true) {
            try {
                Thread.sleep(10000);
            } catch (InterruptedException e) {
                break;
            }

            long now = System.nanoTime();
            double elapsed = (now - oldTime) / 1e9;

            double writeRate = writeOps.sumThenReset() / elapsed;
            double writeBytesRate = writeBytes.sumThenReset() / elapsed / 1024 / 1024;
            double readRate = readOps.sumThenReset() / elapsed;
            double readBytesRate = readBytes.sumThenReset() / elapsed / 1024 / 1024;
            double failedWriteRate = writeFailed.sumThenReset() / elapsed;
            double failedReadRate = readFailed.sumThenReset() / elapsed;

            writeReportHistogram = writeLatency.getIntervalHistogram(writeReportHistogram);
            readReportHistogram = readLatency.getIntervalHistogram(readReportHistogram);

            log.info("""
                    Stats - Total ops: {} ops/s - Failed ops: {} ops/s
                       Write ops {} w/s {} MB/s  Latency ms: 50% {} - 95% {} - 99% {} - 99.9% {} - max {}
                            info {}
                       Read  ops {} r/s {} MB/s Latency ms: 50% {} - 95% {} - 99% {} - 99.9% {} - max {}
                            info {}
                    """,
                INT_FORMAT.apply(writeRate + readRate), INT_FORMAT.apply(failedWriteRate + failedReadRate),

                INT_FORMAT.apply(writeRate),
                DEC_FORMAT.apply(writeBytesRate),
                DEC_FORMAT.apply(writeReportHistogram.getValueAtPercentile(50) / 1000.0),
                DEC_FORMAT.apply(writeReportHistogram.getValueAtPercentile(95) / 1000.0),
                DEC_FORMAT.apply(writeReportHistogram.getValueAtPercentile(99) / 1000.0),
                DEC_FORMAT.apply(writeReportHistogram.getValueAtPercentile(99.9) / 1000.0),
                DEC_FORMAT.apply(writeReportHistogram.getMaxValue() / 1000.0),
                writeOffsets.toString(),

                INT_FORMAT.apply(readRate),
                DEC_FORMAT.apply(readBytesRate),
                DEC_FORMAT.apply(readReportHistogram.getValueAtPercentile(50) / 1000.0),
                DEC_FORMAT.apply(readReportHistogram.getValueAtPercentile(95) / 1000.0),
                DEC_FORMAT.apply(readReportHistogram.getValueAtPercentile(99) / 1000.0),
                DEC_FORMAT.apply(readReportHistogram.getValueAtPercentile(99.9) / 1000.0),
                DEC_FORMAT.apply(readReportHistogram.getMaxValue() / 1000.0),
                readOffsets.toString()
            );

            writeReportHistogram.reset();
            readReportHistogram.reset();

            oldTime = now;
        }
    }

    private static void generateWriteTraffic(StorageApi storageApi, long streamId) {
        double writeRate = arguments.requestsRate;
        RateLimiter limiter = RateLimiter.create(writeRate);
        Semaphore semaphore = new Semaphore(arguments.maxOutstandingRequests);

        byte[] value = new byte[arguments.valueSize];
        ByteBuf valueBuf = Unpooled.buffer(arguments.valueSize);
        valueBuf.writeBytes(value);

        try (StorageApi.StreamWriteLease ignoredLease =
                storageApi.acquireStreamWriteLease(streamId).join()) {

            while (true) {
                limiter.acquire();
                semaphore.acquireUninterruptibly();
                valueBuf.retain();

                long start = System.nanoTime();
                storageApi.append(streamId, 1, valueBuf)
                    .whenComplete((r, ex) -> {
                        if (ex != null) {
                            log.warn("Write operation failed {}", ex.getMessage());
                            writeFailed.increment();
                            try {
                                TimeUnit.SECONDS.sleep(1);
                            } catch (InterruptedException e) {
                                return;
                            }
                        } else {
                            writeOps.increment();
                            writeBytes.add(valueBuf.readableBytes());
                            long latencyMicros = NANOSECONDS.toMicros(System.nanoTime() - start);
                            writeLatency.recordValue(latencyMicros);
                            writeOffsets.put(streamId, r.header().offset());
                        }
                        valueBuf.release();
                        semaphore.release();
                    });
            }
        } finally {
            valueBuf.release();
        }
    }

    private static void generateReadTraffic(StorageApi storageApi, long streamId) {
        if (arguments.delayReadInSeconds < 0) {
            return;
        }
        try {
            TimeUnit.SECONDS.sleep(arguments.delayReadInSeconds);
        } catch (InterruptedException e) {
            return;
        }
        AtomicLong startOffset = new AtomicLong(arguments.startOffset);
        Semaphore semaphore = new Semaphore(1);
        while (true) {
            semaphore.acquireUninterruptibly();
            long start = System.nanoTime();
            storageApi.readEntries(streamId, startOffset.get(),
                    arguments.maxEntriesPerRead, arguments.maxSizePerRead)
                .whenCompleteAsync((entries, ex) -> {
                    if (ex != null || entries.isEmpty()) {
                        if (ex != null) {
                            log.warn("Read operation failed {}", ex.getMessage());
                        }
                        readFailed.increment();
                        try {
                            TimeUnit.SECONDS.sleep(1);
                        } catch (InterruptedException e) {
                            return;
                        }
                    } else {
                        readOps.increment();
                        long latencyMicros = NANOSECONDS.toMicros(System.nanoTime() - start);
                        readLatency.recordValue(latencyMicros);
                        entries.forEach(e -> {
                            readBytes.add(e.payload().readableBytes());
                            e.payload().release();
                        });
                        startOffset.addAndGet(entries.size());
                        readOffsets.put(streamId, startOffset.get());
                    }
                    semaphore.release();
                });

        }
    }
}
