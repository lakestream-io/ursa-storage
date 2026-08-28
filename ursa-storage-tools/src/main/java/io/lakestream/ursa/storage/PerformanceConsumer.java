/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.storage;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.beust.jcommander.Parameters;
import com.google.common.util.concurrent.RateLimiter;
import io.lakestream.api.EntryHeader;
import io.lakestream.ursa.metrics.InstrumentProvider;
import io.lakestream.ursa.storage.impl.LocalFileStorage;
import io.lakestream.ursa.storage.impl.S3FileStorage;
import io.lakestream.ursa.storage.impl.StorageConfig;
import io.netty.util.concurrent.DefaultThreadFactory;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.exporter.prometheus.PrometheusHttpServer;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.metrics.SdkMeterProvider;
import io.oxia.client.api.AsyncOxiaClient;
import io.oxia.client.api.GetResult;
import io.oxia.client.api.OxiaClientBuilder;
import io.oxia.client.api.RangeScanConsumer;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.text.DecimalFormat;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;
import lombok.extern.slf4j.Slf4j;
import org.HdrHistogram.Histogram;
import org.HdrHistogram.HistogramLogWriter;
import org.HdrHistogram.Recorder;
import org.apache.commons.lang3.StringUtils;

@Slf4j
public class PerformanceConsumer {
    private static final ExecutorService executor = Executors
        .newCachedThreadPool(new DefaultThreadFactory("ursa-storage-perf-consumer"));
    private static final Set<Long> streamIds = new HashSet<>();
    private static final BlockingQueue<Long> streamIdQueue = new ArrayBlockingQueue<>(10000);
    private static final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
    private static final int MB = 1024 * 1024;

    private static final LongAdder messagesReceived = new LongAdder();
    private static final LongAdder bytesReceived = new LongAdder();
    private static final DecimalFormat intFormat = new PaddingDecimalFormat("0", 7);
    private static final DecimalFormat dec = new DecimalFormat("0.000");

    private static final LongAdder totalMessagesReceived = new LongAdder();
    private static final LongAdder totalBytesReceived = new LongAdder();

    private static final long MAX_LATENCY = TimeUnit.MINUTES.toMillis(10);
    private static final Recorder recorder = new Recorder(MAX_LATENCY, 5);
    private static final Recorder cumulativeRecorder = new Recorder(MAX_LATENCY, 5);

    @Parameters(commandDescription = "Performance Consumer")
    static class Arguments {
        @Parameter(names = {"-s", "--start-streamId"}, description = "Start stream id",
            validateWith = PositiveNumberParameterValidator.class)
        public long startStreamId = 0;
        @Parameter(names = {"-e", "--end-streamId"}, description = "End stream id",
            validateWith = PositiveNumberParameterValidator.class)
        public long endStreamId = Long.MAX_VALUE;
        @Parameter(names = {"-t", "--time"}, description = "Time to run in seconds. If <= 0, it will keep consuming")
        public int testTime = 0;
        @Parameter(names = { "-r", "--rate" }, description = "Fetch rate msg/s across streams. Default is 10_000")
        public int msgRate = 100000;
        @Parameter(names = {"-h", "--help"}, description = "Help message", help = true)
        boolean help;
        @Parameter(names = {"-o", "--oxia-url"}, required = true, description = "Oxia URL")
        public String oxiaUrl;
        @Parameter(names = {"-th", "--threads"}, description = "Number of threads to fetch messages. Default is 1.")
        public int numThreads = 1;
        @Parameter(names = {"-ef", "--exit-on-failure" },
            description = "Exit from the process on publish failure (default: disable)")
        public boolean exitOnFailure = false;
        @Parameter(names = {"-hf", "--histogram-file" }, description = "HdrHistogram output file")
        public String histogramFile = null;
        @Parameter(names = {"-port", "--port"}, description = "Port for Prometheus metrics. Default is 8099")
        public int port = 8098;
        @Parameter(names = {"-bs", "--batch-size"}, description = "Batch entries in one request. Default is 1000.")
        public int batchSize = 1000;
        @Parameter(names = {"-bfs", "--buffer-size"}, description = "Buffer size for each consumer. Default is 1MB.")
        public int bufferSize = MB;
        @Parameter(names = {"-b", "--bucket"}, description = "Bucket name")
        public String bucket;
        @Parameter(names = {"-p", "--prefix"}, description = "Bucket storage prefix")
        public String prefix = "storage-test";
        @Parameter(names = {"-rg", "--region"}, description = "Bucket region")
        public String region;
        @Parameter(names = {"-sp", "--storagePath"}, description = "File storage path")
        public String storagePath;
        @Parameter(names = {"-sid", "--streamId"}, description = "The reading stream id. "
            + "Negative or 0 means read all streamIds in the oxia")
        public Long streamId;
    }

    public static void main(String[] args) throws Exception {
        if (args.length == 0) {
            System.out.println("Usage: ursa-perf CONF_FILE_PATH consume [options] [command] [command options]");
            System.exit(1);
        }
        String configFile = args[0];
        Properties properties = new Properties();

        if (!StringUtils.isBlank(configFile)) {
            try (FileInputStream fis = new FileInputStream(configFile)) {
                properties.load(fis);
            }
        }

        StorageConfig config = StorageConfig.fromProperties(properties);

        final Arguments arguments = new Arguments();
        JCommander jc = new JCommander(arguments);
        jc.setProgramName("ursa-storage-consume-perf");

        try {
            jc.parse(Arrays.copyOfRange(args, 1, args.length));
        } catch (ParameterException e) {
            System.err.println(e.getMessage());
            jc.usage();
            return;
        }

        if (arguments.help) {
            jc.usage();
            return;
        }

        SdkMeterProvider meterProvider = SdkMeterProvider.builder()
            .registerMetricReader(PrometheusHttpServer.builder().setPort(arguments.port).build())
            .build();
        OpenTelemetry otel = OpenTelemetrySdk.builder()
            .setMeterProvider(meterProvider)
            .build();
        InstrumentProvider instrumentProvider = new InstrumentProvider(otel);

        var oxiaUrl = Utils.validateOxiaUrl(arguments.oxiaUrl);
        AsyncOxiaClient oxia = OxiaClientBuilder.create(oxiaUrl.getLeft())
            .namespace(oxiaUrl.getRight())
            .openTelemetry(otel)
            .maxRequestsPerBatch(1_000)
            .asyncClient()
            .get();

        long start = System.nanoTime();
        long testEndTime = start + (long) (arguments.testTime * 1e9);

        log.info("Ursa storage config: {}", config);
        StorageApi storageApi = new UrsaStorage(config, otel, oxia).getDefaultStorageApi();

        startConsume(arguments, storageApi, oxia, start, testEndTime);

    }

    public static void startConsume(Arguments arguments, StorageApi storageApi, AsyncOxiaClient oxia, long start,
                                    long testEndTime) throws Exception {
        scheduler.scheduleWithFixedDelay(() ->
                monitorNewStreamIds(arguments.startStreamId, arguments.endStreamId, oxia),
            0, 120, TimeUnit.SECONDS);

        if (arguments.streamId < 0) {
            log.info("Waiting 30s for detecting existing streamIds...");
            Thread.sleep(30_000);
        }

        final int msgRatePerThread = arguments.msgRate / arguments.numThreads;
        for (int i = 0; i < arguments.numThreads; ++i) {
            final int threadId = i;
            executor.submit(() -> {
                log.info("Starting thread {}", threadId);
                consumeMessages(storageApi, arguments, oxia, msgRatePerThread, testEndTime);
            });
        }


        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            executorShutdownNow();
            printAggregatedThroughput(start);
            printAggregatedStats();
        }));


        long oldTime = System.nanoTime();

        Histogram reportHistogram = null;
        HistogramLogWriter histogramLogWriter = null;

        if (arguments.histogramFile != null) {
            String statsFileName = arguments.histogramFile;
            log.info("Dumping latency stats to {}", statsFileName);

            PrintStream histogramLog = new PrintStream(
                    new FileOutputStream(statsFileName), false, StandardCharsets.UTF_8);
            histogramLogWriter = new HistogramLogWriter(histogramLog);

            // Some log header bits
            histogramLogWriter.outputLogFormatVersion();
            histogramLogWriter.outputLegend();
        }

        while (true) {
            try {
                Thread.sleep(10000);
            } catch (InterruptedException e) {
                break;
            }

            long now = System.nanoTime();
            double elapsed = (now - oldTime) / 1e9;
            long total = totalMessagesReceived.sum();
            double rate = messagesReceived.sumThenReset() / elapsed;
            double throughput = bytesReceived.sumThenReset() / elapsed / 1024 / 1024;
            reportHistogram = recorder.getIntervalHistogram(reportHistogram);

            log.info(
                "Throughput received: {} msg --- {}  msg/s --- {} MB/s  "
                    + "--- Latency: mean: {} ms - med: {} "
                    + "- 95pct: {} - 99pct: {} - 99.9pct: {} - 99.99pct: {} - Max: {}",
                intFormat.format(total),
                dec.format(rate), dec.format(throughput), dec.format(reportHistogram.getMean()),
                reportHistogram.getValueAtPercentile(50), reportHistogram.getValueAtPercentile(95),
                reportHistogram.getValueAtPercentile(99), reportHistogram.getValueAtPercentile(99.9),
                reportHistogram.getValueAtPercentile(99.99), reportHistogram.getMaxValue());

            if (histogramLogWriter != null) {
                histogramLogWriter.outputIntervalHistogram(reportHistogram);
            }

            reportHistogram.reset();
            oldTime = now;

            if (arguments.testTime > 0) {
                if (now > testEndTime) {
                    log.info("------------------- DONE -----------------------");
                    executorShutdownNow();
                }
            }
        }
    }

    private static FileStorage createBackendStorage(StorageConfig config,
                                                    InstrumentProvider instrumentProvider) {
        if (!StringUtils.isBlank(config.getStoragePath())) {
            return new LocalFileStorage(config, instrumentProvider);
        } else {
            return new S3FileStorage(config, instrumentProvider);
        }
    }

    private static void consumeMessages(StorageApi storageApi, Arguments arguments,
                                        AsyncOxiaClient oxiaClient, int msgRate, long testEndTime) {
        Map<Long, Long> consumedStreamIdOffsetMap = new HashMap<>();
        BlockingQueue<Long> queue = new ArrayBlockingQueue<>(1000);

        RateLimiter rateLimiter = null;
        if (msgRate > 0) {
            rateLimiter = RateLimiter.create(msgRate);
        }

        while (true) {
            if (arguments.testTime > 0) {
                if (System.nanoTime() > testEndTime) {
                    log.info("--------------- Thread {} is done --------------------", Thread.currentThread().getId());
                    Thread.currentThread().interrupt();
                    return;
                }
            }

            try {
                Long streamId;
                if (arguments.streamId > 0) {
                    streamId = arguments.streamId;
                } else {
                    streamId = streamIdQueue.poll(30, TimeUnit.SECONDS);
                    if (streamId == null) {
                        log.info("No new streamIds to consume, continue to fetch from existing streamIds");
                        streamId = queue.poll(30, TimeUnit.SECONDS);
                        if (streamId == null) {
                            log.info("No streamIds to consume, backoff 30s to wait for new streamIds");
                            Thread.sleep(30_000);
                            continue;
                        }
                    }
                }


                long lastOffset = -1;
                if (arguments.streamId > 0) {
                    lastOffset = Long.MAX_VALUE;
                } else {
                    // get last offset of this streamId
                    var lastEntry = storageApi.getLastEntry(streamId).get();
                    if (lastEntry.header() == EntryHeader.NOT_FOUND) {
                        log.error("Failed to get offset for streamId {}", streamId);
                        queue.add(streamId);
                        continue;
                    }
                    lastOffset = lastEntry.header().offset();
                }

                long startOffset = 0;
                if (consumedStreamIdOffsetMap.containsKey(streamId)) {
                    startOffset = consumedStreamIdOffsetMap.get(streamId);
                }


                // start fetch messages from this stream until offset
                log.info("Start fetch messages in stream {} from {} to offset {}", streamId, startOffset, lastOffset);
                while (startOffset < lastOffset) {
                    try {
                        int batchSize = arguments.batchSize;
                        if (rateLimiter != null) {
                            if (batchSize > msgRate) {
                                batchSize = msgRate;
                            }
                            rateLimiter.acquire(batchSize);
                        }

                        long start = System.currentTimeMillis();
                        List<Entry> entries =
                            storageApi.readEntries(streamId, startOffset, batchSize, arguments.bufferSize)
                                .get();
                        if (entries.isEmpty()) {
                            log.info("No more entries for streamId {} at offset {}", streamId, startOffset);
                            break;
                        }

                        log.debug("Fetched {} entries for streamId {} at offset {}", entries.size(), streamId,
                            startOffset);
                        startOffset = entries.get(entries.size() - 1).header().offset() + 1;
                        entries.forEach(entry -> entry.payload().release());

                        for (Entry entry : entries) {
                            long latency = System.currentTimeMillis() - start;
                            recorder.recordValue(latency);
                            cumulativeRecorder.recordValue(latency);
                            messagesReceived.add(entry.header().numberOfMessages());
                            bytesReceived.add(entry.header().entrySize());
                            totalMessagesReceived.add(entry.header().numberOfMessages());
                            totalBytesReceived.add(entry.header().entrySize());
                        }
                    } catch (Exception e) {
                        log.error("Failed to read entries for streamId {} at offset {}", streamId, startOffset, e);
                        if (arguments.exitOnFailure) {
                            System.exit(1);
                        }
                        return;
                    }
                }
                log.info("Reach the end of the stream {}, current offset {}, last offset {}", streamId, startOffset,
                    lastOffset);
                consumedStreamIdOffsetMap.put(streamId, startOffset);
                queue.add(streamId);
            } catch (InterruptedException | ExecutionException e) {
                log.error("Interrupted while waiting for streamId", e);
                return;
            }
        }
    }


    private static void monitorNewStreamIds(long startId, long endId, AsyncOxiaClient oxiaClient) {
        Map<Long, Long> streamIdOffsetMap = new HashMap<>();
        CountDownLatch latch = new CountDownLatch(1);
        // get streamIds
        oxiaClient.rangeScan(Key.smallestKey(startId).toString(),
            Key.largestKey(endId).toString(),
            new RangeScanConsumer() {
                @Override
                public boolean onNext(GetResult result) {
                    Key key = Key.parse(result.key());
                    if (streamIds.contains(key.streamId())) {
                        return true;
                    }
                    streamIdOffsetMap.compute(key.streamId(),
                        (k, v) -> v == null ? key.offset() : Math.max(v, key.offset()));
                    return true;
                }

                @Override
                public void onError(Throwable throwable) {
                    latch.countDown();
                }

                @Override
                public void onCompleted() {
                    latch.countDown();
                }
            });

        try {
            latch.await();
            log.info("Detected the following new streamIds.");
            streamIdOffsetMap.forEach((k, v) -> log.info("StreamId: {}, Offset: {}", k, v));
            streamIds.addAll(streamIdOffsetMap.keySet());
            streamIdQueue.addAll(streamIdOffsetMap.keySet());
        } catch (InterruptedException e) {
            log.error("Failed to get streamIds", e);
        }
    }

    private static void printAggregatedThroughput(long start) {
        double elapsed = (System.nanoTime() - start) / 1e9;
        double rate = totalMessagesReceived.sum() / elapsed;
        double throughput = totalBytesReceived.sum() / elapsed / 1024 / 1024;
        log.info(
            "Aggregated throughput stats --- {} records received --- {} msg/s --- {} MB/s",
            totalMessagesReceived.sum(),
            dec.format(rate),
            dec.format(throughput));
    }

    private static void printAggregatedStats() {
        Histogram reportHistogram = cumulativeRecorder.getIntervalHistogram();

        log.info(
            "Aggregated latency stats --- Latency: mean: {} ms - med: {} - 95pct: {} - 99pct: {} - 99.9pct: {} "
                + "- 99.99pct: {} - 99.999pct: {} - Max: {}",
            dec.format(reportHistogram.getMean()), reportHistogram.getValueAtPercentile(50),
            reportHistogram.getValueAtPercentile(95), reportHistogram.getValueAtPercentile(99),
            reportHistogram.getValueAtPercentile(99.9), reportHistogram.getValueAtPercentile(99.99),
            reportHistogram.getValueAtPercentile(99.999), reportHistogram.getMaxValue());
    }

    private static void executorShutdownNow() {
        executor.shutdown();
        scheduler.shutdown();
        try {
            if (!executor.awaitTermination(10, TimeUnit.SECONDS)) {
                log.warn("Failed to terminate executor within timeout. The following are stack"
                    + " traces of still running threads.");
                executor.shutdownNow();
            }

            if (!scheduler.awaitTermination(10, TimeUnit.SECONDS)) {
                log.warn("Failed to terminate scheduler within timeout. The following are stack"
                    + " traces of still running threads.");
                scheduler.shutdownNow();
            }
        } catch (InterruptedException e) {
            log.warn("Shutdown of thread pool was interrupted");
        }
    }

}
