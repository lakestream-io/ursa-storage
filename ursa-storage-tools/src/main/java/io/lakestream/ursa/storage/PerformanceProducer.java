/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.storage;

import static java.util.concurrent.TimeUnit.NANOSECONDS;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.beust.jcommander.Parameters;
import com.google.common.util.concurrent.RateLimiter;
import io.lakestream.ursa.metrics.InstrumentProvider;
import io.lakestream.ursa.storage.impl.LocalFileStorage;
import io.lakestream.ursa.storage.impl.S3FileStorage;
import io.lakestream.ursa.storage.impl.StorageConfig;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.util.ReferenceCountUtil;
import io.netty.util.concurrent.DefaultThreadFactory;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.exporter.prometheus.PrometheusHttpServer;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.metrics.SdkMeterProvider;
import io.oxia.client.api.AsyncOxiaClient;
import io.oxia.client.api.OxiaClientBuilder;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;
import lombok.extern.slf4j.Slf4j;
import org.HdrHistogram.Histogram;
import org.HdrHistogram.HistogramLogWriter;
import org.HdrHistogram.Recorder;
import org.apache.commons.lang3.StringUtils;

@Slf4j
public class PerformanceProducer {

    private static final ExecutorService executor = Executors
        .newCachedThreadPool(new DefaultThreadFactory("ursa-perf-producer"));

    private static final LongAdder messagesSent = new LongAdder();
    private static final LongAdder messagesFailed = new LongAdder();
    private static final LongAdder bytesSent = new LongAdder();

    private static final LongAdder totalMessagesSent = new LongAdder();
    private static final LongAdder totalBytesSent = new LongAdder();

    private static final Recorder recorder = new Recorder(TimeUnit.SECONDS.toMicros(120000), 5);
    private static final Recorder cumulativeRecorder = new Recorder(TimeUnit.SECONDS.toMicros(120000), 5);

    static final DecimalFormat THROUGHPUTFORMAT = new PaddingDecimalFormat("0.0", 8);
    static final DecimalFormat DEC = new PaddingDecimalFormat("0.000", 7);
    static final DecimalFormat INTFORMAT = new PaddingDecimalFormat("0", 7);
    static final DecimalFormat TOTALFORMAT = new DecimalFormat("0.000");

    @Parameters(commandDescription = "Performance Producer")
    static class Arguments {
        @Parameter(names = { "-s", "--num-streams" }, description = "Number of streams. Default is 1",
            validateWith = PositiveNumberParameterValidator.class)
        public int numStreams = 1;
        @Parameter(names = { "-r", "--rate" }, description = "Write rate msg/s across streams. Default is 10_000")
        public int msgRate = 10000;
        @Parameter(names = {"-size", "--message-size"}, description = "Size of the message in bytes. Default is 1024")
        public int msgSize = 1024;
        @Parameter(names = { "-m",
            "--num-messages" }, description = "Number of messages to write in total. If <= 0, it will keep writing. "
            + "Default is 0")
        public long numMessages = 0;
        @Parameter(names = { "-time",
            "--test-duration" }, description = "Test duration in secs. If <= 0, it will keep publishing. Default is 0")
        public long testTime = 0;
        @Parameter(names = { "-h", "--help" }, description = "Help message", help = true)
        boolean help;
        @Parameter(names = { "-o", "--oxia-url" }, required = true, description = "Oxia URL")
        public String oxiaURL;
        @Parameter(names = {"-w", "--warmup-time"}, description = "Warmup time in seconds. Default is 30")
        public int warmupTimeSeconds = 10;
        @Parameter(names = {"-th", "--threads"}, description = "Number of threads to use. Default is 1")
        public int threads = 1;
        @Parameter(names = {"-b", "--bucket"}, description = "Bucket name")
        public String bucket;
        @Parameter(names = {"-p", "--prefix"}, description = "Bucket storage prefix")
        public String prefix = "storage-test";
        @Parameter(names = {"-rg", "--region"}, description = "Bucket region")
        public String region;
        @Parameter(names = {"-sp", "--storagePath"}, description = "File storage path")
        public String storagePath;
        @Parameter(names = { "-ef",
            "--exit-on-failure" }, description = "Exit from the process on publish failure (default: disable)")
        public boolean exitOnFailure = false;
        @Parameter(names = { "--histogram-file" }, description = "HdrHistogram output file")
        public String histogramFile = null;
        @Parameter(names = {"--port"}, description = "Port for Prometheus metrics. Default is 8099")
        public int port = 8099;
        @Parameter(names = {"-sid", "--streamid"}, description = "The stream id used to write messages. "
            + "Default is -1, which means the stream id will be generated by oxia")
        public long streamId = -1;

    }

    public static void main(String[] args) throws Exception {
        if (args.length == 0) {
            System.out.println("Usage: ursa-storage-perf CONF_FILE_PATH [options] [command] [command options]");
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
        jc.setProgramName("ursa-storage-produce-perf");

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

        var oxiaUrl = Utils.validateOxiaUrl(arguments.oxiaURL);
        AsyncOxiaClient oxia = OxiaClientBuilder.create(oxiaUrl.getLeft())
            .namespace(oxiaUrl.getRight())
            .openTelemetry(otel)
            .maxRequestsPerBatch(1000)
            .asyncClient()
            .get();

        log.info("Ursa storage config: {}", config);

        StorageApi storageApi = new UrsaStorage(config, otel, oxia).getDefaultStorageApi();

        startProduce(arguments, oxia, storageApi);

    }

    public static void startProduce(Arguments arguments, AsyncOxiaClient oxia, StorageApi storageApi) throws Exception {
        List<Long> streamIds = new ArrayList<>(arguments.numStreams);
        log.info("Creating {} streams", arguments.numStreams);
        if (arguments.streamId != -1) {
            for (int i = 0; i < arguments.numStreams; i++) {
                streamIds.add(arguments.streamId + i);
            }
        } else {
            for (int i = 0; i < arguments.numStreams; ++i) {
                long streamId = storageApi.generateStreamId().get();
                log.info("Created stream {}", streamId);
                streamIds.add(streamId);
            }
        }

        log.info("Starting write messages...");
        long start = System.nanoTime();
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            executorShutdownNow();
            printAggregatedThroughput(start);
            printAggregatedStats();
        }));

        CountDownLatch doneLatch = new CountDownLatch(arguments.threads);

        final long numMessagesPerThread = arguments.numMessages / arguments.threads;
        final int msgRatePerThread = arguments.msgRate / arguments.threads;

        for (int i = 0; i < arguments.threads; ++i) {
            final int threadId = i;
            executor.submit(() -> {
                log.info("Starting thread {}", threadId);
                writeMessages(arguments, storageApi, streamIds, numMessagesPerThread, msgRatePerThread, doneLatch);
            });
        }

        // Print report stats
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

            if (doneLatch.getCount() <= 0) {
                break;
            }

            long now = System.nanoTime();
            double elapsed = (now - oldTime) / 1e9;
            long total = totalMessagesSent.sum();
            double rate = messagesSent.sumThenReset() / elapsed;
            double failureRate = messagesFailed.sumThenReset() / elapsed;
            double throughput = bytesSent.sumThenReset() / elapsed / 1024 / 1024;

            reportHistogram = recorder.getIntervalHistogram(reportHistogram);

            log.info(
                "Throughput produced: {} msg --- {} msg/s --- {} MB/s  --- failure {} msg/s "
                    + "--- Latency: mean: "
                    + "{} ms - med: {} - 95pct: {} - 99pct: {} - 99.9pct: {} - 99.99pct: {} - Max: {}",
                INTFORMAT.format(total),
                THROUGHPUTFORMAT.format(rate),
                THROUGHPUTFORMAT.format(throughput),
                THROUGHPUTFORMAT.format(failureRate),
                DEC.format(reportHistogram.getMean() / 1000.0),
                DEC.format(reportHistogram.getValueAtPercentile(50) / 1000.0),
                DEC.format(reportHistogram.getValueAtPercentile(95) / 1000.0),
                DEC.format(reportHistogram.getValueAtPercentile(99) / 1000.0),
                DEC.format(reportHistogram.getValueAtPercentile(99.9) / 1000.0),
                DEC.format(reportHistogram.getValueAtPercentile(99.99) / 1000.0),
                DEC.format(reportHistogram.getMaxValue() / 1000.0));

            if (histogramLogWriter != null) {
                histogramLogWriter.outputIntervalHistogram(reportHistogram);
            }

            reportHistogram.reset();

            oldTime = now;
        }
    }


    private static void writeMessages(Arguments arguments, StorageApi storageApi,
                                      List<Long> streamIds, long numMessages, int msgRate, CountDownLatch doneLatch) {
        byte[] payload = new byte[arguments.msgSize];
        for (int i = 0; i < payload.length; ++i) {
            payload[i] = (byte) (ThreadLocalRandom.current().nextInt(26) + 65);
        }

        RateLimiter rateLimiter = null;
        if (msgRate > 0) {
            rateLimiter = RateLimiter.create(msgRate);
        }
        AtomicLong totalSent = new AtomicLong(0);
        boolean produceEnough = false;

        long startTime = System.nanoTime();
        long warmupEndTime = startTime + (long) (arguments.warmupTimeSeconds * 1e9);
        long testEndTime = startTime + (long) (arguments.testTime * 1e9);

        List<StorageApi.StreamWriteLease> leases = new ArrayList<>();
        try {
            for (long streamId : streamIds) {
                leases.add(storageApi.acquireStreamWriteLease(streamId).join());
            }
            while (true) {
                if (produceEnough) {
                    break;
                }

                for (long streamId : streamIds) {
                    if (arguments.testTime > 0 && System.nanoTime() - testEndTime > 0) {
                        log.info("------------- DONE (reached the maximum duration: [{} seconds] of producing) "
                            + "--------------", arguments.testTime);
                        doneLatch.countDown();
                        produceEnough = true;
                        break;
                    }

                    if (numMessages > 0 && totalSent.get() >= numMessages) {
                        log.info("------------- DONE (reached the maximum number: {} of production) --------------"
                            , numMessages);
                        doneLatch.countDown();
                        produceEnough = true;
                        break;
                    }

                    if (rateLimiter != null) {
                        rateLimiter.acquire();
                    }

                    final long sendTime = System.nanoTime();
                    ByteBuf writePayload = Unpooled.wrappedBuffer(payload);
                    storageApi.append(streamId, 1, writePayload)
                        .whenComplete((ignored, failure) ->
                            ReferenceCountUtil.safeRelease(writePayload))
                        .thenRun(() -> {
                            bytesSent.add(payload.length);
                            messagesSent.increment();
                            totalSent.incrementAndGet();
                            totalMessagesSent.increment();
                            totalBytesSent.add(payload.length);

                            long now = System.nanoTime();
                            if (now > warmupEndTime) {
                                long latencyMicros = NANOSECONDS.toMicros(now - sendTime);
                                recorder.recordValue(latencyMicros);
                                cumulativeRecorder.recordValue(latencyMicros);
                            }

                        }).exceptionally(ex -> {
                            if (ex.getCause() instanceof ArrayIndexOutOfBoundsException) {
                                return null;
                            }
                            log.warn("Write message error with exception ", ex);
                            messagesFailed.increment();
                            if (arguments.exitOnFailure) {
                                System.exit(1);
                            }
                            return null;
                        });
                }
            }
        } finally {
            for (int index = leases.size() - 1; index >= 0; index--) {
                leases.get(index).close();
            }
        }
    }

    private static FileStorage createBackendStorage(StorageConfig config,
                                                    InstrumentProvider instrumentProvider) {
        if (config.getStoragePath() != null) {
            createDirectoryIfNotExists(config.getStoragePath());
            return new LocalFileStorage(config, instrumentProvider);
        } else {
            return new S3FileStorage(config, instrumentProvider);
        }
    }

    private static void createDirectoryIfNotExists(String path) {
        // Create a File object
        File directory = new File(path);
        // Check if the directory exists
        if (!directory.exists()) {
            directory.mkdirs();
        }
    }

    private static void executorShutdownNow() {
        executor.shutdown();
        try {
            if (!executor.awaitTermination(10, TimeUnit.SECONDS)) {
                log.warn("Failed to terminate executor within timeout. The following are stack"
                    + " traces of still running threads.");
                executor.shutdownNow();
            }
        } catch (InterruptedException e) {
            log.warn("Shutdown of thread pool was interrupted");
        }
    }

    private static void printAggregatedThroughput(long start) {
        double elapsed = (System.nanoTime() - start) / 1e9;
        double rate = totalMessagesSent.sum() / elapsed;
        double throughput = totalBytesSent.sum() / elapsed / 1024 / 1024;

        log.info(
            "Aggregated throughput stats --- {} records sent --- {} msg/s --- {} MB/s ",
            totalMessagesSent.sum(),
            TOTALFORMAT.format(rate),
            TOTALFORMAT.format(throughput));
    }

    private static void printAggregatedStats() {
        Histogram reportHistogram = cumulativeRecorder.getIntervalHistogram();

        log.info(
            "Aggregated latency stats --- Latency: mean: {} ms - med: {} - 95pct: {} - 99pct: {} - 99.9pct: {} "
                + "- 99.99pct: {} - 99.999pct: {} - Max: {}",
            DEC.format(reportHistogram.getMean() / 1000.0),
            DEC.format(reportHistogram.getValueAtPercentile(50) / 1000.0),
            DEC.format(reportHistogram.getValueAtPercentile(95) / 1000.0),
            DEC.format(reportHistogram.getValueAtPercentile(99) / 1000.0),
            DEC.format(reportHistogram.getValueAtPercentile(99.9) / 1000.0),
            DEC.format(reportHistogram.getValueAtPercentile(99.99) / 1000.0),
            DEC.format(reportHistogram.getValueAtPercentile(99.999) / 1000.0),
            DEC.format(reportHistogram.getMaxValue() / 1000.0));
    }
}
