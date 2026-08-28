/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.storage;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.beust.jcommander.Parameters;
import com.google.gson.Gson;
import io.lakestream.ursa.metrics.InstrumentProvider;
import io.lakestream.ursa.storage.impl.StorageConfig;
import io.netty.util.concurrent.DefaultThreadFactory;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.exporter.prometheus.PrometheusHttpServer;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.metrics.SdkMeterProvider;
import io.oxia.client.api.AsyncOxiaClient;
import io.oxia.client.api.OxiaClientBuilder;
import java.io.FileInputStream;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

@Slf4j
public class PerformanceTailingRead {

    private static final ExecutorService consumer = Executors
        .newCachedThreadPool(new DefaultThreadFactory("ursa-storage-perf-consumer"));
    private static final ExecutorService producer = Executors
        .newCachedThreadPool(new DefaultThreadFactory("ursa-storage-perf-producer"));

    @Parameters(commandDescription = "Performance test for tailing read")
    static class TailingReadArguments {
        @Parameter(names = { "-s", "--stream" }, description = "Stream ID to produce and consume", required = true)
        public long streamId;

        @Parameter(names = {"-o", "--oxia-url"}, description = "Oxia client URL, default is localhost:6648")
        public String oxiaClientUri = "oxia://localhost:6648/default";

        @Parameter(names = { "-p", "--produce-rate" }, description = "Produce rate (msg/s), default is 10000")
        public int produceRate = 10000;

        @Parameter(names = { "-c", "--consume-rate" }, description = "Consume rate (msg/s), default is 10000")
        public int consumeRate = 10000;

        @Parameter(names = {"-pt", "--produce-threads"}, description = "Number of produce threads, default is 1")
        public int produceThreads = 1;

        @Parameter(names = {"-ct", "--consume-threads"}, description = "Number of consume threads, default is 1")
        public int consumeThreads = 1;

        @Parameter(names = {"-ce", "--consume-entries-per-read"},
            description = "Number of entries to consume per read, default is 1000")
        public int consumeEntriesPerRead = 1000;

        @Parameter(names = {"-cs", "--consume-size-per-read"},
            description = "Size of entries to consume per read, default is 1M")
        public int consumeSizePerRead = 1024 * 1024;

        @Parameter(names = {"-ps", "--produced-message-size"},
            description = "Size of produced messages, default is 1024")
        public int producedMessageSize = 1024;

        @Parameter(names = {"-mp", "--metrics-port"},
            description = "Port for metrics server, default is 8080")
        public int metricsPort = 8080;

    }


    public static void main(String[] args) throws Exception {
        if (args.length == 0) {
            System.out.println("Usage: ursa-perf CONF_FILE_PATH tailing-read [options] [command] [command options]");
            System.exit(1);
        }

        var config = getStorageConfig(args[0]);
        var arguments = parseArgs(args);
        try (var storageApi = initializeStorage(config, arguments.oxiaClientUri, arguments.metricsPort)) {
            CountDownLatch latch = new CountDownLatch(2);
            consumer.execute(()-> {
                try {
                    startingConsumeMessages(storageApi, arguments);
                } catch (Exception e) {
                    log.error("Failed to start consume messages", e);
                    latch.countDown();
                }
                latch.countDown();
            });
            producer.execute(() -> {
                try {
                    startingProduceMessages(storageApi, arguments);
                } catch (Exception e) {
                    log.error("Failed to start produce messages", e);
                    latch.countDown();
                }
                latch.countDown();
            });
            latch.await();
        }
    }

    private static StorageConfig getStorageConfig(String configFilePath) throws Exception {
        Properties properties = new Properties();
        if (!StringUtils.isBlank(configFilePath)) {
            try (FileInputStream fis = new FileInputStream(configFilePath)) {
                properties.load(fis);
            }
        }
        return StorageConfig.fromProperties(properties);
    }

    private static TailingReadArguments parseArgs(String[] args) {
        final TailingReadArguments arguments = new TailingReadArguments();
        JCommander jc = new JCommander(arguments);
        jc.setProgramName("ursa-storage-perf tailing-read");

        try {
            jc.parse(Arrays.copyOfRange(args, 1, args.length));
        } catch (ParameterException e) {
            jc.usage();
            System.exit(1);
        }
        return arguments;
    }

    private static StorageApi initializeStorage(StorageConfig config, String oxiaClientUrl, int metricsPort)
        throws Exception {

        log.info("Ursa storage config: {}", config);
        SdkMeterProvider meterProvider = SdkMeterProvider.builder()
            .registerMetricReader(PrometheusHttpServer.builder().setPort(metricsPort).build())
            .build();
        OpenTelemetry otel = OpenTelemetrySdk.builder()
            .setMeterProvider(meterProvider)
            .build();
        InstrumentProvider instrumentProvider = new InstrumentProvider(otel);
        var oxiaUrl = Utils.validateOxiaUrl(oxiaClientUrl);
        AsyncOxiaClient oxia = OxiaClientBuilder.create(oxiaUrl.getLeft())
            .namespace(oxiaUrl.getRight())
            .openTelemetry(otel)
            .maxRequestsPerBatch(1_000)
            .asyncClient()
            .get();
        UrsaStorage ursaStorage = new UrsaStorage(config, otel, oxia);
        return ursaStorage.getDefaultStorageApi();
    }

    // Write messages to the storage

    private static void startingProduceMessages(StorageApi storageApi, TailingReadArguments arguments)
        throws Exception {

        PerformanceProducer.Arguments producerArgs = new PerformanceProducer.Arguments();
        producerArgs.streamId = arguments.streamId;
        producerArgs.msgRate = arguments.produceRate;
        producerArgs.msgSize = arguments.producedMessageSize;
        producerArgs.threads = arguments.produceThreads;
        log.info("Starting produce messages with arguments: {}", new Gson().toJson(producerArgs));
        PerformanceProducer.startProduce(producerArgs, null, storageApi);
    }

    private static void startingConsumeMessages(StorageApi storageApi, TailingReadArguments arguments)
        throws Exception {

        PerformanceConsumer.Arguments consumerArgs = new PerformanceConsumer.Arguments();
        consumerArgs.streamId = arguments.streamId;
        consumerArgs.msgRate = arguments.consumeRate;
        consumerArgs.numThreads = arguments.consumeThreads;
        consumerArgs.batchSize = arguments.consumeEntriesPerRead;
        consumerArgs.bufferSize = arguments.consumeSizePerRead;
        log.info("Starting consume messages with arguments: {}", new Gson().toJson(consumerArgs));
        PerformanceConsumer.startConsume(consumerArgs, storageApi, null, 0, 0);
    }

}
