/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.storage;

import com.google.common.util.concurrent.RateLimiter;
import io.lakestream.ursa.metrics.InstrumentProvider;
import io.lakestream.ursa.storage.impl.StorageConfig;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class FileStoragePerf {
    private static RateLimiter rateLimiter;

    public static void main(String[] args) throws Exception {
        final var type = getArgs(args, "--type", "local");
        final var bucket = getArgs(args, "--bucket", "ursa-storage");
        final var prefix = getArgs(args, "--prefix", "");
        final var dataSize = Long.parseLong((getArgs(args, "--data-size",
            Long.valueOf(1024 * 1024 * 4).toString())));
        final var rate = Integer.parseInt(getArgs(args, "--rate", "100"));
        final var dataCount = Integer.parseInt(getArgs(args, "--data-count", "100"));
        final var threadCount = Integer.parseInt(getArgs(args, "--thread-count", "1"));
        final var endpoint = getArgs(args, "--endpoint", "");

        final var executors = Executors.newFixedThreadPool(threadCount);

        rateLimiter = RateLimiter.create(rate);

        StorageConfig config = StorageConfig.fromProperties(putAllArgsIntoProperties(args));
        config.setBackendStorageType(type);
        config.setBucket(bucket);
        config.setStoragePath(bucket);
        config.setPrefix(prefix);
        config.setCloudStorageEndpoint(endpoint);

        log.info("Using the following configuration: {}", config);

        try (FileStorage fileStorage = FileStorage.create(config, InstrumentProvider.NOOP)) {
            run(fileStorage, dataSize, dataCount);
        }
    }

    static void run(FileStorage fileStorage, long dataSize, long dataCount)
        throws Exception {

        byte[] data = generateData(Math.toIntExact(dataSize));
        ByteBuf buf = Unpooled.wrappedBuffer(data);

        try {
            AtomicInteger counter = new AtomicInteger(0);
            new Thread(() -> {
                while (dataCount != counter.get()) {
                    log.info("Sending {} data", counter.get());
                    try {
                        TimeUnit.SECONDS.sleep(1);
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                }
            }).start();

            final String filename = generateRandomString(8) + "-";
            List<Long> allLatencies = Collections.synchronizedList(new ArrayList<>());
            List<CompletableFuture<Void>> futures = new ArrayList<>();
            for (int i = 0; i < dataCount; i++) {
                rateLimiter.acquire();
                long start = System.currentTimeMillis();
                CompletableFuture<Void> future = fileStorage.putAsync(buf, filename + i);
                future.whenComplete((v, t) -> {
                    counter.incrementAndGet();
                    if (t != null) {
                        log.error("Failed to put data", t);
                        return;
                    }
                    long end = System.currentTimeMillis();
                    allLatencies.add(end - start);
                });
                futures.add(future);
            }
            CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).get();

            log.info("Latency percentiles for all requests");
            showPercentiles(allLatencies);
            log.info("Latency percentiles for last 50% of requests");
            showPercentiles(allLatencies.subList(allLatencies.size() / 2, allLatencies.size()));
        } finally {
            buf.release();
        }
    }

    static String getArgs(String[] args, String key, String defaultValue) {
        for (int i = 0; i < args.length; i++) {
            if (args[i].equals(key)) {
                return args[i + 1];
            }
        }
        return defaultValue;
    }

    static Properties putAllArgsIntoProperties(String[] args) {
        Properties properties = new Properties();
        for (int i = 0; i < args.length; i++) {
            String key = args[i];
            if (key.startsWith("--")) {
                key = key.substring(2);
            }
            properties.put(key, args[++i]);
        }
        return properties;
    }

    static byte[] generateData(int size) {
        byte[] data = new byte[size];
        for (int i = 0; i < size; i++) {
            data[i] = (byte) (i % 128);
        }
        return data;
    }

    // generate a random string with length
    static String generateRandomString(int size) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < size; i++) {
            sb.append((char) (ThreadLocalRandom.current().nextInt(26) + 'a'));
        }
        return sb.toString();
    }

    static void showPercentiles(List<Long> input) {
        List<Long> latencies = new ArrayList<>(input);
        latencies.sort(Long::compareTo);
        log.info("50th percentile (Median): {} ms", calculatePercentile(latencies, 50.0));
        log.info("75th percentile: {} ms", calculatePercentile(latencies, 75.0));
        log.info("90th percentile: {} ms", calculatePercentile(latencies, 90.0));
        log.info("95th percentile: {} ms", calculatePercentile(latencies, 95.0));
        log.info("99th percentile: {} ms", calculatePercentile(latencies, 99.0));
        log.info("Mean: {} ms", (long) latencies.stream().mapToLong(Long::longValue).average().orElse(0));
        log.info("Min: {} ms", latencies.get(0));
        log.info("Max: {} ms", latencies.get(latencies.size() - 1));
    }

    static long calculatePercentile(List<Long> sortedLatencies, double percentile) {
        int index = (int) Math.ceil(percentile / 100.0 * sortedLatencies.size());
        return sortedLatencies.get(index - 1);
    }
}
