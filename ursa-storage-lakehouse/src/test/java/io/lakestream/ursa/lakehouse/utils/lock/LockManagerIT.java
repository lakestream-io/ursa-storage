/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.lakehouse.utils.lock;

import static io.lakestream.ursa.storage.UrsaStorageTestBase.OXIA_IMAGE;
import static java.util.function.Function.identity;

import io.grpc.netty.shaded.io.netty.util.concurrent.DefaultThreadFactory;
import io.lakestream.ursa.utils.lock.AsyncLock;
import io.lakestream.ursa.utils.lock.LockManager;
import io.lakestream.ursa.utils.lock.OptionAutoRevalidate;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.metrics.SdkMeterProvider;
import io.opentelemetry.sdk.metrics.data.MetricData;
import io.opentelemetry.sdk.resources.Resource;
import io.opentelemetry.sdk.testing.exporter.InMemoryMetricReader;
import io.opentelemetry.semconv.ServiceAttributes;
import io.oxia.client.api.AsyncOxiaClient;
import io.oxia.client.api.OxiaClientBuilder;
import io.oxia.testcontainers.OxiaContainer;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;
import lombok.AllArgsConstructor;
import lombok.Cleanup;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.shaded.org.awaitility.Awaitility;
import org.testcontainers.utility.DockerImageName;


@Slf4j
@Testcontainers
@Tag("lakehouse")
public class LockManagerIT {

    @Container
    private static final OxiaContainer oxia =
            new OxiaContainer(DockerImageName.parse(OXIA_IMAGE))
                    .withShards(10)
                    .withLogConsumer(new Slf4jLogConsumer(log));

    private final OpenTelemetry openTelemetry;
    private final InMemoryMetricReader metricReader;

    {
        final Resource resource =
                Resource.getDefault()
                        .merge(
                                Resource.create(
                                        Attributes.of(ServiceAttributes.SERVICE_NAME, "logical-service-name")));
        metricReader = InMemoryMetricReader.create();
        final SdkMeterProvider sdkMeterProvider =
                SdkMeterProvider.builder().registerMetricReader(metricReader).setResource(resource).build();
        openTelemetry = OpenTelemetrySdk.builder().setMeterProvider(sdkMeterProvider).build();
    }

    @Getter
    @AllArgsConstructor
    static class Counter {
        private int current;
        private final int total;

        public void increment() {
            this.current += 1;
        }
    }

    @Test
    public void testCounterWithSyncLock() throws InterruptedException {
        final String lockKey = UUID.randomUUID().toString();
        @Cleanup("shutdown")
        final ExecutorService service =
                Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
        final Map<String, AsyncOxiaClient> clients = new ConcurrentHashMap<>();
        final Map<String, LockManager> lockManager = new ConcurrentHashMap<>();
        final ScheduledExecutorService scheduledExecutorService = Executors.newScheduledThreadPool(Runtime.getRuntime().availableProcessors(),
                new DefaultThreadFactory("oxia-lock-manager"));
        try {
            final Function<String, AsyncOxiaClient> compute =
                    (threadName) ->
                            OxiaClientBuilder.create(oxia.getServiceAddress())
                                    .clientIdentifier(threadName)
                                    .openTelemetry(openTelemetry)
                                    .asyncClient()
                                    .join();
            final var counter = new Counter(0, 3000);
            final var latch = new CountDownLatch(counter.total);
            for (int i = 0; i < counter.total; i++) {
                service.execute(
                        () -> {
                            final String name = Thread.currentThread().getName();
                            final AsyncOxiaClient client = clients.computeIfAbsent(name, compute);
                            final LockManager lm =
                                    lockManager.computeIfAbsent(
                                            name,
                                            (n) ->
                                                    LockManagers.createLockManager(
                                                            client,
                                                            openTelemetry,
                                                            scheduledExecutorService,
                                                            new OptionAutoRevalidate(true, 10, 10, TimeUnit.SECONDS)));
                            final AsyncLock lock = lm.getSharedLock(lockKey);
                            lock.lock().join();
                            counter.increment();
                            lock.unlock().join();
                            log.info("counter : {}", counter.current);
                            latch.countDown();
                        });
            }

            latch.await();
            Assertions.assertEquals(counter.current, counter.total);
            metricReader.forceFlush();
            var metrics = metricReader.collectAllMetrics();
            var metricsByName =
                    metrics.stream().collect(Collectors.toMap(MetricData::getName, identity()));
            log.debug("metrics: {}", metricsByName);
            Assertions.assertTrue(metricsByName.containsKey("oxia.locks.status"));
            // ensure no delayed task leak
            Awaitility.await().untilAsserted(() -> {
                final Field workQueue = ThreadPoolExecutor.class.getDeclaredField("workQueue");
                workQueue.setAccessible(true);
                final BlockingQueue<Runnable> queue = (BlockingQueue<Runnable>) workQueue.get(scheduledExecutorService);
                Assertions.assertEquals(0, queue.size());
            });
        } finally {
            clients.forEach(
                    (s, c) -> {
                        try {
                            c.close();
                        } catch (Exception e) {
                            log.error("close oxia client failed", e);
                        }
                    });
            lockManager.forEach(
                    (s, c) -> {
                        try {
                            c.close();
                        } catch (Exception e) {
                            log.error("close lock manager failed", e);
                        }
                    }
            );
        }
    }

    @Test
    public void testCounterWithAsyncLock() throws InterruptedException {
        final String lockKey = UUID.randomUUID().toString();
        @Cleanup("shutdown")
        final ExecutorService service =
                Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
        final Map<String, AsyncOxiaClient> clients = new ConcurrentHashMap<>();
        List<LockManager> lockManagers = new ArrayList<>();
        try {
            final Function<String, AsyncOxiaClient> compute =
                    (threadName) ->
                            OxiaClientBuilder.create(oxia.getServiceAddress())
                                    .clientIdentifier(threadName)
                                    .openTelemetry(openTelemetry)
                                    .asyncClient()
                                    .join();
            final var counter = new Counter(0, 3000);
            final var latch = new CountDownLatch(counter.total);
            for (int i = 0; i < counter.total; i++) {
                service.execute(
                        () -> {
                            final String name = Thread.currentThread().getName();
                            final AsyncOxiaClient client = clients.computeIfAbsent(name, compute);
                            LockManager lockManager = LockManagers.createLockManager(
                                    client,
                                    openTelemetry,
                                    Executors.newSingleThreadScheduledExecutor(
                                            new DefaultThreadFactory("oxia-lock-manager")),
                                    OptionAutoRevalidate.DEFAULT);
                            lockManagers.add(lockManager);
                            final AsyncLock lm = lockManager.getSharedLock(lockKey);
                            lm.lock()
                                    .thenAccept(
                                            __ -> {
                                                counter.increment();
                                                log.info("counter : {}", counter.current);
                                            })
                                    .thenCompose(__ -> lm.unlock())
                                    .thenAccept(__ -> latch.countDown())
                                    .exceptionally(
                                            ex -> {
                                                Assertions.fail("unexpected exception", ex);
                                                return null;
                                            });
                        });
            }
            latch.await();
            Assertions.assertEquals(counter.current, counter.total);
            metricReader.forceFlush();
            var metrics = metricReader.collectAllMetrics();
            var metricsByName =
                    metrics.stream().collect(Collectors.toMap(MetricData::getName, identity()));
            log.debug("metrics: {}", metricsByName);
            Assertions.assertTrue(metricsByName.containsKey("oxia.locks.status"));
        } finally {
            clients.forEach(
                    (s, c) -> {
                        try {
                            c.close();
                        } catch (Exception e) {
                            log.error("close oxia client failed", e);
                        }
                    });
            for (LockManager lockManager : lockManagers) {
                try {
                    lockManager.close();
                } catch (Exception e) {
                    log.error("close lock manager failed", e);
                }
            }
        }
    }

    @Test
    public void testCounterWithReentrantSyncLock() throws InterruptedException {
        final String lockKey = UUID.randomUUID().toString();
        // 3 nodes with 10 threads.
        @Cleanup("shutdown")
        final ExecutorService service = Executors.newFixedThreadPool(10);
        final Map<Integer, AsyncOxiaClient> clients = new ConcurrentHashMap<>();
        final Map<Integer, LockManager> lockManager = new ConcurrentHashMap<>();
        try {
            final Function<Integer, AsyncOxiaClient> compute =
                    (threadName) ->
                            OxiaClientBuilder.create(oxia.getServiceAddress())
                                    .clientIdentifier(threadName + "")
                                    .openTelemetry(openTelemetry)
                                    .asyncClient()
                                    .join();
            final var counter = new Counter(0, 3000);
            final var latch = new CountDownLatch(counter.total);
            for (int i = 0; i < counter.total; i++) {
                service.execute(
                        () -> {
                            final String name = Thread.currentThread().getName();
                            int nodeId = name.hashCode() % 3;
                            final AsyncOxiaClient client = clients.computeIfAbsent(nodeId, compute);
                            final LockManager lm =
                                    lockManager.computeIfAbsent(
                                            nodeId,
                                            (n) ->
                                                    LockManagers.createLockManager(
                                                            client,
                                                            openTelemetry,
                                                            Executors.newSingleThreadScheduledExecutor(
                                                                    new DefaultThreadFactory("oxia-lock-manager")),
                                                            OptionAutoRevalidate.DEFAULT));
                            final AsyncLock lock = lm.getThreadSimpleLock(lockKey);
                            lock.lock().join();
                            counter.increment();
                            lock.unlock().join();
                            log.info("counter : {}", counter.current);
                            latch.countDown();
                        });
            }

            latch.await();
            Assertions.assertEquals(counter.current, counter.total);
            metricReader.forceFlush();
            var metrics = metricReader.collectAllMetrics();
            var metricsByName =
                    metrics.stream().collect(Collectors.toMap(MetricData::getName, identity()));
            log.debug("metrics: {}", metricsByName);
            Assertions.assertTrue(metricsByName.containsKey("oxia.locks.status"));
        } finally {
            clients.forEach(
                    (s, c) -> {
                        try {
                            c.close();
                        } catch (Exception e) {
                            log.error("close oxia client failed", e);
                        }
                    });
            lockManager.forEach(
                    (s, c) -> {
                        try {
                            c.close();
                        } catch (Exception e) {
                            log.error("close lock manager failed", e);
                        }
                    }
            );
        }
    }

    @Test
    public void testCounterWithReentrantAsyncLock() throws InterruptedException {
        final String lockKey = UUID.randomUUID().toString();
        // 3 nodes with 10 threads
        @Cleanup("shutdown")
        final ExecutorService service = Executors.newFixedThreadPool(10);
        final Map<Integer, AsyncOxiaClient> clients = new ConcurrentHashMap<>();
        final Map<Integer, LockManager> lockManager = new ConcurrentHashMap<>();
        try {
            final Function<Integer, AsyncOxiaClient> compute =
                    (nodeId) ->
                            OxiaClientBuilder.create(oxia.getServiceAddress())
                                    .clientIdentifier(nodeId + "")
                                    .openTelemetry(openTelemetry)
                                    .asyncClient()
                                    .join();
            final var counter = new Counter(0, 3000);
            final var latch = new CountDownLatch(counter.total);
            for (int i = 0; i < counter.total; i++) {
                service.execute(
                        () -> {
                            final String name = Thread.currentThread().getName();
                            final int nodeId = name.hashCode() % 3;
                            final AsyncOxiaClient client = clients.computeIfAbsent(nodeId, compute);
                            final LockManager lm =
                                    lockManager.computeIfAbsent(
                                            nodeId,
                                            (id) ->
                                                    LockManagers.createLockManager(
                                                            client,
                                                            openTelemetry,
                                                            Executors.newSingleThreadScheduledExecutor(
                                                                    new DefaultThreadFactory("oxia-lock-manager")),
                                                            OptionAutoRevalidate.DEFAULT));
                            final AsyncLock lock = lm.getThreadSimpleLock(lockKey);
                            lock.lock()
                                    .thenAccept(
                                            __ -> {
                                                counter.increment();
                                                log.info("counter : {}", counter.current);
                                            })
                                    .thenCompose(__ -> lock.unlock())
                                    .thenAccept(__ -> latch.countDown())
                                    .exceptionally(
                                            ex -> {
                                                Assertions.fail("unexpected exception", ex);
                                                return null;
                                            });
                        });
            }
            latch.await();
            Assertions.assertEquals(counter.current, counter.total);
            metricReader.forceFlush();
            var metrics = metricReader.collectAllMetrics();
            var metricsByName =
                    metrics.stream().collect(Collectors.toMap(MetricData::getName, identity()));
            log.debug("metrics: {}", metricsByName);
            Assertions.assertTrue(metricsByName.containsKey("oxia.locks.status"));
        } finally {
            clients.forEach(
                    (s, c) -> {
                        try {
                            c.close();
                        } catch (Exception e) {
                            log.error("close oxia client failed", e);
                        }
                    });
            lockManager.forEach(
                    (s, c) -> {
                        try {
                            c.close();
                        } catch (Exception e) {
                            log.error("close lock manager failed", e);
                        }
                    }
            );
        }
    }
}
